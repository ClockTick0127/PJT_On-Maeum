"""Batch loader: outbox events / initial snapshot -> Parquet -> HDFS -> manifest -> DB.

Processing order for one batch::

    (DB) assign events to a batch            <- short transaction
    build Parquet                            <- no DB transaction
    upload to <final>.tmp, verify, rename    <- no DB transaction
    write manifest atomically                <- no DB transaction
    (DB) mark events delivered, batch done   <- short transaction

Any failure leaves the batch ASSIGNED/FAILED with its events still attached,
so the next run retries the same batch id and the same events. Confirmed
files (listed in a manifest) are never overwritten; an existing file is only
reused when its content is identical.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
import json
import logging
from pathlib import Path
import tempfile
from typing import Any

from session_lake_loader.lake_schema import (
    build_table,
    content_fingerprint,
    outbox_event_to_row,
    parquet_bytes,
    parquet_row_count,
    read_parquet_rows,
    sha256_hex,
    snapshot_to_row,
)
from session_lake_loader.repository import (
    BATCH_ASSIGNED,
    BATCH_KIND_INCREMENTAL,
    BATCH_KIND_INITIAL,
    BatchRecord,
    Clock,
    SessionLakeRepository,
    utc_now,
)
from session_lake_loader.storage import LakeStorage, parent_of


logger = logging.getLogger(__name__)

RESULT_COMPLETED = "COMPLETED"
RESULT_FAILED = "FAILED"


class LakeIntegrityError(RuntimeError):
    """A confirmed or existing lake file does not match what this batch produced."""


@dataclass(frozen=True)
class FileInfo:
    path: str
    rows: int
    bytes: int
    sha256: str
    content_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "rows": self.rows,
            "bytes": self.bytes,
            "sha256": self.sha256,
            "content_sha256": self.content_sha256,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "FileInfo":
        return cls(
            path=str(value["path"]),
            rows=int(value["rows"]),
            bytes=int(value["bytes"]),
            sha256=str(value["sha256"]),
            content_sha256=str(value.get("content_sha256", "")),
        )


@dataclass
class ProcessResult:
    batch_id: str
    batch_kind: str
    status: str
    row_count: int = 0
    file_count: int = 0
    resumed_from_manifest: bool = False
    stage: str | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "batch_kind": self.batch_kind,
            "status": self.status,
            "row_count": self.row_count,
            "file_count": self.file_count,
            "resumed_from_manifest": self.resumed_from_manifest,
            "stage": self.stage,
            "error": self.error,
        }


@dataclass
class _SpooledPart:
    local_path: Path
    rows: int
    content_sha256: str


@dataclass
class _InitialLoadOutput:
    files: list[FileInfo] = field(default_factory=list)
    snapshot: dict[str, Any] = field(default_factory=dict)


class SessionLakeLoader:
    def __init__(
        self,
        repository: SessionLakeRepository,
        storage: LakeStorage,
        *,
        rows_per_file: int = 200_000,
        retry_backoff_seconds: float = 30.0,
        retry_max_backoff_seconds: float = 900.0,
        max_attempts: int = 0,
        spool_dir: str | None = None,
        metrics: Any | None = None,
        clock: Clock = utc_now,
    ) -> None:
        if rows_per_file < 1:
            raise ValueError("rows_per_file must be at least 1")
        self._repository = repository
        self._storage = storage
        self._rows_per_file = rows_per_file
        self._retry_backoff = retry_backoff_seconds
        self._retry_max_backoff = retry_max_backoff_seconds
        self._max_attempts = max_attempts
        self._spool_dir = spool_dir or None
        self._metrics = metrics
        self._clock = clock

    # ---------- 실행 단위 ----------

    def run_incremental_cycle(
        self,
        *,
        batch_max_events: int,
        max_batches: int,
    ) -> list[ProcessResult]:
        """Retry incomplete incremental batches, then drain pending events."""

        results: list[ProcessResult] = []
        now = self._clock()
        for batch in self._repository.incomplete_batches():
            if batch.batch_kind == BATCH_KIND_INITIAL:
                # 초기 적재는 명령으로만 재개한다. 실행 중일 수 있어 자동 재시도하지 않는다.
                logger.warning(
                    "미완료 초기 적재 배치가 있습니다. initial-load 명령으로 재개하세요: %s",
                    batch.batch_id,
                )
                continue
            if not self.retry_due(batch, now):
                continue
            results.append(self.process_batch(batch))

        for _ in range(max_batches):
            batch = self._repository.assign_incremental_batch(
                batch_max_events,
                ingest_date=self._clock().date(),
            )
            if batch is None:
                break
            result = self.process_batch(batch)
            results.append(result)
            if result.status == RESULT_FAILED:
                # 같은 원인으로 연속 실패할 가능성이 높으므로 다음 주기까지 기다린다.
                break
        return results

    def run_initial_load(
        self,
        *,
        allow_repeat: bool = False,
        after_snapshot_opened: Callable[[], None] | None = None,
    ) -> ProcessResult:
        """Load every current session from one DB snapshot into a single batch."""

        enabled = self._repository.lake_capture_enabled()
        if enabled is False:
            raise RuntimeError(
                "세션 변경 포착 트리거가 없습니다. 초기 적재 전에 "
                "realtime-analysis-service의 `alembic upgrade head`를 먼저 적용하세요."
            )
        if enabled is None:
            logger.warning("트리거를 확인할 수 없는 DB입니다(PostgreSQL 아님). 그대로 진행합니다.")

        batch = self._repository.find_incomplete_initial_batch()
        if batch is None:
            if self._repository.completed_initial_batch_exists() and not allow_repeat:
                raise RuntimeError(
                    "완료된 초기 적재 배치가 이미 있습니다. 다시 실행하려면 "
                    "--allow-repeat 를 지정하세요(같은 세션·버전 중복은 복원 시 제거됩니다)."
                )
            batch = self._repository.create_initial_batch(ingest_date=self._clock().date())
            logger.info("초기 적재 배치 생성: %s", batch.batch_id)
        else:
            logger.info(
                "미완료 초기 적재 배치를 재개합니다: %s (attempt=%s)",
                batch.batch_id,
                batch.attempt_count,
            )
        return self.process_batch(batch, after_snapshot_opened=after_snapshot_opened)

    def retry_due(self, batch: BatchRecord, now: datetime) -> bool:
        if batch.status == BATCH_ASSIGNED:
            return True
        if self._max_attempts and batch.attempt_count >= self._max_attempts:
            return False
        if batch.last_error_at is None:
            return True
        exponent = max(batch.attempt_count - 1, 0)
        backoff = min(self._retry_backoff * (2**exponent), self._retry_max_backoff)
        return (now - batch.last_error_at).total_seconds() >= backoff

    def is_stalled(self, batch: BatchRecord) -> bool:
        return bool(self._max_attempts) and batch.attempt_count >= self._max_attempts

    # ---------- 배치 처리 ----------

    def process_batch(
        self,
        batch: BatchRecord,
        *,
        after_snapshot_opened: Callable[[], None] | None = None,
    ) -> ProcessResult:
        stage = "manifest-check"
        try:
            if self._storage.exists(batch.manifest_path):
                # 파일과 manifest는 확정됐지만 DB 완료 처리 전에 중단된 경우.
                manifest = json.loads(self._storage.read_bytes(batch.manifest_path))
                stage = "verify-confirmed-files"
                files = self._verify_manifest_files(manifest)
                stage = "finalize"
                row_count = sum(info.rows for info in files)
                self._repository.mark_batch_completed(
                    batch.batch_id,
                    row_count=row_count,
                    file_count=len(files),
                    details={
                        "files": [info.to_dict() for info in files],
                        "manifest_path": batch.manifest_path,
                        "resumed_from_manifest": True,
                    },
                )
                result = ProcessResult(
                    batch_id=str(batch.batch_id),
                    batch_kind=batch.batch_kind,
                    status=RESULT_COMPLETED,
                    row_count=row_count,
                    file_count=len(files),
                    resumed_from_manifest=True,
                )
                self._observe_result(result)
                return result

            extra: dict[str, Any] = {}
            if batch.batch_kind == BATCH_KIND_INCREMENTAL:
                stage = "build"
                rows = self._incremental_rows(batch)
                stage = "upload"
                files = self._commit_rows(
                    batch,
                    rows,
                    name_for=lambda index: f"part-{index:05d}.parquet",
                )
            else:
                stage = "snapshot-upload"
                output = self._initial_files(batch, after_snapshot_opened)
                files = output.files
                extra = {"snapshot": output.snapshot}

            stage = "manifest"
            manifest = self._build_manifest(batch, files, extra)
            self._atomic_write(
                batch.manifest_path,
                json.dumps(manifest, ensure_ascii=False, indent=1, default=str).encode("utf-8"),
            )

            stage = "finalize"
            row_count = sum(info.rows for info in files)
            self._repository.mark_batch_completed(
                batch.batch_id,
                row_count=row_count,
                file_count=len(files),
                details={
                    "files": [info.to_dict() for info in files],
                    "manifest_path": batch.manifest_path,
                    **extra,
                },
            )
            result = ProcessResult(
                batch_id=str(batch.batch_id),
                batch_kind=batch.batch_kind,
                status=RESULT_COMPLETED,
                row_count=row_count,
                file_count=len(files),
            )
            logger.info(
                "배치 완료: %s kind=%s rows=%s files=%s",
                batch.batch_id,
                batch.batch_kind,
                row_count,
                len(files),
            )
            self._observe_result(result)
            return result
        except Exception as error:  # noqa: BLE001 - 배치 실패는 기록하고 다음 주기에 재시도한다.
            message = f"{type(error).__name__}: {error}"
            logger.exception(
                "배치 실패: %s kind=%s stage=%s", batch.batch_id, batch.batch_kind, stage
            )
            try:
                self._repository.mark_batch_failed(batch.batch_id, stage=stage, error=message)
            except Exception:  # noqa: BLE001
                logger.exception("배치 실패 기록에 실패했습니다: %s", batch.batch_id)
            result = ProcessResult(
                batch_id=str(batch.batch_id),
                batch_kind=batch.batch_kind,
                status=RESULT_FAILED,
                stage=stage,
                error=message,
            )
            self._observe_result(result)
            return result

    # ---------- 증분 ----------

    def _incremental_rows(self, batch: BatchRecord) -> list[dict[str, Any]]:
        events = self._repository.batch_events(batch.batch_id)
        if len(events) != batch.event_count:
            raise LakeIntegrityError(
                f"배치 대상 이벤트 수 불일치: expected={batch.event_count} actual={len(events)}"
            )
        return [outbox_event_to_row(event, batch) for event in events]

    def _commit_rows(
        self,
        batch: BatchRecord,
        rows: list[dict[str, Any]],
        *,
        name_for: Callable[[int], str],
    ) -> list[FileInfo]:
        directory = self._repository.batch_directory(batch)
        files: list[FileInfo] = []
        for index in range(0, len(rows), self._rows_per_file):
            chunk = rows[index : index + self._rows_per_file]
            data = parquet_bytes(build_table(chunk))
            path = f"{directory}/{name_for(len(files))}"
            files.append(
                self._commit_file(
                    path,
                    data,
                    expected_rows=len(chunk),
                    expected_content_sha256=content_fingerprint(chunk),
                )
            )
        return files

    # ---------- 초기 적재 ----------

    def _initial_files(
        self,
        batch: BatchRecord,
        after_snapshot_opened: Callable[[], None] | None,
    ) -> _InitialLoadOutput:
        directory = self._repository.batch_directory(batch)
        # manifest가 없으므로 이 디렉터리의 파일은 모두 확정 전 잔여물이다.
        for name in self._storage.list(directory):
            logger.warning("확정되지 않은 이전 시도 파일을 제거합니다: %s/%s", directory, name)
            self._storage.delete(f"{directory}/{name}", recursive=True)

        attempt = batch.attempt_count + 1
        output = _InitialLoadOutput()
        with tempfile.TemporaryDirectory(
            dir=self._spool_dir, prefix="session-lake-initial-"
        ) as spool:
            parts: list[_SpooledPart] = []
            total_rows = 0
            with self._repository.snapshot_sessions() as (snapshot_info, records):
                snapshot_at = self._clock()
                chunk: list[dict[str, Any]] = []
                hook_called = False
                for record in records:
                    if not hook_called and after_snapshot_opened is not None:
                        # 첫 행을 읽은 뒤이므로 스냅샷은 이미 고정됐다.
                        after_snapshot_opened()
                        hook_called = True
                    chunk.append(snapshot_to_row(record, batch, snapshot_at))
                    total_rows += 1
                    if len(chunk) >= self._rows_per_file:
                        parts.append(self._spool(spool, len(parts), chunk))
                        chunk = []
                if not hook_called and after_snapshot_opened is not None:
                    after_snapshot_opened()
                if chunk:
                    parts.append(self._spool(spool, len(parts), chunk))
            # 여기서 DB 트랜잭션이 끝났다. 업로드는 트랜잭션 밖에서 진행한다.
            output.snapshot = {
                **snapshot_info,
                "attempt": attempt,
                "row_count": total_rows,
            }
            for index, part in enumerate(parts):
                path = f"{directory}/part-a{attempt:02d}-{index:05d}.parquet"
                output.files.append(
                    self._commit_file(
                        path,
                        part.local_path.read_bytes(),
                        expected_rows=part.rows,
                        expected_content_sha256=part.content_sha256,
                    )
                )
        return output

    @staticmethod
    def _spool(spool_dir: str, index: int, rows: list[dict[str, Any]]) -> _SpooledPart:
        local_path = Path(spool_dir) / f"part-{index:05d}.parquet"
        local_path.write_bytes(parquet_bytes(build_table(rows)))
        return _SpooledPart(
            local_path=local_path,
            rows=len(rows),
            content_sha256=content_fingerprint(rows),
        )

    # ---------- 저장소 쓰기 ----------

    def _commit_file(
        self,
        path: str,
        data: bytes,
        *,
        expected_rows: int,
        expected_content_sha256: str,
    ) -> FileInfo:
        digest = sha256_hex(data)
        if self._storage.exists(path):
            existing = self._storage.read_bytes(path)
            existing_digest = sha256_hex(existing)
            if existing_digest == digest:
                logger.info("동일한 파일이 이미 있어 재사용합니다: %s", path)
                return FileInfo(path, expected_rows, len(existing), digest, expected_content_sha256)
            try:
                existing_rows = read_parquet_rows(existing)
            except Exception as error:  # noqa: BLE001 - 손상된 기존 파일도 덮어쓰지 않는다.
                raise LakeIntegrityError(
                    f"기존 파일을 Parquet로 읽을 수 없습니다. 덮어쓰지 않습니다: {path} ({error})"
                ) from error
            if (
                len(existing_rows) == expected_rows
                and content_fingerprint(existing_rows) == expected_content_sha256
            ):
                logger.info("내용이 같은 기존 파일을 재사용합니다(바이트 상이): %s", path)
                return FileInfo(
                    path, expected_rows, len(existing), existing_digest, expected_content_sha256
                )
            raise LakeIntegrityError(
                f"기존 파일의 내용이 이번 배치와 다릅니다. 덮어쓰지 않습니다: {path}"
            )

        temporary = f"{path}.tmp"
        self._storage.makedirs(parent_of(path))
        self._storage.write_bytes(temporary, data)
        uploaded_size = self._storage.size(temporary)
        if uploaded_size != len(data):
            raise LakeIntegrityError(
                f"업로드 크기 불일치: {temporary} ({uploaded_size} != {len(data)})"
            )
        readback = self._storage.read_bytes(temporary)
        if sha256_hex(readback) != digest:
            raise LakeIntegrityError(f"업로드 체크섬 불일치: {temporary}")
        uploaded_rows = parquet_row_count(readback)
        if uploaded_rows != expected_rows:
            raise LakeIntegrityError(
                f"업로드 행 수 불일치: {temporary} ({uploaded_rows} != {expected_rows})"
            )
        self._storage.rename(temporary, path)
        return FileInfo(path, expected_rows, len(data), digest, expected_content_sha256)

    def _atomic_write(self, path: str, data: bytes) -> None:
        if self._storage.exists(path):
            raise LakeIntegrityError(f"확정 파일이 이미 존재합니다: {path}")
        temporary = f"{path}.tmp"
        self._storage.makedirs(parent_of(path))
        self._storage.write_bytes(temporary, data)
        if self._storage.size(temporary) != len(data):
            raise LakeIntegrityError(f"업로드 크기 불일치: {temporary}")
        if sha256_hex(self._storage.read_bytes(temporary)) != sha256_hex(data):
            raise LakeIntegrityError(f"업로드 체크섬 불일치: {temporary}")
        self._storage.rename(temporary, path)

    def _verify_manifest_files(self, manifest: dict[str, Any]) -> list[FileInfo]:
        files = [FileInfo.from_dict(entry) for entry in manifest.get("files", [])]
        for info in files:
            if not self._storage.exists(info.path):
                raise LakeIntegrityError(f"manifest에 있는 확정 파일이 없습니다: {info.path}")
            if self._storage.size(info.path) != info.bytes:
                raise LakeIntegrityError(f"확정 파일 크기가 manifest와 다릅니다: {info.path}")
            if sha256_hex(self._storage.read_bytes(info.path)) != info.sha256:
                raise LakeIntegrityError(f"확정 파일 체크섬이 manifest와 다릅니다: {info.path}")
        return files

    def _build_manifest(
        self,
        batch: BatchRecord,
        files: list[FileInfo],
        extra: dict[str, Any],
    ) -> dict[str, Any]:
        combined = sha256_hex(
            "\n".join(info.content_sha256 for info in files).encode("utf-8")
        )
        return {
            "job": "session-lake-loader",
            "schema_version": batch.schema_version,
            "batch_kind": batch.batch_kind,
            "batch_id": str(batch.batch_id),
            "ingest_date": batch.ingest_date.isoformat(),
            "event_count": batch.event_count,
            "first_event_id": batch.first_event_id,
            "last_event_id": batch.last_event_id,
            "row_count": sum(info.rows for info in files),
            "file_count": len(files),
            "files": [info.to_dict() for info in files],
            "content_sha256": combined,
            "attempt": batch.attempt_count + 1,
            "created_at": batch.created_at.isoformat() if batch.created_at else None,
            "completed_at": self._clock().isoformat(),
            **extra,
        }

    def _observe_result(self, result: ProcessResult) -> None:
        if self._metrics is not None:
            self._metrics.observe_result(result)
