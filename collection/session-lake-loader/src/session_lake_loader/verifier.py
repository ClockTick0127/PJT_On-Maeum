"""Restore the latest session state from the lake and compare it with the DB.

Restore rule per ``session_id``: keep the row with the highest
``session_version``. Delete tombstones are kept as the final state. Rows with a
lower version that arrive later (or in a later file) are ignored. Two rows with
the same session and version but different content are a conflict and fail the
verification.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import logging
from typing import Any, Iterable
from uuid import UUID

from session_lake_loader.lake_schema import (
    content_signature,
    read_parquet_rows,
    session_content_row,
    sha256_hex,
)
from session_lake_loader.repository import (
    SessionLakeRepository,
    VerificationSnapshot,
)
from session_lake_loader.storage import LakeStorage


logger = logging.getLogger(__name__)


@dataclass
class RestoredSession:
    session_id: str
    version: int
    row: dict[str, Any]
    signature: str
    source: str

    @property
    def is_deleted(self) -> bool:
        return bool(self.row.get("is_deleted"))


@dataclass
class RestoreResult:
    sessions: dict[str, RestoredSession] = field(default_factory=dict)
    conflicts: list[dict[str, Any]] = field(default_factory=list)
    rows_read: int = 0
    duplicate_rows: int = 0
    ignored_lower_versions: int = 0

    @property
    def live_count(self) -> int:
        return sum(1 for item in self.sessions.values() if not item.is_deleted)

    @property
    def deleted_count(self) -> int:
        return sum(1 for item in self.sessions.values() if item.is_deleted)


class StateRestorer:
    """Incrementally folds lake rows into the latest state per session."""

    def __init__(self) -> None:
        self._result = RestoreResult()
        self._seen: dict[tuple[str, int], tuple[str, str]] = {}

    def add_rows(self, rows: Iterable[dict[str, Any]], source: str = "") -> None:
        result = self._result
        for row in rows:
            result.rows_read += 1
            session_id = str(row["session_id"])
            version = int(row["session_version"])
            signature = content_signature(row)

            key = (session_id, version)
            previous = self._seen.get(key)
            if previous is None:
                self._seen[key] = (signature, source)
            else:
                if previous[0] == signature:
                    result.duplicate_rows += 1
                else:
                    result.conflicts.append(
                        {
                            "session_id": session_id,
                            "session_version": version,
                            "first_source": previous[1],
                            "second_source": source,
                            "first_content": previous[0],
                            "second_content": signature,
                        }
                    )
                # 이미 반영된 버전이므로 최신 상태는 바뀌지 않는다.
                if version <= result.sessions[session_id].version:
                    continue

            current = result.sessions.get(session_id)
            if current is None or version > current.version:
                result.sessions[session_id] = RestoredSession(
                    session_id=session_id,
                    version=version,
                    row=row,
                    signature=signature,
                    source=source,
                )
            elif version < current.version:
                result.ignored_lower_versions += 1

    def result(self) -> RestoreResult:
        return self._result


def restore_latest_state(rows: Iterable[dict[str, Any]], source: str = "") -> RestoreResult:
    restorer = StateRestorer()
    restorer.add_rows(rows, source)
    return restorer.result()


@dataclass
class ComparisonResult:
    matched_live: int = 0
    matched_deleted: int = 0
    pending: list[dict[str, Any]] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict[str, Any]:
        return {
            "matched_live": self.matched_live,
            "matched_deleted": self.matched_deleted,
            "pending_count": len(self.pending),
            "pending": self.pending[:100],
            "error_count": len(self.errors),
            "errors": self.errors[:100],
        }


def compare_with_database(
    restored: RestoreResult,
    snapshot: VerificationSnapshot,
) -> ComparisonResult:
    """Compare restored state with a fixed DB snapshot.

    ``snapshot.unloaded`` explains every legitimate difference: versions that
    exist in the outbox but were not part of any manifest the verifier read.
    Anything not explained by unloaded versions is an error.
    """

    result = ComparisonResult()
    session_ids = set(snapshot.sessions) | set(restored.sessions) | set(snapshot.unloaded)
    for session_id in sorted(session_ids):
        db = snapshot.sessions.get(session_id)
        lake = restored.sessions.get(session_id)
        unloaded = snapshot.unloaded.get(session_id)

        if db is not None:
            db_version = int(db.lake_version)
            if lake is None:
                if unloaded is not None and unloaded.min_version == 1:
                    result.pending.append(
                        _entry("NOT_LOADED_YET", session_id, db_version=db_version)
                    )
                else:
                    result.errors.append(
                        _entry("MISSING_IN_LAKE", session_id, db_version=db_version)
                    )
            elif lake.is_deleted:
                result.errors.append(
                    _entry(
                        "LAKE_DELETED_BUT_DB_ALIVE",
                        session_id,
                        db_version=db_version,
                        lake_version=lake.version,
                    )
                )
            elif lake.version == db_version:
                if lake.signature == content_signature(session_content_row(db)):
                    result.matched_live += 1
                else:
                    result.errors.append(
                        _entry(
                            "CONTENT_MISMATCH",
                            session_id,
                            db_version=db_version,
                            lake_version=lake.version,
                            lake_content=lake.signature,
                            db_content=content_signature(session_content_row(db)),
                        )
                    )
            elif lake.version < db_version:
                if unloaded is not None and unloaded.min_version <= lake.version + 1:
                    result.pending.append(
                        _entry(
                            "LAKE_BEHIND_PENDING",
                            session_id,
                            db_version=db_version,
                            lake_version=lake.version,
                        )
                    )
                else:
                    result.errors.append(
                        _entry(
                            "LAKE_BEHIND",
                            session_id,
                            db_version=db_version,
                            lake_version=lake.version,
                        )
                    )
            else:
                result.errors.append(
                    _entry(
                        "LAKE_AHEAD",
                        session_id,
                        db_version=db_version,
                        lake_version=lake.version,
                    )
                )
            continue

        # DB에 행이 없다: 삭제되었거나(레이크에 tombstone 필요) 아직 전달 전이다.
        if lake is None:
            if unloaded is not None:
                result.pending.append(_entry("DELETED_BEFORE_LOAD", session_id))
            else:
                result.errors.append(_entry("UNKNOWN_SESSION", session_id))
        elif lake.is_deleted:
            result.matched_deleted += 1
        elif unloaded is not None and unloaded.has_delete:
            result.pending.append(
                _entry("DELETE_PENDING", session_id, lake_version=lake.version)
            )
        else:
            result.errors.append(
                _entry("MISSING_DELETE_IN_LAKE", session_id, lake_version=lake.version)
            )
    return result


def _entry(reason: str, session_id: str, **details: Any) -> dict[str, Any]:
    return {"reason": reason, "session_id": session_id, **details}


@dataclass
class VerificationReport:
    manifests_read: int
    batch_ids: list[str]
    files_read: int
    file_errors: list[dict[str, Any]]
    restore: RestoreResult
    comparison: ComparisonResult
    snapshot_taken_at: str
    snapshot_id: str | None

    @property
    def ok(self) -> bool:
        return (
            not self.file_errors
            and not self.restore.conflicts
            and self.comparison.ok
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "manifests_read": self.manifests_read,
            "batch_ids": self.batch_ids,
            "files_read": self.files_read,
            "file_errors": self.file_errors,
            "rows_read": self.restore.rows_read,
            "duplicate_rows": self.restore.duplicate_rows,
            "ignored_lower_versions": self.restore.ignored_lower_versions,
            "restored_live_sessions": self.restore.live_count,
            "restored_deleted_sessions": self.restore.deleted_count,
            "conflicts": self.restore.conflicts[:100],
            "conflict_count": len(self.restore.conflicts),
            "comparison": self.comparison.to_dict(),
            "snapshot_taken_at": self.snapshot_taken_at,
            "snapshot_id": self.snapshot_id,
        }

    def summary_lines(self) -> list[str]:
        lines = [
            f"result: {'OK' if self.ok else 'FAILED'}",
            f"manifests: {self.manifests_read}  files: {self.files_read}  rows: {self.restore.rows_read}",
            f"duplicates dropped: {self.restore.duplicate_rows}  "
            f"lower versions ignored: {self.restore.ignored_lower_versions}",
            f"restored live: {self.restore.live_count}  deleted: {self.restore.deleted_count}",
            f"matched live: {self.comparison.matched_live}  "
            f"matched deleted: {self.comparison.matched_deleted}",
            f"pending (unloaded outbox): {len(self.comparison.pending)}",
            f"conflicts: {len(self.restore.conflicts)}  file errors: {len(self.file_errors)}  "
            f"comparison errors: {len(self.comparison.errors)}",
        ]
        for error in self.comparison.errors[:20]:
            lines.append(f"  error {error}")
        for conflict in self.restore.conflicts[:20]:
            lines.append(f"  conflict {conflict}")
        for error in self.file_errors[:20]:
            lines.append(f"  file {error}")
        return lines


class LakeReader:
    """Reads confirmed manifests and the Parquet files they list."""

    def __init__(self, storage: LakeStorage, manifest_base: str) -> None:
        self._storage = storage
        self._manifest_base = manifest_base.rstrip("/")

    def manifest_paths(self) -> list[str]:
        paths: list[str] = []
        for date_dir in self._storage.list(self._manifest_base):
            if not date_dir.startswith("date="):
                continue
            directory = f"{self._manifest_base}/{date_dir}"
            for name in self._storage.list(directory):
                if name.startswith("manifest-") and name.endswith(".json"):
                    paths.append(f"{directory}/{name}")
        return sorted(paths)

    def load_manifests(self) -> list[dict[str, Any]]:
        manifests = []
        for path in self.manifest_paths():
            manifest = json.loads(self._storage.read_bytes(path))
            manifest["_path"] = path
            manifests.append(manifest)
        manifests.sort(key=lambda item: (item.get("completed_at") or "", item["_path"]))
        return manifests


class SessionLakeVerifier:
    def __init__(
        self,
        repository: SessionLakeRepository,
        storage: LakeStorage,
        *,
        manifest_base: str | None = None,
    ) -> None:
        self._repository = repository
        self._storage = storage
        self._reader = LakeReader(storage, manifest_base or repository.manifest_base)

    def run(self) -> VerificationReport:
        # 1) 레이크를 먼저 읽는다. 이후 DB 스냅샷은 "읽은 manifest 집합"을 기준으로
        #    전달 전 이벤트를 구분하므로, 그 사이 새 배치가 완료돼도 오류로 보지 않는다.
        manifests = self._reader.load_manifests()
        restorer = StateRestorer()
        file_errors: list[dict[str, Any]] = []
        files_read = 0
        for manifest in manifests:
            for entry in manifest.get("files", []):
                path = entry["path"]
                if not self._storage.exists(path):
                    file_errors.append({"path": path, "reason": "MISSING"})
                    continue
                data = self._storage.read_bytes(path)
                if sha256_hex(data) != entry.get("sha256"):
                    file_errors.append({"path": path, "reason": "CHECKSUM_MISMATCH"})
                    continue
                rows = read_parquet_rows(data)
                if len(rows) != int(entry.get("rows", len(rows))):
                    file_errors.append({"path": path, "reason": "ROW_COUNT_MISMATCH"})
                    continue
                restorer.add_rows(rows, source=path)
                files_read += 1
        restored = restorer.result()

        # 2) 고정된 DB 기준(REPEATABLE READ 스냅샷)과 비교한다.
        batch_ids = {UUID(str(manifest["batch_id"])) for manifest in manifests}
        snapshot = self._repository.verification_snapshot(batch_ids)
        comparison = compare_with_database(restored, snapshot)
        return VerificationReport(
            manifests_read=len(manifests),
            batch_ids=sorted(str(value) for value in batch_ids),
            files_read=files_read,
            file_errors=file_errors,
            restore=restored,
            comparison=comparison,
            snapshot_taken_at=snapshot.taken_at.isoformat(),
            snapshot_id=snapshot.snapshot_id,
        )
