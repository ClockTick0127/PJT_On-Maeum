"""완료 manifest 기록과 활성 실행 버전 반영.

파일 이동과 DB 반영은 하나의 트랜잭션이 아니다. 그래서 순서를 고정한다.

1. 실행별 임시 경로에 결과 저장
2. 검증
3. 검증한 디렉터리를 실행별 최종 경로로 이동
4. 완료 manifest 기록  ← 여기까지 끝나면 결과는 파일로 확정된 것이다
5. DB 트랜잭션으로 활성 실행 버전 변경  ← 여기부터 소비 가능

4와 5 사이에서 죽으면 파일은 있는데 아무도 읽지 않는 상태가 된다. 다음 실행이
:meth:`SilverCommitRepository.recover` 로 manifest를 찾아 DB만 다시 반영한다.
같은 입력·규칙·설정이면 다시 계산하지 않는다.

이 복구에는 함정이 하나 있다. 오래된 실행의 manifest를 뒤늦게 반영하면서 **더 최신
실행의 결과를 활성 자리에서 밀어내면 안 된다.** 날짜당 활성 버전이 하나라는 제약만으로는
"어느 하나"만 보장되지 시도 순서는 보장되지 않는다. 그래서 활성화는 항상 현재 활성 실행의
시도 번호와 비교해서 진행한다(:meth:`_activate`).

시작·검증·활성화·복구는 모두 날짜별 단일 작성자 잠금 안에서 돈다(:meth:`date_lock`).
잠금이 없으면 두 실행이 같은 날짜를 중복 계산하고, 시도 번호 채번(`MAX + 1`)이 경쟁해
유니크 제약 위반으로 죽는다.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
import logging
import threading
from uuid import UUID, uuid4

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session, sessionmaker

from realtime_analysis.models import (
    LakeBatchRun,
    LakeDatasetDependency,
    LakeDatasetVersion,
)

from power_silver.constants import (
    JOB_NAME,
    RUN_FAILED,
    RUN_RUNNING,
    RUN_SUCCEEDED,
    VERSION_ACTIVE,
    VERSION_SUPERSEDED,
)
from power_silver.storage import LakeStorage


logger = logging.getLogger(__name__)

# PostgreSQL이 아닌 백엔드(테스트용 SQLite)에서 쓰는 대체 잠금. 프로세스 간 보호는
# 되지 않으므로 운영 경로는 반드시 PostgreSQL이어야 한다.
_PROCESS_LOCKS: dict[str, threading.Lock] = {}
_PROCESS_LOCKS_GUARD = threading.Lock()


class LockNotAcquired(RuntimeError):
    """다른 실행이 이미 이 날짜를 쓰고 있다."""


class ConflictingRunHistory(RuntimeError):
    """manifest가 가리키는 시도 번호를 DB의 다른 실행이 이미 쓰고 있다."""


@dataclass(frozen=True)
class RunHandle:
    run_id: UUID
    attempt: int


@dataclass(frozen=True)
class UpstreamRef:
    """manifest에서 되살린 상위 버전 참조.

    `catalog.DatasetVersionRef`와 같은 속성을 가지므로 둘 다 `publish`에 넘길 수 있다.
    """

    dataset_name: str
    target_date: date
    run_id: UUID
    version_id: UUID


def dependency_entries(depends_on: Iterable) -> list[dict]:
    """manifest에 실을 형태. 복구는 DB가 아니라 이 기록으로 의존성을 되살린다."""

    return [
        {
            "dataset_name": upstream.dataset_name,
            "target_date": upstream.target_date.isoformat(),
            "run_id": str(upstream.run_id),
            "version_id": str(upstream.version_id),
        }
        for upstream in depends_on
    ]


def _dependencies_from_manifest(manifest: dict) -> list[UpstreamRef]:
    return [
        UpstreamRef(
            dataset_name=entry["dataset_name"],
            target_date=date.fromisoformat(entry["target_date"]),
            run_id=UUID(str(entry["run_id"])),
            version_id=UUID(str(entry["version_id"])),
        )
        for entry in manifest.get("depends_on") or []
    ]


def manifest_path(manifest_base: str, target_date: date, run_id: UUID | str) -> str:
    return (
        f"{manifest_base}/target_date={target_date.isoformat()}"
        f"/run_id={run_id}/manifest.json"
    )


def write_manifest(storage: LakeStorage, path: str, manifest: dict) -> None:
    """임시 경로에 쓰고 최종 이름으로 옮긴다. 반쯤 쓰인 manifest를 읽는 일이 없게 한다."""

    payload = json.dumps(manifest, ensure_ascii=False, indent=1, sort_keys=True).encode()
    temporary = path + ".tmp"
    storage.write_bytes(temporary, payload)
    if storage.exists(path):
        storage.delete(path)
    storage.rename(temporary, path)


def read_manifests(
    storage: LakeStorage, manifest_base: str, target_date: date
) -> list[dict]:
    """대상 날짜의 완료 manifest를 모두 읽는다(실행 순서대로)."""

    base = f"{manifest_base}/target_date={target_date.isoformat()}"
    manifests: list[dict] = []
    for name in storage.list(base):
        path = f"{base}/{name}/manifest.json"
        if not storage.exists(path):
            continue
        try:
            payload = json.loads(storage.read_bytes(path))
        except ValueError:
            logger.warning("Unreadable silver manifest skipped: %s", path)
            continue
        payload["manifest_path"] = path
        manifests.append(payload)
    # 시도 번호가 실행 순서다. 복구도 그 순서로 반영해야 마지막 실행이 활성으로 남는다.
    return sorted(
        manifests,
        key=lambda item: (int(item.get("attempt") or 0), item.get("completed_at") or ""),
    )


class SilverCommitRepository:
    """lake_batch_run / lake_dataset_version 갱신."""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        *,
        job_name: str = JOB_NAME,
    ) -> None:
        self._session_factory = session_factory
        self._job_name = job_name
        self._held: dict[str, int] = {}

    # ---------- 날짜별 단일 작성자 잠금 ----------

    def date_lock(self, target_date: date):
        return self.lock(f"{self._job_name}:{target_date.isoformat()}")

    @contextmanager
    def lock(self, lock_name: str):
        """같은 이름의 잠금은 이 인스턴스 안에서 재진입할 수 있다.

        `run_daily`가 전체를 감싸고 그 안에서 `recover`가 다시 잠그려 해도, 새 연결로
        advisory lock을 잡으면 자기 자신 때문에 실패한다. 보유 횟수를 세어 막는다.
        """

        if self._held.get(lock_name):
            self._held[lock_name] += 1
            try:
                yield
            finally:
                self._held[lock_name] -= 1
            return

        release = self._acquire(lock_name)
        self._held[lock_name] = 1
        try:
            yield
        finally:
            self._held.pop(lock_name, None)
            release()

    def _acquire(self, lock_name: str):
        session = self._session_factory()
        if session.get_bind().dialect.name != "postgresql":
            session.close()
            with _PROCESS_LOCKS_GUARD:
                process_lock = _PROCESS_LOCKS.setdefault(lock_name, threading.Lock())
            if not process_lock.acquire(blocking=False):
                raise LockNotAcquired(lock_name)
            return process_lock.release

        acquired = bool(
            session.execute(
                text("SELECT pg_try_advisory_lock(hashtext(:name))"),
                {"name": lock_name},
            ).scalar_one()
        )
        if not acquired:
            session.close()
            raise LockNotAcquired(lock_name)

        def release() -> None:
            try:
                session.execute(
                    text("SELECT pg_advisory_unlock(hashtext(:name))"),
                    {"name": lock_name},
                )
            finally:
                session.close()

        return release

    def start(
        self,
        target_date: date,
        *,
        input_snapshot_id: str | None,
        rule_version: str,
        config_version: str,
        status: str = RUN_RUNNING,
        details: dict | None = None,
    ) -> RunHandle:
        with self._session_factory.begin() as session:
            attempt = int(
                session.execute(
                    select(func.coalesce(func.max(LakeBatchRun.attempt), 0) + 1).where(
                        LakeBatchRun.job_name == self._job_name,
                        LakeBatchRun.target_date == target_date,
                    )
                ).scalar_one()
            )
            run = LakeBatchRun(
                run_id=uuid4(),
                job_name=self._job_name,
                target_date=target_date,
                attempt=attempt,
                status=status,
                input_snapshot_id=input_snapshot_id,
                rule_version=rule_version,
                config_version=config_version,
                started_at=datetime.now(timezone.utc),
                details=details or {},
            )
            session.add(run)
            session.flush()
            return RunHandle(run_id=run.run_id, attempt=attempt)

    def set_status(self, run_id: UUID, status: str) -> None:
        with self._session_factory.begin() as session:
            run = session.get(LakeBatchRun, run_id)
            if run is not None:
                run.status = status

    def finish(
        self,
        run_id: UUID,
        status: str,
        *,
        details: dict | None = None,
        error_message: str | None = None,
    ) -> None:
        with self._session_factory.begin() as session:
            run = session.get(LakeBatchRun, run_id)
            if run is None:
                return
            run.status = status
            run.finished_at = datetime.now(timezone.utc)
            if details is not None:
                run.details = {**(run.details or {}), **details}
            run.error_message = error_message

    def fail(self, run_id: UUID, error: BaseException) -> None:
        self.finish(run_id, RUN_FAILED, error_message=f"{type(error).__name__}: {error}")

    def publish(
        self,
        manifest: dict,
        depends_on: Iterable = (),
        *,
        on_activated: Callable[[Session], None] | None = None,
    ) -> UUID:
        """manifest가 가리키는 출력들을 활성 버전으로 만든다(여러 번 불러도 같은 결과).

        ``depends_on``은 이 실행이 소비한 상위 데이터셋 버전
        (:class:`power_silver.catalog.DatasetVersionRef`)이다. 활성화와 **같은 트랜잭션**
        에 기록해야 "확정은 됐는데 무엇을 썼는지는 모르는" 결과가 생기지 않는다. 상위가
        재처리되면 이 기록으로 다시 계산할 날짜를 찾는다.
        """

        run_id = UUID(str(manifest["run_id"]))
        target_date = date.fromisoformat(manifest["target_date"])
        with self.date_lock(target_date), self._session_factory.begin() as session:
            self._ensure_run(session, manifest, run_id, target_date)
            # 실패한 트랜잭션과 함께 사라졌을 수 있으므로 복구 때는 manifest에서 되살린다.
            self._record_dependencies(
                session, run_id, list(depends_on) or _dependencies_from_manifest(manifest)
            )
            activated = True
            for dataset_name, output in sorted(manifest["outputs"].items()):
                activated &= self._activate(
                    session,
                    dataset_name=dataset_name,
                    target_date=target_date,
                    run_id=run_id,
                    output=output,
                    manifest=manifest,
                )
            run = session.get(LakeBatchRun, run_id)
            run.status = RUN_SUCCEEDED
            run.finished_at = datetime.now(timezone.utc)
            run.details = {
                **(run.details or {}),
                "manifest_path": manifest.get("manifest_path"),
                "outputs": {
                    name: output.get("row_count")
                    for name, output in manifest["outputs"].items()
                },
                # 결과는 확정됐지만 더 최신 실행이 이미 활성이라 자리를 넘겨받지 않았다.
                "activated": activated,
                # 다음 실행이 출력 분할 수를 정할 때 읽는 실측값.
                "bytes_per_row": {
                    name: value
                    for name, value in (manifest.get("bytes_per_row") or {}).items()
                    if value
                },
            }
            # Gold uses this hook to create its delivery outbox in the exact
            # transaction that makes all component datasets ACTIVE.  A callback
            # is deliberately not run for a stale recovered attempt.
            if activated and on_activated is not None:
                on_activated(session)
        return run_id

    def _ensure_run(
        self,
        session: Session,
        manifest: dict,
        run_id: UUID,
        target_date: date,
    ) -> None:
        if session.get(LakeBatchRun, run_id) is not None:
            return
        # manifest는 있는데 실행 기록이 없다(DB 복원 등). manifest 내용으로 되살린다.
        next_attempt = int(
            session.execute(
                select(func.coalesce(func.max(LakeBatchRun.attempt), 0) + 1).where(
                    LakeBatchRun.job_name == self._job_name,
                    LakeBatchRun.target_date == target_date,
                )
            ).scalar_one()
        )
        attempt = int(manifest.get("attempt") or next_attempt)
        # 시도 번호는 실행 순서를 나타내고 활성 버전 비교의 기준이다. 다른 실행이 이미
        # 쓰고 있으면 새 번호를 붙여 순서를 꾸며내지 않고 사람이 보게 남긴다.
        taken = session.scalar(
            select(LakeBatchRun.run_id).where(
                LakeBatchRun.job_name == self._job_name,
                LakeBatchRun.target_date == target_date,
                LakeBatchRun.attempt == attempt,
            )
        )
        if taken is not None:
            raise ConflictingRunHistory(
                f"manifest {manifest.get('manifest_path')} claims attempt {attempt} "
                f"of {target_date}, which run {taken} already owns"
            )
        session.add(
            LakeBatchRun(
                run_id=run_id,
                job_name=manifest.get("job", self._job_name),
                target_date=target_date,
                attempt=attempt,
                status=RUN_RUNNING,
                input_snapshot_id=manifest.get("input_snapshot_id"),
                rule_version=manifest["rule_version"],
                config_version=manifest["config_version"],
                started_at=datetime.now(timezone.utc),
                details={"recovered_from_manifest": True},
            )
        )
        session.flush()

    def _activate(
        self,
        session: Session,
        *,
        dataset_name: str,
        target_date: date,
        run_id: UUID,
        output: dict,
        manifest: dict,
    ) -> bool:
        """이 실행의 출력을 등록한다. 활성 자리를 가져갔으면 True.

        뒤늦게 복구되는 오래된 실행이 더 최신 실행을 활성 자리에서 밀어내면, 소비자가
        이미 읽은 결과가 과거 것으로 되돌아간다. 시도 번호로 순서를 비교해 막는다.
        """

        existing = session.scalars(
            select(LakeDatasetVersion).where(
                LakeDatasetVersion.dataset_name == dataset_name,
                LakeDatasetVersion.target_date == target_date,
            )
        ).all()
        incoming_attempt = self._attempt_of(session, run_id)
        current = None
        active = None
        for version in existing:
            if version.run_id == run_id:
                current = version
            elif version.status == VERSION_ACTIVE:
                active = version

        stale = active is not None and (
            self._attempt_of(session, active.run_id) > incoming_attempt
        )
        if stale:
            logger.warning(
                "Keeping the newer active version of %s %s: run %s is an older attempt",
                dataset_name,
                target_date,
                run_id,
            )
        elif active is not None:
            active.status = VERSION_SUPERSEDED

        status = VERSION_SUPERSEDED if stale else VERSION_ACTIVE
        if current is None:
            session.add(
                LakeDatasetVersion(
                    version_id=uuid4(),
                    dataset_name=dataset_name,
                    target_date=target_date,
                    run_id=run_id,
                    status=status,
                    output_path=output["path"],
                    manifest_path=manifest.get("manifest_path") or "",
                    row_count=int(output["row_count"]),
                    input_snapshot_id=manifest["input_snapshot_id"],
                    rule_version=manifest["rule_version"],
                    config_version=manifest["config_version"],
                    published_at=datetime.now(timezone.utc),
                )
            )
        else:
            current.status = status
            current.output_path = output["path"]
            current.row_count = int(output["row_count"])
        session.flush()
        return not stale

    def _record_dependencies(
        self, session: Session, run_id: UUID, depends_on: Iterable
    ) -> None:
        for upstream in depends_on:
            existing = session.scalars(
                select(LakeDatasetDependency).where(
                    LakeDatasetDependency.consumer_run_id == run_id,
                    LakeDatasetDependency.upstream_dataset_name
                    == upstream.dataset_name,
                    LakeDatasetDependency.upstream_target_date == upstream.target_date,
                )
            ).one_or_none()
            if existing is not None:
                existing.upstream_run_id = upstream.run_id
                existing.upstream_version_id = upstream.version_id
                continue
            session.add(
                LakeDatasetDependency(
                    dependency_id=uuid4(),
                    consumer_run_id=run_id,
                    upstream_dataset_name=upstream.dataset_name,
                    upstream_target_date=upstream.target_date,
                    upstream_run_id=upstream.run_id,
                    upstream_version_id=upstream.version_id,
                    recorded_at=datetime.now(timezone.utc),
                )
            )
        session.flush()

    def _attempt_of(self, session: Session, run_id: UUID) -> int:
        run = session.get(LakeBatchRun, run_id)
        return run.attempt if run is not None else 0

    def active_version(
        self, dataset_name: str, target_date: date
    ) -> LakeDatasetVersion | None:
        with self._session_factory() as session:
            return session.scalars(
                select(LakeDatasetVersion).where(
                    LakeDatasetVersion.dataset_name == dataset_name,
                    LakeDatasetVersion.target_date == target_date,
                    LakeDatasetVersion.status == VERSION_ACTIVE,
                )
            ).one_or_none()

    def completed_run(
        self,
        target_date: date,
        *,
        input_snapshot_id: str,
        rule_version: str,
        config_version: str,
        dataset_names: tuple[str, ...],
    ) -> UUID | None:
        """같은 입력·규칙·설정으로 이미 확정된 실행. 있으면 다시 계산하지 않는다."""

        if not dataset_names:
            return None
        run_ids = set()
        with self._session_factory() as session:
            for dataset_name in dataset_names:
                version = session.scalars(
                    select(LakeDatasetVersion).where(
                        LakeDatasetVersion.dataset_name == dataset_name,
                        LakeDatasetVersion.target_date == target_date,
                        LakeDatasetVersion.status == VERSION_ACTIVE,
                        LakeDatasetVersion.input_snapshot_id == input_snapshot_id,
                        LakeDatasetVersion.rule_version == rule_version,
                        LakeDatasetVersion.config_version == config_version,
                    )
                ).one_or_none()
                if version is None:
                    return None
                run_ids.add(version.run_id)
        # 데이터셋마다 다른 실행이 활성이면 일관된 하루가 아니므로 다시 계산한다.
        return run_ids.pop() if len(run_ids) == 1 else None

    def latest_bytes_per_row(self, dataset_name: str) -> float | None:
        """가장 최근 확정 실행이 측정한 압축 후 행당 바이트."""

        with self._session_factory() as session:
            run = session.scalars(
                select(LakeBatchRun)
                .join(LakeDatasetVersion, LakeDatasetVersion.run_id == LakeBatchRun.run_id)
                .where(
                    LakeDatasetVersion.dataset_name == dataset_name,
                    LakeBatchRun.job_name == self._job_name,
                    LakeBatchRun.status == RUN_SUCCEEDED,
                )
                .order_by(LakeBatchRun.finished_at.desc())
                .limit(1)
            ).one_or_none()
        if run is None:
            return None
        measured = (run.details or {}).get("bytes_per_row", {}).get(dataset_name)
        return float(measured) if measured else None

    def recover(self, storage: LakeStorage, manifest_base: str, target_date: date) -> int:
        """파일은 확정됐는데 DB에 반영되지 않은 실행을 다시 반영한다."""

        published = 0
        with self.date_lock(target_date):
            for manifest in read_manifests(storage, manifest_base, target_date):
                run_id = UUID(str(manifest["run_id"]))
                with self._session_factory() as session:
                    run = session.get(LakeBatchRun, run_id)
                    already = run is not None and run.status == RUN_SUCCEEDED
                if already:
                    continue
                logger.info("Recovering unpublished silver run %s", run_id)
                self.publish(manifest)
                published += 1
        return published
