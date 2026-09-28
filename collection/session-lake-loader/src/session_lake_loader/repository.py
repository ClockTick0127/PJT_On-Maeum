"""analysis_db access for the session lake loader.

Every method opens its own short transaction. Nothing here is held open while
files are uploaded to HDFS; the loader calls these methods before and after
storage work, never around it.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import case, func, or_, select, text, update
from sqlalchemy.orm import Session, sessionmaker

from realtime_analysis.models import (
    ApplianceUsageSession,
    HouseholdActivityDaily,
    HouseholdObservationDaily,
    SessionLakeBatch,
    SessionLakeOutbox,
)

from session_lake_loader.lake_schema import SCHEMA_VERSION, normalize_timestamp


# 마이그레이션 20260920_10이 만드는 트리거 이름. 초기 적재 전에 존재를 확인한다.
TRIGGER_NAME = "trg_appliance_usage_session_lake_outbox"
CYCLE_LOCK_NAME = "batch:session-lake-loader"

BATCH_KIND_INITIAL = "INITIAL"
BATCH_KIND_INCREMENTAL = "INCREMENTAL"
BATCH_ASSIGNED = "ASSIGNED"
BATCH_FAILED = "FAILED"
BATCH_COMPLETED = "COMPLETED"
DELIVERY_PENDING = "PENDING"
DELIVERY_ASSIGNED = "ASSIGNED"
DELIVERY_DELIVERED = "DELIVERED"

Clock = Callable[[], datetime]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class LoaderAlreadyRunning(RuntimeError):
    """Another loader process holds the cycle lock."""


@dataclass(frozen=True)
class BatchRecord:
    batch_id: UUID
    batch_kind: str
    status: str
    ingest_date: date
    schema_version: int
    event_count: int
    first_event_id: int | None
    last_event_id: int | None
    row_count: int | None
    file_count: int | None
    manifest_path: str
    attempt_count: int
    last_error: str | None
    last_error_at: datetime | None
    details: dict[str, Any]
    created_at: datetime | None
    completed_at: datetime | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_id": str(self.batch_id),
            "batch_kind": self.batch_kind,
            "status": self.status,
            "ingest_date": self.ingest_date.isoformat(),
            "schema_version": self.schema_version,
            "event_count": self.event_count,
            "first_event_id": self.first_event_id,
            "last_event_id": self.last_event_id,
            "row_count": self.row_count,
            "file_count": self.file_count,
            "manifest_path": self.manifest_path,
            "attempt_count": self.attempt_count,
            "last_error": self.last_error,
            "last_error_at": (
                self.last_error_at.isoformat() if self.last_error_at else None
            ),
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "completed_at": (
                self.completed_at.isoformat() if self.completed_at else None
            ),
        }


@dataclass(frozen=True)
class OutboxEvent:
    event_id: int
    session_id: UUID
    session_version: int
    operation: str
    payload: dict[str, Any]
    changed_at: datetime


@dataclass(frozen=True)
class SessionSnapshotRecord:
    id: UUID
    activity_daily_id: UUID
    household_id: str
    appliance_type: str
    observation_date: date
    started_at: datetime
    ended_at: datetime | None
    max_probability: Any
    decision_threshold: Any
    updated_at: datetime
    lake_version: int


@dataclass(frozen=True)
class UnloadedVersions:
    """Outbox versions of one session that no loaded manifest covers yet."""

    min_version: int
    max_version: int
    has_delete: bool


@dataclass
class VerificationSnapshot:
    taken_at: datetime
    snapshot_id: str | None
    sessions: dict[str, SessionSnapshotRecord]
    unloaded: dict[str, UnloadedVersions]


@dataclass
class LoaderStatus:
    checked_at: datetime
    pending_events: int
    assigned_events: int
    oldest_undelivered_changed_at: datetime | None
    batches: dict[str, dict[str, int]] = field(default_factory=dict)
    failed_batches: list[BatchRecord] = field(default_factory=list)
    incomplete_initial_batches: list[BatchRecord] = field(default_factory=list)
    last_completed_at: datetime | None = None

    @property
    def undelivered_events(self) -> int:
        return self.pending_events + self.assigned_events

    @property
    def oldest_undelivered_age_seconds(self) -> float | None:
        if self.oldest_undelivered_changed_at is None:
            return None
        return max(
            0.0,
            (self.checked_at - self.oldest_undelivered_changed_at).total_seconds(),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "checked_at": self.checked_at.isoformat(),
            "undelivered_events": self.undelivered_events,
            "pending_events": self.pending_events,
            "assigned_events": self.assigned_events,
            "oldest_undelivered_changed_at": (
                self.oldest_undelivered_changed_at.isoformat()
                if self.oldest_undelivered_changed_at
                else None
            ),
            "oldest_undelivered_age_seconds": self.oldest_undelivered_age_seconds,
            "batches": self.batches,
            "failed_batches": [batch.to_dict() for batch in self.failed_batches],
            "incomplete_initial_batches": [
                batch.to_dict() for batch in self.incomplete_initial_batches
            ],
            "last_completed_at": (
                self.last_completed_at.isoformat() if self.last_completed_at else None
            ),
        }


class SessionLakeRepository:
    def __init__(
        self,
        session_factory: sessionmaker[Session],
        *,
        bronze_base: str,
        manifest_base: str,
        schema_version: int = SCHEMA_VERSION,
        clock: Clock = utc_now,
    ) -> None:
        self._session_factory = session_factory
        self._bronze_base = bronze_base.rstrip("/")
        self._manifest_base = manifest_base.rstrip("/")
        self._schema_version = schema_version
        self._clock = clock

    # ---------- 경로 ----------

    def batch_directory(self, batch: BatchRecord) -> str:
        return (
            f"{self._bronze_base}/ingest_date={batch.ingest_date.isoformat()}"
            f"/batch_id={batch.batch_id}"
        )

    def manifest_path(self, ingest_date: date, batch_id: UUID) -> str:
        return (
            f"{self._manifest_base}/date={ingest_date.isoformat()}"
            f"/manifest-{batch_id}.json"
        )

    @property
    def manifest_base(self) -> str:
        return self._manifest_base

    # ---------- 실행 잠금·전제 확인 ----------

    def _is_postgresql(self, session: Session) -> bool:
        return session.get_bind().dialect.name == "postgresql"

    @contextmanager
    def cycle_lock(self) -> Iterator[None]:
        """Serialize loader cycles across processes with a PostgreSQL advisory lock."""

        session = self._session_factory()
        try:
            if not self._is_postgresql(session):
                yield
                return
            acquired = bool(
                session.execute(
                    text("SELECT pg_try_advisory_lock(hashtext(:name))"),
                    {"name": CYCLE_LOCK_NAME},
                ).scalar_one()
            )
            if not acquired:
                raise LoaderAlreadyRunning(CYCLE_LOCK_NAME)
            try:
                yield
            finally:
                session.execute(
                    text("SELECT pg_advisory_unlock(hashtext(:name))"),
                    {"name": CYCLE_LOCK_NAME},
                )
        finally:
            session.close()

    def lake_capture_enabled(self) -> bool | None:
        """True/False on PostgreSQL; None when the backend has no triggers (tests)."""

        with self._session_factory() as session:
            if not self._is_postgresql(session):
                return None
            return bool(
                session.execute(
                    text(
                        "SELECT EXISTS ("
                        "SELECT 1 FROM pg_trigger "
                        "WHERE tgname = :name AND NOT tgisinternal)"
                    ),
                    {"name": TRIGGER_NAME},
                ).scalar_one()
            )

    # ---------- 배치 배정 ----------

    def assign_incremental_batch(
        self,
        max_events: int,
        ingest_date: date,
    ) -> BatchRecord | None:
        """Move up to ``max_events`` PENDING events into a new ASSIGNED batch."""

        with self._session_factory.begin() as session:
            event_ids = session.scalars(
                select(SessionLakeOutbox.event_id)
                .where(SessionLakeOutbox.delivery_status == DELIVERY_PENDING)
                .order_by(SessionLakeOutbox.event_id)
                .limit(max_events)
                .with_for_update(skip_locked=True)
            ).all()
            if not event_ids:
                return None
            batch_id = uuid4()
            now = self._clock()
            batch = SessionLakeBatch(
                batch_id=batch_id,
                batch_kind=BATCH_KIND_INCREMENTAL,
                status=BATCH_ASSIGNED,
                ingest_date=ingest_date,
                schema_version=self._schema_version,
                event_count=len(event_ids),
                first_event_id=min(event_ids),
                last_event_id=max(event_ids),
                manifest_path=self.manifest_path(ingest_date, batch_id),
                attempt_count=0,
                details={},
                created_at=now,
                updated_at=now,
            )
            session.add(batch)
            session.flush()
            session.execute(
                update(SessionLakeOutbox)
                .where(SessionLakeOutbox.event_id.in_(event_ids))
                .values(delivery_status=DELIVERY_ASSIGNED, batch_id=batch_id)
            )
            return _record(batch)

    def create_initial_batch(self, ingest_date: date) -> BatchRecord:
        with self._session_factory.begin() as session:
            batch_id = uuid4()
            now = self._clock()
            batch = SessionLakeBatch(
                batch_id=batch_id,
                batch_kind=BATCH_KIND_INITIAL,
                status=BATCH_ASSIGNED,
                ingest_date=ingest_date,
                schema_version=self._schema_version,
                event_count=0,
                manifest_path=self.manifest_path(ingest_date, batch_id),
                attempt_count=0,
                details={},
                created_at=now,
                updated_at=now,
            )
            session.add(batch)
            session.flush()
            return _record(batch)

    def find_incomplete_initial_batch(self) -> BatchRecord | None:
        with self._session_factory() as session:
            batch = session.scalar(
                select(SessionLakeBatch)
                .where(
                    SessionLakeBatch.batch_kind == BATCH_KIND_INITIAL,
                    SessionLakeBatch.status != BATCH_COMPLETED,
                )
                .order_by(SessionLakeBatch.created_at)
                .limit(1)
            )
            return _record(batch) if batch is not None else None

    def completed_initial_batch_exists(self) -> bool:
        with self._session_factory() as session:
            return bool(
                session.scalar(
                    select(func.count())
                    .select_from(SessionLakeBatch)
                    .where(
                        SessionLakeBatch.batch_kind == BATCH_KIND_INITIAL,
                        SessionLakeBatch.status == BATCH_COMPLETED,
                    )
                )
            )

    def incomplete_batches(self) -> list[BatchRecord]:
        with self._session_factory() as session:
            batches = session.scalars(
                select(SessionLakeBatch)
                .where(SessionLakeBatch.status != BATCH_COMPLETED)
                .order_by(SessionLakeBatch.created_at, SessionLakeBatch.batch_id)
            ).all()
            return [_record(batch) for batch in batches]

    def get_batch(self, batch_id: UUID) -> BatchRecord:
        with self._session_factory() as session:
            batch = session.get(SessionLakeBatch, batch_id)
            if batch is None:
                raise LookupError(f"session_lake_batch not found: {batch_id}")
            return _record(batch)

    def batch_events(self, batch_id: UUID) -> list[OutboxEvent]:
        with self._session_factory() as session:
            rows = session.scalars(
                select(SessionLakeOutbox)
                .where(SessionLakeOutbox.batch_id == batch_id)
                .order_by(SessionLakeOutbox.event_id)
            ).all()
            return [
                OutboxEvent(
                    event_id=row.event_id,
                    session_id=row.session_id,
                    session_version=row.session_version,
                    operation=row.operation,
                    payload=dict(row.payload),
                    changed_at=normalize_timestamp(row.changed_at),
                )
                for row in rows
            ]

    # ---------- 초기 적재 스냅샷 ----------

    @contextmanager
    def snapshot_sessions(
        self,
    ) -> Iterator[tuple[dict[str, Any], Iterator[SessionSnapshotRecord]]]:
        """Yield every session (open ones included) from one consistent snapshot.

        On PostgreSQL the read runs under REPEATABLE READ so the whole table is
        seen as of one point in time. The transaction ends when the context
        exits; callers must spool rows locally and upload afterwards.
        """

        with self._session_factory() as session:
            snapshot_id = self._open_repeatable_read(session)
            info = {
                "snapshot_id": snapshot_id,
                "taken_at": self._clock().isoformat(),
            }
            statement = (
                _session_join_statement()
                .order_by(ApplianceUsageSession.id)
                .execution_options(yield_per=5000)
            )

            def records() -> Iterator[SessionSnapshotRecord]:
                for row in session.execute(statement):
                    yield _snapshot_record(row)

            try:
                yield info, records()
            finally:
                session.rollback()

    def _open_repeatable_read(self, session: Session) -> str | None:
        if not self._is_postgresql(session):
            return None
        session.connection(execution_options={"isolation_level": "REPEATABLE READ"})
        return session.execute(text("SELECT pg_current_snapshot()::text")).scalar_one()

    # ---------- 배치 완료·실패 ----------

    def mark_batch_completed(
        self,
        batch_id: UUID,
        *,
        row_count: int,
        file_count: int,
        details: dict[str, Any],
    ) -> BatchRecord:
        """Record delivery. For incremental batches every event flips to DELIVERED."""

        with self._session_factory.begin() as session:
            batch = session.get(SessionLakeBatch, batch_id, with_for_update=True)
            if batch is None:
                raise LookupError(f"session_lake_batch not found: {batch_id}")
            now = self._clock()
            if batch.batch_kind == BATCH_KIND_INCREMENTAL:
                updated = session.execute(
                    update(SessionLakeOutbox)
                    .where(
                        SessionLakeOutbox.batch_id == batch_id,
                        SessionLakeOutbox.delivery_status == DELIVERY_ASSIGNED,
                    )
                    .values(delivery_status=DELIVERY_DELIVERED, delivered_at=now)
                ).rowcount
                delivered = session.scalar(
                    select(func.count())
                    .select_from(SessionLakeOutbox)
                    .where(
                        SessionLakeOutbox.batch_id == batch_id,
                        SessionLakeOutbox.delivery_status == DELIVERY_DELIVERED,
                    )
                )
                if delivered != batch.event_count:
                    raise RuntimeError(
                        "배치 대상 이벤트 수가 outbox와 다릅니다: "
                        f"batch={batch_id} expected={batch.event_count} "
                        f"delivered={delivered} updated_now={updated}"
                    )
            batch.status = BATCH_COMPLETED
            batch.row_count = row_count
            batch.file_count = file_count
            batch.details = {**(batch.details or {}), **details}
            batch.completed_at = now
            batch.updated_at = now
            session.flush()
            return _record(batch)

    def mark_batch_failed(
        self,
        batch_id: UUID,
        *,
        stage: str,
        error: str,
    ) -> BatchRecord:
        with self._session_factory.begin() as session:
            batch = session.get(SessionLakeBatch, batch_id, with_for_update=True)
            if batch is None:
                raise LookupError(f"session_lake_batch not found: {batch_id}")
            now = self._clock()
            batch.status = BATCH_FAILED
            batch.attempt_count = (batch.attempt_count or 0) + 1
            batch.last_error = f"{stage}: {error}"[:4000]
            batch.last_error_at = now
            batch.updated_at = now
            history = list((batch.details or {}).get("errors", []))[-9:]
            history.append(
                {
                    "attempt": batch.attempt_count,
                    "stage": stage,
                    "error": error[:1000],
                    "at": now.isoformat(),
                }
            )
            batch.details = {**(batch.details or {}), "errors": history}
            session.flush()
            return _record(batch)

    # ---------- 운영 상태 ----------

    def status_summary(self, now: datetime | None = None) -> LoaderStatus:
        checked_at = now or self._clock()
        with self._session_factory() as session:
            counts = dict(
                session.execute(
                    select(
                        SessionLakeOutbox.delivery_status,
                        func.count(),
                    )
                    .where(SessionLakeOutbox.delivery_status != DELIVERY_DELIVERED)
                    .group_by(SessionLakeOutbox.delivery_status)
                ).all()
            )
            oldest = session.scalar(
                select(func.min(SessionLakeOutbox.changed_at)).where(
                    SessionLakeOutbox.delivery_status != DELIVERY_DELIVERED
                )
            )
            batches: dict[str, dict[str, int]] = {}
            for kind, status, count in session.execute(
                select(
                    SessionLakeBatch.batch_kind,
                    SessionLakeBatch.status,
                    func.count(),
                ).group_by(SessionLakeBatch.batch_kind, SessionLakeBatch.status)
            ).all():
                batches.setdefault(kind, {})[status] = int(count)
            failed = session.scalars(
                select(SessionLakeBatch)
                .where(SessionLakeBatch.status == BATCH_FAILED)
                .order_by(SessionLakeBatch.last_error_at.desc())
                .limit(20)
            ).all()
            incomplete_initial = session.scalars(
                select(SessionLakeBatch)
                .where(
                    SessionLakeBatch.batch_kind == BATCH_KIND_INITIAL,
                    SessionLakeBatch.status != BATCH_COMPLETED,
                )
                .order_by(SessionLakeBatch.created_at)
            ).all()
            last_completed = session.scalar(select(func.max(SessionLakeBatch.completed_at)))
        return LoaderStatus(
            checked_at=checked_at,
            pending_events=int(counts.get(DELIVERY_PENDING, 0)),
            assigned_events=int(counts.get(DELIVERY_ASSIGNED, 0)),
            oldest_undelivered_changed_at=normalize_timestamp(oldest),
            batches=batches,
            failed_batches=[_record(batch) for batch in failed],
            incomplete_initial_batches=[_record(batch) for batch in incomplete_initial],
            last_completed_at=normalize_timestamp(last_completed),
        )

    # ---------- 검증 기준 ----------

    def verification_snapshot(self, loaded_batch_ids: set[UUID]) -> VerificationSnapshot:
        """Current DB state plus the outbox versions no loaded manifest covers.

        Read in one REPEATABLE READ transaction so the comparison baseline is a
        single point in time. ``loaded_batch_ids`` is the set of manifests the
        verifier actually read, so events delivered by later batches count as
        unloaded and are reported as pending rather than as errors.
        """

        with self._session_factory() as session:
            snapshot_id = self._open_repeatable_read(session)
            taken_at = self._clock()
            sessions = {
                str(record.id): record
                for record in (
                    _snapshot_record(row)
                    for row in session.execute(_session_join_statement())
                )
            }
            loaded = [UUID(str(value)) for value in loaded_batch_ids]
            unloaded_filter = or_(
                SessionLakeOutbox.batch_id.is_(None),
                SessionLakeOutbox.batch_id.not_in(loaded),
            )
            is_delete = case(
                (SessionLakeOutbox.operation == "DELETE", 1),
                else_=0,
            )
            unloaded: dict[str, UnloadedVersions] = {}
            for session_id, low, high, deletes in session.execute(
                select(
                    SessionLakeOutbox.session_id,
                    func.min(SessionLakeOutbox.session_version),
                    func.max(SessionLakeOutbox.session_version),
                    func.max(is_delete),
                )
                .where(unloaded_filter)
                .group_by(SessionLakeOutbox.session_id)
            ).all():
                unloaded[str(session_id)] = UnloadedVersions(
                    min_version=int(low),
                    max_version=int(high),
                    has_delete=bool(deletes),
                )
            session.rollback()
        return VerificationSnapshot(
            taken_at=taken_at,
            snapshot_id=snapshot_id,
            sessions=sessions,
            unloaded=unloaded,
        )


def _session_join_statement():
    return (
        select(
            ApplianceUsageSession.id,
            ApplianceUsageSession.activity_daily_id,
            HouseholdObservationDaily.household_id,
            HouseholdActivityDaily.appliance_type,
            HouseholdObservationDaily.observation_date,
            ApplianceUsageSession.started_at,
            ApplianceUsageSession.ended_at,
            ApplianceUsageSession.max_probability,
            ApplianceUsageSession.decision_threshold,
            ApplianceUsageSession.updated_at,
            ApplianceUsageSession.lake_version,
        )
        .join(
            HouseholdActivityDaily,
            HouseholdActivityDaily.id == ApplianceUsageSession.activity_daily_id,
        )
        .join(
            HouseholdObservationDaily,
            HouseholdObservationDaily.id == HouseholdActivityDaily.observation_daily_id,
        )
    )


def _snapshot_record(row: Any) -> SessionSnapshotRecord:
    return SessionSnapshotRecord(
        id=row.id,
        activity_daily_id=row.activity_daily_id,
        household_id=row.household_id,
        appliance_type=row.appliance_type,
        observation_date=row.observation_date,
        started_at=row.started_at,
        ended_at=row.ended_at,
        max_probability=row.max_probability,
        decision_threshold=row.decision_threshold,
        updated_at=row.updated_at,
        lake_version=row.lake_version,
    )


def _record(batch: SessionLakeBatch) -> BatchRecord:
    return BatchRecord(
        batch_id=batch.batch_id,
        batch_kind=batch.batch_kind,
        status=batch.status,
        ingest_date=batch.ingest_date,
        schema_version=batch.schema_version,
        event_count=batch.event_count or 0,
        first_event_id=batch.first_event_id,
        last_event_id=batch.last_event_id,
        row_count=batch.row_count,
        file_count=batch.file_count,
        manifest_path=batch.manifest_path,
        attempt_count=batch.attempt_count or 0,
        last_error=batch.last_error,
        last_error_at=normalize_timestamp(batch.last_error_at),
        details=dict(batch.details or {}),
        created_at=normalize_timestamp(batch.created_at),
        completed_at=normalize_timestamp(batch.completed_at),
    )
