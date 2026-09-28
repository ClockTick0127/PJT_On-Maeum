"""End-to-end on a disposable PostgreSQL: real migration, triggers, loader, verifier.

Run with::

    TEST_DATABASE_URL=postgresql+psycopg://test:test@127.0.0.1:55432/analysis_test \
        pytest -m postgres collection/session-lake-loader/tests
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
from uuid import UUID

import pytest
from sqlalchemy import create_engine, delete, select, text
from sqlalchemy.orm import Session, sessionmaker

from realtime_analysis.activity_repository import SqlAlchemyApplianceActivityRepository
from realtime_analysis.models import (
    ApplianceUsageSession,
    HouseholdActivityDaily,
    HouseholdObservationDaily,
    SessionLakeBatch,
    SessionLakeOutbox,
)
from realtime_analysis.schemas import (
    ApplianceState,
    ApplianceStateTransition,
    ApplianceTransitionType,
)

from session_lake_loader.loader import SessionLakeLoader
from session_lake_loader.repository import SessionLakeRepository
from session_lake_loader.storage import LocalLakeStorage
from session_lake_loader.verifier import SessionLakeVerifier

from conftest import BRONZE_BASE, MANIFEST_BASE, postgres_url, requires_postgres, run_analysis_migrations


pytestmark = [pytest.mark.postgres, requires_postgres]

START = datetime(2026, 9, 20, 9, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def pg_session_factory() -> sessionmaker[Session]:
    url = postgres_url()
    engine = create_engine(url, pool_pre_ping=True)
    with engine.begin() as connection:
        connection.execute(text("DROP SCHEMA public CASCADE"))
        connection.execute(text("CREATE SCHEMA public"))
    run_analysis_migrations(url)
    yield sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)
    engine.dispose()


@pytest.fixture
def clean_tables(pg_session_factory: sessionmaker[Session]) -> None:
    with pg_session_factory.begin() as db:
        db.execute(
            text(
                "TRUNCATE session_lake_outbox, session_lake_batch, appliance_usage_session, "
                "household_activity_daily, household_observation_daily CASCADE"
            )
        )


@pytest.fixture
def pg_repository(pg_session_factory: sessionmaker[Session], clean_tables: None) -> SessionLakeRepository:
    return SessionLakeRepository(
        pg_session_factory, bronze_base=BRONZE_BASE, manifest_base=MANIFEST_BASE
    )


@pytest.fixture
def pg_storage(tmp_path: Path) -> LocalLakeStorage:
    return LocalLakeStorage(tmp_path / "lake")


def activity_repository(session_factory: sessionmaker[Session]) -> SqlAlchemyApplianceActivityRepository:
    return SqlAlchemyApplianceActivityRepository(session_factory, timezone_name="Asia/Seoul")


def turn(transition_type: ApplianceTransitionType, appliance: str, at: datetime, probability: float):
    on = transition_type == ApplianceTransitionType.TURNED_ON
    return ApplianceStateTransition(
        household_id="H001",
        appliance_type=appliance,
        transition_type=transition_type,
        previous_is_on=not on,
        current_is_on=on,
        occurred_at=at,
        confirmed_at=at + timedelta(seconds=3),
        probability=probability,
        threshold=0.5,
    )


def record_on(session_factory, appliance: str, at: datetime, probability: float = 0.8) -> None:
    activity_repository(session_factory).record(
        "H001",
        at,
        [ApplianceState(appliance_type=appliance, probability=probability, threshold=0.5, is_on=True)],
        [turn(ApplianceTransitionType.TURNED_ON, appliance, at, probability)],
        {appliance},
    )


def record_off(session_factory, appliance: str, at: datetime) -> None:
    activity_repository(session_factory).record(
        "H001",
        at,
        [ApplianceState(appliance_type=appliance, probability=0.1, threshold=0.5, is_on=False)],
        [turn(ApplianceTransitionType.TURNED_OFF, appliance, at, 0.1)],
        set(),
    )


def outbox_rows(session_factory) -> list[SessionLakeOutbox]:
    with session_factory() as db:
        return db.scalars(select(SessionLakeOutbox).order_by(SessionLakeOutbox.event_id)).all()


def test_full_flow_initial_then_incremental_restores_latest_state(
    pg_session_factory: sessionmaker[Session],
    pg_repository: SessionLakeRepository,
    pg_storage: LocalLakeStorage,
) -> None:
    # 트리거 도입 전부터 있던 세션을 흉내 낸다: outbox 없이 직접 INSERT 후 이벤트 삭제.
    record_on(pg_session_factory, "MICROWAVE", START)
    record_off(pg_session_factory, "MICROWAVE", START + timedelta(minutes=2))
    with pg_session_factory.begin() as db:
        db.execute(delete(SessionLakeOutbox))
    record_on(pg_session_factory, "KETTLE", START + timedelta(minutes=5))  # 열린 세션, outbox v1

    loader = SessionLakeLoader(pg_repository, pg_storage, rows_per_file=1)
    created_during: list[UUID] = []

    def change_while_loading() -> None:
        record_on(pg_session_factory, "IRON", START + timedelta(minutes=6))
        record_off(pg_session_factory, "KETTLE", START + timedelta(minutes=7))
        with pg_session_factory() as db:
            created_during.extend(
                db.scalars(
                    select(ApplianceUsageSession.id)
                    .join(HouseholdActivityDaily)
                    .where(HouseholdActivityDaily.appliance_type == "IRON")
                ).all()
            )

    initial = loader.run_initial_load(after_snapshot_opened=change_while_loading)
    assert initial.status == "COMPLETED", initial.to_dict()
    # REPEATABLE READ 스냅샷: 적재 중 만든 IRON 세션은 초기 적재에 없다.
    assert initial.row_count == 2
    manifest = json.loads(pg_storage.read_bytes(pg_repository.get_batch(UUID(initial.batch_id)).manifest_path))
    assert manifest["snapshot"]["snapshot_id"]
    assert [row.delivery_status for row in outbox_rows(pg_session_factory)] == ["PENDING"] * 3

    report = SessionLakeVerifier(pg_repository, pg_storage).run()
    assert report.ok, report.to_dict()
    assert len(report.comparison.pending) == 2  # IRON(NOT_LOADED_YET), KETTLE(LAKE_BEHIND_PENDING)

    results = loader.run_incremental_cycle(batch_max_events=2, max_batches=10)
    assert [r.status for r in results] == ["COMPLETED", "COMPLETED"]
    assert all(row.delivery_status == "DELIVERED" for row in outbox_rows(pg_session_factory))

    report = SessionLakeVerifier(pg_repository, pg_storage).run()
    assert report.ok, report.to_dict()
    assert report.comparison.pending == []
    assert report.comparison.matched_live == 3
    assert report.restore.duplicate_rows == 1  # KETTLE v1: SNAPSHOT + outbox INSERT

    # 삭제(직접)와 CASCADE 삭제 모두 tombstone으로 복원된다.
    with pg_session_factory.begin() as db:
        db.execute(delete(ApplianceUsageSession).where(ApplianceUsageSession.id.in_(created_during)))
    with pg_session_factory.begin() as db:
        db.execute(delete(HouseholdObservationDaily))  # CASCADE -> 나머지 세션 삭제
    deletes = [row for row in outbox_rows(pg_session_factory) if row.operation == "DELETE"]
    assert len(deletes) == 3
    assert all("household_id" not in row.payload for row in deletes)

    results = loader.run_incremental_cycle(batch_max_events=100, max_batches=10)
    assert [r.status for r in results] == ["COMPLETED"]
    report = SessionLakeVerifier(pg_repository, pg_storage).run()
    assert report.ok, report.to_dict()
    assert report.comparison.matched_deleted == 3
    assert report.comparison.matched_live == 0
    assert report.restore.live_count == 0

    # 장애 후 재실행: 완료된 배치는 다시 처리되지 않고 상태도 그대로다.
    assert loader.run_incremental_cycle(batch_max_events=100, max_batches=10) == []
    status = pg_repository.status_summary()
    assert status.undelivered_events == 0
    assert status.batches["INCREMENTAL"]["COMPLETED"] == 3
    assert status.batches["INITIAL"]["COMPLETED"] == 1


def test_rolled_back_session_change_leaves_no_outbox_event(
    pg_session_factory: sessionmaker[Session],
    pg_repository: SessionLakeRepository,
) -> None:
    record_on(pg_session_factory, "MICROWAVE", START)
    before = len(outbox_rows(pg_session_factory))

    with pg_session_factory() as db:
        session_id = db.scalar(select(ApplianceUsageSession.id))
        usage = db.get(ApplianceUsageSession, session_id)
        usage.ended_at = START + timedelta(minutes=1)
        db.flush()
        assert len(db.scalars(select(SessionLakeOutbox)).all()) == before + 1
        db.rollback()

    assert len(outbox_rows(pg_session_factory)) == before
    assert pg_repository.lake_capture_enabled() is True
    with pg_session_factory() as db:
        assert db.get(ApplianceUsageSession, session_id).lake_version == 1


def test_cycle_lock_prevents_concurrent_loaders(
    pg_session_factory: sessionmaker[Session],
    pg_repository: SessionLakeRepository,
) -> None:
    from session_lake_loader.repository import LoaderAlreadyRunning

    other = SessionLakeRepository(pg_session_factory, bronze_base=BRONZE_BASE, manifest_base=MANIFEST_BASE)
    with pg_repository.cycle_lock():
        with pytest.raises(LoaderAlreadyRunning):
            with other.cycle_lock():
                pass
    with other.cycle_lock():
        pass


def test_batch_rows_carry_versions_from_the_trigger(
    pg_session_factory: sessionmaker[Session],
    pg_repository: SessionLakeRepository,
    pg_storage: LocalLakeStorage,
) -> None:
    record_on(pg_session_factory, "MICROWAVE", START, probability=0.6)
    record_on(pg_session_factory, "MICROWAVE", START + timedelta(seconds=10), probability=0.9)  # max prob 갱신
    record_on(pg_session_factory, "MICROWAVE", START + timedelta(seconds=20), probability=0.7)  # 값 변화 없음
    record_off(pg_session_factory, "MICROWAVE", START + timedelta(minutes=1))

    versions = [(row.operation, row.session_version) for row in outbox_rows(pg_session_factory)]
    assert versions == [("INSERT", 1), ("UPDATE", 2), ("UPDATE", 3)]
    with pg_session_factory() as db:
        usage = db.scalar(select(ApplianceUsageSession))
        assert usage.lake_version == 3
        assert usage.max_probability == Decimal("0.9000")

    loader = SessionLakeLoader(pg_repository, pg_storage)
    results = loader.run_incremental_cycle(batch_max_events=100, max_batches=1)
    assert results[0].row_count == 3
    with pg_session_factory() as db:
        batch = db.get(SessionLakeBatch, UUID(results[0].batch_id))
        assert batch.first_event_id is not None and batch.last_event_id == batch.first_event_id + 2
        assert batch.details["files"][0]["rows"] == 3
        assert batch.ingest_date == date.today() or batch.ingest_date == datetime.now(timezone.utc).date()
