"""Migration 20260920_10 on a disposable PostgreSQL: trigger, versions, rollback, CASCADE.

Skipped unless ``TEST_DATABASE_URL`` points at a database that may be wiped::

    TEST_DATABASE_URL=postgresql+psycopg://test:test@127.0.0.1:55432/analysis_test \
        pytest -m postgres ai/realtime-analysis-service/tests
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect, select, text
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.orm import Session, sessionmaker

from realtime_analysis.models import (
    ApplianceUsageSession,
    HouseholdActivityDaily,
    HouseholdObservationDaily,
    SessionLakeOutbox,
)


DATABASE_URL = os.environ.get("TEST_DATABASE_URL")
pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(not DATABASE_URL, reason="TEST_DATABASE_URL not set"),
]

SERVICE_DIR = Path(__file__).resolve().parents[1]
TRIGGER_NAME = "trg_appliance_usage_session_lake_outbox"
START = datetime(2026, 9, 20, 9, 0, tzinfo=timezone.utc)


def alembic_config():
    from alembic.config import Config

    from realtime_analysis.config import get_settings

    parsed = make_url(DATABASE_URL)
    os.environ.update(
        DATABASE_HOST=parsed.host or "localhost",
        DATABASE_PORT=str(parsed.port or 5432),
        DATABASE_NAME=parsed.database or "",
        DATABASE_USER=parsed.username or "",
        DATABASE_PASSWORD=parsed.password or "",
    )
    get_settings.cache_clear()
    config = Config(str(SERVICE_DIR / "alembic.ini"))
    config.set_main_option("script_location", str(SERVICE_DIR / "alembic"))
    return config


@pytest.fixture(scope="module")
def engine() -> Engine:
    from alembic import command

    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    with engine.begin() as connection:
        connection.execute(text("DROP SCHEMA public CASCADE"))
        connection.execute(text("CREATE SCHEMA public"))
    command.upgrade(alembic_config(), "head")
    yield engine
    engine.dispose()


@pytest.fixture
def session_factory(engine: Engine) -> sessionmaker[Session]:
    with engine.begin() as connection:
        connection.execute(
            text(
                "TRUNCATE session_lake_outbox, session_lake_batch, appliance_usage_session, "
                "household_activity_daily, household_observation_daily CASCADE"
            )
        )
    return sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)


def trigger_exists(engine: Engine) -> bool:
    with engine.connect() as connection:
        return bool(
            connection.execute(
                text("SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = :name)"),
                {"name": TRIGGER_NAME},
            ).scalar_one()
        )


def create_session(db: Session, appliance: str = "MICROWAVE", started_at: datetime = START) -> ApplianceUsageSession:
    observation = db.scalar(
        select(HouseholdObservationDaily).where(
            HouseholdObservationDaily.household_id == "H001",
            HouseholdObservationDaily.observation_date == date(2026, 9, 20),
        )
    )
    if observation is None:
        observation = HouseholdObservationDaily(
            household_id="H001",
            observation_date=date(2026, 9, 20),
            sample_count=0,
            expected_sample_count=86_400,
            coverage_ratio=Decimal("0"),
            observation_status="COLLECTING",
            updated_at=started_at,
        )
        db.add(observation)
        db.flush()
    activity = HouseholdActivityDaily(
        observation_daily_id=observation.id,
        appliance_type=appliance,
        event_count=1,
        updated_at=started_at,
    )
    db.add(activity)
    db.flush()
    usage = ApplianceUsageSession(
        activity_daily_id=activity.id,
        started_at=started_at,
        ended_at=None,
        max_probability=Decimal("0.8000"),
        decision_threshold=Decimal("0.5000"),
        updated_at=started_at,
    )
    db.add(usage)
    db.flush()
    return usage


def outbox(db: Session) -> list[SessionLakeOutbox]:
    return db.scalars(select(SessionLakeOutbox).order_by(SessionLakeOutbox.event_id)).all()


def test_migration_creates_lake_tables_column_and_trigger(engine: Engine) -> None:
    inspector = inspect(engine)
    assert {"session_lake_outbox", "session_lake_batch"} <= set(inspector.get_table_names())
    columns = {column["name"]: column for column in inspector.get_columns("appliance_usage_session")}
    assert columns["lake_version"]["nullable"] is False
    assert "1" in str(columns["lake_version"]["default"])
    outbox_fks = inspector.get_foreign_keys("session_lake_outbox")
    assert [fk["referred_table"] for fk in outbox_fks] == ["session_lake_batch"]
    assert trigger_exists(engine)
    with engine.connect() as connection:
        identity = connection.execute(
            text(
                "SELECT is_identity FROM information_schema.columns "
                "WHERE table_name = 'session_lake_outbox' AND column_name = 'event_id'"
            )
        ).scalar_one()
    assert identity == "YES"


def test_insert_records_version_one_with_full_content_and_parents(
    session_factory: sessionmaker[Session],
) -> None:
    with session_factory.begin() as db:
        usage = create_session(db)
        session_id, activity_id = usage.id, usage.activity_daily_id

    with session_factory() as db:
        events = outbox(db)
        assert [(e.operation, e.session_version) for e in events] == [("INSERT", 1)]
        payload = events[0].payload
        assert payload["id"] == str(session_id)
        assert payload["activity_daily_id"] == str(activity_id)
        assert payload["household_id"] == "H001"
        assert payload["appliance_type"] == "MICROWAVE"
        assert payload["observation_date"] == "2026-09-20"
        assert Decimal(str(payload["max_probability"])) == Decimal("0.8")
        assert payload["ended_at"] is None
        assert "lake_version" not in payload
        assert events[0].delivery_status == "PENDING"
        assert events[0].batch_id is None
        assert db.get(ApplianceUsageSession, session_id).lake_version == 1


def test_content_change_bumps_version_but_identical_update_does_not(
    session_factory: sessionmaker[Session],
) -> None:
    with session_factory.begin() as db:
        session_id = create_session(db).id

    with session_factory.begin() as db:
        usage = db.get(ApplianceUsageSession, session_id)
        usage.ended_at = START + timedelta(minutes=2)
        usage.updated_at = START + timedelta(minutes=2)

    with session_factory.begin() as db:
        # 같은 값으로 다시 저장: 이벤트도 버전 증가도 없어야 한다.
        db.execute(
            text(
                "UPDATE appliance_usage_session SET max_probability = 0.8000, "
                "lake_version = 999 WHERE id = :id"
            ),
            {"id": session_id},
        )

    with session_factory() as db:
        events = outbox(db)
        assert [(e.operation, e.session_version) for e in events] == [("INSERT", 1), ("UPDATE", 2)]
        assert events[1].payload["ended_at"].startswith("2026-09-20T")
        assert db.get(ApplianceUsageSession, session_id).lake_version == 2

    with session_factory.begin() as db:
        usage = db.get(ApplianceUsageSession, session_id)
        usage.max_probability = Decimal("0.9500")
        usage.lake_version = 1  # 애플리케이션이 잘못 써도 트리거가 덮어쓴다

    with session_factory() as db:
        assert [e.session_version for e in outbox(db)] == [1, 2, 3]
        assert db.get(ApplianceUsageSession, session_id).lake_version == 3
        assert Decimal(str(outbox(db)[-1].payload["max_probability"])) == Decimal("0.95")


def test_direct_delete_creates_tombstone_from_old_row(
    session_factory: sessionmaker[Session],
) -> None:
    with session_factory.begin() as db:
        session_id = create_session(db).id
    with session_factory.begin() as db:
        db.get(ApplianceUsageSession, session_id).ended_at = START + timedelta(minutes=1)
    with session_factory.begin() as db:
        db.delete(db.get(ApplianceUsageSession, session_id))

    with session_factory() as db:
        events = outbox(db)
        assert [(e.operation, e.session_version) for e in events] == [
            ("INSERT", 1),
            ("UPDATE", 2),
            ("DELETE", 3),
        ]
        tombstone = events[-1]
        assert tombstone.session_id == session_id
        assert tombstone.payload["id"] == str(session_id)
        assert "household_id" not in tombstone.payload
        assert db.get(ApplianceUsageSession, session_id) is None


def test_cascade_delete_of_parent_creates_delete_events_without_join(
    session_factory: sessionmaker[Session],
) -> None:
    with session_factory.begin() as db:
        first = create_session(db, "MICROWAVE").id
        second = create_session(db, "KETTLE", START + timedelta(minutes=5)).id

    with session_factory.begin() as db:
        db.execute(text("DELETE FROM household_observation_daily"))

    with session_factory() as db:
        deletes = [e for e in outbox(db) if e.operation == "DELETE"]
        assert {e.session_id for e in deletes} == {first, second}
        assert all(e.session_version == 2 for e in deletes)
        assert all("appliance_type" not in e.payload for e in deletes)
        assert db.scalar(select(ApplianceUsageSession)) is None
        # 원본 세션 FK가 없으므로 outbox 행은 남아 있다.
        assert len(outbox(db)) == 4


def test_rollback_discards_session_and_outbox_together(
    session_factory: sessionmaker[Session],
) -> None:
    with session_factory() as db:
        create_session(db)
        assert len(outbox(db)) == 1
        db.rollback()

    with session_factory() as db:
        assert outbox(db) == []
        assert db.scalar(select(ApplianceUsageSession)) is None


def test_same_session_version_cannot_be_recorded_twice(
    session_factory: sessionmaker[Session],
) -> None:
    from sqlalchemy.exc import IntegrityError

    with session_factory.begin() as db:
        session_id = create_session(db).id

    with pytest.raises(IntegrityError):
        with session_factory.begin() as db:
            db.add(
                SessionLakeOutbox(
                    session_id=session_id,
                    session_version=1,
                    operation="UPDATE",
                    payload={},
                )
            )


def test_zz_downgrade_removes_capture_and_upgrade_restores_it(engine: Engine) -> None:
    from alembic import command

    command.downgrade(alembic_config(), "20260919_09")
    inspector = inspect(engine)
    assert "session_lake_outbox" not in inspector.get_table_names()
    assert "session_lake_batch" not in inspector.get_table_names()
    assert "lake_version" not in {c["name"] for c in inspector.get_columns("appliance_usage_session")}
    assert not trigger_exists(engine)

    command.upgrade(alembic_config(), "head")
    inspector = inspect(engine)
    assert "session_lake_outbox" in inspector.get_table_names()
    assert trigger_exists(engine)
