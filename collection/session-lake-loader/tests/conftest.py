"""Shared fixtures: SQLite schema for loader logic, PostgreSQL for end-to-end."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import os
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from realtime_analysis.database import Base
from realtime_analysis.models import (
    ApplianceUsageSession,
    HouseholdActivityDaily,
    HouseholdObservationDaily,
    SessionLakeOutbox,
)

from session_lake_loader.loader import SessionLakeLoader
from session_lake_loader.repository import SessionLakeRepository
from session_lake_loader.storage import LocalLakeStorage


LAKE_TABLES = (
    "household_observation_daily",
    "household_activity_daily",
    "appliance_usage_session",
    "session_lake_batch",
    "session_lake_outbox",
)
BRONZE_BASE = "/nilm/bronze/appliance-session"
MANIFEST_BASE = "/nilm/manifests/job=session-lake-loader"
T0 = datetime(2026, 9, 20, 0, 0, tzinfo=timezone.utc)


class FakeClock:
    def __init__(self, start: datetime = T0) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **kwargs: Any) -> datetime:
        self.now = self.now + timedelta(**kwargs)
        return self.now


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def sqlite_url(tmp_path: Path) -> str:
    # 파일 기반 + WAL: 스냅샷 읽기 중 다른 연결의 쓰기를 허용한다.
    return f"sqlite+pysqlite:///{tmp_path / 'analysis.sqlite'}"


@pytest.fixture
def session_factory(sqlite_url: str) -> sessionmaker[Session]:
    engine = create_engine(sqlite_url)

    @event.listens_for(engine, "connect")
    def _enable_wal(connection, _record) -> None:  # pragma: no cover - driver hook
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")

    for table_name in LAKE_TABLES:
        Base.metadata.tables[table_name].create(engine)
    return sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)


@pytest.fixture
def storage(tmp_path: Path) -> LocalLakeStorage:
    return LocalLakeStorage(tmp_path / "lake")


@pytest.fixture
def repository(session_factory: sessionmaker[Session], clock: FakeClock) -> SessionLakeRepository:
    return SessionLakeRepository(
        session_factory,
        bronze_base=BRONZE_BASE,
        manifest_base=MANIFEST_BASE,
        clock=clock,
    )


@pytest.fixture
def loader(
    repository: SessionLakeRepository,
    storage: LocalLakeStorage,
    clock: FakeClock,
) -> SessionLakeLoader:
    return SessionLakeLoader(
        repository,
        storage,
        rows_per_file=1000,
        retry_backoff_seconds=30,
        retry_max_backoff_seconds=300,
        clock=clock,
    )


# ---------- 세션 생성·변경 도우미 (SQLite에는 트리거가 없어 outbox를 직접 흉내 낸다) ----------


def iso(value: datetime | None) -> str | None:
    return value.astimezone(timezone.utc).isoformat() if value else None


class SessionFactoryHelper:
    """Creates sessions and mirrors the PostgreSQL trigger into the outbox."""

    def __init__(self, session_factory: sessionmaker[Session], clock: FakeClock) -> None:
        self._session_factory = session_factory
        self._clock = clock

    def create(
        self,
        *,
        household_id: str = "H001",
        appliance_type: str = "MICROWAVE",
        observation_date: date = date(2026, 9, 20),
        started_at: datetime | None = None,
        probability: str = "0.8000",
        capture: bool = True,
    ) -> UUID:
        started_at = started_at or self._clock()
        with self._session_factory.begin() as db:
            observation = db.query(HouseholdObservationDaily).filter_by(
                household_id=household_id, observation_date=observation_date
            ).one_or_none()
            if observation is None:
                observation = HouseholdObservationDaily(
                    household_id=household_id,
                    observation_date=observation_date,
                    sample_count=0,
                    expected_sample_count=86_400,
                    coverage_ratio=Decimal("0.0000"),
                    observation_status="COLLECTING",
                    updated_at=started_at,
                )
                db.add(observation)
                db.flush()
            activity = db.query(HouseholdActivityDaily).filter_by(
                observation_daily_id=observation.id, appliance_type=appliance_type
            ).one_or_none()
            if activity is None:
                activity = HouseholdActivityDaily(
                    observation_daily_id=observation.id,
                    appliance_type=appliance_type,
                    event_count=0,
                    updated_at=started_at,
                )
                db.add(activity)
                db.flush()
            usage = ApplianceUsageSession(
                activity_daily_id=activity.id,
                started_at=started_at,
                ended_at=None,
                max_probability=Decimal(probability),
                decision_threshold=Decimal("0.5000"),
                updated_at=started_at,
                lake_version=1,
            )
            db.add(usage)
            db.flush()
            activity.event_count += 1
            if capture:
                self._capture(db, usage, "INSERT", household_id, appliance_type, observation_date)
            return usage.id

    def finish(self, session_id: UUID, ended_at: datetime | None = None, capture: bool = True) -> None:
        ended_at = ended_at or self._clock()
        with self._session_factory.begin() as db:
            usage = db.get(ApplianceUsageSession, session_id)
            usage.ended_at = ended_at
            usage.updated_at = ended_at
            usage.lake_version += 1
            if capture:
                household_id, appliance_type, observation_date = self._parents(db, usage)
                self._capture(db, usage, "UPDATE", household_id, appliance_type, observation_date)

    def delete(self, session_id: UUID, capture: bool = True) -> None:
        with self._session_factory.begin() as db:
            usage = db.get(ApplianceUsageSession, session_id)
            if capture:
                self._capture(db, usage, "DELETE", None, None, None)
            db.delete(usage)

    def outbox(self, **filters: Any) -> list[SessionLakeOutbox]:
        with self._session_factory() as db:
            return db.query(SessionLakeOutbox).filter_by(**filters).order_by(
                SessionLakeOutbox.event_id
            ).all()

    def lake_version(self, session_id: UUID) -> int:
        with self._session_factory() as db:
            return db.get(ApplianceUsageSession, session_id).lake_version

    @staticmethod
    def _parents(db: Session, usage: ApplianceUsageSession) -> tuple[str, str, date]:
        activity = db.get(HouseholdActivityDaily, usage.activity_daily_id)
        observation = db.get(HouseholdObservationDaily, activity.observation_daily_id)
        return observation.household_id, activity.appliance_type, observation.observation_date

    def _capture(
        self,
        db: Session,
        usage: ApplianceUsageSession,
        operation: str,
        household_id: str | None,
        appliance_type: str | None,
        observation_date: date | None,
    ) -> None:
        payload: dict[str, Any] = {
            "id": str(usage.id),
            "activity_daily_id": str(usage.activity_daily_id),
            "started_at": iso(_aware(usage.started_at)),
            "ended_at": iso(_aware(usage.ended_at)),
            "max_probability": float(usage.max_probability),
            "decision_threshold": float(usage.decision_threshold),
            "updated_at": iso(_aware(usage.updated_at)),
        }
        version = usage.lake_version
        if operation == "DELETE":
            version = usage.lake_version + 1
        else:
            payload.update(
                household_id=household_id,
                appliance_type=appliance_type,
                observation_date=observation_date.isoformat(),
            )
        db.add(
            SessionLakeOutbox(
                session_id=usage.id,
                session_version=version,
                operation=operation,
                payload=payload,
                changed_at=self._clock(),
                delivery_status="PENDING",
            )
        )


def _aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


@pytest.fixture
def sessions(session_factory: sessionmaker[Session], clock: FakeClock) -> SessionFactoryHelper:
    return SessionFactoryHelper(session_factory, clock)


# ---------- PostgreSQL (opt-in) ----------


def postgres_url() -> str | None:
    return os.environ.get("TEST_DATABASE_URL") or None


requires_postgres = pytest.mark.skipif(
    postgres_url() is None,
    reason="TEST_DATABASE_URL not set (disposable PostgreSQL required)",
)


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "postgres: requires TEST_DATABASE_URL")


def run_analysis_migrations(url: str, revision: str = "head") -> None:
    """Apply the realtime-analysis-service Alembic migrations to ``url``."""

    from alembic import command
    from alembic.config import Config
    from sqlalchemy.engine import make_url

    from realtime_analysis.config import get_settings

    parsed = make_url(url)
    os.environ.update(
        DATABASE_HOST=parsed.host or "localhost",
        DATABASE_PORT=str(parsed.port or 5432),
        DATABASE_NAME=parsed.database or "",
        DATABASE_USER=parsed.username or "",
        DATABASE_PASSWORD=parsed.password or "",
    )
    get_settings.cache_clear()
    service_dir = Path(__file__).resolve().parents[3] / "ai" / "realtime-analysis-service"
    config = Config(str(service_dir / "alembic.ini"))
    config.set_main_option("script_location", str(service_dir / "alembic"))
    if revision == "head":
        command.upgrade(config, "head")
    else:
        command.downgrade(config, revision)
