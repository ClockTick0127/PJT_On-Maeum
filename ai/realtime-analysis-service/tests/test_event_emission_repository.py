from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from realtime_analysis.database import Base
from realtime_analysis.event_emission_repository import (
    SqlAlchemyEventEmissionRepository,
)
from realtime_analysis.models import AnalysisEventEmission


EMITTED_AT = datetime(2026, 9, 18, 6, 30, tzinfo=timezone.utc)
EVENT_ID = UUID("34c12866-8329-51e3-9e22-57c326f0a395")


@pytest.fixture
def session_factory() -> sessionmaker[Session]:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.tables["analysis_event_emission"].create(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


def test_records_emission_and_finds_it_by_event_id(
    session_factory: sessionmaker[Session],
) -> None:
    repository = SqlAlchemyEventEmissionRepository(session_factory)

    inserted = repository.record_emission(
        event_id=EVENT_ID,
        household_id="H001",
        event_type="ROUTINE_MISSED",
        appliance_type="MICROWAVE",
        emitted_at=EMITTED_AT,
    )

    assert inserted is True
    assert repository.was_emitted(EVENT_ID) is True
    with session_factory() as session:
        emission = session.scalar(select(AnalysisEventEmission))
        assert emission is not None
        assert emission.household_id == "H001"
        assert emission.event_type == "ROUTINE_MISSED"
        assert emission.appliance_type == "MICROWAVE"
        assert _as_utc(emission.emitted_at) == EMITTED_AT


def test_duplicate_event_id_is_idempotent(
    session_factory: sessionmaker[Session],
) -> None:
    repository = SqlAlchemyEventEmissionRepository(session_factory)

    first = repository.record_emission(
        EVENT_ID,
        "H001",
        "ROUTINE_MISSED",
        "MICROWAVE",
        EMITTED_AT,
    )
    second = repository.record_emission(
        EVENT_ID,
        "H999",
        "PROLONGED_INACTIVITY",
        None,
        EMITTED_AT + timedelta(hours=1),
    )

    assert first is True
    assert second is False
    with session_factory() as session:
        emissions = session.scalars(select(AnalysisEventEmission)).all()
        assert len(emissions) == 1
        assert emissions[0].household_id == "H001"


def test_returns_latest_emission_for_exact_cooldown_scope(
    session_factory: sessionmaker[Session],
) -> None:
    repository = SqlAlchemyEventEmissionRepository(session_factory)
    repository.record_emission(
        UUID("2e945e22-9cbb-5452-931b-b8c87a559909"),
        "H001",
        "ROUTINE_MISSED",
        "MICROWAVE",
        EMITTED_AT,
    )
    repository.record_emission(
        UUID("77217377-aa4a-51f7-b3fd-2476ab7fe20a"),
        "H001",
        "ROUTINE_MISSED",
        "MICROWAVE",
        EMITTED_AT + timedelta(hours=2),
    )
    repository.record_emission(
        UUID("7015f423-e70e-5d2c-9f10-a44bfaf69796"),
        "H001",
        "ROUTINE_MISSED",
        "KETTLE",
        EMITTED_AT + timedelta(hours=3),
    )

    latest = repository.last_emitted_at(
        "H001",
        "ROUTINE_MISSED",
        "MICROWAVE",
    )

    assert latest is not None
    assert _as_utc(latest) == EMITTED_AT + timedelta(hours=2)
    assert repository.last_emitted_at(
        "H002",
        "ROUTINE_MISSED",
        "MICROWAVE",
    ) is None


def test_none_appliance_is_an_independent_cooldown_scope(
    session_factory: sessionmaker[Session],
) -> None:
    repository = SqlAlchemyEventEmissionRepository(session_factory)
    repository.record_emission(
        UUID("431582c1-e614-5625-ad71-761331f2db12"),
        "H001",
        "PROLONGED_INACTIVITY",
        None,
        EMITTED_AT,
    )

    latest = repository.last_emitted_at(
        "H001",
        "PROLONGED_INACTIVITY",
        None,
    )

    assert latest is not None
    assert _as_utc(latest) == EMITTED_AT
    assert repository.last_emitted_at(
        "H001",
        "PROLONGED_INACTIVITY",
        "MICROWAVE",
    ) is None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
