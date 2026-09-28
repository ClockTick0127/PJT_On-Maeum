from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker

from realtime_analysis.baseline import SqlAlchemyBaselineRepository
from aggregation_service.baseline_updater import (
    RoutineBaselineCalculator,
    RoutineBaselineUpdateService,
    first_valid_logical_use,
)
from realtime_analysis.database import Base
from realtime_analysis.models import (
    ApplianceUsageSession,
    HouseholdActivityDaily,
    HouseholdObservationDaily,
    RoutineBaselineModel,
)
from realtime_analysis.schemas import RoutineBaseline


SEOUL = ZoneInfo("Asia/Seoul")


@pytest.fixture
def session_factory() -> sessionmaker[Session]:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    for table_name in (
        "routine_baseline",
        "household_observation_daily",
        "household_activity_daily",
        "appliance_usage_session",
    ):
        Base.metadata.tables[table_name].create(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


def add_observation(
    session: Session,
    observation_date: date,
    status: str = "VALID",
    first_use_minute: int | None = None,
) -> None:
    observed_at = datetime.combine(
        observation_date,
        datetime.min.time(),
        SEOUL,
    ).astimezone(timezone.utc)
    observation = HouseholdObservationDaily(
        household_id="H001",
        observation_date=observation_date,
        sample_count=86_400 if status == "VALID" else 1_000,
        expected_sample_count=86_400,
        coverage_ratio=Decimal("1.0000") if status == "VALID" else Decimal("0.0116"),
        observation_status=status,
        updated_at=observed_at,
    )
    session.add(observation)
    session.flush()
    if first_use_minute is None:
        return

    activity = HouseholdActivityDaily(
        observation_daily_id=observation.id,
        appliance_type="MICROWAVE",
        event_count=1,
        updated_at=observed_at,
    )
    session.add(activity)
    session.flush()
    started_at = datetime(
        observation_date.year,
        observation_date.month,
        observation_date.day,
        8,
        first_use_minute,
        tzinfo=SEOUL,
    ).astimezone(timezone.utc)
    session.add(
        ApplianceUsageSession(
            activity_daily_id=activity.id,
            started_at=started_at,
            ended_at=started_at + timedelta(minutes=2),
            max_probability=Decimal("0.9000"),
            decision_threshold=Decimal("0.5000"),
            updated_at=started_at + timedelta(minutes=2),
        )
    )


def test_daily_update_uses_only_valid_days_and_is_idempotent(
    session_factory: sessionmaker[Session],
) -> None:
    as_of_date = date(2026, 9, 16)
    valid_dates = [as_of_date - timedelta(days=offset) for offset in range(13, -1, -1)]
    with session_factory.begin() as session:
        for index, observation_date in enumerate(valid_dates):
            add_observation(
                session,
                observation_date,
                first_use_minute=index if index < 12 else None,
            )
        add_observation(
            session,
            as_of_date - timedelta(days=14),
            status="INSUFFICIENT_DATA",
            first_use_minute=59,
        )

    repository = SqlAlchemyBaselineRepository(session_factory, "Asia/Seoul")
    service = RoutineBaselineUpdateService(
        session_factory,
        repository,
        "Asia/Seoul",
        window_days=28,
        minimum_sample_days=14,
        minimum_weekday_sample_days=4,
        minimum_daily_use_probability=0.70,
    )

    assert service.update(as_of_date) == 6
    assert service.update(as_of_date) == 6

    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(RoutineBaselineModel)) == 6
        microwave = session.scalar(
            select(RoutineBaselineModel).where(
                RoutineBaselineModel.appliance_type == "MICROWAVE"
            )
        )
        assert microwave is not None
        assert microwave.sample_days == 14
        assert microwave.active_days == 12
        assert microwave.daily_use_probability == Decimal("0.8571")
        assert microwave.enabled is True
        assert microwave.baseline_data["expected_until"] == "08:10"
        assert microwave.baseline_data["as_of_date"] == "2026-09-16"
        assert microwave.baseline_data["source"] == "VALID_DAILY_OBSERVATIONS"

    baselines = repository.find_by_household(
        "H001",
        datetime(2026, 9, 17, 9, tzinfo=SEOUL),
    )
    assert len(baselines) == 1
    assert baselines[0].appliance_type == "MICROWAVE"
    assert baselines[0].normal_days == 12
    assert baselines[0].window_days == 14
    assert baselines[0].expected_until.isoformat(timespec="minutes") == "08:10"


def test_update_skips_household_until_minimum_valid_days(
    session_factory: sessionmaker[Session],
) -> None:
    as_of_date = date(2026, 9, 16)
    with session_factory.begin() as session:
        for offset in range(13):
            add_observation(session, as_of_date - timedelta(days=offset), first_use_minute=0)

    repository = SqlAlchemyBaselineRepository(session_factory, "Asia/Seoul")
    service = RoutineBaselineUpdateService(
        session_factory,
        repository,
        "Asia/Seoul",
        minimum_sample_days=14,
    )

    assert service.update(as_of_date) == 0
    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(RoutineBaselineModel)) == 0


def test_calculator_builds_eligible_weekday_profile() -> None:
    calculator = RoutineBaselineCalculator(
        window_days=28,
        minimum_sample_days=14,
        minimum_weekday_sample_days=2,
        minimum_daily_use_probability=0.70,
    )
    start = date(2026, 9, 1)
    valid_dates = [start + timedelta(days=offset) for offset in range(14)]
    first_use = {
        "MICROWAVE": {
            item: (9 * 3600 if item.weekday() == 0 else 8 * 3600)
            for item in valid_dates
        }
    }

    candidates = calculator.calculate(date(2026, 9, 14), valid_dates, first_use)
    microwave = next(
        item for item in candidates if item.appliance_type == "MICROWAVE"
    )
    monday = microwave.baseline_data["weekday_profiles"]["MON"]

    assert monday["sample_days"] == 2
    assert monday["active_days"] == 2
    assert monday["expected_until"] == "09:00"
    assert monday["eligible"] is True


def test_bootstrap_baseline_is_inserted_only_when_missing(
    session_factory: sessionmaker[Session],
) -> None:
    repository = SqlAlchemyBaselineRepository(session_factory, "Asia/Seoul")
    baseline = RoutineBaseline(
        id=UUID("226dfc19-3222-49a4-8fde-a314573aa23e"),
        household_id="H001",
        appliance_type="MICROWAVE",
        expected_until="08:10",
        normal_days=12,
        window_days=14,
    )

    assert repository.seed_missing([baseline]) == 1
    assert repository.seed_missing([baseline]) == 0

    loaded = repository.find_by_household("H001")
    assert len(loaded) == 1
    assert loaded[0].expected_until.isoformat(timespec="minutes") == "08:10"


def test_nearby_short_sessions_form_one_valid_baseline_use() -> None:
    started_at = datetime(2026, 9, 16, 0, tzinfo=timezone.utc)

    first_use = first_valid_logical_use(
        "MICROWAVE",
        [
            (started_at, started_at + timedelta(seconds=6)),
            (
                started_at + timedelta(seconds=30),
                started_at + timedelta(seconds=36),
            ),
        ],
    )

    assert first_use == started_at
