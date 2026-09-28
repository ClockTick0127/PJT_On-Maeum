from datetime import datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from realtime_analysis.activity_repository import (
    SqlAlchemyApplianceActivityRepository,
)
from realtime_analysis.database import Base
from realtime_analysis.models import (
    ApplianceUsageSession,
    HouseholdActivityDaily,
    HouseholdObservationDaily,
)
from realtime_analysis.schemas import (
    ApplianceState,
    ApplianceStateTransition,
    ApplianceTransitionType,
)


START = datetime.fromisoformat("2026-09-10T09:00:00+09:00")


@pytest.fixture
def session_factory() -> sessionmaker[Session]:
    # 외부 PostgreSQL 없이 저장 로직만 빠르게 검증하기 위한 테스트 DB다.
    engine = create_engine("sqlite+pysqlite:///:memory:")
    for table_name in (
        "household_observation_daily",
        "household_activity_daily",
        "appliance_usage_session",
    ):
        Base.metadata.tables[table_name].create(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


def state(probability: float, is_on: bool) -> ApplianceState:
    return ApplianceState(
        appliance_type="MICROWAVE",
        probability=probability,
        threshold=0.5,
        is_on=is_on,
    )


def transition(
    transition_type: ApplianceTransitionType,
    probability: float,
    occurred_at: datetime,
    confirmed_at: datetime,
) -> ApplianceStateTransition:
    current_is_on = transition_type == ApplianceTransitionType.TURNED_ON
    return ApplianceStateTransition(
        household_id="H001",
        appliance_type="MICROWAVE",
        transition_type=transition_type,
        previous_is_on=not current_is_on,
        current_is_on=current_is_on,
        occurred_at=occurred_at,
        confirmed_at=confirmed_at,
        probability=probability,
        threshold=0.5,
    )


def test_session_lifecycle_and_daily_count_are_persisted(
    session_factory: sessionmaker[Session],
) -> None:
    # OFF → ON: 일일 관측/활동 행과 열린 세션이 함께 생성되는지 검증한다.
    repository = SqlAlchemyApplianceActivityRepository(
        session_factory,
        "Asia/Seoul",
    )
    turned_on = transition(
        ApplianceTransitionType.TURNED_ON,
        0.8,
        START,
        START + timedelta(seconds=2),
    )

    repository.record(
        "H001",
        START + timedelta(seconds=2),
        [state(0.8, True)],
        [turned_on],
        {"MICROWAVE"},
    )

    with session_factory() as session:
        observation = session.scalar(select(HouseholdObservationDaily))
        activity = session.scalar(select(HouseholdActivityDaily))
        usage_session = session.scalar(select(ApplianceUsageSession))

        assert observation is not None
        assert observation.household_id == "H001"
        assert observation.observation_date.isoformat() == "2026-09-10"
        assert activity is not None
        assert activity.appliance_type == "MICROWAVE"
        assert activity.event_count == 1
        assert usage_session is not None
        assert usage_session.ended_at is None
        assert usage_session.max_probability == Decimal("0.8000")
        assert usage_session.decision_threshold == Decimal("0.5000")

    # ON 유지: 더 높은 확률이 들어오면 max_probability가 갱신돼야 한다.
    repository.record(
        "H001",
        START + timedelta(seconds=3),
        [state(0.93, True)],
        [],
        {"MICROWAVE"},
    )

    with session_factory() as session:
        usage_session = session.scalar(select(ApplianceUsageSession))
        assert usage_session is not None
        assert usage_session.max_probability == Decimal("0.9300")

    # ON → OFF: 열린 세션에 종료 시각이 기록돼야 한다.
    turned_off = transition(
        ApplianceTransitionType.TURNED_OFF,
        0.4,
        START + timedelta(seconds=10),
        START + timedelta(seconds=12),
    )
    repository.record(
        "H001",
        START + timedelta(seconds=12),
        [state(0.4, False)],
        [turned_off],
        set(),
    )

    with session_factory() as session:
        usage_session = session.scalar(select(ApplianceUsageSession))
        assert usage_session is not None
        assert usage_session.ended_at is not None
        assert usage_session.ended_at.replace(tzinfo=START.tzinfo) == (
            START + timedelta(seconds=10)
        )


def test_data_gap_closes_open_sessions_at_last_valid_measurement(
    session_factory: sessionmaker[Session],
) -> None:
    repository = SqlAlchemyApplianceActivityRepository(
        session_factory,
        "Asia/Seoul",
    )
    repository.record(
        "H001",
        START + timedelta(seconds=2),
        [state(0.8, True)],
        [
            transition(
                ApplianceTransitionType.TURNED_ON,
                0.8,
                START,
                START + timedelta(seconds=2),
            )
        ],
        {"MICROWAVE"},
    )

    repository.close_open_sessions("H001", START + timedelta(seconds=30))

    with session_factory() as session:
        usage_session = session.scalar(select(ApplianceUsageSession))
        assert usage_session is not None
        assert usage_session.ended_at is not None
        assert usage_session.ended_at.replace(tzinfo=START.tzinfo) == (
            START + timedelta(seconds=30)
        )


def test_replayed_turn_on_does_not_duplicate_session_or_event_count(
    session_factory: sessionmaker[Session],
) -> None:
    # 동일한 시작 이벤트를 두 번 처리해도 세션과 event_count는 한 번만 생성한다.
    repository = SqlAlchemyApplianceActivityRepository(
        session_factory,
        "Asia/Seoul",
    )
    turned_on = transition(
        ApplianceTransitionType.TURNED_ON,
        0.8,
        START,
        START + timedelta(seconds=2),
    )

    for _ in range(2):
        repository.record(
            "H001",
            START + timedelta(seconds=2),
            [state(0.8, True)],
            [turned_on],
            {"MICROWAVE"},
        )

    with session_factory() as session:
        sessions = session.scalars(select(ApplianceUsageSession)).all()
        activity = session.scalar(select(HouseholdActivityDaily))
        assert len(sessions) == 1
        assert activity is not None
        assert activity.event_count == 1


def test_session_insert_failure_rolls_back_daily_rows(
    session_factory: sessionmaker[Session],
) -> None:
    # 세션 INSERT가 제약조건으로 실패하면 앞서 만든 일일 행까지 모두 롤백한다.
    repository = SqlAlchemyApplianceActivityRepository(
        session_factory,
        "Asia/Seoul",
    )
    invalid_transition = transition(
        ApplianceTransitionType.TURNED_ON,
        0.8,
        START,
        START + timedelta(seconds=2),
    ).model_copy(update={"probability": 1.2})

    with pytest.raises(IntegrityError):
        repository.record(
            "H001",
            START + timedelta(seconds=2),
            [state(0.8, True)],
            [invalid_transition],
            {"MICROWAVE"},
        )

    with session_factory() as session:
        assert session.scalar(select(HouseholdObservationDaily)) is None
        assert session.scalar(select(HouseholdActivityDaily)) is None
        assert session.scalar(select(ApplianceUsageSession)) is None


def test_observation_sample_count_and_coverage_are_accumulated(
    session_factory: sessionmaker[Session],
) -> None:
    repository = SqlAlchemyApplianceActivityRepository(
        session_factory,
        "Asia/Seoul",
        expected_samples_per_day=4,
    )

    repository.record_observation("H001", START)
    repository.record_observation("H001", START + timedelta(seconds=1))
    repository.record_observation("H001", START + timedelta(seconds=2))

    with session_factory() as session:
        observation = session.scalar(select(HouseholdObservationDaily))
        assert observation is not None
        assert observation.sample_count == 3
        assert observation.expected_sample_count == 4
        assert observation.coverage_ratio == Decimal("0.7500")
        assert observation.observation_status == "COLLECTING"


def test_replayed_timestamp_is_not_counted_twice(
    session_factory: sessionmaker[Session],
) -> None:
    repository = SqlAlchemyApplianceActivityRepository(
        session_factory,
        "Asia/Seoul",
        expected_samples_per_day=4,
    )

    repository.record_observation("H001", START)
    repository.record_observation("H001", START)

    with session_factory() as session:
        observation = session.scalar(select(HouseholdObservationDaily))
        assert observation is not None
        assert observation.sample_count == 1
        assert observation.coverage_ratio == Decimal("0.2500")


def test_finalized_observation_ignores_late_sample(
    session_factory: sessionmaker[Session],
) -> None:
    repository = SqlAlchemyApplianceActivityRepository(
        session_factory,
        "Asia/Seoul",
        expected_samples_per_day=4,
    )
    with session_factory.begin() as session:
        session.add(
            HouseholdObservationDaily(
                household_id="H001",
                observation_date=START.date(),
                sample_count=3,
                expected_sample_count=4,
                coverage_ratio=Decimal("0.7500"),
                observation_status="VALID",
                updated_at=START,
            )
        )

    repository.record_observation("H001", START + timedelta(seconds=1))

    with session_factory() as session:
        observation = session.scalar(select(HouseholdObservationDaily))
        assert observation is not None
        assert observation.sample_count == 3


def test_next_day_input_keeps_previous_day_collecting_until_daily_job(
    session_factory: sessionmaker[Session],
) -> None:
    repository = SqlAlchemyApplianceActivityRepository(
        session_factory,
        "Asia/Seoul",
        expected_samples_per_day=4,
    )

    for second in range(3):
        repository.record_observation("H001", START + timedelta(seconds=second))
    repository.record_observation("H002", START)

    with session_factory.begin() as session:
        session.add(
            HouseholdObservationDaily(
                household_id="H003",
                observation_date=START.date(),
                sample_count=0,
                expected_sample_count=4,
                coverage_ratio=Decimal("0.0000"),
                observation_status="COLLECTING",
                updated_at=START,
            )
        )

    repository.record_observation("H001", START + timedelta(days=1))

    with session_factory() as session:
        observations = {
            (item.household_id, item.observation_date.isoformat()): item
            for item in session.scalars(select(HouseholdObservationDaily)).all()
        }
        assert observations[("H001", "2026-09-10")].observation_status == "COLLECTING"
        assert observations[("H002", "2026-09-10")].observation_status == "COLLECTING"
        assert observations[("H003", "2026-09-10")].observation_status == "COLLECTING"
        current = observations[("H001", "2026-09-11")]
        assert current.observation_status == "COLLECTING"
        assert current.sample_count == 1
