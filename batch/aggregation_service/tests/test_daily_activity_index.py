from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import Mock

import pytest
from prometheus_client import CollectorRegistry
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from aggregation_service.daily_activity_index import (
    DailyActivityIndexCalculator,
    DailyActivityIndexRepository,
    DailyActivityIndexScheduler,
    DailyActivityIndexService,
    SessionSlice,
)
from realtime_analysis.database import Base
from realtime_analysis.metrics import AnalysisMetrics
from realtime_analysis.models import (
    ApplianceUsageSession,
    HouseholdActivityDaily,
    HouseholdObservationDaily,
)


@pytest.fixture
def session_factory() -> sessionmaker[Session]:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    for table_name in (
        "household_observation_daily",
        "household_activity_daily",
        "appliance_usage_session",
    ):
        Base.metadata.tables[table_name].create(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


def test_nearby_hysteresis_sessions_are_one_logical_use() -> None:
    calculator = DailyActivityIndexCalculator()
    start = datetime(2026, 9, 16, 1, tzinfo=timezone.utc)

    components = calculator.calculate(
        [
            SessionSlice("MICROWAVE", start, start + timedelta(seconds=30), True),
            SessionSlice(
                "MICROWAVE",
                start + timedelta(seconds=60),
                start + timedelta(seconds=90),
                True,
            ),
        ]
    )

    assert components.usage_count == 1
    assert components.appliance_type_count == 1
    assert components.usage_duration_seconds == 60


def test_duration_score_caps_long_kettle_use_but_keeps_raw_duration() -> None:
    calculator = DailyActivityIndexCalculator()
    start = datetime(2026, 9, 16, 1, tzinfo=timezone.utc)

    components = calculator.calculate(
        [SessionSlice("KETTLE", start, start + timedelta(hours=2), True)]
    )

    assert components.usage_duration_seconds == 7_200
    assert components.usage_duration_score == 17


def test_valid_day_builds_absolute_index_from_daily_sessions(
    session_factory: sessionmaker[Session],
) -> None:
    target_date = date(2026, 9, 16)
    started_at = datetime(2026, 9, 16, 0, 0, tzinfo=timezone.utc)
    with session_factory.begin() as session:
        observation = HouseholdObservationDaily(
            household_id="H001",
            observation_date=target_date,
            sample_count=4,
            expected_sample_count=4,
            coverage_ratio=Decimal("1.0000"),
            observation_status="COLLECTING",
            updated_at=started_at,
        )
        session.add(observation)
        session.flush()
        activity = HouseholdActivityDaily(
            observation_daily_id=observation.id,
            appliance_type="MICROWAVE",
            event_count=1,
            updated_at=started_at,
        )
        session.add(activity)
        session.flush()
        session.add(
            ApplianceUsageSession(
                activity_daily_id=activity.id,
                started_at=started_at,
                ended_at=started_at + timedelta(minutes=10),
                max_probability=Decimal("0.9000"),
                decision_threshold=Decimal("0.5000"),
                updated_at=started_at + timedelta(minutes=10),
            )
        )

    repository = DailyActivityIndexRepository(
        session_factory,
        "Asia/Seoul",
        expected_samples_per_day=4,
        valid_coverage_ratio=0.75,
    )
    message = repository.build_messages(target_date, ["H001"])[0]

    assert message.data_status == "VALID"
    assert message.activity_index is not None
    assert message.components is not None
    assert message.components.usage_count == 1
    assert message.components.appliance_type_count == 1
    assert message.components.usage_duration_seconds == 600

    with session_factory() as session:
        observation = session.scalar(select(HouseholdObservationDaily))
        assert observation is not None
        assert observation.observation_status == "VALID"


def test_missing_day_publishes_null_index_and_creates_sensor_gap_observation(
    session_factory: sessionmaker[Session],
) -> None:
    target_date = date(2026, 9, 16)
    repository = DailyActivityIndexRepository(
        session_factory,
        "Asia/Seoul",
        expected_samples_per_day=4,
        valid_coverage_ratio=0.75,
    )

    first = repository.build_messages(target_date, ["H001"])[0]
    second = repository.build_messages(target_date, ["H001"])[0]

    assert first.data_status == "INSUFFICIENT_DATA"
    assert first.activity_index is None
    assert first.components is None
    assert second.message_id == first.message_id

    with session_factory() as session:
        observation = session.scalar(select(HouseholdObservationDaily))
        assert observation is not None
        assert observation.sample_count == 0
        assert observation.observation_status == "SENSOR_GAP"


def test_cross_midnight_session_contributes_duration_without_new_use_count(
    session_factory: sessionmaker[Session],
) -> None:
    target_date = date(2026, 9, 16)
    previous_date = date(2026, 9, 15)
    # Asia/Seoul 2026-09-15 23:50 through 2026-09-16 00:20.
    started_at = datetime(2026, 9, 15, 14, 50, tzinfo=timezone.utc)
    ended_at = datetime(2026, 9, 15, 15, 20, tzinfo=timezone.utc)
    with session_factory.begin() as session:
        previous_observation = HouseholdObservationDaily(
            household_id="H001",
            observation_date=previous_date,
            sample_count=4,
            expected_sample_count=4,
            coverage_ratio=Decimal("1.0000"),
            observation_status="VALID",
            updated_at=started_at,
        )
        target_observation = HouseholdObservationDaily(
            household_id="H001",
            observation_date=target_date,
            sample_count=4,
            expected_sample_count=4,
            coverage_ratio=Decimal("1.0000"),
            observation_status="COLLECTING",
            updated_at=ended_at,
        )
        session.add_all([previous_observation, target_observation])
        session.flush()
        activity = HouseholdActivityDaily(
            observation_daily_id=previous_observation.id,
            appliance_type="MICROWAVE",
            event_count=1,
            updated_at=started_at,
        )
        session.add(activity)
        session.flush()
        session.add(
            ApplianceUsageSession(
                activity_daily_id=activity.id,
                started_at=started_at,
                ended_at=ended_at,
                max_probability=Decimal("0.9000"),
                decision_threshold=Decimal("0.5000"),
                updated_at=ended_at,
            )
        )

    repository = DailyActivityIndexRepository(
        session_factory,
        "Asia/Seoul",
        expected_samples_per_day=4,
        valid_coverage_ratio=0.75,
    )
    message = repository.build_messages(target_date, ["H001"])[0]

    assert message.components is not None
    assert message.components.usage_count == 0
    assert message.components.appliance_type_count == 1
    assert message.components.usage_duration_seconds == 20 * 60


def test_scheduler_publishes_each_due_date_once(
    session_factory: sessionmaker[Session],
) -> None:
    repository = DailyActivityIndexRepository(
        session_factory,
        "Asia/Seoul",
        expected_samples_per_day=4,
        valid_coverage_ratio=0.75,
    )
    publisher = Mock()
    service = DailyActivityIndexService(repository, publisher, ["H001"])
    scheduler = DailyActivityIndexScheduler(
        service,
        "Asia/Seoul",
        publish_hour=0,
        publish_minute=10,
        poll_seconds=30,
    )
    now = datetime(2026, 9, 17, 1, 0, tzinfo=timezone.utc)

    assert scheduler.run_due(now) is True
    assert scheduler.run_due(now) is False
    assert publisher.publish.call_count == 1
    assert publisher.publish.call_args.args[0].activity_date == date(2026, 9, 16)


def test_scheduler_uses_persistent_run_registry_and_still_runs_completion_hook(
    session_factory: sessionmaker[Session],
) -> None:
    repository = DailyActivityIndexRepository(
        session_factory,
        "Asia/Seoul",
        expected_samples_per_day=4,
        valid_coverage_ratio=0.75,
    )
    service = DailyActivityIndexService(repository, Mock(), ["H001"])
    run_repository = Mock()
    run_repository.has_succeeded.return_value = True
    completion_hook = Mock()
    scheduler = DailyActivityIndexScheduler(
        service,
        "Asia/Seoul",
        publish_hour=0,
        publish_minute=10,
        poll_seconds=30,
        run_repository=run_repository,
        completion_hook=completion_hook,
    )

    assert scheduler.run_due(datetime(2026, 9, 17, 1, tzinfo=timezone.utc)) is False
    completion_hook.assert_called_once_with(date(2026, 9, 16))
    run_repository.run.assert_not_called()


def test_baseline_is_updated_after_daily_activity_messages(
    session_factory: sessionmaker[Session],
) -> None:
    repository = DailyActivityIndexRepository(
        session_factory,
        "Asia/Seoul",
        expected_samples_per_day=4,
        valid_coverage_ratio=0.75,
    )
    calls: list[str] = []
    publisher = Mock()
    publisher.publish.side_effect = lambda message: calls.append("publish")
    updater = Mock()
    updater.update.side_effect = lambda activity_date: calls.append("baseline")
    daily_detector = Mock()
    daily_detector.detect_and_publish.side_effect = (
        lambda activity_date: calls.append("daily-event")
    )
    registry = CollectorRegistry()
    metrics = AnalysisMetrics(registry)
    service = DailyActivityIndexService(
        repository,
        publisher,
        ["H001"],
        baseline_updater=updater,
        daily_event_detector=daily_detector,
        metrics=metrics,
    )

    service.publish_date(date(2026, 9, 16))

    assert calls == ["publish", "daily-event", "baseline"]
    daily_detector.detect_and_publish.assert_called_once_with(date(2026, 9, 16))
    updater.update.assert_called_once_with(date(2026, 9, 16))
    for job in ("activity_index", "routine_changed", "baseline_update"):
        assert registry.get_sample_value(
            "nilm_daily_job_duration_seconds_count",
            {"job": job},
        ) == 1
        assert registry.get_sample_value(
            "nilm_daily_job_runs_total",
            {"job": job, "status": "success"},
        ) == 1


def test_daily_job_failure_is_counted(
    session_factory: sessionmaker[Session],
) -> None:
    repository = DailyActivityIndexRepository(
        session_factory,
        "Asia/Seoul",
        expected_samples_per_day=4,
        valid_coverage_ratio=0.75,
    )
    registry = CollectorRegistry()
    metrics = AnalysisMetrics(registry)
    updater = Mock()
    updater.update.side_effect = RuntimeError("baseline update failed")
    service = DailyActivityIndexService(
        repository,
        Mock(),
        ["H001"],
        baseline_updater=updater,
        metrics=metrics,
    )

    with pytest.raises(RuntimeError, match="baseline update failed"):
        service.publish_date(date(2026, 9, 16))

    assert registry.get_sample_value(
        "nilm_daily_job_runs_total",
        {"job": "baseline_update", "status": "error"},
    ) == 1


def test_observation_boundary_82079_is_insufficient_data(
    session_factory: sessionmaker[Session],
) -> None:
    """
    82,079 / 86,400 (약 0.949988) 샘플:
    - raw_ratio < 0.95 이므로 observation_status == "INSUFFICIENT_DATA"
    - 저장·표시용 coverage_ratio == Decimal("0.9500") (소수점 4자리 반올림)
    - 외부 발행 메시지 data_status == "INSUFFICIENT_DATA", activity_index is None, components is None
    """
    target_date = date(2026, 9, 16)
    household_id = "H001"
    with session_factory.begin() as session:
        session.add(
            HouseholdObservationDaily(
                household_id=household_id,
                observation_date=target_date,
                sample_count=82_079,
                expected_sample_count=86_400,
                coverage_ratio=Decimal("0.9500"),
                observation_status="COLLECTING",
                updated_at=datetime(2026, 9, 16, 23, 59, 59, tzinfo=timezone.utc),
            )
        )

    repository = DailyActivityIndexRepository(
        session_factory,
        "Asia/Seoul",
        expected_samples_per_day=86_400,
        valid_coverage_ratio=0.95,
    )
    messages = repository.build_messages(target_date, [household_id])
    assert len(messages) == 1
    msg = messages[0]
    assert msg.data_status == "INSUFFICIENT_DATA"
    assert msg.activity_index is None
    assert msg.components is None

    with session_factory() as session:
        obs = session.scalar(
            select(HouseholdObservationDaily).where(
                HouseholdObservationDaily.household_id == household_id,
                HouseholdObservationDaily.observation_date == target_date,
            )
        )
        assert obs is not None
        assert obs.observation_status == "INSUFFICIENT_DATA"
        assert obs.coverage_ratio == Decimal("0.9500")
        assert obs.sample_count == 82_079
        assert obs.expected_sample_count == 86_400


def test_observation_boundary_82080_is_valid(
    session_factory: sessionmaker[Session],
) -> None:
    """
    82,080 / 86,400 (정확히 0.95) 샘플:
    - raw_ratio == 0.95 이므로 observation_status == "VALID"
    - 저장·표시용 coverage_ratio == Decimal("0.9500")
    - 외부 발행 메시지 data_status == "VALID"
    - 활동 세션이 없으므로 activity_index == 0, components 점수 및 사용량 모두 0
    """
    target_date = date(2026, 9, 16)
    household_id = "H002"
    with session_factory.begin() as session:
        session.add(
            HouseholdObservationDaily(
                household_id=household_id,
                observation_date=target_date,
                sample_count=82_080,
                expected_sample_count=86_400,
                coverage_ratio=Decimal("0.9500"),
                observation_status="COLLECTING",
                updated_at=datetime(2026, 9, 16, 23, 59, 59, tzinfo=timezone.utc),
            )
        )

    repository = DailyActivityIndexRepository(
        session_factory,
        "Asia/Seoul",
        expected_samples_per_day=86_400,
        valid_coverage_ratio=0.95,
    )
    messages = repository.build_messages(target_date, [household_id])
    assert len(messages) == 1
    msg = messages[0]
    assert msg.data_status == "VALID"
    assert msg.activity_index == 0
    assert msg.components is not None
    assert msg.components.usage_count == 0
    assert msg.components.appliance_type_count == 0
    assert msg.components.usage_duration_seconds == 0
    assert msg.components.usage_count_score == 0
    assert msg.components.appliance_diversity_score == 0
    assert msg.components.usage_duration_score == 0

    with session_factory() as session:
        obs = session.scalar(
            select(HouseholdObservationDaily).where(
                HouseholdObservationDaily.household_id == household_id,
                HouseholdObservationDaily.observation_date == target_date,
            )
        )
        assert obs is not None
        assert obs.observation_status == "VALID"
        assert obs.coverage_ratio == Decimal("0.9500")
        assert obs.sample_count == 82_080


def test_observation_boundary_zero_samples_is_sensor_gap(
    session_factory: sessionmaker[Session],
) -> None:
    """
    0 / 86,400 샘플:
    - 내부 observation_status == "SENSOR_GAP", coverage_ratio == Decimal("0.0000")
    - 외부 발행 메시지 data_status == "INSUFFICIENT_DATA", activity_index is None, components is None
    """
    target_date = date(2026, 9, 16)
    household_id = "H003"
    with session_factory.begin() as session:
        session.add(
            HouseholdObservationDaily(
                household_id=household_id,
                observation_date=target_date,
                sample_count=0,
                expected_sample_count=86_400,
                coverage_ratio=Decimal("0.0000"),
                observation_status="COLLECTING",
                updated_at=datetime(2026, 9, 16, 23, 59, 59, tzinfo=timezone.utc),
            )
        )

    repository = DailyActivityIndexRepository(
        session_factory,
        "Asia/Seoul",
        expected_samples_per_day=86_400,
        valid_coverage_ratio=0.95,
    )
    messages = repository.build_messages(target_date, [household_id])
    assert len(messages) == 1
    msg = messages[0]
    assert msg.data_status == "INSUFFICIENT_DATA"
    assert msg.activity_index is None
    assert msg.components is None

    with session_factory() as session:
        obs = session.scalar(
            select(HouseholdObservationDaily).where(
                HouseholdObservationDaily.household_id == household_id,
                HouseholdObservationDaily.observation_date == target_date,
            )
        )
        assert obs is not None
        assert obs.observation_status == "SENSOR_GAP"
        assert obs.coverage_ratio == Decimal("0.0000")


def test_observation_boundary_exact_full_day_is_valid(
    session_factory: sessionmaker[Session],
) -> None:
    """
    86,400 / 86,400 샘플 (100%):
    - raw_ratio == 1.0 >= 0.95 이므로 observation_status == "VALID"
    - coverage_ratio == Decimal("1.0000")
    - 외부 발행 메시지 data_status == "VALID"
    """
    target_date = date(2026, 9, 16)
    household_id = "H004"
    with session_factory.begin() as session:
        session.add(
            HouseholdObservationDaily(
                household_id=household_id,
                observation_date=target_date,
                sample_count=86_400,
                expected_sample_count=86_400,
                coverage_ratio=Decimal("1.0000"),
                observation_status="COLLECTING",
                updated_at=datetime(2026, 9, 16, 23, 59, 59, tzinfo=timezone.utc),
            )
        )

    repository = DailyActivityIndexRepository(
        session_factory,
        "Asia/Seoul",
        expected_samples_per_day=86_400,
        valid_coverage_ratio=0.95,
    )
    messages = repository.build_messages(target_date, [household_id])
    assert len(messages) == 1
    assert messages[0].data_status == "VALID"

    with session_factory() as session:
        obs = session.scalar(
            select(HouseholdObservationDaily).where(
                HouseholdObservationDaily.household_id == household_id,
                HouseholdObservationDaily.observation_date == target_date,
            )
        )
        assert obs is not None
        assert obs.observation_status == "VALID"
        assert obs.coverage_ratio == Decimal("1.0000")


def test_observation_boundary_exceeding_samples_is_valid_and_capped_at_one(
    session_factory: sessionmaker[Session],
) -> None:
    """
    90,000 / 86,400 샘플 (예상치 초과 유입):
    - raw_ratio > 1.0 >= 0.95 이므로 observation_status == "VALID"
    - coverage_ratio는 Decimal("1.0000")으로 상한 처리되어 DB 체크 제약 만족
    - 외부 발행 메시지 data_status == "VALID"
    """
    target_date = date(2026, 9, 16)
    household_id = "H005"
    with session_factory.begin() as session:
        session.add(
            HouseholdObservationDaily(
                household_id=household_id,
                observation_date=target_date,
                sample_count=90_000,
                expected_sample_count=86_400,
                coverage_ratio=Decimal("1.0000"),
                observation_status="COLLECTING",
                updated_at=datetime(2026, 9, 16, 23, 59, 59, tzinfo=timezone.utc),
            )
        )

    repository = DailyActivityIndexRepository(
        session_factory,
        "Asia/Seoul",
        expected_samples_per_day=86_400,
        valid_coverage_ratio=0.95,
    )
    messages = repository.build_messages(target_date, [household_id])
    assert len(messages) == 1
    assert messages[0].data_status == "VALID"

    with session_factory() as session:
        obs = session.scalar(
            select(HouseholdObservationDaily).where(
                HouseholdObservationDaily.household_id == household_id,
                HouseholdObservationDaily.observation_date == target_date,
            )
        )
        assert obs is not None
        assert obs.observation_status == "VALID"
        assert obs.coverage_ratio == Decimal("1.0000")


def test_observation_boundary_custom_threshold_and_expected_generalized(
    session_factory: sessionmaker[Session],
) -> None:
    """
    사용자 정의 설정(expected=10,000, valid_coverage_ratio=0.80)에서
    7,999건은 INSUFFICIENT_DATA, 8,000건은 VALID로 정확히 양분되는 일반화 검증.
    (H006, H007 독립 가구 사용하여 상호 영향 배제)
    """
    target_date = date(2026, 9, 16)
    with session_factory.begin() as session:
        # H006: 7,999 / 10,000 = 0.7999 < 0.80
        session.add(
            HouseholdObservationDaily(
                household_id="H006",
                observation_date=target_date,
                sample_count=7_999,
                expected_sample_count=10_000,
                coverage_ratio=Decimal("0.7999"),
                observation_status="COLLECTING",
                updated_at=datetime(2026, 9, 16, 23, 59, 59, tzinfo=timezone.utc),
            )
        )
        # H007: 8,000 / 10,000 = 0.8000 >= 0.80
        session.add(
            HouseholdObservationDaily(
                household_id="H007",
                observation_date=target_date,
                sample_count=8_000,
                expected_sample_count=10_000,
                coverage_ratio=Decimal("0.8000"),
                observation_status="COLLECTING",
                updated_at=datetime(2026, 9, 16, 23, 59, 59, tzinfo=timezone.utc),
            )
        )

    repository = DailyActivityIndexRepository(
        session_factory,
        "Asia/Seoul",
        expected_samples_per_day=10_000,
        valid_coverage_ratio=0.80,
    )
    messages = repository.build_messages(target_date, ["H006", "H007"])
    msg_by_house = {m.household_id: m for m in messages}

    assert msg_by_house["H006"].data_status == "INSUFFICIENT_DATA"
    assert msg_by_house["H006"].activity_index is None
    assert msg_by_house["H007"].data_status == "VALID"
    assert msg_by_house["H007"].activity_index == 0

    with session_factory() as session:
        obs_6 = session.scalar(
            select(HouseholdObservationDaily).where(
                HouseholdObservationDaily.household_id == "H006",
                HouseholdObservationDaily.observation_date == target_date,
            )
        )
        assert obs_6 is not None
        assert obs_6.observation_status == "INSUFFICIENT_DATA"
        assert obs_6.coverage_ratio == Decimal("0.7999")

        obs_7 = session.scalar(
            select(HouseholdObservationDaily).where(
                HouseholdObservationDaily.household_id == "H007",
                HouseholdObservationDaily.observation_date == target_date,
            )
        )
        assert obs_7 is not None
        assert obs_7.observation_status == "VALID"
        assert obs_7.coverage_ratio == Decimal("0.8000")
