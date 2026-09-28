from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

import pytest
from prometheus_client import CollectorRegistry
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from realtime_analysis.database import Base
from realtime_analysis.metrics import AnalysisMetrics
from realtime_analysis.models import (
    ApplianceUsageSession,
    HouseholdActivityDaily,
    HouseholdObservationDaily,
)
from realtime_analysis.policy import SqlAlchemyPolicyRepository
from aggregation_service.routine_change_detector import RoutineChangeDetectionService
from realtime_analysis.schemas import AnalysisEvent, AnalysisPolicyDefinition


class RecordingPublisher:
    def __init__(self) -> None:
        self.events: list[AnalysisEvent] = []

    def publish(self, event: AnalysisEvent) -> None:
        self.events.append(event)


@pytest.fixture
def session_factory() -> sessionmaker[Session]:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    for table_name in (
        "analysis_policy",
        "household_observation_daily",
        "household_activity_daily",
        "appliance_usage_session",
    ):
        Base.metadata.tables[table_name].create(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


def test_daily_detector_emits_sustained_first_use_time_shift(
    session_factory: sessionmaker[Session],
) -> None:
    as_of_date = date(2026, 9, 16)
    with session_factory.begin() as session:
        for offset in range(28):
            observation_date = as_of_date - timedelta(days=27 - offset)
            observation = HouseholdObservationDaily(
                household_id="H001",
                observation_date=observation_date,
                sample_count=86_400,
                expected_sample_count=86_400,
                coverage_ratio=Decimal("1.0000"),
                observation_status="VALID",
                updated_at=datetime.now(timezone.utc),
            )
            session.add(observation)
            session.flush()
            activity = HouseholdActivityDaily(
                observation_daily_id=observation.id,
                appliance_type="MICROWAVE",
                event_count=1,
                updated_at=datetime.now(timezone.utc),
            )
            session.add(activity)
            session.flush()
            local_hour = 8 if offset < 21 else 10
            local_minute = 0 if offset < 21 else 30
            started_at = datetime.combine(
                observation_date,
                time(local_hour, local_minute, tzinfo=timezone(timedelta(hours=9))),
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

    policies = SqlAlchemyPolicyRepository(session_factory)
    policies.seed_missing(
        [
            AnalysisPolicyDefinition(
                policy_code="ROUTINE_CHANGED_DEFAULT",
                algorithm_type="FIRST_USE_TIME_SHIFT",
                parameters={
                    "recent_window_days": 7,
                    "recent_minimum_valid_days": 5,
                    "reference_window_days": 21,
                    "reference_minimum_valid_days": 14,
                    "minimum_daily_use_probability": 0.7,
                    "minimum_shift_minutes": 120,
                },
                event_type="ROUTINE_CHANGED",
                cooldown_hours=168,
            )
        ]
    )
    publisher = RecordingPublisher()
    registry = CollectorRegistry()
    metrics = AnalysisMetrics(registry)
    service = RoutineChangeDetectionService(
        session_factory,
        policies,
        publisher,
        "Asia/Seoul",
        metrics,
    )

    assert service.detect_and_publish(as_of_date) == 1
    assert service.detect_and_publish(as_of_date) == 0
    event = publisher.events[0]
    assert event.event_type == "ROUTINE_CHANGED"
    assert event.reason["previous_time"] == "08:00"
    assert event.reason["recent_time"] == "10:30"
    assert event.reason["shift_minutes"] == 150
    assert registry.get_sample_value(
        "nilm_pattern_detection_total",
        {"pattern": "ROUTINE_CHANGED", "result": "detected"},
    ) == 1
    assert registry.get_sample_value(
        "nilm_pattern_detection_total",
        {"pattern": "ROUTINE_CHANGED", "result": "not_detected"},
    ) == 1
    assert registry.get_sample_value(
        "nilm_pattern_detection_duration_seconds_count",
        {"pattern": "ROUTINE_CHANGED"},
    ) == 2
    assert registry.get_sample_value(
        "nilm_pattern_events_total",
        {"event_type": "ROUTINE_CHANGED"},
    ) == 1
