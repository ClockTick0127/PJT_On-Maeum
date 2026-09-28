import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import Mock
from uuid import UUID

import pytest
from prometheus_client import CollectorRegistry
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from realtime_analysis.anomaly_detector import (
    RealtimeAnomalyDetector,
    SqlAlchemyEventDetectionRepository,
)
from realtime_analysis.config import Settings
from realtime_analysis.database import Base
from realtime_analysis.event_emission_repository import (
    SqlAlchemyEventEmissionRepository,
)
from realtime_analysis.metrics import AnalysisMetrics
from realtime_analysis.models import (
    ApplianceUsageSession,
    HouseholdActivityDaily,
    HouseholdObservationDaily,
)
from realtime_analysis.outing_consumer import OutingEventConsumer
from realtime_analysis.outing_repository import SqlAlchemyOutingStateRepository
from realtime_analysis.policy import SqlAlchemyPolicyRepository
from realtime_analysis.schemas import AnalysisPolicyDefinition, RoutineBaseline


@pytest.fixture
def session_factory() -> sessionmaker[Session]:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    for table_name in (
        "analysis_policy",
        "analysis_event_emission",
        "household_outing_state",
        "household_observation_daily",
        "household_activity_daily",
        "appliance_usage_session",
    ):
        Base.metadata.tables[table_name].create(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


def build_detector(
    session_factory: sessionmaker[Session],
    outing_repository: SqlAlchemyOutingStateRepository,
) -> RealtimeAnomalyDetector:
    policies = SqlAlchemyPolicyRepository(session_factory)
    policies.seed_missing(
        [
            AnalysisPolicyDefinition(
                policy_code="ROUTINE_MISSED_DEFAULT",
                algorithm_type="EXPECTED_USE_DEADLINE",
                parameters={
                    "minimum_baseline_strength": 80,
                    "evaluation_interval_seconds": 60,
                },
                event_type="ROUTINE_MISSED",
                cooldown_hours=24,
            ),
            AnalysisPolicyDefinition(
                policy_code="PROLONGED_INACTIVITY_DEFAULT",
                algorithm_type="AWAKE_INACTIVITY_ELAPSED",
                parameters={
                    "inactivity_hours": 6,
                    "sleep_window": {"start": "23:00", "end": "07:00"},
                    "evaluation_interval_seconds": 60,
                },
                event_type="PROLONGED_INACTIVITY",
                cooldown_hours=12,
            ),
        ]
    )
    return RealtimeAnomalyDetector(
        repository=SqlAlchemyEventDetectionRepository(
            session_factory,
            "Asia/Seoul",
        ),
        policy_repository=policies,
        timezone_name="Asia/Seoul",
        emission_repository=SqlAlchemyEventEmissionRepository(session_factory),
        outing_state_provider=outing_repository,
        metrics=AnalysisMetrics(CollectorRegistry()),
    )


def build_outing_consumer(
    repository: SqlAlchemyOutingStateRepository,
) -> tuple[OutingEventConsumer, Mock, Mock]:
    kafka_consumer = Mock()
    dlq = Mock()
    consumer = OutingEventConsumer(
        settings=Settings(_env_file=None),
        repository=repository,
        dlq_publisher=dlq,
        consumer=kafka_consumer,
    )
    return consumer, kafka_consumer, dlq


def process_outing_event(
    consumer: OutingEventConsumer,
    event_id: str,
    event_type: str,
    occurred_at: str,
) -> None:
    message = Mock()
    message.value.return_value = json.dumps(
        {
            "event_id": event_id,
            "household_id": "H001",
            "event_type": event_type,
            "occurred_at": occurred_at,
        }
    ).encode("utf-8")
    consumer.process_message(message)


def add_observation(
    session: Session,
    observation_date: date,
) -> HouseholdObservationDaily:
    observation = HouseholdObservationDaily(
        household_id="H001",
        observation_date=observation_date,
        sample_count=1,
        expected_sample_count=86_400,
        coverage_ratio=Decimal("0.0001"),
        observation_status="COLLECTING",
        updated_at=datetime.now(timezone.utc),
    )
    session.add(observation)
    session.flush()
    return observation


def add_completed_usage(
    session: Session,
    observation: HouseholdObservationDaily,
    ended_at: datetime,
) -> None:
    ended_at_utc = ended_at.astimezone(timezone.utc)
    activity = HouseholdActivityDaily(
        observation_daily_id=observation.id,
        appliance_type="MICROWAVE",
        event_count=1,
        updated_at=ended_at_utc,
    )
    session.add(activity)
    session.flush()
    session.add(
        ApplianceUsageSession(
            activity_daily_id=activity.id,
            started_at=ended_at_utc - timedelta(minutes=5),
            ended_at=ended_at_utc,
            max_probability=Decimal("0.9000"),
            decision_threshold=Decimal("0.5000"),
            updated_at=ended_at_utc,
        )
    )


def test_kafka_latest_outing_excludes_only_overlapping_routine(
    session_factory: sessionmaker[Session],
) -> None:
    outing_repository = SqlAlchemyOutingStateRepository(session_factory)
    outing_consumer, kafka_consumer, dlq = build_outing_consumer(
        outing_repository
    )
    observed_at = datetime.fromisoformat("2026-09-17T09:10:00+09:00")
    with session_factory.begin() as session:
        add_observation(session, observed_at.date())

    process_outing_event(
        outing_consumer,
        "da898d82-3183-4d12-ae7c-0e3a9df9772c",
        "OUTING_STARTED",
        "2026-09-17T08:30:00+09:00",
    )
    process_outing_event(
        outing_consumer,
        "160a931f-f59d-403d-8157-fccdeeb88dbe",
        "OUTING_ENDED",
        "2026-09-17T08:45:00+09:00",
    )

    detector = build_detector(session_factory, outing_repository)
    events = detector.detect(
        "H001",
        observed_at,
        [
            RoutineBaseline(
                id=UUID("226dfc19-3222-49a4-8fde-a314573aa23e"),
                household_id="H001",
                appliance_type="MICROWAVE",
                expected_until="08:10",
                normal_days=12,
                window_days=14,
            ),
            RoutineBaseline(
                id=UUID("786b1748-2e0a-4f22-aa72-146ee39e6148"),
                household_id="H001",
                appliance_type="KETTLE",
                expected_until="09:00",
                normal_days=12,
                window_days=14,
            ),
        ],
    )

    routine_events = [
        item.event
        for item in events
        if item.event.event_type == "ROUTINE_MISSED"
    ]
    assert [event.reason["appliance_type"] for event in routine_events] == [
        "MICROWAVE"
    ]
    assert kafka_consumer.store_offsets.call_count == 2
    kafka_consumer.commit.assert_not_called()
    dlq.publish.assert_not_called()


def test_kafka_outing_state_suspends_and_restarts_awake_inactivity(
    session_factory: sessionmaker[Session],
) -> None:
    outing_repository = SqlAlchemyOutingStateRepository(session_factory)
    outing_consumer, kafka_consumer, dlq = build_outing_consumer(
        outing_repository
    )
    last_activity_at = datetime.fromisoformat("2026-09-16T10:00:00+09:00")
    while_outing = datetime.fromisoformat("2026-09-16T21:00:00+09:00")
    returned_at = datetime.fromisoformat("2026-09-16T22:00:00+09:00")
    before_threshold = datetime.fromisoformat("2026-09-17T11:59:00+09:00")
    at_threshold = datetime.fromisoformat("2026-09-17T12:00:00+09:00")
    with session_factory.begin() as session:
        first_day = add_observation(session, last_activity_at.date())
        add_completed_usage(session, first_day, last_activity_at)
        add_observation(session, at_threshold.date())

    process_outing_event(
        outing_consumer,
        "c88be83b-2ac8-47d1-b644-193874289a79",
        "OUTING_STARTED",
        "2026-09-16T17:00:00+09:00",
    )
    detector = build_detector(session_factory, outing_repository)
    assert detector.detect("H001", while_outing, []) == []

    process_outing_event(
        outing_consumer,
        "3dd2e0c8-2832-4dcb-b71d-73007d834827",
        "OUTING_ENDED",
        returned_at.isoformat(),
    )
    assert detector.detect("H001", before_threshold, []) == []

    events = detector.detect("H001", at_threshold, [])

    inactivity_events = [
        item.event
        for item in events
        if item.event.event_type == "PROLONGED_INACTIVITY"
    ]
    assert len(inactivity_events) == 1
    event = inactivity_events[0]
    assert event.occurred_at == at_threshold.astimezone(timezone.utc)
    assert event.reason["inactivity_started_at"] == returned_at.astimezone(
        timezone.utc
    ).isoformat()
    assert event.reason["sleep_window"] == {
        "start": "23:00",
        "end": "07:00",
    }
    assert kafka_consumer.store_offsets.call_count == 2
    kafka_consumer.commit.assert_not_called()
    dlq.publish.assert_not_called()
