from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

import pytest
from prometheus_client import CollectorRegistry
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from realtime_analysis.anomaly_detector import (
    RealtimeAnomalyDetector,
    SqlAlchemyEventDetectionRepository,
)
from realtime_analysis.database import Base
from realtime_analysis.event_emission_repository import (
    SqlAlchemyEventEmissionRepository,
)
from realtime_analysis.metrics import AnalysisMetrics
from realtime_analysis.models import (
    AnalysisEventEmission,
    ApplianceUsageSession,
    HouseholdActivityDaily,
    HouseholdObservationDaily,
)
from realtime_analysis.outing_repository import SqlAlchemyOutingStateRepository
from realtime_analysis.policy import SqlAlchemyPolicyRepository
from realtime_analysis.schemas import (
    AnalysisPolicyDefinition,
    OutingEvent,
    RoutineBaseline,
)


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


def policies(session_factory: sessionmaker[Session]) -> SqlAlchemyPolicyRepository:
    repository = SqlAlchemyPolicyRepository(session_factory)
    repository.seed_missing(
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
                    "sleep_window": {
                        "start": "23:00",
                        "end": "07:00",
                    },
                    "evaluation_interval_seconds": 60,
                },
                event_type="PROLONGED_INACTIVITY",
                cooldown_hours=12,
            ),
            AnalysisPolicyDefinition(
                policy_code="PROLONGED_APPLIANCE_USE_DEFAULT",
                algorithm_type="OPEN_SESSION_DURATION",
                parameters={
                    "limits_minutes": {"INDUCTION": 120, "IRON": 60},
                    "evaluation_interval_seconds": 60,
                },
                event_type="PROLONGED_APPLIANCE_USE",
                cooldown_hours=24,
            ),
        ]
    )
    return repository


def detector(
    session_factory: sessionmaker[Session],
    metrics: AnalysisMetrics | None = None,
) -> RealtimeAnomalyDetector:
    return RealtimeAnomalyDetector(
        repository=SqlAlchemyEventDetectionRepository(
            session_factory,
            "Asia/Seoul",
        ),
        policy_repository=policies(session_factory),
        timezone_name="Asia/Seoul",
        emission_repository=SqlAlchemyEventEmissionRepository(session_factory),
        outing_state_provider=SqlAlchemyOutingStateRepository(session_factory),
        metrics=metrics or AnalysisMetrics(CollectorRegistry()),
    )


def baseline() -> RoutineBaseline:
    return RoutineBaseline(
        id=UUID("226dfc19-3222-49a4-8fde-a314573aa23e"),
        household_id="H001",
        appliance_type="MICROWAVE",
        expected_until="08:10",
        normal_days=12,
        window_days=14,
    )


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


def add_usage(
    session: Session,
    observation: HouseholdObservationDaily,
    appliance_type: str,
    started_at: datetime,
    ended_at: datetime | None,
) -> ApplianceUsageSession:
    stored_start = started_at.astimezone(timezone.utc)
    stored_end = ended_at.astimezone(timezone.utc) if ended_at is not None else None
    activity = HouseholdActivityDaily(
        observation_daily_id=observation.id,
        appliance_type=appliance_type,
        event_count=1,
        updated_at=stored_start,
    )
    session.add(activity)
    session.flush()
    usage = ApplianceUsageSession(
        activity_daily_id=activity.id,
        started_at=stored_start,
        ended_at=stored_end,
        max_probability=Decimal("0.9000"),
        decision_threshold=Decimal("0.5000"),
        updated_at=stored_end or stored_start,
    )
    session.add(usage)
    session.flush()
    return usage


def apply_outing_event(
    session_factory: sessionmaker[Session],
    event_id: UUID,
    event_type: str,
    occurred_at: datetime,
) -> None:
    SqlAlchemyOutingStateRepository(session_factory).apply_event(
        OutingEvent.model_validate(
            {
                "event_id": event_id,
                "household_id": "H001",
                "event_type": event_type,
                "occurred_at": occurred_at,
            }
        ),
        updated_at=occurred_at,
    )


def test_routine_missed_is_restart_stable_and_emitted_once(
    session_factory: sessionmaker[Session],
) -> None:
    observed_at = datetime.fromisoformat("2026-09-17T09:00:00+09:00")
    with session_factory.begin() as session:
        add_observation(session, observed_at.date())

    first_detector = detector(session_factory)
    first = first_detector.detect("H001", observed_at, [baseline()])
    assert len(first) == 1
    assert first[0].event.event_type == "ROUTINE_MISSED"
    assert first[0].event.reason["appliance_type"] == "MICROWAVE"
    first_detector.mark_emitted(first[0])

    assert first_detector.detect(
        "H001",
        observed_at + timedelta(minutes=1),
        [baseline()],
    ) == []

    restarted = detector(session_factory)
    after_restart = restarted.detect("H001", observed_at, [baseline()])
    assert after_restart == []


def test_routine_missed_skips_already_emitted_daily_candidate_early(
    session_factory: sessionmaker[Session],
) -> None:
    observed_at = datetime.fromisoformat("2026-09-17T09:00:00+09:00")
    with session_factory.begin() as session:
        add_observation(session, observed_at.date())

    registry = CollectorRegistry()
    metrics = AnalysisMetrics(registry)
    active_detector = detector(session_factory, metrics)

    first = active_detector.detect("H001", observed_at, [baseline()])
    assert len(first) == 1
    active_detector.mark_emitted(first[0])

    assert active_detector.detect(
        "H001",
        observed_at + timedelta(minutes=1),
        [baseline()],
    ) == []
    assert registry.get_sample_value(
        "nilm_pattern_detection_total",
        {"pattern": "ROUTINE_MISSED", "result": "detected"},
    ) == 1
    assert registry.get_sample_value(
        "nilm_pattern_detection_total",
        {"pattern": "ROUTINE_MISSED", "result": "not_detected"},
    ) == 0
    assert registry.get_sample_value(
        "nilm_pattern_detection_duration_seconds_count",
        {"pattern": "ROUTINE_MISSED"},
    ) == 1


def test_routine_missed_uses_persisted_session_before_deadline(
    session_factory: sessionmaker[Session],
) -> None:
    observed_at = datetime.fromisoformat("2026-09-17T09:00:00+09:00")
    used_at = datetime.fromisoformat("2026-09-17T08:00:00+09:00")
    with session_factory.begin() as session:
        observation = add_observation(session, observed_at.date())
        add_usage(
            session,
            observation,
            "MICROWAVE",
            used_at,
            used_at + timedelta(minutes=5),
        )

    assert detector(session_factory).detect(
        "H001",
        observed_at,
        [baseline()],
    ) == []


def test_routine_missed_excludes_only_appliance_whose_window_overlaps_outing(
    session_factory: sessionmaker[Session],
) -> None:
    observed_at = datetime.fromisoformat("2026-09-17T09:10:00+09:00")
    with session_factory.begin() as session:
        add_observation(session, observed_at.date())
    apply_outing_event(
        session_factory,
        UUID("da898d82-3183-4d12-ae7c-0e3a9df9772c"),
        "OUTING_STARTED",
        datetime.fromisoformat("2026-09-17T08:30:00+09:00"),
    )
    apply_outing_event(
        session_factory,
        UUID("160a931f-f59d-403d-8157-fccdeeb88dbe"),
        "OUTING_ENDED",
        datetime.fromisoformat("2026-09-17T08:45:00+09:00"),
    )
    microwave = baseline()
    kettle = RoutineBaseline(
        id=UUID("786b1748-2e0a-4f22-aa72-146ee39e6148"),
        household_id="H001",
        appliance_type="KETTLE",
        expected_until="09:00",
        normal_days=12,
        window_days=14,
    )

    events = detector(session_factory).detect(
        "H001",
        observed_at,
        [microwave, kettle],
    )

    routine_missed = [
        item.event
        for item in events
        if item.event.event_type == "ROUTINE_MISSED"
    ]
    assert [event.reason["appliance_type"] for event in routine_missed] == [
        "MICROWAVE"
    ]


def test_prolonged_inactivity_uses_last_completed_activity(
    session_factory: sessionmaker[Session],
) -> None:
    ended_at = datetime.fromisoformat("2026-09-16T20:00:00+09:00")
    observed_at = datetime.fromisoformat("2026-09-17T10:00:00+09:00")
    with session_factory.begin() as session:
        add_observation(session, observed_at.date())
        old_observation = add_observation(session, ended_at.date())
        add_usage(
            session,
            old_observation,
            "MICROWAVE",
            ended_at - timedelta(minutes=5),
            ended_at,
        )

    active_detector = detector(session_factory)
    events = active_detector.detect("H001", observed_at, [])

    assert [item.event.event_type for item in events] == [
        "PROLONGED_INACTIVITY"
    ]
    assert events[0].event.occurred_at == observed_at.astimezone(timezone.utc)
    assert events[0].event.reason["threshold_hours"] == 6.0
    assert events[0].event.reason["sleep_window"] == {
        "start": "23:00",
        "end": "07:00",
    }


def test_prolonged_inactivity_does_not_count_sleep_window(
    session_factory: sessionmaker[Session],
) -> None:
    ended_at = datetime.fromisoformat("2026-09-16T20:00:00+09:00")
    observed_at = datetime.fromisoformat("2026-09-17T09:59:00+09:00")
    with session_factory.begin() as session:
        add_observation(session, observed_at.date())
        old_observation = add_observation(session, ended_at.date())
        add_usage(
            session,
            old_observation,
            "MICROWAVE",
            ended_at - timedelta(minutes=5),
            ended_at,
        )

    assert detector(session_factory).detect("H001", observed_at, []) == []


def test_prolonged_inactivity_starts_counting_after_sleep_for_night_activity(
    session_factory: sessionmaker[Session],
) -> None:
    ended_at = datetime.fromisoformat("2026-09-17T01:00:00+09:00")
    observed_at = datetime.fromisoformat("2026-09-17T13:00:00+09:00")
    with session_factory.begin() as session:
        observation = add_observation(session, observed_at.date())
        add_usage(
            session,
            observation,
            "MICROWAVE",
            ended_at - timedelta(minutes=5),
            ended_at,
        )

    events = detector(session_factory).detect("H001", observed_at, [])

    assert [item.event.event_type for item in events] == [
        "PROLONGED_INACTIVITY"
    ]
    assert events[0].event.occurred_at == observed_at.astimezone(timezone.utc)


def test_prolonged_inactivity_is_suspended_while_household_is_outing(
    session_factory: sessionmaker[Session],
) -> None:
    ended_at = datetime.fromisoformat("2026-09-16T20:00:00+09:00")
    observed_at = datetime.fromisoformat("2026-09-17T15:00:00+09:00")
    with session_factory.begin() as session:
        add_observation(session, observed_at.date())
        old_observation = add_observation(session, ended_at.date())
        add_usage(
            session,
            old_observation,
            "MICROWAVE",
            ended_at - timedelta(minutes=5),
            ended_at,
        )
    apply_outing_event(
        session_factory,
        UUID("71aa1792-16cb-45aa-83ae-a1a3f182ac43"),
        "OUTING_STARTED",
        datetime.fromisoformat("2026-09-17T08:00:00+09:00"),
    )

    assert detector(session_factory).detect("H001", observed_at, []) == []


def test_prolonged_inactivity_restarts_at_return_and_excludes_sleep(
    session_factory: sessionmaker[Session],
) -> None:
    last_activity_at = datetime.fromisoformat("2026-09-16T20:00:00+09:00")
    returned_at = datetime.fromisoformat("2026-09-16T22:00:00+09:00")
    before_threshold = datetime.fromisoformat("2026-09-17T11:59:00+09:00")
    at_threshold = datetime.fromisoformat("2026-09-17T12:00:00+09:00")
    with session_factory.begin() as session:
        add_observation(session, at_threshold.date())
        old_observation = add_observation(session, last_activity_at.date())
        add_usage(
            session,
            old_observation,
            "MICROWAVE",
            last_activity_at - timedelta(minutes=5),
            last_activity_at,
        )
    apply_outing_event(
        session_factory,
        UUID("28cd358d-31f8-446a-9a6a-e8d69d729a53"),
        "OUTING_ENDED",
        returned_at,
    )

    active_detector = detector(session_factory)
    assert active_detector.detect("H001", before_threshold, []) == []

    events = active_detector.detect("H001", at_threshold, [])

    assert [item.event.event_type for item in events] == [
        "PROLONGED_INACTIVITY"
    ]
    event = events[0].event
    assert event.occurred_at == at_threshold.astimezone(timezone.utc)
    assert event.reason["last_activity_at"] == last_activity_at.astimezone(
        timezone.utc
    ).isoformat()
    assert event.reason["last_returned_at"] == returned_at.astimezone(
        timezone.utc
    ).isoformat()
    assert event.reason["inactivity_started_at"] == returned_at.astimezone(
        timezone.utc
    ).isoformat()


def test_prolonged_inactivity_can_start_from_return_without_prior_activity(
    session_factory: sessionmaker[Session],
) -> None:
    returned_at = datetime.fromisoformat("2026-09-17T08:00:00+09:00")
    observed_at = datetime.fromisoformat("2026-09-17T14:00:00+09:00")
    with session_factory.begin() as session:
        add_observation(session, observed_at.date())
    apply_outing_event(
        session_factory,
        UUID("12efcaa0-5aae-47d6-8088-a8cbfd62df0c"),
        "OUTING_ENDED",
        returned_at,
    )

    events = detector(session_factory).detect("H001", observed_at, [])

    assert [item.event.event_type for item in events] == [
        "PROLONGED_INACTIVITY"
    ]
    assert events[0].event.reason["last_activity_at"] is None
    assert events[0].event.reason["inactivity_started_at"] == (
        returned_at.astimezone(timezone.utc).isoformat()
    )


def test_prolonged_appliance_use_is_emitted_once_per_open_session(
    session_factory: sessionmaker[Session],
) -> None:
    observed_at = datetime.fromisoformat("2026-09-17T09:00:00+09:00")
    started_at = observed_at - timedelta(hours=3)
    with session_factory.begin() as session:
        observation = add_observation(session, observed_at.date())
        add_usage(
            session,
            observation,
            "INDUCTION",
            started_at,
            None,
        )

    active_detector = detector(session_factory)
    events = active_detector.detect("H001", observed_at, [])

    assert [item.event.event_type for item in events] == [
        "PROLONGED_APPLIANCE_USE"
    ]
    assert events[0].event.reason["appliance_type"] == "INDUCTION"
    active_detector.mark_emitted(events[0])
    assert active_detector.detect(
        "H001",
        observed_at + timedelta(minutes=1),
        [],
    ) == []
    restarted = detector(session_factory).detect("H001", observed_at, [])
    assert restarted == []


def test_persisted_cooldown_survives_detector_restart(
    session_factory: sessionmaker[Session],
) -> None:
    observed_at = datetime.fromisoformat("2026-09-17T09:00:00+09:00")
    with session_factory.begin() as session:
        add_observation(session, observed_at.date())

    emissions = SqlAlchemyEventEmissionRepository(session_factory)
    emissions.record_emission(
        event_id=UUID("dd070da7-0eb8-5915-91aa-a7956fca5eb5"),
        household_id="H001",
        event_type="ROUTINE_MISSED",
        appliance_type="MICROWAVE",
        emitted_at=observed_at.astimezone(timezone.utc) - timedelta(hours=1),
    )

    restarted = detector(session_factory)

    assert restarted.detect("H001", observed_at, [baseline()]) == []


def test_same_event_id_reprocessing_does_not_duplicate_emission(
    session_factory: sessionmaker[Session],
) -> None:
    observed_at = datetime.fromisoformat("2026-09-17T09:00:00+09:00")
    with session_factory.begin() as session:
        add_observation(session, observed_at.date())

    active_detector = detector(session_factory)
    events = active_detector.detect("H001", observed_at, [baseline()])
    assert len(events) == 1

    active_detector.mark_emitted(events[0])
    active_detector.mark_emitted(events[0])

    with session_factory() as session:
        emissions = session.scalars(select(AnalysisEventEmission)).all()
        assert len(emissions) == 1
        assert emissions[0].event_id == events[0].event.event_id
    assert detector(session_factory).detect(
        "H001",
        observed_at,
        [baseline()],
    ) == []


def test_reset_allows_household_pattern_evaluation_to_resume(
    session_factory: sessionmaker[Session],
) -> None:
    observed_at = datetime.fromisoformat("2026-09-17T09:00:00+09:00")
    with session_factory.begin() as session:
        add_observation(session, observed_at.date())
    active_detector = detector(session_factory)

    assert active_detector.detect("H001", observed_at, [baseline()])
    assert active_detector.detect(
        "H001",
        observed_at + timedelta(seconds=1),
        [baseline()],
    ) == []

    active_detector.reset("H001")

    assert active_detector.detect(
        "H001",
        observed_at + timedelta(seconds=1),
        [baseline()],
    )


def test_realtime_detector_records_each_pattern_evaluation(
    session_factory: sessionmaker[Session],
) -> None:
    observed_at = datetime.fromisoformat("2026-09-17T09:00:00+09:00")
    with session_factory.begin() as session:
        add_observation(session, observed_at.date())

    registry = CollectorRegistry()
    metrics = AnalysisMetrics(registry)

    events = detector(session_factory, metrics).detect(
        "H001",
        observed_at,
        [baseline()],
    )

    assert [item.event.event_type for item in events] == ["ROUTINE_MISSED"]
    assert registry.get_sample_value(
        "nilm_pattern_detection_total",
        {"pattern": "ROUTINE_MISSED", "result": "detected"},
    ) == 1
    for pattern in ("ROUTINE_MISSED", "PROLONGED_INACTIVITY", "PROLONGED_APPLIANCE_USE"):
        assert registry.get_sample_value(
            "nilm_pattern_detection_duration_seconds_count",
            {"pattern": pattern},
        ) == 1
