import json
import logging
from datetime import datetime, timedelta
from unittest.mock import Mock
from uuid import UUID

import pytest
from prometheus_client import CollectorRegistry

from realtime_analysis.anomaly_detector import (
    PendingAnomaly,
    RoutineMissedDetector,
)
from realtime_analysis.baseline import BaselineRepository
from realtime_analysis.buffer import HouseholdBuffer
from realtime_analysis.data_quality_monitor import DataQualityMonitor
from realtime_analysis.handler import MeasurementHandler
from realtime_analysis.metrics import AnalysisMetrics
from realtime_analysis.pipeline_timing import pipeline_timing
from realtime_analysis.predictor import APPLIANCE_ORDER, FakePredictor
from realtime_analysis.schemas import (
    AnalysisEvent,
    AnalysisSnapshot,
    DataQualityEvent,
    PowerMeasurement,
    RoutineBaseline,
)
from realtime_analysis.state_tracker import DailyActivityTracker
from realtime_analysis.state_decider import ApplianceStateDecider
from realtime_analysis.state_transition import ApplianceStateTransitionDetector


class RecordingPublisher:
    def __init__(self) -> None:
        self.events: list[AnalysisEvent] = []

    def publish(self, event: AnalysisEvent) -> None:
        self.events.append(event)


class RecordingSnapshotPublisher:
    def __init__(self) -> None:
        self.snapshots: list[AnalysisSnapshot] = []

    def publish(self, snapshot: AnalysisSnapshot) -> None:
        self.snapshots.append(snapshot)


class RecordingDataQualityPublisher:
    def __init__(self) -> None:
        self.events: list[DataQualityEvent] = []

    def publish(self, event: DataQualityEvent) -> None:
        self.events.append(event)


def test_fake_predictor_uses_ai_experiment_appliance_order() -> None:
    predictions = FakePredictor(("KETTLE", "VACUUM_CLEANER")).predict(
        [(100.0, 10.0, 0.98, 0.5)]
    )

    assert APPLIANCE_ORDER == (
        "KETTLE",
        "INDUCTION",
        "IRON",
        "MICROWAVE",
        "HAIR_DRYER",
        "VACUUM_CLEANER",
    )
    assert [
        prediction.appliance_type for prediction in predictions
    ] == list(APPLIANCE_ORDER)
    assert {
        prediction.appliance_type
        for prediction in predictions
        if prediction.probability == 1.0
    } == {"KETTLE", "VACUUM_CLEANER"}


def measurement(second: int) -> PowerMeasurement:
    return PowerMeasurement(
        message_id=UUID(int=second + 1),
        household_id="H001",
        device_id="main",
        measured_at=datetime.fromisoformat(
            f"2026-09-08T09:00:{second:02d}+09:00"
        ),
        active_power=100.0,
        reactive_power=10.0,
        power_factor=0.98,
        current=0.5,
    )


def test_pipeline_publishes_event_after_buffer_is_ready() -> None:
    tracker = DailyActivityTracker()
    publisher = RecordingPublisher()
    snapshot_publisher = RecordingSnapshotPublisher()
    activity_repository = Mock()
    registry = CollectorRegistry()
    metrics = AnalysisMetrics(registry)
    handler = MeasurementHandler(
        buffer=HouseholdBuffer(window_size=3),
        predictor=FakePredictor(),   # 지금은 FakePredictor 사용
        state_decider=ApplianceStateDecider(
            {appliance_type: 0.5 for appliance_type in APPLIANCE_ORDER}
        ),
        state_transition_detector=ApplianceStateTransitionDetector(3, 3, 0.05),
        activity_repository=activity_repository,  # 단위 테스트에서는 실제 DB 저장을 대체
        baseline_repository=BaselineRepository(
            [
                RoutineBaseline(
                    id=UUID("226dfc19-3222-49a4-8fde-a314573aa23e"),
                    household_id="H001",
                    appliance_type="MICROWAVE",
                    expected_until="08:10",
                    normal_days=12,
                    window_days=14,
                )
            ]
        ),
        tracker=tracker,
        detector=RoutineMissedDetector(tracker, 80, "Asia/Seoul"),
        event_publisher=publisher,  # type: ignore[arg-type]
        snapshot_publisher=snapshot_publisher,  # type: ignore[arg-type]
        timezone_name="Asia/Seoul",
        metrics=metrics,
    )

    handler(measurement(0))
    handler(measurement(1))
    assert publisher.events == []

    handler(measurement(2))
    handler(measurement(3))

    assert len(publisher.events) == 1
    assert publisher.events[0].household_id == "H001"
    assert publisher.events[0].event_type == "ROUTINE_MISSED"
    # 모델 버퍼 준비 여부와 관계없이 검증된 원본 샘플은 모두 관측 집계로 전달한다.
    assert activity_repository.record_observation.call_count == 4
    assert len(snapshot_publisher.snapshots) == 2
    assert snapshot_publisher.snapshots[0].snapshot_id == measurement(2).message_id
    assert registry.get_sample_value(
        "nilm_pattern_events_total",
        {"event_type": "ROUTINE_MISSED"},
    ) == 1


def test_pipeline_records_predictor_duration_as_inference_stage(caplog) -> None:
    observed = measurement(0)
    handler = MeasurementHandler(
        buffer=HouseholdBuffer(window_size=1),
        predictor=FakePredictor(),
        state_decider=ApplianceStateDecider(
            {appliance_type: 0.5 for appliance_type in APPLIANCE_ORDER}
        ),
        state_transition_detector=ApplianceStateTransitionDetector(3, 3, 0.05),
        activity_repository=Mock(),
        baseline_repository=BaselineRepository([]),
        tracker=DailyActivityTracker(),
        detector=RoutineMissedDetector(DailyActivityTracker(), 80, "Asia/Seoul"),
        event_publisher=RecordingPublisher(),  # type: ignore[arg-type]
        snapshot_publisher=RecordingSnapshotPublisher(),  # type: ignore[arg-type]
        timezone_name="Asia/Seoul",
    )

    with caplog.at_level(
        logging.INFO,
        logger="realtime_analysis.pipeline_timing",
    ):
        with pipeline_timing(
            kafka_topic="power.raw.v1",
            kafka_partition=0,
            kafka_offset=1,
        ) as timer:
            timer.bind_measurement(observed)
            handler(observed)
            timer.mark("processed")

    records = [
        json.loads(record.message)
        for record in caplog.records
        if record.name == "realtime_analysis.pipeline_timing"
    ]
    assert len(records) == 1
    assert records[0]["stage_counts"]["inference"] == 1
    assert records[0]["stage_durations_ns"]["inference"] >= 0


def test_pipeline_does_not_mark_event_when_kafka_publish_fails() -> None:
    observed = measurement(0)
    pending = PendingAnomaly(
        event=AnalysisEvent(
            event_id=UUID("34c12866-8329-51e3-9e22-57c326f0a395"),
            household_id="H001",
            event_type="PROLONGED_INACTIVITY",
            occurred_at=observed.measured_at,
            reason={},
        ),
        activity_date=observed.measured_at.date(),
        appliance_type="*",
        baseline_type="PROLONGED_INACTIVITY",
    )
    detector = Mock()
    detector.detect.return_value = [pending]
    publisher = Mock()
    publisher.publish.side_effect = RuntimeError("Kafka publish failed")
    handler = MeasurementHandler(
        buffer=HouseholdBuffer(window_size=1),
        predictor=FakePredictor(),
        state_decider=ApplianceStateDecider(
            {appliance_type: 0.5 for appliance_type in APPLIANCE_ORDER}
        ),
        state_transition_detector=ApplianceStateTransitionDetector(3, 3, 0.05),
        activity_repository=Mock(),
        baseline_repository=BaselineRepository([]),
        tracker=DailyActivityTracker(),
        detector=detector,
        event_publisher=publisher,
        snapshot_publisher=RecordingSnapshotPublisher(),
        timezone_name="Asia/Seoul",
    )

    with pytest.raises(RuntimeError, match="Kafka publish failed"):
        handler(observed)

    publisher.publish.assert_called_once_with(pending.event)
    detector.mark_emitted.assert_not_called()


def test_pipeline_records_usage_only_after_confirmed_on_transition() -> None:
    tracker = DailyActivityTracker()
    snapshot_publisher = RecordingSnapshotPublisher()
    handler = MeasurementHandler(
        buffer=HouseholdBuffer(window_size=1),
        predictor=FakePredictor(("MICROWAVE",)),
        state_decider=ApplianceStateDecider(
            {appliance_type: 0.5 for appliance_type in APPLIANCE_ORDER}
        ),
        state_transition_detector=ApplianceStateTransitionDetector(3, 3, 0.05),
        activity_repository=Mock(),  # 단위 테스트에서는 실제 DB 저장을 대체
        baseline_repository=BaselineRepository([]),
        tracker=tracker,
        detector=RoutineMissedDetector(tracker, 80, "Asia/Seoul"),
        event_publisher=RecordingPublisher(),  # type: ignore[arg-type]
        snapshot_publisher=snapshot_publisher,  # type: ignore[arg-type]
        timezone_name="Asia/Seoul",
    )

    handler(measurement(0))
    handler(measurement(1))
    assert tracker.was_used("H001", measurement(1).measured_at.date(), "MICROWAVE") is False

    handler(measurement(2))

    assert tracker.was_used("H001", measurement(2).measured_at.date(), "MICROWAVE") is True
    microwave_states = [
        next(
            appliance
            for appliance in snapshot.appliances
            if appliance.appliance_type == "MICROWAVE"
        ).is_on
        for snapshot in snapshot_publisher.snapshots
    ]
    assert microwave_states == [False, False, True]


def test_pipeline_refills_model_buffer_before_processing_recovered_data() -> None:
    clock_value = datetime.fromisoformat("2026-09-08T09:00:00+09:00")

    def clock() -> datetime:
        return clock_value

    quality_publisher = RecordingDataQualityPublisher()
    quality_monitor = DataQualityMonitor(
        quality_publisher,
        gap_threshold_seconds=120,
        recovery_confirmation_samples=2,
        clock=clock,
    )
    snapshots = RecordingSnapshotPublisher()
    handler = MeasurementHandler(
        buffer=HouseholdBuffer(window_size=2),
        predictor=FakePredictor(),
        state_decider=ApplianceStateDecider(
            {appliance_type: 0.5 for appliance_type in APPLIANCE_ORDER}
        ),
        state_transition_detector=ApplianceStateTransitionDetector(3, 3, 0.05),
        activity_repository=Mock(),
        baseline_repository=BaselineRepository([]),
        tracker=DailyActivityTracker(),
        detector=RoutineMissedDetector(DailyActivityTracker(), 80, "Asia/Seoul"),
        event_publisher=RecordingPublisher(),  # type: ignore[arg-type]
        snapshot_publisher=snapshots,  # type: ignore[arg-type]
        timezone_name="Asia/Seoul",
        data_quality_monitor=quality_monitor,
    )

    handler(measurement(0))
    clock_value += timedelta(seconds=1)
    handler(measurement(1))
    assert len(snapshots.snapshots) == 1

    clock_value += timedelta(seconds=120)
    assert quality_monitor.detect_gaps(checked_at=clock_value) == 1

    clock_value += timedelta(seconds=1)
    first_recovery = measurement(2).model_copy(
        update={"measured_at": measurement(2).measured_at + timedelta(minutes=3)}
    )
    handler(first_recovery)
    assert len(snapshots.snapshots) == 1

    clock_value += timedelta(seconds=1)
    second_recovery = measurement(3).model_copy(
        update={"measured_at": measurement(3).measured_at + timedelta(minutes=3)}
    )
    handler(second_recovery)

    assert len(snapshots.snapshots) == 2
    assert [event.event_type for event in quality_publisher.events] == [
        "DATA_GAP",
        "DATA_RECOVERED",
    ]


def test_partition_revoke_reset_requires_target_household_to_refill_buffer() -> None:
    buffer = HouseholdBuffer(window_size=299)
    for household_id in ("H001", "H002"):
        for _ in range(299):
            buffer.append(
                measurement(0).model_copy(
                    update={"household_id": household_id}
                )
            )
    transition_detector = Mock()
    quality_monitor = Mock()
    detector = Mock()
    handler = MeasurementHandler(
        buffer=buffer,
        predictor=FakePredictor(),
        state_decider=ApplianceStateDecider(
            {appliance_type: 0.5 for appliance_type in APPLIANCE_ORDER}
        ),
        state_transition_detector=transition_detector,
        activity_repository=Mock(),
        baseline_repository=BaselineRepository([]),
        tracker=DailyActivityTracker(),
        detector=detector,
        event_publisher=RecordingPublisher(),  # type: ignore[arg-type]
        snapshot_publisher=RecordingSnapshotPublisher(),  # type: ignore[arg-type]
        timezone_name="Asia/Seoul",
        data_quality_monitor=quality_monitor,
    )

    handler.reset_household("H001")

    assert buffer.is_ready("H001") is False
    assert buffer.is_ready("H002") is True
    transition_detector.reset.assert_called_once_with("H001")
    quality_monitor.reset.assert_called_once_with("H001")
    detector.reset.assert_called_once_with("H001")

    for _ in range(298):
        buffer.append(measurement(0))
    assert buffer.is_ready("H001") is False
    buffer.append(measurement(0))
    assert buffer.is_ready("H001") is True


def test_warmup_metric_tracks_each_household_until_its_window_is_ready() -> None:
    registry = CollectorRegistry()
    metrics = AnalysisMetrics(registry)
    detector = Mock()
    detector.detect.return_value = []
    handler = MeasurementHandler(
        buffer=HouseholdBuffer(window_size=3),
        predictor=FakePredictor(),
        state_decider=ApplianceStateDecider(
            {appliance_type: 0.5 for appliance_type in APPLIANCE_ORDER}
        ),
        state_transition_detector=ApplianceStateTransitionDetector(3, 3, 0.05),
        activity_repository=Mock(),
        baseline_repository=BaselineRepository([]),
        tracker=DailyActivityTracker(),
        detector=detector,
        event_publisher=RecordingPublisher(),  # type: ignore[arg-type]
        snapshot_publisher=RecordingSnapshotPublisher(),  # type: ignore[arg-type]
        timezone_name="Asia/Seoul",
        metrics=metrics,
    )
    h002 = measurement(0).model_copy(update={"household_id": "H002"})

    handler(measurement(0))
    handler(h002)
    assert registry.get_sample_value("nilm_analysis_warmup_households") == 2

    handler(measurement(1))
    handler(measurement(2))
    assert registry.get_sample_value("nilm_analysis_warmup_households") == 1

    handler.reset_household("H002")
    assert registry.get_sample_value("nilm_analysis_warmup_households") == 0
