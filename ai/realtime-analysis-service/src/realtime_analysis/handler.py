"""버퍼링, 추론, 상태 감지, DB 저장과 이벤트 발행 순서를 조정한다."""

import logging
from uuid import UUID, uuid4

from zoneinfo import ZoneInfo

from realtime_analysis.activity_repository import ApplianceActivityRepository
from realtime_analysis.anomaly_detector import AnomalyDetector
from realtime_analysis.baseline import RoutineBaselineProvider
from realtime_analysis.buffer import HouseholdBuffer
from realtime_analysis.data_quality_monitor import DataQualityMonitor
from realtime_analysis.event_producer import AnalysisEventPublisher
from realtime_analysis.metrics import AnalysisMetrics, METRICS
from realtime_analysis.pipeline_timing import stage
from realtime_analysis.processing_receipt import ProcessingReceipt
from realtime_analysis.predictor import Predictor
from realtime_analysis.schemas import (
    AnalysisProcessingOutcome,
    PowerMeasurement,
    ProcessingSource,
)
from realtime_analysis.snapshot import create_analysis_snapshot
from realtime_analysis.snapshot_publisher import AnalysisSnapshotPublisher
from realtime_analysis.state_tracker import DailyActivityTracker
from realtime_analysis.state_decider import ApplianceStateDecider
from realtime_analysis.state_transition import ApplianceStateTransitionDetector

logger = logging.getLogger(__name__)


# 실제 처리 순서를 담당하는 역할 
class MeasurementHandler:
    def __init__(
        self,
        buffer: HouseholdBuffer,
        predictor: Predictor,
        state_decider: ApplianceStateDecider,
        state_transition_detector: ApplianceStateTransitionDetector,
        activity_repository: ApplianceActivityRepository,
        baseline_repository: RoutineBaselineProvider,
        tracker: DailyActivityTracker,
        detector: AnomalyDetector,
        event_publisher: AnalysisEventPublisher,
        snapshot_publisher: AnalysisSnapshotPublisher,
        timezone_name: str,
        data_quality_monitor: DataQualityMonitor | None = None,
        metrics: AnalysisMetrics = METRICS,
        analysis_run_id: str = "realtime-v1",
        model_version: str = "UNKNOWN",
        pipeline_version: str = "1",
    ) -> None:
        self._buffer = buffer
        self._predictor = predictor
        self._state_decider = state_decider
        self._state_transition_detector = state_transition_detector
        self._activity_repository = activity_repository
        self._baseline_repository = baseline_repository
        self._tracker = tracker
        self._detector = detector
        self._event_publisher = event_publisher
        self._snapshot_publisher = snapshot_publisher
        self._timezone = ZoneInfo(timezone_name)
        self._data_quality_monitor = data_quality_monitor
        self._metrics = metrics
        self._analysis_run_id = analysis_run_id
        self._model_version = model_version
        self._pipeline_version = pipeline_version
        self._state_epochs: dict[str, UUID] = {}
        self._warming_households: set[str] = set()

    def reset_household(self, household_id: str) -> None:
        """Discard volatile state whose Kafka partition ownership was lost."""

        self._buffer.reset(household_id)
        self._state_transition_detector.reset(household_id)
        if self._data_quality_monitor is not None:
            self._data_quality_monitor.reset(household_id)
        self._detector.reset(household_id)
        self._state_epochs.pop(household_id, None)
        self._warming_households.discard(household_id)
        self._metrics.set_warmup_households(len(self._warming_households))
        logger.info("Volatile household state reset: household=%s", household_id)

    def __call__(
        self,
        measurement: PowerMeasurement,
        source: ProcessingSource | None = None,
    ) -> None:
        epoch = self._state_epochs.setdefault(measurement.household_id, uuid4())
        quality_is_healthy = True
        if self._data_quality_monitor is not None:
            quality = self._data_quality_monitor.observe(
                measurement.household_id,
                measurement.measured_at,
            )
            quality_is_healthy = quality.is_healthy
            if quality.reset_required:
                self._buffer.reset(measurement.household_id)
                self._state_transition_detector.reset(
                    measurement.household_id
                )
                epoch = uuid4()
                self._state_epochs[measurement.household_id] = epoch

        # 모델 버퍼가 아직 차지 않았더라도 검증을 통과한 원본 샘플은 일일 관측에 집계한다.
        with stage("observation_db"):
            self._activity_repository.record_observation(
                measurement.household_id,
                measurement.measured_at,
            )
        with stage("buffer_append"):
            self._buffer.append(measurement)  # 입력값을 가구별 버퍼에 넣음
        self._update_warmup_state(measurement.household_id)
        # 복구 확인 중에는 원본 관측만 누적하고 추론과 위험 이벤트 판단을 보류한다.
        if not quality_is_healthy:
            self._record_outcome(
                measurement, epoch, AnalysisProcessingOutcome.SKIPPED_QUALITY_GATE, source
            )
            return
        if not self._buffer.is_ready(measurement.household_id): # 버퍼가 준비됐는지 확인
            self._record_outcome(
                measurement, epoch, AnalysisProcessingOutcome.SKIPPED_WARMUP, source
            )
            return

        try:
            with stage("inference"):
                predictions = self._predictor.predict(
                    self._buffer.get_window(measurement.household_id)
                )
        except Exception as error:
            self._record_outcome(
                measurement,
                epoch,
                AnalysisProcessingOutcome.FAILED_INFERENCE,
                source,
                error_type=type(error).__name__,
            )
            raise
        state_checkpoint = self._state_transition_detector.checkpoint(
            measurement.household_id
        )
        with stage("state_decision_transition"):
            states = self._state_decider.decide(predictions)
            activity_date = measurement.measured_at.astimezone(self._timezone).date()
            transitions = self._state_transition_detector.detect(
                measurement.household_id,
                measurement.measured_at,
                states,
            )
            # 단순 threshold 결과가 아니라 히스테리시스까지 적용된 확정 ON 상태를 구한다.
            active_appliance_types = {
                state.appliance_type
                for state in states
                if self._state_transition_detector.is_on(
                    measurement.household_id,
                    state.appliance_type,
                )
            }
        # 상태 변화와 ON 유지 확률을 DB에 저장한다.
        # Repository 내부에서 세션 INSERT와 event_count 증가를 한 트랜잭션으로 처리한다.
        success_receipt = self._receipt(
            measurement,
            epoch,
            AnalysisProcessingOutcome.SUCCEEDED,
            source,
            appliance_types=tuple(state.appliance_type for state in states),
        )
        try:
            with stage("activity_db"):
                self._activity_repository.record(
                    household_id=measurement.household_id,
                    observed_at=measurement.measured_at,
                    states=states,
                    transitions=transitions,
                    active_appliance_types=active_appliance_types,
                    processing_receipt=success_receipt,
                )
        except Exception as error:
            self._state_transition_detector.restore(
                measurement.household_id, state_checkpoint
            )
            try:
                self._record_outcome(
                    measurement,
                    epoch,
                    AnalysisProcessingOutcome.FAILED_PERSISTENCE,
                    source,
                    appliance_types=tuple(state.appliance_type for state in states),
                    error_type=type(error).__name__,
                )
            except Exception:
                logger.exception("Failed to persist analysis failure receipt")
            raise
        # DB 저장이 끝난 최신 측정값과 확정 가전 상태를 매 추론마다 발행한다.
        snapshot = create_analysis_snapshot(
            measurement=measurement,
            states=states,
            active_appliance_types=active_appliance_types,
        )
        with stage("snapshot_publish_ack"):
            self._snapshot_publisher.publish(snapshot)
        self._metrics.observe_e2e(
            (snapshot.published_at - snapshot.observed_at).total_seconds()
        )
        with stage("state_tracking"):
            self._tracker.record_transitions(
                activity_date,
                transitions,
            )
        for transition in transitions:
            logger.info(
                "Appliance state changed: household=%s appliance=%s transition=%s "
                "probability=%.4f threshold=%.4f occurred_at=%s confirmed_at=%s",
                transition.household_id,
                transition.appliance_type,
                transition.transition_type,
                transition.probability,
                transition.threshold,
                transition.occurred_at.isoformat(),
                transition.confirmed_at.isoformat(),
            )

        # 가구 ID로 baseline을 찾음 
        with stage("anomaly_detection"):
            baselines = self._baseline_repository.find_by_household(
                measurement.household_id,
                measurement.measured_at,
            )
            anomalies = self._detector.detect(
                measurement.household_id,
                measurement.measured_at,
                baselines,
            )
        for anomaly in anomalies:
            with stage("event_publish_ack"):
                self._event_publisher.publish(anomaly.event)
            self._metrics.record_pattern_event(anomaly.event.event_type)
            self._detector.mark_emitted(anomaly)

    def _update_warmup_state(self, household_id: str) -> None:
        if self._buffer.is_ready(household_id):
            self._warming_households.discard(household_id)
        else:
            self._warming_households.add(household_id)
        self._metrics.set_warmup_households(len(self._warming_households))

    def _receipt(
        self,
        measurement: PowerMeasurement,
        epoch: UUID,
        outcome: AnalysisProcessingOutcome,
        source: ProcessingSource | None,
        *,
        appliance_types: tuple[str, ...] = (),
        error_type: str | None = None,
    ) -> ProcessingReceipt:
        return ProcessingReceipt.for_measurement(
            measurement,
            analysis_run_id=self._analysis_run_id,
            model_version=self._model_version,
            pipeline_version=self._pipeline_version,
            state_epoch=epoch,
            outcome=outcome,
            source=source,
            appliance_types=appliance_types,
            error_type=error_type,
        )

    def _record_outcome(
        self,
        measurement: PowerMeasurement,
        epoch: UUID,
        outcome: AnalysisProcessingOutcome,
        source: ProcessingSource | None,
        *,
        appliance_types: tuple[str, ...] = (),
        error_type: str | None = None,
    ) -> None:
        self._activity_repository.record_processing_outcome(
            self._receipt(
                measurement,
                epoch,
                outcome,
                source,
                appliance_types=appliance_types,
                error_type=error_type,
            )
        )
