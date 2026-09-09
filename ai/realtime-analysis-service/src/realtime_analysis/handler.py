"""Orchestration for buffering, prediction, detection, and event publication."""

from zoneinfo import ZoneInfo

from realtime_analysis.anomaly_detector import RoutineMissedDetector
from realtime_analysis.baseline import BaselineRepository
from realtime_analysis.buffer import HouseholdBuffer
from realtime_analysis.event_producer import AnalysisEventPublisher
from realtime_analysis.predictor import Predictor
from realtime_analysis.schemas import PowerMeasurement
from realtime_analysis.state_tracker import DailyActivityTracker

# 실제 처리 순서를 담당하는 역할 
class MeasurementHandler:
    def __init__(
        self,
        buffer: HouseholdBuffer,
        predictor: Predictor,
        baseline_repository: BaselineRepository,
        tracker: DailyActivityTracker,
        detector: RoutineMissedDetector,
        event_publisher: AnalysisEventPublisher,
        timezone_name: str,
    ) -> None:
        self._buffer = buffer
        self._predictor = predictor
        self._baseline_repository = baseline_repository
        self._tracker = tracker
        self._detector = detector
        self._event_publisher = event_publisher
        self._timezone = ZoneInfo(timezone_name)

    def __call__(self, measurement: PowerMeasurement) -> None:
        self._buffer.append(measurement)  # 입력값을 가구별 버퍼에 넣음 
        if not self._buffer.is_ready(measurement.household_id): # 버퍼가 준비 좼는지 확인
            return

        states = self._predictor.predict(  # 가전 6종 판단
            self._buffer.get_window(measurement.household_id)
        )
        activity_date = measurement.measured_at.astimezone(self._timezone).date()
        # ON으로 판단된 가전을 오늘 사용한 것으로 기록 
        self._tracker.record_states(  
            measurement.household_id,
            activity_date,
            states,
        )

        # 가구 ID로 baseline을 찾음 
        baselines = self._baseline_repository.find_by_household(
            measurement.household_id
        )
        # baseline과 비교해 이상 여부 판단
        for anomaly in self._detector.detect(
            measurement.household_id,
            measurement.measured_at,
            baselines,
        ):
            self._event_publisher.publish(anomaly.event)  # 이상 이벤트 Kafka로 보냄 
            self._detector.mark_emitted(anomaly)  # 동일 이벤트가 반복되지 않도록 발행 완료 기록 
