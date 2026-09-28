"""Per-household rolling model input buffers."""

from collections import defaultdict, deque
import logging

from realtime_analysis.schemas import PowerMeasurement

FeatureRow = tuple[float, float, float, float]
logger = logging.getLogger(__name__)

# 가구별로 299개 모아서 AI로 보내기 
class HouseholdBuffer:
    def __init__(self, window_size: int) -> None:
        self._window_size = window_size
        self._buffers: dict[str, deque[FeatureRow]] = defaultdict(
            lambda: deque(maxlen=self._window_size)  # 300개가 되면 오래된 데이터 자동으로 제거 
        )

    def append(self, measurement: PowerMeasurement) -> None:  # 가구마다 버퍼가 따로 필요
        buffer = self._buffers[measurement.household_id]
        buffer.append(
            (
                measurement.active_power,
                measurement.reactive_power,
                measurement.power_factor,
                measurement.current,
            )
        )
        logger.info(
            "분석 버퍼 적재: 가구=%s 메시지=%s 샘플=%s/%s",
            measurement.household_id, measurement.message_id, len(buffer), self._window_size,
        )

    def is_ready(self, household_id: str) -> bool:
        return len(self._buffers[household_id]) == self._window_size

    def get_window(self, household_id: str) -> list[FeatureRow]:
        if not self.is_ready(household_id):
            raise ValueError(f"Buffer is not ready for household {household_id}")
        return list(self._buffers[household_id])

    def reset(self, household_id: str) -> None:
        """Discard samples that preceded a confirmed input gap."""

        self._buffers.pop(household_id, None)
