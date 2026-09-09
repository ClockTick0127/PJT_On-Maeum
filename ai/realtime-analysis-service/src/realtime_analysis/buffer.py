"""Per-household rolling model input buffers."""

from collections import defaultdict, deque

from realtime_analysis.schemas import PowerMeasurement

FeatureRow = tuple[float, float, float, float]

# 가구별로 299개 모아서 AI로 보내기 
class HouseholdBuffer:
    def __init__(self, window_size: int) -> None:
        self._window_size = window_size
        self._buffers: dict[str, deque[FeatureRow]] = defaultdict(
            lambda: deque(maxlen=self._window_size)  # 300개가 되면 오래된 데이터 자동으로 제거 
        )

    def append(self, measurement: PowerMeasurement) -> None:  # 가구마다 버퍼가 따로 필요
        self._buffers[measurement.household_id].append(
            (
                measurement.active_power,
                measurement.reactive_power,
                measurement.power_factor,
                measurement.current,
            )
        )

    def is_ready(self, household_id: str) -> bool:
        return len(self._buffers[household_id]) == self._window_size

    def get_window(self, household_id: str) -> list[FeatureRow]:
        if not self.is_ready(household_id):
            raise ValueError(f"Buffer is not ready for household {household_id}")
        return list(self._buffers[household_id])
