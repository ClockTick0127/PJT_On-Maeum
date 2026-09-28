"""Predictor interface and deterministic MVP fake implementation."""

from collections.abc import Sequence
from typing import Protocol

from realtime_analysis.buffer import FeatureRow
from realtime_analysis.schemas import AppliancePrediction

APPLIANCE_ORDER = (
    "KETTLE",
    "INDUCTION",
    "IRON",
    "MICROWAVE",
    "HAIR_DRYER",
    "VACUUM_CLEANER",
)

# 환경변수 FAKE_ON_APPLIANCES에 들어간 가전만 ON으로 반환 
class Predictor(Protocol):
    def predict(self, window: Sequence[FeatureRow]) -> list[AppliancePrediction]: ...


class FakePredictor:
    """Returns deterministic probabilities so the pipeline works without a model."""

    def __init__(self, on_appliances: Sequence[str] = ()) -> None:
        unknown = set(on_appliances) - set(APPLIANCE_ORDER)
        if unknown:
            raise ValueError(f"Unknown appliance types: {sorted(unknown)}")
        self._on_appliances = set(on_appliances)

    def predict(self, window: Sequence[FeatureRow]) -> list[AppliancePrediction]:
        if not window:
            raise ValueError("Model input window cannot be empty")
        return [
            AppliancePrediction(
                appliance_type=appliance_type,
                probability=1.0 if appliance_type in self._on_appliances else 0.0,   # 지정 가전에 1.0 할당
            )
            for appliance_type in APPLIANCE_ORDER
        ]
