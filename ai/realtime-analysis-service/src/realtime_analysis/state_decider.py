"""Convert appliance probabilities into thresholded ON/OFF states."""

from collections.abc import Mapping, Sequence

from realtime_analysis.model_manifest import ModelManifest
from realtime_analysis.predictor import APPLIANCE_ORDER
from realtime_analysis.schemas import AppliancePrediction, ApplianceState

# 확률과 가전별 threshold를 비교해 ON/OFF 판정
class ApplianceStateDecider:
    def __init__(self, thresholds: Mapping[str, float]) -> None:
        expected = set(APPLIANCE_ORDER)
        actual = set(thresholds)
        if actual != expected:
            missing = sorted(expected - actual)
            unknown = sorted(actual - expected)
            raise ValueError(
                f"Threshold appliance mismatch: missing={missing}, unknown={unknown}"
            )

        invalid = {
            appliance_type: threshold
            for appliance_type, threshold in thresholds.items()
            if not 0 <= threshold <= 1
        }
        if invalid:
            raise ValueError(f"Thresholds must be between 0 and 1: {invalid}")
        self._thresholds = dict(thresholds)

    @classmethod
    def from_manifest(cls, manifest: ModelManifest) -> "ApplianceStateDecider":
        return cls(manifest.thresholds)

    def decide(
        self,
        predictions: Sequence[AppliancePrediction],
    ) -> list[ApplianceState]:
        by_appliance: dict[str, AppliancePrediction] = {}
        for prediction in predictions:
            if prediction.appliance_type in by_appliance:
                raise ValueError(
                    f"Duplicate prediction: {prediction.appliance_type}"
                )
            by_appliance[prediction.appliance_type] = prediction

        expected = set(APPLIANCE_ORDER)
        actual = set(by_appliance)
        if actual != expected:
            missing = sorted(expected - actual)
            unknown = sorted(actual - expected)
            raise ValueError(
                f"Prediction appliance mismatch: missing={missing}, unknown={unknown}"
            )

        states = []
        for appliance_type in APPLIANCE_ORDER:
            probability = by_appliance[appliance_type].probability
            threshold = self._thresholds[appliance_type]
            states.append(
                ApplianceState(
                    appliance_type=appliance_type,
                    probability=probability,
                    threshold=threshold,
                    is_on=probability >= threshold,     # 임계값보다 큰지 확인 
                )
            )
        return states
