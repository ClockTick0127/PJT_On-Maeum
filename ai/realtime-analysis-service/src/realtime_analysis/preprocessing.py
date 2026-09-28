"""Manifest의 학습 통계로 Predictor 입력을 정규화한다."""

from collections.abc import Sequence

from realtime_analysis.buffer import FeatureRow
from realtime_analysis.model_manifest import ModelManifest
from realtime_analysis.pipeline_timing import stage
from realtime_analysis.predictor import Predictor
from realtime_analysis.schemas import AppliancePrediction


class FeatureStandardizer:
    """각 Feature에 학습 때 사용한 `(x - mean) / std`를 적용한다."""

    def __init__(self, manifest: ModelManifest) -> None:
        self._parameters = manifest.normalization_parameters

    def transform(self, window: Sequence[FeatureRow]) -> list[FeatureRow]:
        if not window:
            raise ValueError("Model input window cannot be empty")

        normalized: list[FeatureRow] = []
        for row in window:
            normalized.append(
                tuple(
                    (value - mean) / std
                    for value, (mean, std) in zip(
                        row,
                        self._parameters,
                        strict=True,
                    )
                )
            )
        return normalized


class StandardizingPredictor:
    """실제 Predictor 호출 직전에 Manifest 기반 정규화를 적용한다."""

    def __init__(self, predictor: Predictor, manifest: ModelManifest) -> None:
        self._predictor = predictor
        self._standardizer = FeatureStandardizer(manifest)

    def predict(self, window: Sequence[FeatureRow]) -> list[AppliancePrediction]:
        with stage("preprocess"):
            normalized_window = self._standardizer.transform(window)
        with stage("inference"):
            return self._predictor.predict(normalized_window)
