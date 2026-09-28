from pathlib import Path

import pytest

from realtime_analysis.model_manifest import ModelManifest
from realtime_analysis.preprocessing import FeatureStandardizer, StandardizingPredictor


MANIFEST_PATH = Path(__file__).parents[1] / "config" / "model_manifest.json"


class RecordingPredictor:
    def __init__(self) -> None:
        self.window = None

    def predict(self, window):
        self.window = window
        return []


def manifest_with_training_statistics() -> ModelManifest:
    manifest = ModelManifest.from_json_file(MANIFEST_PATH)
    payload = manifest.model_dump(mode="json")
    statistics = ((100.0, 20.0), (40.0, 10.0), (0.8, 0.1), (2.0, 0.5))
    for feature, (mean, std) in zip(
        payload["input"]["features"],
        statistics,
        strict=True,
    ):
        feature["mean"] = mean
        feature["std"] = std
    return ModelManifest.model_validate(payload)


def test_feature_standardizer_uses_manifest_mean_and_std() -> None:
    standardizer = FeatureStandardizer(manifest_with_training_statistics())

    result = standardizer.transform([(120.0, 30.0, 0.9, 2.5)])

    assert result[0] == pytest.approx((1.0, -1.0, 1.0, 1.0))


def test_standardizing_predictor_passes_normalized_window_to_model() -> None:
    predictor = RecordingPredictor()
    wrapped = StandardizingPredictor(
        predictor,  # type: ignore[arg-type]
        manifest_with_training_statistics(),
    )

    wrapped.predict([(120.0, 30.0, 0.9, 2.5)])

    assert predictor.window is not None
    assert predictor.window[0] == pytest.approx((1.0, -1.0, 1.0, 1.0))
