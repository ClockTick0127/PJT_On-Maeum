from pathlib import Path

import pytest
from pydantic import ValidationError

from realtime_analysis.model_manifest import ModelManifest
from realtime_analysis.predictor import APPLIANCE_ORDER

MANIFEST_PATH = Path(__file__).parents[1] / "config" / "model_manifest.json"


def test_manifest_loads_model_contract() -> None:
    manifest = ModelManifest.from_json_file(MANIFEST_PATH)

    assert manifest.input.shape == (-1, 4, 299)
    assert manifest.output.shape == (-1, 6)
    assert tuple(
        appliance.appliance_type
        for appliance in manifest.output.appliances
    ) == APPLIANCE_ORDER
    assert manifest.thresholds == {
        appliance_type: 0.5 for appliance_type in APPLIANCE_ORDER
    }
    assert manifest.normalization_parameters == (
        (0.0, 1.0),
        (0.0, 1.0),
        (0.0, 1.0),
        (0.0, 1.0),
    )


def test_manifest_rejects_wrong_output_order() -> None:
    manifest = ModelManifest.from_json_file(MANIFEST_PATH)
    payload = manifest.model_dump(mode="json")
    payload["output"]["appliances"][0]["appliance_type"] = "MICROWAVE"

    with pytest.raises(ValidationError):
        ModelManifest.model_validate(payload)


def test_manifest_requires_training_mean_and_std() -> None:
    manifest = ModelManifest.from_json_file(MANIFEST_PATH)
    payload = manifest.model_dump(mode="json")
    payload["input"]["features"][0]["mean"] = None

    with pytest.raises(ValidationError):
        ModelManifest.model_validate(payload)
