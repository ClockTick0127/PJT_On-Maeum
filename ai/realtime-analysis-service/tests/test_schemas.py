import pytest
from pydantic import ValidationError

from realtime_analysis.schemas import PowerMeasurement


def valid_payload() -> dict[str, object]:
    return {
        "message_id": "8f3b2a19-4d6e-4c72-9b12-a1b2c3d4e5f6",
        "household_id": "H001",
        "device_id": "main",
        "measured_at": "2026-09-08T03:41:05.120Z",
        "active_power": 1789.47,
        "reactive_power": 340.01,
        "power_factor": 0.982,
        "current": 8.279,
        "house": "H001",
        "power_w": 1789.47,
    }


def test_measurement_accepts_contract_and_ignores_legacy_fields() -> None:
    measurement = PowerMeasurement.model_validate(valid_payload())

    assert measurement.household_id == "H001"
    assert measurement.active_power == 1789.47
    assert not hasattr(measurement, "power_w")


def test_measurement_rejects_missing_model_feature() -> None:
    payload = valid_payload()
    del payload["reactive_power"]

    with pytest.raises(ValidationError):
        PowerMeasurement.model_validate(payload)
