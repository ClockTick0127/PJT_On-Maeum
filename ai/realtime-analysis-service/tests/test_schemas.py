from datetime import date
from uuid import UUID

import pytest
from pydantic import ValidationError

from realtime_analysis.schemas import (
    ActivityIndexComponents,
    ActivityIndexMessage,
    DataQualityEvent,
    OutingEvent,
    PowerMeasurement,
)


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


def test_valid_daily_activity_contract() -> None:
    message = ActivityIndexMessage(
        message_id=UUID("8f3b2a19-4d6e-4c72-9b12-a1b2c3d4e5f6"),
        household_id="H001",
        activity_date=date(2026, 9, 16),
        activity_index=62,
        data_status="VALID",
        components=ActivityIndexComponents(
            usage_count=5,
            appliance_type_count=3,
            usage_duration_seconds=2400,
            usage_count_score=65,
            appliance_diversity_score=75,
            usage_duration_score=60,
        ),
    )

    assert message.activity_index == 62


def test_insufficient_activity_contract_has_no_index() -> None:
    message = ActivityIndexMessage(
        message_id=UUID("8f3b2a19-4d6e-4c72-9b12-a1b2c3d4e5f6"),
        household_id="H001",
        activity_date=date(2026, 9, 16),
        activity_index=None,
        data_status="INSUFFICIENT_DATA",
    )

    assert message.components is None


def test_non_valid_activity_rejects_numeric_index() -> None:
    with pytest.raises(ValidationError):
        ActivityIndexMessage(
            message_id=UUID("8f3b2a19-4d6e-4c72-9b12-a1b2c3d4e5f6"),
            household_id="H001",
            activity_date=date(2026, 9, 16),
            activity_index=0,
            data_status="INSUFFICIENT_DATA",
        )


def test_data_gap_contract_accepts_gap_evidence() -> None:
    event = DataQualityEvent.model_validate(
        {
            "event_id": "1bb4edcf-77ae-44d6-ad7c-b87a6e071f5a",
            "household_id": "H001",
            "event_type": "DATA_GAP",
            "occurred_at": "2026-09-16T10:02:00+09:00",
            "reason": {
                "last_valid_received_at": "2026-09-16T10:00:00+09:00",
                "gap_seconds": 120,
            },
        }
    )

    assert event.event_type == "DATA_GAP"


@pytest.mark.parametrize("event_type", ["OUTING_STARTED", "OUTING_ENDED"])
def test_outing_contract_accepts_supported_event_types(event_type: str) -> None:
    event = OutingEvent.model_validate(
        {
            "event_id": "1bb4edcf-77ae-44d6-ad7c-b87a6e071f5a",
            "household_id": "H001",
            "event_type": event_type,
            "occurred_at": "2026-09-19T15:00:00+09:00",
        }
    )

    assert event.event_type == event_type
    assert event.occurred_at.utcoffset() is not None


def test_outing_contract_rejects_unknown_event_type() -> None:
    with pytest.raises(ValidationError):
        OutingEvent.model_validate(
            {
                "event_id": "1bb4edcf-77ae-44d6-ad7c-b87a6e071f5a",
                "household_id": "H001",
                "event_type": "OUTING_CANCELLED",
                "occurred_at": "2026-09-19T15:00:00+09:00",
            }
        )


def test_outing_contract_rejects_naive_occurred_at() -> None:
    with pytest.raises(ValidationError):
        OutingEvent.model_validate(
            {
                "event_id": "1bb4edcf-77ae-44d6-ad7c-b87a6e071f5a",
                "household_id": "H001",
                "event_type": "OUTING_STARTED",
                "occurred_at": "2026-09-19T15:00:00",
            }
        )
