from datetime import datetime, timezone
from uuid import UUID

import pytest

from realtime_analysis.predictor import APPLIANCE_ORDER
from realtime_analysis.schemas import ApplianceState, PowerMeasurement
from realtime_analysis.snapshot import create_analysis_snapshot


def measurement() -> PowerMeasurement:
    return PowerMeasurement(
        message_id=UUID("8f3b2a19-d6e4-4c72-9b12-a1b2c3d4e5f6"),
        household_id="H001",
        device_id="main",
        measured_at=datetime.fromisoformat("2026-09-10T09:10:00+09:00"),
        active_power=1789.47,
        reactive_power=340.01,
        power_factor=0.982,
        current=8.279,
    )


def states() -> list[ApplianceState]:
    return [
        ApplianceState(
            appliance_type=appliance_type,
            probability=0.48 if appliance_type == "MICROWAVE" else 0.01,
            threshold=0.5,
            # MICROWAVE도 단순 threshold 결과만 보면 False인 상황을 만든다.
            is_on=False,
        )
        for appliance_type in APPLIANCE_ORDER
    ]


def test_snapshot_uses_raw_measurement_and_final_hysteresis_state() -> None:
    published_at = datetime(2026, 9, 10, 0, 10, 0, 125000, tzinfo=timezone.utc)
    snapshot = create_analysis_snapshot(
        measurement(),
        states(),
        {"MICROWAVE"},
        published_at,
    )

    assert snapshot.schema_version == 1
    assert snapshot.snapshot_id == measurement().message_id
    assert snapshot.household_id == "H001"
    assert snapshot.observed_at == datetime(2026, 9, 10, 0, 10, tzinfo=timezone.utc)
    assert snapshot.published_at == published_at
    assert snapshot.measurement.model_dump() == {
        "active_power": 1789.47,
        "reactive_power": 340.01,
        "power_factor": 0.982,
        "current": 8.279,
    }
    assert [item.appliance_type for item in snapshot.appliances] == list(
        APPLIANCE_ORDER
    )
    assert [item.appliance_type for item in snapshot.appliances if item.is_on] == [
        "MICROWAVE"
    ]


def test_snapshot_requires_all_six_appliance_states() -> None:
    with pytest.raises(ValueError, match="all six appliance types"):
        create_analysis_snapshot(
            measurement(),
            states()[:-1],
            {"MICROWAVE"},
        )


def test_snapshot_rejects_unknown_active_appliance_type() -> None:
    with pytest.raises(ValueError, match="unknown active appliance type"):
        create_analysis_snapshot(
            measurement(),
            states(),
            {"MICROWAVE", "TV"},
        )
