import json
from datetime import datetime, timezone
from unittest.mock import Mock
from uuid import UUID

import pytest

from realtime_analysis.config import Settings
from realtime_analysis.schemas import (
    AnalysisSnapshot,
    SnapshotApplianceState,
    SnapshotMeasurement,
    SNAPSHOT_APPLIANCE_ORDER,
)
from realtime_analysis.snapshot_publisher import AnalysisSnapshotPublisher


def snapshot() -> AnalysisSnapshot:
    return AnalysisSnapshot(
        snapshot_id=UUID("8f3b2a19-d6e4-4c72-9b12-a1b2c3d4e5f6"),
        household_id="H001",
        observed_at=datetime(2026, 9, 10, 0, 10, tzinfo=timezone.utc),
        published_at=datetime(
            2026,
            9,
            10,
            0,
            10,
            0,
            125000,
            tzinfo=timezone.utc,
        ),
        measurement=SnapshotMeasurement(
            active_power=1789.47,
            reactive_power=340.01,
            power_factor=0.982,
            current=8.279,
        ),
        appliances=[
            SnapshotApplianceState(
                appliance_type=appliance_type,
                is_on=appliance_type == "MICROWAVE",
            )
            for appliance_type in SNAPSHOT_APPLIANCE_ORDER
        ],
    )


def test_snapshot_is_published_with_household_key_and_json_contract() -> None:
    producer = Mock()
    producer.flush.return_value = 0
    publisher = AnalysisSnapshotPublisher(
        Settings(_env_file=None),
        producer=producer,
    )

    publisher.publish(snapshot())

    kwargs = producer.produce.call_args.kwargs
    assert kwargs["topic"] == "analysis.snapshot.v1"
    assert kwargs["key"] == b"H001"
    payload = json.loads(kwargs["value"].decode("utf-8"))
    assert payload["schema_version"] == 1
    assert payload["measurement"]["active_power"] == 1789.47
    assert len(payload["appliances"]) == 6
    assert "probability" not in payload["appliances"][0]
    producer.flush.assert_called_once_with(timeout=30)


def test_snapshot_delivery_failure_is_propagated() -> None:
    producer = Mock()

    def fail_delivery(**kwargs: object) -> None:
        callback = kwargs["callback"]
        assert callable(callback)
        callback(RuntimeError("broker unavailable"), None)

    producer.produce.side_effect = fail_delivery
    producer.flush.return_value = 0
    publisher = AnalysisSnapshotPublisher(
        Settings(_env_file=None),
        producer=producer,
    )

    with pytest.raises(RuntimeError, match="snapshot delivery failed"):
        publisher.publish(snapshot())
