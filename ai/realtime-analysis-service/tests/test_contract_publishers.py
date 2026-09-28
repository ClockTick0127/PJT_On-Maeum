import json
from datetime import date, datetime, timezone
from unittest.mock import Mock
from uuid import UUID

from realtime_analysis.activity_publisher import ActivityIndexPublisher
from realtime_analysis.config import Settings
from realtime_analysis.data_quality_publisher import DataQualityEventPublisher
from realtime_analysis.schemas import (
    ActivityIndexComponents,
    ActivityIndexMessage,
    DataQualityEvent,
)


def test_activity_index_is_published_with_household_key() -> None:
    producer = Mock()
    producer.flush.return_value = 0
    publisher = ActivityIndexPublisher(Settings(_env_file=None), producer=producer)
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

    publisher.publish(message)

    kwargs = producer.produce.call_args.kwargs
    assert kwargs["topic"] == "analysis.activity.v1"
    assert kwargs["key"] == b"H001"
    payload = json.loads(kwargs["value"].decode("utf-8"))
    assert payload["activity_index"] == 62
    assert payload["components"]["usage_count"] == 5


def test_insufficient_activity_publishes_null_index_without_components() -> None:
    producer = Mock()
    producer.flush.return_value = 0
    publisher = ActivityIndexPublisher(Settings(_env_file=None), producer=producer)
    message = ActivityIndexMessage(
        message_id=UUID("8f3b2a19-4d6e-4c72-9b12-a1b2c3d4e5f6"),
        household_id="H001",
        activity_date=date(2026, 9, 16),
        activity_index=None,
        data_status="INSUFFICIENT_DATA",
    )

    publisher.publish(message)

    payload = json.loads(producer.produce.call_args.kwargs["value"].decode("utf-8"))
    assert payload["activity_index"] is None
    assert "components" not in payload


def test_data_quality_event_is_published_with_household_key() -> None:
    producer = Mock()
    producer.flush.return_value = 0
    publisher = DataQualityEventPublisher(
        Settings(_env_file=None),
        producer=producer,
    )
    event = DataQualityEvent(
        event_id=UUID("1bb4edcf-77ae-44d6-ad7c-b87a6e071f5a"),
        household_id="H001",
        event_type="DATA_GAP",
        occurred_at=datetime(2026, 9, 16, 1, 2, tzinfo=timezone.utc),
        reason={
            "last_valid_received_at": "2026-09-16T10:00:00+09:00",
            "gap_seconds": 120,
        },
    )

    publisher.publish(event)

    kwargs = producer.produce.call_args.kwargs
    assert kwargs["topic"] == "analysis.data-quality.v1"
    assert kwargs["key"] == b"H001"
    payload = json.loads(kwargs["value"].decode("utf-8"))
    assert payload["event_type"] == "DATA_GAP"
    assert payload["reason"]["gap_seconds"] == 120
