import json
from datetime import datetime, timezone
from unittest.mock import Mock
from uuid import UUID

from realtime_analysis.config import Settings
from realtime_analysis.event_producer import AnalysisEventPublisher
from realtime_analysis.schemas import AnalysisEvent


def test_event_is_published_with_event_type_and_without_score() -> None:
    producer = Mock()
    producer.flush.return_value = 0
    publisher = AnalysisEventPublisher(Settings(_env_file=None), producer=producer)
    event = AnalysisEvent(
        event_id=UUID("1bb4edcf-77ae-44d6-ad7c-b87a6e071f5a"),
        household_id="H001",
        event_type="ROUTINE_MISSED",
        occurred_at=datetime(2026, 9, 9, 23, 15, tzinfo=timezone.utc),
        reason={
            "expected_until": "08:10",
            "normal_days": 12,
            "window_days": 14,
        },
    )

    publisher.publish(event)

    kwargs = producer.produce.call_args.kwargs
    assert kwargs["topic"] == "analysis.event.v1"
    assert kwargs["key"] == b"H001"
    payload = json.loads(kwargs["value"].decode("utf-8"))
    assert payload["event_type"] == "ROUTINE_MISSED"
    assert "score" not in payload
