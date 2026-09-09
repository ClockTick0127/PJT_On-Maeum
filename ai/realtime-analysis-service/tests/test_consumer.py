import json
from unittest.mock import Mock

import pytest

from realtime_analysis.config import Settings
from realtime_analysis.consumer import AnalysisConsumer


def payload() -> dict[str, object]:
    return {
        "message_id": "8f3b2a19-4d6e-4c72-9b12-a1b2c3d4e5f6",
        "household_id": "H001",
        "device_id": "main",
        "measured_at": "2026-09-08T03:41:05.120Z",
        "active_power": 1789.47,
        "reactive_power": 340.01,
        "power_factor": 0.982,
        "current": 8.279,
    }


def kafka_message(value: object) -> Mock:
    message = Mock()
    message.value.return_value = json.dumps(value).encode("utf-8")
    return message


def service(consumer: Mock, dlq: Mock, handler: Mock) -> AnalysisConsumer:
    return AnalysisConsumer(
        settings=Settings(_env_file=None),
        dlq_publisher=dlq,
        handler=handler,
        consumer=consumer,
    )


def test_valid_message_is_handled_before_commit() -> None:
    consumer = Mock()
    handler = Mock()
    dlq = Mock()
    message = kafka_message(payload())

    service(consumer, dlq, handler).process_message(message)

    handler.assert_called_once()
    dlq.publish.assert_not_called()
    consumer.commit.assert_called_once_with(
        message=message,
        asynchronous=False,
    )


def test_invalid_message_goes_to_dlq_and_is_committed() -> None:
    consumer = Mock()
    handler = Mock()
    dlq = Mock()
    invalid_payload = payload()
    del invalid_payload["reactive_power"]
    message = kafka_message(invalid_payload)

    service(consumer, dlq, handler).process_message(message)

    dlq.publish.assert_called_once()
    handler.assert_not_called()
    consumer.commit.assert_called_once()


def test_handler_failure_is_not_committed() -> None:
    consumer = Mock()
    handler = Mock(side_effect=RuntimeError("temporary failure"))
    dlq = Mock()
    message = kafka_message(payload())

    with pytest.raises(RuntimeError, match="temporary failure"):
        service(consumer, dlq, handler).process_message(message)

    consumer.commit.assert_not_called()
