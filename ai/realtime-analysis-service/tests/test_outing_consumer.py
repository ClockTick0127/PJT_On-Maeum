import json
from threading import Event
from unittest.mock import Mock, call

import pytest
from confluent_kafka import KafkaError, KafkaException

from realtime_analysis.config import Settings
from realtime_analysis.outing_consumer import OutingEventConsumer
from realtime_analysis.schemas import OutingEvent


def payload() -> dict[str, object]:
    return {
        "event_id": "1bb4edcf-77ae-44d6-ad7c-b87a6e071f5a",
        "household_id": "H001",
        "event_type": "OUTING_STARTED",
        "occurred_at": "2026-09-19T09:00:00+09:00",
    }


def kafka_message(value: object) -> Mock:
    message = Mock()
    message.value.return_value = json.dumps(value).encode("utf-8")
    return message


def service(
    consumer: Mock,
    repository: Mock,
    dlq: Mock,
) -> OutingEventConsumer:
    return OutingEventConsumer(
        settings=Settings(_env_file=None),
        repository=repository,
        dlq_publisher=dlq,
        consumer=consumer,
    )


def test_valid_event_is_applied_before_offset_store() -> None:
    consumer = Mock()
    repository = Mock()
    repository.apply_event.return_value = True
    dlq = Mock()
    message = kafka_message(payload())

    service(consumer, repository, dlq).process_message(message)

    event = repository.apply_event.call_args.args[0]
    assert isinstance(event, OutingEvent)
    assert event.household_id == "H001"
    dlq.publish.assert_not_called()
    consumer.store_offsets.assert_called_once_with(message=message)
    consumer.commit.assert_not_called()


def test_ignored_old_or_duplicate_event_still_stores_offset() -> None:
    consumer = Mock()
    repository = Mock()
    repository.apply_event.return_value = False
    dlq = Mock()
    message = kafka_message(payload())

    service(consumer, repository, dlq).process_message(message)

    consumer.store_offsets.assert_called_once_with(message=message)
    consumer.commit.assert_not_called()


def test_invalid_event_goes_to_dlq_and_stores_offset() -> None:
    consumer = Mock()
    repository = Mock()
    dlq = Mock()
    invalid_payload = payload()
    invalid_payload["event_type"] = "OUTING_CANCELLED"
    message = kafka_message(invalid_payload)

    service(consumer, repository, dlq).process_message(message)

    dlq.publish.assert_called_once()
    assert dlq.publish.call_args.args[2] == "VALIDATION_ERROR"
    repository.apply_event.assert_not_called()
    consumer.store_offsets.assert_called_once_with(message=message)
    consumer.commit.assert_not_called()


def test_invalid_json_goes_to_dlq_and_stores_offset() -> None:
    consumer = Mock()
    repository = Mock()
    dlq = Mock()
    message = Mock()
    message.value.return_value = b"{not-json"

    service(consumer, repository, dlq).process_message(message)

    dlq.publish.assert_called_once()
    assert dlq.publish.call_args.args[2] == "INVALID_JSON"
    repository.apply_event.assert_not_called()
    consumer.store_offsets.assert_called_once_with(message=message)
    consumer.commit.assert_not_called()


def test_repository_failure_does_not_store_offset() -> None:
    consumer = Mock()
    repository = Mock()
    repository.apply_event.side_effect = RuntimeError("database unavailable")
    dlq = Mock()
    message = kafka_message(payload())

    with pytest.raises(RuntimeError, match="database unavailable"):
        service(consumer, repository, dlq).process_message(message)

    consumer.store_offsets.assert_not_called()
    consumer.commit.assert_not_called()
    dlq.publish.assert_not_called()


def test_offset_store_after_partition_loss_does_not_stop_processing() -> None:
    consumer = Mock()
    consumer.store_offsets.side_effect = KafkaException(
        KafkaError(KafkaError._STATE)
    )
    repository = Mock()
    repository.apply_event.return_value = True
    message = kafka_message(payload())

    service(consumer, repository, Mock()).process_message(message)

    repository.apply_event.assert_called_once()
    consumer.store_offsets.assert_called_once_with(message=message)
    consumer.commit.assert_not_called()


def test_run_subscribes_polls_and_closes_consumer() -> None:
    consumer = Mock()
    repository = Mock()
    repository.apply_event.return_value = True
    dlq = Mock()
    message = kafka_message(payload())
    message.error.return_value = None
    stop_event = Event()

    def poll(*, timeout: float) -> Mock:
        assert timeout == 1.0
        stop_event.set()
        return message

    consumer.poll.side_effect = poll

    service(consumer, repository, dlq).run(stop_event)

    assert consumer.method_calls[0] == call.subscribe(
        ["monitoring.household-presence.v1"]
    )
    repository.apply_event.assert_called_once()
    consumer.store_offsets.assert_called_once_with(message=message)
    consumer.commit.assert_not_called()
    consumer.close.assert_called_once()
