"""Kafka consumer for monitoring-owned household outing events."""

import json
import logging
from threading import Event
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from pydantic import ValidationError

from realtime_analysis.config import Settings
from realtime_analysis.dlq import DlqPublisher
from realtime_analysis.metrics import METRICS
from realtime_analysis.outing_repository import OutingStateRepository
from realtime_analysis.schemas import OutingEvent


logger = logging.getLogger(__name__)


class OutingEventConsumer:
    """Validates outing events and applies them to the latest household state."""

    def __init__(
        self,
        settings: Settings,
        repository: OutingStateRepository,
        dlq_publisher: DlqPublisher,
        consumer: Consumer | None = None,
    ) -> None:
        self._topic = settings.kafka_outing_event_topic
        self._repository = repository
        self._dlq_publisher = dlq_publisher
        self._consumer = consumer or Consumer(settings.outing_consumer_config())

    def run(self, stop_event: Event) -> None:
        self._consumer.subscribe([self._topic])
        logger.info("Outing event consumer started: topic=%s", self._topic)
        try:
            while not stop_event.is_set():
                message = self._consumer.poll(timeout=1.0)
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(message.error())
                self.process_message(message)
        finally:
            self._consumer.close()
            logger.info("Outing event consumer stopped")

    def process_message(self, message: Message) -> None:
        try:
            payload = self._decode_json(message)
            event = OutingEvent.model_validate(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raw_payload = (
                message.value().decode("utf-8", errors="replace")
                if message.value() is not None
                else None
            )
            self._publish_to_dlq(
                message,
                raw_payload,
                "INVALID_JSON",
                error,
            )
            return
        except ValidationError as error:
            self._publish_to_dlq(
                message,
                payload,
                "VALIDATION_ERROR",
                error,
            )
            return

        try:
            applied = self._repository.apply_event(event)
        except Exception as error:
            METRICS.record_error("outing_state", type(error).__name__)
            raise

        self._store_offset(message)
        logger.info(
            "외출 메시지 소비 완료: 토픽=%s 가구=%s 이벤트=%s 유형=%s 상태반영=%s",
            self._topic, event.household_id, event.event_id, event.event_type, applied,
        )

    def _publish_to_dlq(
        self,
        message: Message,
        payload: Any,
        error_code: str,
        error: Exception,
    ) -> None:
        self._dlq_publisher.publish(
            message,
            payload,
            error_code,
            str(error),
        )
        METRICS.record_dlq(error_code)
        self._store_offset(message)

    @staticmethod
    def _decode_json(message: Message) -> Any:
        raw_value = message.value()
        if raw_value is None:
            raise json.JSONDecodeError("Kafka message value is null", "", 0)
        return json.loads(raw_value.decode("utf-8"))

    def _store_offset(self, message: Message) -> bool:
        try:
            self._consumer.store_offsets(message=message)
            return True
        except KafkaException as error:
            kafka_error = error.args[0] if error.args else None
            if (
                isinstance(kafka_error, KafkaError)
                and kafka_error.code() == KafkaError._STATE
            ):
                METRICS.record_error("outing_offset_store", kafka_error.name())
                logger.warning(
                    "Outing offset store skipped after partition loss: error=%s",
                    kafka_error.name(),
                )
                return False
            raise
