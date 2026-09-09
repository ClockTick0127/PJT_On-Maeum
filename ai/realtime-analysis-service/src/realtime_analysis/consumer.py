"""Kafka consumer lifecycle, validation, DLQ handling, and manual commits."""

import json
import logging
from collections.abc import Callable
from threading import Event
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from pydantic import ValidationError

from realtime_analysis.config import Settings
from realtime_analysis.dlq import DlqPublisher
from realtime_analysis.schemas import PowerMeasurement

logger = logging.getLogger(__name__)
MeasurementHandler = Callable[[PowerMeasurement], None]


class AnalysisConsumer:
    def __init__(
        self,
        settings: Settings,
        dlq_publisher: DlqPublisher,
        handler: MeasurementHandler,
        consumer: Consumer | None = None,
    ) -> None:
        self._input_topic = settings.kafka_input_topic
        self._dlq_publisher = dlq_publisher
        self._handler = handler
        self._consumer = consumer or Consumer(settings.consumer_config())

    def run(self, stop_event: Event) -> None:
        self._consumer.subscribe([self._input_topic])
        logger.info("Kafka consumer started: topic=%s", self._input_topic)
        # consumer는 계속 메시지 기다림 
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
            logger.info("Kafka consumer stopped")

    def process_message(self, message: Message) -> None:
        try:
            payload = self._decode_json(message)  # JONE 변환 
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raw_payload = (
                message.value().decode("utf-8", errors="replace")
                if message.value() is not None
                else None
            )
            self._dlq_publisher.publish(
                message,
                raw_payload,
                "INVALID_JSON",
                str(error),
            )
            self._commit(message)
            return

        try:
            measurement = PowerMeasurement.model_validate(payload) # 입력 스키마 검사 
        except ValidationError as error:
            self._dlq_publisher.publish(
                message,
                payload,
                "VALIDATION_ERROR",
                str(error),
            )
            self._commit(message)
            return

        self._handler(measurement)  # 검증 성공하면 Handler 호출 
        self._commit(message)  # 모든 처리가 성공하면 Kafka offset을 완료 처리

    @staticmethod
    def _decode_json(message: Message) -> Any:
        raw_value = message.value()
        if raw_value is None:
            raise json.JSONDecodeError("Kafka message value is null", "", 0)
        return json.loads(raw_value.decode("utf-8"))

    def _commit(self, message: Message) -> None:
        self._consumer.commit(message=message, asynchronous=False)
