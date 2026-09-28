"""Dead-letter queue message publishing."""

import json
import logging
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Message, Producer

from realtime_analysis.config import Settings
from realtime_analysis.schemas import DlqMessage

logger = logging.getLogger(__name__)

class DlqPublisher:
    def __init__(
        self,
        settings: Settings,
        producer: Producer | None = None,
    ) -> None:
        self._topic = settings.kafka_dlq_topic
        self._producer = producer or Producer(settings.producer_config())

    def publish(
        self,
        source_message: Message,
        payload: Any,
        error_code: str,
        error_message: str,
    ) -> None:
        event = DlqMessage(
            source_topic=source_message.topic(),
            source_partition=source_message.partition(),
            source_offset=source_message.offset(),
            error_code=error_code,
            error_message=error_message,
            failed_at=datetime.now(timezone.utc),
            payload=payload,
        )
        delivery_errors: list[Any] = []

        def on_delivery(error: Any, message: Any) -> None:
            if error is not None:
                delivery_errors.append(error)

        self._producer.produce(
            topic=self._topic,
            key=source_message.key(),
            value=json.dumps(
                event.model_dump(mode="json"),
                ensure_ascii=False,
            ).encode("utf-8"),
            callback=on_delivery,
        )
        remaining = self._producer.flush(timeout=30)
        if remaining > 0:
            raise RuntimeError(f"{remaining} DLQ events were not delivered")
        if delivery_errors:
            raise RuntimeError(f"DLQ event delivery failed: {delivery_errors[0]}")
        logger.info(
            "오류 메시지 발행 완료: 토픽=%s 원본토픽=%s 파티션=%s 오프셋=%s 오류=%s",
            self._topic, event.source_topic, event.source_partition,
            event.source_offset, error_code,
        )
