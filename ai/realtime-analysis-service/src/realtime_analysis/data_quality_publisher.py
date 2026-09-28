"""Kafka publisher for analysis data-quality state changes."""

import json
import logging
from typing import Any

from confluent_kafka import Producer

from realtime_analysis.config import Settings
from realtime_analysis.schemas import DataQualityEvent

logger = logging.getLogger(__name__)

class DataQualityEventPublisher:
    def __init__(self, settings: Settings, producer: Producer | None = None) -> None:
        self._topic = settings.kafka_analysis_data_quality_topic
        self._producer = producer or Producer(settings.producer_config())

    def publish(self, event: DataQualityEvent) -> None:
        delivery_errors: list[Any] = []

        def on_delivery(error: Any, delivered_message: Any) -> None:
            if error is not None:
                delivery_errors.append(error)

        self._producer.produce(
            topic=self._topic,
            key=event.household_id.encode("utf-8"),
            value=json.dumps(
                event.model_dump(mode="json"),
                ensure_ascii=False,
            ).encode("utf-8"),
            callback=on_delivery,
        )
        remaining = self._producer.flush(timeout=30)
        if remaining > 0:
            raise RuntimeError(f"{remaining} data-quality events were not delivered")
        if delivery_errors:
            raise RuntimeError(
                f"Data-quality event delivery failed: {delivery_errors[0]}"
            )
        logger.info(
            "데이터 품질 메시지 발행 완료: 토픽=%s 가구=%s 이벤트=%s 유형=%s",
            self._topic, event.household_id, event.event_id, event.event_type,
        )
