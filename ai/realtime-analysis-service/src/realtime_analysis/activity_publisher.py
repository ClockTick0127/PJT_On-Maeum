"""Kafka publisher for the daily activity index contract."""

import json
import logging
from typing import Any

from confluent_kafka import Producer

from realtime_analysis.config import Settings
from realtime_analysis.schemas import ActivityIndexMessage

logger = logging.getLogger(__name__)

class ActivityIndexPublisher:
    def __init__(self, settings: Settings, producer: Producer | None = None) -> None:
        self._topic = settings.kafka_analysis_activity_topic
        self._producer = producer or Producer(settings.producer_config())

    def publish(self, message: ActivityIndexMessage) -> None:
        delivery_errors: list[Any] = []

        def on_delivery(error: Any, delivered_message: Any) -> None:
            if error is not None:
                delivery_errors.append(error)

        self._producer.produce(
            topic=self._topic,
            key=message.household_id.encode("utf-8"),
            value=json.dumps(
                message.model_dump(
                    mode="json",
                    exclude={"components"} if message.components is None else None,
                ),
                ensure_ascii=False,
            ).encode("utf-8"),
            callback=on_delivery,
        )
        remaining = self._producer.flush(timeout=30)
        if remaining > 0:
            raise RuntimeError(f"{remaining} activity index messages were not delivered")
        if delivery_errors:
            raise RuntimeError(
                f"Activity index delivery failed: {delivery_errors[0]}"
            )
        logger.info(
            "활동 지수 메시지 발행 완료: 토픽=%s 가구=%s 날짜=%s",
            self._topic, message.household_id, message.activity_date,
        )
