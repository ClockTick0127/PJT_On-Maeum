"""Kafka publisher for the agreed MVP analysis event contract."""

import json
import logging
from typing import Any

from confluent_kafka import Producer

from realtime_analysis.config import Settings
from realtime_analysis.schemas import AnalysisEvent

logger = logging.getLogger(__name__)

# 이상 이벤트 발행 
class AnalysisEventPublisher:
    def __init__(
        self,
        settings: Settings,
        producer: Producer | None = None,
    ) -> None:
        self._topic = settings.kafka_analysis_event_topic
        self._producer = producer or Producer(settings.producer_config())

    def publish(self, event: AnalysisEvent) -> None:
        delivery_errors: list[Any] = []

        def on_delivery(error: Any, message: Any) -> None:
            if error is not None:
                delivery_errors.append(error)

        # Kafka로 보냄 
        self._producer.produce(
            topic=self._topic,
            key=event.household_id.encode("utf-8"),
            # 이벤트 JSON 문자열로 변환 
            value=json.dumps(
                event.model_dump(mode="json"),
                ensure_ascii=False,
            ).encode("utf-8"),
            callback=on_delivery,
        )
        remaining = self._producer.flush(timeout=30)  # 최대 5초 동안 전송 결과를 기다림 (Kafka 브로커가 잘 가져갔는지) 

        if remaining > 0:
            raise RuntimeError(f"{remaining} analysis events were not delivered")
        if delivery_errors:
            raise RuntimeError(f"Analysis event delivery failed: {delivery_errors[0]}")
        logger.info(
            "분석 이벤트 발행 완료: 토픽=%s 가구=%s 이벤트=%s 유형=%s",
            self._topic, event.household_id, event.event_id, event.event_type,
        )
