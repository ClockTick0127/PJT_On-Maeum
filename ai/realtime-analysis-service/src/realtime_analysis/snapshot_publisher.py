"""analysis.snapshot.v1 Kafka Publisher."""

import json
import logging
from typing import Any

from confluent_kafka import Producer

from realtime_analysis.config import Settings
from realtime_analysis.schemas import AnalysisSnapshot

logger = logging.getLogger(__name__)

class AnalysisSnapshotPublisher:
    """가구별 최신 분석 Snapshot을 Kafka에 발행한다."""

    def __init__(
        self,
        settings: Settings,
        producer: Producer | None = None,
        *, topic: str | None = None,
    ) -> None:
        self._topic = topic or settings.kafka_analysis_snapshot_topic
        self._producer = producer or Producer(settings.producer_config())

    def publish(self, snapshot: AnalysisSnapshot | dict) -> None:
        payload = snapshot if isinstance(snapshot, dict) else snapshot.model_dump(mode="json")
        delivery_errors: list[Any] = []

        def on_delivery(error: Any, message: Any) -> None:
            if error is not None:
                delivery_errors.append(error)

        self._producer.produce(
            topic=self._topic,
            # 같은 가구의 Snapshot 순서가 유지되도록 household_id를 Key로 사용한다.
            key=payload["household_id"].encode("utf-8"),
            value=json.dumps(
                payload,
                ensure_ascii=False,
            ).encode("utf-8"),
            callback=on_delivery,
        )
        # Handler가 끝나기 전에 전송 성공을 확인해야 입력 Offset을 안전하게 커밋할 수 있다.
        remaining = self._producer.flush(timeout=30)

        if remaining > 0:
            raise RuntimeError(f"{remaining} analysis snapshots were not delivered")
        if delivery_errors:
            raise RuntimeError(
                f"Analysis snapshot delivery failed: {delivery_errors[0]}"
            )
        logger.info(
            "분석 메시지 발행 완료: 토픽=%s 가구=%s 스냅샷=%s",
            self._topic, payload["household_id"], payload.get("snapshot_id", "없음"),
        )
