"""Kafka consumer lifecycle, validation, DLQ handling, and manual commits."""

import json
import logging
import time
from threading import Event
from typing import Any, Protocol

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    TopicPartition,
)
from pydantic import ValidationError

from realtime_analysis.config import Settings
from realtime_analysis.dlq import DlqPublisher
from realtime_analysis.metrics import AnalysisMetrics, METRICS
from realtime_analysis.pipeline_timing import pipeline_timing, stage
from realtime_analysis.schemas import PowerMeasurement, ProcessingSource

logger = logging.getLogger(__name__)
PartitionKey = tuple[str, int]


class MeasurementHandler(Protocol):
    def __call__(
        self,
        measurement: PowerMeasurement,
        source: ProcessingSource | None = None,
    ) -> None: ...

    def reset_household(self, household_id: str) -> None: ...


class AnalysisConsumer:
    def __init__(
        self,
        settings: Settings,
        dlq_publisher: DlqPublisher,
        handler: MeasurementHandler,
        consumer: Consumer | None = None,
        metrics: AnalysisMetrics = METRICS,
    ) -> None:
        self._input_topic = settings.kafka_input_topic
        self._measurement_type = PowerMeasurement
        self._scene_run_id = settings.analysis_run_id if settings.model_backend == "selected_scene" else None
        if settings.model_backend == "selected_scene":
            from realtime_analysis.scene_pipeline import SceneMeasurement
            self._measurement_type = SceneMeasurement
        self._dlq_publisher = dlq_publisher
        self._handler = handler
        self._consumer = consumer or Consumer(settings.consumer_config())
        self._metrics = metrics
        self._lag_refresh_seconds = settings.consumer_lag_refresh_seconds
        self._households_by_partition: dict[PartitionKey, set[str]] = {}
        self._assigned_partitions: set[PartitionKey] = set()

    def run(self, stop_event: Event) -> None:
        self._consumer.subscribe(
            [self._input_topic],
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
            on_lost=self._on_lost,
        )
        logger.info("Kafka consumer started: topic=%s", self._input_topic)
        next_lag_refresh = time.monotonic()
        # consumer는 계속 메시지 기다림 
        try:
            while not stop_event.is_set():
                poll_started_ns = time.perf_counter_ns()
                message = self._consumer.poll(timeout=1.0)
                poll_duration_ns = time.perf_counter_ns() - poll_started_ns
                now = time.monotonic()
                if now >= next_lag_refresh:
                    self._refresh_consumer_lag()
                    next_lag_refresh = now + self._lag_refresh_seconds
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(message.error())
                self.process_message(
                    message,
                    poll_duration_ns=poll_duration_ns,
                )
        finally:
            self._consumer.close()
            logger.info("Kafka consumer stopped")

    def process_message(
        self,
        message: Message,
        *,
        poll_duration_ns: int | None = None,
    ) -> None:
        with pipeline_timing(
            kafka_topic=self._message_string(message, "topic"),
            kafka_partition=self._message_int(message, "partition"),
            kafka_offset=self._message_int(message, "offset"),
        ) as timer:
            if poll_duration_ns is not None:
                timer.record_duration("consumer_poll", poll_duration_ns)

            try:
                with stage("deserialize_validate"):
                    payload = self._decode_json(message)
                    measurement = (self._measurement_type.model_validate_json(json.dumps(payload))
                                   if self._scene_run_id is not None else self._measurement_type.model_validate(payload))
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raw_payload = (
                    message.value().decode("utf-8", errors="replace")
                    if message.value() is not None
                    else None
                )
                with stage("dlq_publish_ack"):
                    self._dlq_publisher.publish(
                        message,
                        raw_payload,
                        "INVALID_JSON",
                        str(error),
                    )
                self._metrics.record_dlq("INVALID_JSON")
                with stage("offset_store"):
                    self._store_offset(message)
                timer.mark("dlq")
                return
            except ValidationError as error:
                with stage("dlq_publish_ack"):
                    self._dlq_publisher.publish(
                        message,
                        payload,
                        "VALIDATION_ERROR",
                        str(error),
                    )
                self._metrics.record_dlq("VALIDATION_ERROR")
                with stage("offset_store"):
                    self._store_offset(message)
                timer.mark("dlq")
                return

            if self._scene_run_id is not None and measurement.run_id != self._scene_run_id:
                self._store_offset(message)
                timer.mark("skipped_other_scene_run")
                return
            timer.bind_measurement(measurement)
            topic = self._message_string(message, "topic")
            partition = self._message_int(message, "partition")
            offset = self._message_int(message, "offset")
            logger.info(
                "카프카 메시지 소비: 토픽=%s 파티션=%s 오프셋=%s 가구=%s 메시지=%s",
                topic, partition, offset, measurement.household_id, measurement.message_id,
            )
            self._remember_household(topic, partition, measurement.household_id)
            self._handler(
                measurement,
                ProcessingSource(
                    topic=topic,
                    partition=partition,
                    offset=offset,
                ),
            )
            with stage("offset_store"):
                self._store_offset(message)
            timer.mark("processed")

    @staticmethod
    def _decode_json(message: Message) -> Any:
        raw_value = message.value()
        if raw_value is None:
            raise json.JSONDecodeError("Kafka message value is null", "", 0)
        return json.loads(raw_value.decode("utf-8"))

    def _store_offset(self, message: Message) -> bool:
        """Mark a processed offset for the rebalance-aware auto committer."""

        try:
            self._consumer.store_offsets(message=message)
            return True
        except KafkaException as error:
            kafka_error = error.args[0] if error.args else None
            if (
                isinstance(kafka_error, KafkaError)
                and kafka_error.code() == KafkaError._STATE
            ):
                self._metrics.record_error("offset_store", kafka_error.name())
                logger.warning(
                    "Offset store skipped after partition loss: "
                    "topic=%s partition=%s offset=%s error=%s",
                    self._message_string(message, "topic"),
                    self._message_int(message, "partition"),
                    self._message_int(message, "offset"),
                    kafka_error.name(),
                )
                return False
            raise

    def _on_assign(
        self,
        _consumer: Consumer,
        partitions: list[TopicPartition],
    ) -> None:
        """Register newly assigned partitions without touching retained state."""

        for partition in partitions:
            key = (partition.topic, partition.partition)
            self._assigned_partitions.add(key)
            self._households_by_partition.setdefault(key, set())
        self._metrics.record_consumer_rebalance("assign")
        self._metrics.set_assigned_partitions(len(self._assigned_partitions))
        logger.info(
            "Kafka partitions assigned: partitions=%s",
            self._partition_labels(partitions),
        )

    def _on_revoke(
        self,
        _consumer: Consumer,
        partitions: list[TopicPartition],
    ) -> None:
        """Reset only households owned by partitions being revoked."""

        reset_count = self._discard_partition_state(partitions)
        self._metrics.record_consumer_rebalance("revoke")
        self._metrics.set_assigned_partitions(len(self._assigned_partitions))
        self._metrics.record_household_state_resets(reset_count)
        logger.info(
            "Kafka partitions revoked: partitions=%s reset_households=%s",
            self._partition_labels(partitions),
            reset_count,
        )

    def _on_lost(
        self,
        _consumer: Consumer,
        partitions: list[TopicPartition],
    ) -> None:
        """Reset state for partitions lost before a normal revoke completed."""

        reset_count = self._discard_partition_state(partitions)
        self._metrics.record_consumer_rebalance("lost")
        self._metrics.set_assigned_partitions(len(self._assigned_partitions))
        self._metrics.record_household_state_resets(reset_count)
        logger.warning(
            "Kafka partitions lost: partitions=%s reset_households=%s",
            self._partition_labels(partitions),
            reset_count,
        )

    def _discard_partition_state(
        self,
        partitions: list[TopicPartition],
    ) -> int:
        revoked_households: set[str] = set()
        for partition in partitions:
            key = (partition.topic, partition.partition)
            self._assigned_partitions.discard(key)
            revoked_households.update(
                self._households_by_partition.pop(
                    key,
                    set(),
                )
            )
        for household_id in sorted(revoked_households):
            self._handler.reset_household(household_id)
        return len(revoked_households)

    def _remember_household(
        self,
        topic: str | None,
        partition: int | None,
        household_id: str,
    ) -> None:
        if topic is None or partition is None:
            return
        self._households_by_partition.setdefault((topic, partition), set()).add(
            household_id
        )

    @staticmethod
    def _partition_labels(partitions: list[TopicPartition]) -> list[str]:
        return [
            f"{partition.topic}[{partition.partition}]"
            for partition in partitions
        ]

    def _refresh_consumer_lag(self) -> None:
        """Refresh lag for assigned partitions using locally cached watermarks."""

        try:
            assignments = self._consumer.assignment()
            positions = self._consumer.position(assignments) if assignments else []
            lags: dict[tuple[str, int], int] = {}
            for position in positions:
                if position.offset < 0:
                    continue
                _, high = self._consumer.get_watermark_offsets(
                    position,
                    timeout=1.0,
                    cached=True,
                )
                lags[(position.topic, position.partition)] = high - position.offset
            self._metrics.replace_consumer_lag(lags)
        except Exception as error:
            self._metrics.record_error("consumer_lag", type(error).__name__)
            logger.warning("Kafka consumer lag refresh failed", exc_info=True)

    @staticmethod
    def _message_string(message: Message, attribute: str) -> str | None:
        try:
            value = getattr(message, attribute)()
        except (AttributeError, TypeError):
            return None
        return value if isinstance(value, str) else None

    @staticmethod
    def _message_int(message: Message, attribute: str) -> int | None:
        try:
            value = getattr(message, attribute)()
        except (AttributeError, TypeError):
            return None
        return value if isinstance(value, int) else None
