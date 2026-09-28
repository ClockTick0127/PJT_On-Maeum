"""메시지 단위 파이프라인 구간 측정과 구조화 로그를 제공한다."""

from __future__ import annotations

import json
import logging
import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterator

from realtime_analysis.metrics import METRICS
from realtime_analysis.schemas import PowerMeasurement


logger = logging.getLogger("realtime_analysis.pipeline_timing")


@dataclass
class PipelineTimer:
    """Kafka 메시지 한 건의 구간별 단조 시계 소요 시간을 수집한다."""

    kafka_topic: str | None = None
    kafka_partition: int | None = None
    kafka_offset: int | None = None
    started_ns: int = field(default_factory=time.perf_counter_ns)
    stage_durations_ns: dict[str, int] = field(default_factory=dict)
    stage_counts: dict[str, int] = field(default_factory=dict)
    status: str = "processing"
    error_type: str | None = None
    message_id: str | None = None
    household_id: str | None = None
    measured_at: datetime | None = None

    def record_duration(self, name: str, duration_ns: int) -> None:
        """한 메시지에서 여러 번 실행될 수 있는 구간의 시간과 횟수를 누적한다."""

        if duration_ns < 0:
            raise ValueError("stage duration cannot be negative")
        self.stage_durations_ns[name] = (
            self.stage_durations_ns.get(name, 0) + duration_ns
        )
        self.stage_counts[name] = self.stage_counts.get(name, 0) + 1
        METRICS.observe_stage(name, duration_ns)

    @contextmanager
    def measure(self, name: str) -> Iterator[None]:
        started_ns = time.perf_counter_ns()
        try:
            yield
        finally:
            self.record_duration(name, time.perf_counter_ns() - started_ns)

    def bind_measurement(self, measurement: PowerMeasurement) -> None:
        self.message_id = str(measurement.message_id)
        self.household_id = measurement.household_id
        self.measured_at = measurement.measured_at

    def mark(self, status: str, error: BaseException | None = None) -> None:
        self.status = status
        self.error_type = type(error).__name__ if error is not None else None

    def as_record(self) -> dict[str, object]:
        logged_at = datetime.now(timezone.utc)
        processing_total_ns = time.perf_counter_ns() - self.started_ns
        record: dict[str, object] = {
            "event": "pipeline_timing",
            "logged_at": logged_at.isoformat(),
            "status": self.status,
            "message_id": self.message_id,
            "household_id": self.household_id,
            "measured_at": (
                self.measured_at.isoformat() if self.measured_at is not None else None
            ),
            "kafka": {
                "topic": self.kafka_topic,
                "partition": self.kafka_partition,
                "offset": self.kafka_offset,
            },
            "processing_total_ns": processing_total_ns,
            "stage_durations_ns": dict(sorted(self.stage_durations_ns.items())),
            "stage_counts": dict(sorted(self.stage_counts.items())),
        }
        if self.measured_at is not None:
            measured_at_utc = self.measured_at.astimezone(timezone.utc)
            sensor_to_log_ns = int(
                (logged_at - measured_at_utc).total_seconds() * 1_000_000_000
            )
            record["sensor_to_log_ns"] = sensor_to_log_ns
            record["clock_skew_detected"] = sensor_to_log_ns < 0
        if self.error_type is not None:
            record["error_type"] = self.error_type
        return record

    def log(self) -> None:
        # 로그 메시지 자체는 한 줄짜리 유효한 JSON이다. 전역 로그 포맷이 앞에 문자열을
        # 붙이더라도 로그 수집기는 record.message 부분을 JSON으로 파싱할 수 있다.
        logger.info(
            json.dumps(
                self.as_record(),
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )


_current_timer: ContextVar[PipelineTimer | None] = ContextVar(
    "pipeline_timer",
    default=None,
)


@contextmanager
def pipeline_timing(
    *,
    kafka_topic: str | None = None,
    kafka_partition: int | None = None,
    kafka_offset: int | None = None,
) -> Iterator[PipelineTimer]:
    """하위 처리 단계가 측정값을 추가할 메시지 단위 타이밍 문맥을 생성한다."""

    timer = PipelineTimer(
        kafka_topic=kafka_topic,
        kafka_partition=kafka_partition,
        kafka_offset=kafka_offset,
    )
    token = _current_timer.set(timer)
    try:
        yield timer
    except BaseException as error:
        timer.mark("failed", error)
        raise
    finally:
        METRICS.record_message(timer.status)
        if timer.status == "failed":
            METRICS.record_error(
                "pipeline",
                timer.error_type or "UnknownError",
            )
        timer.log()
        _current_timer.reset(token)


@contextmanager
def stage(name: str) -> Iterator[None]:
    """메시지 타이밍 문맥 안에서 호출된 처리 구간의 시간을 측정한다."""

    timer = _current_timer.get()
    if timer is None:
        yield
        return
    with timer.measure(name):
        yield
