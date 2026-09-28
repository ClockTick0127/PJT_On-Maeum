"""Realtime per-household input-gap detection and recovery tracking."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import Event, Lock, Thread
from typing import Literal, Protocol
from uuid import UUID, uuid5

from realtime_analysis.schemas import DataQualityEvent


logger = logging.getLogger(__name__)

DATA_QUALITY_EVENT_NAMESPACE = UUID("239bbf79-3734-4dcc-8616-d4ad0a654f32")


class DataQualityPublisher(Protocol):
    def publish(self, event: DataQualityEvent) -> None: ...


GapCallback = Callable[[str, datetime], None]
Clock = Callable[[], datetime]


@dataclass(frozen=True)
class DataQualityObservation:
    """Result used by the measurement pipeline after one valid input."""

    is_healthy: bool
    reset_required: bool = False


@dataclass
class _HouseholdState:
    status: Literal["NORMAL", "GAP"]
    last_valid_received_at: datetime
    last_valid_measured_at: datetime
    gap_detected_at: datetime | None = None
    gap_last_valid_received_at: datetime | None = None
    recovery_sample_count: int = 0


class DataQualityMonitor:
    """Emits one GAP and one RECOVERED event for each input-gap episode."""

    def __init__(
        self,
        publisher: DataQualityPublisher,
        gap_threshold_seconds: float,
        recovery_confirmation_samples: int,
        *,
        on_gap: GapCallback | None = None,
        clock: Clock | None = None,
    ) -> None:
        if gap_threshold_seconds <= 0:
            raise ValueError("gap_threshold_seconds must be greater than zero")
        if recovery_confirmation_samples < 1:
            raise ValueError("recovery_confirmation_samples must be at least one")
        self._publisher = publisher
        self._gap_threshold = timedelta(seconds=gap_threshold_seconds)
        self._recovery_confirmation_samples = recovery_confirmation_samples
        self._on_gap = on_gap
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._states: dict[str, _HouseholdState] = {}
        self._lock = Lock()

    def observe(
        self,
        household_id: str,
        measured_at: datetime,
        *,
        received_at: datetime | None = None,
    ) -> DataQualityObservation:
        """Record one schema-valid input and emit recovery after confirmation."""

        measured_at = self._as_utc(measured_at)
        received_at = self._as_utc(received_at or self._clock())
        recovery_event: DataQualityEvent | None = None

        with self._lock:
            state = self._states.get(household_id)
            if state is None:
                self._states[household_id] = _HouseholdState(
                    status="NORMAL",
                    last_valid_received_at=received_at,
                    last_valid_measured_at=measured_at,
                )
                return DataQualityObservation(is_healthy=True)

            # Kafka 재처리나 순서가 뒤바뀐 측정값은 센서 복구 증거로 사용하지 않는다.
            if measured_at <= state.last_valid_measured_at:
                return DataQualityObservation(is_healthy=state.status == "NORMAL")

            reset_required = (
                state.status == "GAP" and state.recovery_sample_count == 0
            )
            state.last_valid_received_at = received_at
            state.last_valid_measured_at = measured_at

            if state.status == "NORMAL":
                return DataQualityObservation(is_healthy=True)

            state.recovery_sample_count += 1
            if (
                state.recovery_sample_count
                < self._recovery_confirmation_samples
            ):
                return DataQualityObservation(
                    is_healthy=False,
                    reset_required=reset_required,
                )

            gap_last_valid = state.gap_last_valid_received_at
            gap_detected_at = state.gap_detected_at
            if gap_last_valid is None or gap_detected_at is None:
                raise RuntimeError("GAP state is missing its timestamps")

            recovery_event = DataQualityEvent(
                event_id=self._event_id(
                    household_id,
                    "DATA_RECOVERED",
                    gap_last_valid.isoformat(),
                    received_at.isoformat(),
                ),
                household_id=household_id,
                event_type="DATA_RECOVERED",
                occurred_at=received_at,
                reason={
                    "last_valid_received_at": gap_last_valid.isoformat(),
                    "gap_detected_at": gap_detected_at.isoformat(),
                    "gap_seconds": max(
                        0,
                        int((received_at - gap_last_valid).total_seconds()),
                    ),
                },
            )
            # 발행 성공 전에는 GAP 상태를 유지하여 복구 이벤트 유실 시 다음
            # 최신 입력에서 같은 공백 구간을 다시 처리할 수 있게 한다.
            self._publisher.publish(recovery_event)
            state.status = "NORMAL"
            state.gap_detected_at = None
            state.gap_last_valid_received_at = None
            state.recovery_sample_count = 0

        logger.info(
            "Data input recovered: household=%s gap_seconds=%s",
            household_id,
            recovery_event.reason["gap_seconds"],
        )
        return DataQualityObservation(
            is_healthy=True,
            reset_required=reset_required,
        )

    def detect_gaps(self, *, checked_at: datetime | None = None) -> int:
        """Evaluate all observed households even when Kafka input is silent."""

        checked_at = self._as_utc(checked_at or self._clock())
        published = 0

        with self._lock:
            for household_id, state in self._states.items():
                if state.status == "GAP":
                    continue
                elapsed = checked_at - state.last_valid_received_at
                if elapsed < self._gap_threshold:
                    continue

                event = DataQualityEvent(
                    event_id=self._event_id(
                        household_id,
                        "DATA_GAP",
                        state.last_valid_received_at.isoformat(),
                    ),
                    household_id=household_id,
                    event_type="DATA_GAP",
                    occurred_at=checked_at,
                    reason={
                        "last_valid_received_at": (
                            state.last_valid_received_at.isoformat()
                        ),
                        "gap_seconds": max(0, int(elapsed.total_seconds())),
                    },
                )
                # GAP 발행과 상태 변경을 같은 임계 구역에서 순서대로 처리한다.
                # 따라서 빠르게 들어온 복구 입력이 GAP보다 먼저 발행될 수 없다.
                if self._on_gap is not None:
                    self._on_gap(household_id, state.last_valid_measured_at)
                self._publisher.publish(event)
                state.status = "GAP"
                state.gap_detected_at = checked_at
                state.gap_last_valid_received_at = state.last_valid_received_at
                state.recovery_sample_count = 0
                published += 1
                logger.warning(
                    "Data input gap detected: household=%s gap_seconds=%s",
                    household_id,
                    event.reason["gap_seconds"],
                )
        return published

    def is_healthy(self, household_id: str) -> bool:
        with self._lock:
            state = self._states.get(household_id)
            return state is None or state.status == "NORMAL"

    def reset(self, household_id: str) -> None:
        """Forget one household after its Kafka partition is revoked."""

        with self._lock:
            self._states.pop(household_id, None)

    @staticmethod
    def _event_id(
        household_id: str,
        event_type: str,
        *parts: str,
    ) -> UUID:
        identity = ":".join((household_id, event_type, *parts))
        return uuid5(DATA_QUALITY_EVENT_NAMESPACE, identity)

    @staticmethod
    def _as_utc(value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("data-quality timestamps must include a timezone")
        return value.astimezone(timezone.utc)


class DataQualityWatchdog:
    """Background lifecycle wrapper for periodic gap evaluation."""

    def __init__(self, monitor: DataQualityMonitor, poll_seconds: float) -> None:
        if poll_seconds <= 0:
            raise ValueError("poll_seconds must be greater than zero")
        self._monitor = monitor
        self._poll_seconds = poll_seconds
        self._thread: Thread | None = None

    def start(self, stop_event: Event) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = Thread(
            target=self._run,
            args=(stop_event,),
            name="data-quality-watchdog",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        if self._thread is not None:
            self._thread.join(timeout=self._poll_seconds + 1)

    def _run(self, stop_event: Event) -> None:
        while not stop_event.is_set():
            try:
                self._monitor.detect_gaps()
            except Exception:
                logger.exception("Data-quality watchdog evaluation failed")
            stop_event.wait(self._poll_seconds)
