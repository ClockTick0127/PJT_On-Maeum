"""
NILM 스마트홈 전력 시뮬레이터 결정론적 실행 오케스트레이터 (Deterministic Schedule Executor)

- CompiledExecutionPlan을 지연 실행기(DeterministicScheduleRunner)와 연동하여 1초 단위 스트리밍 실행
- publish_scheduled_tick 어댑터를 통한 QoS 1 비동기 발행 및 결측 OMITTED 슬롯 집계
- REALTIME / ACCELERATED / BURST 실행 모드 및 페이싱 세그먼트 드리프트 보정
- 레이스 컨디션 없는 pause/resume 및 PAUSED 상태를 깨우는 request_stop 제어
- 2단계 날짜 완료 장벽(pending_day_result) 및 엄격한 commit 순서 보장
- CANCELLED 취소 상태 전이 및 미완료 장벽(pending_tick, pending_day_result) 보존
- 동기식 Single-flight 중복 실행 방지 가드
- 가구별 완전 격리 및 O(D) 메모리 복잡도 유지
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
import logging
import math
import time
from typing import Any

logger = logging.getLogger(__name__)

from .schedule import CompiledExecutionPlan, KST
from .schedule_runtime import (
    DeterministicScheduleRunner,
    ExecutionCompletedError,
    ScheduledTick,
)
from .schedule_publisher import (
    MqttPublishClient,
    PublishDisposition,
    SchedulePublishError,
    _validate_run_id,
    _validate_topic_household_id,
    publish_scheduled_tick,
)


class ExecutionAlreadyRunningError(RuntimeError):
    """동일 executor 인스턴스에서 run()이 이미 실행 중일 때 발생하는 동시성 예외"""
    pass


class DayCallbackError(RuntimeError):
    """날짜 완료 콜백(day_completed_callback) 실행 중 발생한 예외 (원인 체이닝 유지)"""
    pass


class ExecutionInvariantError(RuntimeError):
    """실행 오케스트레이터의 불변식(런타임 정합성 제약) 위반 시 발생하는 예외"""
    pass


class ExecutionMode(str, Enum):
    """오케스트레이터 실행 속도 모드"""
    REALTIME = "REALTIME"          # 가상 1초당 실제 1초 (monotonic 드리프트 보정)
    ACCELERATED = "ACCELERATED"    # 사용자 지정 배속 (간격 = 1 / speed_multiplier)
    BURST = "BURST"                # 의도적 sleep 없는 최고속 순차 스트리밍


class ExecutionStatus(str, Enum):
    """오케스트레이터 생명주기 상태"""
    READY = "READY"                # 초기화 완료, 실행 대기
    RUNNING = "RUNNING"            # 활성 슬롯 스트리밍 진행 중
    PAUSED = "PAUSED"              # 슬롯 경계에서 일시정지 중
    STOPPED = "STOPPED"            # 영구 중지됨 (재실행 불가)
    COMPLETED = "COMPLETED"        # 전체 계획 완료 (재실행 불가)
    FAILED = "FAILED"              # 발행 또는 콜백 실패 (장벽 보존, 재개 가능)
    CANCELLED = "CANCELLED"        # 태스크 취소됨 (장벽 보존, 재개 가능)


@dataclass(frozen=True)
class ExecutorConfig:
    """오케스트레이터 실행 설정 (불변)"""
    mode: ExecutionMode = ExecutionMode.REALTIME
    speed_multiplier: float | None = None

    def __post_init__(self):
        if not isinstance(self.mode, ExecutionMode):
            raise TypeError(f"mode는 ExecutionMode 열거형이어야 합니다: {type(self.mode).__name__}")
        if self.mode == ExecutionMode.ACCELERATED:
            if self.speed_multiplier is None:
                raise ValueError("ACCELERATED 모드에서는 speed_multiplier가 필수입니다.")
            if isinstance(self.speed_multiplier, bool) or not isinstance(self.speed_multiplier, (int, float)):
                raise TypeError(f"speed_multiplier는 양의 숫자여야 합니다: {type(self.speed_multiplier).__name__}")
            if math.isnan(self.speed_multiplier) or math.isinf(self.speed_multiplier):
                raise ValueError("speed_multiplier는 NaN이나 Infinity일 수 없습니다.")
            if self.speed_multiplier <= 0.0:
                raise ValueError(f"speed_multiplier는 0보다 커야 합니다: {self.speed_multiplier}")
        else:
            if self.speed_multiplier is not None:
                raise ValueError(f"{self.mode.value} 모드에서는 speed_multiplier를 지정할 수 없습니다.")


@dataclass(frozen=True)
class DayExecutionResult:
    """
    단일 일자(Day) 실행 완료 집계 결과 (불변).
    안정적 멱등성 식별자: (run_id, scenario, household_id, activity_date)
    """
    scenario: str
    household_id: str
    run_id: str
    activity_date: str            # "YYYY-MM-DD"
    published_samples: int        # 실제 PUBLISHED 확정 수
    omitted_samples: int          # 계획된 OMITTED 확정 수
    status: str = "COMPLETED"     # "COMPLETED" 고정

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario,
            "household_id": self.household_id,
            "run_id": self.run_id,
            "activity_date": self.activity_date,
            "published_samples": self.published_samples,
            "omitted_samples": self.omitted_samples,
            "status": self.status,
        }


DayCompletedCallback = Callable[[DayExecutionResult], Awaitable[None]]


@dataclass(frozen=True)
class OverallExecutionResult:
    """전체 시나리오 최종 완료 결과 (불변)"""
    scenario: str
    household_id: str
    run_id: str
    start_date: str               # "YYYY-MM-DD"
    end_date: str                 # "YYYY-MM-DD"
    total_virtual_slots: int      # 총 가상 슬롯 수
    published_samples: int        # 총 발행 성공 수
    omitted_samples: int          # 총 결측 생략 수
    status: str = "COMPLETED"     # "COMPLETED" 고정
    day_results: tuple[DayExecutionResult, ...] = ()


@dataclass(frozen=True)
class ExecutorSnapshot:
    """오케스트레이터 불변 상태 스냅샷"""
    scenario: str
    household_id: str
    run_id: str
    execution_mode: ExecutionMode
    speed_multiplier: float | None
    status: ExecutionStatus
    runner_virtual_slots: int     # runner.step() 호출 누적 횟수
    settled_virtual_slots: int    # 발행/결측 확정된 슬롯 수
    published_samples: int        # 총 PUBLISHED 수
    omitted_samples: int          # 총 OMITTED 수
    pending_cycle: int | None     # 현재 미해결 틱 cycle (발행 실패/취소 시 보존)
    pending_day_result: DayExecutionResult | None # 미해결 날짜 결과 (콜백 실패/취소 시 보존)
    current_activity_date: date   # 현재 진행 일자
    completed_days: tuple[DayExecutionResult, ...] # 커밋된 일자별 결과 (O(D))
    last_error: str | None        # 마지막 에러 메시지
    started_at: datetime | None   # 실행 시작 시각 (벽시계)
    completed_at: datetime | None # 전체 완료 시각 (벽시계, COMPLETED 전용)


class DeterministicScheduleExecutor:
    """
    단일 가구용 결정론적 E2E 시나리오 실행 오케스트레이터.

    - CompiledExecutionPlan과 DeterministicScheduleRunner를 소유
    - 가상 시간 1초 슬롯을 전진시키며 publish_scheduled_tick() 호출
    - 단일 asyncio 이벤트 루프 스레드 내 실행을 전제로 설계됨
    """

    def __init__(
        self,
        plan: CompiledExecutionPlan,
        household_id: str,
        run_id: str,
        config: ExecutorConfig | None = None,
        seed: int | str | None = None,
        day_completed_callback: DayCompletedCallback | None = None,
        sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
        time_func: Callable[[], float] = time.monotonic,
        broadcast_callback: Callable[[dict[str, Any]], Any] | None = None,
        max_broadcast_rate_hz: float = 20.0,
    ):
        if not isinstance(plan, CompiledExecutionPlan):
            raise TypeError(f"plan은 CompiledExecutionPlan 인스턴스여야 합니다: {type(plan).__name__}")
        _validate_topic_household_id(household_id)
        _validate_run_id(run_id)

        if config is None:
            config = ExecutorConfig()
        elif not isinstance(config, ExecutorConfig):
            raise TypeError(f"config는 ExecutorConfig 인스턴스여야 합니다: {type(config).__name__}")

        if day_completed_callback is not None and not callable(day_completed_callback):
            raise TypeError("day_completed_callback은 callable이어야 합니다.")

        if broadcast_callback is not None and not callable(broadcast_callback):
            raise TypeError("broadcast_callback은 callable이어야 합니다.")

        if not isinstance(max_broadcast_rate_hz, (int, float)) or max_broadcast_rate_hz <= 0:
            raise ValueError(f"max_broadcast_rate_hz는 양수여야 합니다: {max_broadcast_rate_hz}")

        self._plan = plan
        self._household_id = household_id
        self._run_id = run_id
        self._config = config
        self._day_completed_callback = day_completed_callback
        self._sleeper = sleeper
        self._time_func = time_func
        self._broadcast_callback = broadcast_callback
        self._max_broadcast_rate_hz = float(max_broadcast_rate_hz)
        self._last_broadcast_time: float | None = None
        self._broadcast_count: int = 0

        # 가구별 격리 실행기
        self._runner = DeterministicScheduleRunner(plan, household_id, seed=seed)

        # 생명주기 및 제어 상태
        self._status: ExecutionStatus = ExecutionStatus.READY
        self._is_running_coroutine: bool = False
        self._pause_requested: bool = False
        self._stop_requested: bool = False
        self._resume_event: asyncio.Event = asyncio.Event()
        self._resume_event.set()  # 초기에는 대기 없음

        # 슬롯 및 날짜 장벽 상태
        self._pending_tick: ScheduledTick | None = None
        self._pending_day_result: DayExecutionResult | None = None
        self._current_day_index: int = 0
        self._current_day_published: int = 0
        self._current_day_omitted: int = 0

        # 누적 집계 카운터 (O(D) 메모리)
        self._settled_virtual_slots: int = 0
        self._total_published: int = 0
        self._total_omitted: int = 0
        self._completed_days: list[DayExecutionResult] = []

        # 페이싱 세그먼트
        self._pacing_anchor_time: float = 0.0
        self._pacing_segment_slots: int = 0

        # 타임스탬프 및 진단
        self._last_error: str | None = None
        self._started_at: datetime | None = None
        self._completed_at: datetime | None = None

    @property
    def household_id(self) -> str:
        return self._household_id

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def status(self) -> ExecutionStatus:
        return self._status

    @property
    def config(self) -> ExecutorConfig:
        return self._config

    @property
    def plan(self) -> CompiledExecutionPlan:
        return self._plan

    @property
    def broadcast_count(self) -> int:
        return self._broadcast_count

    def _should_broadcast(self, now: float) -> bool:
        """화면 전송 여부 판정 (BURST 제외, max_broadcast_rate_hz 상한 적용)"""
        if self._broadcast_callback is None:
            return False
        if self._config.mode == ExecutionMode.BURST:
            return False
        if self._last_broadcast_time is None:
            self._last_broadcast_time = now
            return True
        min_interval = 1.0 / self._max_broadcast_rate_hz
        if (now - self._last_broadcast_time) >= min_interval:
            self._last_broadcast_time = now
            return True
        return False

    def _build_broadcast_payload(self, tick: ScheduledTick) -> dict[str, Any]:
        """기존 파형 뷰어가 요구하는 필드에 맞추어 실제 물리 계측치로부터 페이로드 생성"""
        is_pub = tick.is_publish_candidate
        active_set = set(tick.active_appliances) if is_pub else set()
        devices = {
            app: {
                "state": "ON" if app in active_set else "OFF",
                "enabled": app in active_set,
                "manualHold": False,
            }
            for app in ("kettle", "induction", "iron", "microwave", "hair_dryer", "vacuum_cleaner")
        }
        return {
            "house": self._household_id,
            "scenario": self._plan.scenario_id,
            "status": self._status.value.lower(),
            "simTimeKst": tick.measured_at.strftime("%H:%M:%S"),
            "simDateKst": tick.measured_at.strftime("%Y-%m-%d"),
            "simDateTimeKst": tick.measured_at.strftime("%Y-%m-%d %H:%M:%S KST"),
            "totalP": tick.active_power if is_pub else None,
            "totalQ": tick.reactive_power if is_pub else None,
            "apparentS": tick.apparent_power if is_pub else None,
            "pf": tick.power_factor if is_pub else None,
            "voltage": tick.voltage if is_pub else None,
            "currentA": tick.current if is_pub else None,
            "activeNames": list(tick.active_appliances) if is_pub else [],
            "devices": devices,
            "measurementAvailable": is_pub,
            "sensorFault": False,
            "sec": tick.cycle,
            "source": "E2E",
            "run_id": self._run_id,
        }

    def _maybe_broadcast(self, tick: ScheduledTick) -> None:
        """화면 전송 콜백 호출 (예외 발생 시 삼키고 경고 로그만 기록)"""
        if self._broadcast_callback is None:
            return
        now = self._time_func()
        if not self._should_broadcast(now):
            return
        try:
            payload = self._build_broadcast_payload(tick)
            self._broadcast_callback(payload)
            self._broadcast_count += 1
        except Exception as err:
            logger.warning(
                "E2E broadcast callback error for household %s (run_id: %s): %s",
                self._household_id,
                self._run_id,
                err,
            )

    def _reset_pacing_segment(self) -> None:
        """페이싱 세그먼트 앵커 시각 및 슬롯 카운터를 현재 시점으로 재설정"""
        self._pacing_anchor_time = self._time_func()
        self._pacing_segment_slots = 0

    def pause(self) -> None:
        """
        다음 슬롯 경계에서 일시정지하도록 요청.
        RUNNING 상태에서만 유효하게 신호를 설정한다.
        """
        if self._status == ExecutionStatus.RUNNING:
            self._pause_requested = True
            self._resume_event.clear()

    def resume(self) -> None:
        """
        일시정지 해제 또는 슬롯 경계 도달 전 대기 중인 일시정지 요청 취소.
        - RUNNING 상태: 슬롯 경계 도달 전 pause 요청 취소 (_pause_requested=False, _resume_event.set())
        - PAUSED 상태: 대기 중인 run coroutine을 깨움 (_pause_requested=False, _resume_event.set())
        """
        if self._status in (ExecutionStatus.RUNNING, ExecutionStatus.PAUSED):
            self._pause_requested = False
            self._resume_event.set()

    def request_stop(self) -> None:
        """
        실행을 영구 중지하도록 요청.
        - RUNNING / PAUSED 상태: 슬롯 경계에서 중지하도록 요청하며 PAUSED 대기 coroutine을 즉시 깨움
        - FAILED / CANCELLED / READY 상태: 실행을 영구 폐기하고 STOPPED 상태로 즉시 전이
        """
        if self._status in (ExecutionStatus.RUNNING, ExecutionStatus.PAUSED, ExecutionStatus.READY):
            self._stop_requested = True
            self._pause_requested = False
            self._resume_event.set()
        elif self._status in (ExecutionStatus.FAILED, ExecutionStatus.CANCELLED):
            self._status = ExecutionStatus.STOPPED
            self._stop_requested = True

    def snapshot(self) -> ExecutorSnapshot:
        """현재 진행 상태의 불변 스냅샷 반환 (O(1) 시간 복잡도)"""
        # 완료 상태 또는 마지막 일자 초과 시 IndexError 방지: plan.end_date 반환
        if self._current_day_index < len(self._plan.day_plans):
            curr_date = self._plan.day_plans[self._current_day_index].calendar_date
        else:
            curr_date = self._plan.end_date

        pending_cycle = self._pending_tick.cycle if self._pending_tick is not None else None

        return ExecutorSnapshot(
            scenario=self._plan.scenario_id,
            household_id=self._household_id,
            run_id=self._run_id,
            execution_mode=self._config.mode,
            speed_multiplier=self._config.speed_multiplier,
            status=self._status,
            runner_virtual_slots=self._runner.processed_virtual_slots,
            settled_virtual_slots=self._settled_virtual_slots,
            published_samples=self._total_published,
            omitted_samples=self._total_omitted,
            pending_cycle=pending_cycle,
            pending_day_result=self._pending_day_result,
            current_activity_date=curr_date,
            completed_days=tuple(self._completed_days),
            last_error=self._last_error,
            started_at=self._started_at,
            completed_at=self._completed_at,
        )

    def _commit_pending_day(self) -> None:
        """
        날짜 콜백 성공 후 일자 완료를 1회 커밋하는 내부 루틴.
        1. completed_days에 추가
        2. current_day_index 1 증가
        3. 일자별 카운터 0 초기화
        4. pending_day_result 해제
        """
        if self._pending_day_result is None:
            msg = "_commit_pending_day called with pending_day_result is None"
            self._status = ExecutionStatus.FAILED
            self._last_error = f"ExecutionInvariantError: {msg}"
            raise ExecutionInvariantError(msg)

        self._completed_days.append(self._pending_day_result)
        self._current_day_index += 1
        self._current_day_published = 0
        self._current_day_omitted = 0
        self._pending_day_result = None

    def _verify_day_result_invariants(self, day_result: DayExecutionResult) -> None:
        """
        단일 일자 완료 결과의 명시적 불변식 검증.
        위반 시 self._status를 FAILED로 기록하고 last_error 설정 후 ExecutionInvariantError 발생.
        pending_day_result는 진단 및 재검증을 위해 보존됨.
        """
        if not (0 <= self._current_day_index < len(self._plan.day_plans)):
            msg = (
                f"Day invariant failed: current_day_index={self._current_day_index} "
                f"out of range [0, {len(self._plan.day_plans)})"
            )
            self._status = ExecutionStatus.FAILED
            self._last_error = f"ExecutionInvariantError: {msg}"
            raise ExecutionInvariantError(msg)

        current_day_plan = self._plan.day_plans[self._current_day_index]

        if day_result.scenario != self._plan.scenario_id:
            msg = (
                f"Day invariant failed: scenario mismatch "
                f"(actual='{day_result.scenario}', expected='{self._plan.scenario_id}')"
            )
            self._status = ExecutionStatus.FAILED
            self._last_error = f"ExecutionInvariantError: {msg}"
            raise ExecutionInvariantError(msg)

        if day_result.household_id != self._household_id:
            msg = (
                f"Day invariant failed: household_id mismatch "
                f"(actual='{day_result.household_id}', expected='{self._household_id}')"
            )
            self._status = ExecutionStatus.FAILED
            self._last_error = f"ExecutionInvariantError: {msg}"
            raise ExecutionInvariantError(msg)

        if day_result.run_id != self._run_id:
            msg = (
                f"Day invariant failed: run_id mismatch "
                f"(actual='{day_result.run_id}', expected='{self._run_id}')"
            )
            self._status = ExecutionStatus.FAILED
            self._last_error = f"ExecutionInvariantError: {msg}"
            raise ExecutionInvariantError(msg)

        expected_date = current_day_plan.calendar_date.isoformat()
        if day_result.activity_date != expected_date:
            msg = (
                f"Day invariant failed: activity_date mismatch "
                f"(actual='{day_result.activity_date}', expected='{expected_date}')"
            )
            self._status = ExecutionStatus.FAILED
            self._last_error = f"ExecutionInvariantError: {msg}"
            raise ExecutionInvariantError(msg)

        if day_result.published_samples != current_day_plan.planned_publish_samples:
            msg = (
                f"Day invariant failed: published_samples mismatch "
                f"(actual={day_result.published_samples}, expected={current_day_plan.planned_publish_samples})"
            )
            self._status = ExecutionStatus.FAILED
            self._last_error = f"ExecutionInvariantError: {msg}"
            raise ExecutionInvariantError(msg)

        if day_result.omitted_samples != current_day_plan.planned_omitted_samples:
            msg = (
                f"Day invariant failed: omitted_samples mismatch "
                f"(actual={day_result.omitted_samples}, expected={current_day_plan.planned_omitted_samples})"
            )
            self._status = ExecutionStatus.FAILED
            self._last_error = f"ExecutionInvariantError: {msg}"
            raise ExecutionInvariantError(msg)

        total_day_samples = day_result.published_samples + day_result.omitted_samples
        if total_day_samples != current_day_plan.expected_samples:
            msg = (
                f"Day invariant failed: published+omitted != expected_samples "
                f"({total_day_samples} != {current_day_plan.expected_samples})"
            )
            self._status = ExecutionStatus.FAILED
            self._last_error = f"ExecutionInvariantError: {msg}"
            raise ExecutionInvariantError(msg)

    async def _resolve_pending_day(self) -> None:
        """
        pending_day_result의 불변식 검증, callback 호출, commit, 페이싱 리셋을 수행하는 공통 헬퍼.
        - 초기 날짜 완료 시점 및 이전 콜백 실패 후 재시도 시점에 공통 사용
        - 오직 callback await 중 발생한 일반 예외만 DayCallbackError로 변환
        """
        day_result = self._pending_day_result
        if day_result is None:
            return

        # 1. 날짜별 명시적 불변식 검증 (ExecutionInvariantError 발생)
        self._verify_day_result_invariants(day_result)

        # 2. 콜백 호출 (오직 이 블록만 DayCallbackError 변환 대상)
        if self._day_completed_callback is not None:
            try:
                await self._day_completed_callback(day_result)
            except asyncio.CancelledError:
                self._status = ExecutionStatus.CANCELLED
                raise
            except Exception as err:
                self._status = ExecutionStatus.FAILED
                self._last_error = f"DayCallbackError: {err}"
                raise DayCallbackError(
                    f"Day completed callback failed for date '{day_result.activity_date}': {err}"
                ) from err

        # 3. 커밋 및 페이싱 리셋 (try/except 바깥)
        self._commit_pending_day()
        self._reset_pacing_segment()

    def _verify_overall_completion_invariants(self) -> None:
        """
        전체 시나리오 완료 불변식의 명시적 검증.
        위반 시 self._status를 FAILED로 기록하고 last_error 설정 후 ExecutionInvariantError 발생.
        """
        if self._settled_virtual_slots != self._plan.total_virtual_slots:
            msg = (
                f"Overall completion invariant failed: settled_virtual_slots mismatch "
                f"(actual={self._settled_virtual_slots}, expected={self._plan.total_virtual_slots})"
            )
            self._status = ExecutionStatus.FAILED
            self._last_error = f"ExecutionInvariantError: {msg}"
            raise ExecutionInvariantError(msg)

        if not self._runner.is_completed:
            msg = "Overall completion invariant failed: runner.is_completed is False"
            self._status = ExecutionStatus.FAILED
            self._last_error = f"ExecutionInvariantError: {msg}"
            raise ExecutionInvariantError(msg)

        if self._pending_tick is not None:
            msg = f"Overall completion invariant failed: pending_tick is not None (cycle={self._pending_tick.cycle})"
            self._status = ExecutionStatus.FAILED
            self._last_error = f"ExecutionInvariantError: {msg}"
            raise ExecutionInvariantError(msg)

        if self._pending_day_result is not None:
            msg = (
                f"Overall completion invariant failed: pending_day_result is not None "
                f"(date={self._pending_day_result.activity_date})"
            )
            self._status = ExecutionStatus.FAILED
            self._last_error = f"ExecutionInvariantError: {msg}"
            raise ExecutionInvariantError(msg)

        if self._total_published != self._plan.total_planned_publish_samples:
            msg = (
                f"Overall completion invariant failed: total_published mismatch "
                f"(actual={self._total_published}, expected={self._plan.total_planned_publish_samples})"
            )
            self._status = ExecutionStatus.FAILED
            self._last_error = f"ExecutionInvariantError: {msg}"
            raise ExecutionInvariantError(msg)

        if self._total_omitted != self._plan.total_planned_omitted_samples:
            msg = (
                f"Overall completion invariant failed: total_omitted mismatch "
                f"(actual={self._total_omitted}, expected={self._plan.total_planned_omitted_samples})"
            )
            self._status = ExecutionStatus.FAILED
            self._last_error = f"ExecutionInvariantError: {msg}"
            raise ExecutionInvariantError(msg)

        if self._total_published + self._total_omitted != self._settled_virtual_slots:
            msg = (
                f"Overall completion invariant failed: total_published + total_omitted != settled_virtual_slots "
                f"({self._total_published} + {self._total_omitted} != {self._settled_virtual_slots})"
            )
            self._status = ExecutionStatus.FAILED
            self._last_error = f"ExecutionInvariantError: {msg}"
            raise ExecutionInvariantError(msg)

        if len(self._completed_days) != len(self._plan.day_plans):
            msg = (
                f"Overall completion invariant failed: len(completed_days) mismatch "
                f"(actual={len(self._completed_days)}, expected={len(self._plan.day_plans)})"
            )
            self._status = ExecutionStatus.FAILED
            self._last_error = f"ExecutionInvariantError: {msg}"
            raise ExecutionInvariantError(msg)

    async def _wait_pacing(self) -> None:
        """실행 모드에 따른 슬롯 간 페이싱 대기"""
        if self._config.mode == ExecutionMode.BURST:
            return

        if self._config.mode == ExecutionMode.REALTIME:
            tau = 1.0
        else:
            speed_mult = self._config.speed_multiplier
            if speed_mult is None or speed_mult <= 0:
                msg = f"ACCELERATED mode requires valid speed_multiplier, got {speed_mult}"
                self._status = ExecutionStatus.FAILED
                self._last_error = f"ExecutionInvariantError: {msg}"
                raise ExecutionInvariantError(msg)
            tau = 1.0 / speed_mult

        target_deadline = self._pacing_anchor_time + (self._pacing_segment_slots + 1) * tau
        delay = target_deadline - self._time_func()
        self._pacing_segment_slots += 1

        if delay > 0:
            try:
                await self._sleeper(delay)
            except asyncio.CancelledError:
                self._status = ExecutionStatus.CANCELLED
                raise

    async def run(self, client: MqttPublishClient) -> OverallExecutionResult | None:
        """
        주어진 MQTT 클라이언트를 사용하여 계획의 슬롯을 스트리밍 실행.

        - COMPLETED 시: OverallExecutionResult 반환
        - STOPPED 시: None 반환
        - FAILED 시: 원본 예외 발생
        - CANCELLED 시: asyncio.CancelledError 재발생
        - 중복 실행 시: ExecutionAlreadyRunningError 발생
        """
        # 1. 동기식 원자적 검사 (await 이전에 수행)
        if self._is_running_coroutine:
            raise ExecutionAlreadyRunningError(
                f"Executor for household '{self._household_id}' is already running or paused."
            )
        self._is_running_coroutine = True

        try:
            # 2. 상태 검사
            if self._status == ExecutionStatus.COMPLETED:
                raise ExecutionCompletedError("실행 계획의 모든 가상 슬롯 처리가 완료되었습니다.")
            if self._status == ExecutionStatus.STOPPED:
                raise RuntimeError("중지된(STOPPED) 실행기는 재실행할 수 없습니다.")
            if self._stop_requested:
                self._status = ExecutionStatus.STOPPED
                return None

            # READY, FAILED, CANCELLED -> RUNNING 전이
            self._status = ExecutionStatus.RUNNING
            self._last_error = None
            if self._started_at is None:
                self._started_at = datetime.now(timezone.utc)

            # 페이싱 세그먼트 시작
            self._reset_pacing_segment()

            # 3. 슬롯 스트리밍 루프
            while True:
                # [장벽 1: 이전 날짜 콜백 실패/취소/불변식 재시도 우선 처리]
                if self._pending_day_result is not None:
                    await self._resolve_pending_day()
                    # 마지막 날짜였을 경우 완료 루프로 이동
                    if self._current_day_index >= len(self._plan.day_plans):
                        break

                # [제어 확인 1: stop 요청]
                if self._stop_requested:
                    self._status = ExecutionStatus.STOPPED
                    return None

                # [제어 확인 2: pause 요청]
                if self._pause_requested:
                    self._status = ExecutionStatus.PAUSED
                    try:
                        await self._resume_event.wait()
                    except asyncio.CancelledError:
                        self._status = ExecutionStatus.CANCELLED
                        raise

                    # 깨어난 직후 stop 재확인
                    if self._stop_requested:
                        self._status = ExecutionStatus.STOPPED
                        return None

                    # 정상 재개
                    self._status = ExecutionStatus.RUNNING
                    self._reset_pacing_segment()

                # [장벽 2: 미완료 pending_tick 확인 또는 신규 step()]
                if self._pending_tick is None:
                    if self._runner.is_completed:
                        break
                    self._pending_tick = self._runner.step()

                tick = self._pending_tick

                # [발행 실행]
                try:
                    result = await publish_scheduled_tick(client, tick, self._run_id)
                except asyncio.CancelledError:
                    self._status = ExecutionStatus.CANCELLED
                    raise
                except Exception as err:
                    self._status = ExecutionStatus.FAILED
                    self._last_error = f"SchedulePublishError: {err}"
                    raise

                # [Settled 확정 및 슬롯 카운터 갱신]
                # 1) result.disposition 검증 및 카운터 증가
                if result.disposition == PublishDisposition.PUBLISHED:
                    self._total_published += 1
                    self._current_day_published += 1
                elif result.disposition == PublishDisposition.OMITTED:
                    self._total_omitted += 1
                    self._current_day_omitted += 1
                else:
                    msg = f"Unknown or invalid PublishDisposition: {result.disposition!r}"
                    self._status = ExecutionStatus.FAILED
                    self._last_error = f"ExecutionInvariantError: {msg}"
                    raise ExecutionInvariantError(msg)

                # 2) 그 후 settled_virtual_slots 증가
                self._settled_virtual_slots += 1

                # 3) pending_tick 제거
                self._pending_tick = None

                # [화면 전송 부가 작업 (발행 및 카운터 확정 후 수행, 예외 격리)]
                self._maybe_broadcast(tick)

                # [날짜 경계 검사]
                current_day_plan = self._plan.day_plans[self._current_day_index]
                if tick.cycle == current_day_plan.end_cycle:
                    day_res = DayExecutionResult(
                        scenario=self._plan.scenario_id,
                        household_id=self._household_id,
                        run_id=self._run_id,
                        activity_date=current_day_plan.calendar_date.isoformat(),
                        published_samples=self._current_day_published,
                        omitted_samples=self._current_day_omitted,
                        status="COMPLETED",
                    )
                    self._pending_day_result = day_res
                    await self._resolve_pending_day()

                    # 마지막 날짜 콜백 완료 시 루프 종료
                    if self._current_day_index >= len(self._plan.day_plans):
                        break

                # [페이싱 대기]
                # 마지막 슬롯이 아니고 stop/pause 요청이 없을 때만 대기
                if not self._runner.is_completed and not self._stop_requested and not self._pause_requested:
                    await self._wait_pacing()

            # [전체 완료 불변식 검증 및 완료 처리]
            if self._status == ExecutionStatus.STOPPED:
                return None

            self._verify_overall_completion_invariants()

            self._status = ExecutionStatus.COMPLETED
            self._completed_at = datetime.now(timezone.utc)

            return OverallExecutionResult(
                scenario=self._plan.scenario_id,
                household_id=self._household_id,
                run_id=self._run_id,
                start_date=self._plan.start_date.isoformat(),
                end_date=self._plan.end_date.isoformat(),
                total_virtual_slots=self._settled_virtual_slots,
                published_samples=self._total_published,
                omitted_samples=self._total_omitted,
                status="COMPLETED",
                day_results=tuple(self._completed_days),
            )
        except asyncio.CancelledError:
            self._status = ExecutionStatus.CANCELLED
            raise
        except (ExecutionCompletedError, ExecutionAlreadyRunningError):
            raise
        except RuntimeError as err:
            if self._status == ExecutionStatus.STOPPED:
                raise
            self._status = ExecutionStatus.FAILED
            if not self._last_error:
                self._last_error = f"{type(err).__name__}: {err}"
            raise
        except Exception as err:
            self._status = ExecutionStatus.FAILED
            if not self._last_error:
                self._last_error = f"{type(err).__name__}: {err}"
            raise
        finally:
            self._is_running_coroutine = False
