"""
NILM 스마트홈 시뮬레이터 단일 세션 기반 E2E 스케줄 세션 관리자 (E2EScheduleSessionManager)

- 단일 활성 E2E 세션 관리 (한 번에 1개 세션만 실행)
- Two-Phase Activation Barrier (준비 -> 커밋, 타임아웃/오류 시 MQTT context 미진입 조기 종료)
- Atomic Command Envelope (QUEUED/CLAIMED/ABORTED/COMPLETED 선형화 및 accepted_result 선행 기록)
- 가구별 독립 runner, executor, aiomqtt.Client 및 asyncio.Task 격리
- Executor 생성, 제어(pause/resume/stop), 스냅샷 조회의 E2E 이벤트 루프 단일 스레드 소유권 보장
- Executor의 private 필드 접근 완전 제거 및 순수 공개 API만 활용
- 가구별 독립 생명주기 및 MQTT context 종료 후 최종 terminal 상태 확정
- 표준 비동기 제어 API (202 Accepted) 및 불변 스냅샷 조회
- Bounded 셧다운 (단일 monotonic deadline, 2단계 bounded asyncio.wait, wait_for(gather) 미사용)
"""
from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from enum import Enum
import re
import threading
import time
import uuid
from typing import Any, AsyncContextManager

import aiomqtt

from engine.schedule import (
    CompiledExecutionPlan,
    ScenarioDefinition,
    SECONDS_PER_DAY,
    compile_schedule,
    resolve_base_date,
)
from engine.schedule_executor import (
    DeterministicScheduleExecutor,
    ExecutorConfig,
    ExecutionMode,
    ExecutionStatus,
)
from engine.unified_catalog import get_unified_scenario_definition
from engine.tls import resolve_mqtt_config, get_mqtt_tls_context

from .config import resolve_auto_speed


class E2EError(Exception):
    """E2E 도메인 기본 예외"""


KST = timezone(timedelta(hours=9))


def resolve_start_time_delay(start_time_str: str | None, now: datetime | None = None) -> float:
    """
    start_time 문자열을 Asia/Seoul(KST) 기준 실제 MQTT 발행 개시 예약 시각으로 해석하여 현재 시각 대비 대기 시간(초)을 산출합니다.

    해석 및 처리 규칙:
    1. 생략 또는 None:
       - 즉시 시작 (지연 0.0초 반환).
    2. 시간만 입력된 경우 (HH:MM 또는 HH:MM:SS):
       - 요청 처리 시점의 Asia/Seoul 현재 날짜(오늘)에 해당 시각을 결합하여 목표 시각을 생성합니다.
       - 예: 현재 시각 2026-09-19 20:00:00 KST에 "20:30:00" 입력 시 -> 2026-09-19 20:30:00 KST로 해석.
    3. 날짜와 시간이 함께 입력된 경우 (ISO 8601 형식: YYYY-MM-DDTHH:MM:SS 등):
       - 입력된 날짜와 시각을 파싱합니다.
       - 타임존 오프셋이 없으면 기본 Asia/Seoul로 간주하고, 오프셋이 지정되어 있으면 Asia/Seoul로 변환합니다.
       - 예: "2026-09-20T01:00:00" -> 2026-09-20 01:00:00 KST로 해석.
    4. 과거 시각 처리 규칙:
       - 산출된 목표 예약 시각이 현재 시각 이전이거나 동일하면 (target_dt <= current_dt),
         지연 시간은 0.0초로 산출되어 대기 없이 즉시 시작합니다 (음수 대기 방지).
    """
    if not start_time_str:
        return 0.0

    clean_st = start_time_str.strip()
    if not clean_st:
        return 0.0

    current_now = now or datetime.now(KST)
    if current_now.tzinfo is None:
        current_now = current_now.replace(tzinfo=KST)
    else:
        current_now = current_now.astimezone(KST)

    # 1) 시간만 입력된 경우 (HH:MM 또는 HH:MM:SS)
    time_match = re.match(r"^(\d{2}):(\d{2})(?::(\d{2}))?$", clean_st)
    if time_match:
        hour = int(time_match.group(1))
        minute = int(time_match.group(2))
        second = int(time_match.group(3)) if time_match.group(3) is not None else 0
        target_dt = current_now.replace(hour=hour, minute=minute, second=second, microsecond=0)
    else:
        # 2) 날짜 및 시간이 포함된 ISO 형식
        try:
            parsed_dt = datetime.fromisoformat(clean_st)
            if parsed_dt.tzinfo is None:
                target_dt = parsed_dt.replace(tzinfo=KST)
            else:
                target_dt = parsed_dt.astimezone(KST)
        except ValueError:
            return 0.0

    delay = (target_dt - current_now).total_seconds()
    return max(0.0, delay)


class E2EConflictError(E2EError):
    """활성 세션 충돌 예외 (409 E2E_SIMULATOR_RUNNING)"""
    pass


class E2ERunNotFoundError(E2EError):
    """실행 세션을 찾을 수 없음 (404 RUN_NOT_FOUND)"""
    pass


class E2EHouseholdNotFoundError(E2EError):
    """가구를 찾을 수 없음 (404 HOUSEHOLD_NOT_FOUND)"""
    pass


class E2EStartTimeoutError(E2EError):
    """세션 시작 준비 타임아웃 예외 (503 E2E_START_TIMEOUT)"""
    pass


class E2EStartError(E2EError):
    """세션 시작 준비 실패 예외 (500 START_FAILED)"""
    pass


class E2EOperationTimeoutError(E2EError):
    """제어 명령 타임아웃 예외 (503 E2E_OPERATION_TIMEOUT)"""
    pass


class E2ESnapshotTimeoutError(E2EError):
    """스냅샷 조회 타임아웃 예외 (503 E2E_SNAPSHOT_TIMEOUT)"""
    pass


class SessionActivationState(str, Enum):
    """세션 활성화 생명주기 상태"""
    PREPARING = "PREPARING"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"


class CommandStatus(str, Enum):
    """제어 명령 생명주기 상태"""
    QUEUED = "QUEUED"
    CLAIMED = "CLAIMED"
    ABORTED = "ABORTED"
    COMPLETED = "COMPLETED"


class CommandEnvelope:
    """제어 명령 선형화를 위한 원자적 Envelope"""
    def __init__(self, action: str, run_id: str, household_id: str | None = None):
        self.action = action
        self.run_id = run_id
        self.household_id = household_id
        self.status: CommandStatus = CommandStatus.QUEUED
        self.accepted_result: dict | None = None
        self.result: dict | None = None
        self.error: Exception | None = None
        self.lock = threading.Lock()


class HouseholdTaskStatus(str, Enum):
    """가구별 실행 태스크 생명주기 상태"""
    STARTING = "STARTING"    # 태스크 생성 및 MQTT 연결 수립 중
    RUNNING = "RUNNING"      # 활성 슬롯 스트리밍 진행 중
    PAUSING = "PAUSING"      # 일시정지 요청 접수, 슬롯 경계 도달 대기 중
    PAUSED = "PAUSED"        # 슬롯 경계에서 안전하게 정지됨
    STOPPING = "STOPPING"    # 중단 요청 접수, executor 종료 및 소켓 정리 중
    STOPPED = "STOPPED"      # 중단 완료 (terminal)
    COMPLETED = "COMPLETED"  # 전체 일정 정상 완주 (terminal)
    FAILED = "FAILED"        # 연결 실패 또는 런타임 오류 (terminal)


class SessionStatus(str, Enum):
    """세션 전체 집계 상태"""
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    PARTIAL_FAILED = "PARTIAL_FAILED"


def aggregate_session_status(statuses: list[HouseholdTaskStatus]) -> str:
    """
    다중 가구의 상태 목록을 받아 세션의 overall_state를 결정하는 순수 함수.
    우선순위:
    1. 하나라도 STARTING -> "STARTING"
    2. 하나라도 RUNNING, PAUSING, STOPPING -> "RUNNING"
    3. 활성 실행 없이 하나라도 PAUSED -> "PAUSED"
    4. 전원 COMPLETED -> "COMPLETED"
    5. 전원 FAILED -> "FAILED"
    6. 전원 STOPPED -> "STOPPED"
    7. terminal 혼합 상태이며 FAILED 존재 -> "PARTIAL_FAILED"
    8. COMPLETED와 STOPPED 혼합 (FAILED 없음) -> "STOPPED"
    """
    if not statuses:
        return "STOPPED"

    status_set = set(statuses)

    if HouseholdTaskStatus.STARTING in status_set:
        return "STARTING"

    if any(s in status_set for s in (HouseholdTaskStatus.RUNNING, HouseholdTaskStatus.PAUSING, HouseholdTaskStatus.STOPPING)):
        return "RUNNING"

    if HouseholdTaskStatus.PAUSED in status_set:
        return "PAUSED"

    # 모든 가구가 terminal 상태 (COMPLETED, STOPPED, FAILED)
    has_completed = HouseholdTaskStatus.COMPLETED in status_set
    has_stopped = HouseholdTaskStatus.STOPPED in status_set
    has_failed = HouseholdTaskStatus.FAILED in status_set

    if has_completed and not has_stopped and not has_failed:
        return "COMPLETED"
    if has_failed and not has_completed and not has_stopped:
        return "FAILED"
    if has_stopped and not has_completed and not has_failed:
        return "STOPPED"

    if has_failed:
        return "PARTIAL_FAILED"

    # COMPLETED + STOPPED 혼합 (FAILED 없음)
    return "STOPPED"


class HouseholdExecutionTask:
    """개별 가구의 독립 실행 태스크"""
    def __init__(self, household_id: str, scenario_id: str, plan: CompiledExecutionPlan):
        self.household_id = household_id
        self.scenario_id = scenario_id
        self.plan = plan
        self.executor: DeterministicScheduleExecutor | None = None
        self.asyncio_task: asyncio.Task | None = None
        self.stop_event: asyncio.Event | None = None
        self.state: HouseholdTaskStatus = HouseholdTaskStatus.STARTING
        self.last_error: str | None = None
        self.overall_result: Any | None = None
        self.completed_days: int = 0
        self._lock = threading.Lock()


class E2ESession:
    """단일 E2E 실행 세션"""
    def __init__(
        self,
        run_id: str,
        reference_date: date,
        execution_mode: ExecutionMode,
        speed_multiplier: float | None = None,
        start_time: str | None = None,
    ):
        self.run_id = run_id
        self.reference_date = reference_date
        self.execution_mode = execution_mode
        self.speed_multiplier = speed_multiplier
        self.start_time = start_time
        self.created_at = datetime.now(timezone.utc)
        self.tasks: dict[str, HouseholdExecutionTask] = {}
        self.overall_state: SessionStatus = SessionStatus.STARTING
        self._lock = threading.Lock()
        self.activation_state: SessionActivationState = SessionActivationState.PREPARING
        self.activation_lock = threading.Lock()
        self.activation_gate: asyncio.Event | None = None
        self.scheduled_start_event: asyncio.Event | None = None
        self.scheduled_task: asyncio.Task | None = None

    def is_active(self) -> bool:
        """세션이 아직 실행 중이거나 정지/대기 중인지 확인 (terminal 도달 여부)"""
        with self._lock:
            return self.overall_state in (
                SessionStatus.STARTING,
                SessionStatus.RUNNING,
                SessionStatus.PAUSED,
                SessionStatus.STOPPING,
            )


class DefaultMqttClientFactory:
    """실제 aiomqtt.Client 비동기 컨텍스트 매니저 팩토리"""
    def __init__(self, broker_config: dict):
        self.broker_config = broker_config
        self._tls_context = get_mqtt_tls_context(
            tls_enabled=broker_config.get("tls_enabled", False),
            ca_file=broker_config.get("ca_file")
        )

    def __call__(self, household_id: str) -> AsyncContextManager:
        client_kwargs: dict[str, Any] = {
            "hostname": self.broker_config["host"],
            "port": self.broker_config["port"],
            "username": self.broker_config.get("username"),
            "password": self.broker_config.get("password"),
        }
        if self._tls_context is not None:
            client_kwargs["tls_context"] = self._tls_context
        return aiomqtt.Client(**client_kwargs)


def get_effective_state(task: HouseholdExecutionTask) -> HouseholdTaskStatus:
    """
    외부 노출용 유효 상태 계산 (Side-effect 없는 순수 변환 함수):
    - task가 이미 terminal이면 해당 상태 사용
    - executor가 아직 없으면 wrapper 상태 사용
    - executor snapshot 상태와 동기화
    - MQTT context 종료 전에는 premature terminal 방지
    """
    with task._lock:
        if task.state in (HouseholdTaskStatus.COMPLETED, HouseholdTaskStatus.STOPPED, HouseholdTaskStatus.FAILED):
            return task.state

        if task.executor is None:
            return task.state

        if task.state == HouseholdTaskStatus.STARTING:
            return HouseholdTaskStatus.STARTING

        if task.state == HouseholdTaskStatus.STOPPING:
            return HouseholdTaskStatus.STOPPING

        snap = task.executor.snapshot()
        if snap.status == ExecutionStatus.PAUSED:
            return HouseholdTaskStatus.PAUSED
        elif snap.status == ExecutionStatus.RUNNING:
            if task.state == HouseholdTaskStatus.PAUSING:
                return HouseholdTaskStatus.PAUSING
            return HouseholdTaskStatus.RUNNING
        elif snap.status == ExecutionStatus.STOPPED:
            return HouseholdTaskStatus.STOPPING
        elif snap.status == ExecutionStatus.COMPLETED:
            return HouseholdTaskStatus.RUNNING

        return task.state


FINALIZER_DRAIN_TIMEOUT_SEC: float = 0.2


async def _drain_cancelled_tasks(
    tasks: set[asyncio.Task] | list[asyncio.Task],
    timeout_sec: float,
) -> set[asyncio.Task]:
    """
    이벤트 루프 종료 시 잔존 태스크에 대한 제한된 취소 수렴 대기(bounded drain).
    cancellation을 협조적으로 처리하는 태스크는 정상 종료되고 pending-task 경고가 방지되며,
    cancellation을 지연하거나 영구 무시하는 비협조 태스크가 있더라도
    무제한 gather 대기 없이 timeout_sec 내에 반환하여 루프 스레드 영구 정지를 방지합니다.
    (주의: 비협조적인 태스크가 있을 경우 제한 시간 초과 후에도 태스크가 미완료 상태로 남을 수 있으며,
    bounded drain 계약은 루프 스레드의 유한 시간 내 종료를 우선 보장합니다.)
    """
    if not tasks:
        return set()
    done, pending = await asyncio.wait(tasks, timeout=timeout_sec)
    for task in done:
        if not task.cancelled():
            try:
                task.exception()
            except Exception:
                pass
    return pending


class E2EScheduleSessionManager:
    """
    단일 활성 E2E 세션 관리자
    """
    def __init__(
        self,
        broker_config: dict | None = None,
        mqtt_client_factory: Callable[[str], AsyncContextManager] | None = None,
        broadcast_callback: Callable[[dict[str, Any]], Any] | None = None,
    ):
        self.broker_config = broker_config or resolve_mqtt_config()
        self._client_factory = mqtt_client_factory or DefaultMqttClientFactory(self.broker_config)
        self._broadcast_callback = broadcast_callback
        self._current_session: E2ESession | None = None
        self._session_lock = threading.Lock()
        self._is_shutting_down = False

        # 이벤트 루프 스레드 기동 및 준비 대기
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ready_event = threading.Event()
        self._loop_thread = threading.Thread(
            target=self._run_event_loop,
            daemon=True,
            name="E2ESessionManagerLoop"
        )
        self._loop_thread.start()

        if not self._ready_event.wait(timeout=5.0):
            with self._session_lock:
                self._is_shutting_down = True
                loop = self._loop
            if loop is not None:
                try:
                    loop.call_soon_threadsafe(loop.stop)
                except Exception:
                    pass
            if self._loop_thread is not None and self._loop_thread.is_alive():
                self._loop_thread.join(timeout=1.0)
            raise RuntimeError("E2E 전용 이벤트 루프 스레드 초기화 실패 (5.0초 타임아웃)")

    def _create_client_context(self, household_id: str) -> AsyncContextManager:
        """가구별 MQTT Client AsyncContextManager 인스턴스 생성 (단일 계약: factory(household_id))"""
        return self._client_factory(household_id)

    def _run_event_loop(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        with self._session_lock:
            self._loop = loop
            if self._is_shutting_down:
                loop.close()
                return
        self._ready_event.set()
        try:
            with self._session_lock:
                if self._is_shutting_down:
                    return
            loop.run_forever()
        finally:
            if not loop.is_closed():
                try:
                    pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
                    for t in pending:
                        t.cancel()
                    if pending:
                        loop.run_until_complete(
                            _drain_cancelled_tasks(pending, FINALIZER_DRAIN_TIMEOUT_SEC)
                        )
                except Exception:
                    pass
                loop.close()

    def is_active(self) -> bool:
        """현재 활성 상태인 E2E 세션이 존재하는지 확인"""
        with self._session_lock:
            if self._current_session is None:
                return False
            return self._current_session.is_active()

    def get_current_session(self) -> E2ESession | None:
        """현재 세션 참조 반환"""
        with self._session_lock:
            return self._current_session

    def create_and_start_session(self, params: dict, timeout_sec: float = 1.0) -> dict:
        """
        신규 세션을 생성하고 Two-Phase Activation Barrier를 통해 비동기 실행을 시작한 뒤 202 Accepted 응답 반환.
        HTTP 스레드: 검증, 스케줄 컴파일, 세션/태스크 객체 생성, 2단계 커밋 수행.
        """
        if self._is_shutting_down or self._loop is None or not self._loop.is_running() or not self._ready_event.is_set():
            raise RuntimeError("E2E manager가 종료되었거나 루프가 사용 불가능합니다.")

        ref_date_str = params["reference_date"]
        reference_date = datetime.strptime(ref_date_str, "%Y-%m-%d").date()

        exec_config_raw = params["execution"]
        mode_str = exec_config_raw["mode"]
        execution_mode = ExecutionMode(mode_str)
        speed_multiplier = exec_config_raw.get("speed")

        households_raw = params["households"]

        # ACCELERATED에서 speed를 생략하면 가구 수 기반 안전 배속을 자동 산출한다.
        # 무손실 상한은 가구당이 아닌 합계 기준이므로 가구 수로 나눈다.
        if execution_mode == ExecutionMode.ACCELERATED and speed_multiplier is None:
            speed_multiplier = resolve_auto_speed(len(households_raw))

        run_id = f"run_{uuid.uuid4().hex}"

        start_time_str = params.get("start_time")

        session = E2ESession(
            run_id=run_id,
            reference_date=reference_date,
            execution_mode=execution_mode,
            speed_multiplier=speed_multiplier,
            start_time=start_time_str,
        )

        # HTTP 스레드: 가구별 plan 컴파일 및 Task 래퍼 생성만 수행 (Executor는 E2E 루프에서 생성)
        for h_item in households_raw:
            h_id = h_item["household_id"]
            sc_id = h_item["scenario"]
            defn = get_unified_scenario_definition(sc_id)

            total_days = len(defn.days)
            base_date = resolve_base_date(reference_date, total_days)
            plan = compile_schedule(defn, base_date=base_date)

            task = HouseholdExecutionTask(household_id=h_id, scenario_id=sc_id, plan=plan)
            session.tasks[h_id] = task

        session.overall_state = SessionStatus.STARTING

        # Manager-level 단일 세션 보호 (원자적 확인 및 등록)
        with self._session_lock:
            if self._current_session is not None and self._current_session.is_active():
                raise E2EConflictError("이미 실행 중인 활성 E2E 세션이 존재합니다.")
            self._current_session = session

        # Phase 1: 루프에 세션 준비 코루틴 제출
        coro = self._prepare_session_on_loop(session)
        future = None
        try:
            future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        except Exception as exc:
            coro.close()
            with session.activation_lock:
                session.activation_state = SessionActivationState.ABORTED
            with self._session_lock:
                if self._current_session is session:
                    self._current_session = None
            raise RuntimeError(f"E2E manager가 종료되었거나 루프 제출 실패: {exc}") from exc

        # Phase 2: 준비 완료 대기 (타임아웃) 및 커밋
        try:
            future.result(timeout=timeout_sec)
        except (TimeoutError, asyncio.TimeoutError) as exc:
            with session.activation_lock:
                session.activation_state = SessionActivationState.ABORTED
            if future is not None:
                future.cancel()
            with self._session_lock:
                if self._current_session is session:
                    self._current_session = None
            if session.activation_gate is not None and self._loop is not None and self._loop.is_running():
                try:
                    self._loop.call_soon_threadsafe(session.activation_gate.set)
                except Exception:
                    pass
            if self._loop is not None and self._loop.is_running():
                try:
                    self._loop.call_soon_threadsafe(self._cleanup_aborted_session_on_loop, session)
                except Exception:
                    pass
            raise E2EStartTimeoutError(f"세션 활성화 준비 시간 초과 ({timeout_sec}s)") from exc
        except Exception as exc:
            with session.activation_lock:
                session.activation_state = SessionActivationState.ABORTED
            with self._session_lock:
                if self._current_session is session:
                    self._current_session = None
            if session.activation_gate is not None and self._loop is not None and self._loop.is_running():
                try:
                    self._loop.call_soon_threadsafe(session.activation_gate.set)
                except Exception:
                    pass
            if self._loop is not None and self._loop.is_running():
                try:
                    self._loop.call_soon_threadsafe(self._cleanup_aborted_session_on_loop, session)
                except Exception:
                    pass
            raise E2EStartError(f"세션 활성화 준비 실패: {exc}") from exc

        # 준비 성공 -> 커밋 및 게이트 개방
        with session.activation_lock:
            session.activation_state = SessionActivationState.COMMITTED

        try:
            if self._loop is None or not self._loop.is_running():
                raise RuntimeError("E2E_MANAGER_UNAVAILABLE")
            self._loop.call_soon_threadsafe(session.activation_gate.set)
        except Exception as schedule_err:
            with session.activation_lock:
                session.activation_state = SessionActivationState.ABORTED
            with self._session_lock:
                if self._current_session is session:
                    self._current_session = None
            if self._loop is not None and self._loop.is_running():
                try:
                    self._loop.call_soon_threadsafe(self._cleanup_aborted_session_on_loop, session)
                except Exception:
                    pass
            raise RuntimeError(f"E2E_MANAGER_UNAVAILABLE: 게이트 개방 예약 실패: {schedule_err}") from schedule_err

        return {
            "status": "accepted",
            "run_id": session.run_id,
            "state": "STARTING",
            "households": [
                {
                    "household_id": h_id,
                    "scenario": task.scenario_id,
                    "state": "STARTING",
                }
                for h_id, task in session.tasks.items()
            ],
        }

    async def _prepare_session_on_loop(self, session: E2ESession) -> None:
        """이벤트 루프 스레드 내에서 가구별 activation_gate 생성 및 태스크 스폰"""
        with self._session_lock:
            if self._current_session is not session:
                raise RuntimeError("세션 등록 상태 불일치")
        with session.activation_lock:
            if session.activation_state != SessionActivationState.PREPARING:
                raise RuntimeError("세션 활성화 상태가 PREPARING이 아닙니다.")

        session.activation_gate = asyncio.Event()
        session.scheduled_start_event = asyncio.Event()

        # start_time 예약 대기 지연 계산
        delay = resolve_start_time_delay(session.start_time)
        if delay <= 0.0:
            session.scheduled_start_event.set()
        else:
            session.scheduled_task = asyncio.create_task(
                self._wait_and_trigger_scheduled_start(session, delay),
                name=f"e2e_schedule_{session.run_id}"
            )

        try:
            for task in session.tasks.values():
                task.stop_event = asyncio.Event()
                task.asyncio_task = asyncio.create_task(
                    self._run_household_task(session, task),
                    name=f"e2e_{session.run_id}_{task.household_id}"
                )
        except Exception:
            if session.scheduled_task is not None and not session.scheduled_task.done():
                session.scheduled_task.cancel()
            with session._lock:
                session.overall_state = SessionStatus.FAILED
            raise

    async def _wait_and_trigger_scheduled_start(self, session: E2ESession, delay: float) -> None:
        """지정된 start_time 예약 시각까지 비동기 대기 후 scheduled_start_event 개방"""
        try:
            await asyncio.sleep(delay)
            if session.scheduled_start_event is not None:
                session.scheduled_start_event.set()
        except asyncio.CancelledError:
            pass

    def _cleanup_aborted_session_on_loop(self, session: E2ESession) -> None:
        """취소된 세션의 잔여 태스크 및 executor 정리"""
        if session.scheduled_task is not None and not session.scheduled_task.done():
            session.scheduled_task.cancel()
        for task in session.tasks.values():
            if task.stop_event is not None:
                task.stop_event.set()
            if task.executor is not None:
                task.executor.request_stop()
            if task.asyncio_task is not None and not task.asyncio_task.done():
                task.asyncio_task.cancel()

    async def _run_household_task(self, session: E2ESession, task: HouseholdExecutionTask) -> None:
        """
        개별 가구의 독립 실행 코루틴.
        E2E 이벤트 루프에서 Executor를 생성하고 activation_gate를 대기합니다.
        게이트 개방 후 COMMITTED 상태를 lock 아래에서 재확인한 뒤에만 MQTT context에 진입합니다.
        """
        executor_result = None
        run_error: Exception | None = None
        context_exit_error: Exception | None = None

        # E2E 루프 스레드 내에서 Executor 인스턴스화
        executor_config = ExecutorConfig(
            mode=session.execution_mode,
            speed_multiplier=session.speed_multiplier,
        )

        def make_day_cb(t: HouseholdExecutionTask):
            async def day_completed_callback(day_result):
                t.completed_days += 1
            return day_completed_callback

        task.executor = DeterministicScheduleExecutor(
            plan=task.plan,
            household_id=task.household_id,
            run_id=session.run_id,
            config=executor_config,
            day_completed_callback=make_day_cb(task),
            broadcast_callback=self._broadcast_callback,
        )

        # Gate 대기
        assert session.activation_gate is not None
        await session.activation_gate.wait()

        # Gate 개방 후 활성화 상태 및 현재 세션 확인 (동기화 lock 아래)
        with session.activation_lock:
            is_committed = (session.activation_state == SessionActivationState.COMMITTED)
        with self._session_lock:
            is_current = (self._current_session is session)

        if not is_committed or not is_current:
            # 타임아웃 또는 취소됨 -> MQTT client context 진입 금지, publish 금지
            with task._lock:
                task.state = HouseholdTaskStatus.STOPPED
            self._sync_session_state(session)
            return

        # 예약 시각 대기 (start_time이 미래 시각으로 지정된 경우)
        if session.scheduled_start_event is not None and not session.scheduled_start_event.is_set():
            start_waiter = asyncio.create_task(session.scheduled_start_event.wait())
            stop_waiter = asyncio.create_task(task.stop_event.wait()) if task.stop_event else None
            wait_tasks = [start_waiter]
            if stop_waiter is not None:
                wait_tasks.append(stop_waiter)

            try:
                done, pending = await asyncio.wait(wait_tasks, return_when=asyncio.FIRST_COMPLETED)
                for p in pending:
                    p.cancel()
            except asyncio.CancelledError:
                for w in wait_tasks:
                    w.cancel()
                with task._lock:
                    task.state = HouseholdTaskStatus.STOPPED
                self._sync_session_state(session)
                raise

            should_stop = False
            with task._lock:
                if task.state in (HouseholdTaskStatus.STOPPING, HouseholdTaskStatus.STOPPED):
                    task.state = HouseholdTaskStatus.STOPPED
                    should_stop = True

            if should_stop:
                self._sync_session_state(session)
                return

            with self._session_lock:
                is_not_current = (self._current_session is not session)
            if is_not_current:
                with task._lock:
                    task.state = HouseholdTaskStatus.STOPPED
                self._sync_session_state(session)
                return

        try:
            client_ctx = self._create_client_context(task.household_id)
            async with client_ctx as client:
                with task._lock:
                    if task.state == HouseholdTaskStatus.STARTING:
                        task.state = HouseholdTaskStatus.RUNNING
                self._sync_session_state(session)

                try:
                    executor_result = await task.executor.run(client)
                except asyncio.CancelledError:
                    raise
                except Exception as err:
                    run_error = err
        except asyncio.CancelledError:
            with task._lock:
                task.state = HouseholdTaskStatus.STOPPED
            self._sync_session_state(session)
            raise
        except Exception as exit_err:
            # context 진입 실패 또는 __aexit__ 실패 (TypeError 포함)
            context_exit_error = exit_err

        # Context가 완전히 종료된 후에만 최종 terminal 상태 확정
        with task._lock:
            if context_exit_error is not None:
                task.state = HouseholdTaskStatus.FAILED
                task.last_error = f"MQTTContextError: {context_exit_error}"
            elif run_error is not None:
                task.state = HouseholdTaskStatus.FAILED
                task.last_error = str(run_error)
            elif task.state == HouseholdTaskStatus.STOPPING:
                task.state = HouseholdTaskStatus.STOPPED
            elif executor_result is not None:
                task.state = HouseholdTaskStatus.COMPLETED
                task.overall_result = executor_result
            else:
                task.state = HouseholdTaskStatus.STOPPED

        self._sync_session_state(session)

    def _sync_session_state(self, session: E2ESession) -> None:
        """세션 내 모든 가구의 유효 상태를 집계하여 overall_state 갱신"""
        statuses = []
        for t in session.tasks.values():
            statuses.append(get_effective_state(t))
        new_status_str = aggregate_session_status(statuses)
        with session._lock:
            session.overall_state = SessionStatus(new_status_str)

        # 모든 가구가 terminal 상태(COMPLETED, STOPPED, FAILED)에 도달한 경우,
        # 미래 예약 시각 대기 타이머가 남아 있다면 즉시 취소하여 리소스 누수 방지
        terminal_statuses = (HouseholdTaskStatus.COMPLETED, HouseholdTaskStatus.STOPPED, HouseholdTaskStatus.FAILED)
        if all(s in terminal_statuses for s in statuses):
            if session.scheduled_task is not None and not session.scheduled_task.done():
                session.scheduled_task.cancel()

    async def _build_household_snapshot_on_loop(self, session: E2ESession, household_id: str) -> dict:
        """이벤트 루프 스레드 내에서 가구 스냅샷 생성"""
        task = session.tasks.get(household_id)
        if task is None:
            raise E2EHouseholdNotFoundError(f"가구 ID '{household_id}'를 찾을 수 없습니다.")

        effective_state = get_effective_state(task)

        settled_slots = 0
        runner_slots = 0
        published_samples = 0
        omitted_samples = 0
        current_activity_date = session.reference_date.isoformat()
        day_results_raw: list[Any] = []

        if task.executor is not None:
            snap = task.executor.snapshot()
            published_samples = snap.published_samples
            omitted_samples = snap.omitted_samples
            settled_slots = published_samples + omitted_samples
            runner_slots = snap.runner_virtual_slots

            if task.plan.day_plans:
                relative_index = min(max(0, snap.runner_virtual_slots), task.plan.total_virtual_slots - 1)
                day_index = relative_index // SECONDS_PER_DAY
                if 0 <= day_index < len(task.plan.day_plans):
                    current_activity_date = task.plan.day_plans[day_index].calendar_date.isoformat()

            day_results_raw = list(snap.completed_days)
        elif task.overall_result is not None and getattr(task.overall_result, "day_results", None):
            day_results_raw = list(task.overall_result.day_results)

        day_results = [
            d.to_dict() if hasattr(d, "to_dict") else {
                "scenario": d.scenario,
                "household_id": d.household_id,
                "run_id": d.run_id,
                "activity_date": d.activity_date,
                "published_samples": d.published_samples,
                "omitted_samples": d.omitted_samples,
                "status": d.status,
            }
            for d in day_results_raw
        ]

        total_days = len(task.plan.day_plans)
        planned_virtual_slots = task.plan.total_virtual_slots
        planned_publish_samples = task.plan.total_planned_publish_samples

        with task._lock:
            last_error = task.last_error
            completed_days = len(day_results) if task.executor is not None else task.completed_days

        return {
            "run_id": session.run_id,
            "household_id": task.household_id,
            "scenario": task.scenario_id,
            "scenario_id": task.scenario_id,
            "state": effective_state.value,
            "reference_date": session.reference_date.isoformat(),
            "start_time": session.start_time,
            "current_activity_date": current_activity_date,
            "current_day": current_activity_date,
            "execution_mode": session.execution_mode.value,
            "speed_multiplier": session.speed_multiplier,
            "settled_virtual_slots": settled_slots,
            "runner_virtual_slots": runner_slots,
            "current_slot": runner_slots,
            "planned_virtual_slots": planned_virtual_slots,
            "total_slots": planned_virtual_slots,
            "published_samples": published_samples,
            "planned_publish_samples": planned_publish_samples,
            "omitted_samples": omitted_samples,
            "completed_days": completed_days,
            "total_days": total_days,
            "last_error": last_error,
            "day_results": day_results,
            "daily_results": day_results,
        }

    async def _build_session_snapshot_on_loop(self, session: E2ESession) -> dict:
        """이벤트 루프 스레드 내에서 세션 전체 스냅샷 생성 및 최신 상태 즉시 집계"""
        household_snaps = []
        effective_states = []

        for h_id in sorted(session.tasks.keys()):
            h_snap = await self._build_household_snapshot_on_loop(session, h_id)
            household_snaps.append(h_snap)
            effective_states.append(HouseholdTaskStatus(h_snap["state"]))

        overall_status = aggregate_session_status(effective_states)
        with session._lock:
            session.overall_state = SessionStatus(overall_status)

        return {
            "status": "success",
            "run_id": session.run_id,
            "reference_date": session.reference_date.isoformat(),
            "start_time": session.start_time,
            "execution_mode": session.execution_mode.value,
            "speed_multiplier": session.speed_multiplier,
            "overall_status": overall_status,
            "overall_state": overall_status,
            "created_at": session.created_at.isoformat(),
            "households": household_snaps,
        }

    def get_session_snapshot(self, run_id: str, timeout_sec: float = 1.0) -> dict:
        """세션 전체 상태 불변 스냅샷 조회 (이벤트 루프 위임 및 타임아웃 보장)"""
        if self._is_shutting_down or self._loop is None or not self._loop.is_running():
            raise RuntimeError("E2E_MANAGER_UNAVAILABLE")

        with self._session_lock:
            session = self._current_session

        if session is None or session.run_id != run_id:
            raise E2ERunNotFoundError(f"실행 ID '{run_id}'를 찾을 수 없습니다.")

        future = None
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._build_session_snapshot_on_loop(session),
                self._loop,
            )
            return future.result(timeout=timeout_sec)
        except (TimeoutError, asyncio.TimeoutError) as exc:
            if future is not None:
                future.cancel()
            raise E2ESnapshotTimeoutError(f"세션 스냅샷 조회 시간 초과 ({timeout_sec}s)") from exc

    def get_household_snapshot(self, run_id: str, household_id: str, timeout_sec: float = 1.0) -> dict:
        """개별 가구 상세 실행 메트릭 스냅샷 조회 (이벤트 루프 위임 및 타임아웃 보장)"""
        if self._is_shutting_down or self._loop is None or not self._loop.is_running():
            raise RuntimeError("E2E_MANAGER_UNAVAILABLE")

        with self._session_lock:
            session = self._current_session

        if session is None or session.run_id != run_id:
            raise E2ERunNotFoundError(f"실행 ID '{run_id}'를 찾을 수 없습니다.")

        if household_id not in session.tasks:
            raise E2EHouseholdNotFoundError(f"가구 ID '{household_id}'를 찾을 수 없습니다.")

        future = None
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._build_household_snapshot_on_loop(session, household_id),
                self._loop,
            )
            return future.result(timeout=timeout_sec)
        except (TimeoutError, asyncio.TimeoutError) as exc:
            if future is not None:
                future.cancel()
            raise E2ESnapshotTimeoutError(f"가구 스냅샷 조회 시간 초과 ({timeout_sec}s)") from exc

    async def _execute_command_envelope_on_loop(self, envelope: CommandEnvelope) -> dict:
        """이벤트 루프 내에서 CommandEnvelope 검증, CLAIMED 전이, 동기 제어 및 완료 수행"""
        with envelope.lock:
            if envelope.status == CommandStatus.ABORTED:
                return {}

            with self._session_lock:
                session = self._current_session

            if session is None or session.run_id != envelope.run_id:
                envelope.error = E2ERunNotFoundError(f"실행 ID '{envelope.run_id}'를 찾을 수 없습니다.")
                envelope.status = CommandStatus.COMPLETED
                raise envelope.error

            if envelope.action in ("PAUSE", "RESUME", "STOP_HOUSEHOLD"):
                assert envelope.household_id is not None
                task = session.tasks.get(envelope.household_id)
                if task is None:
                    envelope.error = E2EHouseholdNotFoundError(f"가구 ID '{envelope.household_id}'를 찾을 수 없습니다.")
                    envelope.status = CommandStatus.COMPLETED
                    raise envelope.error

                effective_state = get_effective_state(task)
                if envelope.action == "PAUSE":
                    if effective_state in (HouseholdTaskStatus.COMPLETED, HouseholdTaskStatus.FAILED, HouseholdTaskStatus.STOPPED):
                        envelope.error = ValueError("TASK_ALREADY_TERMINAL")
                        envelope.status = CommandStatus.COMPLETED
                        raise envelope.error
                    if effective_state == HouseholdTaskStatus.STARTING:
                        envelope.error = ValueError("INVALID_STATE")
                        envelope.status = CommandStatus.COMPLETED
                        raise envelope.error
                    if effective_state == HouseholdTaskStatus.PAUSED:
                        accepted = {
                            "status": "success",
                            "run_id": envelope.run_id,
                            "household_id": envelope.household_id,
                            "requested_action": "PAUSE",
                            "current_state": "PAUSED",
                        }
                        envelope.accepted_result = accepted
                        envelope.result = accepted
                        envelope.status = CommandStatus.COMPLETED
                        return accepted
                    if effective_state == HouseholdTaskStatus.PAUSING:
                        accepted = {
                            "status": "accepted",
                            "run_id": envelope.run_id,
                            "household_id": envelope.household_id,
                            "requested_action": "PAUSE",
                            "current_state": "PAUSING",
                        }
                        envelope.accepted_result = accepted
                        envelope.result = accepted
                        envelope.status = CommandStatus.COMPLETED
                        return accepted
                    envelope.accepted_result = {
                        "status": "accepted",
                        "run_id": envelope.run_id,
                        "household_id": envelope.household_id,
                        "requested_action": "PAUSE",
                        "current_state": "PAUSING",
                    }
                elif envelope.action == "RESUME":
                    if effective_state in (HouseholdTaskStatus.COMPLETED, HouseholdTaskStatus.FAILED, HouseholdTaskStatus.STOPPED):
                        envelope.error = ValueError("TASK_ALREADY_TERMINAL")
                        envelope.status = CommandStatus.COMPLETED
                        raise envelope.error
                    if effective_state == HouseholdTaskStatus.STARTING:
                        envelope.error = ValueError("INVALID_STATE")
                        envelope.status = CommandStatus.COMPLETED
                        raise envelope.error
                    if effective_state == HouseholdTaskStatus.RUNNING:
                        accepted = {
                            "status": "success",
                            "run_id": envelope.run_id,
                            "household_id": envelope.household_id,
                            "requested_action": "RESUME",
                            "current_state": "RUNNING",
                        }
                        envelope.accepted_result = accepted
                        envelope.result = accepted
                        envelope.status = CommandStatus.COMPLETED
                        return accepted
                    envelope.accepted_result = {
                        "status": "accepted",
                        "run_id": envelope.run_id,
                        "household_id": envelope.household_id,
                        "requested_action": "RESUME",
                        "current_state": "RUNNING",
                    }
                elif envelope.action == "STOP_HOUSEHOLD":
                    if effective_state == HouseholdTaskStatus.STOPPED:
                        accepted = {
                            "status": "success",
                            "run_id": envelope.run_id,
                            "household_id": envelope.household_id,
                            "requested_action": "STOP",
                            "current_state": "STOPPED",
                        }
                        envelope.accepted_result = accepted
                        envelope.result = accepted
                        envelope.status = CommandStatus.COMPLETED
                        return accepted
                    if effective_state in (HouseholdTaskStatus.COMPLETED, HouseholdTaskStatus.FAILED):
                        envelope.error = ValueError("TASK_ALREADY_TERMINAL")
                        envelope.status = CommandStatus.COMPLETED
                        raise envelope.error
                    if effective_state == HouseholdTaskStatus.STOPPING:
                        accepted = {
                            "status": "accepted",
                            "run_id": envelope.run_id,
                            "household_id": envelope.household_id,
                            "requested_action": "STOP",
                            "current_state": "STOPPING",
                        }
                        envelope.accepted_result = accepted
                        envelope.result = accepted
                        envelope.status = CommandStatus.COMPLETED
                        return accepted
                    envelope.accepted_result = {
                        "status": "accepted",
                        "run_id": envelope.run_id,
                        "household_id": envelope.household_id,
                        "requested_action": "STOP",
                        "current_state": "STOPPING",
                    }
            elif envelope.action == "STOP_SESSION":
                envelope.accepted_result = {
                    "status": "accepted",
                    "run_id": envelope.run_id,
                    "requested_action": "STOP",
                }

            # 검증 성공 -> CLAIMED 전이 및 accepted_result 선행 기록 확정
            envelope.status = CommandStatus.CLAIMED

        # 동기식 상태 변경 및 executor 제어 (await나 외부 I/O 없음)
        try:
            if envelope.action == "PAUSE":
                task = session.tasks[envelope.household_id]
                with task._lock:
                    task.state = HouseholdTaskStatus.PAUSING
                    if task.executor is not None:
                        task.executor.pause()
                self._sync_session_state(session)
            elif envelope.action == "RESUME":
                task = session.tasks[envelope.household_id]
                with task._lock:
                    if task.executor is not None:
                        task.executor.resume()
                    task.state = HouseholdTaskStatus.RUNNING
                self._sync_session_state(session)
            elif envelope.action == "STOP_HOUSEHOLD":
                task = session.tasks[envelope.household_id]
                with task._lock:
                    task.state = HouseholdTaskStatus.STOPPING
                    if task.executor is not None:
                        task.executor.request_stop()
                    if task.stop_event is not None:
                        task.stop_event.set()
                    elif task.asyncio_task is not None and not task.asyncio_task.done():
                        task.asyncio_task.cancel()
                self._sync_session_state(session)
                non_terminal = [
                    t for t in session.tasks.values()
                    if get_effective_state(t) not in (HouseholdTaskStatus.COMPLETED, HouseholdTaskStatus.FAILED, HouseholdTaskStatus.STOPPED, HouseholdTaskStatus.STOPPING)
                ]
                if not non_terminal:
                    if session.scheduled_task is not None and not session.scheduled_task.done():
                        session.scheduled_task.cancel()
            elif envelope.action == "STOP_SESSION":
                if session.scheduled_task is not None and not session.scheduled_task.done():
                    session.scheduled_task.cancel()
                for t in session.tasks.values():
                    eff = get_effective_state(t)
                    if eff not in (HouseholdTaskStatus.COMPLETED, HouseholdTaskStatus.FAILED, HouseholdTaskStatus.STOPPED):
                        with t._lock:
                            t.state = HouseholdTaskStatus.STOPPING
                            if t.executor is not None:
                                t.executor.request_stop()
                            if t.stop_event is not None:
                                t.stop_event.set()
                            elif t.asyncio_task is not None and not t.asyncio_task.done():
                                t.asyncio_task.cancel()
                self._sync_session_state(session)

            with envelope.lock:
                envelope.result = envelope.accepted_result
                envelope.status = CommandStatus.COMPLETED
                return envelope.result
        except Exception as exc:
            with envelope.lock:
                envelope.error = exc
                envelope.status = CommandStatus.COMPLETED
                raise

    def _dispatch_command(self, envelope: CommandEnvelope, timeout_sec: float) -> dict:
        """제어 명령을 이벤트 루프에 제출하고 CommandEnvelope 상태에 따라 결과를 반환하는 공통 헬퍼"""
        if self._is_shutting_down or self._loop is None or not self._loop.is_running():
            raise RuntimeError("E2E_MANAGER_UNAVAILABLE")

        coro = self._execute_command_envelope_on_loop(envelope)
        future = None
        try:
            future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        except Exception:
            coro.close()
            raise

        try:
            return future.result(timeout=timeout_sec)
        except (TimeoutError, asyncio.TimeoutError) as exc:
            with envelope.lock:
                if envelope.status == CommandStatus.QUEUED:
                    envelope.status = CommandStatus.ABORTED
                    if future is not None:
                        future.cancel()
                    raise E2EOperationTimeoutError(f"작업 처리 시간 초과 ({timeout_sec}s)") from exc
                elif envelope.status == CommandStatus.CLAIMED:
                    # 루프가 이미 검증을 통과하고 실행권을 획득하였으므로 취소하지 않고 accepted_result 반환
                    assert envelope.accepted_result is not None
                    return envelope.accepted_result
                else:  # COMPLETED
                    if envelope.error is not None:
                        raise envelope.error
                    assert envelope.result is not None
                    return envelope.result

    def pause_household(self, run_id: str, household_id: str, timeout_sec: float = 1.0) -> dict:
        """개별 가구 일시정지 (루프 스레드 위임 및 Atomic CommandEnvelope 적용)"""
        envelope = CommandEnvelope(action="PAUSE", run_id=run_id, household_id=household_id)
        return self._dispatch_command(envelope, timeout_sec)

    def resume_household(self, run_id: str, household_id: str, timeout_sec: float = 1.0) -> dict:
        """개별 가구 재개 (루프 스레드 위임 및 Atomic CommandEnvelope 적용)"""
        envelope = CommandEnvelope(action="RESUME", run_id=run_id, household_id=household_id)
        return self._dispatch_command(envelope, timeout_sec)

    def stop_household(self, run_id: str, household_id: str, timeout_sec: float = 1.0) -> dict:
        """개별 가구 중단 (루프 스레드 위임 및 Atomic CommandEnvelope 적용)"""
        envelope = CommandEnvelope(action="STOP_HOUSEHOLD", run_id=run_id, household_id=household_id)
        return self._dispatch_command(envelope, timeout_sec)

    def stop_session(self, run_id: str, timeout_sec: float = 1.0) -> dict:
        """세션 전체 일괄 중단 (루프 스레드 위임 및 Atomic CommandEnvelope 적용)"""
        envelope = CommandEnvelope(action="STOP_SESSION", run_id=run_id)
        return self._dispatch_command(envelope, timeout_sec)

    async def _shutdown_on_loop(self, session: E2ESession | None, deadline: float) -> None:
        """이벤트 루프 내에서 실행 중인 태스크 및 executor 정리 (2단계 bounded asyncio.wait, wait_for(gather) 미사용)"""
        if session is not None:
            if session.scheduled_task is not None and not session.scheduled_task.done():
                session.scheduled_task.cancel()
            for task in session.tasks.values():
                if task.stop_event is not None:
                    task.stop_event.set()
                if task.executor is not None:
                    task.executor.request_stop()
                if task.asyncio_task is not None and not task.asyncio_task.done():
                    task.asyncio_task.cancel()

            active_tasks = [
                t.asyncio_task for t in session.tasks.values()
                if t.asyncio_task is not None and not t.asyncio_task.done()
            ]
            if active_tasks:
                remaining = max(0.0, deadline - time.monotonic())
                if remaining > 0:
                    done, pending = await asyncio.wait(active_tasks, timeout=remaining)
                else:
                    pending = set(active_tasks)

                for p in pending:
                    p.cancel()

                remaining = max(0.0, deadline - time.monotonic())
                if remaining > 0 and pending:
                    await asyncio.wait(pending, timeout=remaining)

        # loop 내 다른 task 정리
        loop = asyncio.get_running_loop()
        other_tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task() and not t.done()]
        if other_tasks:
            for t in other_tasks:
                t.cancel()
            remaining = max(0.0, deadline - time.monotonic())
            if remaining > 0:
                await asyncio.wait(other_tasks, timeout=remaining)

    def shutdown(self, timeout_sec: float = 5.0) -> bool:
        """
        Bounded 셧다운 (단일 monotonic deadline, 2단계 bounded asyncio.wait).
        모든 executor 제어 및 태스크 정리는 이벤트 루프 소유권 하에서만 수행.
        """
        start_mono = time.monotonic()
        deadline = start_mono + timeout_sec

        with self._session_lock:
            if self._is_shutting_down:
                remaining = max(0.0, deadline - time.monotonic())
                if self._loop_thread is not None and self._loop_thread.is_alive() and remaining > 0:
                    self._loop_thread.join(timeout=remaining)
                return not (self._loop_thread is not None and self._loop_thread.is_alive())
            self._is_shutting_down = True
            session = self._current_session

        if self._loop is not None and self._loop.is_running():
            remaining = max(0.0, deadline - time.monotonic())
            if remaining > 0:
                coro = self._shutdown_on_loop(session, deadline)
                future = None
                try:
                    future = asyncio.run_coroutine_threadsafe(coro, self._loop)
                    future.result(timeout=remaining)
                except (TimeoutError, asyncio.TimeoutError):
                    if future is not None:
                        future.cancel()
                except Exception:
                    pass

            self._loop.call_soon_threadsafe(self._loop.stop)

        remaining = max(0.0, deadline - time.monotonic())
        if self._loop_thread is not None and self._loop_thread.is_alive() and remaining > 0:
            self._loop_thread.join(timeout=remaining)

        return not (self._loop_thread is not None and self._loop_thread.is_alive())
