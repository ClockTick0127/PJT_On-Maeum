"""
NILM 스마트홈 전력 시뮬레이터 결정론적 일정 실행기 모듈 (Deterministic Schedule Runner)

- CompiledExecutionPlan을 가구별로 1초 가상 슬롯 단위로 지연 실행(Lazy Iteration)
- 전역 상태 및 전역 random 배제, SHA-256 기반 가구별 격리 시드 및 독립 random.Random 인스턴스 소유
- 메인 분전반 다변량 물리 계측값(P, Q, S, PF, V, I)의 엄격한 계산 순서 및 반올림 보장
- O(D + E + R + A) 전체 메모리 복잡도 및 O(A + R_day + T_cycle) 슬롯당 시간 복잡도 유지
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import math
import random
from typing import Any

from .profiles import DEVICE_PROFILES
from .schedule import (
    KST,
    SUPPORTED_APPLIANCES,
    ApplianceTransition,
    CompiledExecutionPlan,
    ScheduleError,
    TransitionType,
)


class ExecutionCompletedError(ScheduleError):
    """모든 가상 슬롯 처리가 완료된 후(is_completed == True) 추가 step() 호출 시 발생하는 도메인 예외"""
    pass


@dataclass(frozen=True)
class ScheduledTick:
    """단일 가상 슬롯(1초)의 결정론적 물리 계측 및 일정 실행 결과 (불변)"""
    household_id: str
    cycle: int
    measured_at: datetime
    is_publish_candidate: bool
    transitions: tuple[ApplianceTransition, ...]
    active_appliances: tuple[str, ...]
    active_power: float
    reactive_power: float
    apparent_power: float
    power_factor: float
    voltage: float
    current: float

    def to_dict(self) -> dict[str, Any]:
        """
        순수 필드 직렬화 딕셔너리 반환.
        반환 객체는 순수 JSON 타입(str, int, float, bool, list, dict)만 포함하며,
        dataclass, Enum, datetime, tuple 객체가 일절 남지 않아 json.dumps()가 즉시 성공함.
        (MQTT 토픽, message_id, QoS, ts 등 브로커 연동 필드는 포함하지 않음)
        """
        return {
            "household_id": self.household_id,
            "cycle": self.cycle,
            "measured_at": self.measured_at.isoformat(),
            "is_publish_candidate": self.is_publish_candidate,
            "transitions": [
                {
                    "transition_type": t.transition_type.value,
                    "appliance": t.appliance,
                    "second_of_day": t.second_of_day,
                    "absolute_cycle": t.absolute_cycle,
                    "virtual_time": t.virtual_time.isoformat(),
                }
                for t in self.transitions
            ],
            "active_appliances": list(self.active_appliances),
            "active_power": self.active_power,
            "reactive_power": self.reactive_power,
            "apparent_power": self.apparent_power,
            "power_factor": self.power_factor,
            "voltage": self.voltage,
            "current": self.current,
        }


@dataclass(frozen=True)
class RunnerSnapshot:
    """실행기 진행 상태 스냅샷 (불변)"""
    household_id: str
    last_processed_cycle: int | None
    next_cycle: int
    first_cycle: int
    last_cycle: int
    total_virtual_slots: int
    processed_virtual_slots: int
    generated_publish_candidates: int
    omitted_slots: int
    planned_publish_samples: int
    planned_omitted_samples: int
    is_completed: bool
    active_appliances: tuple[str, ...]


@dataclass
class _StandbyEnvironmentState:
    """단일 가구의 전압 및 대기전력 연속 환경 런타임 상태"""
    voltage: float
    base_nominal_w: float
    base_current_w: float
    fridge_active: bool
    fridge_remaining_sec: int
    fridge_nominal_w: float
    fridge_pf: float


@dataclass
class _ApplianceRuntimeState:
    """
    단일 가전의 런타임 상태 머신 객체.

    일정 기반 실행에서는 선언된 ON 구간이 곧 '사람이 가전을 사용한 구간'이므로
    서모스탯/인버터 듀티 사이클(휴지 구간)을 적용하지 않고 연속 가동으로 유지한다.
    따라서 RESTING 상태와 duty 잔여 카운터가 없다.
    (확률 기반 레거시 시뮬레이터의 듀티 사이클은 engine/power_model.py에 그대로 남아 있다.)
    """
    state: str = "OFF"  # "OFF", "STARTING", "RUNNING"
    nominal_w: float = 0.0
    nominal_pf: float = 0.0
    inrush_remaining: int = 0

    @property
    def is_active(self) -> bool:
        return self.state in ("STARTING", "RUNNING")


def derive_household_seed(
    plan: CompiledExecutionPlan,
    household_id: str,
    seed: int | str | None,
) -> int:
    """
    타입 태그(Type Tag)를 포함하여 정수와 문자열 충돌 없이 가구별 고유 64비트 정수 시드 파생.
    """
    if seed is None:
        typed_seed = "NONE"
    elif isinstance(seed, int):
        typed_seed = f"INT:{seed}"
    elif isinstance(seed, str):
        typed_seed = f"STR:{seed}"
    else:
        raise TypeError(f"seed는 int, str, None만 허용됩니다: {type(seed).__name__}")

    seed_string = (
        f"SCHEDULE_RUNNER_V1::{typed_seed}::{plan.scenario_id}::"
        f"{plan.start_date.isoformat()}::{plan.end_date.isoformat()}::"
        f"{plan.total_virtual_slots}::{household_id}"
    )
    digest = hashlib.sha256(seed_string.encode("utf-8")).hexdigest()
    return int(digest[:16], 16)


class DeterministicScheduleRunner:
    """
    단일 가구에 대해 CompiledExecutionPlan을 1초 가상 슬롯 단위로 지연 실행하는 결정론적 런타임.
    - 외부 전역 상태(random, global dict) 배제 및 가구별 독립 RNG 보장
    - O(A) 실행기 상태 메모리 유지 (사전 생성 배열 전무)
    - iterator 프로토콜 (__iter__, __next__) 및 명시적 step() 지원
    """

    def __init__(
        self,
        plan: CompiledExecutionPlan,
        household_id: str,
        seed: int | str | None = None,
    ):
        # 1. 생성자 입력 엄격 검증
        if not isinstance(plan, CompiledExecutionPlan):
            raise TypeError(f"plan은 CompiledExecutionPlan 인스턴스여야 합니다: {type(plan).__name__}")
        if not isinstance(household_id, str) or isinstance(household_id, bool):
            raise TypeError(f"household_id는 str이어야 합니다: {type(household_id).__name__}")
        if len(household_id) == 0:
            raise ValueError("household_id는 비어 있을 수 없습니다.")
        if household_id != household_id.strip():
            raise ValueError(f"household_id는 앞뒤 공백을 포함할 수 없습니다: {household_id!r}")

        if seed is not None:
            if isinstance(seed, bool):
                raise TypeError("seed는 bool일 수 없습니다.")
            elif isinstance(seed, int):
                pass
            elif isinstance(seed, str):
                if len(seed) == 0:
                    raise ValueError("seed 문자열은 비어 있을 수 없습니다.")
                if seed != seed.strip():
                    raise ValueError(f"seed 문자열은 앞뒤 공백을 포함할 수 없습니다: {seed!r}")
            else:
                raise TypeError(f"seed는 int, str, None만 허용됩니다: {type(seed).__name__}")

        self._plan = plan
        self._household_id = household_id
        self._raw_seed = seed
        self._initial_seed = derive_household_seed(plan, household_id, seed)

        # 2. 공통 초기화 루틴 수행
        self._initialize_state()

    def _initialize_state(self) -> None:
        """새 Random 인스턴스 및 초기 환경/가전 객체 생성 루틴 (생성자 및 reset 공유)"""
        # random.Random.gauss()의 내부 캐시까지 완벽히 초기화되도록 새 Random 객체 생성
        self._rng = random.Random(self._initial_seed)

        # 7개 초기 난수 순차 호출
        v_raw = round(self._rng.gauss(220.0, 1.2), 1)
        init_voltage = max(212.0, min(228.0, v_raw))
        init_base_nom = self._rng.uniform(40.0, 65.0)
        init_base_cur = self._rng.uniform(45.0, 60.0)
        init_fridge_act = (self._rng.random() < 0.4)
        init_fridge_rem = self._rng.randint(300, 1200)
        init_fridge_nom = self._rng.uniform(55.0, 85.0)
        init_fridge_pf = self._rng.uniform(0.76, 0.82)

        self._env = _StandbyEnvironmentState(
            voltage=init_voltage,
            base_nominal_w=init_base_nom,
            base_current_w=init_base_cur,
            fridge_active=init_fridge_act,
            fridge_remaining_sec=init_fridge_rem,
            fridge_nominal_w=init_fridge_nom,
            fridge_pf=init_fridge_pf,
        )

        self._app_states: dict[str, _ApplianceRuntimeState] = {
            app: _ApplianceRuntimeState() for app in SUPPORTED_APPLIANCES
        }

        self._last_processed_cycle: int | None = None
        self._next_cycle: int = self._plan.first_cycle
        self._processed_virtual_slots: int = 0
        self._generated_publish_candidates: int = 0
        self._omitted_slots: int = 0
        self._is_completed: bool = False

    @property
    def plan(self) -> CompiledExecutionPlan:
        return self._plan

    @property
    def household_id(self) -> str:
        return self._household_id

    @property
    def is_completed(self) -> bool:
        return self._is_completed

    @property
    def processed_virtual_slots(self) -> int:
        return self._processed_virtual_slots

    @property
    def generated_publish_candidates(self) -> int:
        return self._generated_publish_candidates

    @property
    def omitted_slots(self) -> int:
        return self._omitted_slots

    def reset(self) -> None:
        """실행기 상태를 최초 생성 시점과 100% 동일하게 복원"""
        self._initialize_state()

    def snapshot(self) -> RunnerSnapshot:
        """현재 실행기 상태의 불변 스냅샷 반환"""
        active_tuple = tuple(
            app for app in SUPPORTED_APPLIANCES if self._app_states[app].is_active
        )
        return RunnerSnapshot(
            household_id=self._household_id,
            last_processed_cycle=self._last_processed_cycle,
            next_cycle=self._next_cycle,
            first_cycle=self._plan.first_cycle,
            last_cycle=self._plan.last_cycle,
            total_virtual_slots=self._plan.total_virtual_slots,
            processed_virtual_slots=self._processed_virtual_slots,
            generated_publish_candidates=self._generated_publish_candidates,
            omitted_slots=self._omitted_slots,
            planned_publish_samples=self._plan.total_planned_publish_samples,
            planned_omitted_samples=self._plan.total_planned_omitted_samples,
            is_completed=self._is_completed,
            active_appliances=active_tuple,
        )

    def step(self) -> ScheduledTick:
        """단일 가상 슬롯(1초)을 결정적으로 계산하고 불변 ScheduledTick 반환"""
        if self._is_completed:
            raise ExecutionCompletedError("실행 계획의 모든 가상 슬롯 처리가 완료되었습니다.")

        cycle = self._next_cycle
        virtual_time = self._plan.virtual_time_at(cycle)

        # 1. 스케줄 전이 적용 (plan.transitions_at 순서 그대로 보존)
        transitions = self._plan.transitions_at(cycle)
        for t in transitions:
            app_state = self._app_states[t.appliance]
            profile = DEVICE_PROFILES[t.appliance]
            if t.transition_type == TransitionType.OFF:
                app_state.state = "OFF"
                app_state.nominal_w = 0.0
                app_state.nominal_pf = 0.0
                app_state.inrush_remaining = 0
            elif t.transition_type == TransitionType.ON:
                app_state.state = "STARTING"
                app_state.nominal_w = profile["median_w"]  # EDA 실측 중앙값 고정
                app_state.nominal_pf = self._rng.uniform(*profile["pf_nominal"])
                app_state.inrush_remaining = profile["inrush_sec"]

        # 2. 전압 AR-1 드리프트
        noise_v = self._rng.gauss(0, 0.12)
        raw_v = 0.98 * self._env.voltage + 0.02 * 220.0 + noise_v
        self._env.voltage = max(212.0, min(228.0, raw_v))

        # 3. 상시 기저 대기전력 Random Walk
        noise_base = self._rng.gauss(0, 0.2)
        self._env.base_current_w = (
            0.96 * self._env.base_current_w + 0.04 * self._env.base_nominal_w + noise_base
        )
        base_p = max(20.0, self._env.base_current_w)
        base_pf = 0.92
        base_q = base_p * math.sqrt(1.0 - base_pf * base_pf) / base_pf

        # 4. 냉장고 컴프레서
        self._env.fridge_remaining_sec -= 1
        if self._env.fridge_remaining_sec <= 0:
            self._env.fridge_active = not self._env.fridge_active
            if self._env.fridge_active:
                self._env.fridge_remaining_sec = self._rng.randint(900, 1500)
            else:
                self._env.fridge_remaining_sec = self._rng.randint(1200, 2100)

        if self._env.fridge_active:
            noise_fridge = self._rng.gauss(0, 0.8)
            fridge_p = max(0.0, self._env.fridge_nominal_w + noise_fridge)
            fridge_pf = self._env.fridge_pf
            fridge_q = fridge_p * math.sqrt(1.0 - fridge_pf * fridge_pf) / fridge_pf
        else:
            fridge_p = 0.0
            fridge_q = 0.0

        standby_p = base_p + fridge_p
        standby_q = base_q + fridge_q

        # 5. 6대 가전 부하 (SUPPORTED_APPLIANCES 고정 순서)
        total_app_p = 0.0
        total_app_q = 0.0
        for app in SUPPORTED_APPLIANCES:
            app_state = self._app_states[app]
            profile = DEVICE_PROFILES[app]

            if app_state.state == "STARTING":
                p_raw = app_state.nominal_w * profile["inrush_factor"] + self._rng.gauss(0, 2.0)
                pf_raw = max(0.40, app_state.nominal_pf - 0.08)
                app_state.inrush_remaining -= 1
                if app_state.inrush_remaining <= 0:
                    app_state.state = "RUNNING"
            elif app_state.state == "RUNNING":
                # 듀티 사이클 가전(induction, iron)도 선언된 ON 구간 동안에는
                # 휴지 없이 연속 가동한다. 프로파일별 노이즈 특성만 구분한다.
                if profile["type"] == "single_block":
                    p_raw = app_state.nominal_w + self._rng.gauss(0, 1.2)
                    pf_raw = app_state.nominal_pf + self._rng.gauss(0, 0.003)
                else:  # duty_cycle
                    p_raw = app_state.nominal_w + self._rng.gauss(0, 1.5)
                    pf_raw = app_state.nominal_pf
            else:  # OFF
                low, high = profile["standby_w"]
                p_raw = self._rng.uniform(low, high) if high > 0 else 0.0
                pf_raw = 0.95

            p_dev = max(0.0, p_raw)
            pf_dev = max(0.10, min(0.999, pf_raw))
            q_dev = p_dev * math.sqrt(1.0 - pf_dev * pf_dev) / pf_dev if p_dev > 0.1 else 0.0

            total_app_p += p_dev
            total_app_q += q_dev

        # 6. 센서 노이즈
        noise_main_p = self._rng.gauss(0, 0.8)
        noise_main_q = self._rng.gauss(0, 0.8)
        raw_P = standby_p + total_app_p + noise_main_p
        raw_Q = standby_q + total_app_q + noise_main_q
        raw_V = self._env.voltage

        # 7. 계측값 계산 및 반올림 순서 고정
        P_out = round(max(0.0, raw_P), 2)
        Q_out = round(max(0.0, raw_Q), 2)
        V_out = round(max(212.0, min(228.0, raw_V)), 1)
        S_out = round(math.sqrt(P_out * P_out + Q_out * Q_out), 2)
        if S_out > 1e-4:
            raw_pf = P_out / S_out
        else:
            raw_pf = 1.0
        PF_out = round(max(0.0, min(1.0, raw_pf)), 3)
        I_out = round(S_out / V_out, 3)

        # 8. 활성 가전 목록 (SUPPORTED_APPLIANCES 순서)
        active_appliances = tuple(
            app for app in SUPPORTED_APPLIANCES if self._app_states[app].is_active
        )

        # 9. 발행 후보 여부
        is_publish_candidate = self._plan.should_publish(cycle)

        # 10. 카운터 및 사이클 전진
        self._processed_virtual_slots += 1
        if is_publish_candidate:
            self._generated_publish_candidates += 1
        else:
            self._omitted_slots += 1

        self._last_processed_cycle = cycle
        self._next_cycle = cycle + 1
        if self._processed_virtual_slots >= self._plan.total_virtual_slots:
            self._is_completed = True

        return ScheduledTick(
            household_id=self._household_id,
            cycle=cycle,
            measured_at=virtual_time,
            is_publish_candidate=is_publish_candidate,
            transitions=transitions,
            active_appliances=active_appliances,
            active_power=P_out,
            reactive_power=Q_out,
            apparent_power=S_out,
            power_factor=PF_out,
            voltage=V_out,
            current=I_out,
        )

    def __iter__(self) -> "DeterministicScheduleRunner":
        return self

    def __next__(self) -> ScheduledTick:
        if self._is_completed:
            raise StopIteration
        return self.step()
