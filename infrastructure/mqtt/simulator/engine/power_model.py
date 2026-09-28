"""
NILM 스마트홈 전력 시뮬레이터 물리 엔진 및 전력 모델링 모듈

가전 부하/돌입전류/듀티사이클 FSM 시뮬레이션 및
메인 분전반 다변량 교류 전력(P, Q, S, PF, V, I) 계산을 전담합니다.
"""

import math

from .profiles import DEVICE_PROFILES
from .state import house_states, device_states, get_house_rng

try:
    from scenarios import (
        update_standby_environment,
        inject_peak_scenario_event as _inject_peak_scenario_event,
        inject_normal_routine_scenario_event as _inject_normal_routine_scenario_event,
        inject_prolonged_use_scenario_event as _inject_prolonged_use_scenario_event,
    )
except ModuleNotFoundError:
    # engine 패키지가 상대 패키지 내부에서 독립 임포트될 경우를 위한 fallback
    from ..scenarios import (
        update_standby_environment,
        inject_peak_scenario_event as _inject_peak_scenario_event,
        inject_normal_routine_scenario_event as _inject_normal_routine_scenario_event,
        inject_prolonged_use_scenario_event as _inject_prolonged_use_scenario_event,
    )


def inject_peak_scenario_event(cycle_sec: int, house: str) -> str | None:
    """피크 시연 시나리오 타임라인 이벤트 주입 (scenarios 모듈 위임)"""
    return _inject_peak_scenario_event(cycle_sec, house, device_states)


def inject_normal_routine_scenario_event(cycle_sec: int, house: str) -> str | None:
    """정상 루틴 시나리오 타임라인 이벤트 주입 (scenarios 모듈 위임)"""
    return _inject_normal_routine_scenario_event(cycle_sec, house, device_states)


def inject_prolonged_use_scenario_event(cycle_sec: int, house: str) -> str | None:
    """사용시간 초과 시나리오 타임라인 이벤트 주입 (scenarios 모듈 위임)"""
    return _inject_prolonged_use_scenario_event(cycle_sec, house, device_states)


def update_house_environment(house: str) -> tuple[float, float, float]:
    """가구별 전압(V) 드리프트 및 순수 대기전력 + 냉장고 컴프레서 주기 계산"""
    return update_standby_environment(house_states[house], rng=get_house_rng(house))


def update_and_generate_device_load(house: str, device: str, allow_random: bool = True) -> tuple[float, float, bool]:
    """
    가전 상태 머신 전이 및 파형 특성(돌입전류 오버슈트, 듀티사이클) 계산
    반환: (P, Q, is_active)
    """
    profile = DEVICE_PROFILES[device]
    state = device_states[house][device]
    rng = get_house_rng(house)

    if state["state"] == "OFF":
        # 가전 켜짐 트리거 확인 (랜덤 트리거 허용 시에만 자동 켜짐)
        if allow_random and rng.random() < profile["turn_on_prob"]:
            dur_min, dur_max = profile["session_sec"]
            state["session_remaining"] = rng.randint(dur_min, dur_max)
            state["nominal_w"] = profile["median_w"]  # EDA 실측 중앙값 고정
            state["nominal_pf"] = rng.uniform(*profile["pf_nominal"])
            state["state"] = "STARTING"
            state["manual_hold"] = False
            state["inrush_remaining"] = profile["inrush_sec"]

            if profile["type"] == "duty_cycle":
                state["is_heating"] = True
                state["duty_remaining"] = rng.randint(*profile["duty_on_sec"])

    p = 0.0
    pf = 0.95
    is_active = False

    if state["state"] == "STARTING":
        is_active = True
        # 1. 돌입전류(Inrush / Overshoot) 단계: 피크 전력 + 순간 역률 저하
        p = state["nominal_w"] * profile["inrush_factor"] + rng.gauss(0, 2.0)
        pf = max(0.40, state["nominal_pf"] - 0.08)

        state["inrush_remaining"] -= 1
        if not state.get("manual_hold", False):
            state["session_remaining"] -= 1

        if state["inrush_remaining"] <= 0:
            state["state"] = "RUNNING"
        if not state.get("manual_hold", False) and state["session_remaining"] <= 0:
            state["state"] = "OFF"

    elif state["state"] == "RUNNING":
        if not state.get("manual_hold", False):
            state["session_remaining"] -= 1
            if state["session_remaining"] <= 0:
                state["state"] = "OFF"

        if state["state"] == "RUNNING":
            if profile["type"] == "single_block":
                # 2-A. 단일 구형파 블록: 정격 전력 유지 + 미세 변동
                is_active = True
                p = state["nominal_w"] + rng.gauss(0, 1.2)
                pf = state["nominal_pf"] + rng.gauss(0, 0.003)

            elif profile["type"] == "duty_cycle":
                # 2-B. 듀티 사이클: 서모스탯 제어 (가열 ON ↔ 휴지 OFF)
                state["duty_remaining"] -= 1
                if state["is_heating"]:
                    is_active = True
                    p = state["nominal_w"] + rng.gauss(0, 1.5)
                    pf = state["nominal_pf"]
                    if state["duty_remaining"] <= 0:
                        state["is_heating"] = False
                        state["duty_remaining"] = rng.randint(*profile["duty_off_sec"])
                else:
                    # 휴지 구간: 내부 대기전력만 소모
                    is_active = True  # 세션 가동 중
                    p = profile["standby_w"][1] + rng.uniform(5.0, 15.0)
                    pf = 0.92
                    if state["duty_remaining"] <= 0:
                        state["is_heating"] = True
                        state["duty_remaining"] = rng.randint(*profile["duty_on_sec"])

    if state["state"] == "OFF":
        # 3. 완전 대기 상태
        low, high = profile["standby_w"]
        p = rng.uniform(low, high) if high > 0 else 0.0
        pf = 0.95

    p = max(0.0, p)
    pf = max(0.1, min(0.999, pf))
    q = p * math.sqrt(1.0 - pf * pf) / pf if p > 0.1 else 0.0

    return p, q, is_active


def calculate_main_panel_metrics(house: str, allow_random: bool = True) -> dict:
    """가구 내 메인 분전반의 다변량 교류 전력 특성(P, Q, S, PF, V, I) 계산"""
    # 1. 완만한 전압 및 기저 부하 획득
    voltage, base_p, base_q = update_house_environment(house)

    total_p = base_p
    total_q = base_q
    active_devices = []

    # 2. 6대 수동 가전 부하 합산 및 파형 생성
    for device in DEVICE_PROFILES:
        dev_p, dev_q, is_active = update_and_generate_device_load(house, device, allow_random=allow_random)
        total_p += dev_p
        total_q += dev_q
        if is_active:
            active_devices.append(DEVICE_PROFILES[device]["name_ko"])

    # 3. 분전반 메인 계측기 미세 센서 노이즈
    rng = get_house_rng(house)
    total_p += rng.gauss(0, 0.8)
    total_q += rng.gauss(0, 0.8)
    total_p = max(0.0, total_p)
    total_q = max(0.0, total_q)

    # 4. 피상전력 S (VA) = sqrt(P^2 + Q^2)
    apparent_power = math.sqrt(total_p * total_p + total_q * total_q)

    # 5. 역률 PF = P / S
    if apparent_power > 1e-3:
        power_factor = total_p / apparent_power
    else:
        power_factor = 1.0
    power_factor = max(0.0, min(1.0, power_factor))

    # 6. 전류 I (A) = S / V
    current = apparent_power / voltage

    return {
        "active_power": round(total_p, 2),
        "reactive_power": round(total_q, 2),
        "apparent_power": round(apparent_power, 2),
        "power_factor": round(power_factor, 3),
        "voltage": voltage,
        "current": round(current, 3),
        "active_devices": active_devices
    }
