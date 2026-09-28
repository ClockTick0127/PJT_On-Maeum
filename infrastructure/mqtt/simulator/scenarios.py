"""
NILM 스마트홈 전력 시뮬레이터 시나리오 및 대기전력 모델 (Scenarios & Standby Power Model)

- 일상 루틴 누락(ROUTINE_MISSED) 이상치 시나리오 정의 및 대기전력 물리 파라미터
- 발표 및 시연을 위한 10초 3,000W+ 피크 전력 시나리오 정의
- 가상 시각(Virtual Time) 생성 및 KST/UTC 타임존 헬퍼
"""

import math
import re
import random
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta, date

# 한국 표준시 (KST = UTC+9)
KST = timezone(timedelta(hours=9))

# ==========================================
# 1. 대기전력(기저부하) 물리 파라미터 정의
# ==========================================
# - 상시 대기전력(Base Standby): 공유기, 셋톱박스, 센서 등 상시 40~65W (역률 ~0.92)
# - 냉장고 컴프레서(Fridge): 15~25분 가동(55~85W, 역률 0.76~0.82), 20~35분 정지 주기
# - 계측 전압(Voltage): 220V 기준 1차 자기회귀(AR-1) 완만 변동 (212V ~ 228V)
BASE_STANDBY_P_RANGE = (40.0, 65.0)
BASE_STANDBY_PF = 0.92
FRIDGE_P_RANGE = (55.0, 85.0)
FRIDGE_PF_RANGE = (0.76, 0.82)
VOLTAGE_NOMINAL = 220.0
VOLTAGE_RANGE = (212.0, 228.0)


def create_initial_house_environment(rng=random) -> dict:
    """단일 가구의 초기 전압 및 대기전력 환경 상태 생성 (rng: random.Random 또는 random 모듈)"""
    return {
        "voltage": round(rng.gauss(VOLTAGE_NOMINAL, 1.2), 1),
        "base_nominal_w": rng.uniform(*BASE_STANDBY_P_RANGE),
        "base_current_w": rng.uniform(45.0, 60.0),
        "fridge_active": rng.random() < 0.4,
        "fridge_remaining_sec": rng.randint(300, 1200),
        "fridge_nominal_w": rng.uniform(*FRIDGE_P_RANGE),
        "fridge_pf": rng.uniform(*FRIDGE_PF_RANGE),
    }


def update_standby_environment(env: dict, rng=random) -> tuple[float, float, float]:
    """
    가구별 전압 드리프트(AR-1) 및 순수 대기전력 + 냉장고 주기 계산
    반환: (voltage, total_base_p, base_q)
    """
    # 1. 전압 AR-1 완만 드리프트
    env["voltage"] = 0.98 * env["voltage"] + 0.02 * VOLTAGE_NOMINAL + rng.gauss(0, 0.12)
    voltage = round(max(VOLTAGE_RANGE[0], min(VOLTAGE_RANGE[1], env["voltage"])), 1)

    # 2. 상시 대기전력의 완만한 변동 (Random Walk)
    env["base_current_w"] = 0.96 * env["base_current_w"] + 0.04 * env["base_nominal_w"] + rng.gauss(0, 0.2)
    base_p = max(20.0, env["base_current_w"])
    base_pf = BASE_STANDBY_PF

    # 3. 냉장고 컴프레서 주기적 가동/정지
    env["fridge_remaining_sec"] -= 1
    if env["fridge_remaining_sec"] <= 0:
        env["fridge_active"] = not env["fridge_active"]
        # 가동: 15~25분(900~1500초), 정지: 20~35분(1200~2100초)
        env["fridge_remaining_sec"] = rng.randint(900, 1500) if env["fridge_active"] else rng.randint(1200, 2100)

    fridge_p = 0.0
    fridge_q = 0.0
    if env["fridge_active"]:
        fridge_p = env["fridge_nominal_w"] + rng.gauss(0, 0.8)
        fridge_pf = env["fridge_pf"]
        fridge_q = fridge_p * math.sqrt(1.0 - fridge_pf * fridge_pf) / fridge_pf

    base_q = base_p * math.sqrt(1.0 - base_pf * base_pf) / base_pf + fridge_q
    total_base_p = base_p + fridge_p

    return voltage, total_base_p, base_q


# ==========================================
# 2. 루틴 누락(ROUTINE_MISSED) 이상치 시나리오
# ==========================================
class RoutineMissedScenario:
    """
    실시간 분석 서비스(realtime-analysis-service)의 이상 감지 검증 시나리오
    - 기준: H001 가구는 평소 08:10 이전에 전자레인지(MICROWAVE)를 사용하는 루틴이 있음
    - 이상 상태: 08:10이 지났음에도 전자레인지 사용 없이 대기전력만 지속 흐름
    - 버퍼 요건: 299개 입력 윈도우 충족 시 AI 이상 감지 판정 조건 제공
    """
    TARGET_HOUSE = "H001"
    EXPECTED_UNTIL = "08:10"
    DEFAULT_START_TIME = "08:15:00"
    BUFFER_WINDOW_SIZE = 299
    DEFAULT_COUNT = 300

    @classmethod
    def get_cycle_status(cls, cycle: int) -> tuple[str, str]:
        """사이클에 따른 진행 상태 태그 및 이벤트 알림 메시지 반환"""
        if cycle < cls.BUFFER_WINDOW_SIZE:
            return f"버퍼 적재 중 ({cycle:03d}/{cls.BUFFER_WINDOW_SIZE})", ""
        elif cycle == cls.BUFFER_WINDOW_SIZE:
            return "299개 분석 입력 데이터 충족 — AI 이상 감지 판정 대기", "  <== [299개 분석 입력 데이터 충족 — AI 이상 감지 판정 대기]"
        else:
            return "이상 감지 상태 유지", ""


class RoutineMissedDemoScenario:
    """첫 계측 시점부터 평소 기록과 같은 330초 구간을 대기전력으로 발행한다."""
    TARGET_HOUSE = "H001"
    TOTAL_CYCLES = 330
    DEFAULT_COUNT = TOTAL_CYCLES


# ==========================================
# 2-A. 정상 일상(NORMAL_ROUTINE) 시나리오
# ==========================================
class NormalRoutineScenario:
    """
    H001 정상 일상(NORMAL_ROUTINE) 시나리오
    - 평소 아침 루틴대로 08:10 이전에 전자레인지(MICROWAVE)를 정확히 60초간 가동
    - 기본 시작 시각: 08:04:58 KST
    - 전자레인지 ON: 08:09:00 KST (T+243s)
    - 전자레인지 OFF: 08:10:00 KST (T+303s)
    - 전자레인지 ON 유지 구간: 08:09:00 ~ 08:09:59 (정확히 60초간 60개 ON 샘플)
    - 08:10:00 OFF 이후 5초 동안 대기전력 발행 후 자동 완료 (T+308s)
    """
    TARGET_HOUSE = "H001"
    DEFAULT_START_TIME = "08:04:58"
    MICROWAVE_ON_TIME = "08:09:00"
    MICROWAVE_OFF_TIME = "08:10:00"
    MICROWAVE_ON_CYCLE = 243
    MICROWAVE_OFF_CYCLE = 303
    MICROWAVE_DURATION_SEC = 60
    TOTAL_CYCLES = 308
    DEFAULT_COUNT = 308


NORMAL_ROUTINE_SCHEDULE = {
    NormalRoutineScenario.MICROWAVE_ON_CYCLE: {
        "event": "MICROWAVE_START",
        "desc": "08:09 아침 정상 루틴 시작 — 전자레인지 가동",
        "actions": {
            "microwave": {
                "state": "STARTING",
                "session_remaining": 61,
                "inrush_remaining": 2,
                "nominal_w": 940.0,
                "nominal_pf": 0.91,
                "manual_hold": False,
            }
        }
    },
    NormalRoutineScenario.MICROWAVE_OFF_CYCLE: {
        "event": "MICROWAVE_STOP",
        "desc": "08:10 이전 전자레인지 60초 사용 완료 — 대기전력 복귀",
        "actions": {
            "microwave": {
                "state": "OFF",
                "session_remaining": 0,
                "inrush_remaining": 0,
                "nominal_w": 0.0,
                "nominal_pf": 0.0,
                "manual_hold": False,
            }
        }
    },
    NormalRoutineScenario.TOTAL_CYCLES: {
        "event": "NORMAL_ROUTINE_COMPLETE",
        "desc": "H001 정상 일상 전력 패턴 발행 완료",
        "actions": {}
    }
}


def inject_normal_routine_scenario_event(cycle_sec: int, house: str, device_states: dict) -> str | None:
    """정상 루틴 시나리오 타임라인 이벤트 주입"""
    if cycle_sec in NORMAL_ROUTINE_SCHEDULE:
        item = NORMAL_ROUTINE_SCHEDULE[cycle_sec]
        if house in device_states:
            for dev_name, dev_conf in item["actions"].items():
                if dev_name in device_states[house]:
                    device_states[house][dev_name].update(dev_conf)
        return item["desc"]
    return None


# ==========================================
# 2-A-2. 사용시간 초과(PROLONGED_USE) 시나리오
# ==========================================
class ProlongedUseScenario:
    """
    H001 전자레인지 사용시간 초과(PROLONGED_USE) 시나리오
    - 평소 점심에는 전자레인지를 약 90초 쓰고 끄지만, 오늘은 켠 채로 5분간 이어진다.
    - 기본 시작 시각: 11:55:00 KST (cycle N의 가상 시각 = 시작 시각 + (N-1)초)
    - cycle 1~300 (11:55:00~11:59:59): 대기전력만 발행. 분석 모델 입력 창(255초)을 먼저 채운다.
    - 전자레인지 ON: 12:00:00 KST (cycle 301), manual_hold로 명시적 OFF 전까지 유지 (약 941W 연속 블록)
    - 12:02:00 KST (cycle 421): 켠 뒤 120초 경과 (시연 화면의 허용 사용시간 표시 기준)
    - 전자레인지 OFF: 12:05:00 KST (cycle 601). ON 활성 샘플은 cycle 301~600, 정확히 300개
    - 12:05:00 이후 30초 동안 대기전력 발행 후 자동 완료 (cycle 630, 12:05:29)
    시뮬레이터는 MQTT 원천 데이터만 만든다. 이상 판정은 분석 서비스가 수행한다.
    """
    TARGET_HOUSE = "H001"
    APPLIANCE = "microwave"
    DEFAULT_START_TIME = "11:55:00"
    APPLIANCE_ON_TIME = "12:00:00"
    ALLOWED_DURATION_ELAPSED_TIME = "12:02:00"
    APPLIANCE_OFF_TIME = "12:05:00"
    APPLIANCE_ON_CYCLE = 301
    ALLOWED_DURATION_SEC = 120
    ALLOWED_DURATION_ELAPSED_CYCLE = APPLIANCE_ON_CYCLE + ALLOWED_DURATION_SEC
    APPLIANCE_OFF_CYCLE = 601
    APPLIANCE_DURATION_SEC = APPLIANCE_OFF_CYCLE - APPLIANCE_ON_CYCLE
    TOTAL_CYCLES = 630
    DEFAULT_COUNT = 630


PROLONGED_USE_SCHEDULE = {
    ProlongedUseScenario.APPLIANCE_ON_CYCLE: {
        "event": "MICROWAVE_START",
        "desc": "12:00 점심 데우기 시작 — 전자레인지 가동 (평소 약 90초 사용)",
        "actions": {
            "microwave": {
                "state": "STARTING",
                "session_remaining": ProlongedUseScenario.APPLIANCE_DURATION_SEC,
                "inrush_remaining": 2,
                "nominal_w": 940.0,
                "nominal_pf": 0.91,
                "manual_hold": True,
            }
        }
    },
    ProlongedUseScenario.ALLOWED_DURATION_ELAPSED_CYCLE: {
        "event": "ALLOWED_DURATION_ELAPSED",
        "desc": "12:02 전자레인지 연속 사용 120초 경과 — 평소 사용시간 초과, 계속 가동 중",
        "actions": {}
    },
    ProlongedUseScenario.APPLIANCE_OFF_CYCLE: {
        "event": "MICROWAVE_STOP",
        "desc": "12:05 전자레인지 5분 연속 사용 후 종료 — 대기전력 복귀",
        "actions": {
            "microwave": {
                "state": "OFF",
                "session_remaining": 0,
                "inrush_remaining": 0,
                "nominal_w": 0.0,
                "nominal_pf": 0.0,
                "manual_hold": False,
            }
        }
    },
    ProlongedUseScenario.TOTAL_CYCLES: {
        "event": "PROLONGED_USE_COMPLETE",
        "desc": "H001 전자레인지 사용시간 초과 전력 패턴 발행 완료",
        "actions": {}
    }
}


def inject_prolonged_use_scenario_event(cycle_sec: int, house: str, device_states: dict) -> str | None:
    """사용시간 초과 시나리오 타임라인 이벤트 주입"""
    if cycle_sec in PROLONGED_USE_SCHEDULE:
        item = PROLONGED_USE_SCHEDULE[cycle_sec]
        if house in device_states:
            for dev_name, dev_conf in item["actions"].items():
                if dev_name in device_states[house]:
                    device_states[house][dev_name].update(dev_conf)
        return item["desc"]
    return None


# ==========================================
# 2-B. 센서 고장/결측(SENSOR_FAULT) 시나리오
# ==========================================
@dataclass(frozen=True)
class SensorFaultTimeline:
    """센서 고장 시나리오의 경계값 및 총 사이클을 보관하는 불변 객체"""
    duration_sec: int
    fault_start: int
    fault_end: int
    recovery_start: int
    recovery_end: int
    total_cycles: int
    publish_count: int = 20

    @classmethod
    def create(cls, duration_sec: int = 120) -> "SensorFaultTimeline":
        fault_end = 10 + duration_sec
        recovery_start = fault_end + 1
        total_cycles = 20 + duration_sec
        return cls(
            duration_sec=duration_sec,
            fault_start=11,
            fault_end=fault_end,
            recovery_start=recovery_start,
            recovery_end=total_cycles,
            total_cycles=total_cycles,
            publish_count=20,
        )


class SensorFaultScenario:
    """
    센서 고장(SENSOR_FAULT) 시나리오:
    - 타임라인 (D = fault_duration_sec, 기본 120초):
      * cycle 1~10: 정상 대기전력 계측 및 MQTT 발행 (10회)
      * cycle 11: "센서 고장 시작" 이벤트 발생
      * cycle 11~(10+D): 센서 고장. 해당 가구 MQTT 메시지 0건 발행 (D초간 무발행)
      * cycle (11+D): "센서 복구" 이벤트 발생 및 복구 후 정상 계측/발행 재개 (D+1초 시간 간격)
      * cycle (11+D)~(20+D): 센서 복구 후 정상 계측 및 MQTT 발행 (10회)
      * cycle (20+D): "시나리오 완료" 이벤트 발생 및 완료 처리
    - 총 사이클: D + 20초 (기본 140초)
    - 고장 지속 시간: D초 (기본 120초, 1~3600초 가변)
    - 총 MQTT 발행 횟수: 20회 (1~10: 10회, 복구 후: 10회)
    """
    NAME = "sensor_fault"
    SCENARIO_NAME = "sensor_fault"
    DEFAULT_FAULT_DURATION_SEC = 120
    NORMAL_BEFORE_FAULT_CYCLES = 10
    FAULT_START_CYCLE = 11
    RECOVERED_DURATION_CYCLES = 10
    TOTAL_PUBLISH_COUNT = 20

    # 하위 호환 레퍼런스 상수 (기본값 D=120 기준)
    TOTAL_CYCLES = 140
    DEFAULT_COUNT = 140
    FAULT_END_CYCLE = 130
    FAULT_DURATION_SEC = 120
    RECOVERED_START_CYCLE = 131
    RECOVERED_END_CYCLE = 140

    @classmethod
    def get_timeline(cls, duration_sec: int = DEFAULT_FAULT_DURATION_SEC) -> SensorFaultTimeline:
        return SensorFaultTimeline.create(duration_sec)

    @classmethod
    def is_fault_cycle(cls, cycle: int, duration_sec: int = DEFAULT_FAULT_DURATION_SEC) -> bool:
        """현재 사이클이 센서 고장(결측) 구간인지 여부 반환"""
        tl = cls.get_timeline(duration_sec)
        return tl.fault_start <= cycle <= tl.fault_end

    @classmethod
    def get_gap_metrics(cls, cycle: int, duration_sec: int = DEFAULT_FAULT_DURATION_SEC) -> tuple[bool, int, int]:
        """
        반환: (is_fault, gap_elapsed_sec, gap_remaining_sec)
        - cycle 1~10: (False, 0, D)
        - cycle 11: (True, 1, D - 1)
        - cycle 10+D: (True, D, 0)
        - cycle (11+D)~: (False, D, 0)
        """
        tl = cls.get_timeline(duration_sec)
        if cycle < tl.fault_start:
            return False, 0, tl.duration_sec
        elif cycle <= tl.fault_end:
            elapsed = cycle - tl.fault_start + 1
            remaining = tl.fault_end - cycle
            return True, elapsed, remaining
        else:
            return False, tl.duration_sec, 0

    @classmethod
    def get_cycle_status(cls, cycle: int, duration_sec: int = DEFAULT_FAULT_DURATION_SEC) -> tuple[str, str | None]:
        """
        사이클에 따른 진행 상태 태그 및 이벤트 알림 메시지 반환.
        반환: (status_tag, notice_desc)
        """
        tl = cls.get_timeline(duration_sec)
        is_fault = tl.fault_start <= cycle <= tl.fault_end
        status_tag = "시나리오 완료" if cycle >= tl.total_cycles else "센서 고장" if is_fault else "정상 계측"
        notice = None
        if cycle == tl.fault_start:
            notice = f"센서 고장 시작 — 전력 계측 MQTT 메시지 발행 중단 ({tl.duration_sec}초간 결측)"
        elif cycle == tl.recovery_start:
            notice = "센서 복구 — 전력 계측 데이터 정상 발행 재개"
        elif cycle >= tl.total_cycles:
            notice = f"시나리오 완료 — 센서 고장 및 복구 시연 완료 (총 {tl.total_cycles}초)"
        return status_tag, notice




# ==========================================
# 3. 10초 피크(3,000W+) 시연 시나리오
# ==========================================
PEAK_SCENARIO_SCHEDULE = {
    10: {
        "event": "PEAK_START",
        "desc": "피크 발생 (전기포트 1,700W + 인덕션 1,600W 동시 기동 -> 3,000W+ 돌파)",
        "actions": {
            "kettle": {
                "state": "STARTING",
                "session_remaining": 21,
                "inrush_remaining": 1,
                "nominal_w": 1700.0,
                "nominal_pf": 0.99
            },
            "induction": {
                "state": "STARTING",
                "session_remaining": 35,
                "inrush_remaining": 1,
                "nominal_w": 1600.0,
                "nominal_pf": 0.93,
                "is_heating": True,
                "duty_remaining": 25
            }
        }
    },
    31: {
        "event": "PEAK_EASE",
        "desc": "피크 해소 (전기포트 종료 -> 인덕션 단독 1,600W 유지)",
        "actions": {
            "kettle": {"state": "OFF", "session_remaining": 0}
        }
    },
    45: {
        "event": "NORMAL_RETURN",
        "desc": "정상 복귀 (인덕션 종료 -> 평상시 대기전력 약 60W 복귀)",
        "actions": {
            "induction": {"state": "OFF", "session_remaining": 0}
        }
    }
}


def inject_peak_scenario_event(cycle_sec: int, house: str, device_states: dict) -> str | None:
    """피크 시연 시나리오 타임라인 이벤트 주입"""
    if cycle_sec in PEAK_SCENARIO_SCHEDULE:
        item = PEAK_SCENARIO_SCHEDULE[cycle_sec]
        if house in device_states:
            for dev_name, dev_conf in item["actions"].items():
                if dev_name in device_states[house]:
                    device_states[house][dev_name].update(dev_conf)
        return item["desc"]
    return None


# ==========================================
# 4. 가상 시각(Virtual Time) 및 날짜 파싱 헬퍼
# ==========================================
def parse_simulation_date(val: object) -> str:
    """
    엄격한 YYYY-MM-DD 형식의 실제 존재하는 유효한 날짜인지 검증 및 파싱.
    - 문자열이 아니거나 공백이 포함되어 있거나 형식이 맞지 않거나 실제 존재하지 않는 날짜(예: 2026-02-30)인 경우 ValueError 발생.
    - 성공 시 검증된 YYYY-MM-DD 문자열 반환.
    """
    if not isinstance(val, str) or isinstance(val, bool):
        raise ValueError("simulation_date는 YYYY-MM-DD 형식의 문자열이어야 합니다.")
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", val):
        raise ValueError(f"올바르지 않은 simulation_date 형식입니다: '{val}'. YYYY-MM-DD 형식이어야 합니다.")
    try:
        parsed_dt = datetime.strptime(val, "%Y-%m-%d")
        return parsed_dt.strftime("%Y-%m-%d")
    except ValueError as err:
        raise ValueError(f"존재하지 않는 유효하지 않은 simulation_date입니다: '{val}' ({err})")


def format_iso_utc(dt: datetime) -> str:
    """timezone-aware datetime을 밀리초 3자리 UTC ISO 8601 포맷 문자열로 변환 (예: 2026-09-10T05:30:15.000Z)"""
    aware_dt = dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
    return aware_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _parse_time_parts(time_str: str) -> tuple[int, int, int]:
    """HH:MM:SS 또는 HH:MM 시간 문자열을 파싱하여 (hour, minute, second) 튜플을 반환한다."""
    parts = time_str.strip().split(":")
    if len(parts) not in (2, 3):
        raise ValueError(f"올바르지 않은 시간 형식입니다: '{time_str}' (HH:MM:SS 또는 HH:MM)")
    try:
        h = int(parts[0])
        m = int(parts[1])
        s = int(parts[2]) if len(parts) > 2 else 0
    except ValueError:
        raise ValueError(f"시간 값은 정수여야 합니다: '{time_str}'")
    if not (0 <= h <= 23 and 0 <= m <= 59 and 0 <= s <= 59):
        raise ValueError(f"유효하지 않은 시간 범위입니다: '{time_str}' (00:00:00 ~ 23:59:59)")
    return h, m, s


def resolve_simulation_start_time(
    scenario: str,
    simulation_date: str | date | None = None,
    start_time_str: str | None = None,
    now: datetime | None = None,
    routine_default_time: str = "08:10:01"
) -> datetime | None:
    """
    시나리오, 기준 날짜(simulation_date), 시작 시각(start_time_str)을 기반으로 가상 시작 시각(aware datetime KST)을 결정한다.

    우선순위 및 규칙:
    A. simulation_date와 시간 형식 start_time_str 함께 지정: 선택 날짜 + 지정 시간 (KST)
    B. 시간 형식 start_time_str만 지정: 오늘 날짜 + 지정 시간 (KST)
    C. 완전한 ISO datetime start_time_str만 지정: ISO datetime 그대로 사용 (tz 없으면 KST)
    D. simulation_date와 완전한 ISO datetime 동시 지정: 충돌 오류(ValueError) 발생
    E. simulation_date만 지정:
       - routine_missed: 선택 날짜 + routine_default_time (KST)
       - normal_routine / prolonged_use: 선택 날짜 + 시나리오 DEFAULT_START_TIME (KST)
       - peak/random/manual: 선택 날짜 + 현재 KST 시각(now)
    F. 아무 값도 지정하지 않음:
       - routine_missed: 오늘 날짜 + routine_default_time (KST)
       - normal_routine / prolonged_use: 오늘 날짜 + 시나리오 DEFAULT_START_TIME (KST)
       - peak/random/manual: None 반환 (실제 현재 시각 기반 동작)
    """
    # 1. 기준 now_dt 준비 (KST aware)
    if now is None:
        now_dt = datetime.now(KST)
    elif now.tzinfo is None:
        now_dt = now.replace(tzinfo=KST)
    else:
        now_dt = now.astimezone(KST)

    # 2. simulation_date 파싱
    target_date: date | None = None
    if simulation_date:
        if isinstance(simulation_date, datetime):
            target_date = simulation_date.date()
        elif isinstance(simulation_date, date):
            target_date = simulation_date
        else:
            date_str = parse_simulation_date(simulation_date)
            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()

    # 3. start_time_str 분석
    clean_start_time = start_time_str.strip() if start_time_str is not None and isinstance(start_time_str, str) else None
    is_time_only = False
    is_iso_dt = False
    parsed_time_tuple: tuple[int, int, int] | None = None
    parsed_iso_dt: datetime | None = None

    if clean_start_time:
        # 시간 형식(HH:MM[:SS]) 여부 확인: ":" 포함, "T" 및 "-" 미포함
        if ":" in clean_start_time and "T" not in clean_start_time and "-" not in clean_start_time:
            parsed_time_tuple = _parse_time_parts(clean_start_time)
            is_time_only = True
        else:
            try:
                parsed_iso_dt = datetime.fromisoformat(clean_start_time)
                is_iso_dt = True
            except ValueError as err:
                raise ValueError(f"올바르지 않은 --start-time 형식입니다: '{clean_start_time}' ({err})")

    # D. simulation_date와 완전한 ISO datetime 동시 지정 충돌 거절
    if target_date is not None and is_iso_dt:
        raise ValueError("--date와 완전한 ISO --start-time을 함께 사용할 수 없습니다.")

    # C. 완전한 ISO datetime 단독 지정
    if is_iso_dt and parsed_iso_dt is not None:
        if parsed_iso_dt.tzinfo is None:
            return parsed_iso_dt.replace(tzinfo=KST)
        return parsed_iso_dt

    # A & B. 시간 형식 start_time이 지정된 경우 (시나리오 무관하게 명시된 시간 사용)
    if is_time_only and parsed_time_tuple is not None:
        base_d = target_date if target_date is not None else now_dt.date()
        h, m, s = parsed_time_tuple
        return datetime(base_d.year, base_d.month, base_d.day, h, m, s, tzinfo=KST)

    # E. simulation_date만 지정된 경우
    if target_date is not None:
        if scenario == "routine_missed":
            def_h, def_m, def_s = _parse_time_parts(routine_default_time)
            return datetime(target_date.year, target_date.month, target_date.day, def_h, def_m, def_s, tzinfo=KST)
        elif scenario == "normal_routine":
            def_h, def_m, def_s = _parse_time_parts(NormalRoutineScenario.DEFAULT_START_TIME)
            return datetime(target_date.year, target_date.month, target_date.day, def_h, def_m, def_s, tzinfo=KST)
        elif scenario == "prolonged_use":
            def_h, def_m, def_s = _parse_time_parts(ProlongedUseScenario.DEFAULT_START_TIME)
            return datetime(target_date.year, target_date.month, target_date.day, def_h, def_m, def_s, tzinfo=KST)
        else:
            return datetime(
                target_date.year, target_date.month, target_date.day,
                now_dt.hour, now_dt.minute, now_dt.second,
                tzinfo=KST
            )

    # F. 아무 값도 지정하지 않은 경우
    if scenario == "routine_missed":
        today = now_dt.date()
        def_h, def_m, def_s = _parse_time_parts(routine_default_time)
        return datetime(today.year, today.month, today.day, def_h, def_m, def_s, tzinfo=KST)
    elif scenario == "normal_routine":
        today = now_dt.date()
        def_h, def_m, def_s = _parse_time_parts(NormalRoutineScenario.DEFAULT_START_TIME)
        return datetime(today.year, today.month, today.day, def_h, def_m, def_s, tzinfo=KST)
    elif scenario == "prolonged_use":
        today = now_dt.date()
        def_h, def_m, def_s = _parse_time_parts(ProlongedUseScenario.DEFAULT_START_TIME)
        return datetime(today.year, today.month, today.day, def_h, def_m, def_s, tzinfo=KST)

    return None


def parse_simulation_start_time(start_time_str: str | None, is_missed_mode: bool) -> datetime | None:
    """
    시작 가상 시각 파싱 (레거시 CLI 및 헬퍼 호환).
    - routine_missed 모드는 미지정 시 기본값으로 오늘 아침 08:15:00 KST 반환
    """
    if start_time_str:
        try:
            val = start_time_str.strip()
            if ":" in val and "T" not in val and "-" not in val:
                h, m, s = _parse_time_parts(val)
                today = datetime.now(KST).date()
                return datetime(today.year, today.month, today.day, h, m, s, tzinfo=KST)
            else:
                dt = datetime.fromisoformat(val)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=KST)
                return dt
        except Exception as err:
            print(f"[경고] --start-time '{start_time_str}' 파싱 실패 ({err}). 기본 시간 로직을 적용합니다.", flush=True)

    if is_missed_mode:
        today = datetime.now(KST).date()
        return datetime(today.year, today.month, today.day, 8, 15, 0, tzinfo=KST)

    return None


def resolve_fixed_start_time(
    start_time: object,
    simulation_date: str | date | None = None,
    now: datetime | None = None,
) -> datetime:
    """
    웹/API의 시작 시각 고정 옵션(start_time="HH:MM" 또는 "HH:MM:SS")을 KST aware datetime으로 변환한다.
    날짜는 simulation_date, 없으면 오늘(KST). seed와 함께 쓰면 measured_at까지 매 실행 동일해진다.
    """
    if not isinstance(start_time, str) or not re.fullmatch(r"\d{2}:\d{2}(:\d{2})?", start_time.strip()):
        raise ValueError(f"start_time은 HH:MM 또는 HH:MM:SS 형식의 문자열이어야 합니다: {start_time!r}")
    h, m, s = _parse_time_parts(start_time.strip())

    if simulation_date:
        if isinstance(simulation_date, date):
            target_date = simulation_date
        else:
            target_date = datetime.strptime(parse_simulation_date(simulation_date), "%Y-%m-%d").date()
    else:
        now_dt = datetime.now(KST) if now is None else now.astimezone(KST)
        target_date = now_dt.date()

    return datetime(target_date.year, target_date.month, target_date.day, h, m, s, tzinfo=KST)


def resolve_multi_simulation_start_time(
    households: list[dict],
    simulation_date: str | date | datetime | None = None,
    now: datetime | None = None,
    routine_default_time: str = "08:10:01"
) -> datetime | None:
    """
    다중 가구 실행 시 모든 가구가 공유할 단일 공통 가상 시작 시각(base_dt)을 결정합니다.
    - 실행 대상 가구 중 'routine_missed' 시나리오가 하나라도 포함된 경우:
        선택한 simulation_date의 08:10:01 KST (미지정 시 오늘 날짜의 08:10:01 KST).
    - 'routine_missed'가 포함되지 않은 경우:
        simulation_date가 지정되었으면 해당 날짜 + 실행 시작 시점의 현재 KST 시각.
        simulation_date가 없으면 None (실시간 현재 시각 사용).
    """
    if now is None:
        now_dt = datetime.now(KST)
    else:
        now_dt = now.astimezone(KST)

    target_date: date | None = None
    if simulation_date:
        if isinstance(simulation_date, datetime):
            target_date = simulation_date.date()
        elif isinstance(simulation_date, date):
            target_date = simulation_date
        else:
            date_str = parse_simulation_date(simulation_date)
            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()

    has_routine_missed = any(isinstance(h, dict) and h.get("scenario") == "routine_missed" for h in households)
    has_normal_routine = any(isinstance(h, dict) and h.get("scenario") == "normal_routine" for h in households)

    if has_normal_routine and has_routine_missed:
        raise ValueError("normal_routine과 routine_missed는 동일한 다중 실행에서 함께 사용할 수 없습니다.")

    has_prolonged_use = any(isinstance(h, dict) and h.get("scenario") == "prolonged_use" for h in households)
    if has_prolonged_use and (has_normal_routine or has_routine_missed):
        raise ValueError("prolonged_use는 normal_routine·routine_missed와 동일한 다중 실행에서 함께 사용할 수 없습니다.")

    if has_prolonged_use:
        base_d = target_date if target_date is not None else now_dt.date()
        def_h, def_m, def_s = _parse_time_parts(ProlongedUseScenario.DEFAULT_START_TIME)
        return datetime(base_d.year, base_d.month, base_d.day, def_h, def_m, def_s, tzinfo=KST)

    if has_normal_routine:
        base_d = target_date if target_date is not None else now_dt.date()
        def_h, def_m, def_s = _parse_time_parts(NormalRoutineScenario.DEFAULT_START_TIME)
        return datetime(base_d.year, base_d.month, base_d.day, def_h, def_m, def_s, tzinfo=KST)

    if has_routine_missed:
        base_d = target_date if target_date is not None else now_dt.date()
        def_h, def_m, def_s = _parse_time_parts(routine_default_time)
        return datetime(base_d.year, base_d.month, base_d.day, def_h, def_m, def_s, tzinfo=KST)

    if target_date is not None:
        return datetime(
            target_date.year, target_date.month, target_date.day,
            now_dt.hour, now_dt.minute, now_dt.second,
            tzinfo=KST
        )

    return now_dt


def get_scenario_target_cycles(scenario: str, fault_duration_sec: int = 120) -> int:
    """시나리오별 자동 종료 목표 사이클 수 반환 (0이면 무제한)"""
    if scenario == "peak":
        return 60
    elif scenario == "routine_missed":
        return 300
    elif scenario == "routine_missed_demo":
        return RoutineMissedDemoScenario.TOTAL_CYCLES
    elif scenario == "normal_routine":
        return NormalRoutineScenario.TOTAL_CYCLES
    elif scenario == "prolonged_use":
        return ProlongedUseScenario.TOTAL_CYCLES
    elif scenario == "sensor_fault":
        return fault_duration_sec + 20
    return 0
