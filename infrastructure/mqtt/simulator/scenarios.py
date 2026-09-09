"""
NILM 스마트홈 전력 시뮬레이터 시나리오 및 대기전력 모델 (Scenarios & Standby Power Model)

- 일상 루틴 누락(ROUTINE_MISSED) 이상치 시나리오 정의 및 대기전력 물리 파라미터
- 발표 및 시연을 위한 10초 3,000W+ 피크 전력 시나리오 정의
- 가상 시각(Virtual Time) 생성 및 KST/UTC 타임존 헬퍼
"""

import math
import random
from datetime import datetime, timezone, timedelta

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


def create_initial_house_environment() -> dict:
    """단일 가구의 초기 전압 및 대기전력 환경 상태 생성"""
    return {
        "voltage": round(random.gauss(VOLTAGE_NOMINAL, 1.2), 1),
        "base_nominal_w": random.uniform(*BASE_STANDBY_P_RANGE),
        "base_current_w": random.uniform(45.0, 60.0),
        "fridge_active": random.random() < 0.4,
        "fridge_remaining_sec": random.randint(300, 1200),
        "fridge_nominal_w": random.uniform(*FRIDGE_P_RANGE),
        "fridge_pf": random.uniform(*FRIDGE_PF_RANGE),
    }


def update_standby_environment(env: dict) -> tuple[float, float, float]:
    """
    가구별 전압 드리프트(AR-1) 및 순수 대기전력 + 냉장고 주기 계산
    반환: (voltage, total_base_p, base_q)
    """
    # 1. 전압 AR-1 완만 드리프트
    env["voltage"] = 0.98 * env["voltage"] + 0.02 * VOLTAGE_NOMINAL + random.gauss(0, 0.12)
    voltage = round(max(VOLTAGE_RANGE[0], min(VOLTAGE_RANGE[1], env["voltage"])), 1)

    # 2. 상시 대기전력의 완만한 변동 (Random Walk)
    env["base_current_w"] = 0.96 * env["base_current_w"] + 0.04 * env["base_nominal_w"] + random.gauss(0, 0.2)
    base_p = max(20.0, env["base_current_w"])
    base_pf = BASE_STANDBY_PF

    # 3. 냉장고 컴프레서 주기적 가동/정지
    env["fridge_remaining_sec"] -= 1
    if env["fridge_remaining_sec"] <= 0:
        env["fridge_active"] = not env["fridge_active"]
        # 가동: 15~25분(900~1500초), 정지: 20~35분(1200~2100초)
        env["fridge_remaining_sec"] = random.randint(900, 1500) if env["fridge_active"] else random.randint(1200, 2100)

    fridge_p = 0.0
    fridge_q = 0.0
    if env["fridge_active"]:
        fridge_p = env["fridge_nominal_w"] + random.gauss(0, 0.8)
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
    - 버퍼 요건: 299초 윈도우 완충 시점(T+299s)에 Kafka 'analysis.event.v1' 이상 이벤트 발행
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
            return " 299개 완충 (이상 감지 조건 충족)", "  <== [Kafka 'analysis.event.v1' 이상 이벤트 발행!]"
        else:
            return "이상 감지 상태 유지", ""


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
# 4. 가상 시각(Virtual Time) 파싱 헬퍼
# ==========================================
def parse_simulation_start_time(start_time_str: str | None, is_missed_mode: bool) -> datetime | None:
    """
    시작 가상 시각 파싱.
    - routine_missed 모드는 미지정 시 기본값으로 오늘 아침 08:15:00 KST 반환
    """
    if start_time_str:
        try:
            val = start_time_str.strip()
            if ":" in val and "T" not in val:
                # "08:15:00" 또는 "08:15"
                parts = [int(p) for p in val.split(":")]
                hour = parts[0]
                minute = parts[1]
                second = parts[2] if len(parts) > 2 else 0
                today = datetime.now(KST).date()
                return datetime(today.year, today.month, today.day, hour, minute, second, tzinfo=KST)
            else:
                # ISO 문자열 파싱
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
