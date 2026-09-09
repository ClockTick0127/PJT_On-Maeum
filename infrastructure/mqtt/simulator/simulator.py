import os
import sys
import json
import math
import time
import uuid
import random
import asyncio
import argparse
from datetime import datetime, timezone, timedelta
import aiomqtt

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.append(CURRENT_DIR)

from scenarios import (
    KST,
    PEAK_SCENARIO_SCHEDULE,
    RoutineMissedScenario,
    create_initial_house_environment,
    update_standby_environment,
    inject_peak_scenario_event as _inject_peak_scenario_event,
    parse_simulation_start_time,
)

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
if sys.stderr and hasattr(sys.stderr, "reconfigure"):
    try:
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

# Windows 환경 호환성 설정 (aiomqtt 소켓 처리를 위해 SelectorEventLoop 적용)
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# ==========================================
# 1. 브로커 접속 및 시뮬레이션 기본 설정
# ==========================================
DEFAULT_BROKER_HOST = os.getenv("MQTT_HOST", "localhost")
DEFAULT_BROKER_PORT = int(os.getenv("MQTT_PORT", "1883"))
DEFAULT_BROKER_USER = os.getenv("MQTT_USER", "simulator_user")
DEFAULT_BROKER_PASS = os.getenv("MQTT_PASS", "test1234")

# 시뮬레이션 기본 대상 가구 목록 (H001 ~ H010 총 10개 가구)
DEFAULT_HOUSES = [f"H{i:03d}" for i in range(1, 11)]

# AI 모델(TCN/Seq2Point) 학습 기준 6대 수동 타겟 가전 프로파일 및 파형 특성 정의
# - AI Hub 71685 데이터셋 실측 중앙값(EDA 리포트) 및 교류 물리 특성 완벽 연동:
#   1. 전기포트: 순수 저항 히터 (실측 중앙 1,657W, PF 0.98-1.00, 돌입 없음, 단일 구형파)
#   2. 인덕션(전기레인지): 인버터 유도 가열 (실측 중앙 1,463W, PF 0.91-0.95, 18초 서모스탯 듀티 사이클)
#   3. 전기다리미: 전열선 바이메탈 제어 (실측 중앙 1,389W, PF 0.98-1.00, 18초 듀티 사이클)
#   4. 전자레인지: 마그네트론/고압변압기 (실측 중앙 941W, PF 0.88-0.94, 자화 돌입 1.35배)
#   5. 헤어드라이기: 열선+소형팬모터 (실측 중앙 934W, PF 0.94-0.98, 모터 돌입 1.20배)
#   6. 진공 청소기(유선): 고속 직권 모터 (실측 중앙 819W, PF 0.75-0.85, 강한 모터 돌입 1.50배)
DEVICE_PROFILES = {
    "kettle": {
        "name_ko": "전기포트",
        "type": "single_block",         # 단순 단일 구형파 블록
        "nominal_w": (1500.0, 1800.0),  # 정상 가동 전력 (EDA 실측 중앙: 1,657W, 가구간 CV 0.11)
        "standby_w": (0.0, 0.5),
        "pf_nominal": (0.98, 1.00),     # 순수 저항성 히터
        "inrush_factor": 1.02,          # 저항성 부하 (기동 돌입 없음)
        "inrush_sec": 1,
        "session_sec": (90, 210),       # 1.5분 - 3.5분 가동 (EDA 실측 지속 중앙: 2.6분 = 156초)
        "turn_on_prob": 0.003,          # 일 평균 3회 발생
    },
    "induction": {
        "name_ko": "인덕션(전기레인지)",
        "type": "duty_cycle",           # 서모스탯 듀티 사이클 반복
        "nominal_w": (1300.0, 1750.0),  # EDA 실측 중앙: 1,463W (가구간 CV 0.28)
        "standby_w": (1.0, 3.0),
        "pf_nominal": (0.91, 0.95),     # 고주파 유도 가열 인버터
        "inrush_factor": 1.08,          # 인버터 소프트 스타트 회로 적용
        "inrush_sec": 1,
        "duty_on_sec": (14, 22),        # 가열 ON 구간 (EDA 실측 지속 중앙: 정확히 18초)
        "duty_off_sec": (6, 14),        # 휴지 OFF 구간 (평균 10초)
        "session_sec": (180, 480),      # 전체 조리 세션 3분 - 8분
        "turn_on_prob": 0.002,
    },
    "iron": {
        "name_ko": "전기다리미",
        "type": "duty_cycle",           # 바이메탈 온도 제어 듀티 사이클
        "nominal_w": (1200.0, 1550.0),  # EDA 실측 중앙: 1,389W (가구간 CV 0.22)
        "standby_w": (0.0, 0.5),
        "pf_nominal": (0.98, 1.00),     # 순수 저항 열선
        "inrush_factor": 1.02,          # 저항선 부하 (돌입 없음)
        "inrush_sec": 1,
        "duty_on_sec": (14, 22),        # 가열 주기 (EDA 실측 지속 중앙: 정확히 18초)
        "duty_off_sec": (12, 28),       # 열판 축열로 인한 식힘 유지 주기
        "session_sec": (240, 600),      # 다림질 세션 4분 - 10분
        "turn_on_prob": 0.001,
    },
    "microwave": {
        "name_ko": "전자레인지",
        "type": "single_block",
        "nominal_w": (850.0, 1150.0),   # EDA 실측 중앙: 941W (가구간 CV 0.38)
        "standby_w": (1.0, 2.5),
        "pf_nominal": (0.88, 0.94),     # 고압 변압기/마그네트론 유도 부하
        "inrush_factor": 1.35,          # 변압기 자화 돌입전류 피크
        "inrush_sec": 2,
        "session_sec": (45, 240),       # 45초 - 4분 (EDA 실측 지속 중앙: 3.6분 = 216초)
        "turn_on_prob": 0.003,
    },
    "hair_dryer": {
        "name_ko": "헤어드라이기",
        "type": "single_block",
        "nominal_w": (800.0, 1200.0),   # EDA 실측 중앙: 934W (가구간 CV 0.33)
        "standby_w": (0.0, 0.2),
        "pf_nominal": (0.94, 0.98),     # 저항 열선 + 소형 모터
        "inrush_factor": 1.20,          # 소형 팬 모터 기동 피크
        "inrush_sec": 1,
        "session_sec": (60, 150),       # 1분 - 2.5분 (EDA 실측 지속 중앙: 1.5분 = 90초)
        "turn_on_prob": 0.003,
    },
    "vacuum_cleaner": {
        "name_ko": "진공 청소기(유선)",
        "type": "single_block",
        "nominal_w": (700.0, 950.0),    # EDA 실측 중앙: 819W (가구간 CV 0.30)
        "standby_w": (0.0, 0.5),
        "pf_nominal": (0.75, 0.85),     # 고속 직권 유도 전동기 (강한 지상 무효전력)
        "inrush_factor": 1.50,          # 직권 모터 강한 기동 돌입 피크 (1.50배)
        "inrush_sec": 2,
        "session_sec": (60, 180),       # 1분 - 3분 (EDA 실측 지속 중앙: 1.9분 = 114초)
        "turn_on_prob": 0.002,
    }
}

# ==========================================
# 2. 가구별 연속 환경 및 가전 상태 머신 관리
# ==========================================
house_states = {}
device_states = {}

def init_simulation_states(houses: list[str]):
    """가구 목록에 맞추어 시계열 환경 및 가전 상태 머신 초기화"""
    house_states.clear()
    device_states.clear()

    for house in houses:
        # 1) 가구별 시계열 대기전력 환경 상태 (전압 AR-1 드리프트, 기본 대기전력, 냉장고 주기)
        house_states[house] = create_initial_house_environment()

        # 2) 가구별 가전 상태 머신 객체
        device_states[house] = {}
        for device, profile in DEVICE_PROFILES.items():
            device_states[house][device] = {
                "state": "OFF",           # "OFF", "STARTING", "RUNNING"
                "session_remaining": 0,   # 전체 세션 잔여 시간 (초)
                "inrush_remaining": 0,    # 돌입전류 잔여 시간 (1~2초)
                "nominal_w": 0.0,         # 이번 세션의 정격 전력
                "nominal_pf": 0.0,        # 이번 세션의 정격 역률
                "is_heating": False,      # 듀티 사이클(인덕션, 다리미) 가열 여부
                "duty_remaining": 0       # 듀티 주기 잔여 시간
            }

# 기본 10가구로 초기 상태 셋업
init_simulation_states(DEFAULT_HOUSES)

def inject_peak_scenario_event(cycle_sec: int, house: str) -> str | None:
    """피크 시연 시나리오 타임라인 이벤트 주입 (scenarios 모듈 위임)"""
    return _inject_peak_scenario_event(cycle_sec, house, device_states)

def update_house_environment(house: str) -> tuple[float, float, float]:
    """가구별 전압(V) 드리프트 및 순수 대기전력 + 냉장고 컴프레서 주기 계산"""
    return update_standby_environment(house_states[house])

def update_and_generate_device_load(house: str, device: str, allow_random: bool = True) -> tuple[float, float, bool]:
    """
    가전 상태 머신 전이 및 파형 특성(돌입전류 오버슈트, 듀티사이클) 계산
    반환: (P, Q, is_active)
    """
    profile = DEVICE_PROFILES[device]
    state = device_states[house][device]

    if state["state"] == "OFF":
        # 가전 켜짐 트리거 확인 (랜덤 트리거 허용 시에만 자동 켜짐)
        if allow_random and random.random() < profile["turn_on_prob"]:
            dur_min, dur_max = profile["session_sec"]
            state["session_remaining"] = random.randint(dur_min, dur_max)
            state["nominal_w"] = random.uniform(*profile["nominal_w"])
            state["nominal_pf"] = random.uniform(*profile["pf_nominal"])
            state["state"] = "STARTING"
            state["inrush_remaining"] = profile["inrush_sec"]

            if profile["type"] == "duty_cycle":
                state["is_heating"] = True
                state["duty_remaining"] = random.randint(*profile["duty_on_sec"])

    p = 0.0
    pf = 0.95
    is_active = False

    if state["state"] == "STARTING":
        is_active = True
        # 1. 돌입전류(Inrush / Overshoot) 단계: 피크 전력 + 순간 역률 저하
        p = state["nominal_w"] * profile["inrush_factor"] + random.gauss(0, 2.0)
        pf = max(0.40, state["nominal_pf"] - 0.08)

        state["inrush_remaining"] -= 1
        state["session_remaining"] -= 1

        if state["inrush_remaining"] <= 0:
            state["state"] = "RUNNING"
        if state["session_remaining"] <= 0:
            state["state"] = "OFF"

    elif state["state"] == "RUNNING":
        state["session_remaining"] -= 1
        if state["session_remaining"] <= 0:
            state["state"] = "OFF"
        else:
            if profile["type"] == "single_block":
                # 2-A. 단일 구형파 블록: 정격 전력 유지 + 미세 변동
                is_active = True
                p = state["nominal_w"] + random.gauss(0, 1.2)
                pf = state["nominal_pf"] + random.gauss(0, 0.003)

            elif profile["type"] == "duty_cycle":
                # 2-B. 듀티 사이클: 서모스탯 제어 (가열 ON ↔ 휴지 OFF)
                state["duty_remaining"] -= 1
                if state["is_heating"]:
                    is_active = True
                    p = state["nominal_w"] + random.gauss(0, 1.5)
                    pf = state["nominal_pf"]
                    if state["duty_remaining"] <= 0:
                        state["is_heating"] = False
                        state["duty_remaining"] = random.randint(*profile["duty_off_sec"])
                else:
                    # 휴지 구간: 내부 대기전력만 소모
                    is_active = True  # 세션 가동 중
                    p = profile["standby_w"][1] + random.uniform(5.0, 15.0)
                    pf = 0.92
                    if state["duty_remaining"] <= 0:
                        state["is_heating"] = True
                        state["duty_remaining"] = random.randint(*profile["duty_on_sec"])

    if state["state"] == "OFF":
        # 3. 완전 대기 상태
        low, high = profile["standby_w"]
        p = random.uniform(low, high) if high > 0 else 0.0
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
    total_p += random.gauss(0, 0.8)
    total_q += random.gauss(0, 0.8)
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

# ==========================================
# 3. 비동기 MQTT 전송 루프 & CLI
# ==========================================
async def publish_house_power(client: aiomqtt.Client, house: str, now_iso: str, qos: int = 1, allow_random: bool = True) -> dict:
    """단일 가구의 메인 분전반 전력 계측 데이터(4특징 및 물리 특성) 발행"""
    metrics = calculate_main_panel_metrics(house, allow_random=allow_random)

    payload = {
        # [신규 명세] realtime-analysis-service MVP 요구사항 명세서 6.1 규격 대응
        "message_id": str(uuid.uuid4()),
        "household_id": house,
        "device_id": "main",
        "measured_at": now_iso,
        "active_power": metrics["active_power"],     # AI 모델 입력 피처 1 (W)
        "reactive_power": metrics["reactive_power"], # AI 모델 입력 피처 2 (var)
        "power_factor": metrics["power_factor"],     # AI 모델 입력 피처 3 (역률)
        "current": metrics["current"],               # AI 모델 입력 피처 4 (A)

        # [하위 호환] 기존 MQTT-Kafka Bridge, HDFS Loader 및 레거시 호환 필드
        "house": house,
        "device": "main",
        "ts": now_iso,
        "power_w": metrics["active_power"],
        "voltage": metrics["voltage"],               # 전압 (V)
        "apparent_power": metrics["apparent_power"]  # 피상전력 (VA)
    }

    topic = f"v1/power/sim/{house}/main"
    await client.publish(topic, json.dumps(payload), qos=qos)
    return {"house": house, "power": metrics["active_power"], "devices": metrics["active_devices"]}



def parse_args():
    """커맨드라인 실행 인자 파싱"""
    parser = argparse.ArgumentParser(
        description="NILM IoT 스마트홈 메인 분전반 전력 시뮬레이터 (MQTT 비동기 발행)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--scenario", "-s",
        choices=["random", "peak", "routine_missed"],
        default="random",
        help="시뮬레이션 시나리오 모드 (random: 확률 기반 연속 시뮬레이션, peak: H001 단일 가구 10초 3,000W+ 피크 시연 모드, routine_missed: H001 08:10 루틴 누락 이상치 검증 모드)"
    )
    parser.add_argument(
        "--houses", "-n",
        type=int,
        default=10,
        help="시뮬레이션 대상 가구 수 (H001~H{n:03d} 자동 생성)"
    )
    parser.add_argument(
        "--interval", "-i",
        type=float,
        default=1.0,
        help="데이터 발행 주기 (초 단위)"
    )
    parser.add_argument(
        "--hz",
        type=float,
        default=None,
        help="가구당 초당 측정 횟수 (지정 시 interval = 1/hz 로 자동 환산)"
    )
    parser.add_argument(
        "--count", "-c",
        type=int,
        default=0,
        help="전송 사이클 횟수 (0: 무한 연속 발행, N > 0: N회 전송 후 자동 종료, peak 기본값: 60회, routine_missed 기본값: 300회)"
    )
    parser.add_argument(
        "--start-time",
        default=None,
        help="시뮬레이션 시작 가상 시각 (예: '08:15:00' 또는 '2026-09-08T08:15:00'). routine_missed 모드는 기본값으로 오늘 아침 08:15:00 KST 적용"
    )
    parser.add_argument(
        "--host",
        default=DEFAULT_BROKER_HOST,
        help="MQTT 브로커 호스트 주소"
    )
    parser.add_argument(
        "--port", "-p",
        type=int,
        default=DEFAULT_BROKER_PORT,
        help="MQTT 브로커 포트 번호"
    )
    parser.add_argument(
        "--user", "-u",
        default=DEFAULT_BROKER_USER,
        help="MQTT 인증 사용자명"
    )
    parser.add_argument(
        "--password",
        default=DEFAULT_BROKER_PASS,
        help="MQTT 인증 비밀번호"
    )
    parser.add_argument(
        "--qos",
        type=int,
        default=1,
        choices=[0, 1],
        help="MQTT 발행 QoS 레벨"
    )
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="요약 모드 (매초 상세 로그 생략 및 5초 주기 통계/TPS 출력)"
    )
    return parser.parse_args()

async def run_simulator(args):
    """시뮬레이터 메인 비동기 실행 루프"""
    is_peak_mode = (args.scenario == "peak")
    is_missed_mode = (args.scenario == "routine_missed")

    # 1. 가구 목록 동적 생성 및 상태 머신 초기화
    if (is_peak_mode or is_missed_mode) and args.houses == 10:
        houses = ["H001"]  # peak 및 routine_missed 모드는 기본 단일 가구 H001 대상
    else:
        houses = [f"H{i:03d}" for i in range(1, args.houses + 1)]

    init_simulation_states(houses)

    # 2. 발행 주기(interval) 및 목표 사이클 수 계산
    interval = (1.0 / args.hz) if args.hz and args.hz > 0 else max(0.001, args.interval)
    if args.count > 0:
        target_count = args.count
    elif is_peak_mode:
        target_count = 60
    elif is_missed_mode:
        target_count = 300  # 299개 슬라이딩 버퍼 완충 후 이상 검증
    else:
        target_count = 0

    allow_random = not (is_peak_mode or is_missed_mode)
    base_dt = parse_simulation_start_time(args.start_time, is_missed_mode)

    print(f"============================================================")
    if is_peak_mode:
        print(f" NILM IoT 전력 시뮬레이터 시작 [10초 3,000W+ 피크 시연 모드]")
        print(f" - 브로커: {args.host}:{args.port} (QoS {args.qos})")
        print(f" - 대상 가구: {', '.join(houses)} (총 {len(houses)}개)")
        print(f" - 시연 타임라인:")
        print(f"   * T+01s ~ T+09s: 평상시 대기 상태 (약 55~65W)")
        print(f"   * T+10s ~ T+30s: [피크 경보] 전기포트(1,700W) + 인덕션(1,600W) 동시 기동 (3,300~3,500W 도달)")
        print(f"   * T+31s ~ T+44s: [피크 해소] 전기포트 자동 정지, 인덕션 단독 가동 (약 1,600W)")
        print(f"   * T+45s ~ T+60s: [정상 복귀] 인덕션 조리 완료, 대기전력 상태 복귀 (약 60W)")
        print(f" - 전송 주기: {interval:.3f}초 (약 {1.0/interval:.1f}Hz)")
        print(f" - 목표 사이클: {target_count}회 발행 후 자동 종료")
    elif is_missed_mode:
        start_desc = base_dt.strftime('%Y-%m-%d %H:%M:%S KST') if base_dt else '현재 시각'
        print(f" NILM IoT 전력 시뮬레이터 시작 [루틴 누락(ROUTINE_MISSED) 이상치 검증 모드]")
        print(f" - 브로커: {args.host}:{args.port} (QoS {args.qos})")
        print(f" - 대상 가구: {', '.join(houses)} (총 {len(houses)}개)")
        print(f" - 시작 가상 시각: {start_desc}")
        print(f" - 시연 타임라인:")
        print(f"   * 전자레인지 가동 없이 대기전력(약 45~65W) 및 냉장고 주기만 연속 유지")
        print(f"   * T+001s ~ T+298s: 분석 서비스 입력 윈도우(299개) 슬라이딩 버퍼 적재")
        print(f"   * T+299s: 299초 버퍼 충족 ➡️ Kafka 'analysis.event.v1' (score 86) 이상 이벤트 발행!")
        print(f"   * T+300s: 검증 완료 후 자동 종료")
        print(f" - 전송 주기: {interval:.3f}초 (약 {1.0/interval:.1f}Hz)")
        print(f" - 목표 사이클: {target_count}회 발행 후 자동 종료")
    else:
        print(f" NILM IoT 전력 시뮬레이터 시작")
        print(f" - 브로커: {args.host}:{args.port} (QoS {args.qos})")
        print(f" - 대상 가구: 총 {len(houses)}개 ({houses[0]} ~ {houses[-1]})")
        print(f" - 전송 주기: {interval:.3f}초 (약 {1.0/interval:.1f}Hz)")
        if target_count > 0:
            print(f" - 목표 사이클: {target_count}회 발행 후 자동 종료 (총 {len(houses) * target_count}건)")
        else:
            print(f" - 실행 모드: 무한 연속 발행 (종료: Ctrl+C)")
    print(f"============================================================", flush=True)

    total_sent = 0
    cycle = 0
    start_time = time.time()
    last_summary_time = start_time
    last_summary_sent = 0

    async with aiomqtt.Client(
        hostname=args.host,
        port=args.port,
        username=args.user,
        password=args.password,
        keepalive=60,
        timeout=5
    ) as client:
        # 대량 가구 동시 발행 시 aiomqtt 기본 경고 임계값(10) 조정
        client.pending_calls_threshold = max(len(houses) * 4, 200)

        while True:
            cycle_start = time.time()
            if base_dt is not None:
                sim_dt = base_dt + timedelta(seconds=cycle)
                now_iso = sim_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            else:
                now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            cycle += 1

            # 피크 시연 모드일 경우 타임라인 이벤트 주입
            event_desc = None
            if is_peak_mode:
                event_desc = inject_peak_scenario_event(cycle, "H001")

            # N개 가구 동시 비동기 발행 (Concurrent Publish)
            results = await asyncio.gather(
                *(publish_house_power(client, house, now_iso, qos=args.qos, allow_random=allow_random) for house in houses)
            )
            total_sent += len(houses)

            # 로그 출력 제어
            if is_peak_mode:
                res = results[0]
                power_w = res["power"]
                devs = res["devices"]
                dev_str = ", ".join(devs) if devs else "대기(기저부하)"

                status_tag = "대기"
                if power_w >= 3000.0:
                    status_tag = "피크 경보 (3,000W+ 초과!)"
                elif power_w >= 1000.0:
                    status_tag = "가전 가동 중"

                event_notice = f"  <== [{event_desc}]" if event_desc else ""
                print(f"[{now_iso}] (T+{cycle:02d}s) 소비전력: {power_w:7.1f} W | 상태: {status_tag:<22} | 가전: {dev_str}{event_notice}", flush=True)
            elif is_missed_mode:
                res = results[0]
                power_w = res["power"]
                devs = res["devices"]
                dev_str = ", ".join(devs) if devs else "대기전력(기저부하)"
                sim_dt = base_dt + timedelta(seconds=cycle - 1) if base_dt else datetime.now(KST)
                kst_str = sim_dt.astimezone(KST).strftime("%H:%M:%S")

                status_tag, notice = RoutineMissedScenario.get_cycle_status(cycle)
                print(f"[{now_iso} | KST {kst_str}] (T+{cycle:03d}s) 소비전력: {power_w:6.1f} W | 상태: {status_tag:<28} | 가전: {dev_str}{notice}", flush=True)
            elif not args.quiet:
                active_info = [f"{r['house']}:{','.join(r['devices'])}" for r in results if r["devices"]]
                active_summary = f" [가전 ON: {'; '.join(active_info)}]" if active_info else ""
                print(f"[{now_iso}] (사이클 {cycle:04d}) {len(houses)}개 가구 발행 완료{active_summary}", flush=True)
            else:
                now = time.time()
                if now - last_summary_time >= 5.0 or (target_count > 0 and cycle >= target_count):
                    elapsed = max(0.001, now - last_summary_time)
                    batch_sent = total_sent - last_summary_sent
                    tps = batch_sent / elapsed
                    print(f"[{now_iso}] 진행 중: 사이클 {cycle}, 누적 {total_sent}건 (처리량: {tps:.1f} msg/s)", flush=True)
                    last_summary_time = now
                    last_summary_sent = total_sent


            # 목표 횟수 도달 시 종료
            if target_count > 0 and cycle >= target_count:
                break

            # 주기 보정 대기 (실제 전송 소요시간 차감)
            elapsed_in_cycle = time.time() - cycle_start
            sleep_time = max(0.0, interval - elapsed_in_cycle)
            await asyncio.sleep(sleep_time)

    total_elapsed = max(0.001, time.time() - start_time)
    avg_tps = total_sent / total_elapsed
    print(f"\n[완료] 총 {cycle}개 사이클, {total_sent}건 메시지 전송 완료 (평균 처리량: {avg_tps:.1f} msg/s)", flush=True)

def main():
    args = parse_args()
    try:
        asyncio.run(run_simulator(args))
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\n시뮬레이터를 정지합니다.", flush=True)
    except aiomqtt.MqttError as error:
        print(f"\nMQTT 연결 오류 발생: {error}", flush=True)
    except Exception as error:
        print(f"\n오류 발생: {error}", flush=True)

if __name__ == "__main__":
    main()


