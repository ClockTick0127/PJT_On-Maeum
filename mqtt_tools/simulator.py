import sys
import json
import random
import time
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

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

# ==========================================
# 1. 브로커 접속 및 시뮬레이션 설정
# ==========================================
BROKER_HOST = "localhost"
BROKER_PORT = 1883
BROKER_USER = "simulator_user"
BROKER_PASS = "test1234"

# 시뮬레이션 대상 가구 목록 (H001 ~ H010 총 10개 가구)
HOUSES = [f"H{i:03d}" for i in range(1, 11)]

# 기기별 전력 소비 특성 정의 (단위: W)
DEVICE_PROFILES = {
    "fridge": {
        "standby_w": (10.0, 20.0),
        "active_w": (90.0, 140.0),
        "on_probability": 0.4,       # 컴프레서 가동 확률
    },
    "tv": {
        "standby_w": (0.5, 2.0),
        "active_w": (70.0, 150.0),
        "toggle_prob": 0.05,         # ON/OFF 전환 확률
    },
    "aircon": {
        "standby_w": (1.0, 3.0),
        "active_w": (600.0, 1600.0),
        "toggle_prob": 0.03,
    },
    "washing_machine": {
        "standby_w": (0.5, 1.5),
        "active_w": (200.0, 450.0),
        "toggle_prob": 0.02,
    },
    "microwave": {
        "standby_w": (0.8, 1.5),
        "active_w": (1000.0, 1300.0),
        "toggle_prob": 0.02,
    },
    "lights": {
        "standby_w": (0.0, 0.0),
        "active_w": (20.0, 60.0),
        "toggle_prob": 0.08,
    }
}

# 기본 상시 대기전력 (공유기, 셋톱박스, 월패드 등 상시 가동 전력)
BASE_STANDBY_W = (25.0, 45.0)

# ==========================================
# 2. 각 가구/기기별 상태(State) 관리
# ==========================================
device_states = {}
for house in HOUSES:
    device_states[house] = {}
    for device in DEVICE_PROFILES:
        # 초기 상태: 30% 확률로 켜져 있음
        device_states[house][device] = random.random() < 0.3

def generate_device_power(house: str, device: str) -> float:
    """개별 가전 상태에 따른 소비 전력 계산 (노이즈 포함)"""
    profile = DEVICE_PROFILES[device]
    
    # 냉장고는 컴프레서 주기적 가동
    if device == "fridge":
        is_active = random.random() < profile["on_probability"]
    else:
        # 상태 전환 확률 체크
        if random.random() < profile["toggle_prob"]:
            device_states[house][device] = not device_states[house][device]
        is_active = device_states[house][device]

    if is_active:
        low, high = profile["active_w"]
        val = random.uniform(low, high) + random.gauss(0, 1.5)
    else:
        low, high = profile["standby_w"]
        val = random.uniform(low, high)
        
    return max(0.0, val)

def calculate_main_panel_power(house: str) -> float:
    """가구 내 모든 가전 및 상시 대기전력을 합산하여 메인 분전반 총 전력(W) 계산"""
    # 1. 상시 기저 부하(기본 대기전력)
    total_w = random.uniform(*BASE_STANDBY_W)
    
    # 2. 가구 내 각 가전기기 소비 전력 합산
    for device in DEVICE_PROFILES:
        total_w += generate_device_power(house, device)
        
    # 3. 메인 계측기 전체 미세 센서 노이즈 추가
    total_w += random.gauss(0, 1.0)
    
    return round(max(0.0, total_w), 2)

# ==========================================
# 3. 메인 전송 루프
# ==========================================
def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(BROKER_USER, BROKER_PASS)
    
    print(f"Mosquitto 브로커 ({BROKER_HOST}:{BROKER_PORT}) 연결 중...")
    client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
    client.loop_start()
    print(f" 총 {len(HOUSES)}개 가구의 메인 분전반 전력 데이터 전송을 시작합니다 (종료: Ctrl+C)")

    try:
        while True:
            # ISO 8601 UTC 타임스탬프 (밀리초 포함: YYYY-MM-DDTHH:mm:ss.sssZ)
            now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

            for house in HOUSES:
                main_power = calculate_main_panel_power(house)

                # 메인 분전반 계측 데이터 페이로드
                payload = {
                    "house": house,
                    "device": "main",
                    "ts": now_iso,
                    "power_w": main_power
                }

                # 메인 분전반 토픽 발행 (v1/power/sim/{house}/main)
                topic = f"v1/power/sim/{house}/main"
                client.publish(topic, json.dumps(payload), qos=1)

            print(f"[{now_iso}] {len(HOUSES)}개 가구 메인 분전반(main) 데이터 발행 완료")
            time.sleep(1)  # 1초마다 전송 (주기 조절 가능)

    except KeyboardInterrupt:
        print("\n시뮬레이터를 정지합니다.")
    finally:
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    main()
