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

# 시뮬레이션 대상 가구 목록 (필요에 따라 확장 가능)
HOUSES = ["H001", "H002", "H003"]

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

# ==========================================
# 2. 각 가구/기기별 상태(State) 관리
# ==========================================
device_states = {}
for house in HOUSES:
    device_states[house] = {}
    for device in DEVICE_PROFILES:
        # 초기 상태: 30% 확률로 켜져 있음
        device_states[house][device] = random.random() < 0.3

def generate_power(house: str, device: str) -> float:
    """기기 상태에 따른 현실적인 소비 전력 계산 (노이즈 포함)"""
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
        # 기기 고유 전력 범위 내에서 미세한 가우시안 노이즈 추가
        val = random.uniform(low, high) + random.gauss(0, 1.5)
    else:
        low, high = profile["standby_w"]
        val = random.uniform(low, high)
        
    return round(max(0.0, val), 2)

# ==========================================
# 3. 메인 전송 루프
# ==========================================
def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(BROKER_USER, BROKER_PASS)
    
    print(f"Mosquitto 브로커 ({BROKER_HOST}:{BROKER_PORT}) 연결 중...")
    client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
    client.loop_start()
    print(" 연결 성공 전력 데이터 전송을 시작합니다 (종료: Ctrl+C)")

    try:
        while True:
            # ISO 8601 UTC 타임스탬프 (밀리초 포함: YYYY-MM-DDTHH:mm:ss.sssZ)
            now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

            for house in HOUSES:
                for device in DEVICE_PROFILES:
                    power = generate_power(house, device)

                    # 요구하신 JSON 스키마
                    payload = {
                        "house": house,
                        "device": device,
                        "ts": now_iso,
                        "power_w": power
                    }

                    topic = f"v1/power/sim/{house}/{device}"
                    client.publish(topic, json.dumps(payload), qos=1)

            print(f"[{now_iso}] {len(HOUSES)}개 가구 ({len(DEVICE_PROFILES)}개 기기) 데이터 발행 완료")
            time.sleep(1)  # 1초마다 전송 (주기 조절 가능)

    except KeyboardInterrupt:
        print("\n시뮬레이터를 정지합니다.")
    finally:
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    main()
