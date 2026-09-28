"""리플레이 시뮬레이터 — 가상 다가구 전력 데이터를 Kafka(power-raw)로 발행.

두 가지 모드:
  1) 합성 모드(기본): 가구별 상태머신으로 냉장고 사이클 + 랜덤 가전 이벤트를 생성
  2) CSV 모드(--csv): timestamp,house,power_w 컬럼의 실데이터를 타임스탬프 순 재생

사용 예:
  python producer.py                          # 10가구, 1Hz, 실시간 배속
  python producer.py --houses 10 --hz 10 --speed 10
  python producer.py --csv data/enertalk_h01.csv --speed 60
"""
import argparse
import json
import os
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

TOPIC = os.getenv("KAFKA_TOPIC", "power.raw.v1")  # 브릿지·적재기와 통일 (설계안 0914)


def to_iso(epoch: float) -> str:
    """epoch 초 → ISO 8601 UTC 문자열 (밀리초 포함, MQTT 시뮬레이터와 동일 포맷)."""
    dt = datetime.fromtimestamp(epoch, timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

# ---------- 합성 데이터: 가구별 상태머신 ----------

APPLIANCE_PROFILES = {
    # (정상소비 W, 지속시간 초 범위, 시간당 발생 확률)
    "kettle":    (1800, (120, 240), 0.15),
    "microwave": (1100, (30, 180),  0.20),
    "fan":       (45,   (600, 3600), 0.10),
}
FRIDGE_ON_W, FRIDGE_ON_S, FRIDGE_OFF_S = 120, 900, 1800  # 15분 운전 / 30분 휴지
BASE_W = 60  # 대기전력


class House:
    def __init__(self, house_id: str, rng: random.Random):
        self.id = house_id
        self.rng = rng
        # 냉장고 위상을 가구마다 다르게 시작
        self.fridge_timer = rng.uniform(0, FRIDGE_ON_S + FRIDGE_OFF_S)
        self.active = {}  # appliance -> 남은 초

    def tick(self, dt: float) -> float:
        """dt초 경과 후 현재 총 전력(W)을 반환."""
        power = BASE_W + self.rng.gauss(0, 2)

        # 냉장고 사이클
        self.fridge_timer = (self.fridge_timer + dt) % (FRIDGE_ON_S + FRIDGE_OFF_S)
        if self.fridge_timer < FRIDGE_ON_S:
            surge = 3.0 if self.fridge_timer < 1.5 else 1.0  # 기동 돌입전류
            power += FRIDGE_ON_W * surge

        # 진행 중인 가전
        for name in list(self.active):
            self.active[name] -= dt
            if self.active[name] <= 0:
                del self.active[name]
            else:
                power += APPLIANCE_PROFILES[name][0]

        # 새 가전 이벤트 발생 (시간당 확률을 dt 기준으로 환산)
        for name, (_, dur, p_hour) in APPLIANCE_PROFILES.items():
            if name not in self.active and self.rng.random() < p_hour * dt / 3600:
                self.active[name] = self.rng.uniform(*dur)

        return round(power, 1)


def run_synthetic(producer: Producer, n_houses: int, hz: float, speed: float):
    rng = random.Random(42)
    houses = [House(f"H{i:03d}", random.Random(rng.random())) for i in range(1, n_houses + 1)]
    dt = 1.0 / hz  # 시뮬레이션 시간 간격(초)
    sim_time = time.time()
    sent = 0
    print(f"[synthetic] houses={n_houses} hz={hz} speed=x{speed} -> topic '{TOPIC}'")
    try:
        while True:
            for h in houses:
                msg = {"house": h.id, "device": "main", "ts": to_iso(sim_time), "power_w": h.tick(dt)}
                producer.produce(TOPIC, key=h.id, value=json.dumps(msg))
                sent += 1
            producer.poll(0)
            if sent % (n_houses * int(hz) * 10 or 1) < n_houses:
                print(f"  sent={sent}")
            sim_time += dt
            time.sleep(dt / speed)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        print(f"done. total sent={sent}")


# ---------- CSV 리플레이 ----------

def run_csv(producer: Producer, path: str, speed: float):
    import csv
    print(f"[csv] {path} speed=x{speed} -> topic '{TOPIC}'")
    sent, prev_ts = 0, None
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            ts = float(row["timestamp"])
            if prev_ts is not None and ts > prev_ts:
                time.sleep((ts - prev_ts) / speed)
            prev_ts = ts
            msg = {"house": row["house"], "device": "main", "ts": to_iso(ts), "power_w": float(row["power_w"])}
            producer.produce(TOPIC, key=row["house"], value=json.dumps(msg))
            producer.poll(0)
            sent += 1
            if sent % 10000 == 0:
                print(f"  sent={sent}")
    producer.flush()
    print(f"done. total sent={sent}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--houses", type=int, default=10)
    ap.add_argument("--hz", type=float, default=1.0, help="가구당 초당 측정 횟수")
    ap.add_argument("--speed", type=float, default=1.0, help="재생 배속")
    ap.add_argument("--csv", help="CSV 재생 모드 (timestamp,house,power_w)")
    args = ap.parse_args()

    p = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 20})
    if args.csv:
        run_csv(p, args.csv, args.speed)
    else:
        run_synthetic(p, args.houses, args.hz, args.speed)
