"""수신 검증 — 원천 토픽을 N초간 소비해서 가구별 수신 건수/속도를 출력."""
import argparse
import json
import os
import time
from collections import Counter

from confluent_kafka import Consumer

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--seconds", type=int, default=10)
    ap.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "power.raw.v1"))
    args = ap.parse_args()

    c = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": f"check-{int(time.time())}",
        "auto.offset.reset": "latest",
    })
    c.subscribe([args.topic])

    counts = Counter()
    partitions = Counter()
    deadline = time.time() + args.seconds
    print(f"consuming for {args.seconds}s ...")
    while time.time() < deadline:
        msg = c.poll(0.5)
        if msg is None or msg.error():
            continue
        data = json.loads(msg.value())
        counts[data["house"]] += 1
        partitions[msg.partition()] += 1
    c.close()

    total = sum(counts.values())
    print(f"\ntotal={total} msgs  ({total / args.seconds:.1f} msg/s)")
    print("\nhouse별 수신:")
    for house in sorted(counts):
        print(f"  {house}: {counts[house]}")
    print("\npartition별 분배:")
    for part in sorted(partitions):
        print(f"  partition {part}: {partitions[part]}")
