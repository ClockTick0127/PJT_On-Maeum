"""Kafka -> HDFS Parquet 적재 잡 (스트림 원본 보존, 배치 레이어의 재료).

power 토픽을 구독해서 가구/날짜 파티션 구조로 HDFS에 Parquet 적재:
  /data/stream/house=H001/date=2026-09-03/batch_1756900000.parquet

- FLUSH_SECS(기본 60초)마다, 또는 버퍼가 MAX_BUFFER를 넘으면 플러시
- 종료 신호(SIGTERM/Ctrl+C) 시 잔여 버퍼를 마저 플러시 (유실 방지)
- 커밋은 플러시 성공 후에만 (at-least-once: 장애 시 중복은 가능, 유실은 없음)
"""
import json
import os
import signal
import time
from collections import defaultdict

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer
from hdfs import InsecureClient

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "power-raw")
GROUP_ID = os.getenv("GROUP_ID", "hdfs-loader")
HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")
HDFS_BASE = os.getenv("HDFS_BASE", "/data/stream")
FLUSH_SECS = int(os.getenv("FLUSH_SECS", "60"))
MAX_BUFFER = int(os.getenv("MAX_BUFFER", "50000"))

SCHEMA = pa.schema([
    ("house", pa.string()),
    ("device", pa.string()),
    ("ts", pa.string()),      # ISO 8601 UTC
    ("power_w", pa.float64()),
])

running = True


def flush(hdfs: InsecureClient, buffer: dict) -> int:
    """버퍼를 (house, date)별 Parquet 파일로 HDFS에 업로드. 총 적재 행 수 반환."""
    total = 0
    now = int(time.time())
    for (house, date), rows in buffer.items():
        table = pa.Table.from_pylist(rows, schema=SCHEMA)
        local = f"/tmp/{house}_{date}_{now}.parquet"
        pq.write_table(table, local, compression="snappy")
        remote_dir = f"{HDFS_BASE}/house={house}/date={date}"
        hdfs.makedirs(remote_dir)
        hdfs.upload(f"{remote_dir}/batch_{now}.parquet", local, overwrite=True)
        os.remove(local)
        total += len(rows)
    buffer.clear()
    return total


def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,   # 플러시 성공 후 수동 커밋
    })
    consumer.subscribe([KAFKA_TOPIC])
    hdfs = InsecureClient(HDFS_URL, user="root")

    buffer = defaultdict(list)   # (house, date) -> rows
    buffered = 0
    last_flush = time.time()
    print(f"loader start: topic={KAFKA_TOPIC} -> {HDFS_URL}{HDFS_BASE} (flush {FLUSH_SECS}s)", flush=True)

    while running:
        msg = consumer.poll(1.0)
        if msg is not None and not msg.error():
            try:
                row = json.loads(msg.value())
                date = row["ts"][:10]            # ISO ts 앞 10자 = YYYY-MM-DD
                buffer[(row["house"], date)].append({
                    "house": row["house"],
                    "device": row.get("device", "main"),
                    "ts": row["ts"],
                    "power_w": float(row["power_w"]),
                })
                buffered += 1
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                print(f"[skip] malformed message: {e}", flush=True)

        if buffered and (time.time() - last_flush >= FLUSH_SECS or buffered >= MAX_BUFFER):
            n = flush(hdfs, buffer)
            consumer.commit(asynchronous=False)
            print(f"flushed {n} rows", flush=True)
            buffered, last_flush = 0, time.time()

    if buffered:
        n = flush(hdfs, buffer)
        consumer.commit(asynchronous=False)
        print(f"final flush {n} rows", flush=True)
    consumer.close()
    print("loader stopped", flush=True)


def stop(*_):
    global running
    running = False


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    main()
