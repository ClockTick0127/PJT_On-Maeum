"""Kafka -> HDFS Bronze 적재기 v2 (원천 보관 Consumer).

HDFS_수집·일일배치_설계안(0914)의 지적 4건을 반영한 개정판:
  1) 안전한 커밋: 임시 경로 업로드 -> 크기 검증 -> rename -> manifest -> offset commit
     파일명에 (파티션, offset 구간)을 넣어 재처리·동시 실행에도 덮어쓰기가 없다.
  2) 오류 격리(Quarantine): 깨진 메시지도 버리지 않고 원본 바이트로 격리 저장.
     격리 저장이 성공하기 전에는 해당 구간을 커밋하지 않는다.
  3) 스몰 파일 완화: 가구별 디렉터리 제거(가구는 컬럼), Kafka 파티션 단위로 묶어
     5분 / 50,000건 중 먼저 도달하는 조건으로 플러시.
  4) 토픽 통일: 기본 power.raw.v1 (브릿지·B Compose와 동일).

보존 필드: 현재 메시지 계약(message_id/household_id/device_id/measured_at/
active_power/reactive_power/power_factor/current) + Kafka 위치(topic/partition/offset/
kafka_ts) + 수신 시각 + 원본 payload 바이트.

경로 구조 (도착일 기준):
  {BRONZE}/ingest_date=YYYY-MM-DD/hour=HH/partition=P/part-P-{startOffset}-{endOffset}.parquet
  {QUARANTINE}/ingest_date=YYYY-MM-DD/partition=P/part-P-{...}.parquet
  {MANIFESTS}/job=bronze-loader/date=YYYY-MM-DD/manifest-P-{...}.json
"""
import io
import json
import math
import os
import signal
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, TopicPartition
from hdfs import InsecureClient

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "power.raw.v1")
GROUP_ID = os.getenv("GROUP_ID", "hdfs-bronze-loader")
HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")
BRONZE_BASE = os.getenv("BRONZE_BASE", "/nilm/bronze/power")
QUARANTINE_BASE = os.getenv("QUARANTINE_BASE", "/nilm/quarantine/power")
MANIFEST_BASE = os.getenv("MANIFEST_BASE", "/nilm/manifests/job=bronze-loader")
FLUSH_SECS = int(os.getenv("FLUSH_SECS", "300"))
MAX_BUFFER = int(os.getenv("MAX_BUFFER", "50000"))
BUSINESS_TIMEZONE_NAME = os.getenv("BUSINESS_TIMEZONE", "Asia/Seoul")
# Korea has no daylight-saving transition. The fixed offset also keeps local
# contract tests portable on Windows hosts without the optional tzdata wheel.
BUSINESS_TIMEZONE = (
    timezone(timedelta(hours=9))
    if BUSINESS_TIMEZONE_NAME == "Asia/Seoul"
    else ZoneInfo(BUSINESS_TIMEZONE_NAME)
)

OK_SCHEMA = pa.schema([
    ("message_id", pa.string()),
    ("household_id", pa.string()),
    ("device_id", pa.string()),
    ("measured_at", pa.string()),   # 생산자 측정 시각 (ISO 8601, timezone 필수)
    ("active_power", pa.float64()),
    ("reactive_power", pa.float64()),
    ("power_factor", pa.float64()),
    ("current", pa.float64()),
    ("topic", pa.string()),
    ("partition", pa.int32()),
    ("kafka_offset", pa.int64()),
    ("kafka_ts", pa.string()),      # 브로커 기록 시각
    ("ingested_at", pa.string()),   # 적재기 수신 시각
    ("raw_payload", pa.binary()),   # 원본 바이트 — 재처리·재해석용
])

QUARANTINE_SCHEMA = pa.schema([
    ("error_code", pa.string()),
    ("topic", pa.string()),
    ("partition", pa.int32()),
    ("kafka_offset", pa.int64()),
    ("kafka_ts", pa.string()),
    ("ingested_at", pa.string()),
    ("raw_payload", pa.binary()),
])

running = True


def iso(epoch_ms: int | None) -> str:
    if epoch_ms is None or epoch_ms < 0:
        return ""
    return datetime.fromtimestamp(epoch_ms / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def required_id(row: dict, field_name: str) -> str:
    value = row[field_name]
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    value = value.strip()
    if not 1 <= len(value) <= 50:
        raise ValueError(f"{field_name} length must be between 1 and 50")
    return value


def required_number(row: dict, field_name: str) -> float:
    value = row[field_name]
    if isinstance(value, bool):
        raise TypeError(f"{field_name} must be a number")
    value = float(value)
    if not math.isfinite(value):
        raise ValueError(f"{field_name} must be finite")
    return value


def parse_measurement(raw: bytes) -> dict:
    """신규 power.raw.v1 계약을 검증하고 Bronze 컬럼으로 정규화한다."""
    row = json.loads(raw)
    if not isinstance(row, dict):
        raise TypeError("payload must be a JSON object")

    message_id = str(uuid.UUID(str(row["message_id"])))
    household_id = required_id(row, "household_id")
    device_id = required_id(row, "device_id")

    measured_at_raw = row["measured_at"]
    if not isinstance(measured_at_raw, str):
        raise TypeError("measured_at must be a string")
    measured_at = datetime.fromisoformat(measured_at_raw.replace("Z", "+00:00"))
    if measured_at.tzinfo is None or measured_at.utcoffset() is None:
        raise ValueError("measured_at must include a timezone")

    active_power = required_number(row, "active_power")
    reactive_power = required_number(row, "reactive_power")
    power_factor = required_number(row, "power_factor")
    current = required_number(row, "current")
    if active_power < 0:
        raise ValueError("active_power must be greater than or equal to 0")
    if not -1 <= power_factor <= 1:
        raise ValueError("power_factor must be between -1 and 1")
    if current < 0:
        raise ValueError("current must be greater than or equal to 0")

    return {
        "message_id": message_id,
        "household_id": household_id,
        "device_id": device_id,
        "measured_at": measured_at.isoformat(),
        "active_power": active_power,
        "reactive_power": reactive_power,
        "power_factor": power_factor,
        "current": current,
    }


def measurement_manifest_summary(rows: list[dict]) -> dict:
    """Return the event-time range needed for safe downstream retention."""
    if not rows:
        return {
            "min_measured_at": None,
            "max_measured_at": None,
            "business_dates": [],
        }
    measured = [datetime.fromisoformat(row["measured_at"]) for row in rows]
    return {
        "min_measured_at": min(measured).astimezone(timezone.utc).isoformat(),
        "max_measured_at": max(measured).astimezone(timezone.utc).isoformat(),
        "business_dates": sorted(
            {value.astimezone(BUSINESS_TIMEZONE).date().isoformat() for value in measured}
        ),
    }


@dataclass
class PartitionBuffer:
    """Kafka 파티션 하나의 미커밋 구간 버퍼."""
    ok_rows: list = field(default_factory=list)
    quarantine_rows: list = field(default_factory=list)
    first_offset: int | None = None
    last_offset: int | None = None
    opened_at: float = field(default_factory=time.time)

    def count(self) -> int:
        return len(self.ok_rows) + len(self.quarantine_rows)


class BronzeLoader:

    def __init__(self):
        self.consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,   # 저장 완료 구간만 수동 커밋
        })
        self.hdfs = InsecureClient(HDFS_URL, user="root")
        self.buffers: dict[int, PartitionBuffer] = {}

    # ---------- 소비 ----------

    def run(self):
        # rebalance로 파티션을 뺏기기 전에 해당 버퍼를 안전하게 마감한다
        self.consumer.subscribe([KAFKA_TOPIC], on_revoke=self.on_revoke)
        print(f"bronze loader v2: {KAFKA_TOPIC} -> {BRONZE_BASE} "
              f"(flush {FLUSH_SECS}s / {MAX_BUFFER}건, quarantine {QUARANTINE_BASE})", flush=True)
        while running:
            msg = self.consumer.poll(1.0)
            now = time.time()
            if msg is not None and not msg.error():
                self.ingest(msg)
            self.flush_due(now)
        self.flush_all("shutdown")
        self.consumer.close()
        print("bronze loader stopped", flush=True)

    def ingest(self, msg):
        p = msg.partition()
        buf = self.buffers.setdefault(p, PartitionBuffer())
        if buf.first_offset is None:
            buf.first_offset = msg.offset()
        buf.last_offset = msg.offset()

        raw = msg.value() or b""
        meta = {
            "topic": msg.topic(),
            "partition": p,
            "kafka_offset": msg.offset(),
            "kafka_ts": iso(msg.timestamp()[1]),
            "ingested_at": iso(int(time.time() * 1000)),
            "raw_payload": raw,
        }
        try:
            record = {
                **parse_measurement(raw),
                **meta,
            }
            buf.ok_rows.append(record)
        except (json.JSONDecodeError, UnicodeDecodeError):
            buf.quarantine_rows.append({"error_code": "MALFORMED_JSON", **meta})
        except (KeyError, TypeError, ValueError) as e:
            buf.quarantine_rows.append({"error_code": f"SCHEMA_VIOLATION:{type(e).__name__}", **meta})

        if buf.count() >= MAX_BUFFER:
            self.flush_partition(p, "max-buffer")

    # ---------- 플러시 ----------

    def flush_due(self, now: float):
        for p in list(self.buffers):
            buf = self.buffers[p]
            if buf.count() > 0 and now - buf.opened_at >= FLUSH_SECS:
                self.flush_partition(p, "interval")

    def flush_all(self, reason: str):
        for p in list(self.buffers):
            if self.buffers[p].count() > 0:
                self.flush_partition(p, reason)

    def on_revoke(self, consumer, partitions):
        revoked = [tp.partition for tp in partitions]
        print(f"rebalance revoke: {revoked} — 버퍼 마감", flush=True)
        for p in revoked:
            if p in self.buffers and self.buffers[p].count() > 0:
                self.flush_partition(p, "rebalance")

    def flush_partition(self, p: int, reason: str):
        """한 파티션의 연속 offset 구간을 확정: 파일 저장 -> manifest -> offset commit."""
        buf = self.buffers.pop(p)
        span = f"{buf.first_offset}-{buf.last_offset}"
        now = datetime.now(timezone.utc)
        ingest_date, hour = now.strftime("%Y-%m-%d"), now.strftime("%H")
        files = []

        if buf.ok_rows:
            path = (f"{BRONZE_BASE}/ingest_date={ingest_date}/hour={hour}/partition={p}"
                    f"/part-{p}-{span}.parquet")
            files.append(self.write_parquet(path, buf.ok_rows, OK_SCHEMA))
        if buf.quarantine_rows:
            path = (f"{QUARANTINE_BASE}/ingest_date={ingest_date}/partition={p}"
                    f"/part-{p}-{span}.parquet")
            files.append(self.write_parquet(path, buf.quarantine_rows, QUARANTINE_SCHEMA))

        # 구간의 입출력을 기록하는 manifest — 일일 마감 배치의 입력 스냅샷 근거
        measurement_summary = measurement_manifest_summary(buf.ok_rows)
        file_bytes = sum(self.hdfs.status(path)["length"] for path in files)
        manifest = {
            "topic": KAFKA_TOPIC, "partition": p,
            "start_offset": buf.first_offset, "end_offset": buf.last_offset,
            "ok_count": len(buf.ok_rows), "quarantine_count": len(buf.quarantine_rows),
            **measurement_summary,
            "file_bytes": file_bytes,
            "files": files, "flush_reason": reason, "committed_at": iso(int(time.time() * 1000)),
        }
        mpath = f"{MANIFEST_BASE}/date={ingest_date}/manifest-{p}-{span}.json"
        self.atomic_upload(mpath, json.dumps(manifest, ensure_ascii=False, indent=1).encode())

        # 정상+격리 저장이 모두 끝난 뒤에만 다음 offset 커밋 (at-least-once)
        self.consumer.commit(offsets=[TopicPartition(KAFKA_TOPIC, p, buf.last_offset + 1)],
                             asynchronous=False)
        print(f"flushed p{p} offsets {span}: ok={len(buf.ok_rows)} "
              f"quarantine={len(buf.quarantine_rows)} ({reason})", flush=True)

    # ---------- HDFS 쓰기 ----------

    def write_parquet(self, final_path: str, rows: list, schema: pa.Schema) -> str:
        sink = io.BytesIO()
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), sink, compression="snappy")
        self.atomic_upload(final_path, sink.getvalue())
        return final_path

    def atomic_upload(self, final_path: str, data: bytes):
        """임시 경로 업로드 -> 크기 검증 -> rename. 실패 시 예외로 중단(커밋 안 됨)."""
        tmp_path = final_path + ".tmp"
        parent = final_path.rsplit("/", 1)[0]
        self.hdfs.makedirs(parent)
        self.hdfs.write(tmp_path, data=data, overwrite=True)
        size = self.hdfs.status(tmp_path)["length"]
        if size != len(data):
            raise IOError(f"업로드 크기 불일치: {tmp_path} ({size} != {len(data)})")
        self.hdfs.delete(final_path)  # 동일 구간 재처리 시 이전 결과 교체 (구간이 같으므로 내용 동일)
        self.hdfs.rename(tmp_path, final_path)


def stop(*_):
    global running
    running = False


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    BronzeLoader().run()
