"""Bronze Parquet schema for appliance usage session changes.

One Parquet row is one *version* of one session: an outbox event (INSERT /
UPDATE / DELETE) or a snapshot row from the initial full load. Rows that share
``session_id`` and ``session_version`` must have identical content, which is
what lets the verifier drop duplicates safely and treat any difference as an
error.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import io
import json
from typing import Any, Iterable
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq


SCHEMA_VERSION = 1
OPERATION_SNAPSHOT = "SNAPSHOT"
OPERATION_DELETE = "DELETE"
PROBABILITY_QUANTUM = Decimal("0.0001")

LAKE_SCHEMA = pa.schema(
    [
        # outbox 식별자. 초기 적재 SNAPSHOT 행은 null.
        ("event_id", pa.int64()),
        ("operation", pa.string()),
        ("session_id", pa.string()),
        ("session_version", pa.int32()),
        ("is_deleted", pa.bool_()),
        # 변경(outbox) 또는 스냅샷 시각
        ("changed_at", pa.timestamp("us", tz="UTC")),
        # 세션 내용. DELETE 행은 부모 조인 값(가구·가전·관측일)이 null이다.
        ("activity_daily_id", pa.string()),
        ("household_id", pa.string()),
        ("appliance_type", pa.string()),
        ("observation_date", pa.date32()),
        ("started_at", pa.timestamp("us", tz="UTC")),
        ("ended_at", pa.timestamp("us", tz="UTC")),
        ("max_probability", pa.decimal128(5, 4)),
        ("decision_threshold", pa.decimal128(5, 4)),
        ("updated_at", pa.timestamp("us", tz="UTC")),
        # 적재 배치 정보
        ("batch_id", pa.string()),
        ("batch_kind", pa.string()),
        ("schema_version", pa.int32()),
    ]
)

# 같은 세션·버전이면 반드시 같아야 하는 컬럼. 배치·이벤트 식별자는 제외한다.
CONTENT_FIELDS = (
    "session_id",
    "session_version",
    "is_deleted",
    "activity_daily_id",
    "household_id",
    "appliance_type",
    "observation_date",
    "started_at",
    "ended_at",
    "max_probability",
    "decision_threshold",
    "updated_at",
)


def normalize_timestamp(value: Any) -> datetime | None:
    """Return an aware UTC datetime for DB values or JSON strings."""

    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
    else:
        raise TypeError(f"timestamp value must be datetime or str: {type(value).__name__}")
    if parsed.tzinfo is None:
        # SQLite는 시간대를 저장하지 않는다. 저장 시각은 UTC로 간주한다.
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def normalize_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value.strip())
    raise TypeError(f"date value must be date or str: {type(value).__name__}")


def normalize_probability(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value.quantize(PROBABILITY_QUANTUM)
    # psycopg는 jsonb 숫자를 float로 돌려준다. NUMERIC(5,4) 범위에서는
    # repr 문자열이 원래 값을 그대로 복원한다.
    return Decimal(str(value)).quantize(PROBABILITY_QUANTUM)


def normalize_uuid_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, UUID):
        return str(value)
    return str(UUID(str(value)))


def outbox_event_to_row(event: Any, batch: Any) -> dict[str, Any]:
    """Map one ``session_lake_outbox`` row to a lake row."""

    payload = event.payload
    is_deleted = event.operation == OPERATION_DELETE
    return {
        "event_id": int(event.event_id),
        "operation": event.operation,
        "session_id": normalize_uuid_text(event.session_id),
        "session_version": int(event.session_version),
        "is_deleted": is_deleted,
        "changed_at": normalize_timestamp(event.changed_at),
        "activity_daily_id": normalize_uuid_text(payload.get("activity_daily_id")),
        "household_id": None if is_deleted else payload.get("household_id"),
        "appliance_type": None if is_deleted else payload.get("appliance_type"),
        "observation_date": (
            None if is_deleted else normalize_date(payload.get("observation_date"))
        ),
        "started_at": normalize_timestamp(payload.get("started_at")),
        "ended_at": normalize_timestamp(payload.get("ended_at")),
        "max_probability": normalize_probability(payload.get("max_probability")),
        "decision_threshold": normalize_probability(payload.get("decision_threshold")),
        "updated_at": normalize_timestamp(payload.get("updated_at")),
        "batch_id": str(batch.batch_id),
        "batch_kind": batch.batch_kind,
        "schema_version": int(batch.schema_version),
    }


def session_content_row(record: Any) -> dict[str, Any]:
    """Content columns of a live DB session (joined with its parents)."""

    return {
        "session_id": normalize_uuid_text(record.id),
        "session_version": int(record.lake_version),
        "is_deleted": False,
        "activity_daily_id": normalize_uuid_text(record.activity_daily_id),
        "household_id": record.household_id,
        "appliance_type": record.appliance_type,
        "observation_date": normalize_date(record.observation_date),
        "started_at": normalize_timestamp(record.started_at),
        "ended_at": normalize_timestamp(record.ended_at),
        "max_probability": normalize_probability(record.max_probability),
        "decision_threshold": normalize_probability(record.decision_threshold),
        "updated_at": normalize_timestamp(record.updated_at),
    }


def snapshot_to_row(record: Any, batch: Any, snapshot_at: datetime) -> dict[str, Any]:
    """Map one session from the initial-load snapshot to a lake row."""

    return {
        "event_id": None,
        "operation": OPERATION_SNAPSHOT,
        **session_content_row(record),
        "changed_at": normalize_timestamp(snapshot_at),
        "batch_id": str(batch.batch_id),
        "batch_kind": batch.batch_kind,
        "schema_version": int(batch.schema_version),
    }


def build_table(rows: list[dict[str, Any]]) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=LAKE_SCHEMA)


def parquet_bytes(table: pa.Table) -> bytes:
    sink = io.BytesIO()
    pq.write_table(table, sink, compression="snappy")
    return sink.getvalue()


def parquet_row_count(data: bytes) -> int:
    return int(pq.read_metadata(io.BytesIO(data)).num_rows)


def read_parquet_rows(data: bytes) -> list[dict[str, Any]]:
    table = pq.read_table(io.BytesIO(data))
    rows = table.to_pylist()
    for row in rows:
        for column in ("changed_at", "started_at", "ended_at", "updated_at"):
            row[column] = normalize_timestamp(row.get(column))
        for column in ("max_probability", "decision_threshold"):
            row[column] = normalize_probability(row.get(column))
        row["observation_date"] = normalize_date(row.get("observation_date"))
    return rows


def _canonical(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, datetime):
        return normalize_timestamp(value).isoformat(timespec="microseconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value.quantize(PROBABILITY_QUANTUM))
    if isinstance(value, UUID):
        return str(value)
    return str(value)


def content_signature(row: dict[str, Any]) -> str:
    """Canonical JSON of the content columns; equal rows produce equal strings."""

    return json.dumps(
        {field: _canonical(row.get(field)) for field in CONTENT_FIELDS},
        sort_keys=True,
        separators=(",", ":"),
    )


def content_fingerprint(rows: Iterable[dict[str, Any]]) -> str:
    """Order-independent SHA-256 over the content of all rows."""

    signatures = sorted(content_signature(row) for row in rows)
    digest = hashlib.sha256()
    for signature in signatures:
        digest.update(signature.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
