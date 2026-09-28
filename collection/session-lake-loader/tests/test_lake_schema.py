from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

from session_lake_loader.lake_schema import (
    LAKE_SCHEMA,
    build_table,
    content_fingerprint,
    content_signature,
    normalize_probability,
    normalize_timestamp,
    outbox_event_to_row,
    parquet_bytes,
    read_parquet_rows,
    session_content_row,
    snapshot_to_row,
)


BATCH = SimpleNamespace(batch_id=uuid4(), batch_kind="INCREMENTAL", schema_version=1)


def test_timestamp_normalization_accepts_db_and_json_values() -> None:
    aware = datetime(2026, 9, 20, 9, 0, tzinfo=timezone.utc)
    assert normalize_timestamp("2026-09-20T18:00:00+09:00") == aware
    assert normalize_timestamp("2026-09-20T09:00:00Z") == aware
    assert normalize_timestamp(datetime(2026, 9, 20, 9, 0)) == aware
    assert normalize_timestamp(None) is None


def test_probability_normalization_matches_numeric_scale() -> None:
    assert normalize_probability(0.5) == Decimal("0.5000")
    assert normalize_probability("0.1234") == Decimal("0.1234")
    assert normalize_probability(Decimal("0.80")) == Decimal("0.8000")


def test_outbox_update_and_snapshot_of_same_version_have_equal_content() -> None:
    session_id = uuid4()
    activity_id = uuid4()
    started = datetime(2026, 9, 20, 9, 0, tzinfo=timezone.utc)
    ended = datetime(2026, 9, 20, 9, 5, tzinfo=timezone.utc)
    event = SimpleNamespace(
        event_id=7,
        session_id=session_id,
        session_version=2,
        operation="UPDATE",
        changed_at=ended,
        payload={
            "id": str(session_id),
            "activity_daily_id": str(activity_id),
            "started_at": "2026-09-20T18:00:00+09:00",
            "ended_at": "2026-09-20T18:05:00+09:00",
            "max_probability": 0.8,
            "decision_threshold": 0.5,
            "updated_at": "2026-09-20T18:05:00+09:00",
            "household_id": "H001",
            "appliance_type": "MICROWAVE",
            "observation_date": "2026-09-20",
        },
    )
    record = SimpleNamespace(
        id=session_id,
        activity_daily_id=activity_id,
        household_id="H001",
        appliance_type="MICROWAVE",
        observation_date=date(2026, 9, 20),
        started_at=started,
        ended_at=ended,
        max_probability=Decimal("0.8000"),
        decision_threshold=Decimal("0.5000"),
        updated_at=ended,
        lake_version=2,
    )
    outbox_row = outbox_event_to_row(event, BATCH)
    snapshot_row = snapshot_to_row(record, BATCH, ended)

    assert outbox_row["operation"] == "UPDATE"
    assert snapshot_row["operation"] == "SNAPSHOT"
    assert content_signature(outbox_row) == content_signature(snapshot_row)
    assert content_signature(outbox_row) == content_signature(session_content_row(record))


def test_delete_event_row_is_a_tombstone_without_parent_fields() -> None:
    session_id = uuid4()
    event = SimpleNamespace(
        event_id=8,
        session_id=session_id,
        session_version=3,
        operation="DELETE",
        changed_at=datetime(2026, 9, 20, 10, tzinfo=timezone.utc),
        payload={
            "id": str(session_id),
            "activity_daily_id": str(uuid4()),
            "started_at": "2026-09-20T09:00:00+00:00",
            "ended_at": None,
            "max_probability": 0.8,
            "decision_threshold": 0.5,
            "updated_at": "2026-09-20T09:00:00+00:00",
        },
    )
    row = outbox_event_to_row(event, BATCH)

    assert row["is_deleted"] is True
    assert row["household_id"] is None
    assert row["appliance_type"] is None
    assert row["observation_date"] is None
    assert row["session_version"] == 3


def test_parquet_round_trip_preserves_content_fingerprint() -> None:
    session_id = uuid4()
    rows = [
        {
            "event_id": 1,
            "operation": "INSERT",
            "session_id": str(session_id),
            "session_version": 1,
            "is_deleted": False,
            "changed_at": datetime(2026, 9, 20, 9, 0, 0, 123456, tzinfo=timezone.utc),
            "activity_daily_id": str(uuid4()),
            "household_id": "H001",
            "appliance_type": "KETTLE",
            "observation_date": date(2026, 9, 20),
            "started_at": datetime(2026, 9, 20, 9, 0, tzinfo=timezone.utc),
            "ended_at": None,
            "max_probability": Decimal("0.9123"),
            "decision_threshold": Decimal("0.5000"),
            "updated_at": datetime(2026, 9, 20, 9, 0, tzinfo=timezone.utc),
            "batch_id": str(BATCH.batch_id),
            "batch_kind": "INCREMENTAL",
            "schema_version": 1,
        }
    ]
    data = parquet_bytes(build_table(rows))
    restored = read_parquet_rows(data)

    assert len(restored) == 1
    assert restored[0]["max_probability"] == Decimal("0.9123")
    assert restored[0]["observation_date"] == date(2026, 9, 20)
    assert content_fingerprint(restored) == content_fingerprint(rows)
    assert set(LAKE_SCHEMA.names) == set(rows[0])
