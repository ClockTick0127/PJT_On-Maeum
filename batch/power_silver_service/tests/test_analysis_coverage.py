from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from power_silver.analysis_coverage import build_analysis_coverage
from power_silver.analysis_job import SESSION_INPUT_SCHEMA
from power_silver.appliance_usage import (
    restore_latest_sessions,
    session_daily_slices,
    summarize_daily_slices,
)


pytestmark = pytest.mark.spark


def test_successful_retry_is_counted_once_by_input_slot(spark) -> None:
    start = datetime(2026, 9, 19, 0, 0, tzinfo=timezone.utc)
    power = spark.createDataFrame(
        [
            ("m1", "H001", date(2026, 9, 19), start),
            ("m2", "H001", date(2026, 9, 19), start + timedelta(seconds=1)),
            ("m3", "H001", date(2026, 9, 19), start + timedelta(seconds=2)),
        ],
        ["message_id", "household_id", "event_date", "measured_at_utc"],
    )
    receipts = spark.createDataFrame(
        [
            ("r1", "m1", "run-a", 1, "SKIPPED_WARMUP", [], start, "m1", "1", "e1", "[]"),
            ("r2", "m2", "run-a", 1, "FAILED_INFERENCE", [], start, "m1", "1", "e1", "[]"),
            ("r3", "m2", "run-a", 2, "SUCCEEDED", ["KETTLE"], start, "m1", "1", "e1", "[]"),
        ],
        [
            "receipt_id", "message_id", "analysis_run_id", "attempt", "outcome",
            "appliance_types", "processed_at", "model_version", "pipeline_version",
            "state_epoch", "session_change_refs_json",
        ],
    )

    row = build_analysis_coverage(
        power,
        receipts,
        appliance_types=["KETTLE"],
        input_snapshot_id="snapshot-1",
    ).collect()[0]

    assert row.observed_slot_count == 3
    assert row.analyzed_slot_count == 1
    assert row.warmup_slot_count == 1
    assert row.failed_slot_count == 0
    assert row.unknown_slot_count == 1
    assert row.max_unanalyzed_seconds == 1
    assert row.analysis_status == "INCOMPLETE"
    assert row.delivery_status == "COMPLETE"


def test_nearby_sessions_merge_before_ten_second_rule(spark) -> None:
    first = datetime(2026, 9, 18, 15, 0, tzinfo=timezone.utc)
    changes = spark.createDataFrame(
        [
            ("s1", "H001", "KETTLE", first, first + timedelta(seconds=6), False),
            (
                "s2",
                "H001",
                "KETTLE",
                first + timedelta(seconds=36),
                first + timedelta(seconds=42),
                False,
            ),
        ],
        ["session_id", "household_id", "appliance_type", "started_at", "ended_at", "is_deleted"],
    )
    slices = session_daily_slices(changes, date(2026, 9, 19))
    row = summarize_daily_slices(slices).collect()[0]

    assert row.logical_use_count == 1
    assert row.usage_start_count == 1
    assert row.usage_duration_us == 12_000_000
    assert row.rejected_short_use_count == 0


def test_latest_revision_and_delete_restore_the_past_session_state(spark) -> None:
    start = datetime(2026, 9, 18, 15, 0, tzinfo=timezone.utc)
    changes = spark.createDataFrame(
        [
            ("updated", 1, False, start, "a", "H001", "KETTLE", date(2026, 9, 19), start, start + timedelta(seconds=10), Decimal("0.9"), Decimal("0.5"), start, 1),
            ("updated", 2, False, start, "a", "H001", "KETTLE", date(2026, 9, 19), start, start + timedelta(seconds=30), Decimal("0.9"), Decimal("0.5"), start, 2),
            ("deleted", 1, False, start, "a", "H001", "KETTLE", date(2026, 9, 19), start, start + timedelta(seconds=20), Decimal("0.9"), Decimal("0.5"), start, 3),
            ("deleted", 2, True, start, "a", "H001", "KETTLE", date(2026, 9, 19), start, start + timedelta(seconds=20), Decimal("0.9"), Decimal("0.5"), start, 4),
        ],
        SESSION_INPUT_SCHEMA,
    )

    restored = restore_latest_sessions(changes).collect()

    assert [(row.session_id, row.session_version) for row in restored] == [
        ("updated", 2)
    ]
    # Spark hands timestamps back as naive UTC (session time zone is UTC).
    assert restored[0].ended_at.replace(tzinfo=timezone.utc) == start + timedelta(seconds=30)
