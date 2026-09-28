from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from gold_profile.logical_uses import build_logical_uses
from gold_profile.session_snapshot import (
    inspect_session_snapshot,
    require_matching_session_outputs,
    require_one_session_snapshot,
)


pytestmark = pytest.mark.spark


def test_only_latest_session_revision_is_used_across_dates(spark):
    start = datetime(2026, 9, 18, 15, 0, tzinfo=timezone.utc)
    slices = spark.createDataFrame([
        ("s1", 2, "H001", "KETTLE", start, start + timedelta(seconds=20), False),
        ("s1", 3, "H001", "KETTLE", start, start + timedelta(seconds=40), False),
    ], "session_id string, session_version int, household_id string, appliance_type string, "
       "original_started_at timestamp, original_ended_at timestamp, end_imputed boolean")

    rows = build_logical_uses(
        slices, run_id="run", rule_version="rule",
        business_utc_offset_seconds=9 * 3600,
    ).collect()

    assert len(rows) == 1
    assert rows[0].active_duration_us == 40_000_000
    assert [(item.session_id, item.session_version) for item in rows[0].source_session_refs] == [("s1", 3)]
    assert rows[0].quality_status == "VALID"


def test_partial_date_reprocessing_is_rejected_for_delete_or_date_move():
    days = (date(2026, 9, 18), date(2026, 9, 19))
    refs = [
        SimpleNamespace(config_version="receipts=a;sessions=before-delete"),
        SimpleNamespace(config_version="receipts=b;sessions=after-delete"),
    ]
    with pytest.raises(ValueError, match="different session snapshots"):
        require_one_session_snapshot(refs, days)


def test_all_dates_reprocessed_after_delete_or_move_are_accepted():
    days = (date(2026, 9, 18), date(2026, 9, 19))
    refs = [
        SimpleNamespace(config_version="receipts=a;sessions=after-change"),
        SimpleNamespace(config_version="receipts=b;sessions=after-change"),
    ]
    assert require_one_session_snapshot(refs, days) == "after-change"


def test_usage_and_zero_row_slice_versions_must_share_the_selected_input():
    days = (date(2026, 9, 18), date(2026, 9, 19))
    runs = [uuid4(), uuid4()]
    usage = [
        SimpleNamespace(
            target_date=day, run_id=run,
            config_version="receipts=r;sessions=selected",
        )
        for day, run in zip(days, runs)
    ]
    # A version reference exists even when its parquet contains zero rows.
    slices = [
        SimpleNamespace(
            target_date=day, run_id=run,
            config_version="receipts=r;sessions=selected",
        )
        for day, run in zip(days, runs)
    ]

    assert require_matching_session_outputs(
        usage, slices, days, expected_token="selected"
    ) == "selected"


def test_missing_and_provenance_failures_are_reported_separately():
    days = (date(2026, 9, 18), date(2026, 9, 19))
    run = uuid4()
    usage = [
        SimpleNamespace(target_date=days[0], run_id=run, config_version="legacy")
    ]
    slices = [
        SimpleNamespace(target_date=days[0], run_id=run, config_version="legacy")
    ]

    check = inspect_session_snapshot(
        usage, slices, days, expected_token="selected"
    )

    assert check.aligned is False
    assert check.missing_dates == (days[1],)
    assert check.provenance_missing_dates == (days[0],)
