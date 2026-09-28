from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from gold_profile.routine_baseline import build_routine_baselines
from gold_profile.statistical_profile import _distribution, build_statistical_profiles


@pytest.mark.spark
def test_used_not_used_and_unknown_contract(spark):
    start = date(2026, 8, 23)
    rows = []
    for index in range(28):
        if index < 16:
            status, used, eligible, first, analysis = "USED", True, True, (index + 1) * 100, "COMPLETE"
        elif index < 20:
            status, used, eligible, first, analysis = "NOT_USED", False, True, None, "COMPLETE"
        else:
            status, used, eligible, first, analysis = "UNKNOWN", None, False, None, "INCOMPLETE"
        rows.append(
            (start + timedelta(days=index), "H001", "KETTLE", status, used,
             eligible, first, 1 if used else 0 if eligible else None, analysis,
             "COMPLETE" if eligible else "PENDING")
        )
    daily = spark.createDataFrame(
        rows,
        "usage_date date, household_id string, appliance_type string, usage_status string, "
        "is_used boolean, baseline_eligible boolean, first_use_second int, "
        "usage_start_count long, analysis_status string, delivery_status string",
    )
    result = build_routine_baselines(
        daily, as_of_date=date(2026, 9, 19), window_start_date=start,
        input_snapshot_id="snapshot", profile_version="profile", rule_version="rule",
        effective_from=datetime(2026, 9, 20, 0, 30, tzinfo=timezone.utc),
        input_incomplete=False,
    )
    overall = result.filter("baseline_scope = 'OVERALL'").collect()[0]
    assert overall.sample_days == 20
    assert overall.active_days == 16
    assert overall.daily_use_probability == Decimal("0.8000")
    assert overall.preferred_window_start_second == 200
    assert overall.first_use_time_p50_second == 800
    assert overall.expected_until_second == 1500
    assert overall.quality_status == "READY"
    assert overall.enabled is True


@pytest.mark.spark
def test_sufficient_no_usage_history_is_retained_but_disabled(spark):
    start = date(2026, 9, 6)
    rows = [
        (start + timedelta(days=index), "H001", "IRON", "NOT_USED", False, True,
         None, 0, "COMPLETE", "COMPLETE")
        for index in range(14)
    ]
    daily = spark.createDataFrame(
        rows,
        "usage_date date, household_id string, appliance_type string, usage_status string, "
        "is_used boolean, baseline_eligible boolean, first_use_second int, "
        "usage_start_count long, analysis_status string, delivery_status string",
    )
    overall = build_routine_baselines(
        daily, as_of_date=date(2026, 9, 19), window_start_date=start,
        input_snapshot_id="snapshot", profile_version="profile", rule_version="rule",
        effective_from=datetime.now(timezone.utc), input_incomplete=False,
    ).filter("baseline_scope = 'OVERALL'").collect()[0]
    assert overall.quality_status == "NO_USAGE_HISTORY"
    assert overall.expected_until_second is None
    assert overall.enabled is False


@pytest.mark.spark
def test_constant_distribution_has_zero_mad(spark):
    values = spark.createDataFrame(
        [("H001", date(2026, 9, day), 5.0) for day in range(1, 5)],
        "household_id string, usage_date date, metric_value double",
    )
    row = _distribution(values, ["household_id"], "metric_value").collect()[0]
    assert row.p50 == 5.0
    assert row.p90 == 5.0
    assert row.mad == 0.0


@pytest.mark.spark
def test_statistical_metric_rows_share_one_profile_version(spark):
    daily = spark.createDataFrame(
        [
            (date(2026, 9, 18), "H001", "KETTLE", True, 1),
            (date(2026, 9, 19), "H001", "KETTLE", True, 0),
        ],
        "usage_date date, household_id string, appliance_type string, "
        "baseline_eligible boolean, usage_start_count long",
    )
    logical = spark.createDataFrame(
        [(
            "H001", "KETTLE", "VALID",
            datetime(2026, 9, 17, 15, 15, tzinfo=timezone.utc),
            datetime(2026, 9, 17, 15, 16, tzinfo=timezone.utc),
            60_000_000,
        )],
        "household_id string, appliance_type string, quality_status string, "
        "started_at timestamp, ended_at timestamp, active_duration_us long",
    )
    rows = build_statistical_profiles(
        daily, logical, as_of_date=date(2026, 9, 19),
        window_start_date=date(2026, 9, 18), profile_version="profile-1",
        rule_version="statistics-v1", input_snapshot_id="snapshot-1",
        input_incomplete=False,
        effective_from=datetime(2026, 9, 20, 0, 30, tzinfo=timezone.utc),
        bucket_minutes=30, business_utc_offset_seconds=9 * 3600,
    ).collect()
    assert {row.profile_version for row in rows} == {"profile-1"}
    duration = next(row for row in rows if row.metric_name == "LOGICAL_USE_ACTIVE_DURATION")
    assert (duration.p50, duration.p90, duration.mad) == (60.0, 60.0, 0.0)
    cumulative = next(
        row for row in rows
        if row.metric_name == "CUMULATIVE_ACTIVITY_START_COUNT" and row.time_bucket == "00:30"
    )
    assert (cumulative.p50, cumulative.p90, cumulative.mad) == (0.0, 1.0, 0.0)
