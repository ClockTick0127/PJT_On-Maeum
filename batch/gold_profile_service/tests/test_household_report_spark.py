from datetime import date, datetime, timezone

import pytest

from household_report.assessment_summary import build_assessment_report
from household_report.usage_summary import build_usage_report


pytestmark = pytest.mark.spark


def test_usage_preserves_no_data_and_unknown(spark):
    targets = spark.createDataFrame([("h1",), ("h2",)], ["household_id"])
    usage = spark.createDataFrame(
        [
            ("h1", date(2026, 9, 22), "KETTLE", "UNKNOWN", False, None, None),
        ],
        "household_id string, usage_date date, appliance_type string, "
        "usage_status string, baseline_eligible boolean, usage_start_count long, "
        "usage_duration_us long",
    )
    result = build_usage_report(
        usage, targets, start=date(2026, 9, 22), end=date(2026, 9, 22)
    )
    household = {row.household_id: row for row in result["household_summary"].collect()}
    assert household["h1"].usage_data_status == "INSUFFICIENT_HISTORY"
    assert household["h2"].usage_data_status == "NO_DATA"
    appliance = result["usage_summary"].first()
    assert appliance.unknown_days == 1
    assert appliance.use_probability is None


def test_assessment_uses_korean_business_day_and_keeps_empty_targets(spark):
    targets = spark.createDataFrame([("h1",), ("h2",)], ["household_id"])
    assessments = spark.createDataFrame(
        [
            ("a1", "h1", datetime(2026, 9, 21, 15, 0, tzinfo=timezone.utc), "VALID", 0.8),
            ("a2", "h1", datetime(2026, 9, 22, 15, 0, tzinfo=timezone.utc), "VALID", 0.9),
        ],
        "assessment_id string, household_id string, assessed_at timestamp, "
        "assessment_status string, risk_score double",
    )
    result = build_assessment_report(
        assessments, targets, start=date(2026, 9, 22), end=date(2026, 9, 22)
    )
    assert [row.assessment_id for row in result["assessment_detail"].collect()] == ["a1"]
    summary = {row.household_id: row for row in result["assessment_summary"].collect()}
    assert summary["h1"].recorded_assessment_count == 1
    assert summary["h2"].assessment_data_status == "NO_RECORDED_ASSESSMENT"
