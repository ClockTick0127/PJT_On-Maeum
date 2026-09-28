from datetime import date, datetime, timezone
import pytest

from household_report.daily_trends import assessment_mode, build_daily_trends

pytestmark = pytest.mark.spark


def test_daily_grid_separates_partial_and_preserves_unknown(spark):
    targets = spark.createDataFrame([('h1',), ('h2',)], 'household_id string')
    usage = spark.createDataFrame([
        ('h1', date(2026, 9, 21), 'IRON', 'NOT_USED', True, 0, 0),
        ('h1', date(2026, 9, 22), 'IRON', 'UNKNOWN', False, None, None),
    ], 'household_id string, usage_date date, appliance_type string, usage_status string, baseline_eligible boolean, usage_start_count long, usage_duration_us long')
    assessments = spark.createDataFrame([
        ('h1', datetime(2026,9,21,14,59,tzinfo=timezone.utc), 'VALID', 10),
        ('h1', datetime(2026,9,21,15,0,tzinfo=timezone.utc), 'VALID', 20),
        ('h1', datetime(2026,9,21,16,0,tzinfo=timezone.utc), 'PARTIAL', 90),
    ], 'household_id string, assessed_at timestamp, assessment_status string, risk_score int')
    frames = build_daily_trends(usage, assessments, targets, start=date(2026,9,21), end=date(2026,9,23), mode='EVENT_TIME_REASSESSMENT')
    days = {(r.household_id,r.usage_date):r for r in frames['household_daily_trends'].collect()}
    assert len(days) == 6
    assert days['h1',date(2026,9,21)].max_valid_score == 10
    assert days['h1',date(2026,9,22)].max_valid_score == 20
    assert days['h1',date(2026,9,22)].max_partial_score == 90
    assert days['h1',date(2026,9,23)].max_valid_score is None
    assert days['h2',date(2026,9,22)].recorded_count == 0
    appliance = {r.usage_date:r for r in frames['appliance_daily_trends'].collect()}
    assert appliance[date(2026,9,21)].usage_minutes == 0
    assert appliance[date(2026,9,22)].usage_minutes is None
    assert appliance[date(2026,9,23)].usage_status == 'MISSING'
    assert appliance[date(2026,9,23)].usage_minutes is None
    assert 'EVENT_TIME_REASSESSMENT' in frames['report_summary'].first().statement


def test_origin_is_never_guessed_and_mixed_origins_rejected(spark):
    legacy = spark.createDataFrame([('h1',)], 'household_id string')
    assert assessment_mode(legacy, {}) == 'UNKNOWN_ORIGIN'
    frame = spark.createDataFrame([('LIVE_RECORDED',), ('EVENT_TIME_REASSESSMENT',)], 'assessment_mode string')
    with pytest.raises(ValueError, match='one assessment origin'):
        assessment_mode(frame, {})
    missing = spark.createDataFrame([('LIVE_RECORDED',), (None,)], 'assessment_mode string')
    with pytest.raises(ValueError, match='missing'):
        assessment_mode(missing, {})
