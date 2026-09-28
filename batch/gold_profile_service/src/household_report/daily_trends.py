"""Calendar-complete report projections. Missing measurements are never zero-filled."""
from pyspark.sql import functions as F


MODES = {"LIVE_RECORDED", "EVENT_TIME_REASSESSMENT", "UNKNOWN_ORIGIN"}


def assessment_mode(frame, document):
    declared = document.get("assessment_mode")
    modes = {r[0] for r in frame.select("assessment_mode").distinct().collect()} if "assessment_mode" in frame.columns else set()
    modes.discard(None)
    if declared:
        modes.add(declared)
    if not modes:
        return "UNKNOWN_ORIGIN"
    if len(modes) != 1 or not modes.issubset(MODES):
        raise ValueError("one assessment origin is required per report")
    mode = next(iter(modes))
    if "assessment_mode" in frame.columns and frame.filter(F.col("assessment_mode").isNull()).limit(1).count():
        raise ValueError("assessment origin must not mix known and missing values")
    return mode


def build_daily_trends(usage, assessments, targets, *, start, end, mode):
    days = targets.sparkSession.range(1).select(F.explode(F.sequence(F.lit(start), F.lit(end))).alias("usage_date"))
    grid = targets.select("household_id").crossJoin(days)
    # The session timezone is explicitly UTC: convert event timestamps to KST once.
    daily = assessments.withColumn("usage_date", F.to_date(F.from_utc_timestamp("assessed_at", "Asia/Seoul")))
    scores = daily.groupBy("household_id", "usage_date").agg(
        F.count("*").alias("recorded_count"),
        F.sum(F.when(F.col("assessment_status") == "VALID", 1).otherwise(0)).alias("valid_count"),
        F.sum(F.when(F.col("assessment_status") == "PARTIAL", 1).otherwise(0)).alias("partial_count"),
        F.max(F.when(F.col("assessment_status") == "VALID", F.col("risk_score"))).alias("max_valid_score"),
        F.max(F.when(F.col("assessment_status") == "PARTIAL", F.col("risk_score"))).alias("max_partial_score"),
    )
    household = (grid.join(scores, ["household_id", "usage_date"], "left")
        .fillna(0, subset=["recorded_count", "valid_count", "partial_count"])
        .withColumn("assessment_mode", F.lit(mode))
        .withColumn("assessment_data_status", F.when(F.col("recorded_count") == 0, "NO_RECORDED_ASSESSMENT").otherwise("RECORDED")))
    appliances = usage.select("household_id", "appliance_type").distinct().crossJoin(days)
    eligible = F.col("baseline_eligible") & F.col("usage_status").isin("USED", "NOT_USED")
    appliance = (appliances.join(usage, ["household_id", "appliance_type", "usage_date"], "left")
        .withColumn("usage_minutes", F.when(eligible, F.col("usage_duration_us") / 60000000.0))
        .withColumn("usage_count", F.when(eligible, F.col("usage_start_count")))
        .withColumn("usage_status", F.coalesce("usage_status", F.lit("MISSING"))))
    summary = household.filter(F.col("usage_date") == F.lit(end)).withColumn(
        "statement", F.concat(
            F.lit("평가 출처: "), F.col("assessment_mode"), F.lit(". 당일 저장 평가 "),
            F.col("recorded_count").cast("string"), F.lit("건, VALID "), F.col("valid_count").cast("string"),
            F.lit("건, PARTIAL "), F.col("partial_count").cast("string"), F.lit("건. VALID 최대 점수: "),
            F.coalesce(F.col("max_valid_score").cast("string"), F.lit("산출 불가")),
            F.lit(". 평가 건수는 위험 지속시간이 아닙니다. 장시간 사용 위험 이벤트는 별도 미연결 항목입니다.")))
    return {"household_daily_trends": household, "appliance_daily_trends": appliance, "report_summary": summary}
