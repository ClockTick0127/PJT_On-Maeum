from datetime import datetime, time, timedelta, timezone

from pyspark.sql import functions as F


KST = timezone(timedelta(hours=9))


def build_assessment_report(assessments, targets, *, start, end):
    start_at = datetime.combine(start, time.min, KST).astimezone(timezone.utc)
    end_at = datetime.combine(end + timedelta(days=1), time.min, KST).astimezone(
        timezone.utc
    )
    detail = assessments.filter(
        (F.col("assessed_at") >= F.lit(start_at))
        & (F.col("assessed_at") < F.lit(end_at))
    ).join(targets.select("household_id"), "household_id", "inner")
    summary = detail.groupBy("household_id").agg(
        F.count("*").alias("recorded_assessment_count"),
        F.sum(F.when(F.col("assessment_status") == "VALID", 1).otherwise(0)).alias(
            "valid_record_count"
        ),
        F.sum(F.when(F.col("assessment_status") == "PARTIAL", 1).otherwise(0)).alias(
            "partial_record_count"
        ),
        F.sum(
            F.when(
                F.col("assessment_status").isin("LEARNING", "INSUFFICIENT_DATA"), 1
            ).otherwise(0)
        ).alias("unavailable_record_count"),
        F.max(
            F.when(F.col("assessment_status") == "VALID", F.col("risk_score"))
        ).alias("max_recorded_valid_score"),
        F.max(
            F.when(F.col("assessment_status") == "PARTIAL", F.col("risk_score"))
        ).alias("max_recorded_partial_score"),
        F.min("assessed_at").alias("first_recorded_at"),
        F.max("assessed_at").alias("last_recorded_at"),
    )
    summary = (
        targets.select("household_id")
        .join(summary, "household_id", "left")
        .fillna(
            {
                "recorded_assessment_count": 0,
                "valid_record_count": 0,
                "partial_record_count": 0,
                "unavailable_record_count": 0,
            }
        )
        .withColumn(
            "assessment_data_status",
            F.when(
                F.col("recorded_assessment_count") == 0, "NO_RECORDED_ASSESSMENT"
            ).otherwise("RECORDED"),
        )
    )
    return {"assessment_detail": detail, "assessment_summary": summary}
