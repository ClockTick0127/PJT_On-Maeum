from pyspark.sql import functions as F


def build_usage_report(usage, targets, *, start, end):
    daily = usage.filter(F.col("usage_date").between(F.lit(start), F.lit(end))).join(
        targets.select("household_id"), "household_id", "inner"
    )
    eligible = F.col("baseline_eligible") & F.col("usage_status").isin(
        "USED", "NOT_USED"
    )
    appliance = (
        daily.groupBy("household_id", "appliance_type")
        .agg(
            F.count("*").alias("available_days"),
            F.sum(F.when(eligible, 1).otherwise(0)).alias("eligible_days"),
            F.sum(
                F.when(eligible & (F.col("usage_status") == "USED"), 1).otherwise(0)
            ).alias("active_days"),
            F.sum(F.when(F.col("usage_status") == "UNKNOWN", 1).otherwise(0)).alias(
                "unknown_days"
            ),
            F.sum(F.when(eligible, F.col("usage_start_count"))).alias(
                "usage_start_count"
            ),
            F.sum(F.when(eligible, F.col("usage_duration_us"))).alias(
                "usage_duration_us"
            ),
        )
        .withColumn("expected_days", F.lit((end - start).days + 1))
        .withColumn("missing_days", F.col("expected_days") - F.col("available_days"))
        .withColumn(
            "use_probability",
            F.when(F.col("eligible_days") > 0, F.col("active_days") / F.col("eligible_days")),
        )
        .withColumn(
            "quality_status",
            F.when(F.col("eligible_days") == 0, "INSUFFICIENT_HISTORY")
            .when(F.col("eligible_days") < F.col("expected_days"), "PARTIAL")
            .otherwise("READY"),
        )
    )
    counts = daily.groupBy("household_id").agg(
        F.count("*").alias("usage_row_count"),
        F.sum(F.when(eligible, 1).otherwise(0)).alias("eligible_appliance_day_count"),
    )
    summary = (
        targets.select("household_id")
        .join(counts, "household_id", "left")
        .fillna({"usage_row_count": 0, "eligible_appliance_day_count": 0})
        .withColumn(
            "usage_data_status",
            F.when(F.col("usage_row_count") == 0, "NO_DATA")
            .when(F.col("eligible_appliance_day_count") == 0, "INSUFFICIENT_HISTORY")
            .otherwise("AVAILABLE"),
        )
    )
    return {
        "usage_daily": daily,
        "usage_summary": appliance,
        "household_summary": summary,
    }
