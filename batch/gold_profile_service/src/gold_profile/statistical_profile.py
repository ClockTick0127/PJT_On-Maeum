"""Metric-row Gold statistical profiles with exact nearest-rank percentiles and MAD."""

from __future__ import annotations

from datetime import timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

from gold_profile.routine_baseline import nearest_rank


COMMON_COLUMNS = [
    "household_id", "profile_version", "metric_name", "appliance_type",
    "weekday_group", "time_bucket", "sample_count", "eligible_day_count",
    "p50", "p90", "mad", "unit", "quality_status", "window_start_date",
    "window_end_date", "as_of_date", "effective_from", "published_at",
    "rule_version", "input_snapshot_id",
]


def _distribution(values: DataFrame, keys: list[str], value_col: str) -> DataFrame:
    grouped = values.groupBy(*keys).agg(
        F.sort_array(F.collect_list(F.col(value_col).cast("double"))).alias("_values"),
        F.count(F.col(value_col)).cast("long").alias("sample_count"),
        F.countDistinct("usage_date").cast("long").alias("eligible_day_count"),
    ).withColumn("p50", nearest_rank(F.col("_values"), 0.50).cast("double"))
    return (
        grouped.withColumn(
            "_deviations",
            F.expr("sort_array(transform(_values, x -> abs(x - p50)))"),
        )
        .withColumn("p90", nearest_rank(F.col("_values"), 0.90).cast("double"))
        .withColumn("mad", nearest_rank(F.col("_deviations"), 0.50).cast("double"))
        .drop("_values", "_deviations")
    )


def _decorate(
    frame: DataFrame,
    *,
    profile_version: str,
    rule_version: str,
    input_snapshot_id: str,
    window_start_date,
    window_end_date,
    effective_from,
    input_incomplete: bool,
) -> DataFrame:
    return (
        frame.withColumn("profile_version", F.lit(profile_version))
        .withColumn("weekday_group", F.coalesce(F.col("weekday_group"), F.lit("ALL")))
        .withColumn(
            "quality_status",
            F.when(F.lit(input_incomplete), "INPUT_INCOMPLETE")
            .when(F.col("sample_count") == 0, "INSUFFICIENT_HISTORY")
            .otherwise(F.coalesce(F.col("quality_status"), F.lit("READY"))),
        )
        .withColumn("window_start_date", F.lit(window_start_date))
        .withColumn("window_end_date", F.lit(window_end_date))
        .withColumn("as_of_date", F.lit(window_end_date))
        .withColumn("effective_from", F.lit(effective_from))
        .withColumn("published_at", F.lit(effective_from))
        .withColumn("rule_version", F.lit(rule_version))
        .withColumn("input_snapshot_id", F.lit(input_snapshot_id))
        .select(*COMMON_COLUMNS)
    )


def build_statistical_profiles(
    daily_usage: DataFrame,
    logical_uses: DataFrame,
    *,
    as_of_date,
    window_start_date,
    profile_version: str,
    rule_version: str,
    input_snapshot_id: str,
    input_incomplete: bool,
    effective_from,
    bucket_minutes: int,
    business_utc_offset_seconds: int,
) -> DataFrame:
    bucket_seconds = bucket_minutes * 60
    bucket_count = 86_400 // bucket_seconds
    eligible_days = (
        daily_usage.groupBy("household_id", "usage_date")
        .agg(F.min(F.col("baseline_eligible").cast("int")).alias("eligible"))
        .filter(F.col("eligible") == 1)
        .select("household_id", "usage_date")
    )
    buckets = daily_usage.sparkSession.range(bucket_count).select(
        F.col("id").cast("int").alias("bucket_index")
    )
    bucket_end_minutes = (F.col("bucket_index") + 1) * F.lit(bucket_minutes)
    day_buckets = (
        eligible_days.crossJoin(F.broadcast(buckets))
        .withColumn(
            "time_bucket",
            F.format_string(
                "%02d:%02d",
                F.floor(bucket_end_minutes / F.lit(60)).cast("int"),
                F.pmod(bucket_end_minutes, F.lit(60)).cast("int"),
            ),
        )
        .withColumn(
            "evaluation_epoch",
            F.unix_timestamp(F.col("usage_date").cast("timestamp"))
            - F.lit(business_utc_offset_seconds)
            + (F.col("bucket_index") + 1) * F.lit(bucket_seconds),
        )
    )

    valid_uses = logical_uses.filter(F.col("quality_status") == "VALID").withColumn(
        "usage_date",
        F.to_date(
            F.from_unixtime(
                F.col("started_at").cast("long") + F.lit(business_utc_offset_seconds)
            )
        ),
    ).withColumn(
        "start_bucket",
        F.floor(
            F.pmod(F.col("started_at").cast("long") + F.lit(business_utc_offset_seconds), F.lit(86_400))
            / F.lit(bucket_seconds)
        ).cast("int"),
    )
    eligible_episode_days = daily_usage.filter(F.col("baseline_eligible")).select(
        "household_id", "appliance_type", "usage_date"
    ).distinct()
    valid_uses = valid_uses.join(
        eligible_episode_days,
        ["household_id", "appliance_type", "usage_date"],
        "inner",
    )
    starts = valid_uses.groupBy("household_id", "usage_date", "start_bucket").agg(
        F.count(F.lit(1)).alias("starts")
    )
    cumulative_window = Window.partitionBy("household_id", "usage_date").orderBy("bucket_index").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    cumulative_values = (
        day_buckets.join(
            starts,
            (day_buckets.household_id == starts.household_id)
            & (day_buckets.usage_date == starts.usage_date)
            & (day_buckets.bucket_index == starts.start_bucket),
            "left",
        )
        .select(
            day_buckets.household_id,
            day_buckets.usage_date,
            day_buckets.bucket_index,
            day_buckets.time_bucket,
            day_buckets.evaluation_epoch,
            F.coalesce(starts.starts, F.lit(0)).alias("starts"),
        )
        .withColumn("metric_value", F.sum("starts").over(cumulative_window).cast("double"))
    )
    cumulative = (
        _distribution(cumulative_values, ["household_id", "time_bucket"], "metric_value")
        .withColumn("metric_name", F.lit("CUMULATIVE_ACTIVITY_START_COUNT"))
        .withColumn("appliance_type", F.lit(None).cast("string"))
        .withColumn("weekday_group", F.lit("ALL"))
        .withColumn("unit", F.lit("count"))
        .withColumn("quality_status", F.lit("READY"))
    )

    # At each bucket end: active => zero; otherwise elapsed seconds since latest end.
    episode_times = valid_uses.select(
        "household_id",
        F.col("started_at").cast("long").alias("episode_start"),
        F.col("ended_at").cast("long").alias("episode_end"),
    )
    inactivity_values = (
        day_buckets.alias("b")
        .join(
            episode_times.alias("e"),
            (F.col("b.household_id") == F.col("e.household_id"))
            & (F.col("e.episode_start") < F.col("b.evaluation_epoch")),
            "left",
        )
        .groupBy("b.household_id", "b.usage_date", "b.time_bucket", "b.evaluation_epoch")
        .agg(
            F.max(F.when(F.col("e.episode_end") > F.col("b.evaluation_epoch"), 1).otherwise(0)).alias("is_active"),
            F.max(F.when(F.col("e.episode_end") <= F.col("b.evaluation_epoch"), F.col("e.episode_end"))).alias("last_end"),
        )
        .withColumn(
            "metric_value",
            F.when(F.col("is_active") == 1, F.lit(0.0))
            .when(F.col("last_end").isNotNull(), (F.col("evaluation_epoch") - F.col("last_end")).cast("double")),
        )
        .filter(F.col("metric_value").isNotNull())
    )
    inactivity = (
        _distribution(inactivity_values, ["household_id", "time_bucket"], "metric_value")
        .withColumn("metric_name", F.lit("INACTIVITY_ELAPSED"))
        .withColumn("appliance_type", F.lit(None).cast("string"))
        .withColumn("weekday_group", F.lit("ALL"))
        .withColumn("unit", F.lit("seconds"))
        .withColumn("quality_status", F.lit("READY"))
    )

    duration_values = valid_uses.select(
        "household_id", "appliance_type", "usage_date",
        (F.col("active_duration_us") / F.lit(1_000_000)).alias("metric_value"),
    )
    duration_distribution = _distribution(
        duration_values, ["household_id", "appliance_type"], "metric_value"
    )
    appliance_grid = daily_usage.select("household_id", "appliance_type").distinct()
    duration = (
        appliance_grid.join(
            duration_distribution, ["household_id", "appliance_type"], "left"
        )
        .fillna({"sample_count": 0, "eligible_day_count": 0})
        .withColumn("metric_name", F.lit("LOGICAL_USE_ACTIVE_DURATION"))
        .withColumn("weekday_group", F.lit("ALL"))
        .withColumn("time_bucket", F.lit(None).cast("string"))
        .withColumn("unit", F.lit("seconds"))
        .withColumn(
            "quality_status",
            F.when(F.col("sample_count") == 0, "INSUFFICIENT_HISTORY").otherwise("READY"),
        )
    )

    eligible = daily_usage.filter(F.col("baseline_eligible")).withColumn(
        "metric_value", F.coalesce(F.col("usage_start_count"), F.lit(0)).cast("double")
    )
    recent_start = as_of_date - timedelta(days=6)
    change = eligible.groupBy("household_id", "appliance_type").agg(
        F.avg(F.when(F.col("usage_date") >= F.lit(recent_start), F.col("metric_value"))).alias("recent"),
        F.avg(F.when(F.col("usage_date") < F.lit(recent_start), F.col("metric_value"))).alias("previous"),
        F.count(F.lit(1)).cast("long").alias("sample_count"),
        F.countDistinct("usage_date").cast("long").alias("eligible_day_count"),
    ).withColumn("p50", F.col("recent") - F.col("previous"))
    change = (
        change.withColumn("p90", F.lit(None).cast("double"))
        .withColumn("mad", F.lit(None).cast("double"))
        .withColumn("metric_name", F.lit("RECENT_ACTIVITY_COUNT_DELTA"))
        .withColumn("weekday_group", F.lit("ALL"))
        .withColumn("time_bucket", F.lit(None).cast("string"))
        .withColumn("unit", F.lit("count_per_day"))
        .withColumn("quality_status", F.when(F.col("previous").isNull(), "INSUFFICIENT_HISTORY").otherwise("READY"))
    )

    combined = cumulative.unionByName(inactivity, allowMissingColumns=True).unionByName(
        duration, allowMissingColumns=True
    ).unionByName(change, allowMissingColumns=True)
    return _decorate(
        combined,
        profile_version=profile_version,
        rule_version=rule_version,
        input_snapshot_id=input_snapshot_id,
        window_start_date=window_start_date,
        window_end_date=as_of_date,
        effective_from=effective_from,
        input_incomplete=input_incomplete,
    )
