"""Spark calculation of compatible overall and weekday routine baselines."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


WEEKDAY_NAMES = ("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN")


def nearest_rank_value(values, percentile: float):
    """Python reference used to lock compatibility with the legacy algorithm."""

    if not values:
        return None
    if not 0 < percentile <= 1:
        raise ValueError("percentile must be in (0, 1]")
    from math import ceil

    ordered = sorted(values)
    return ordered[ceil(percentile * len(ordered)) - 1]


def nearest_rank(sorted_values, percentile: float):
    """Exact legacy percentile: sorted ceil(p*N)-th value, without interpolation."""

    rank = F.greatest(F.lit(1), F.ceil(F.size(sorted_values) * F.lit(percentile)))
    return F.when(F.size(sorted_values) > 0, F.element_at(sorted_values, rank.cast("int")))


def validate_daily_usage(frame: DataFrame) -> None:
    keys = ["usage_date", "household_id", "appliance_type"]
    duplicate = frame.groupBy(*keys).count().filter(F.col("count") != 1).limit(1).collect()
    if duplicate:
        raise ValueError(f"duplicate daily usage key: {duplicate[0].asDict()}")
    invalid = frame.filter(
        F.col("usage_status").isNull()
        | F.col("baseline_eligible").isNull()
        | ((F.col("usage_status") == "USED") & ((F.col("is_used") != True) | F.col("first_use_second").isNull()))
        | ((F.col("usage_status") == "NOT_USED") & (F.col("is_used") != False))
        | ((F.col("usage_status") == "UNKNOWN") & F.col("baseline_eligible"))
        | (F.col("baseline_eligible") & ~F.col("usage_status").isin("USED", "NOT_USED"))
        | (F.col("baseline_eligible") & (F.col("analysis_status") != "COMPLETE"))
        | (F.col("baseline_eligible") & (F.col("delivery_status") != "COMPLETE"))
    ).limit(1).collect()
    if invalid:
        raise ValueError(f"invalid appliance_usage_daily row: {invalid[0].asDict()}")


def _statistics(frame: DataFrame, keys: list[str]) -> DataFrame:
    return frame.groupBy(*keys).agg(
        F.sum(F.when(F.col("baseline_eligible"), 1).otherwise(0)).cast("long").alias("sample_days"),
        F.sum(F.when(F.col("baseline_eligible") & (F.col("usage_status") == "USED"), 1).otherwise(0)).cast("long").alias("active_days"),
        F.sort_array(
            F.collect_list(
                F.when(
                    F.col("baseline_eligible") & (F.col("usage_status") == "USED"),
                    F.col("first_use_second"),
                )
            )
        ).alias("_active_times"),
    )


def build_routine_baselines(
    daily_usage: DataFrame,
    *,
    as_of_date,
    window_start_date,
    input_snapshot_id: str,
    profile_version: str,
    rule_version: str,
    effective_from,
    input_incomplete: bool,
    minimum_sample_days: int = 14,
    minimum_weekday_sample_days: int = 4,
    minimum_daily_use_probability: float = 0.70,
) -> DataFrame:
    validate_daily_usage(daily_usage)
    base_keys = ["household_id", "appliance_type"]
    overall = _statistics(daily_usage, base_keys).withColumn("baseline_scope", F.lit("OVERALL")).withColumn(
        "weekday", F.lit(None).cast("string")
    )
    weekdays = (
        daily_usage.withColumn("_weekday_index", F.pmod(F.dayofweek("usage_date") + F.lit(5), F.lit(7)))
        .withColumn("weekday", F.element_at(F.array(*[F.lit(item) for item in WEEKDAY_NAMES]), F.col("_weekday_index") + 1))
    )
    # Emit every weekday for every known household/appliance, including zero-sample rows.
    weekday_grid = daily_usage.select(*base_keys).distinct().crossJoin(
        daily_usage.sparkSession.createDataFrame([(item,) for item in WEEKDAY_NAMES], ["weekday"])
    )
    weekday_stats = _statistics(weekdays, [*base_keys, "weekday"])
    weekday_rows = (
        weekday_grid.join(weekday_stats, [*base_keys, "weekday"], "left")
        .fillna({"sample_days": 0, "active_days": 0})
        .withColumn("_active_times", F.coalesce(F.col("_active_times"), F.expr("array()").cast("array<int>")))
        .withColumn("baseline_scope", F.lit("WEEKDAY"))
    )
    combined = overall.unionByName(weekday_rows, allowMissingColumns=True)
    minimum = F.when(F.col("baseline_scope") == "OVERALL", F.lit(minimum_sample_days)).otherwise(
        F.lit(minimum_weekday_sample_days)
    )
    probability = F.when(
        F.col("sample_days") > 0,
        F.bround(F.col("active_days") / F.col("sample_days"), 4),
    ).otherwise(F.lit(0.0))
    expected = nearest_rank(F.col("_active_times"), 0.90)
    return (
        combined.withColumn("daily_use_probability", probability.cast("decimal(5,4)"))
        # Reliability is comparable across scopes: always normalize against the
        # 14-day overall confidence horizon, not the weekday readiness minimum.
        .withColumn(
            "reliability_weight",
            F.bround(
                F.least(
                    F.lit(1.0), F.col("sample_days") / F.lit(minimum_sample_days)
                ),
                4,
            ).cast("decimal(5,4)"),
        )
        .withColumn("first_use_time_p50_second", nearest_rank(F.col("_active_times"), 0.50).cast("int"))
        .withColumn("expected_until_second", expected.cast("int"))
        .withColumn("preferred_window_start_second", nearest_rank(F.col("_active_times"), 0.10).cast("int"))
        .withColumn("preferred_window_end_second", expected.cast("int"))
        .withColumn(
            "quality_status",
            F.when(F.lit(input_incomplete), "INPUT_INCOMPLETE")
            .when(F.col("sample_days") < minimum, "INSUFFICIENT_HISTORY")
            .when(F.col("active_days") == 0, "NO_USAGE_HISTORY")
            .otherwise("READY"),
        )
        .withColumn(
            "enabled",
            (F.col("quality_status") == "READY")
            & F.col("expected_until_second").isNotNull()
            & (F.col("daily_use_probability") >= F.lit(minimum_daily_use_probability)),
        )
        .withColumn("as_of_date", F.lit(as_of_date))
        .withColumn("window_start_date", F.lit(window_start_date))
        .withColumn("window_end_date", F.lit(as_of_date))
        .withColumn("effective_from", F.lit(effective_from))
        .withColumn("published_at", F.lit(effective_from))
        .withColumn("profile_version", F.lit(profile_version))
        .withColumn("rule_version", F.lit(rule_version))
        .withColumn("input_snapshot_id", F.lit(input_snapshot_id))
        .drop("_active_times")
    )
