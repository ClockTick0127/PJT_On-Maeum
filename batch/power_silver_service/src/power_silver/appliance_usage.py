"""Restore Bronze sessions and build quality-aware daily appliance usage."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window


MERGE_GAP_SECONDS = {
    "KETTLE": 60,
    "MICROWAVE": 60,
    "HAIR_DRYER": 60,
    "VACUUM_CLEANER": 60,
    "INDUCTION": 120,
    "IRON": 300,
}
MINIMUM_USE_US = 10_000_000


def restore_latest_sessions(changes: DataFrame) -> DataFrame:
    """Restore current session state and reject conflicting duplicate versions."""

    content = [
        "is_deleted", "activity_daily_id", "household_id", "appliance_type",
        "observation_date", "started_at", "ended_at", "max_probability",
        "decision_threshold", "updated_at",
    ]
    conflicts = (
        changes.groupBy("session_id", "session_version")
        .agg(F.countDistinct(F.to_json(F.struct(*content))).alias("variants"))
        .filter(F.col("variants") > 1)
        .limit(1)
        .collect()
    )
    if conflicts:
        row = conflicts[0]
        raise ValueError(
            "conflicting session version: "
            f"{row['session_id']} v{row['session_version']}"
        )
    deduplicated = changes.dropDuplicates(["session_id", "session_version", *content])
    latest = Window.partitionBy("session_id").orderBy(
        F.col("session_version").desc(), F.col("changed_at").desc()
    )
    return (
        deduplicated.withColumn("_latest", F.row_number().over(latest))
        .filter((F.col("_latest") == 1) & ~F.col("is_deleted"))
        .drop("_latest")
    )


def session_daily_slices(
    current_sessions: DataFrame,
    target_date: date,
    *,
    timezone_name: str = "Asia/Seoul",
) -> DataFrame:
    zone = ZoneInfo(timezone_name)
    day_start = datetime.combine(target_date, time.min, zone).astimezone(timezone.utc)
    day_end = (datetime.combine(target_date, time.min, zone) + timedelta(days=1)).astimezone(
        timezone.utc
    )
    overlapping = current_sessions.filter(
        (F.col("started_at") < F.lit(day_end))
        & (F.col("ended_at").isNull() | (F.col("ended_at") > F.lit(day_start)))
    )
    return (
        overlapping.withColumn("usage_date", F.lit(target_date))
        .withColumn("original_started_at", F.col("started_at"))
        .withColumn("original_ended_at", F.col("ended_at"))
        .withColumn("clipped_started_at", F.greatest("started_at", F.lit(day_start)))
        .withColumn("clipped_ended_at", F.least(F.coalesce("ended_at", F.lit(day_end)), F.lit(day_end)))
        .withColumn("started_in_day", (F.col("started_at") >= F.lit(day_start)) & (F.col("started_at") < F.lit(day_end)))
        .withColumn("end_imputed", F.col("ended_at").isNull())
        .withColumn("start_us", F.unix_micros("clipped_started_at"))
        .withColumn("end_us", F.unix_micros("clipped_ended_at"))
        .withColumn("duration_us", F.col("end_us") - F.col("start_us"))
        .filter(F.col("duration_us") > 0)
    )


def summarize_daily_slices(slices: DataFrame) -> DataFrame:
    """Merge nearby closed slices and retain diagnostics that can block certainty."""

    keys = ["usage_date", "household_id", "appliance_type"]
    diagnostics = slices.groupBy(*keys).agg(
        F.count(F.lit(1)).alias("source_session_count"),
        F.sum(F.when(F.col("end_imputed"), 1).otherwise(0)).alias("open_session_count"),
    )
    closed = slices.filter(~F.col("end_imputed"))
    ordering = Window.partitionBy(*keys).orderBy("start_us", "end_us", "session_id")
    previous = ordering.rowsBetween(Window.unboundedPreceding, -1)
    prepared = (
        closed.withColumn("previous_max_end", F.max("end_us").over(previous))
        .withColumn(
            "overlaps_previous",
            F.col("previous_max_end").isNotNull() & (F.col("start_us") < F.col("previous_max_end")),
        )
        .withColumn(
            "merge_gap_us",
            F.create_map(
                *sum(
                    ([F.lit(name), F.lit(seconds * 1_000_000)] for name, seconds in MERGE_GAP_SECONDS.items()),
                    [],
                )
            )[F.col("appliance_type")],
        )
        .fillna({"merge_gap_us": 60_000_000})
        .withColumn(
            "starts_group",
            F.when(
                F.col("previous_max_end").isNull()
                | ((F.col("start_us") - F.col("previous_max_end")) > F.col("merge_gap_us")),
                1,
            ).otherwise(0),
        )
        .withColumn("logical_group", F.sum("starts_group").over(ordering))
    )
    groups = prepared.groupBy(*keys, "logical_group").agg(
        F.min("start_us").alias("first_us"),
        F.max("end_us").alias("last_us"),
        F.sum("duration_us").alias("active_us"),
        F.max(F.col("overlaps_previous").cast("int")).alias("has_overlap"),
        # A group carried over from the previous day is not a new daily start,
        # even when later fragments of the same logical use start today.
        F.min(F.col("started_in_day").cast("int")).alias("started_in_day"),
    )
    valid = groups.filter(F.col("active_us") >= MINIMUM_USE_US)
    summary = valid.groupBy(*keys).agg(
        F.count(F.lit(1)).alias("logical_use_count"),
        F.sum("started_in_day").alias("usage_start_count"),
        F.sum("active_us").alias("usage_duration_us"),
        F.timestamp_micros(F.min("first_us")).alias("first_use_at"),
        F.timestamp_micros(F.max("last_us")).alias("last_use_end_at"),
        F.max("has_overlap").alias("overlap_count"),
    )
    rejected = groups.filter(F.col("active_us") < MINIMUM_USE_US).groupBy(*keys).agg(
        F.count(F.lit(1)).alias("rejected_short_use_count")
    )
    return (
        diagnostics.join(summary, keys, "left")
        .join(rejected, keys, "left")
        .fillna(
            {
                "logical_use_count": 0,
                "usage_start_count": 0,
                "usage_duration_us": 0,
                "overlap_count": 0,
                "rejected_short_use_count": 0,
            }
        )
    )


def build_appliance_usage_daily(
    observations: DataFrame,
    coverage: DataFrame,
    session_summary: DataFrame,
    *,
    appliance_types: Sequence[str],
    business_utc_offset_seconds: int = 9 * 3600,
) -> DataFrame:
    """Create empty-day rows and keep NOT_USED distinct from UNKNOWN."""

    spark = observations.sparkSession
    appliances = spark.createDataFrame(
        [(item,) for item in sorted(set(appliance_types))], ["appliance_type"]
    )
    grid = observations.select(
        "household_id",
        F.col("observation_date").alias("usage_date"),
        "observation_status",
        "coverage_ratio",
        "quality_flags",
        F.col("run_id").alias("observation_run_id"),
        "input_snapshot_id",
        "config_version",
    ).crossJoin(F.broadcast(appliances))
    coverage_keys = ["household_id", "appliance_type"]
    coverage_for_join = coverage.withColumnRenamed(
        "coverage_date", "usage_date"
    ).withColumnRenamed("input_snapshot_id", "analysis_input_snapshot_id")
    combined = (
        grid.join(
            coverage_for_join,
            ["usage_date", *coverage_keys],
            "left",
        )
        .join(session_summary, ["usage_date", *coverage_keys], "left")
        .fillna(
            {
                "source_session_count": 0,
                "open_session_count": 0,
                "overlap_count": 0,
                "rejected_short_use_count": 0,
            }
        )
    )
    invalid_observation = (F.col("observation_status") != "VALID") | F.array_contains(
        F.col("quality_flags"), "PARTIAL_DAY"
    )
    analysis_unknown = F.col("analysis_status").isNull() | (
        F.col("analysis_status") != "COMPLETE"
    ) | (F.col("delivery_status") != "COMPLETE") | (
        F.col("analysis_input_snapshot_id") != F.col("input_snapshot_id")
    )
    session_invalid = (F.col("open_session_count") > 0) | (F.col("overlap_count") > 0)
    certain = ~(invalid_observation | analysis_unknown | session_invalid)
    used = F.coalesce(F.col("logical_use_count"), F.lit(0)) > 0
    return (
        combined.withColumn(
            "usage_status",
            F.when(F.col("observation_status") == "NOT_APPLICABLE", "NOT_APPLICABLE")
            .when(~certain, "UNKNOWN")
            .when(used, "USED")
            .otherwise("NOT_USED"),
        )
        .withColumn(
            "is_used",
            F.when(F.col("usage_status") == "USED", F.lit(True))
            .when(F.col("usage_status") == "NOT_USED", F.lit(False))
            .otherwise(F.lit(None).cast("boolean")),
        )
        .withColumn("baseline_eligible", F.col("usage_status").isin("USED", "NOT_USED"))
        .withColumn(
            "logical_use_count",
            F.when(F.col("usage_status").isin("USED", "NOT_USED"), F.coalesce("logical_use_count", F.lit(0))).otherwise(F.lit(None).cast("long")),
        )
        .withColumn(
            "usage_duration_us",
            F.when(F.col("usage_status").isin("USED", "NOT_USED"), F.coalesce("usage_duration_us", F.lit(0))).otherwise(F.lit(None).cast("long")),
        )
        .withColumn(
            "usage_start_count",
            F.when(
                F.col("usage_status").isin("USED", "NOT_USED"),
                F.coalesce("usage_start_count", F.lit(0)),
            ).otherwise(F.lit(None).cast("long")),
        )
        .withColumn(
            "first_use_at",
            F.when(F.col("usage_status").isin("USED", "NOT_USED"), F.col("first_use_at")),
        )
        .withColumn(
            "last_use_end_at",
            F.when(F.col("usage_status").isin("USED", "NOT_USED"), F.col("last_use_end_at")),
        )
        .withColumn(
            "first_use_second",
            F.when(
                F.col("first_use_at").isNotNull(),
                F.pmod(
                    F.col("first_use_at").cast("long")
                    + F.lit(business_utc_offset_seconds),
                    F.lit(86_400),
                ).cast("int"),
            ),
        )
        .withColumn(
            "quality_flags",
            F.array_sort(
                F.array_distinct(
                    F.concat(
                        F.col("quality_flags"),
                        F.when(F.col("open_session_count") > 0, F.array(F.lit("OPEN_SESSION"))).otherwise(F.expr("array()").cast("array<string>")),
                        F.when(F.col("overlap_count") > 0, F.array(F.lit("SESSION_OVERLAP"))).otherwise(F.expr("array()").cast("array<string>")),
                    )
                )
            ),
        )
    )
