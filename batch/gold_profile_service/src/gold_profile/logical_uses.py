"""Reconstruct complete logical-use episodes from versioned daily session slices."""

from __future__ import annotations

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


def build_logical_uses(
    slices: DataFrame,
    *,
    run_id: str,
    rule_version: str,
    business_utc_offset_seconds: int,
    window_start_at=None,
    window_end_at=None,
) -> DataFrame:
    content = [
        "household_id", "appliance_type", "original_started_at",
        "original_ended_at", "end_imputed",
    ]
    conflicts = (
        slices.groupBy("session_id", "session_version")
        .agg(F.countDistinct(F.to_json(F.struct(*content))).alias("variants"))
        .filter(F.col("variants") > 1)
        .limit(1)
        .collect()
    )
    if conflicts:
        row = conflicts[0]
        raise ValueError(
            f"conflicting session slice: {row['session_id']} "
            f"v{row['session_version']}"
        )

    # A session can occur in more than one daily output (cross-midnight), and an
    # older daily output can still contain the pre-correction version.  Once the
    # caller has proved that every date was prepared from one session-state
    # snapshot, the largest producer revision is the only current state.
    latest = Window.partitionBy("session_id").orderBy(F.col("session_version").desc())
    sessions = (
        slices.dropDuplicates(["session_id", "session_version", *content])
        .withColumn("_latest", F.row_number().over(latest))
        .filter((F.col("_latest") == 1) & ~F.col("end_imputed"))
        .select(
            "session_id", "session_version", "household_id", "appliance_type",
            F.col("original_started_at").alias("started_at"),
            F.col("original_ended_at").alias("ended_at"),
        )
        .dropDuplicates(["session_id", "session_version"])
        .filter(F.col("ended_at").isNotNull() & (F.col("ended_at") > F.col("started_at")))
        .withColumn("start_us", F.unix_micros("started_at"))
        .withColumn("end_us", F.unix_micros("ended_at"))
        .withColumn("active_duration_us", F.col("end_us") - F.col("start_us"))
    )
    # Boundary-crossing slices are not complete episodes for this profile window.
    # Excluding them also prevents post-as-of activity from leaking into the model.
    if window_start_at is not None:
        sessions = sessions.filter(F.col("started_at") >= F.lit(window_start_at))
    if window_end_at is not None:
        sessions = sessions.filter(F.col("ended_at") <= F.lit(window_end_at))
    keys = ["household_id", "appliance_type"]
    ordering = Window.partitionBy(*keys).orderBy("start_us", "end_us", "session_id")
    previous = ordering.rowsBetween(Window.unboundedPreceding, -1)
    merge_map = F.create_map(
        *sum(
            ([F.lit(name), F.lit(seconds * 1_000_000)] for name, seconds in MERGE_GAP_SECONDS.items()),
            [],
        )
    )
    prepared = (
        sessions.withColumn("previous_max_end", F.max("end_us").over(previous))
        .withColumn("overlaps_previous", F.col("previous_max_end").isNotNull() & (F.col("start_us") < F.col("previous_max_end")))
        .withColumn("merge_gap_us", F.coalesce(merge_map[F.col("appliance_type")], F.lit(60_000_000)))
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
    grouped = prepared.groupBy(*keys, "logical_group").agg(
        F.min("start_us").alias("start_us"),
        F.max("end_us").alias("end_us"),
        F.sum("active_duration_us").alias("active_duration_us"),
        F.max(F.col("overlaps_previous").cast("int")).alias("overlap_count"),
        F.sort_array(F.collect_list(F.struct("session_id", "session_version"))).alias("source_session_refs"),
    )
    start_day = F.floor((F.col("start_us") / F.lit(1_000_000) + F.lit(business_utc_offset_seconds)) / F.lit(86_400))
    end_day = F.floor(((F.col("end_us") - F.lit(1)) / F.lit(1_000_000) + F.lit(business_utc_offset_seconds)) / F.lit(86_400))
    return (
        grouped.withColumn("started_at", F.timestamp_micros("start_us"))
        .withColumn("ended_at", F.timestamp_micros("end_us"))
        .withColumn("elapsed_duration_us", F.col("end_us") - F.col("start_us"))
        .withColumn("crosses_midnight", start_day != end_day)
        .withColumn(
            "quality_status",
            F.when(F.col("overlap_count") > 0, "INVALID_OVERLAP")
            .when(F.col("active_duration_us") < F.lit(10_000_000), "TOO_SHORT")
            .otherwise("VALID"),
        )
        .withColumn(
            "logical_use_id",
            F.sha2(
                F.concat_ws(
                    ":", "household_id", "appliance_type",
                    F.col("start_us").cast("string"), F.col("end_us").cast("string"),
                ),
                256,
            ),
        )
        .withColumn("run_id", F.lit(run_id))
        .withColumn("rule_version", F.lit(rule_version))
        .drop("logical_group", "start_us", "end_us")
    )
