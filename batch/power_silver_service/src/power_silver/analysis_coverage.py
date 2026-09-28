"""Silver analysis coverage built from fixed power input and delivered receipts."""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType


SUCCESS = "SUCCEEDED"
WARMUP = "SKIPPED_WARMUP"
QUALITY = "SKIPPED_QUALITY_GATE"
FAILED = ("FAILED_INFERENCE", "FAILED_PERSISTENCE")


def _chosen_receipts(receipts: DataFrame) -> DataFrame:
    """Choose one terminal interpretation without counting retries twice.

    A success wins over every failed/skipped attempt. Otherwise the latest attempt
    describes why the input was not analyzed.
    """

    ranking = Window.partitionBy("message_id", "analysis_run_id").orderBy(
        F.when(F.col("outcome") == SUCCESS, F.lit(1)).otherwise(F.lit(0)).desc(),
        F.col("attempt").desc(),
        F.col("processed_at").desc(),
        F.col("receipt_id").desc(),
    )
    return (
        receipts.withColumn("_rank", F.row_number().over(ranking))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


def build_analysis_coverage(
    power_clean: DataFrame,
    receipts: DataFrame,
    *,
    appliance_types: Sequence[str],
    input_snapshot_id: str,
    delivered_session_changes: DataFrame | None = None,
) -> DataFrame:
    """Return one row per household/date/appliance for one-second input slots."""

    if not appliance_types:
        raise ValueError("appliance_types must not be empty")
    spark = power_clean.sparkSession
    appliances = spark.createDataFrame(
        [(item,) for item in sorted(set(appliance_types))], ["appliance_type"]
    )
    delivered_receipts = _with_delivery_status(receipts, delivered_session_changes)
    chosen = _chosen_receipts(delivered_receipts).select(
        "message_id",
        "analysis_run_id",
        "outcome",
        "appliance_types",
        "processed_at",
        "model_version",
        "pipeline_version",
        "state_epoch",
        "session_change_refs_json",
        "receipt_delivery_status",
    )
    # One input contributes to one slot only. Duplicated device records in the same
    # second are resolved after message-level receipt selection, not by receipt count.
    inputs = (
        power_clean.select(
            "message_id", "household_id", "event_date", "measured_at_utc"
        )
        .dropDuplicates(["message_id"])
        .crossJoin(F.broadcast(appliances))
        .join(chosen, "message_id", "left")
        .withColumn("slot_second", F.col("measured_at_utc").cast("long"))
        .withColumn(
            "effective_outcome",
            F.when(
                (F.col("outcome") == SUCCESS)
                & F.expr("array_contains(appliance_types, appliance_type)"),
                F.lit(SUCCESS),
            )
            .when(F.col("outcome") == SUCCESS, F.lit(None).cast("string"))
            .otherwise(F.col("outcome")),
        )
    )

    # Prefer success when multiple messages land in one household slot; otherwise
    # preserve the strongest diagnostic category deterministically.
    priority = (
        F.when(F.col("effective_outcome") == SUCCESS, 5)
        .when(F.col("effective_outcome").isin(*FAILED), 4)
        .when(F.col("effective_outcome") == QUALITY, 3)
        .when(F.col("effective_outcome") == WARMUP, 2)
        .otherwise(1)
    )
    slot_window = Window.partitionBy(
        "household_id", "event_date", "appliance_type", "slot_second"
    ).orderBy(priority.desc(), F.col("message_id"))
    slots = (
        inputs.withColumn("_slot_rank", F.row_number().over(slot_window))
        .filter(F.col("_slot_rank") == 1)
        .drop("_slot_rank")
        .withColumn("is_analyzed", F.col("effective_outcome") == SUCCESS)
    )

    gap_rows = slots.filter(~F.col("is_analyzed")).select(
        "household_id", "event_date", "appliance_type", "slot_second"
    )
    gap_window = Window.partitionBy(
        "household_id", "event_date", "appliance_type"
    ).orderBy("slot_second")
    gaps = (
        gap_rows.withColumn("_seq", F.row_number().over(gap_window))
        .withColumn("_island", F.col("slot_second") - F.col("_seq"))
        .groupBy("household_id", "event_date", "appliance_type", "_island")
        .agg((F.max("slot_second") - F.min("slot_second") + 1).alias("gap_seconds"))
        .groupBy("household_id", "event_date", "appliance_type")
        .agg(F.max("gap_seconds").alias("max_unanalyzed_seconds"))
    )

    grouped = slots.groupBy(
        "household_id", "event_date", "appliance_type"
    ).agg(
        F.count(F.lit(1)).alias("observed_slot_count"),
        F.sum(F.when(F.col("effective_outcome") == SUCCESS, 1).otherwise(0)).alias(
            "analyzed_slot_count"
        ),
        F.sum(F.when(F.col("effective_outcome") == WARMUP, 1).otherwise(0)).alias(
            "warmup_slot_count"
        ),
        F.sum(F.when(F.col("effective_outcome") == QUALITY, 1).otherwise(0)).alias(
            "quality_skipped_slot_count"
        ),
        F.sum(F.when(F.col("effective_outcome").isin(*FAILED), 1).otherwise(0)).alias(
            "failed_slot_count"
        ),
        F.sum(F.when(F.col("effective_outcome").isNull(), 1).otherwise(0)).alias(
            "unknown_slot_count"
        ),
        F.sort_array(F.collect_set("analysis_run_id")).alias("analysis_run_ids"),
        F.sort_array(F.collect_set("model_version")).alias("model_versions"),
        F.sort_array(F.collect_set("pipeline_version")).alias("pipeline_versions"),
        F.sort_array(F.collect_set("state_epoch")).alias("state_epochs"),
        F.sum(
            F.when(
                (F.col("effective_outcome") == SUCCESS)
                & (F.col("receipt_delivery_status") != "COMPLETE"),
                1,
            ).otherwise(0)
        ).alias("delivery_pending_slot_count"),
    )
    result = (
        grouped.join(
            gaps, ["household_id", "event_date", "appliance_type"], "left"
        )
        .fillna({"max_unanalyzed_seconds": 0})
        .withColumn(
            "analysis_coverage_ratio",
            F.col("analyzed_slot_count") / F.col("observed_slot_count"),
        )
        .withColumn(
            "analysis_status",
            F.when(F.col("failed_slot_count") > 0, F.lit("ERROR"))
            .when(F.col("analyzed_slot_count") == F.col("observed_slot_count"), F.lit("COMPLETE"))
            .otherwise(F.lit("INCOMPLETE")),
        )
        .withColumn(
            "delivery_status",
            F.when(F.col("delivery_pending_slot_count") == 0, "COMPLETE").otherwise("PENDING"),
        )
        .withColumn("input_snapshot_id", F.lit(input_snapshot_id))
        .withColumnRenamed("event_date", "coverage_date")
    )
    return result


def _with_delivery_status(
    receipts: DataFrame,
    delivered_session_changes: DataFrame | None,
) -> DataFrame:
    """Verify every referenced session/version against confirmed session manifests."""

    refs_schema = ArrayType(
        StructType(
            [
                StructField("session_id", StringType(), False),
                StructField("session_version", IntegerType(), False),
            ]
        )
    )
    parsed = receipts.withColumn(
        "_refs", F.from_json("session_change_refs_json", refs_schema)
    )
    if delivered_session_changes is None:
        return parsed.withColumn(
            "receipt_delivery_status",
            F.when(F.size("_refs") == 0, "COMPLETE").otherwise("PENDING"),
        ).drop("_refs")

    confirmed = delivered_session_changes.select(
        F.col("session_id").alias("_delivered_session_id"),
        F.col("session_version").alias("_delivered_session_version"),
    ).dropDuplicates()
    exploded = parsed.select(
        "receipt_id",
        F.explode_outer("_refs").alias("_ref"),
    ).join(
        confirmed,
        (F.col("_ref.session_id") == F.col("_delivered_session_id"))
        & (F.col("_ref.session_version") == F.col("_delivered_session_version")),
        "left",
    )
    status = exploded.groupBy("receipt_id").agg(
        F.sum(F.when(F.col("_ref").isNotNull(), 1).otherwise(0)).alias("_required"),
        F.sum(F.when(F.col("_delivered_session_id").isNotNull(), 1).otherwise(0)).alias(
            "_delivered"
        ),
    ).withColumn(
        "receipt_delivery_status",
        F.when(F.col("_required") == F.col("_delivered"), "COMPLETE").otherwise("PENDING"),
    )
    return parsed.drop("_refs").join(
        status.select("receipt_id", "receipt_delivery_status"), "receipt_id", "left"
    )


def apply_completion_policy(
    coverage: DataFrame,
    *,
    minimum_coverage_ratio: float = 0.95,
    maximum_gap_seconds: int = 120,
) -> DataFrame:
    """Keep completion and baseline eligibility as separate decisions."""

    if not 0 <= minimum_coverage_ratio <= 1:
        raise ValueError("minimum_coverage_ratio must be between zero and one")
    if maximum_gap_seconds < 0:
        raise ValueError("maximum_gap_seconds must be nonnegative")
    return (
        coverage.withColumn(
            "completion_status",
            F.when(F.col("delivery_status") != "COMPLETE", F.lit("WAITING_DELIVERY"))
            .when(
                F.col("analysis_status").isin("COMPLETE", "INCOMPLETE"),
                F.lit("COMPLETED"),
            ).otherwise(F.lit("ERROR")),
        )
        .withColumn(
            "baseline_eligible",
            (F.col("analysis_status") == "COMPLETE")
            & (F.col("delivery_status") == "COMPLETE")
            & (F.col("analysis_coverage_ratio") >= F.lit(minimum_coverage_ratio))
            & (F.col("max_unanalyzed_seconds") <= F.lit(maximum_gap_seconds)),
        )
    )
