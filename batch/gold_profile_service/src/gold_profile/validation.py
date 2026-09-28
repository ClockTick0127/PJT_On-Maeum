"""Fail-closed output contract checks before Gold publication."""

from __future__ import annotations

from pyspark.sql import functions as F


def _reject_duplicate(frame, keys, label):
    row = frame.groupBy(*keys).count().filter(F.col("count") != 1).limit(1).collect()
    if row:
        raise ValueError(f"duplicate {label} key: {row[0].asDict()}")


def validate_baselines(frame, *, input_incomplete: bool) -> None:
    _reject_duplicate(
        frame,
        ["household_id", "appliance_type", "baseline_scope", "weekday"],
        "routine baseline",
    )
    invalid = frame.filter(
        (F.col("sample_days") < 0)
        | (F.col("active_days") < 0)
        | (F.col("active_days") > F.col("sample_days"))
        | ~F.col("daily_use_probability").between(0, 1)
        | ~F.col("reliability_weight").between(0, 1)
        | (
            F.col("enabled")
            & ((F.col("quality_status") != "READY") | F.col("expected_until_second").isNull())
        )
        | (F.lit(input_incomplete) & (F.col("quality_status") != "INPUT_INCOMPLETE"))
    ).limit(1).collect()
    if invalid:
        raise ValueError(f"invalid routine baseline: {invalid[0].asDict()}")


def validate_logical_uses(frame) -> None:
    _reject_duplicate(frame, ["logical_use_id"], "logical use")
    invalid = frame.filter(
        (F.col("ended_at") <= F.col("started_at"))
        | (F.col("active_duration_us") <= 0)
        | (F.col("elapsed_duration_us") <= 0)
    ).limit(1).collect()
    if invalid:
        raise ValueError(f"invalid logical use: {invalid[0].asDict()}")


def validate_statistics(frame, *, input_incomplete: bool) -> None:
    _reject_duplicate(
        frame,
        ["household_id", "metric_name", "appliance_type", "weekday_group", "time_bucket"],
        "statistical profile",
    )
    invalid = frame.filter(
        (F.col("sample_count") < 0)
        | (F.col("eligible_day_count") < 0)
        | (F.col("mad") < 0)
        | (F.col("p90").isNotNull() & F.col("p50").isNotNull() & (F.col("p90") < F.col("p50")))
        | (F.lit(input_incomplete) & (F.col("quality_status") != "INPUT_INCOMPLETE"))
    ).limit(1).collect()
    if invalid:
        raise ValueError(f"invalid statistical profile: {invalid[0].asDict()}")
