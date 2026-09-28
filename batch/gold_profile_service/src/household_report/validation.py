from pyspark.sql import functions as F


def require_columns(frame, names):
    missing = set(names) - set(frame.columns)
    if missing:
        raise ValueError(f"missing columns: {sorted(missing)}")


def reject_rows(frame, condition, message):
    if frame.filter(condition).limit(1).count():
        raise ValueError(message)


def require_unique(frame, keys):
    for key in keys:
        reject_rows(frame, F.col(key).isNull(), f"null key: {key}")
    if frame.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError(f"duplicate key: {keys}")


def validate_inputs(sources, inputs):
    require_columns(sources["targets"], ["household_id"])
    require_columns(
        sources["usage"],
        [
            "household_id",
            "usage_date",
            "appliance_type",
            "usage_status",
            "baseline_eligible",
            "usage_start_count",
            "usage_duration_us",
        ],
    )
    require_columns(
        sources["assessments"],
        [
            "assessment_id",
            "household_id",
            "assessed_at",
            "assessment_status",
            "risk_score",
            "risk_level",
            "profile_version",
            "policy_version",
            "score_version",
            "indicators",
        ],
    )
    require_unique(sources["targets"], ["household_id"])
    if not sources["targets"].limit(1).count():
        raise ValueError("empty report target set")
    require_unique(sources["usage"], ["household_id", "usage_date", "appliance_type"])
    require_unique(sources["assessments"], ["assessment_id"])

    for name in ("baseline", "statistics"):
        frame = sources[name]
        require_columns(frame, ["household_id", "profile_version"])
        reject_rows(
            frame,
            F.col("profile_version").isNull()
            | (F.col("profile_version") != inputs.gold_run_id),
            f"{name} does not match selected Gold run",
        )

    reject_rows(
        sources["usage"],
        F.col("baseline_eligible")
        & (
            ~F.col("usage_status").isin("USED", "NOT_USED")
            | F.col("usage_start_count").isNull()
            | F.col("usage_duration_us").isNull()
        ),
        "eligible usage has invalid status or missing metrics",
    )


def validate_outputs(frames):
    require_unique(frames["usage_summary"], ["household_id", "appliance_type"])
    require_unique(frames["evidence"], ["household_id", "evidence_id"])
    reject_rows(
        frames["usage_summary"],
        (F.col("active_days") > F.col("eligible_days"))
        | (F.col("missing_days") < 0)
        | (F.col("eligible_days") < 0),
        "invalid usage summary",
    )
