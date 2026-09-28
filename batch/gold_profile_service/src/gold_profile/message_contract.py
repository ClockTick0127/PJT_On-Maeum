"""Build the schema-v2 household profile envelope from one Gold run."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal


BASELINE_FIELDS = (
    "appliance_type", "baseline_scope", "weekday", "sample_days", "active_days",
    "daily_use_probability", "reliability_weight", "first_use_time_p50_second",
    "expected_until_second", "preferred_window_start_second",
    "preferred_window_end_second", "quality_status", "enabled",
)
STATISTIC_FIELDS = (
    "metric_name", "appliance_type", "weekday_group", "time_bucket",
    "sample_count", "eligible_day_count", "p50", "p90", "mad", "unit",
    "quality_status",
)


def _json_value(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def _project(row, fields):
    values = row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
    return {name: _json_value(values.get(name)) for name in fields}


def build_household_messages(
    baseline,
    statistics,
    *,
    profile_version: str,
    profile_revision: int,
    delivery_mode: str,
    as_of_date,
    window_start_date,
    effective_from,
    published_at,
    input_snapshot_id: str,
    rule_version: str,
    statistic_rule_version: str,
    input_incomplete: bool,
) -> list[dict]:
    if profile_revision < 1:
        raise ValueError("profile_revision must be positive")
    if delivery_mode not in {"ACTIVE", "SHADOW"}:
        raise ValueError(f"unsupported delivery_mode: {delivery_mode}")
    baseline_rows = baseline.collect()
    statistic_rows = statistics.collect()
    households = sorted({row.household_id for row in [*baseline_rows, *statistic_rows]})
    messages = []
    for household_id in households:
        household_baselines = [
            _project(row, BASELINE_FIELDS)
            for row in baseline_rows if row.household_id == household_id
        ]
        household_statistics = [
            _project(row, STATISTIC_FIELDS)
            for row in statistic_rows if row.household_id == household_id
        ]
        versions = {
            row.profile_version for row in [*baseline_rows, *statistic_rows]
            if row.household_id == household_id
        }
        if versions != {profile_version}:
            raise ValueError(
                f"profile components do not share version for {household_id}: {versions}"
            )
        messages.append({
            "schema_version": 2,
            "household_id": household_id,
            "profile_version": profile_version,
            "profile_revision": profile_revision,
            "delivery_mode": delivery_mode,
            "as_of_date": as_of_date.isoformat(),
            "window_start_date": window_start_date.isoformat(),
            "window_end_date": as_of_date.isoformat(),
            "effective_from": effective_from.isoformat(),
            "published_at": published_at.isoformat(),
            "input_snapshot_id": input_snapshot_id,
            "rule_version": rule_version,
            "statistic_rule_version": statistic_rule_version,
            "quality_status": "INPUT_INCOMPLETE" if input_incomplete else "READY",
            "routine_baselines": household_baselines,
            "statistics": household_statistics,
        })
    return messages
