"""Synthesize ``analysis.snapshot.v1`` lines and historicalAssessment inputs.

The monitoring backfill (docs/historical-assessment.md) replays snapshots in
``observed_at`` order. One line per ``interval`` seconds is enough because the
policy treats gaps under 120 seconds as continuous observation. Seconds inside a
missing window produce no line, so the gap is visible to the assessor.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
import json
from pathlib import Path
from typing import Callable, Iterable
from uuid import uuid5

import numpy as np

from history_generator.lake import APPLIANCE_TYPES, NAMESPACE
from history_generator.scenario import SECONDS_PER_DAY, Household, Scenario
from history_generator.schedule import DayPlan


DEFAULT_POLICY = {"policyVersion": "history-generator-v1"}


def snapshot_lines(scenario: Scenario, plan: DayPlan, waveform: np.ndarray, *, interval: int) -> Iterable[str]:
    day_start = scenario.day_start_utc(plan.day).astimezone(scenario.tzinfo)
    for second in range(0, SECONDS_PER_DAY, interval):
        if plan.is_missing(second):
            continue
        observed = day_start + timedelta(seconds=second)
        row = waveform[second]
        payload = {
            "schema_version": 1,
            "snapshot_id": str(uuid5(NAMESPACE, f"snapshot:{plan.household_id}:{observed.isoformat()}")),
            "household_id": plan.household_id,
            "observed_at": observed.isoformat(),
            "published_at": (observed + timedelta(milliseconds=100)).isoformat(),
            "measurement": {
                "active_power": round(float(row[0]), 2),
                "reactive_power": round(float(row[1]), 2),
                "power_factor": round(float(row[2]), 3),
                "current": round(float(row[3]), 3),
            },
            "appliances": [
                {"appliance_type": name, "is_on": plan.is_on(name.lower(), second)}
                for name in APPLIANCE_TYPES
            ],
        }
        yield json.dumps(payload, separators=(",", ":"))


def write_household_inputs(
    scenario: Scenario,
    household: Household,
    output_dir: Path,
    day_plans: Callable[[date], tuple[DayPlan, np.ndarray]],
    *,
    interval: int = 60,
    policy: dict | None = None,
    profiles_name: str = "profiles.json",
) -> dict:
    """Write ``snapshots.jsonl``, ``config.json`` and ``policy.json`` for one household."""

    target = output_dir / household.household_id
    target.mkdir(parents=True, exist_ok=True)
    count = 0
    with (target / "snapshots.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
        for day in scenario.dates():
            plan, waveform = day_plans(day)
            for line in snapshot_lines(scenario, plan, waveform, interval=interval):
                handle.write(line + "\n")
                count += 1
    end = scenario.day_start_utc(scenario.end + timedelta(days=1)).astimezone(scenario.tzinfo)
    config = {
        "householdId": household.household_id,
        "start": scenario.day_start_utc(scenario.start).astimezone(scenario.tzinfo).isoformat(),
        "end": end.isoformat(),
        "stepSeconds": interval,
        "snapshots": "snapshots.jsonl",
        "profiles": profiles_name,
        "policy": "policy.json",
        "awayPeriods": [
            {"start": away.start.astimezone(scenario.tzinfo).isoformat(),
             "end": away.end.astimezone(scenario.tzinfo).isoformat()}
            for away in household.away
        ],
    }
    (target / "config.json").write_text(json.dumps(config, indent=2, ensure_ascii=False), encoding="utf-8")
    (target / "policy.json").write_text(
        json.dumps(policy or DEFAULT_POLICY, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return {"household_id": household.household_id, "snapshots": count, "directory": str(target)}
