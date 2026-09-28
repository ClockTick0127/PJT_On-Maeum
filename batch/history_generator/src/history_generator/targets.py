"""Emit an ``observation_targets.json`` that includes the scenario households."""

from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path

from history_generator.scenario import Scenario


def merged_targets(scenario: Scenario, existing: dict | None, *, include_load: bool) -> dict:
    document = {"config_version": "observation-targets-v1", "targets": []}
    if existing:
        if not isinstance(existing, dict) or not isinstance(existing.get("targets"), list):
            raise ValueError("existing targets file must be an object with a targets list")
        document["config_version"] = str(existing.get("config_version", document["config_version"]))
        document["targets"] = [dict(item) for item in existing["targets"]]
    present = {(item.get("household_id"), item.get("device_id")) for item in document["targets"]}
    effective_from = datetime(scenario.start.year, scenario.start.month, scenario.start.day,
                              tzinfo=scenario.tzinfo).isoformat()
    for household in scenario.households:
        if household.is_load and not include_load:
            continue
        key = (household.household_id, household.device_id)
        if key in present:
            continue
        document["targets"].append({
            "household_id": household.household_id,
            "device_id": household.device_id,
            "effective_from": effective_from,
            "effective_to": None,
            "sampling_interval_seconds": household.sampling_interval_seconds,
            "observation_enabled": True,
        })
        present.add(key)
    return document


def write_targets(scenario: Scenario, output: Path, existing_path: Path | None, *, include_load: bool) -> int:
    existing = None
    if existing_path is not None and existing_path.exists():
        existing = json.loads(existing_path.read_text(encoding="utf-8"))
    document = merged_targets(scenario, existing, include_load=include_load)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(document, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return len(document["targets"])
