from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
import re

import numpy as np
import pyarrow.parquet as pq
import pytest

from history_generator.lake import (
    APPLIANCE_TYPES,
    BRONZE_POWER_SCHEMA,
    RECEIPT_SCHEMA,
    SESSION_SCHEMA,
    HouseholdDay,
    LakePaths,
    LakeWriter,
)
from history_generator.scenario import load_scenario
from history_generator.schedule import plan_day
from history_generator.waveform import SyntheticWaveform, seed_from

power_silver_storage = pytest.importorskip("power_silver.storage")

# power-silver quarantines message ids that do not look like UUIDs and instants
# that are not ISO-8601 with an offset. Keep the generator inside those rules.
UUID_PATTERN = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
INSTANT_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]\d{2}:\d{2})$")


def scenario_file(tmp_path: Path, interval: int = 1) -> Path:
    document = {
        "range": {"start": "2026-06-25", "end": "2026-06-26"},
        "households": [
            {"household_id": "T001", "seed": 1, "sampling_interval_seconds": interval,
             "schedule": [{"appliance": "kettle", "median": "08:50", "jitter_minutes": 5,
                           "probability": 1.0, "duration_seconds": [150, 210]},
                          {"appliance": "iron", "median": "23:58", "probability": 1.0,
                           "duration_seconds": [300, 420]}],
             "periods": [{"name": "missing", "start": "2026-06-26",
                          "missing_windows": [["09:00", "15:00"]]}]},
            {"household_id": "T002", "seed": 2, "sampling_interval_seconds": 10,
             "schedule": [{"appliance": "microwave", "median": "17:50", "probability": 1.0,
                           "duration_seconds": [240, 360]}]},
        ],
    }
    path = tmp_path / "scenario.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def build_items(scenario, day):
    generator = SyntheticWaveform()
    items = []
    for household in scenario.households:
        plan = plan_day(household, day)
        items.append(HouseholdDay(household, plan, generator.generate(plan, seed_from(household.seed, day))))
    return items


def test_writer_produces_loader_compatible_files_and_manifests(tmp_path):
    scenario = load_scenario(scenario_file(tmp_path))
    storage = power_silver_storage.LocalLakeStorage(tmp_path / "lake")
    paths = LakePaths()
    writer = LakeWriter(storage, paths, scenario)
    day = date(2026, 6, 25)
    items = build_items(scenario, day)
    result = writer.write_day(day, items, now=datetime(2026, 9, 24, tzinfo=timezone.utc))

    # 86,400 one-second rows + 8,640 ten-second rows.
    assert result.power_rows == 86_400 + 8_640
    assert result.receipt_rows == result.power_rows
    assert result.session_rows == sum(len(item.plan.sessions) for item in items)
    assert not result.skipped and writer.day_written(day)

    bronze_manifest = json.loads(storage.read_bytes(writer.bronze_manifest_path(day)))
    assert bronze_manifest["business_dates"] == ["2026-06-25"]
    assert bronze_manifest["ok_count"] == result.power_rows
    assert len(bronze_manifest["files"]) == 24
    assert bronze_manifest["min_measured_at"] == "2026-06-24T15:00:00+00:00"  # KST midnight
    assert bronze_manifest["committed_at"] > bronze_manifest["max_measured_at"]

    power = pq.read_table(storage.resolve(bronze_manifest["files"][0]))
    assert power.schema.equals(BRONZE_POWER_SCHEMA)
    rows = power.to_pylist()
    assert all(UUID_PATTERN.match(row["message_id"]) for row in rows)
    assert all(INSTANT_PATTERN.match(row["measured_at"]) for row in rows)
    assert rows[0]["measured_at"] == "2026-06-24T15:00:00+00:00"
    assert len({row["message_id"] for row in rows}) == len(rows)
    assert all(row["active_power"] >= 0 and 0 <= row["power_factor"] <= 1 and row["current"] >= 0 for row in rows)
    hour_seconds = {row["measured_at"] for row in rows if row["household_id"] == "T002"}
    assert len(hour_seconds) == 360  # ten-second household

    receipt_manifest = json.loads(storage.read_bytes(writer.receipt_manifest_path(day)))
    assert receipt_manifest["row_count"] == result.receipt_rows
    receipt = pq.read_table(storage.resolve(receipt_manifest["files"][0]["path"]))
    assert receipt.schema.equals(RECEIPT_SCHEMA)
    first = receipt.slice(0, 1).to_pylist()[0]
    assert first["outcome"] == "SUCCEEDED" and first["attempt"] == 1
    assert first["appliance_types"] == list(APPLIANCE_TYPES)
    assert first["processed_at"] - first["measured_at"] == timedelta(seconds=1)
    assert first["receipt_id"] == "r-" + first["message_id"]
    assert set(power.column("message_id").to_pylist()) == set(receipt.column("message_id").to_pylist())

    session_manifest = json.loads(storage.read_bytes(writer.session_manifest_path(day)))
    assert session_manifest["batch_kind"] == "INCREMENTAL"
    sessions = pq.read_table(storage.resolve(session_manifest["files"][0]["path"]))
    assert sessions.schema.equals(SESSION_SCHEMA)
    session_rows = sessions.to_pylist()
    assert all(row["operation"] == "INSERT" and row["session_version"] == 1 and not row["is_deleted"]
               for row in session_rows)
    assert all(row["max_probability"] == Decimal("0.9900") for row in session_rows)
    assert all(row["ended_at"] > row["started_at"] and row["changed_at"] > row["ended_at"] for row in session_rows)
    assert all(row["observation_date"] == day for row in session_rows)
    assert len({row["session_id"] for row in session_rows}) == len(session_rows)
    kettle = [row for row in session_rows if row["appliance_type"] == "KETTLE"]
    assert kettle and kettle[0]["started_at"].astimezone(timezone(timedelta(hours=9))).hour == 8

    # Second call is a no-op unless forced.
    assert writer.write_day(day, items).skipped
    assert not writer.write_day(day, items, force=True).skipped


def test_missing_window_drops_rows_and_receipts(tmp_path):
    scenario = load_scenario(scenario_file(tmp_path))
    storage = power_silver_storage.LocalLakeStorage(tmp_path / "lake")
    writer = LakeWriter(storage, LakePaths(), scenario)
    day = date(2026, 6, 26)
    result = writer.write_day(day, build_items(scenario, day))
    assert result.power_rows == 86_400 - 6 * 3600 + 8_640
    assert result.receipt_rows == result.power_rows
    bronze_manifest = json.loads(storage.read_bytes(writer.bronze_manifest_path(day)))
    assert len(bronze_manifest["files"]) == 24  # T002 still has rows in every hour


def test_schemas_match_the_lake_loaders():
    loader_schema = pytest.importorskip("session_lake_loader.lake_schema")
    receipt_lake = pytest.importorskip("session_lake_loader.receipt_lake")
    assert SESSION_SCHEMA.equals(loader_schema.LAKE_SCHEMA)
    assert RECEIPT_SCHEMA.equals(receipt_lake.RECEIPT_SCHEMA)


def test_waveform_matches_plan_and_is_deterministic(tmp_path):
    scenario = load_scenario(scenario_file(tmp_path))
    household = scenario.household("T001")
    plan = plan_day(household, date(2026, 6, 25))
    generator = SyntheticWaveform()
    first = generator.generate(plan, 42)
    second = generator.generate(plan, 42)
    assert np.array_equal(first, second) and first.shape == (86_400, 4)
    kettle = [session for session in plan.sessions if session.appliance == "kettle"][0]
    on = first[kettle.start_second:kettle.end_second, 0].mean()
    off = first[kettle.start_second - 600:kettle.start_second - 300, 0].mean()
    assert on - off > 1000
