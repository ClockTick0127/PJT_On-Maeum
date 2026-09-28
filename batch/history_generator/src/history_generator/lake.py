"""Bronze power, analysis receipt and session writers with loader-compatible manifests.

Column types mirror the producers the aggregation already reads:

- Bronze power: ``power_silver.schemas.BRONZE_POWER_SCHEMA`` (strings for instants)
- receipts: ``session_lake_loader.receipt_lake.RECEIPT_SCHEMA``
- sessions: ``session_lake_loader.lake_schema.LAKE_SCHEMA``

``power-silver`` reads receipt and session Parquet without an explicit schema, so
the Arrow types here must stay identical to those producers (tests pin this).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import io
import json
import os
from uuid import UUID, uuid5

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from history_generator.scenario import APPLIANCES, SECONDS_PER_DAY, Household, Scenario
from history_generator.schedule import DayPlan


NAMESPACE = UUID("5f0c6a5e-9c7d-4c1a-9d6e-3f1b2a7c8d90")
SESSION_OPERATION_INSERT = "INSERT"
SESSION_BATCH_KIND = "INCREMENTAL"
RECEIPT_OUTCOME_SUCCEEDED = "SUCCEEDED"
MODEL_VERSION = "history-generator-v1"
PIPELINE_VERSION = "history-generator-v1"
STATE_EPOCH = "epoch-1"
APPLIANCE_TYPES = tuple(name.upper() for name in APPLIANCES)
PROBABILITY_QUANTUM = Decimal("0.0001")
SESSION_MAX_PROBABILITY = Decimal("0.9900")
# Locked ON thresholds of the reviewed R3 profiles (ai/realtime-analysis-service real_models).
DECISION_THRESHOLDS = {
    "KETTLE": Decimal("0.9900"),
    "INDUCTION": Decimal("0.9500"),
    "IRON": Decimal("0.9000"),
    "MICROWAVE": Decimal("0.9000"),
    "HAIR_DRYER": Decimal("0.9500"),
    "VACUUM_CLEANER": Decimal("0.6000"),
}

BRONZE_POWER_SCHEMA = pa.schema([
    ("message_id", pa.string()),
    ("household_id", pa.string()),
    ("device_id", pa.string()),
    ("measured_at", pa.string()),
    ("active_power", pa.float64()),
    ("reactive_power", pa.float64()),
    ("power_factor", pa.float64()),
    ("current", pa.float64()),
    ("topic", pa.string()),
    ("partition", pa.int32()),
    ("kafka_offset", pa.int64()),
    ("kafka_ts", pa.string()),
    ("ingested_at", pa.string()),
])

RECEIPT_SCHEMA = pa.schema([
    ("event_id", pa.int64()),
    ("receipt_id", pa.string()),
    ("message_id", pa.string()),
    ("household_id", pa.string()),
    ("device_id", pa.string()),
    ("source_topic", pa.string()),
    ("source_partition", pa.int32()),
    ("source_offset", pa.int64()),
    ("measured_at", pa.timestamp("us", tz="UTC")),
    ("processed_at", pa.timestamp("us", tz="UTC")),
    ("analysis_run_id", pa.string()),
    ("attempt", pa.int32()),
    ("model_version", pa.string()),
    ("pipeline_version", pa.string()),
    ("state_epoch", pa.string()),
    ("outcome", pa.string()),
    ("appliance_types", pa.list_(pa.string())),
    ("session_change_refs_json", pa.string()),
    ("error_type", pa.string()),
    ("batch_id", pa.string()),
    ("schema_version", pa.int32()),
])

SESSION_SCHEMA = pa.schema([
    ("event_id", pa.int64()),
    ("operation", pa.string()),
    ("session_id", pa.string()),
    ("session_version", pa.int32()),
    ("is_deleted", pa.bool_()),
    ("changed_at", pa.timestamp("us", tz="UTC")),
    ("activity_daily_id", pa.string()),
    ("household_id", pa.string()),
    ("appliance_type", pa.string()),
    ("observation_date", pa.date32()),
    ("started_at", pa.timestamp("us", tz="UTC")),
    ("ended_at", pa.timestamp("us", tz="UTC")),
    ("max_probability", pa.decimal128(5, 4)),
    ("decision_threshold", pa.decimal128(5, 4)),
    ("updated_at", pa.timestamp("us", tz="UTC")),
    ("batch_id", pa.string()),
    ("batch_kind", pa.string()),
    ("schema_version", pa.int32()),
])


@dataclass(frozen=True)
class LakePaths:
    """Lake locations; defaults match the local/EC2 compose services."""

    bronze_base: str = "/nilm/bronze/power"
    bronze_manifest_base: str = "/nilm/manifests/job=bronze-loader"
    receipt_bronze_base: str = "/nilm/bronze/analysis-processing-receipt"
    receipt_manifest_base: str = "/nilm/manifests/job=analysis-receipt-lake-loader"
    session_bronze_base: str = "/nilm/bronze/appliance-session"
    session_manifest_base: str = "/nilm/manifests/job=session-lake-loader"

    @classmethod
    def from_env(cls, environ=os.environ) -> "LakePaths":
        defaults = cls()
        return cls(
            bronze_base=environ.get("BRONZE_BASE", defaults.bronze_base).rstrip("/"),
            bronze_manifest_base=environ.get("BRONZE_MANIFEST_BASE", defaults.bronze_manifest_base).rstrip("/"),
            receipt_bronze_base=environ.get("RECEIPT_BRONZE_BASE", defaults.receipt_bronze_base).rstrip("/"),
            receipt_manifest_base=environ.get("RECEIPT_MANIFEST_BASE", defaults.receipt_manifest_base).rstrip("/"),
            session_bronze_base=environ.get("SESSION_BRONZE_BASE", defaults.session_bronze_base).rstrip("/"),
            session_manifest_base=environ.get("SESSION_MANIFEST_BASE", defaults.session_manifest_base).rstrip("/"),
        )


@dataclass
class HouseholdDay:
    household: Household
    plan: DayPlan
    waveform: np.ndarray  # (SECONDS_PER_DAY, 4) in lake column order


@dataclass
class DayResult:
    day: date
    households: int
    power_rows: int
    receipt_rows: int
    session_rows: int
    bytes_written: int
    skipped: bool = False
    files: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "date": self.day.isoformat(),
            "households": self.households,
            "power_rows": self.power_rows,
            "receipt_rows": self.receipt_rows,
            "session_rows": self.session_rows,
            "bytes_written": self.bytes_written,
            "skipped": self.skipped,
        }


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def parquet_bytes(table: pa.Table) -> bytes:
    sink = io.BytesIO()
    pq.write_table(table, sink, compression="snappy")
    return sink.getvalue()


def _iso_strings(start_epoch: int, suffix: str) -> np.ndarray:
    """Vectorised ``YYYY-MM-DDTHH:MM:SS<suffix>`` for every second of the day."""

    base = np.datetime64(int(start_epoch), "s") + np.arange(SECONDS_PER_DAY, dtype="int64")
    return np.char.add(np.datetime_as_string(base, unit="s"), suffix)


def _to_timestamp_us(epoch_seconds: np.ndarray) -> pa.Array:
    return pa.array((epoch_seconds.astype("int64") * 1_000_000), type=pa.timestamp("us", tz="UTC"))


class LakeWriter:
    """Write one business date for many households, idempotently."""

    def __init__(self, storage, paths: LakePaths, scenario: Scenario, *,
                 batch_prefix: str = "hist", partition: int = 0) -> None:
        self._storage = storage
        self._paths = paths
        self._scenario = scenario
        self._prefix = batch_prefix
        self._partition = partition
        self._epoch_ordinal = date(2020, 1, 1).toordinal()

    # --- paths ---------------------------------------------------------------------

    def batch_id(self, day: date) -> str:
        return f"{self._prefix}-{day.isoformat()}"

    def bronze_manifest_path(self, day: date) -> str:
        return f"{self._paths.bronze_manifest_base}/date={day}/manifest-{self._partition}-{self._prefix}.json"

    def receipt_manifest_path(self, day: date) -> str:
        return f"{self._paths.receipt_manifest_base}/ingest_date={day}/batch_id={self.batch_id(day)}/manifest.json"

    def session_manifest_path(self, day: date) -> str:
        return f"{self._paths.session_manifest_base}/ingest_date={day}/manifest-{self.batch_id(day)}.json"

    def day_written(self, day: date) -> bool:
        return all(self._storage.exists(path) for path in (
            self.bronze_manifest_path(day), self.receipt_manifest_path(day), self.session_manifest_path(day)))

    # --- writing -------------------------------------------------------------------

    def write_day(self, day: date, items: list[HouseholdDay], *, force: bool = False,
                  now: datetime | None = None) -> DayResult:
        if not force and self.day_written(day):
            return DayResult(day, len(items), 0, 0, 0, 0, skipped=True)
        now = now or datetime.now(timezone.utc)
        day_start = self._scenario.day_start_utc(day)
        start_epoch = int(day_start.timestamp())
        day_code = day.toordinal() - self._epoch_ordinal
        measured_iso = _iso_strings(start_epoch, "+00:00")
        kafka_iso = _iso_strings(start_epoch, "Z")
        ingested_iso = _iso_strings(start_epoch + 2, "Z")
        second_tail = [f"{second:012x}" for second in range(SECONDS_PER_DAY)]
        type_values = pa.array(APPLIANCE_TYPES)

        # Per household: sampled seconds that are not inside a missing window.
        sampled: list[np.ndarray] = []
        for item in items:
            seconds = np.arange(0, SECONDS_PER_DAY, item.household.sampling_interval_seconds, dtype="int64")
            if item.plan.missing_windows:
                keep = np.ones(len(seconds), dtype=bool)
                for a, b in item.plan.missing_windows:
                    keep &= ~((seconds >= a) & (seconds < b))
                seconds = seconds[keep]
            sampled.append(seconds)

        result = DayResult(day, len(items), 0, 0, 0, 0)
        power_files: list[str] = []
        receipt_files: list[dict] = []
        min_measured: int | None = None
        max_measured: int | None = None
        offset_lo: int | None = None
        offset_hi: int | None = None

        for hour in range(24):
            lo, hi = hour * 3600, (hour + 1) * 3600
            columns: dict[str, list] = {name: [] for name in BRONZE_POWER_SCHEMA.names}
            receipt_parts: dict[str, list] = {"event_id": [], "receipt_id": [], "message_id": [],
                                              "household_id": [], "device_id": [], "source_offset": [],
                                              "measured": []}
            for index, item in enumerate(items):
                seconds = sampled[index]
                seconds = seconds[(seconds >= lo) & (seconds < hi)]
                if len(seconds) == 0:
                    continue
                n = len(seconds)
                head = f"{day_code:08x}-{index & 0xFFFF:04x}-4000-8000-"
                message_ids = [head + second_tail[second] for second in seconds.tolist()]
                offsets = ((day_code * 4096 + index) * SECONDS_PER_DAY + seconds).astype("int64")
                waveform = item.waveform[seconds]
                columns["message_id"].append(pa.array(message_ids))
                columns["household_id"].append(pa.array([item.household.household_id] * n))
                columns["device_id"].append(pa.array([item.household.device_id] * n))
                columns["measured_at"].append(pa.array(measured_iso[seconds]))
                columns["active_power"].append(pa.array(waveform[:, 0]))
                columns["reactive_power"].append(pa.array(waveform[:, 1]))
                columns["power_factor"].append(pa.array(waveform[:, 2]))
                columns["current"].append(pa.array(waveform[:, 3]))
                columns["topic"].append(pa.array([self._scenario.topic] * n))
                columns["partition"].append(pa.array(np.full(n, self._partition, dtype="int32")))
                columns["kafka_offset"].append(pa.array(offsets))
                columns["kafka_ts"].append(pa.array(kafka_iso[seconds]))
                columns["ingested_at"].append(pa.array(ingested_iso[seconds]))
                receipt_parts["event_id"].append(offsets)
                receipt_parts["receipt_id"].append(pa.array(["r-" + value for value in message_ids]))
                receipt_parts["message_id"].append(pa.array(message_ids))
                receipt_parts["household_id"].append(pa.array([item.household.household_id] * n))
                receipt_parts["device_id"].append(pa.array([item.household.device_id] * n))
                receipt_parts["source_offset"].append(offsets)
                receipt_parts["measured"].append(seconds + start_epoch)
                min_measured = seconds[0] if min_measured is None else min(min_measured, int(seconds[0]))
                max_measured = int(seconds[-1]) if max_measured is None else max(max_measured, int(seconds[-1]))
                offset_lo = int(offsets[0]) if offset_lo is None else min(offset_lo, int(offsets[0]))
                offset_hi = int(offsets[-1]) if offset_hi is None else max(offset_hi, int(offsets[-1]))

            if not columns["message_id"]:
                continue
            power_table = pa.table(
                {name: pa.concat_arrays(parts) for name, parts in columns.items()},
                schema=BRONZE_POWER_SCHEMA,
            )
            power_path = (f"{self._paths.bronze_base}/ingest_date={day}/hour={hour:02d}"
                          f"/partition={self._partition}/part-{self._prefix}-{hour:02d}.parquet")
            data = parquet_bytes(power_table)
            self._storage.write_bytes(power_path, data)
            power_files.append(power_path)
            result.power_rows += power_table.num_rows
            result.bytes_written += len(data)

            rows = power_table.num_rows
            measured = np.concatenate(receipt_parts["measured"])
            offsets_np = np.concatenate(receipt_parts["event_id"])
            list_offsets = pa.array(np.arange(0, 6 * (rows + 1), 6, dtype="int32"))
            types = pa.ListArray.from_arrays(
                list_offsets, type_values.take(pa.array(np.tile(np.arange(6, dtype="int32"), rows)))
            )
            receipt_table = pa.table({
                "event_id": pa.array(offsets_np),
                "receipt_id": pa.concat_arrays(receipt_parts["receipt_id"]),
                "message_id": pa.concat_arrays(receipt_parts["message_id"]),
                "household_id": pa.concat_arrays(receipt_parts["household_id"]),
                "device_id": pa.concat_arrays(receipt_parts["device_id"]),
                "source_topic": pa.array([self._scenario.topic] * rows),
                "source_partition": pa.array(np.full(rows, self._partition, dtype="int32")),
                "source_offset": pa.array(np.concatenate(receipt_parts["source_offset"])),
                "measured_at": _to_timestamp_us(measured),
                "processed_at": _to_timestamp_us(measured + 1),
                "analysis_run_id": pa.array([self._scenario.analysis_run_id] * rows),
                "attempt": pa.array(np.ones(rows, dtype="int32")),
                "model_version": pa.array([MODEL_VERSION] * rows),
                "pipeline_version": pa.array([PIPELINE_VERSION] * rows),
                "state_epoch": pa.array([STATE_EPOCH] * rows),
                "outcome": pa.array([RECEIPT_OUTCOME_SUCCEEDED] * rows),
                "appliance_types": types,
                "session_change_refs_json": pa.array(["[]"] * rows),
                "error_type": pa.array([None] * rows, type=pa.string()),
                "batch_id": pa.array([self.batch_id(day)] * rows),
                "schema_version": pa.array(np.ones(rows, dtype="int32")),
            }, schema=RECEIPT_SCHEMA)
            receipt_path = (f"{self._paths.receipt_bronze_base}/ingest_date={day}"
                            f"/batch_id={self.batch_id(day)}/part-{hour:05d}.parquet")
            receipt_data = parquet_bytes(receipt_table)
            self._storage.write_bytes(receipt_path, receipt_data)
            receipt_files.append({"path": receipt_path, "rows": rows, "bytes": len(receipt_data),
                                  "sha256": sha256_hex(receipt_data)})
            result.receipt_rows += rows
            result.bytes_written += len(receipt_data)

        session_table = self._session_table(day, items, day_start)
        session_path = (f"{self._paths.session_bronze_base}/ingest_date={day}"
                        f"/batch_id={self.batch_id(day)}/part-00000.parquet")
        session_data = parquet_bytes(session_table)
        self._storage.write_bytes(session_path, session_data)
        result.session_rows = session_table.num_rows
        result.bytes_written += len(session_data)

        # Manifests last: a date counts as written only when all three exist.
        day_end = day_start + timedelta(seconds=SECONDS_PER_DAY)
        total_bytes = sum(self._storage.status(path).length for path in power_files)
        bronze_manifest = {
            "topic": self._scenario.topic,
            "partition": self._partition,
            "start_offset": offset_lo,
            "end_offset": offset_hi,
            "ok_count": result.power_rows,
            "quarantine_count": 0,
            "min_measured_at": (day_start + timedelta(seconds=min_measured or 0)).isoformat(),
            "max_measured_at": (day_start + timedelta(seconds=max_measured or 0)).isoformat(),
            "business_dates": [day.isoformat()],
            "file_bytes": total_bytes,
            "files": power_files,
            "flush_reason": "history-generator",
            "committed_at": (day_end + timedelta(hours=1)).isoformat(),
            "generator": {"scenario": os.path.basename(self._scenario.source_path), "generated_at": now.isoformat()},
        }
        self._storage.write_bytes(self.bronze_manifest_path(day), json.dumps(bronze_manifest).encode())
        receipt_manifest = {
            "job": "analysis-receipt-lake-loader",
            "schema_version": 1,
            "batch_id": self.batch_id(day),
            "ingest_date": day.isoformat(),
            "row_count": result.receipt_rows,
            "files": receipt_files,
            "completed_at": now.isoformat(),
            "generator": "history-generator",
        }
        self._storage.write_bytes(self.receipt_manifest_path(day), json.dumps(receipt_manifest).encode())
        session_manifest = {
            "job": "session-lake-loader",
            "schema_version": 1,
            "batch_kind": SESSION_BATCH_KIND,
            "batch_id": self.batch_id(day),
            "ingest_date": day.isoformat(),
            "row_count": result.session_rows,
            "file_count": 1,
            "files": [{"path": session_path, "rows": result.session_rows, "bytes": len(session_data),
                       "sha256": sha256_hex(session_data), "content_sha256": sha256_hex(session_data)}],
            "completed_at": now.isoformat(),
            "generator": "history-generator",
        }
        self._storage.write_bytes(self.session_manifest_path(day), json.dumps(session_manifest).encode())
        result.files = power_files + [item["path"] for item in receipt_files] + [session_path]
        return result

    def _session_table(self, day: date, items: list[HouseholdDay], day_start: datetime) -> pa.Table:
        rows: list[dict] = []
        day_code = day.toordinal() - self._epoch_ordinal
        for index, item in enumerate(items):
            household_id = item.household.household_id
            activity_daily_id = str(uuid5(NAMESPACE, f"{household_id}:{day.isoformat()}"))
            for position, session in enumerate(item.plan.sessions):
                started = day_start + timedelta(seconds=session.start_second)
                ended = day_start + timedelta(seconds=session.end_second)
                appliance = session.appliance.upper()
                changed = ended + timedelta(seconds=1)
                rows.append({
                    "event_id": (day_code * 4096 + index) * 10_000 + position,
                    "operation": SESSION_OPERATION_INSERT,
                    "session_id": str(uuid5(NAMESPACE, f"{household_id}:{appliance}:{started.isoformat()}")),
                    "session_version": 1,
                    "is_deleted": False,
                    "changed_at": changed,
                    "activity_daily_id": activity_daily_id,
                    "household_id": household_id,
                    "appliance_type": appliance,
                    "observation_date": day,
                    "started_at": started,
                    "ended_at": ended,
                    "max_probability": SESSION_MAX_PROBABILITY,
                    "decision_threshold": DECISION_THRESHOLDS[appliance],
                    "updated_at": changed,
                    "batch_id": self.batch_id(day),
                    "batch_kind": SESSION_BATCH_KIND,
                    "schema_version": 1,
                })
        return pa.Table.from_pylist(rows, schema=SESSION_SCHEMA)
