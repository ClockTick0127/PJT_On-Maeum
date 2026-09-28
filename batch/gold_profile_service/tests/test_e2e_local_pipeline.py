"""Local end-to-end run against real Spark, PostgreSQL, Kafka and monitoring.

Bronze power/receipt/session files -> power Silver -> analysis usage-daily ->
28-day-style Gold window (shortened to 3 dates) -> delivery outbox -> Kafka ->
monitoring-service household profile.

This module is skipped unless ``GOLD_E2E_DATABASE_URL`` points at a *disposable*
analysis database (its tables are dropped and recreated).  Optional variables:

``GOLD_E2E_KAFKA_BOOTSTRAP``         broker for the delivery tests (kafka:19092)
``GOLD_E2E_KAFKA_TOPIC``             defaults to gold.household-profile.v1
``GOLD_E2E_MONITORING_DATABASE_URL`` read-only check of the consuming monitoring DB
``GOLD_E2E_HOUSEHOLD_ID``            registered test household (default H901)
``GOLD_E2E_CONTRACT_DUMP``           file path; the first outbox payload is written here

The default ``docker build --target test`` and ``pytest`` runs skip it, so it never
replaces the fast suites.  It exists because FakeStages/FakePublisher tests cannot
prove that the real batches, the outbox, the broker and the consumer connect.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import sessionmaker

from realtime_analysis.database import Base
from realtime_analysis.models import APPLIANCE_TYPES, LakeBatchRun

from power_silver.analysis_job import select_analysis_input_snapshot
from power_silver.catalog import SilverCatalog
from power_silver.commit import SilverCommitRepository
from power_silver.constants import (
    DATASET_APPLIANCE_USAGE_DAILY,
    DATASET_OBSERVATION,
    DATASET_POWER_CLEAN,
    DATASET_SESSION_SLICES,
)
from power_silver.job import run_daily as run_power_silver
from power_silver.storage import LocalLakeStorage
from power_silver.targets import load_targets

from gold_profile.config import GoldProfileSettings
from gold_profile.daily import (
    RUN_INPUT_INCOMPLETE,
    RUN_PUBLISH_PENDING,
    STAGE_ALIGN_WINDOW,
    STAGE_GOLD_PROFILE,
    STAGE_PUBLISH,
    STATUS_SUCCEEDED,
    SparkDailyStages,
    run_daily_pipeline,
)
from gold_profile.delivery import GoldProfileDeliveryOutbox, GoldProfilePublisher
from gold_profile.job import (
    DATASET_LOGICAL_USES, DATASET_ROUTINE_BASELINE, DATASET_STATISTICAL_PROFILE,
    JOB_NAME as GOLD_JOB,
)


pytestmark = [pytest.mark.spark, pytest.mark.e2e]

DATABASE_URL = os.environ.get("GOLD_E2E_DATABASE_URL")
KAFKA = os.environ.get("GOLD_E2E_KAFKA_BOOTSTRAP")
TOPIC = os.environ.get("GOLD_E2E_KAFKA_TOPIC", "gold.household-profile.v1")
MONITORING_URL = os.environ.get("GOLD_E2E_MONITORING_DATABASE_URL")
HOUSEHOLD = os.environ.get("GOLD_E2E_HOUSEHOLD_ID", "H901")
OTHER_HOUSEHOLD = "E2EX02"

if not DATABASE_URL:
    pytest.skip(
        "set GOLD_E2E_DATABASE_URL to a disposable analysis database",
        allow_module_level=True,
    )

KST = timezone(timedelta(hours=9))
OFFSET = 9 * 3600
D1, D2, D3 = date(2026, 9, 15), date(2026, 9, 16), date(2026, 9, 17)
WINDOW = (D1, D2, D3)
INTERVAL = 60
SLOTS_PER_DAY = 86_400 // INTERVAL
RUN_ID = "realtime-v1"


def kst(day: date, hh: int, mm: int = 0, ss: int = 0) -> datetime:
    return datetime.combine(day, time(hh, mm, ss), KST).astimezone(timezone.utc)


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# --- lake writers ---------------------------------------------------------------


def _single_parquet(spark, storage, frame, path: str) -> None:
    directory = path + ".d"
    frame.coalesce(1).write.mode("overwrite").parquet(storage.uri(directory))
    written = [
        item for item in storage.walk_files(directory) if item.path.endswith(".parquet")
    ]
    assert len(written) == 1, written
    storage.rename(written[0].path, path)
    storage.delete(directory, recursive=True)


def write_bronze_power(spark, storage, settings, day: date, household: str, index: int):
    """One Bronze file per household and date, sampled every INTERVAL seconds."""

    from pyspark.sql import functions as F

    from power_silver.schemas import BRONZE_POWER_SCHEMA

    start = int(kst(day, 0).timestamp())
    suffix = household.encode().hex()[:12].ljust(12, "0")
    base_offset = index * 1_000_000
    second = F.lit(start) + F.col("id") * F.lit(INTERVAL)
    frame = (
        spark.range(SLOTS_PER_DAY)
        .select(
            F.concat(
                F.lpad(F.hex(F.col("id") + F.lit(base_offset)), 8, "0"),
                F.lit(f"-0000-4000-8000-{suffix}"),
            ).alias("message_id"),
            F.lit(household).alias("household_id"),
            F.lit("main").alias("device_id"),
            F.concat(
                F.date_format(F.timestamp_seconds(second), "yyyy-MM-dd'T'HH:mm:ss"),
                F.lit("+00:00"),
            ).alias("measured_at"),
            F.lit(120.0).alias("active_power"),
            F.lit(10.0).alias("reactive_power"),
            F.lit(0.9).alias("power_factor"),
            F.lit(1.5).alias("current"),
            F.lit("power.raw.v1").alias("topic"),
            F.lit(0).cast("int").alias("partition"),
            (F.col("id") + F.lit(base_offset)).alias("kafka_offset"),
            F.concat(
                F.date_format(F.timestamp_seconds(second), "yyyy-MM-dd'T'HH:mm:ss"),
                F.lit("Z"),
            ).alias("kafka_ts"),
            F.concat(
                F.date_format(F.timestamp_seconds(second + F.lit(2)), "yyyy-MM-dd'T'HH:mm:ss"),
                F.lit("Z"),
            ).alias("ingested_at"),
        )
        .select(*[field.name for field in BRONZE_POWER_SCHEMA.fields])
    )
    path = (
        f"{settings.bronze_base}/ingest_date={day}/hour=00/partition=0"
        f"/part-{household}.parquet"
    )
    _single_parquet(spark, storage, frame, path)
    day_end = kst(day + timedelta(days=1), 0)
    manifest = {
        "topic": "power.raw.v1",
        "partition": 0,
        "start_offset": base_offset,
        "end_offset": base_offset + SLOTS_PER_DAY - 1,
        "ok_count": SLOTS_PER_DAY,
        "quarantine_count": 0,
        "min_measured_at": kst(day, 0).isoformat(),
        "max_measured_at": (day_end - timedelta(seconds=INTERVAL)).isoformat(),
        "business_dates": [day.isoformat()],
        "file_bytes": storage.status(path).length,
        "files": [path],
        "flush_reason": "e2e",
        "committed_at": (day_end + timedelta(hours=1)).isoformat(),
    }
    storage.write_bytes(
        f"{settings.bronze_manifest_base}/date={day}/manifest-0-{household}.json",
        json.dumps(manifest).encode(),
    )
    return frame.select("message_id", "measured_at")


def write_receipts(spark, storage, settings, day: date, household: str, power, *, until_minute=None):
    """SUCCEEDED receipts for every power input; ``until_minute`` truncates the day."""

    from pyspark.sql import functions as F

    receipts = power.select(
        F.concat(F.lit("r-"), F.col("message_id")).alias("receipt_id"),
        F.col("message_id"),
        F.lit(household).alias("household_id"),
        F.lit("main").alias("device_id"),
        F.lit("power.raw.v1").alias("source_topic"),
        F.lit(0).cast("int").alias("source_partition"),
        F.lit(0).cast("long").alias("source_offset"),
        F.to_timestamp(F.col("measured_at")).alias("measured_at"),
        (F.to_timestamp(F.col("measured_at")) + F.expr("INTERVAL 1 SECOND")).alias("processed_at"),
        F.lit(RUN_ID).alias("analysis_run_id"),
        F.lit(1).cast("int").alias("attempt"),
        F.lit("model-e2e").alias("model_version"),
        F.lit("pipeline-e2e").alias("pipeline_version"),
        F.lit("epoch-1").alias("state_epoch"),
        F.lit("SUCCEEDED").alias("outcome"),
        F.array(*[F.lit(item) for item in APPLIANCE_TYPES]).alias("appliance_types"),
        F.lit("[]").alias("session_change_refs_json"),
        F.lit(None).cast("string").alias("error_type"),
        F.lit(f"batch-{day}-{household}").alias("batch_id"),
        F.lit(1).cast("int").alias("schema_version"),
    )
    if until_minute is not None:
        limit = kst(day, 0) + timedelta(minutes=until_minute)
        receipts = receipts.filter(F.col("measured_at") < F.lit(limit))
    batch = f"{day}-{household}"
    path = f"/nilm/bronze/analysis_receipt/ingest_date={day}/batch_id={batch}/part-00000.parquet"
    _single_parquet(spark, storage, receipts, path)
    data = storage.read_bytes(path)
    manifest = {
        "job": "analysis-receipt-lake-loader",
        "schema_version": 1,
        "batch_id": batch,
        "ingest_date": day.isoformat(),
        "row_count": receipts.count(),
        "files": [{"path": path, "rows": receipts.count(), "bytes": len(data), "sha256": sha256(data)}],
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    storage.write_bytes(
        f"{settings.receipt_manifest_base}/ingest_date={day}/batch_id={batch}/manifest.json",
        json.dumps(manifest).encode(),
    )


SESSION_SCHEMA = (
    "event_id long, operation string, session_id string, session_version int, "
    "is_deleted boolean, changed_at timestamp, activity_daily_id string, "
    "household_id string, appliance_type string, observation_date date, "
    "started_at timestamp, ended_at timestamp, max_probability decimal(5,4), "
    "decision_threshold decimal(5,4), updated_at timestamp, batch_id string, "
    "batch_kind string, schema_version int"
)


def session_row(session_id, version, household, appliance, started, ended, *, deleted=False, event_id=1):
    changed = (ended or started) + timedelta(seconds=1)
    return (
        event_id, "DELETE" if deleted else ("INSERT" if version == 1 else "UPDATE"),
        session_id, version, deleted, changed, f"activity-{session_id}", household,
        appliance, started.astimezone(KST).date(), started, ended,
        Decimal("0.9000"), Decimal("0.5000"), changed, "batch", "INCREMENTAL", 1,
    )


def write_session_batch(spark, storage, settings, batch: str, rows) -> None:
    frame = spark.createDataFrame(rows, SESSION_SCHEMA)
    path = f"/nilm/bronze/appliance_usage_session/ingest_date={D3}/part-{batch}.parquet"
    _single_parquet(spark, storage, frame, path)
    data = storage.read_bytes(path)
    manifest = {
        "job": "session-lake-loader",
        "schema_version": 1,
        "batch_kind": "INCREMENTAL",
        "batch_id": batch,
        "ingest_date": D3.isoformat(),
        "row_count": len(rows),
        "file_count": 1,
        "files": [{
            "path": path, "rows": len(rows), "bytes": len(data),
            "sha256": sha256(data), "content_sha256": sha256(data),
        }],
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    storage.write_bytes(
        f"{settings.session_manifest_base}/ingest_date={D3}/manifest-{batch}.json",
        json.dumps(manifest).encode(),
    )


# --- environment ------------------------------------------------------------------


class Env:
    def __init__(self, spark, root: Path):
        self.spark = spark
        self.root = root
        self.lake_root = root / "lake"
        self.storage = LocalLakeStorage(self.lake_root)
        self.targets_file = root / "observation_targets.json"
        self.targets_file.write_text(json.dumps({
            "config_version": "e2e-targets-v1",
            "targets": [
                {
                    "household_id": household, "device_id": "main",
                    "effective_from": "2026-09-01T00:00:00+09:00", "effective_to": None,
                    "sampling_interval_seconds": INTERVAL, "observation_enabled": True,
                }
                for household in (HOUSEHOLD, OTHER_HOUSEHOLD)
            ],
        }), encoding="utf-8")
        url = make_url(DATABASE_URL)
        self.database = {
            "DATABASE_HOST": url.host, "DATABASE_PORT": str(url.port or 5432),
            "DATABASE_NAME": url.database, "DATABASE_USER": url.username,
            "DATABASE_PASSWORD": url.password or "",
        }
        self.engine = create_engine(DATABASE_URL)
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)
        self.sessions = sessionmaker(bind=self.engine, autoflush=False, expire_on_commit=False)
        self.settings = self.build_settings("SHADOW")
        self.catalog = SilverCatalog(self.sessions)
        self.gold = SilverCommitRepository(self.sessions, job_name=GOLD_JOB)
        self.state: dict = {}

    def build_settings(self, mode: str) -> GoldProfileSettings:
        return GoldProfileSettings(
            lake_local_root=str(self.lake_root),
            observation_targets_file=str(self.targets_file),
            business_utc_offset_seconds=OFFSET,
            spark_master="local[2]",
            profile_window_days=len(WINDOW),
            profile_minimum_sample_days=1,
            profile_minimum_weekday_sample_days=1,
            profile_delivery_mode=mode,
            kafka_bootstrap_servers=KAFKA or "unused:9092",
            profile_kafka_topic=TOPIC,
            profile_publisher_retry_seconds=1,
            **{key.lower(): value for key, value in self.database.items()},
        )

    def cli_env(self, mode: str = "SHADOW") -> dict:
        return {
            **os.environ,
            **self.database,
            "LAKE_LOCAL_ROOT": str(self.lake_root),
            "OBSERVATION_TARGETS_FILE": str(self.targets_file),
            "BUSINESS_UTC_OFFSET_SECONDS": str(OFFSET),
            "SPARK_MASTER": "local[2]",
            "PROFILE_WINDOW_DAYS": str(len(WINDOW)),
            "PROFILE_MINIMUM_SAMPLE_DAYS": "1",
            "PROFILE_MINIMUM_WEEKDAY_SAMPLE_DAYS": "1",
            "PROFILE_DELIVERY_MODE": mode,
            "KAFKA_BOOTSTRAP_SERVERS": KAFKA or "unused:9092",
            "PROFILE_KAFKA_TOPIC": TOPIC,
            "LOG_LEVEL": "WARNING",
        }

    def cli_daily(self, day: date, *extra: str) -> tuple[int, dict]:
        """Run the real ``gold-profile daily`` process; exit code is the contract."""

        completed = subprocess.run(
            [sys.executable, "-m", "gold_profile", "daily", "--as-of", day.isoformat(),
             "--attempts", "1", "--retry-seconds", "0", *extra],
            env=self.cli_env(), capture_output=True, text=True, check=False, timeout=1800,
        )
        stdout = completed.stdout
        report = json.loads(stdout[stdout.index("{"):])
        return completed.returncode, report

    def stages(self, *, publisher=None, settings=None) -> SparkDailyStages:
        return SparkDailyStages(
            settings or self.settings, storage=self.storage, session_factory=self.sessions,
            spark=self.spark, publisher=publisher,
        )

    def daily(self, day: date, *, publisher=None, settings=None, stages=None) -> dict:
        return run_daily_pipeline(
            stages or self.stages(publisher=publisher, settings=settings), day,
            attempts=1, retry_seconds=0, publish=publisher is not None,
            sleep=lambda _seconds: None,
        )

    def power_silver(self, day: date):
        return run_power_silver(
            self.settings, day, storage=self.storage,
            targets=load_targets(self.targets_file),
            repository=SilverCommitRepository(self.sessions), spark=self.spark,
        )

    def usage(self, day: date) -> dict:
        ref = self.catalog.active_version(DATASET_APPLIANCE_USAGE_DAILY, day)
        assert ref is not None, f"no active usage for {day}"
        rows = self.spark.read.parquet(self.storage.uri(ref.output_path)).collect()
        return {(row.household_id, row.appliance_type): row for row in rows}

    def gold_frame(self, dataset: str, day: date):
        ref = self.gold.active_version(dataset, day)
        assert ref is not None, f"no active {dataset} for {day}"
        return ref, self.spark.read.parquet(self.storage.uri(ref.output_path))

    def outbox(self) -> list[GoldProfileDeliveryOutbox]:
        with self.sessions() as session:
            return session.query(GoldProfileDeliveryOutbox).order_by(
                GoldProfileDeliveryOutbox.created_at, GoldProfileDeliveryOutbox.household_id
            ).all()

    def gold_runs(self) -> list[LakeBatchRun]:
        with self.sessions() as session:
            return session.query(LakeBatchRun).filter(
                LakeBatchRun.job_name == GOLD_JOB
            ).order_by(LakeBatchRun.attempt).all()


@pytest.fixture(scope="module")
def env(spark, tmp_path_factory) -> Env:
    root = tmp_path_factory.mktemp("gold-e2e")
    built = Env(spark, root)
    for index, household in enumerate((HOUSEHOLD, OTHER_HOUSEHOLD)):
        for day_index, day in enumerate(WINDOW):
            power = write_bronze_power(
                spark, built.storage, built.settings, day, household,
                index * 10 + day_index,
            )
            until = 720 if (household == OTHER_HOUSEHOLD and day == D2) else None
            write_receipts(spark, built.storage, built.settings, day, household, power, until_minute=until)
    write_session_batch(spark, built.storage, built.settings, "0001", [
        # D1: two kettle uses 30 s apart merge into one logical use.
        session_row("s-d1-kettle-a", 1, HOUSEHOLD, "KETTLE", kst(D1, 7, 30), kst(D1, 7, 31), event_id=1),
        session_row("s-d1-kettle-b", 1, HOUSEHOLD, "KETTLE", kst(D1, 7, 31, 30), kst(D1, 7, 32), event_id=2),
        # D1: a five-second microwave burst is not a use.
        session_row("s-d1-micro", 1, HOUSEHOLD, "MICROWAVE", kst(D1, 12, 0), kst(D1, 12, 0, 5), event_id=3),
        # D2: a morning use and one that crosses midnight into D3.
        session_row("s-d2-kettle", 1, HOUSEHOLD, "KETTLE", kst(D2, 8, 0), kst(D2, 8, 1), event_id=4),
        session_row("s-d2-midnight", 1, HOUSEHOLD, "KETTLE", kst(D2, 23, 59, 30), kst(D3, 0, 0, 40), event_id=5),
        # The other household uses the kettle every day.
        *[
            session_row(f"s-x-{day}", 1, OTHER_HOUSEHOLD, "KETTLE", kst(day, 9, 0), kst(day, 9, 2), event_id=10 + i)
            for i, day in enumerate(WINDOW)
        ],
    ])
    return built


def stage(report: dict, name: str) -> dict:
    return next(item for item in report["stages"] if item["stage"] == name)


# --- scenarios --------------------------------------------------------------------


def test_daily_before_history_is_prepared_exits_input_incomplete(env: Env):
    """The window cannot be aligned when older dates have no power Silver."""

    code, report = env.cli_daily(D3, "--no-publish")

    assert report["status"] == RUN_INPUT_INCOMPLETE
    assert report["ok"] is False
    assert report["exit_code"] == 10
    assert code == 10
    align = stage(report, STAGE_ALIGN_WINDOW)
    assert align["status"] == "PARTIAL"
    assert align["detail"]["aligned"] is False
    assert align["detail"]["skipped_dates"] == [D1.isoformat(), D2.isoformat()]
    # The target date itself was prepared by the real Silver and usage batches.
    assert env.catalog.active_version(DATASET_POWER_CLEAN, D3) is not None
    assert env.catalog.active_version(DATASET_APPLIANCE_USAGE_DAILY, D3) is not None
    # No Gold profile and nothing to deliver.
    assert env.gold.active_version(DATASET_ROUTINE_BASELINE, D3) is None
    assert env.outbox() == []


def test_prepared_window_builds_a_ready_shadow_profile_and_hands_off_the_outbox(env: Env):
    for day in (D1, D2):
        result = env.power_silver(day)
        assert result.status == "SUCCEEDED", result

    code, report = env.cli_daily(D3, "--no-publish")

    assert report["status"] == RUN_PUBLISH_PENDING
    assert report["exit_code"] == 12
    assert code == 12
    align = stage(report, STAGE_ALIGN_WINDOW)["detail"]
    assert align["aligned"] is True
    assert align["rebuilt_dates"] == [D1.isoformat(), D2.isoformat()]
    assert len(set(align["tokens_by_date"].values())) == 1
    gold = stage(report, STAGE_GOLD_PROFILE)
    assert gold["status"] == STATUS_SUCCEEDED
    assert gold["detail"]["incomplete"] is False
    assert gold["detail"]["reused_run_id"] is None
    assert stage(report, STAGE_PUBLISH)["status"] == "SKIPPED"
    env.state["shadow_run"] = gold["detail"]["run_id"]
    env.state["token"] = next(iter(align["tokens_by_date"].values()))

    # Silver -> usage facts: merge, short use, midnight crossing, no-use day.
    d1, d2, d3 = env.usage(D1), env.usage(D2), env.usage(D3)
    kettle_d1 = d1[(HOUSEHOLD, "KETTLE")]
    assert kettle_d1.usage_status == "USED"
    assert kettle_d1.usage_start_count == 1
    assert kettle_d1.logical_use_count == 1
    assert kettle_d1.first_use_second == 7 * 3600 + 30 * 60
    micro_d1 = d1[(HOUSEHOLD, "MICROWAVE")]
    assert micro_d1.usage_status == "NOT_USED"
    assert micro_d1.rejected_short_use_count == 1
    kettle_d2 = d2[(HOUSEHOLD, "KETTLE")]
    assert kettle_d2.usage_status == "USED"
    assert kettle_d2.usage_start_count == 2
    kettle_d3 = d3[(HOUSEHOLD, "KETTLE")]
    # The tail of yesterday's use is activity on D3 but not a new daily start.
    assert kettle_d3.usage_status == "USED"
    assert kettle_d3.logical_use_count == 1
    assert kettle_d3.usage_start_count == 0
    # Observed all day with no activity at all: NOT_USED, and still a baseline sample.
    micro_d3 = d3[(HOUSEHOLD, "MICROWAVE")]
    assert micro_d3.usage_status == "NOT_USED"
    assert micro_d3.is_used is False
    assert micro_d3.baseline_eligible is True
    assert micro_d3.logical_use_count == 0
    assert micro_d3.rejected_short_use_count == 0
    # Half a day without receipts is UNKNOWN, never NOT_USED.
    other_d2 = d2[(OTHER_HOUSEHOLD, "KETTLE")]
    assert other_d2.usage_status == "UNKNOWN"
    assert other_d2.baseline_eligible is False
    assert other_d2.analysis_status == "INCOMPLETE"

    # Gold: one atomic run, complete cross-day episode, baselines from eligible days.
    refs = [env.gold_frame(name, D3)[0] for name in (
        DATASET_ROUTINE_BASELINE, DATASET_LOGICAL_USES, DATASET_STATISTICAL_PROFILE,
    )]
    assert {str(ref.run_id) for ref in refs} == {env.state["shadow_run"]}
    _, uses = env.gold_frame(DATASET_LOGICAL_USES, D3)
    episodes = {
        (row.household_id, row.appliance_type, row.quality_status, row.crosses_midnight)
        for row in uses.collect()
    }
    assert (HOUSEHOLD, "KETTLE", "VALID", True) in episodes
    assert (HOUSEHOLD, "MICROWAVE", "TOO_SHORT", False) in episodes
    midnight = [
        row for row in uses.filter("crosses_midnight").collect()
        if row.household_id == HOUSEHOLD
    ]
    assert len(midnight) == 1
    assert midnight[0].active_duration_us == 70_000_000
    _, baseline = env.gold_frame(DATASET_ROUTINE_BASELINE, D3)
    overall = {
        (row.household_id, row.appliance_type): row
        for row in baseline.filter("baseline_scope = 'OVERALL'").collect()
    }
    kettle = overall[(HOUSEHOLD, "KETTLE")]
    # D3 counts as active through the midnight tail; the daily summary says so too.
    assert (kettle.sample_days, kettle.active_days) == (3, 3)
    assert kettle.quality_status == "READY"
    assert kettle.enabled is True
    microwave = overall[(HOUSEHOLD, "MICROWAVE")]
    # Three observed days, one rejected burst: a retained but disabled baseline.
    assert (microwave.sample_days, microwave.active_days) == (3, 0)
    assert microwave.quality_status == "NO_USAGE_HISTORY"
    assert microwave.enabled is False
    other = overall[(OTHER_HOUSEHOLD, "KETTLE")]
    # The UNKNOWN day is not a sample.
    assert (other.sample_days, other.active_days) == (2, 2)

    # Outbox: one READY SHADOW message per household, not yet sent.
    rows = env.outbox()
    assert [(row.household_id, row.delivery_mode, row.status, row.profile_revision) for row in rows] == [
        (OTHER_HOUSEHOLD, "SHADOW", "PENDING", 1),
        (HOUSEHOLD, "SHADOW", "PENDING", 1),
    ]
    payload = next(row.payload for row in rows if row.household_id == HOUSEHOLD)
    assert payload["schema_version"] == 2
    assert payload["profile_version"] == env.state["shadow_run"]
    assert payload["quality_status"] == "READY"
    assert payload["as_of_date"] == D3.isoformat()
    assert payload["window_start_date"] == D1.isoformat()
    assert {row["metric_name"] for row in payload["statistics"]} >= {
        "CUMULATIVE_ACTIVITY_START_COUNT", "INACTIVITY_ELAPSED", "LOGICAL_USE_ACTIVE_DURATION",
    }
    assert all(len(row["time_bucket"]) == 5 for row in payload["statistics"] if row["time_bucket"])
    dump = os.environ.get("GOLD_E2E_CONTRACT_DUMP")
    if dump:
        Path(dump).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def test_rerunning_the_same_day_reuses_the_gold_run(env: Env):
    report = env.daily(D3)

    assert report["status"] == RUN_PUBLISH_PENDING
    gold = stage(report, STAGE_GOLD_PROFILE)["detail"]
    assert gold["reused_run_id"] == env.state["shadow_run"]
    assert gold["run_id"] is None
    assert stage(report, STAGE_ALIGN_WINDOW)["detail"]["rebuilt_dates"] == []
    assert len(env.outbox()) == 2
    assert len(env.gold_runs()) == 1


@pytest.mark.skipif(not KAFKA, reason="set GOLD_E2E_KAFKA_BOOTSTRAP for delivery tests")
def test_broker_outage_leaves_rows_pending_and_the_publisher_delivers_later(env: Env):
    from confluent_kafka import Producer

    down = GoldProfilePublisher(
        env.settings, env.sessions,
        producer=Producer({
            "bootstrap.servers": "kafka-down.invalid:19092",
            "enable.idempotence": True, "acks": "all",
            "message.timeout.ms": 3000, "socket.timeout.ms": 1000,
        }),
    )
    assert down.publish_pending() == (0, 2)
    rows = env.outbox()
    assert all(row.status == "PENDING" and row.attempt_count == 1 for row in rows)
    assert all(row.last_error for row in rows)

    later = datetime.now(timezone.utc) + timedelta(seconds=2)
    publisher = GoldProfilePublisher(env.settings, env.sessions)
    assert publisher.publish_pending(now=later) == (2, 0)
    assert publisher.publish_pending(now=later) == (0, 0)
    rows = env.outbox()
    assert all(row.status == "PUBLISHED" and row.attempt_count == 2 for row in rows)

    message = consume_profile(HOUSEHOLD, env.state["shadow_run"])
    assert message["delivery_mode"] == "SHADOW"
    assert message["quality_status"] == "READY"

    if MONITORING_URL:
        stored = wait_for_monitoring(HOUSEHOLD, env.state["shadow_run"])
        assert stored == ("SHADOW", "SHADOW")


def test_session_delete_reaggregates_history_and_a_late_manifest_waits_for_the_next_run(env: Env):
    # The D2 kettle uses are deleted after the fact.
    write_session_batch(env.spark, env.storage, env.settings, "0002", [
        session_row("s-d2-kettle", 2, HOUSEHOLD, "KETTLE", kst(D2, 8, 0), kst(D2, 8, 1), deleted=True, event_id=20),
        session_row("s-d2-midnight", 2, HOUSEHOLD, "KETTLE", kst(D2, 23, 59, 30), kst(D3, 0, 0, 40), deleted=True, event_id=21),
    ])
    expected_token = select_analysis_input_snapshot(env.settings, storage=env.storage).session_token
    assert expected_token != env.state["token"]

    class LateManifest(SparkDailyStages):
        """A new session batch lands while the window is being re-aggregated."""

        calls = 0

        def usage_daily(self, day, input_snapshot=None):
            LateManifest.calls += 1
            if LateManifest.calls == 2:
                write_session_batch(env.spark, env.storage, env.settings, "0003", [
                    session_row("s-d1-micro-late", 1, HOUSEHOLD, "MICROWAVE", kst(D1, 18, 0), kst(D1, 18, 3), event_id=30),
                ])
            return super().usage_daily(day, input_snapshot)

    stages = LateManifest(
        env.settings, storage=env.storage, session_factory=env.sessions,
        spark=env.spark, publisher=None,
    )
    report = env.daily(D3, stages=stages)

    assert report["status"] == RUN_PUBLISH_PENDING
    align = stage(report, STAGE_ALIGN_WINDOW)["detail"]
    assert set(align["rebuilt_dates"]) == {D1.isoformat(), D2.isoformat()}
    assert set(align["tokens_by_date"].values()) == {expected_token}
    assert LateManifest.calls >= 2
    gold = stage(report, STAGE_GOLD_PROFILE)["detail"]
    assert gold["run_id"] not in (None, env.state["shadow_run"])
    env.state["deleted_run"] = gold["run_id"]
    # The window that was aggregated is the fixed one, not the manifest that arrived mid-run.
    assert env.usage(D2)[(HOUSEHOLD, "KETTLE")].usage_status == "NOT_USED"
    kettle_d3 = env.usage(D3)[(HOUSEHOLD, "KETTLE")]
    assert kettle_d3.usage_status == "NOT_USED"
    assert kettle_d3.logical_use_count == 0
    assert env.usage(D1)[(HOUSEHOLD, "MICROWAVE")].usage_status == "NOT_USED"
    _, baseline = env.gold_frame(DATASET_ROUTINE_BASELINE, D3)
    kettle = baseline.filter(
        f"baseline_scope = 'OVERALL' and household_id = '{HOUSEHOLD}' and appliance_type = 'KETTLE'"
    ).collect()[0]
    assert (kettle.sample_days, kettle.active_days) == (3, 1)
    runs = env.gold_runs()
    assert [run.attempt for run in runs] == [1, 2]
    rows = env.outbox()
    assert [(row.profile_revision, row.status) for row in rows if row.household_id == HOUSEHOLD] == [
        (1, "PUBLISHED" if KAFKA else "PENDING"), (2, "PENDING"),
    ]

    # The late manifest is picked up by the next run, which rebuilds every date again.
    latest_token = select_analysis_input_snapshot(env.settings, storage=env.storage).session_token
    assert latest_token != expected_token
    report = env.daily(D3)
    align = stage(report, STAGE_ALIGN_WINDOW)["detail"]
    assert set(align["rebuilt_dates"]) == {D1.isoformat(), D2.isoformat()}
    assert set(align["tokens_by_date"].values()) == {latest_token}
    assert env.usage(D1)[(HOUSEHOLD, "MICROWAVE")].usage_status == "USED"
    assert [run.attempt for run in env.gold_runs()] == [1, 2, 3]
    env.state["late_run"] = stage(report, STAGE_GOLD_PROFILE)["detail"]["run_id"]


@pytest.mark.skipif(not KAFKA, reason="set GOLD_E2E_KAFKA_BOOTSTRAP for delivery tests")
def test_active_mode_is_a_new_version_that_monitoring_promotes(env: Env):
    active = env.build_settings("ACTIVE")
    publisher = GoldProfilePublisher(active, env.sessions)

    report = env.daily(D3, publisher=publisher, settings=active)

    assert report["status"] == STATUS_SUCCEEDED
    assert report["ok"] is True
    gold = stage(report, STAGE_GOLD_PROFILE)["detail"]
    assert gold["reused_run_id"] is None
    assert gold["run_id"] not in (env.state["shadow_run"], env.state["deleted_run"], env.state["late_run"])
    publish = stage(report, STAGE_PUBLISH)["detail"]
    assert publish["outbox_messages_failed"] == 0
    # Both households' ACTIVE rows plus the SHADOW rows left PENDING by the two rebuilds.
    assert publish["outbox_messages_published"] == 6
    rows = env.outbox()
    assert all(row.status == "PUBLISHED" for row in rows)
    mine = [(row.profile_revision, row.delivery_mode) for row in rows if row.household_id == HOUSEHOLD]
    assert mine == [(1, "SHADOW"), (2, "SHADOW"), (3, "SHADOW"), (4, "ACTIVE")]

    message = consume_profile(HOUSEHOLD, gold["run_id"])
    assert message["delivery_mode"] == "ACTIVE"
    assert message["profile_revision"] == 4

    # Repeating the ACTIVE request reuses the run and does not resend.
    again = env.daily(D3, publisher=publisher, settings=active)
    assert stage(again, STAGE_GOLD_PROFILE)["detail"]["reused_run_id"] == gold["run_id"]
    assert stage(again, STAGE_PUBLISH)["detail"]["outbox_messages_published"] == 0

    if MONITORING_URL:
        assert wait_for_monitoring(HOUSEHOLD, gold["run_id"]) == ("ACTIVE", "ACTIVE")
        assert wait_for_monitoring(HOUSEHOLD, env.state["shadow_run"]) == ("SHADOW", "SHADOW")
        # Unregistered households are ignored by the consumer.
        assert monitoring_row(OTHER_HOUSEHOLD, gold["run_id"]) is None


# --- delivery observers -----------------------------------------------------------


def consume_profile(household: str, profile_version: str, timeout_seconds: int = 90) -> dict:
    from confluent_kafka import Consumer

    consumer = Consumer({
        "bootstrap.servers": KAFKA,
        "group.id": f"gold-e2e-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])
    deadline = datetime.now(timezone.utc) + timedelta(seconds=timeout_seconds)
    try:
        while datetime.now(timezone.utc) < deadline:
            record = consumer.poll(1.0)
            if record is None or record.error():
                continue
            if record.key() != household.encode():
                continue
            payload = json.loads(record.value())
            if payload.get("profile_version") == profile_version:
                return payload
    finally:
        consumer.close()
    raise AssertionError(f"profile {profile_version} for {household} was not seen on {TOPIC}")


def monitoring_row(household: str, profile_version: str):
    engine = create_engine(MONITORING_URL)
    with engine.connect() as connection:
        return connection.execute(
            text(
                "select status, delivery_mode from household_profiles "
                "where household_id = :household and profile_version = :version"
            ),
            {"household": household, "version": profile_version},
        ).first()


def wait_for_monitoring(household: str, profile_version: str, timeout_seconds: int = 90):
    import time as clock

    deadline = clock.monotonic() + timeout_seconds
    while clock.monotonic() < deadline:
        row = monitoring_row(household, profile_version)
        if row is not None:
            return (row[0], row[1])
        clock.sleep(2)
    raise AssertionError(f"monitoring did not store {profile_version} for {household}")
