"""analysis-usage-daily on a local Spark: publish once, reuse on identical reruns."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json

import pytest
from sqlalchemy import func, select

from realtime_analysis.models import APPLIANCE_TYPES, LakeBatchRun, LakeDatasetVersion

from power_silver.analysis_job import JOB_NAME, run_analysis_daily
from power_silver.catalog import SilverCatalog
from power_silver.commit import SilverCommitRepository
from power_silver.constants import DATASET_APPLIANCE_USAGE_DAILY
from power_silver.job import run_daily
from power_silver.storage import LocalLakeStorage
from power_silver.targets import load_targets

from conftest import DAY_END_EPOCH, DAY_START_EPOCH, TARGET_DATE, bronze_rows, write_bronze


pytestmark = pytest.mark.spark

AFTER_THE_DAY = datetime.fromtimestamp(DAY_END_EPOCH + 3600, timezone.utc)
SESSION_SCHEMA = (
    "event_id long, operation string, session_id string, session_version int, "
    "is_deleted boolean, changed_at timestamp, activity_daily_id string, "
    "household_id string, appliance_type string, observation_date date, "
    "started_at timestamp, ended_at timestamp, max_probability decimal(5,4), "
    "decision_threshold decimal(5,4), updated_at timestamp"
)


def _single_parquet(spark, storage: LocalLakeStorage, frame, path: str) -> bytes:
    directory = path + ".d"
    frame.coalesce(1).write.mode("overwrite").parquet(storage.uri(directory))
    written = [item for item in storage.walk_files(directory) if item.path.endswith(".parquet")]
    assert len(written) == 1, written
    storage.rename(written[0].path, path)
    storage.delete(directory, recursive=True)
    return storage.read_bytes(path)


def _write_receipts(spark, storage, settings, power_rows) -> None:
    rows = [
        (
            f"r-{row['message_id']}", row["message_id"], "realtime-v1", 1, "SUCCEEDED",
            list(APPLIANCE_TYPES),
            datetime.fromisoformat(row["measured_at"]) + timedelta(seconds=1),
            "model", "pipeline", "epoch", "[]",
            datetime.fromisoformat(row["measured_at"]),
        )
        for row in power_rows
    ]
    frame = spark.createDataFrame(
        rows,
        "receipt_id string, message_id string, analysis_run_id string, attempt int, "
        "outcome string, appliance_types array<string>, processed_at timestamp, "
        "model_version string, pipeline_version string, state_epoch string, "
        "session_change_refs_json string, measured_at timestamp",
    )
    path = "/nilm/bronze/analysis_receipt/ingest_date=2026-09-19/batch_id=b1/part-00000.parquet"
    data = _single_parquet(spark, storage, frame, path)
    storage.write_bytes(
        f"{settings.receipt_manifest_base}/ingest_date=2026-09-19/batch_id=b1/manifest.json",
        json.dumps({
            "job": "analysis-receipt-lake-loader", "batch_id": "b1",
            "files": [{"path": path, "rows": len(rows), "bytes": len(data),
                       "sha256": hashlib.sha256(data).hexdigest()}],
        }).encode(),
    )


def _write_sessions(spark, storage, settings, batch: str, rows) -> None:
    frame = spark.createDataFrame(rows, SESSION_SCHEMA)
    path = f"/nilm/bronze/appliance_usage_session/ingest_date=2026-09-19/part-{batch}.parquet"
    data = _single_parquet(spark, storage, frame, path)
    storage.write_bytes(
        f"{settings.session_manifest_base}/ingest_date=2026-09-19/manifest-{batch}.json",
        json.dumps({
            "job": "session-lake-loader", "batch_id": batch,
            "files": [{"path": path, "rows": len(rows), "bytes": len(data),
                       "sha256": hashlib.sha256(data).hexdigest()}],
        }).encode(),
    )


def _session(session_id, version, started, ended, *, deleted=False, event_id=1):
    return (
        event_id, "DELETE" if deleted else "INSERT", session_id, version, deleted,
        ended + timedelta(seconds=1), "activity", "H001", "KETTLE", TARGET_DATE,
        started, ended, Decimal("0.9000"), Decimal("0.5000"), ended,
    )


def _runs(session_factory) -> int:
    with session_factory() as session:
        return session.execute(
            select(func.count()).select_from(LakeBatchRun).where(LakeBatchRun.job_name == JOB_NAME)
        ).scalar_one()


def _versions(session_factory) -> int:
    with session_factory() as session:
        return session.execute(
            select(func.count()).select_from(LakeDatasetVersion).where(
                LakeDatasetVersion.dataset_name == DATASET_APPLIANCE_USAGE_DAILY
            )
        ).scalar_one()


def test_an_identical_rerun_reuses_the_completed_usage_run(spark, lake, settings, session_factory):
    power_rows = bronze_rows(household_id="H001", seconds=range(DAY_START_EPOCH, DAY_START_EPOCH + 300))
    write_bronze(spark, lake, settings, power_rows)
    silver = run_daily(
        settings, TARGET_DATE, storage=lake, targets=load_targets(settings.observation_targets_file),
        repository=SilverCommitRepository(session_factory), spark=spark, now=AFTER_THE_DAY,
    )
    assert silver.status == "SUCCEEDED"
    _write_receipts(spark, lake, settings, power_rows)
    start = datetime.fromtimestamp(DAY_START_EPOCH + 3600, timezone.utc)
    _write_sessions(spark, lake, settings, "0001", [
        _session("s1", 1, start, start + timedelta(minutes=1)),
    ])

    first = run_analysis_daily(
        settings, TARGET_DATE, storage=lake, session_factory=session_factory, spark=spark,
    )
    second = run_analysis_daily(
        settings, TARGET_DATE, storage=lake, session_factory=session_factory, spark=spark,
    )

    # Same power input, same confirmed receipts/sessions, same rule and policy: no new run.
    assert "reused_run_id" not in first
    assert second["reused_run_id"] == first["run_id"]
    assert second["run_id"] == first["run_id"]
    assert second["config_version"] == first["config_version"]
    assert _runs(session_factory) == 1
    assert _versions(session_factory) == 1
    catalog = SilverCatalog(session_factory)
    assert str(catalog.active_version(DATASET_APPLIANCE_USAGE_DAILY, TARGET_DATE).run_id) == first["run_id"]

    # A changed completion policy is a different computation even with equal inputs.
    stricter = settings.model_copy(
        update={"analysis_maximum_gap_seconds": settings.analysis_maximum_gap_seconds + 1}
    )
    third = run_analysis_daily(
        stricter, TARGET_DATE, storage=lake, session_factory=session_factory, spark=spark,
    )
    assert "reused_run_id" not in third
    assert third["run_id"] != first["run_id"]
    assert _runs(session_factory) == 2

    # A new confirmed session manifest moves the selected input and is recomputed.
    _write_sessions(spark, lake, settings, "0002", [
        _session("s1", 2, start, start + timedelta(minutes=1), deleted=True, event_id=2),
    ])
    fourth = run_analysis_daily(
        stricter, TARGET_DATE, storage=lake, session_factory=session_factory, spark=spark,
    )
    assert "reused_run_id" not in fourth
    assert fourth["config_version"] != third["config_version"]
    assert _runs(session_factory) == 3

    # force always recomputes.
    forced = run_analysis_daily(
        stricter, TARGET_DATE, storage=lake, session_factory=session_factory, spark=spark,
        force=True,
    )
    assert "reused_run_id" not in forced
    assert forced["run_id"] != fourth["run_id"]
    assert _runs(session_factory) == 4
