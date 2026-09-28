"""Operational daily job joining power, receipts, delivered sessions, and usage."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import json
from uuid import UUID

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, BooleanType, DateType, DecimalType, IntegerType, LongType,
    StringType, StructField, StructType, TimestampType,
)

from realtime_analysis.models import APPLIANCE_TYPES, LakeBatchRun
from power_silver.analysis_coverage import apply_completion_policy, build_analysis_coverage
from power_silver.appliance_usage import (
    build_appliance_usage_daily,
    restore_latest_sessions,
    session_daily_slices,
    summarize_daily_slices,
)
from power_silver.catalog import SilverCatalog
from power_silver.commit import SilverCommitRepository, dependency_entries, manifest_path, write_manifest
from power_silver.completion import AnalysisCompletionRepository, DailyCompletionRecord
from power_silver.constants import (
    DATASET_ANALYSIS_COVERAGE,
    DATASET_APPLIANCE_USAGE_DAILY,
    DATASET_OBSERVATION,
    DATASET_POWER_CLEAN,
    DATASET_SESSION_SLICES,
)


JOB_NAME = "analysis-usage-daily"

RECEIPT_INPUT_SCHEMA = StructType([
    StructField("receipt_id", StringType()), StructField("message_id", StringType()),
    StructField("analysis_run_id", StringType()), StructField("attempt", IntegerType()),
    StructField("outcome", StringType()), StructField("appliance_types", ArrayType(StringType())),
    StructField("processed_at", TimestampType()), StructField("model_version", StringType()),
    StructField("pipeline_version", StringType()), StructField("state_epoch", StringType()),
    StructField("session_change_refs_json", StringType()), StructField("measured_at", TimestampType()),
])
SESSION_INPUT_SCHEMA = StructType([
    StructField("session_id", StringType()), StructField("session_version", IntegerType()),
    StructField("is_deleted", BooleanType()), StructField("changed_at", TimestampType()),
    StructField("activity_daily_id", StringType()), StructField("household_id", StringType()),
    StructField("appliance_type", StringType()), StructField("observation_date", DateType()),
    StructField("started_at", TimestampType()), StructField("ended_at", TimestampType()),
    StructField("max_probability", DecimalType(5, 4)),
    StructField("decision_threshold", DecimalType(5, 4)),
    StructField("updated_at", TimestampType()), StructField("event_id", LongType()),
])


def _is_confirmed_manifest(path: str) -> bool:
    """두 적재기가 쓰는 확정 표시를 모두 알아본다.

    영수증 lake는 배치 디렉터리마다 ``manifest.json`` 하나를 쓰고, 세션 lake는
    날짜 디렉터리를 공유하며 ``manifest-<batch_id>.json`` 으로 쓴다. 어느 쪽이든
    파일이 확정된 뒤에야 나타나는 이름이므로 둘 다 확정으로 본다.
    """

    name = path.rsplit("/", 1)[-1]
    if name == "manifest.json":
        return True
    return name.startswith("manifest-") and name.endswith(".json")


@dataclass(frozen=True)
class ConfirmedFile:
    path: str
    sha256: str
    length: int
    modification_time: int


@dataclass(frozen=True)
class ConfirmedManifest:
    path: str
    content_sha256: str


@dataclass(frozen=True)
class AnalysisInputSnapshot:
    """One immutable selection of delivered receipts and session changes.

    Confirmed lake files are append-only by contract.  The captured size and
    modification time are checked again before publication so an accidental
    in-place rewrite cannot silently produce a result under the old snapshot id.
    """

    snapshot_id: str
    receipt_snapshot_id: str
    session_snapshot_id: str
    receipt_files: tuple[ConfirmedFile, ...]
    session_files: tuple[ConfirmedFile, ...]
    receipt_manifests: tuple[ConfirmedManifest, ...]
    session_manifests: tuple[ConfirmedManifest, ...]

    @property
    def session_token(self) -> str:
        """Token embedded in the 100-character dataset config_version column."""

        return self.session_snapshot_id[:32]

    def validate(self, storage) -> None:
        for manifest in (*self.receipt_manifests, *self.session_manifests):
            actual = hashlib.sha256(storage.read_bytes(manifest.path)).hexdigest()
            if actual != manifest.content_sha256:
                raise RuntimeError(
                    f"confirmed manifest changed after selection: {manifest.path}"
                )
        for selected in (*self.receipt_files, *self.session_files):
            if not storage.exists(selected.path):
                raise RuntimeError(
                    f"confirmed manifest file is missing: {selected.path}"
                )
            status = storage.status(selected.path)
            if (
                status.length != selected.length
                or status.modification_time != selected.modification_time
            ):
                raise RuntimeError(
                    f"confirmed manifest file changed after selection: {selected.path}"
                )

    def as_dict(self) -> dict:
        def files(items):
            return [
                {
                    "path": item.path,
                    "sha256": item.sha256,
                    "length": item.length,
                    "modification_time": item.modification_time,
                }
                for item in items
            ]

        return {
            "snapshot_id": self.snapshot_id,
            "receipt_snapshot_id": self.receipt_snapshot_id,
            "session_snapshot_id": self.session_snapshot_id,
            "session_token": self.session_token,
            "receipt_manifests": [item.path for item in self.receipt_manifests],
            "session_manifests": [item.path for item in self.session_manifests],
            "receipt_files": files(self.receipt_files),
            "session_files": files(self.session_files),
        }


def _confirmed_selection(storage, manifest_base: str):
    manifests = [
        item.path
        for item in storage.walk_files(manifest_base)
        if _is_confirmed_manifest(item.path)
    ]
    manifest_refs: list[ConfirmedManifest] = []
    files: dict[str, ConfirmedFile] = {}
    for path in sorted(manifests):
        raw = storage.read_bytes(path)
        manifest_refs.append(ConfirmedManifest(path, hashlib.sha256(raw).hexdigest()))
        manifest = json.loads(raw)
        for item in manifest.get("files", []):
            file_path = item["path"]
            if not storage.exists(file_path):
                raise RuntimeError(f"confirmed manifest file is missing: {file_path}")
            status = storage.status(file_path)
            selected = ConfirmedFile(
                path=file_path,
                sha256=str(item.get("sha256") or ""),
                length=status.length,
                modification_time=status.modification_time,
            )
            previous = files.get(file_path)
            if previous is not None and previous.sha256 != selected.sha256:
                raise RuntimeError(
                    f"confirmed manifests disagree about file digest: {file_path}"
                )
            files[file_path] = selected
    ordered_files = tuple(files[path] for path in sorted(files))
    entries = {
        "manifests": [(item.path, item.content_sha256) for item in manifest_refs],
        "files": [
            (item.path, item.sha256, item.length, item.modification_time)
            for item in ordered_files
        ],
    }
    digest = hashlib.sha256(
        json.dumps(entries, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()
    return ordered_files, tuple(manifest_refs), digest


def _confirmed_files(storage, manifest_base: str) -> tuple[list[str], str, int]:
    """Compatibility helper for callers that only need the current selection."""

    files, manifests, digest = _confirmed_selection(storage, manifest_base)
    return [item.path for item in files], digest, len(manifests)


def select_analysis_input_snapshot(settings, *, storage) -> AnalysisInputSnapshot:
    receipt_files, receipt_manifests, receipt_id = _confirmed_selection(
        storage, settings.receipt_manifest_base
    )
    session_files, session_manifests, session_id = _confirmed_selection(
        storage, settings.session_manifest_base
    )
    if not session_manifests:
        raise RuntimeError("no confirmed session manifest; run initial-load first")
    snapshot_id = hashlib.sha256(
        json.dumps(
            {"receipts": receipt_id, "sessions": session_id},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return AnalysisInputSnapshot(
        snapshot_id=snapshot_id,
        receipt_snapshot_id=receipt_id,
        session_snapshot_id=session_id,
        receipt_files=receipt_files,
        session_files=session_files,
        receipt_manifests=receipt_manifests,
        session_manifests=session_manifests,
    )


ANALYSIS_DATASETS = (
    DATASET_ANALYSIS_COVERAGE,
    DATASET_SESSION_SLICES,
    DATASET_APPLIANCE_USAGE_DAILY,
)


def analysis_policy_digest(settings) -> str:
    """Settings that change the output but do not fit in ``config_version``.

    ``config_version`` carries the receipt and session snapshot tokens and is
    limited to 100 characters.  The completion policy is recorded on the run
    instead, so a completed run is only reused when it was produced under the
    same policy as the current request.
    """

    payload = {
        "analysis_run_id": settings.analysis_run_id,
        "minimum_coverage_ratio": settings.analysis_minimum_coverage_ratio,
        "maximum_gap_seconds": settings.analysis_maximum_gap_seconds,
        "quality_policy_version": settings.quality_policy_version,
        "business_utc_offset_seconds": settings.business_utc_offset_seconds,
        "appliance_types": list(APPLIANCE_TYPES),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:16]


def _reusable_manifest(
    storage, session_factory, catalog, target_date: date, run_id: UUID, digest: str
) -> dict | None:
    """The manifest of a completed run that used the same inputs and policy."""

    with session_factory() as session:
        run = session.get(LakeBatchRun, run_id)
    if run is None or (run.details or {}).get("analysis_policy_digest") != digest:
        return None
    ref = catalog.active_version(DATASET_APPLIANCE_USAGE_DAILY, target_date)
    if ref is None or ref.run_id != run_id:
        return None
    manifest = json.loads(storage.read_bytes(ref.manifest_path))
    return {**manifest, "reused_run_id": str(run_id)}


def run_analysis_daily(
    settings,
    target_date: date,
    *,
    storage,
    session_factory,
    spark,
    input_snapshot: AnalysisInputSnapshot | None = None,
    force: bool = False,
) -> dict:
    """Publish (or reuse) the daily coverage, slices and usage for one date.

    A rerun with the same power Silver input, the same confirmed receipt and
    session selection, the same rule and the same completion policy returns the
    completed run's manifest (with ``reused_run_id``) instead of publishing a
    new version.  Without this, every ``gold-profile daily`` rerun would move the
    Gold input snapshot and produce a new profile revision from identical data.
    """

    catalog = SilverCatalog(session_factory)
    power_ref = catalog.active_version(DATASET_POWER_CLEAN, target_date)
    observation_ref = catalog.active_version(DATASET_OBSERVATION, target_date)
    if power_ref is None or observation_ref is None:
        raise RuntimeError(f"active power Silver inputs are missing for {target_date}")
    if power_ref.input_snapshot_id != observation_ref.input_snapshot_id:
        raise RuntimeError("power and observation inputs do not share one snapshot")

    selected = input_snapshot or select_analysis_input_snapshot(settings, storage=storage)
    selected.validate(storage)
    receipt_files = [item.path for item in selected.receipt_files]
    session_files = [item.path for item in selected.session_files]
    receipt_snapshot = selected.receipt_snapshot_id
    session_snapshot = selected.session_snapshot_id

    repository = SilverCommitRepository(session_factory, job_name=JOB_NAME)
    config_version = (
        f"receipts={receipt_snapshot[:32]};sessions={selected.session_token}"
    )
    policy_digest = analysis_policy_digest(settings)
    if not force:
        reusable = repository.completed_run(
            target_date,
            input_snapshot_id=power_ref.input_snapshot_id,
            rule_version=settings.analysis_rule_version,
            config_version=config_version,
            dataset_names=ANALYSIS_DATASETS,
        )
        if reusable is not None:
            manifest = _reusable_manifest(
                storage, session_factory, catalog, target_date, reusable, policy_digest
            )
            if manifest is not None:
                return manifest
    handle = repository.start(
        target_date,
        input_snapshot_id=power_ref.input_snapshot_id,
        rule_version=settings.analysis_rule_version,
        config_version=config_version,
        details={
            "analysis_input_snapshot": selected.as_dict(),
            "analysis_input_snapshot_id": selected.snapshot_id,
            "receipt_snapshot": receipt_snapshot,
            "session_snapshot": session_snapshot,
            "analysis_policy_digest": policy_digest,
        },
    )
    run_id = str(handle.run_id)
    root = f"{settings.analysis_staging_base}/target_date={target_date}/run_id={run_id}"
    outputs = {
        DATASET_ANALYSIS_COVERAGE: f"{settings.silver_base}/{DATASET_ANALYSIS_COVERAGE}/coverage_date={target_date}/run_id={run_id}",
        DATASET_SESSION_SLICES: f"{settings.silver_base}/{DATASET_SESSION_SLICES}/usage_date={target_date}/run_id={run_id}",
        DATASET_APPLIANCE_USAGE_DAILY: f"{settings.gold_base}/{DATASET_APPLIANCE_USAGE_DAILY}/usage_date={target_date}/run_id={run_id}",
    }
    try:
        power = spark.read.parquet(storage.uri(power_ref.output_path))
        observations = spark.read.parquet(storage.uri(observation_ref.output_path))
        receipts = (
            spark.read.parquet(*[storage.uri(path) for path in receipt_files])
            if receipt_files
            else spark.createDataFrame([], RECEIPT_INPUT_SCHEMA)
        )
        receipt_day = receipts.filter(
            (F.col("analysis_run_id") == F.lit(settings.analysis_run_id))
            & (F.to_date(
                F.from_unixtime(
                    F.col("measured_at").cast("long")
                    + F.lit(settings.business_utc_offset_seconds)
                )
            ) == F.lit(target_date))
        )
        session_changes = (
            spark.read.parquet(*[storage.uri(path) for path in session_files])
            if session_files
            else spark.createDataFrame([], SESSION_INPUT_SCHEMA)
        )
        coverage = apply_completion_policy(
            build_analysis_coverage(
                power,
                receipt_day,
                appliance_types=APPLIANCE_TYPES,
                input_snapshot_id=power_ref.input_snapshot_id,
                delivered_session_changes=session_changes,
            ),
            minimum_coverage_ratio=settings.analysis_minimum_coverage_ratio,
            maximum_gap_seconds=settings.analysis_maximum_gap_seconds,
        ).cache()
        slices = session_daily_slices(
            restore_latest_sessions(session_changes), target_date
        ).cache()
        usage = build_appliance_usage_daily(
            observations,
            coverage,
            summarize_daily_slices(slices),
            appliance_types=APPLIANCE_TYPES,
            business_utc_offset_seconds=settings.business_utc_offset_seconds,
        ).cache()

        frames = {
            DATASET_ANALYSIS_COVERAGE: coverage,
            DATASET_SESSION_SLICES: slices,
            DATASET_APPLIANCE_USAGE_DAILY: usage,
        }
        output_meta = {}
        for name, frame in frames.items():
            staging = f"{root}/{name}"
            frame.write.mode("overwrite").parquet(storage.uri(staging))
            row_count = frame.count()
            if storage.exists(outputs[name]):
                storage.delete(outputs[name], recursive=True)
            storage.rename(staging, outputs[name])
            output_meta[name] = {"path": outputs[name], "row_count": row_count}

        manifest = {
            "job": JOB_NAME,
            "schema_version": 1,
            "run_id": run_id,
            "attempt": handle.attempt,
            "target_date": target_date.isoformat(),
            "input_snapshot_id": power_ref.input_snapshot_id,
            "rule_version": settings.analysis_rule_version,
            "config_version": config_version,
            "analysis_evidence_snapshot_id": receipt_snapshot,
            "session_manifest_set_id": session_snapshot,
            "analysis_input_snapshot_id": selected.snapshot_id,
            "analysis_input_snapshot": selected.as_dict(),
            "depends_on": dependency_entries([power_ref, observation_ref]),
            "outputs": output_meta,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
        path = manifest_path(settings.analysis_manifest_base, target_date, run_id)
        manifest["manifest_path"] = path
        # Spark actions above may take minutes.  Refuse to publish if a producer
        # violated the append-only contract while this run was reading the files.
        selected.validate(storage)
        write_manifest(storage, path, manifest)
        repository.publish(manifest, depends_on=[power_ref, observation_ref])

        now = datetime.now(timezone.utc)
        records = []
        for row in usage.collect():
            records.append(
                DailyCompletionRecord(
                    household_id=row.household_id,
                    appliance_type=row.appliance_type,
                    target_date=target_date,
                    input_snapshot_id=power_ref.input_snapshot_id,
                    analysis_run_id=settings.analysis_run_id,
                    analysis_evidence_snapshot_id=receipt_snapshot,
                    session_manifest_set_id=session_snapshot,
                    analysis_status=row.analysis_status or "UNKNOWN",
                    delivery_status=row.delivery_status or "PENDING",
                    coverage_ratio=Decimal(str(row.analysis_coverage_ratio or 0)),
                    max_unanalyzed_seconds=(
                        row.max_unanalyzed_seconds
                        if row.max_unanalyzed_seconds is not None
                        else 86_400
                    ),
                    quality_policy_version=settings.quality_policy_version,
                    baseline_eligible=bool(row.baseline_eligible),
                    completed_at=now,
                )
            )
        AnalysisCompletionRepository(session_factory).publish(records)
        return manifest
    except BaseException as error:
        repository.fail(UUID(run_id), error)
        raise
    finally:
        storage.delete(root, recursive=True)
