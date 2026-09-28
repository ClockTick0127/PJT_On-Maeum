"""Build and atomically publish rolling Gold baseline/statistical profiles."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
from uuid import UUID

from pyspark.sql.types import (
    BooleanType, DateType, IntegerType, LongType, StringType, StructField,
    StructType, TimestampType,
)

from power_silver.catalog import SilverCatalog
from power_silver.commit import (
    LockNotAcquired, SilverCommitRepository, dependency_entries, manifest_path,
    write_manifest,
)
from power_silver.constants import (
    DATASET_APPLIANCE_USAGE_DAILY, DATASET_SESSION_SLICES, RUN_SKIPPED,
    RUN_VALIDATING,
)
from power_silver.writer import write_parquet
from realtime_analysis.models import LakeBatchRun

from gold_profile.delivery import enqueue_payloads
from gold_profile.input_snapshot import build_profile_snapshot, window_dates
from gold_profile.logical_uses import build_logical_uses
from gold_profile.message_contract import build_household_messages
from gold_profile.routine_baseline import build_routine_baselines
from gold_profile.statistical_profile import build_statistical_profiles
from gold_profile.validation import (
    validate_baselines, validate_logical_uses, validate_statistics,
)
from gold_profile.session_snapshot import require_matching_session_outputs


JOB_NAME = "gold-profile"
DATASET_ROUTINE_BASELINE = "routine_baseline_history"
DATASET_LOGICAL_USES = "appliance_logical_uses"
DATASET_STATISTICAL_PROFILE = "household_statistical_profile"
OUTPUT_DATASETS = (
    DATASET_ROUTINE_BASELINE, DATASET_LOGICAL_USES, DATASET_STATISTICAL_PROFILE,
)

USAGE_SCHEMA = StructType([
    StructField("usage_date", DateType()),
    StructField("household_id", StringType()),
    StructField("appliance_type", StringType()),
    StructField("usage_status", StringType()),
    StructField("is_used", BooleanType()),
    StructField("baseline_eligible", BooleanType()),
    StructField("first_use_second", IntegerType()),
    StructField("usage_start_count", LongType()),
    StructField("analysis_status", StringType()),
    StructField("delivery_status", StringType()),
])
SLICE_SCHEMA = StructType([
    StructField("session_id", StringType()),
    StructField("session_version", IntegerType()),
    StructField("household_id", StringType()),
    StructField("appliance_type", StringType()),
    StructField("original_started_at", TimestampType()),
    StructField("original_ended_at", TimestampType()),
    StructField("end_imputed", BooleanType()),
])


@dataclass(frozen=True)
class GoldJobResult:
    status: str
    as_of_date: date
    run_id: str | None = None
    reused_run_id: str | None = None
    incomplete: bool = False


def config_version_of(settings) -> str:
    payload = {
        "window_days": settings.profile_window_days,
        "minimum_sample_days": settings.profile_minimum_sample_days,
        "minimum_weekday_sample_days": settings.profile_minimum_weekday_sample_days,
        "minimum_daily_use_probability": settings.profile_minimum_daily_use_probability,
        "bucket_minutes": settings.profile_time_bucket_minutes,
        "offset": settings.business_utc_offset_seconds,
        "statistic_rule_version": settings.profile_statistic_rule_version,
        # Delivery intent is part of the run identity.  A SHADOW result must
        # never satisfy an ACTIVE request, even when every data input is equal.
        "delivery_mode": settings.profile_delivery_mode,
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    return f"gold-profile-config@{digest[:16]}"


def _read_paths(spark, storage, refs, schema):
    if not refs:
        return spark.createDataFrame([], schema)
    return spark.read.parquet(*[storage.uri(ref.output_path) for ref in refs])


def _paths(settings, as_of_date: date, run_id: str, *, staging: bool) -> dict[str, str]:
    if staging:
        root = f"{settings.profile_staging_base}/as_of_date={as_of_date}/run_id={run_id}"
        return {name: f"{root}/{name}" for name in OUTPUT_DATASETS}
    return {
        name: (
            f"{settings.silver_base if name == DATASET_LOGICAL_USES else settings.gold_base}"
            f"/{name}/as_of_date={as_of_date}/run_id={run_id}"
        )
        for name in OUTPUT_DATASETS
    }


def _revision_of(session_factory, run_id: UUID) -> int:
    with session_factory() as session:
        run = session.get(LakeBatchRun, run_id)
        if run is None:
            raise RuntimeError(f"Gold run is missing: {run_id}")
        return int(run.attempt)


def _repair_reused_delivery(
    settings, as_of_date, run_id, *, storage, session_factory, spark
) -> None:
    repository = SilverCommitRepository(session_factory, job_name=JOB_NAME)
    baseline_ref = repository.active_version(DATASET_ROUTINE_BASELINE, as_of_date)
    statistic_ref = repository.active_version(DATASET_STATISTICAL_PROFILE, as_of_date)
    if baseline_ref is None or statistic_ref is None:
        raise RuntimeError("reused Gold run does not have both active profile components")
    if baseline_ref.run_id != run_id or statistic_ref.run_id != run_id:
        raise RuntimeError("active Gold components do not match the reused run")
    manifest = json.loads(storage.read_bytes(baseline_ref.manifest_path))
    manifest_delivery_mode = manifest.get("delivery_mode", "SHADOW")
    if manifest_delivery_mode != settings.profile_delivery_mode:
        raise RuntimeError(
            "reused Gold run delivery mode does not match the requested mode: "
            f"manifest={manifest_delivery_mode}, requested={settings.profile_delivery_mode}"
        )
    effective_from = datetime.fromisoformat(manifest["effective_from"])
    baseline = spark.read.parquet(storage.uri(baseline_ref.output_path))
    statistics = spark.read.parquet(storage.uri(statistic_ref.output_path))
    payloads = build_household_messages(
        baseline, statistics,
        profile_version=str(run_id), profile_revision=_revision_of(session_factory, run_id),
        # Provenance and content come from the immutable manifest.  Delivery
        # policy comes from the current request after the equality guard above.
        delivery_mode=settings.profile_delivery_mode,
        as_of_date=as_of_date,
        window_start_date=date.fromisoformat(manifest["input"]["window_start_date"]),
        effective_from=effective_from, published_at=effective_from,
        input_snapshot_id=manifest["input_snapshot_id"],
        rule_version=manifest["rule_version"],
        statistic_rule_version=manifest["statistic_rule_version"],
        input_incomplete=bool(manifest.get("input_incomplete")),
    )
    with session_factory.begin() as session:
        enqueue_payloads(session, payloads)


def run_gold_profile(
    settings,
    as_of_date: date,
    *,
    storage,
    session_factory,
    spark,
    now: datetime | None = None,
    force: bool = False,
    input_snapshot=None,
) -> GoldJobResult:
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    repository = SilverCommitRepository(session_factory, job_name=JOB_NAME)
    try:
        lock = repository.date_lock(as_of_date)
        lock.__enter__()
    except LockNotAcquired:
        return GoldJobResult(RUN_SKIPPED, as_of_date)
    try:
        return _run_locked(
            settings, as_of_date, storage=storage, session_factory=session_factory,
            spark=spark, repository=repository, now=now, force=force,
            input_snapshot=input_snapshot,
        )
    finally:
        lock.__exit__(None, None, None)


def _run_locked(
    settings, as_of_date: date, *, storage, session_factory, spark, repository,
    now: datetime, force: bool, input_snapshot=None,
) -> GoldJobResult:
    repository.recover(storage, settings.profile_manifest_base, as_of_date)
    dates = window_dates(as_of_date, settings.profile_window_days)
    catalog = SilverCatalog(session_factory)
    usage_versions = catalog.active_versions(DATASET_APPLIANCE_USAGE_DAILY, dates)
    slice_versions = catalog.active_versions(DATASET_SESSION_SLICES, dates)
    current_snapshot = build_profile_snapshot(
        as_of_date, settings.profile_window_days, usage_versions, slice_versions,
        rule_version=settings.profile_rule_version,
        statistic_rule_version=settings.profile_statistic_rule_version,
        analysis_run_id=settings.analysis_run_id,
        timezone_name=f"UTC{settings.business_utc_offset_seconds:+d}s",
    )
    if input_snapshot is not None:
        if (
            input_snapshot.as_of_date != as_of_date
            or input_snapshot.expected_dates != dates
        ):
            raise ValueError("selected Gold input does not match the requested window")
        if input_snapshot.snapshot_id != current_snapshot.snapshot_id:
            raise RuntimeError(
                "active analysis versions changed after Gold input selection"
            )
        snapshot = input_snapshot
    else:
        snapshot = current_snapshot
    config_version = config_version_of(settings)
    if not force:
        reusable = repository.completed_run(
            as_of_date, input_snapshot_id=snapshot.snapshot_id,
            rule_version=settings.profile_rule_version,
            config_version=config_version, dataset_names=OUTPUT_DATASETS,
        )
        if reusable is not None:
            _repair_reused_delivery(
                settings, as_of_date, reusable, storage=storage,
                session_factory=session_factory, spark=spark,
            )
            return GoldJobResult(
                "SUCCEEDED", as_of_date, reused_run_id=str(reusable),
                incomplete=snapshot.incomplete,
            )

    handle = repository.start(
        as_of_date, input_snapshot_id=snapshot.snapshot_id,
        rule_version=settings.profile_rule_version, config_version=config_version,
        details={"input": snapshot.as_dict(), "shadow_publish": True},
    )
    run_id = str(handle.run_id)
    staging = _paths(settings, as_of_date, run_id, staging=True)
    final = _paths(settings, as_of_date, run_id, staging=False)
    all_refs = [*snapshot.usage_refs, *snapshot.slice_refs]
    try:
        # A tombstone has no slice row.  Version-level provenance must therefore
        # prove that the daily summary and even a zero-row slice were published
        # together from one global session state.
        if set(snapshot.missing_usage_dates) != set(snapshot.missing_slice_dates):
            raise ValueError(
                "daily usage and session slice availability differs by date"
            )
        present_dates = tuple(
            day for day in snapshot.expected_dates
            if day not in set(snapshot.missing_usage_dates)
        )
        require_matching_session_outputs(
            snapshot.usage_refs,
            snapshot.slice_refs,
            present_dates,
        )
        daily_usage = _read_paths(spark, storage, snapshot.usage_refs, USAGE_SCHEMA).cache()
        slices = _read_paths(spark, storage, snapshot.slice_refs, SLICE_SCHEMA).cache()
        business_zone = timezone(timedelta(seconds=settings.business_utc_offset_seconds))
        window_start_at = datetime.combine(
            snapshot.window_start_date, time.min, business_zone
        ).astimezone(timezone.utc)
        window_end_at = datetime.combine(
            as_of_date + timedelta(days=1), time.min, business_zone
        ).astimezone(timezone.utc)
        logical_uses = build_logical_uses(
            slices, run_id=run_id,
            rule_version=settings.profile_statistic_rule_version,
            business_utc_offset_seconds=settings.business_utc_offset_seconds,
            window_start_at=window_start_at,
            window_end_at=window_end_at,
        ).cache()
        baseline = build_routine_baselines(
            daily_usage, as_of_date=as_of_date,
            window_start_date=snapshot.window_start_date,
            input_snapshot_id=snapshot.snapshot_id, profile_version=run_id,
            rule_version=settings.profile_rule_version, effective_from=now,
            input_incomplete=snapshot.incomplete,
            minimum_sample_days=settings.profile_minimum_sample_days,
            minimum_weekday_sample_days=settings.profile_minimum_weekday_sample_days,
            minimum_daily_use_probability=settings.profile_minimum_daily_use_probability,
        ).cache()
        statistics = build_statistical_profiles(
            daily_usage, logical_uses, as_of_date=as_of_date,
            window_start_date=snapshot.window_start_date, profile_version=run_id,
            rule_version=settings.profile_statistic_rule_version,
            input_snapshot_id=snapshot.snapshot_id,
            input_incomplete=snapshot.incomplete,
            effective_from=now,
            bucket_minutes=settings.profile_time_bucket_minutes,
            business_utc_offset_seconds=settings.business_utc_offset_seconds,
        ).cache()
        validate_baselines(baseline, input_incomplete=snapshot.incomplete)
        validate_logical_uses(logical_uses)
        validate_statistics(statistics, input_incomplete=snapshot.incomplete)
        payloads = build_household_messages(
            baseline, statistics,
            profile_version=run_id, profile_revision=handle.attempt,
            delivery_mode=settings.profile_delivery_mode,
            as_of_date=as_of_date, window_start_date=snapshot.window_start_date,
            effective_from=now, published_at=now,
            input_snapshot_id=snapshot.snapshot_id,
            rule_version=settings.profile_rule_version,
            statistic_rule_version=settings.profile_statistic_rule_version,
            input_incomplete=snapshot.incomplete,
        )
        frames = {
            DATASET_ROUTINE_BASELINE: baseline,
            DATASET_LOGICAL_USES: logical_uses,
            DATASET_STATISTICAL_PROFILE: statistics,
        }
        output_meta = {}
        for name, frame in frames.items():
            result = write_parquet(frame, storage, staging[name], partitions=1)
            row_count = spark.read.parquet(storage.uri(staging[name])).count()
            if storage.exists(final[name]):
                storage.delete(final[name], recursive=True)
            storage.rename(staging[name], final[name])
            output_meta[name] = {
                "path": final[name], "row_count": row_count,
                "file_count": result.file_count, "byte_count": result.byte_count,
            }

        repository.set_status(UUID(run_id), RUN_VALIDATING)
        manifest = {
            "job": JOB_NAME, "schema_version": 1, "run_id": run_id,
            "attempt": handle.attempt, "target_date": as_of_date.isoformat(),
            "as_of_date": as_of_date.isoformat(),
            "effective_from": now.isoformat(),
            "input_snapshot_id": snapshot.snapshot_id,
            "rule_version": settings.profile_rule_version,
            "statistic_rule_version": settings.profile_statistic_rule_version,
            "config_version": config_version,
            "delivery_mode": settings.profile_delivery_mode,
            "shadow_publish": settings.profile_delivery_mode == "SHADOW",
            "profile_revision": handle.attempt,
            "risk_score_generated": False,
            "input_incomplete": snapshot.incomplete,
            "components": {
                name: {
                    "status": "INPUT_INCOMPLETE" if snapshot.incomplete else "READY",
                    "profile_version": run_id,
                }
                for name in OUTPUT_DATASETS
            },
            "input": snapshot.as_dict(),
            "depends_on": dependency_entries(all_refs),
            "outputs": output_meta,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
        path = manifest_path(settings.profile_manifest_base, as_of_date, run_id)
        manifest["manifest_path"] = path
        write_manifest(storage, path, manifest)
        repository.publish(
            manifest,
            depends_on=all_refs,
            on_activated=lambda session: enqueue_payloads(session, payloads),
        )
    except BaseException as error:
        repository.fail(handle.run_id, error)
        raise
    finally:
        for frame_name in ("daily_usage", "slices", "logical_uses", "baseline", "statistics"):
            frame = locals().get(frame_name)
            if frame is not None:
                frame.unpersist()
        storage.delete(
            f"{settings.profile_staging_base}/as_of_date={as_of_date}/run_id={run_id}",
            recursive=True,
        )
    return GoldJobResult("SUCCEEDED", as_of_date, run_id=run_id, incomplete=snapshot.incomplete)
