"""power-silver-daily 실행 흐름.

    입력 확정 → 검증·정규화 → 중복 제거 → 측정일별 분류 → 관측일 계산
      → 파일 병합 저장 → 검증 → 최종 경로로 이동 → 완료 manifest → 활성 버전 반영

이 단계는 가전별 사용 판정·사용 세션 적재·baseline 계산을 하지 않는다. 후속 루틴 집계가
여기서 만든 관측일과 Silver 사용 세션을 결합한다.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
import logging
from uuid import UUID

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from power_silver.commit import (
    LockNotAcquired,
    SilverCommitRepository,
    manifest_path,
    write_manifest,
)
from power_silver.constants import (
    DATASET_OBSERVATION,
    DATASET_POWER_CLEAN,
    JOB_NAME,
    RUN_SKIPPED,
    RUN_VALIDATING,
    RUN_WAITING_INPUT,
    SECONDS_PER_DAY,
)
from power_silver.deduplicate import STATE_CLEAN, STATE_DUPLICATE, STATE_REJECTED, classify
from power_silver.input_snapshot import InputSnapshot, build_snapshot, verify_unchanged
from power_silver.normalize import normalize, read_bronze
from power_silver.observation import observation_daily, segments_frame
from power_silver.schemas import POWER_CLEAN_SCHEMA, QUARANTINE_SCHEMA
from power_silver.storage import LakeStorage
from power_silver.targets import ObservationTargets
from power_silver.validation import validate_observation, validate_power_clean
from power_silver.writer import plan_partitions, write_parquet


logger = logging.getLogger(__name__)

DATASET_NAMES = (DATASET_POWER_CLEAN, DATASET_OBSERVATION)


@dataclass
class JobResult:
    status: str
    target_date: date
    run_id: str | None = None
    reused_run_id: str | None = None
    wait_reason: str | None = None
    manifest: dict = field(default_factory=dict)
    recovered_runs: int = 0

    @property
    def published(self) -> bool:
        return self.status == "SUCCEEDED" and not self.reused_run_id


def config_version_of(targets: ObservationTargets) -> str:
    """이름만으로는 내용 변경을 알 수 없으므로 지문을 붙인다."""

    return f"{targets.config_version}@{targets.fingerprint[:12]}"


def _output_paths(settings, target_date: date, run_id: str) -> dict[str, str]:
    day = target_date.isoformat()
    return {
        DATASET_POWER_CLEAN: (
            f"{settings.silver_base}/{DATASET_POWER_CLEAN}"
            f"/event_date={day}/run_id={run_id}"
        ),
        DATASET_OBSERVATION: (
            f"{settings.silver_base}/{DATASET_OBSERVATION}"
            f"/observation_date={day}/run_id={run_id}"
        ),
        "quarantine": f"{settings.quarantine_base}/target_date={day}/run_id={run_id}",
    }


def _staging_paths(settings, target_date: date, run_id: str) -> dict[str, str]:
    base = (
        f"{settings.staging_base}/target_date={target_date.isoformat()}/run_id={run_id}"
    )
    return {name: f"{base}/{name}" for name in (*DATASET_NAMES, "quarantine")}


def _row_state_stats(labelled: DataFrame, target_date: date) -> dict:
    rows = (
        labelled.groupBy("row_state", "reject_code")
        .agg(
            F.count(F.lit(1)).alias("count"),
            F.sum(
                F.when(F.col("event_date") == F.lit(target_date), 1).otherwise(0)
            ).alias("target_date_count"),
            F.sum(F.when(F.col("household_id").isNull(), 1).otherwise(0)).alias(
                "unattributable_count"
            ),
        )
        .collect()
    )
    stats: dict = {
        "input_rows": 0,
        "clean_rows": 0,
        "clean_rows_for_target_date": 0,
        "duplicate_rows": 0,
        "rejected_rows": 0,
        "rows_for_other_dates": 0,
        "unattributable_rows": 0,
        "reject_codes": {},
    }
    for row in rows:
        count = int(row["count"])
        stats["input_rows"] += count
        stats["rows_for_other_dates"] += count - int(row["target_date_count"])
        stats["unattributable_rows"] += int(row["unattributable_count"])
        if row["row_state"] == STATE_CLEAN:
            stats["clean_rows"] += count
            stats["clean_rows_for_target_date"] += int(row["target_date_count"])
        elif row["row_state"] == STATE_DUPLICATE:
            stats["duplicate_rows"] += count
        elif row["row_state"] == STATE_REJECTED:
            stats["rejected_rows"] += count
            code = row["reject_code"] or "UNKNOWN"
            stats["reject_codes"][code] = stats["reject_codes"].get(code, 0) + count
    return stats


def _expected_by_household(targets: ObservationTargets, target_date: date, offset: int):
    segments = targets.segments_for_date(target_date, offset)
    expected: dict[str, int] = defaultdict(int)
    covered: dict[str, int] = defaultdict(int)
    devices: dict[str, set[str]] = defaultdict(set)
    for segment in segments:
        expected[segment.household_id] += segment.expected_sample_count
        covered[segment.household_id] += segment.end_second - segment.start_second
        devices[segment.household_id].add(segment.device_id)
    partial = frozenset(
        household_id
        for household_id, seconds in covered.items()
        if 0 < seconds < SECONDS_PER_DAY
    )
    multi_device = frozenset(
        household_id for household_id, used in devices.items() if len(used) > 1
    )
    return segments, dict(expected), partial, multi_device


def run_daily(
    settings,
    target_date: date,
    *,
    storage: LakeStorage,
    targets: ObservationTargets,
    repository: SilverCommitRepository,
    spark: SparkSession,
    now: datetime | None = None,
    force: bool = False,
) -> JobResult:
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    config_version = config_version_of(targets)

    # 같은 날짜를 두 실행이 동시에 쓰면 시도 번호 채번이 경쟁하고 결과가 중복 계산된다.
    # 시작·검증·확정·복구를 모두 이 잠금 안에서 한다.
    lock = repository.date_lock(target_date)
    try:
        lock.__enter__()
    except LockNotAcquired:
        logger.info("Another writer already owns %s", target_date)
        return JobResult(
            status=RUN_SKIPPED,
            target_date=target_date,
            wait_reason="ANOTHER_WRITER",
        )
    try:
        return _run_locked(
            settings,
            target_date,
            storage=storage,
            targets=targets,
            repository=repository,
            spark=spark,
            now=now,
            force=force,
            config_version=config_version,
        )
    finally:
        lock.__exit__(None, None, None)


def _run_locked(
    settings,
    target_date: date,
    *,
    storage: LakeStorage,
    targets: ObservationTargets,
    repository: SilverCommitRepository,
    spark: SparkSession,
    now: datetime,
    force: bool,
    config_version: str,
) -> JobResult:
    # 파일은 확정됐는데 DB 반영 전에 죽은 실행이 있으면 먼저 되살린다.
    recovered = repository.recover(storage, settings.manifest_base, target_date)

    snapshot = build_snapshot(storage, settings, target_date, now=now)
    if not snapshot.ready and not force:
        handle = repository.start(
            target_date,
            input_snapshot_id=snapshot.snapshot_id,
            rule_version=settings.rule_version,
            config_version=config_version,
            status=RUN_WAITING_INPUT,
            details={"wait_reason": snapshot.wait_reason, "input": snapshot.as_dict()},
        )
        repository.finish(handle.run_id, RUN_WAITING_INPUT)
        logger.info(
            "Input is not ready for %s: %s", target_date, snapshot.wait_reason
        )
        return JobResult(
            status=RUN_WAITING_INPUT,
            target_date=target_date,
            run_id=str(handle.run_id),
            wait_reason=snapshot.wait_reason,
            recovered_runs=recovered,
        )

    if not force:
        reusable = repository.completed_run(
            target_date,
            input_snapshot_id=snapshot.snapshot_id,
            rule_version=settings.rule_version,
            config_version=config_version,
            dataset_names=DATASET_NAMES,
        )
        if reusable is not None:
            logger.info(
                "Reusing silver run %s for %s (same input, rule and config)",
                reusable,
                target_date,
            )
            return JobResult(
                status="SUCCEEDED",
                target_date=target_date,
                reused_run_id=str(reusable),
                recovered_runs=recovered,
            )

    handle = repository.start(
        target_date,
        input_snapshot_id=snapshot.snapshot_id,
        rule_version=settings.rule_version,
        config_version=config_version,
        details={"input": snapshot.as_dict()},
    )
    run_id = str(handle.run_id)
    staging = _staging_paths(settings, target_date, run_id)
    try:
        manifest = _execute(
            settings,
            target_date,
            storage=storage,
            targets=targets,
            repository=repository,
            spark=spark,
            snapshot=snapshot,
            run_id=run_id,
            attempt=handle.attempt,
            config_version=config_version,
            staging=staging,
            now=now,
        )
        # manifest를 쓴 뒤 여기서 죽으면 파일은 확정됐지만 아무도 읽지 않는다.
        # 다음 실행의 recover가 같은 manifest로 DB만 다시 반영한다.
        repository.publish(manifest)
    except BaseException as error:  # noqa: BLE001 - 실패도 기록하고 다시 올린다
        try:
            repository.fail(handle.run_id, error)
        except Exception:  # pragma: no cover - DB 자체가 죽은 경우
            logger.exception("Could not record the failure of run %s", run_id)
        raise
    finally:
        storage.delete(staging_root(settings, target_date, run_id), recursive=True)

    return JobResult(
        status="SUCCEEDED",
        target_date=target_date,
        run_id=run_id,
        manifest=manifest,
        recovered_runs=recovered,
    )


def staging_root(settings, target_date: date, run_id: str) -> str:
    return (
        f"{settings.staging_base}/target_date={target_date.isoformat()}/run_id={run_id}"
    )


def _execute(
    settings,
    target_date: date,
    *,
    storage: LakeStorage,
    targets: ObservationTargets,
    repository: SilverCommitRepository,
    spark: SparkSession,
    snapshot: InputSnapshot,
    run_id: str,
    attempt: int,
    config_version: str,
    staging: dict[str, str],
    now: datetime,
) -> dict:
    offset = settings.business_utc_offset_seconds
    segments, expected, partial_day, multi_device = _expected_by_household(
        targets, target_date, offset
    )
    household_ids = targets.household_ids_for_date(target_date, offset)

    labelled = classify(
        normalize(
            read_bronze(spark, storage, snapshot),
            business_utc_offset_seconds=offset,
            future_skew_seconds=settings.future_skew_seconds,
            now=now,
            run_id=run_id,
            rule_version=settings.rule_version,
        )
    )
    # 정제 전력·격리·관측일·통계가 모두 같은 결과를 봐야 하고, 각각 원본을 다시 읽으면
    # 입력 파일을 네 번 훑게 된다.
    labelled = labelled.persist(StorageLevel.MEMORY_AND_DISK)

    try:
        stats = _row_state_stats(labelled, target_date)

        power_clean = labelled.filter(
            (F.col("row_state") == F.lit(STATE_CLEAN))
            & (F.col("event_date") == F.lit(target_date))
        ).select(*[field.name for field in POWER_CLEAN_SCHEMA.fields])

        # 다른 날짜의 격리 행은 그 날짜의 실행이 담당한다. 측정일을 읽을 수 없는 행은
        # 어느 날짜에도 속하지 않으므로 입력을 고른 이 실행이 담는다.
        quarantine = labelled.filter(
            (F.col("row_state") == F.lit(STATE_REJECTED))
            & (
                (F.col("event_date") == F.lit(target_date))
                | F.col("event_date").isNull()
            )
        ).select(*[field.name for field in QUARANTINE_SCHEMA.fields])

        observation = observation_daily(
            spark,
            labelled,
            segments_frame(spark, segments),
            household_ids=household_ids,
            expected_by_household=expected,
            partial_day_households=partial_day,
            multi_device_households=multi_device,
            target_date=target_date,
            valid_coverage_ratio=settings.valid_coverage_ratio,
            run_id=run_id,
            input_snapshot_id=snapshot.snapshot_id,
            rule_version=settings.rule_version,
            config_version=config_version,
        )

        bytes_per_row = (
            repository.latest_bytes_per_row(DATASET_POWER_CLEAN)
            or settings.power_clean_default_bytes_per_row
        )
        partitions = plan_partitions(
            stats["clean_rows_for_target_date"],
            bytes_per_row=bytes_per_row,
            target_file_bytes=settings.power_clean_target_file_bytes,
            max_files=settings.power_clean_max_output_files,
        )
        clean_write = write_parquet(
            power_clean, storage, staging[DATASET_POWER_CLEAN], partitions=partitions
        )
        # 관측일 결과는 가구 수만큼이라 작다. 전력 데이터와 같은 분할 수를 쓰지 않는다.
        observation_write = write_parquet(
            observation, storage, staging[DATASET_OBSERVATION], partitions=1
        )
        quarantine_write = write_parquet(
            quarantine, storage, staging["quarantine"], partitions=1
        )
    finally:
        labelled.unpersist()

    repository.set_status(UUID(run_id), RUN_VALIDATING)
    clean_stats = validate_power_clean(
        spark, storage, staging[DATASET_POWER_CLEAN], target_date=target_date
    )
    observation_stats = validate_observation(
        spark,
        storage,
        staging[DATASET_OBSERVATION],
        target_date=target_date,
        expected_household_ids=household_ids,
    )
    quarantine_rows = spark.read.parquet(storage.uri(staging["quarantine"])).count()

    # 읽는 동안 입력이 바뀌었으면 결과를 신뢰할 수 없다. 옮기기 전에 확인한다.
    verify_unchanged(storage, snapshot)

    final = _output_paths(settings, target_date, run_id)
    for name in (*DATASET_NAMES, "quarantine"):
        if storage.exists(final[name]):
            storage.delete(final[name], recursive=True)
        storage.rename(staging[name], final[name])

    manifest = {
        "job": JOB_NAME,
        "schema_version": 1,
        "run_id": run_id,
        "attempt": attempt,
        "target_date": target_date.isoformat(),
        "input_snapshot_id": snapshot.snapshot_id,
        "rule_version": settings.rule_version,
        "config_version": config_version,
        "business_utc_offset_seconds": offset,
        "valid_coverage_ratio": settings.valid_coverage_ratio,
        "input": snapshot.as_dict(),
        # 과거 원본을 지금의 매핑으로 해석하면 결과가 달라진다. 그때 쓴 관측 대상 설정과
        # 하루 안에서 실제로 적용된 구간을 결과 옆에 남긴다.
        "observation_targets": targets.as_dict(),
        "observation_segments": [
            {
                "household_id": segment.household_id,
                "device_id": segment.device_id,
                "start_second": segment.start_second,
                "end_second": segment.end_second,
                "sampling_interval_seconds": segment.sampling_interval_seconds,
                "expected_sample_count": segment.expected_sample_count,
            }
            for segment in segments
        ],
        "outputs": {
            DATASET_POWER_CLEAN: {
                "path": final[DATASET_POWER_CLEAN],
                "row_count": clean_stats.row_count,
                "file_count": clean_write.file_count,
                "byte_count": clean_write.byte_count,
                "output_partitions": partitions,
            },
            DATASET_OBSERVATION: {
                "path": final[DATASET_OBSERVATION],
                "row_count": observation_stats.row_count,
                "file_count": observation_write.file_count,
                "byte_count": observation_write.byte_count,
                **observation_stats.details,
            },
        },
        "quarantine": {
            "path": final["quarantine"],
            "row_count": quarantine_rows,
            "file_count": quarantine_write.file_count,
            "byte_count": quarantine_write.byte_count,
        },
        "row_states": stats,
        # 다음 실행이 출력 분할 수를 정할 때 쓰는 실측값.
        "bytes_per_row": {
            DATASET_POWER_CLEAN: clean_write.bytes_per_row(clean_stats.row_count),
        },
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    path = manifest_path(settings.manifest_base, target_date, run_id)
    manifest["manifest_path"] = path
    write_manifest(storage, path, manifest)
    return manifest
