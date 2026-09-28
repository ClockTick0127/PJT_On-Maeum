"""가구별 관측일 계산.

DB 방식은 수집 건수를 기대량 86,400으로 나눈다. 같은 1초에 재전송이나 여러 측정값이
들어오면 관측하지 못한 시간을 관측한 것처럼 만든다. 레이크에서는 **실제로 관측한 시간
구간 수**를 센다. 두 방식의 결과가 다를 수 있으므로 규칙 버전을 분리한다.

    expected_sample_count = 관측 대상 구간의 기대 슬롯 수 (1초 간격 하루면 86,400)
    observed_slot_count   = 정상 측정값이 존재하는 서로 다른 슬롯 수
    coverage_ratio        = observed_slot_count / expected_sample_count
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from power_silver.constants import (
    FLAG_DEVICE_CHANGED,
    FLAG_DUPLICATES_REMOVED,
    FLAG_INVALID_ROWS,
    FLAG_MESSAGE_CONFLICT,
    FLAG_PARTIAL_DAY,
    OBSERVATION_INSUFFICIENT,
    OBSERVATION_NOT_APPLICABLE,
    OBSERVATION_SENSOR_GAP,
    OBSERVATION_VALID,
    REJECT_KAFKA_RECORD_CONFLICT,
    REJECT_MEASUREMENT_CONFLICT,
    REJECT_MESSAGE_CONFLICT,
)
from power_silver.deduplicate import STATE_CLEAN, STATE_DUPLICATE, STATE_REJECTED
from power_silver.schemas import SEGMENT_SCHEMA
from power_silver.targets import DaySegment


def _in(household_ids: frozenset[str]):
    """빈 목록에 대한 ``IN ()``을 피하면서 가구 소속 여부를 표현한다."""

    if not household_ids:
        return F.lit(False)
    return F.col("household_id").isin(sorted(household_ids))


def segments_frame(spark: SparkSession, segments: tuple[DaySegment, ...]) -> DataFrame:
    rows = [
        (
            segment.household_id,
            segment.device_id,
            segment.segment_index,
            segment.start_second,
            segment.end_second,
            segment.sampling_interval_seconds,
        )
        for segment in segments
    ]
    return spark.createDataFrame(rows, SEGMENT_SCHEMA)


def slots(clean: DataFrame, segments: DataFrame) -> DataFrame:
    """정상 측정 행을 관측 대상 구간에 붙이고 슬롯 좌표를 계산한다.

    대표 계측기가 아니거나 관측 대상 시간 밖의 측정값은 정제 전력에는 남지만 관측량에는
    들어가지 않는다.
    """

    joined = clean.join(
        F.broadcast(segments),
        (clean["household_id"] == segments["household_id"])
        & (clean["device_id"] == segments["segment_device_id"])
        & (clean["local_day_second"] >= segments["segment_start_second"])
        & (clean["local_day_second"] < segments["segment_end_second"]),
    ).drop(segments["household_id"])

    return joined.withColumn(
        "slot_key",
        F.col("segment_start_second")
        + F.floor(
            (F.col("local_day_second") - F.col("segment_start_second"))
            / F.col("sampling_interval_seconds")
        )
        * F.col("sampling_interval_seconds"),
    )


def _max_missing_seconds(slot_frame: DataFrame, segments: DataFrame) -> DataFrame:
    """하루 중 가장 긴 미관측 구간.

    전체 가구에 86,400행짜리 빈 시간표를 만들 필요는 없다. 관측한 슬롯만 정렬해 인접
    슬롯의 차이와 구간 시작·끝 경계를 본다. 관측 대상이 아닌 시간(구간 사이)은 누락으로
    세지 않는다.
    """

    distinct_slots = slot_frame.select(
        "household_id",
        "segment_index",
        "slot_key",
        "segment_start_second",
        "segment_end_second",
        "sampling_interval_seconds",
    ).distinct()

    ordered = Window.partitionBy("household_id", "segment_index").orderBy("slot_key")
    gaps = distinct_slots.withColumn(
        "previous_slot", F.lag("slot_key").over(ordered)
    ).withColumn(
        "gap_seconds",
        F.when(
            F.col("previous_slot").isNull(),
            F.col("slot_key") - F.col("segment_start_second"),
        ).otherwise(
            F.col("slot_key")
            - F.col("previous_slot")
            - F.col("sampling_interval_seconds")
        ),
    )

    observed = gaps.groupBy("household_id", "segment_index").agg(
        F.max("gap_seconds").alias("internal_gap"),
        F.max("slot_key").alias("last_slot"),
        F.first("segment_end_second").alias("segment_end_second"),
        F.first("sampling_interval_seconds").alias("sampling_interval_seconds"),
    ).withColumn(
        "segment_missing",
        F.greatest(
            F.col("internal_gap"),
            F.col("segment_end_second")
            - (F.col("last_slot") + F.col("sampling_interval_seconds")),
        ),
    )

    per_segment = segments.join(
        observed.select("household_id", "segment_index", "segment_missing"),
        ["household_id", "segment_index"],
        "left",
    ).withColumn(
        "segment_missing",
        F.coalesce(
            F.col("segment_missing"),
            F.col("segment_end_second") - F.col("segment_start_second"),
        ),
    )

    return per_segment.groupBy("household_id").agg(
        F.max("segment_missing").cast("long").alias("max_missing_seconds")
    )


def observation_daily(
    spark: SparkSession,
    labelled: DataFrame,
    segments: DataFrame,
    *,
    household_ids: tuple[str, ...],
    expected_by_household: dict[str, int],
    partial_day_households: frozenset[str],
    multi_device_households: frozenset[str],
    target_date: date,
    valid_coverage_ratio: float,
    run_id: str,
    input_snapshot_id: str,
    rule_version: str,
    config_version: str,
) -> DataFrame:
    """관측 대상 가구 목록을 기준으로 관측일 한 행씩 만든다."""

    # 입력 파일에는 다른 날짜의 행이 섞여 있다. 하루 안의 좌표(local_day_second)로
    # 조인하므로 날짜를 먼저 걸러내지 않으면 전날 23:59:59가 이 날의 슬롯으로 들어온다.
    clean = labelled.filter(
        (F.col("row_state") == F.lit(STATE_CLEAN))
        & (F.col("event_date") == F.lit(target_date))
    )
    slot_frame = slots(clean, segments)

    measured = slot_frame.groupBy("household_id").agg(
        F.count(F.lit(1)).cast("long").alias("valid_measurement_count"),
        F.countDistinct("slot_key").cast("long").alias("observed_slot_count"),
        F.min("measured_at_utc").alias("first_measured_at"),
        F.max("measured_at_utc").alias("last_measured_at"),
        F.collect_set("device_id").alias("observed_device_ids"),
    )
    missing = _max_missing_seconds(slot_frame, segments)

    # 가구에 귀속할 수 있는 중복·오류만 센다. 가구 ID나 측정일을 읽을 수 없는 행은
    # 특정 가구에 억지로 붙이지 않고 실행 전체 통계로 남긴다.
    attributable = labelled.filter(
        (F.col("row_state") != F.lit(STATE_CLEAN))
        & F.col("household_id").isNotNull()
        & (F.col("event_date") == F.lit(target_date))
    )
    problems = attributable.groupBy("household_id").agg(
        F.count(F.when(F.col("row_state") == F.lit(STATE_DUPLICATE), 1))
        .cast("long")
        .alias("duplicate_count"),
        F.count(F.when(F.col("row_state") == F.lit(STATE_REJECTED), 1))
        .cast("long")
        .alias("invalid_count"),
        F.count(
            F.when(
                F.col("reject_code").isin(
                    REJECT_MESSAGE_CONFLICT,
                    REJECT_KAFKA_RECORD_CONFLICT,
                    REJECT_MEASUREMENT_CONFLICT,
                ),
                1,
            )
        )
        .cast("long")
        .alias("conflict_count"),
    )

    expected = spark.createDataFrame(
        [
            (household_id, int(expected_by_household.get(household_id, 0)))
            for household_id in household_ids
        ],
        "household_id string, expected_sample_count long",
    )

    frame = (
        expected.join(measured, "household_id", "left")
        .join(missing, "household_id", "left")
        .join(problems, "household_id", "left")
        .fillna(
            {
                "valid_measurement_count": 0,
                "observed_slot_count": 0,
                "max_missing_seconds": 0,
                "duplicate_count": 0,
                "invalid_count": 0,
                "conflict_count": 0,
            }
        )
    )

    raw_ratio = F.when(
        F.col("expected_sample_count") > 0,
        F.col("observed_slot_count") / F.col("expected_sample_count"),
    ).otherwise(F.lit(0.0))

    status = (
        F.when(
            F.col("expected_sample_count") <= 0, F.lit(OBSERVATION_NOT_APPLICABLE)
        )
        .when(F.col("observed_slot_count") <= 0, F.lit(OBSERVATION_SENSOR_GAP))
        # 95% 판정은 반올림 전 값으로 한다. 표시용 비율만 뒤에서 반올림한다.
        .when(raw_ratio >= F.lit(valid_coverage_ratio), F.lit(OBSERVATION_VALID))
        .otherwise(F.lit(OBSERVATION_INSUFFICIENT))
    )

    quality_flags = F.array_compact(
        F.array(
            F.when(_in(partial_day_households), F.lit(FLAG_PARTIAL_DAY)),
            F.when(_in(multi_device_households), F.lit(FLAG_DEVICE_CHANGED)),
            F.when(F.col("duplicate_count") > 0, F.lit(FLAG_DUPLICATES_REMOVED)),
            F.when(F.col("invalid_count") > 0, F.lit(FLAG_INVALID_ROWS)),
            F.when(F.col("conflict_count") > 0, F.lit(FLAG_MESSAGE_CONFLICT)),
        )
    )

    return (
        frame.withColumn("observation_date", F.lit(target_date).cast("date"))
        .withColumn(
            "device_ids",
            F.array_sort(F.coalesce(F.col("observed_device_ids"), F.array())),
        )
        .withColumn("coverage_ratio", F.round(raw_ratio, 4))
        .withColumn("observation_status", status)
        .withColumn("quality_flags", quality_flags)
        .withColumn("run_id", F.lit(run_id))
        .withColumn("input_snapshot_id", F.lit(input_snapshot_id))
        .withColumn("rule_version", F.lit(rule_version))
        .withColumn("config_version", F.lit(config_version))
        .select(
            "household_id",
            "observation_date",
            "device_ids",
            "valid_measurement_count",
            "observed_slot_count",
            "expected_sample_count",
            "coverage_ratio",
            "observation_status",
            "quality_flags",
            "first_measured_at",
            "last_measured_at",
            "max_missing_seconds",
            "duplicate_count",
            "invalid_count",
            "run_id",
            "input_snapshot_id",
            "rule_version",
            "config_version",
        )
    )
