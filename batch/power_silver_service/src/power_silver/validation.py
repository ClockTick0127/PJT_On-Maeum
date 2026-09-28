"""확정 전 출력 검증.

임시 경로에 쓴 결과를 다시 읽어 행 수·키 중복·상태값·날짜를 확인한다. 여기를 통과한
디렉터리만 최종 경로로 옮긴다. 파일을 옮긴 뒤에 깨진 것을 발견하면 소비자가 이미 읽은
뒤일 수 있다.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from power_silver.constants import OBSERVATION_STATUSES
from power_silver.storage import LakeStorage


class OutputInvalid(RuntimeError):
    pass


@dataclass
class DatasetStats:
    dataset: str
    row_count: int
    details: dict = field(default_factory=dict)


def validate_power_clean(
    spark: SparkSession,
    storage: LakeStorage,
    path: str,
    *,
    target_date: date,
) -> DatasetStats:
    frame = spark.read.parquet(storage.uri(path))
    row_count = frame.count()
    summary = frame.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("message_id").alias("distinct_messages"),
        F.countDistinct("household_id", "device_id", "measured_at_utc").alias(
            "distinct_measurements"
        ),
        F.sum(F.when(F.col("event_date") != F.lit(target_date), 1).otherwise(0)).alias(
            "wrong_date"
        ),
        F.sum(
            F.when(
                F.col("household_id").isNull()
                | F.col("measured_at_utc").isNull()
                | F.col("message_id").isNull(),
                1,
            ).otherwise(0)
        ).alias("null_keys"),
    ).collect()[0]

    if summary["rows"] != row_count:
        raise OutputInvalid("power_clean row count is unstable between reads")
    if summary["distinct_messages"] != row_count:
        raise OutputInvalid(
            f"power_clean has duplicate message_id rows: "
            f"{row_count - summary['distinct_messages']}"
        )
    if summary["distinct_measurements"] != row_count:
        raise OutputInvalid(
            f"power_clean has duplicate (household, device, measured_at) rows: "
            f"{row_count - summary['distinct_measurements']}"
        )
    if summary["wrong_date"]:
        raise OutputInvalid(
            f"power_clean contains {summary['wrong_date']} rows outside {target_date}"
        )
    if summary["null_keys"]:
        raise OutputInvalid(f"power_clean has {summary['null_keys']} rows with null keys")

    return DatasetStats(dataset="power_clean", row_count=row_count)


def validate_observation(
    spark: SparkSession,
    storage: LakeStorage,
    path: str,
    *,
    target_date: date,
    expected_household_ids: tuple[str, ...],
) -> DatasetStats:
    frame = spark.read.parquet(storage.uri(path))
    row_count = frame.count()
    if row_count != len(expected_household_ids):
        raise OutputInvalid(
            f"observation rows {row_count} != configured households "
            f"{len(expected_household_ids)}"
        )

    summary = frame.agg(
        F.countDistinct("household_id").alias("distinct_households"),
        F.sum(
            F.when(~F.col("observation_status").isin(list(OBSERVATION_STATUSES)), 1)
            .otherwise(0)
        ).alias("unknown_status"),
        F.sum(
            F.when(
                (F.col("coverage_ratio") < 0) | (F.col("coverage_ratio") > 1), 1
            ).otherwise(0)
        ).alias("bad_ratio"),
        F.sum(
            F.when(F.col("observation_date") != F.lit(target_date), 1).otherwise(0)
        ).alias("wrong_date"),
    ).collect()[0]

    if summary["distinct_households"] != row_count:
        raise OutputInvalid("observation has more than one row for a household")
    if summary["unknown_status"]:
        raise OutputInvalid("observation contains an unknown observation_status")
    if summary["bad_ratio"]:
        raise OutputInvalid("observation coverage_ratio is outside [0, 1]")
    if summary["wrong_date"]:
        raise OutputInvalid(f"observation contains rows outside {target_date}")

    by_status = {
        row["observation_status"]: row["count"]
        for row in frame.groupBy("observation_status")
        .agg(F.count(F.lit(1)).alias("count"))
        .collect()
    }
    return DatasetStats(
        dataset="household_observation_daily",
        row_count=row_count,
        details={"by_status": by_status},
    )
