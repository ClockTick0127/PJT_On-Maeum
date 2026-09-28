"""Bronze 원본을 읽어 검증하고 시각을 정규화한다.

적재기도 기본 검증을 한다. 여기서 다시 검증하는 이유는 과거 파일·계약 변경·입력
불일치까지 포함해 Silver 품질을 보장해야 하기 때문이다. 깨진 행은 버리지 않고 사유와
Bronze 위치를 붙여 격리한다.
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from power_silver.constants import (
    REJECT_FUTURE_MEASURED_AT,
    REJECT_INVALID_DEVICE_ID,
    REJECT_INVALID_HOUSEHOLD_ID,
    REJECT_INVALID_MEASURED_AT,
    REJECT_INVALID_MESSAGE_ID,
    REJECT_INVALID_NUMBER,
    REJECT_OUT_OF_RANGE,
    SECONDS_PER_DAY,
)
from power_silver.input_snapshot import InputSnapshot
from power_silver.schemas import BRONZE_POWER_SCHEMA
from power_silver.storage import LakeStorage


UUID_PATTERN = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
# 시간대가 반드시 있어야 한다. 없으면 어느 날짜의 데이터인지 확정할 수 없다.
INSTANT_PATTERN = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]\d{2}:\d{2})$"
MEASUREMENT_COLUMNS = ("active_power", "reactive_power", "power_factor", "current")


def read_bronze(
    spark: SparkSession,
    storage: LakeStorage,
    snapshot: InputSnapshot,
) -> DataFrame:
    """스냅샷에 적힌 파일만 읽는다. 이후 도착한 파일은 다음 수정 실행이 처리한다."""

    if not snapshot.files:
        return spark.createDataFrame([], BRONZE_POWER_SCHEMA).withColumn(
            "source_file", F.lit(None).cast("string")
        )
    uris = [storage.uri(item.path) for item in snapshot.files]
    return (
        spark.read.schema(BRONZE_POWER_SCHEMA).parquet(*uris)
        .withColumn("source_file", F.input_file_name())
    )


def _epoch_micros(column: Column) -> Column:
    """ISO 8601 문자열을 UTC epoch 마이크로초로 바꾼다.

    Spark의 선택적 패턴 해석에 기대지 않고, 형식을 정규식으로 고정한 뒤 초 단위·소수부·
    오프셋을 따로 계산한다. 세션 시간대는 UTC로 고정되어 있으므로 앞 19자는 그대로
    UTC 벽시계 시각으로 읽힌다.
    """

    valid = column.rlike(INSTANT_PATTERN)
    naive_seconds = F.unix_timestamp(
        F.substring(column, 1, 19), "yyyy-MM-dd'T'HH:mm:ss"
    )
    fraction = F.rpad(F.regexp_extract(column, r"\.(\d{1,6})", 1), 6, "0")
    fraction_micros = F.when(fraction == F.lit("000000"), F.lit(0)).otherwise(
        fraction.cast("long")
    )
    offset_text = F.substring(column, -6, 6)
    offset_seconds = (
        F.when(column.endswith("Z"), F.lit(0))
        .otherwise(
            F.when(F.substring(offset_text, 1, 1) == F.lit("-"), F.lit(-1))
            .otherwise(F.lit(1))
            * (
                F.substring(offset_text, 2, 2).cast("long") * F.lit(3600)
                + F.substring(offset_text, 5, 2).cast("long") * F.lit(60)
            )
        )
    )
    return F.when(
        valid & naive_seconds.isNotNull(),
        (naive_seconds - offset_seconds) * F.lit(1_000_000) + fraction_micros,
    )


def _not_finite(name: str) -> Column:
    column = F.col(name)
    return (
        column.isNull()
        | F.isnan(column)
        | (column == F.lit(float("inf")))
        | (column == F.lit(float("-inf")))
    )


def _blank_id(name: str) -> Column:
    column = F.trim(F.col(name))
    return column.isNull() | (F.length(column) < 1) | (F.length(column) > 50)


def normalize(
    frame: DataFrame,
    *,
    business_utc_offset_seconds: int,
    future_skew_seconds: int,
    now: datetime,
    run_id: str,
    rule_version: str,
) -> DataFrame:
    """검증 결과(`reject_code`)와 파생 시각 컬럼을 붙인다. 행은 아직 버리지 않는다."""

    measured_micros = _epoch_micros(F.col("measured_at"))
    future_limit_micros = F.lit(
        int(now.timestamp() + future_skew_seconds) * 1_000_000
    )

    invalid_number = _not_finite(MEASUREMENT_COLUMNS[0])
    for name in MEASUREMENT_COLUMNS[1:]:
        invalid_number = invalid_number | _not_finite(name)

    out_of_range = (
        (F.col("active_power") < 0)
        | (F.col("current") < 0)
        | (F.col("power_factor") < -1)
        | (F.col("power_factor") > 1)
    )

    reject_code = (
        F.when(
            F.col("message_id").isNull() | ~F.col("message_id").rlike(UUID_PATTERN),
            F.lit(REJECT_INVALID_MESSAGE_ID),
        )
        .when(_blank_id("household_id"), F.lit(REJECT_INVALID_HOUSEHOLD_ID))
        .when(_blank_id("device_id"), F.lit(REJECT_INVALID_DEVICE_ID))
        .when(measured_micros.isNull(), F.lit(REJECT_INVALID_MEASURED_AT))
        .when(measured_micros > future_limit_micros, F.lit(REJECT_FUTURE_MEASURED_AT))
        .when(invalid_number, F.lit(REJECT_INVALID_NUMBER))
        .when(out_of_range, F.lit(REJECT_OUT_OF_RANGE))
    )

    local_second = (
        F.floor(F.col("measured_epoch_micros") / F.lit(1_000_000))
        + F.lit(business_utc_offset_seconds)
    )
    day_index = F.floor(local_second / F.lit(SECONDS_PER_DAY))

    return (
        frame.withColumn("measured_epoch_micros", measured_micros)
        .withColumn("reject_code", reject_code)
        .withColumn("household_id", F.trim(F.col("household_id")))
        .withColumn("device_id", F.trim(F.col("device_id")))
        .withColumn(
            "measured_at_utc", F.timestamp_micros(F.col("measured_epoch_micros"))
        )
        .withColumn("event_date", F.date_from_unix_date(day_index.cast("int")))
        .withColumn(
            "local_day_second",
            (local_second - day_index * F.lit(SECONDS_PER_DAY)).cast("int"),
        )
        .withColumn(
            "ingested_at_utc",
            F.timestamp_micros(_epoch_micros(F.col("ingested_at"))),
        )
        .withColumn("kafka_partition", F.col("partition"))
        .withColumn(
            "content_hash",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.coalesce(F.col("household_id"), F.lit("")),
                    F.coalesce(F.col("device_id"), F.lit("")),
                    F.coalesce(
                        F.col("measured_epoch_micros").cast("string"), F.lit("")
                    ),
                    *[
                        F.coalesce(F.col(name).cast("string"), F.lit(""))
                        for name in MEASUREMENT_COLUMNS
                    ],
                ),
                256,
            ),
        )
        .withColumn("run_id", F.lit(run_id))
        .withColumn("rule_version", F.lit(rule_version))
    )
