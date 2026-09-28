"""중복 제거와 충돌 격리.

세 단계로 나눈다.

| 단계 | 중복 키 | 처리 |
|---|---|---|
| 물리 중복 | topic + partition + kafka_offset | Kafka 레코드의 반복 적재 |
| 논리 중복 | message_id | 생산자 재전송 |
| 측정 중복 | household_id + device_id + measured_at_utc | 같은 측정의 재발행(시뮬레이터 재실행 등) |

같은 키의 내용이 같으면 한 행만 남기고, 내용이 다르면 임의로 하나를 고르지 않고
충돌 그룹 전체를 격리한다. 보존 행은 (수신시각, 파일, 파티션, offset) 순서로 고정해
노드 수가 달라도 같은 행이 남는다.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from power_silver.constants import (
    REJECT_KAFKA_RECORD_CONFLICT,
    REJECT_MEASUREMENT_CONFLICT,
    REJECT_MESSAGE_CONFLICT,
)


STATE_CLEAN = "CLEAN"
STATE_DUPLICATE = "DUPLICATE"
STATE_REJECTED = "REJECTED"

def _order() -> list[Column]:
    """보존 행 선택 순서.

    물리 중복 그룹은 (파티션, offset)이 이미 같으므로 수신시각과 입력 파일이 순서를
    정하고, 논리·측정 중복 그룹은 서로 다른 Kafka 레코드이므로 (파티션, offset)이
    순서를 확정한다. 수신시각이 앞서므로 재발행분이 아니라 최초 적재분이 남는다.
    """

    return [
        F.col("ingested_at_utc").asc_nulls_last(),
        F.col("source_file").asc_nulls_last(),
        F.col("kafka_partition").asc_nulls_last(),
        F.col("kafka_offset").asc_nulls_last(),
    ]


def _mark(frame: DataFrame, keys: list[Column], prefix: str) -> DataFrame:
    """그룹 안의 내용 충돌 여부와 보존 순위를 붙인다."""

    group = Window.partitionBy(*keys)
    return (
        frame.withColumn(
            f"{prefix}_conflict",
            F.min("content_hash").over(group) != F.max("content_hash").over(group),
        ).withColumn(f"{prefix}_rank", F.row_number().over(group.orderBy(*_order())))
    )


def classify(frame: DataFrame) -> DataFrame:
    """행마다 ``row_state``(CLEAN/DUPLICATE/REJECTED)와 최종 ``reject_code``를 정한다.

    검증에서 이미 탈락한 행은 중복 판정에 끼어들지 않도록 ``is_valid``를 그룹 키에
    넣는다. 같은 이유로 물리 중복에서 살아남은 행만 논리 중복 그룹을 이루고, 논리
    중복에서 살아남은 행만 측정 중복 그룹을 이룬다.
    """

    marked = frame.withColumn("is_valid", F.col("reject_code").isNull())
    marked = _mark(
        marked,
        [F.col("topic"), F.col("kafka_partition"), F.col("kafka_offset"), F.col("is_valid")],
        "physical",
    )
    marked = marked.withColumn(
        "survives_physical",
        F.col("is_valid") & ~F.col("physical_conflict") & (F.col("physical_rank") == 1),
    )
    marked = _mark(
        marked,
        [F.col("message_id"), F.col("is_valid"), F.col("survives_physical")],
        "logical",
    )
    marked = marked.withColumn(
        "survives_logical",
        F.col("survives_physical") & ~F.col("logical_conflict") & (F.col("logical_rank") == 1),
    )
    marked = _mark(
        marked,
        [
            F.col("household_id"),
            F.col("device_id"),
            F.col("measured_at_utc"),
            F.col("is_valid"),
            F.col("survives_logical"),
        ],
        "measurement",
    )

    reject_code = (
        F.when(F.col("reject_code").isNotNull(), F.col("reject_code"))
        .when(F.col("physical_conflict"), F.lit(REJECT_KAFKA_RECORD_CONFLICT))
        .when(
            F.col("survives_physical") & F.col("logical_conflict"),
            F.lit(REJECT_MESSAGE_CONFLICT),
        )
        .when(
            F.col("survives_logical") & F.col("measurement_conflict"),
            F.lit(REJECT_MEASUREMENT_CONFLICT),
        )
    )
    row_state = (
        F.when(reject_code.isNotNull(), F.lit(STATE_REJECTED))
        .when(~F.col("survives_physical"), F.lit(STATE_DUPLICATE))
        .when(F.col("logical_rank") > 1, F.lit(STATE_DUPLICATE))
        .when(F.col("measurement_rank") > 1, F.lit(STATE_DUPLICATE))
        .otherwise(F.lit(STATE_CLEAN))
    )

    return (
        marked.withColumn("reject_code", reject_code)
        .withColumn("row_state", row_state)
        .drop(
            "is_valid",
            "physical_conflict",
            "physical_rank",
            "survives_physical",
            "logical_conflict",
            "logical_rank",
            "survives_logical",
            "measurement_conflict",
            "measurement_rank",
        )
    )
