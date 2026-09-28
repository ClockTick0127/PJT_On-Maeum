"""SparkSession 생성.

세션 시간대를 UTC로 고정하는 것이 중요하다. 정규화·관측 슬롯 계산이 "앞 19자는 UTC
벽시계"라는 가정 위에 서 있고, 업무 날짜는 고정 오프셋 산술로 따로 만든다. 실행 노드의
시간대 설정이 결과를 바꾸면 안 된다.
"""

from __future__ import annotations

from pyspark.sql import SparkSession


def build_session(settings) -> SparkSession:
    builder = (
        SparkSession.builder.appName(settings.spark_app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
    )
    if settings.spark_master:
        builder = builder.master(settings.spark_master)
    if settings.spark_shuffle_partitions:
        builder = builder.config(
            "spark.sql.shuffle.partitions", str(settings.spark_shuffle_partitions)
        )
    return builder.getOrCreate()
