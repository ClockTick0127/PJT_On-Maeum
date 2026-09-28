"""Explicit Spark schemas for the Bronze input and the Silver outputs.

입력 스키마를 명시하지 않으면 파일마다 다른 추론 결과가 나오고, 계약이 바뀐 과거
파일을 조용히 다르게 읽는다. 읽기는 항상 이 스키마로 고정한다. ``raw_payload``는
읽지 않는다. 원문이 필요하면 Bronze 위치(``source_file`` + Kafka 좌표)로 되돌아간다.
"""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


BRONZE_POWER_SCHEMA = StructType(
    [
        StructField("message_id", StringType()),
        StructField("household_id", StringType()),
        StructField("device_id", StringType()),
        StructField("measured_at", StringType()),
        StructField("active_power", DoubleType()),
        StructField("reactive_power", DoubleType()),
        StructField("power_factor", DoubleType()),
        StructField("current", DoubleType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("kafka_offset", LongType()),
        StructField("kafka_ts", StringType()),
        StructField("ingested_at", StringType()),
    ]
)

POWER_CLEAN_SCHEMA = StructType(
    [
        StructField("message_id", StringType(), False),
        StructField("household_id", StringType(), False),
        StructField("device_id", StringType(), False),
        StructField("measured_at_utc", TimestampType(), False),
        StructField("event_date", DateType(), False),
        StructField("active_power", DoubleType(), False),
        StructField("reactive_power", DoubleType(), False),
        StructField("power_factor", DoubleType(), False),
        StructField("current", DoubleType(), False),
        StructField("topic", StringType()),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
        StructField("source_file", StringType()),
        StructField("ingested_at_utc", TimestampType()),
        StructField("run_id", StringType(), False),
        StructField("rule_version", StringType(), False),
    ]
)

OBSERVATION_SCHEMA = StructType(
    [
        StructField("household_id", StringType(), False),
        StructField("observation_date", DateType(), False),
        StructField("device_ids", ArrayType(StringType()), False),
        StructField("valid_measurement_count", LongType(), False),
        StructField("observed_slot_count", LongType(), False),
        StructField("expected_sample_count", LongType(), False),
        StructField("coverage_ratio", DoubleType(), False),
        StructField("observation_status", StringType(), False),
        StructField("quality_flags", ArrayType(StringType()), False),
        StructField("first_measured_at", TimestampType()),
        StructField("last_measured_at", TimestampType()),
        StructField("max_missing_seconds", LongType(), False),
        StructField("duplicate_count", LongType(), False),
        StructField("invalid_count", LongType(), False),
        StructField("run_id", StringType(), False),
        StructField("input_snapshot_id", StringType(), False),
        StructField("rule_version", StringType(), False),
        StructField("config_version", StringType(), False),
    ]
)

QUARANTINE_SCHEMA = StructType(
    [
        StructField("reject_code", StringType(), False),
        StructField("message_id", StringType()),
        StructField("household_id", StringType()),
        StructField("device_id", StringType()),
        StructField("measured_at", StringType()),
        StructField("event_date", DateType()),
        StructField("active_power", DoubleType()),
        StructField("reactive_power", DoubleType()),
        StructField("power_factor", DoubleType()),
        StructField("current", DoubleType()),
        StructField("topic", StringType()),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
        StructField("source_file", StringType()),
        StructField("ingested_at", StringType()),
        StructField("run_id", StringType(), False),
        StructField("rule_version", StringType(), False),
    ]
)

SEGMENT_SCHEMA = StructType(
    [
        StructField("household_id", StringType(), False),
        StructField("segment_device_id", StringType(), False),
        StructField("segment_index", IntegerType(), False),
        StructField("segment_start_second", IntegerType(), False),
        StructField("segment_end_second", IntegerType(), False),
        StructField("sampling_interval_seconds", IntegerType(), False),
    ]
)
