"""Shared fixtures: a local lake directory, a SQLite analysis_db and a local Spark."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from realtime_analysis.database import Base

from power_silver.config import SilverSettings
from power_silver.storage import LocalLakeStorage
from power_silver.targets import load_targets


TARGET_DATE = date(2026, 9, 19)
KST_OFFSET = 9 * 3600
# 2026-09-19 00:00:00 +09:00
DAY_START_EPOCH = int(
    datetime(2026, 9, 18, 15, 0, tzinfo=timezone.utc).timestamp()
)
DAY_END_EPOCH = DAY_START_EPOCH + 86_400
TOPIC = "power.raw.v1"


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    try:
        session = (
            SparkSession.builder.master("local[2]")
            .appName("power-silver-tests")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "127.0.0.1")
            .getOrCreate()
        )
    except Exception as error:  # pragma: no cover - 환경에 JVM이 없을 때
        pytest.skip(f"local Spark is unavailable: {error} (pyspark {pyspark.__version__})")
    yield session
    session.stop()


@pytest.fixture
def lake(tmp_path) -> LocalLakeStorage:
    return LocalLakeStorage(tmp_path / "lake")


@pytest.fixture
def targets_file(tmp_path) -> Path:
    path = tmp_path / "observation_targets.json"
    write_targets(path, ["H001", "H002", "H003"])
    return path


def write_targets(path: Path, household_ids, *, device_id="main", interval=1) -> None:
    path.write_text(
        json.dumps(
            {
                "config_version": "test-targets-v1",
                "targets": [
                    {
                        "household_id": household_id,
                        "device_id": device_id,
                        "effective_from": "2026-09-01T00:00:00+09:00",
                        "effective_to": None,
                        "sampling_interval_seconds": interval,
                        "observation_enabled": True,
                    }
                    for household_id in household_ids
                ],
            }
        ),
        encoding="utf-8",
    )


@pytest.fixture
def settings(tmp_path, targets_file) -> SilverSettings:
    return SilverSettings(
        lake_local_root=str(tmp_path / "lake"),
        observation_targets_file=str(targets_file),
        business_utc_offset_seconds=KST_OFFSET,
        spark_master="local[2]",
        power_clean_target_file_bytes=1024 * 1024,
    )


@pytest.fixture
def session_factory(tmp_path):
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'analysis.db'}")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


@pytest.fixture
def observation_targets(targets_file):
    return load_targets(targets_file)


def iso(epoch_seconds: float, offset_seconds: int = 0) -> str:
    """Bronze 적재기가 쓰는 형식의 ISO 8601 문자열."""

    moment = datetime.fromtimestamp(
        epoch_seconds, timezone(timedelta(seconds=offset_seconds))
    )
    return moment.isoformat()


def household_hex(household_id: str) -> str:
    return household_id.encode().hex()[:12].ljust(12, "0")


def bronze_rows(
    *,
    household_id: str,
    device_id: str = "main",
    seconds,
    offset_seconds: int = 0,
    partition: int = 0,
    first_offset: int = 0,
    active_power: float = 100.0,
    message_seed: int = 0,
    ingested_at: str | None = None,
) -> list[dict]:
    rows = []
    suffix = household_hex(household_id)
    for index, second in enumerate(seconds):
        identifier = message_seed + index
        rows.append(
            {
                "message_id": f"{identifier:08x}-0000-4000-8000-{suffix}",
                "household_id": household_id,
                "device_id": device_id,
                "measured_at": iso(second, offset_seconds),
                "active_power": active_power,
                "reactive_power": 10.0,
                "power_factor": 0.9,
                "current": 1.5,
                "topic": TOPIC,
                "partition": partition,
                "kafka_offset": first_offset + index,
                "kafka_ts": iso(second, 0),
                "ingested_at": ingested_at or iso(second + 1, 0),
            }
        )
    return rows


def day_frame(
    spark,
    *,
    household_id: str,
    device_id: str = "main",
    start_epoch: int,
    count: int,
    step: int = 1,
    partition: int = 0,
    first_offset: int = 0,
    active_power: float = 100.0,
):
    """Spark 안에서 1Hz 측정값을 생성한다(큰 하루치를 파이썬으로 옮기지 않기 위해)."""

    from pyspark.sql import functions as F

    from power_silver.schemas import BRONZE_POWER_SCHEMA

    suffix = household_hex(household_id)
    second = F.lit(start_epoch) + F.col("id") * F.lit(step)
    return (
        spark.range(count)
        .select(
            F.concat(
                F.lpad(F.hex(F.col("id") + F.lit(first_offset)), 8, "0"),
                F.lit(f"-0000-4000-8000-{suffix}"),
            ).alias("message_id"),
            F.lit(household_id).alias("household_id"),
            F.lit(device_id).alias("device_id"),
            F.concat(
                F.date_format(F.timestamp_seconds(second), "yyyy-MM-dd'T'HH:mm:ss"),
                F.lit("+00:00"),
            ).alias("measured_at"),
            F.lit(active_power).alias("active_power"),
            F.lit(10.0).alias("reactive_power"),
            F.lit(0.9).alias("power_factor"),
            F.lit(1.5).alias("current"),
            F.lit(TOPIC).alias("topic"),
            F.lit(partition).cast("int").alias("partition"),
            (F.col("id") + F.lit(first_offset)).alias("kafka_offset"),
            F.concat(
                F.date_format(F.timestamp_seconds(second), "yyyy-MM-dd'T'HH:mm:ss"),
                F.lit("Z"),
            ).alias("kafka_ts"),
            F.concat(
                F.date_format(
                    F.timestamp_seconds(second + F.lit(2)), "yyyy-MM-dd'T'HH:mm:ss"
                ),
                F.lit("Z"),
            ).alias("ingested_at"),
        )
        .select(*[field.name for field in BRONZE_POWER_SCHEMA.fields])
    )


def write_bronze(
    spark,
    storage: LocalLakeStorage,
    settings: SilverSettings,
    rows,
    *,
    ingest_date: str = "2026-09-19",
    partition: int = 0,
    name: str = "part-0",
    business_dates=None,
    committed_at: datetime | None = None,
) -> str:
    """Bronze Parquet 한 덩어리와 그 manifest를 쓴다."""

    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F

    from power_silver.schemas import BRONZE_POWER_SCHEMA

    if isinstance(rows, DataFrame):
        frame = rows
    else:
        ordered = [
            tuple(row[field.name] for field in BRONZE_POWER_SCHEMA.fields)
            for row in rows
        ]
        frame = spark.createDataFrame(ordered, BRONZE_POWER_SCHEMA)

    path = (
        f"{settings.bronze_base}/ingest_date={ingest_date}/hour=00"
        f"/partition={partition}/{name}.parquet"
    )
    # 한 파일로 만들어 테스트가 경로를 그대로 manifest에 적을 수 있게 한다.
    directory = path + ".d"
    frame.coalesce(1).write.mode("overwrite").parquet(storage.uri(directory))
    written = [
        item for item in storage.walk_files(directory) if item.path.endswith(".parquet")
    ]
    assert len(written) == 1, written
    storage.rename(written[0].path, path)
    storage.delete(directory, recursive=True)

    summary = frame.agg(
        F.min("measured_at").alias("first"),
        F.max("measured_at").alias("last"),
        F.min("kafka_offset").alias("start_offset"),
        F.max("kafka_offset").alias("end_offset"),
        F.count(F.lit(1)).alias("rows"),
    ).collect()[0]
    minimum = datetime.fromisoformat(summary["first"].replace("Z", "+00:00"))
    maximum = datetime.fromisoformat(summary["last"].replace("Z", "+00:00"))
    if business_dates is None:
        business = timezone(timedelta(seconds=KST_OFFSET))
        first_day = minimum.astimezone(business).date()
        last_day = maximum.astimezone(business).date()
        business_dates = [
            (first_day + timedelta(days=offset)).isoformat()
            for offset in range((last_day - first_day).days + 1)
        ]
    manifest = {
        "topic": TOPIC,
        "partition": partition,
        "start_offset": summary["start_offset"],
        "end_offset": summary["end_offset"],
        "ok_count": summary["rows"],
        "quarantine_count": 0,
        "min_measured_at": minimum.astimezone(timezone.utc).isoformat(),
        "max_measured_at": maximum.astimezone(timezone.utc).isoformat(),
        "business_dates": list(business_dates),
        "file_bytes": storage.status(path).length,
        "files": [path],
        "flush_reason": "test",
        "committed_at": (
            committed_at or datetime.fromtimestamp(DAY_END_EPOCH + 2000, timezone.utc)
        ).isoformat(),
    }
    manifest_path = (
        f"{settings.bronze_manifest_base}/date={ingest_date}"
        f"/manifest-{partition}-{name}.json"
    )
    storage.write_bytes(manifest_path, json.dumps(manifest, ensure_ascii=False).encode())
    return path
