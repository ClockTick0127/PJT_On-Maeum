"""End-to-end runs of power-silver-daily on a local Spark and a local lake."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from power_silver.commit import SilverCommitRepository
from power_silver.constants import (
    DATASET_OBSERVATION,
    DATASET_POWER_CLEAN,
    FLAG_MESSAGE_CONFLICT,
    OBSERVATION_INSUFFICIENT,
    OBSERVATION_SENSOR_GAP,
    OBSERVATION_VALID,
    REJECT_MEASUREMENT_CONFLICT,
    REJECT_MESSAGE_CONFLICT,
    RUN_WAITING_INPUT,
)
from power_silver.job import run_daily
from power_silver.targets import load_targets

from conftest import (
    DAY_END_EPOCH,
    DAY_START_EPOCH,
    TARGET_DATE,
    bronze_rows,
    day_frame,
    iso,
    write_bronze,
    write_targets,
)


pytestmark = pytest.mark.spark

AFTER_THE_DAY = datetime.fromtimestamp(DAY_END_EPOCH + 3600, timezone.utc)


class Harness:
    def __init__(self, spark, lake, settings, session_factory):
        self.spark = spark
        self.lake = lake
        self.settings = settings
        self.repository = SilverCommitRepository(session_factory)
        self.targets = load_targets(settings.observation_targets_file)

    def reload_targets(self):
        self.targets = load_targets(self.settings.observation_targets_file)

    def run(self, *, now=AFTER_THE_DAY, force=False):
        return run_daily(
            self.settings,
            TARGET_DATE,
            storage=self.lake,
            targets=self.targets,
            repository=self.repository,
            spark=self.spark,
            now=now,
            force=force,
        )

    def observation(self):
        version = self.repository.active_version(DATASET_OBSERVATION, TARGET_DATE)
        assert version is not None
        rows = self.spark.read.parquet(self.lake.uri(version.output_path)).collect()
        return {row["household_id"]: row for row in rows}

    def power_clean(self):
        version = self.repository.active_version(DATASET_POWER_CLEAN, TARGET_DATE)
        assert version is not None
        return self.spark.read.parquet(self.lake.uri(version.output_path))

    def quarantine(self, run_id):
        path = f"{self.settings.quarantine_base}/target_date={TARGET_DATE}/run_id={run_id}"
        return self.spark.read.parquet(self.lake.uri(path))


@pytest.fixture
def harness(spark, lake, settings, session_factory):
    return Harness(spark, lake, settings, session_factory)


def test_coverage_thresholds_and_a_household_without_any_data(
    spark, harness, tmp_path
):
    write_targets(
        tmp_path / "observation_targets.json", ["H001", "H002", "H003", "H004"]
    )
    harness.reload_targets()

    # H001: 하루 전체 1Hz / H002: 정확히 95% / H003: 95%보다 한 구간 부족 / H004: 없음
    for household_id, count in (("H001", 86_400), ("H002", 82_080), ("H003", 82_079)):
        # 가구마다 다른 Kafka 파티션을 쓴다. 같은 (파티션, offset)을 나눠 쓰면 서로 다른
        # 가구의 행이 같은 Kafka 레코드로 보여 충돌 격리된다.
        partition = int(household_id[-1])
        write_bronze(
            spark,
            harness.lake,
            harness.settings,
            day_frame(
                spark,
                household_id=household_id,
                start_epoch=DAY_START_EPOCH,
                count=count,
                partition=partition,
                first_offset=0,
            ),
            partition=partition,
            name=household_id,
        )

    result = harness.run()
    assert result.status == "SUCCEEDED"

    rows = harness.observation()
    assert set(rows) == {"H001", "H002", "H003", "H004"}

    assert rows["H001"]["observed_slot_count"] == 86_400
    assert rows["H001"]["expected_sample_count"] == 86_400
    assert rows["H001"]["coverage_ratio"] == pytest.approx(1.0)
    assert rows["H001"]["observation_status"] == OBSERVATION_VALID
    assert rows["H001"]["max_missing_seconds"] == 0

    assert rows["H002"]["observed_slot_count"] == 82_080
    assert rows["H002"]["coverage_ratio"] == pytest.approx(0.95)
    assert rows["H002"]["observation_status"] == OBSERVATION_VALID
    assert rows["H002"]["max_missing_seconds"] == 86_400 - 82_080

    assert rows["H003"]["observed_slot_count"] == 82_079
    assert rows["H003"]["observation_status"] == OBSERVATION_INSUFFICIENT

    assert rows["H004"]["observed_slot_count"] == 0
    assert rows["H004"]["valid_measurement_count"] == 0
    assert rows["H004"]["observation_status"] == OBSERVATION_SENSOR_GAP
    assert rows["H004"]["max_missing_seconds"] == 86_400
    assert rows["H004"]["first_measured_at"] is None

    assert harness.power_clean().count() == 86_400 + 82_080 + 82_079


def test_reloading_the_same_kafka_records_changes_nothing(spark, harness):
    rows = bronze_rows(
        household_id="H001",
        seconds=[DAY_START_EPOCH + offset for offset in range(10)],
        first_offset=100,
    )
    write_bronze(spark, harness.lake, harness.settings, rows, name="a")
    write_bronze(
        spark,
        harness.lake,
        harness.settings,
        rows,
        ingest_date="2026-09-20",
        name="a-again",
    )

    harness.run()
    observation = harness.observation()["H001"]
    assert observation["observed_slot_count"] == 10
    assert observation["valid_measurement_count"] == 10
    assert observation["duplicate_count"] == 10
    assert harness.power_clean().count() == 10


def test_the_same_message_id_with_different_content_is_quarantined(spark, harness):
    first = bronze_rows(
        household_id="H001",
        seconds=[DAY_START_EPOCH],
        first_offset=1,
        active_power=100.0,
    )
    second = bronze_rows(
        household_id="H001",
        seconds=[DAY_START_EPOCH],
        first_offset=2,
        active_power=250.0,
    )
    assert first[0]["message_id"] == second[0]["message_id"]
    write_bronze(spark, harness.lake, harness.settings, first + second, name="a")

    result = harness.run()
    assert harness.power_clean().count() == 0

    observation = harness.observation()["H001"]
    assert observation["invalid_count"] == 2
    assert FLAG_MESSAGE_CONFLICT in observation["quality_flags"]
    assert observation["observation_status"] == OBSERVATION_SENSOR_GAP

    quarantined = harness.quarantine(result.run_id).collect()
    assert {row["reject_code"] for row in quarantined} == {REJECT_MESSAGE_CONFLICT}
    assert len(quarantined) == 2


def test_republishing_the_same_measurements_with_new_ids_is_deduplicated(spark, harness):
    """시뮬레이터를 같은 측정시각으로 재실행하면 message_id와 offset이 모두 바뀐다."""
    seconds = [DAY_START_EPOCH + offset for offset in range(10)]
    original = bronze_rows(household_id="H001", seconds=seconds, first_offset=100)
    rerun = bronze_rows(
        household_id="H001",
        seconds=seconds,
        first_offset=500,
        message_seed=1000,
        ingested_at=iso(DAY_START_EPOCH + 3600, 0),
    )
    assert {row["message_id"] for row in original}.isdisjoint(
        row["message_id"] for row in rerun
    )
    write_bronze(spark, harness.lake, harness.settings, original, name="a")
    write_bronze(spark, harness.lake, harness.settings, rerun, name="a-rerun")

    harness.run()
    observation = harness.observation()["H001"]
    assert observation["observed_slot_count"] == 10
    assert observation["valid_measurement_count"] == 10
    assert observation["duplicate_count"] == 10
    assert observation["invalid_count"] == 0

    kept = harness.power_clean().collect()
    assert len(kept) == 10
    # 수신시각이 앞선 최초 적재분이 남는다.
    assert {row["kafka_offset"] for row in kept} == set(range(100, 110))


def test_the_same_measurement_with_different_content_is_quarantined(spark, harness):
    first = bronze_rows(
        household_id="H001", seconds=[DAY_START_EPOCH], first_offset=1, active_power=100.0
    )
    second = bronze_rows(
        household_id="H001",
        seconds=[DAY_START_EPOCH],
        first_offset=2,
        message_seed=1000,
        active_power=250.0,
    )
    assert first[0]["message_id"] != second[0]["message_id"]
    write_bronze(spark, harness.lake, harness.settings, first + second, name="a")

    result = harness.run()
    assert harness.power_clean().count() == 0

    observation = harness.observation()["H001"]
    assert observation["invalid_count"] == 2
    assert FLAG_MESSAGE_CONFLICT in observation["quality_flags"]
    assert observation["observation_status"] == OBSERVATION_SENSOR_GAP

    quarantined = harness.quarantine(result.run_id).collect()
    assert {row["reject_code"] for row in quarantined} == {REJECT_MEASUREMENT_CONFLICT}
    assert len(quarantined) == 2


def test_rows_are_split_by_the_korean_business_date(spark, harness):
    seconds = [
        DAY_START_EPOCH - 1,  # 전날 23:59:59 KST
        DAY_START_EPOCH,  # 당일 00:00:00 KST
        DAY_END_EPOCH - 1,  # 당일 23:59:59 KST
        DAY_END_EPOCH,  # 다음날 00:00:00 KST
    ]
    write_bronze(
        spark,
        harness.lake,
        harness.settings,
        bronze_rows(household_id="H001", seconds=seconds, first_offset=0),
        name="boundary",
    )

    harness.run()
    clean = harness.power_clean().collect()
    assert len(clean) == 2
    assert {str(row["event_date"]) for row in clean} == {"2026-09-19"}

    observation = harness.observation()["H001"]
    # 전날 23:59:59와 다음날 00:00:00은 하루 안의 좌표로는 이 날의 슬롯과 겹친다.
    # 측정일로 먼저 거르지 않으면 관측 건수가 2가 아니라 4가 된다.
    assert observation["valid_measurement_count"] == 2
    assert observation["observed_slot_count"] == 2
    assert observation["max_missing_seconds"] == 86_400 - 2


def test_late_data_for_yesterday_is_included_by_a_corrective_run(spark, harness):
    write_bronze(
        spark,
        harness.lake,
        harness.settings,
        bronze_rows(
            household_id="H001",
            seconds=[DAY_START_EPOCH + offset for offset in range(5)],
            first_offset=0,
        ),
        name="first",
    )
    first = harness.run()
    assert harness.observation()["H001"]["observed_slot_count"] == 5

    # 다음날 도착한 전날 데이터. 수집일 폴더는 09-20이지만 business_dates가 09-19다.
    write_bronze(
        spark,
        harness.lake,
        harness.settings,
        bronze_rows(
            household_id="H001",
            seconds=[DAY_START_EPOCH + 100 + offset for offset in range(3)],
            first_offset=500,
            message_seed=500,
        ),
        ingest_date="2026-09-20",
        name="late",
    )
    second = harness.run()

    assert second.run_id != first.run_id
    assert second.reused_run_id is None
    assert harness.observation()["H001"]["observed_slot_count"] == 8
    assert harness.power_clean().count() == 8

    version = harness.repository.active_version(DATASET_POWER_CLEAN, TARGET_DATE)
    assert str(version.run_id) == second.run_id


def test_an_unchanged_input_is_not_recomputed(spark, harness):
    write_bronze(
        spark,
        harness.lake,
        harness.settings,
        bronze_rows(
            household_id="H001",
            seconds=[DAY_START_EPOCH + offset for offset in range(4)],
        ),
        name="a",
    )
    first = harness.run()
    second = harness.run()
    assert second.reused_run_id == first.run_id
    assert second.run_id is None

    # 과거 실행을 레이크만으로 재현할 수 있도록 그때 쓴 가구·기기 매핑을 함께 남긴다.
    manifest = first.manifest
    assert manifest["observation_targets"]["config_version"] == "test-targets-v1"
    assert {
        segment["household_id"] for segment in manifest["observation_segments"]
    } == {"H001", "H002", "H003"}
    assert manifest["row_states"]["clean_rows_for_target_date"] == 4


def test_a_database_failure_after_the_manifest_is_repaired_by_the_next_run(
    spark, harness, monkeypatch
):
    write_bronze(
        spark,
        harness.lake,
        harness.settings,
        bronze_rows(
            household_id="H001",
            seconds=[DAY_START_EPOCH + offset for offset in range(4)],
        ),
        name="a",
    )

    original = harness.repository.publish
    calls = {"count": 0}

    def failing_publish(manifest):
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("database is unreachable")
        return original(manifest)

    monkeypatch.setattr(harness.repository, "publish", failing_publish)

    with pytest.raises(RuntimeError):
        harness.run()
    assert harness.repository.active_version(DATASET_POWER_CLEAN, TARGET_DATE) is None

    repaired = harness.run()
    assert repaired.recovered_runs == 1
    # 같은 입력이므로 다시 계산하지 않고 이미 확정된 파일을 그대로 쓴다.
    assert repaired.reused_run_id is not None
    assert harness.power_clean().count() == 4
    assert harness.observation()["H001"]["observed_slot_count"] == 4


def test_the_run_waits_while_a_kafka_partition_is_behind(spark, harness):
    write_bronze(
        spark,
        harness.lake,
        harness.settings,
        bronze_rows(household_id="H001", seconds=[DAY_START_EPOCH]),
        name="a",
        committed_at=datetime.fromtimestamp(DAY_END_EPOCH - 3600, timezone.utc),
        business_dates=["2026-09-19"],
    )
    # 이 manifest의 측정값은 날짜 경계를 넘지 않았고 커밋도 유예시간 전이다.
    result = harness.run(
        now=datetime.fromtimestamp(DAY_END_EPOCH + 600, timezone.utc)
    )
    assert result.status == RUN_WAITING_INPUT
    assert result.wait_reason == "PARTITIONS_BEHIND:0"
    assert harness.repository.active_version(DATASET_OBSERVATION, TARGET_DATE) is None


def test_a_second_writer_skips_a_date_another_run_owns(spark, harness, session_factory):
    write_bronze(
        spark,
        harness.lake,
        harness.settings,
        bronze_rows(household_id="H001", seconds=[DAY_START_EPOCH]),
        name="a",
    )
    other = SilverCommitRepository(session_factory)
    with other.date_lock(TARGET_DATE):
        result = harness.run()

    assert result.status == "SKIPPED"
    assert result.wait_reason == "ANOTHER_WRITER"
    # 실행 기록도 출력도 남기지 않는다.
    assert harness.repository.active_version(DATASET_OBSERVATION, TARGET_DATE) is None

    # 잠금이 풀리면 평소대로 돈다.
    assert harness.run().status == "SUCCEEDED"


def test_the_result_does_not_depend_on_the_shuffle_partition_count(spark, harness):
    rows = bronze_rows(
        household_id="H001",
        seconds=[DAY_START_EPOCH + offset for offset in range(50)],
        first_offset=0,
    )
    duplicates = bronze_rows(
        household_id="H001",
        seconds=[DAY_START_EPOCH + offset for offset in range(50)],
        first_offset=0,
    )
    write_bronze(spark, harness.lake, harness.settings, rows, name="a")
    write_bronze(
        spark,
        harness.lake,
        harness.settings,
        duplicates,
        ingest_date="2026-09-20",
        name="b",
    )

    results = []
    for partitions in ("1", "7"):
        spark.conf.set("spark.sql.shuffle.partitions", partitions)
        harness.run(force=True)
        clean = harness.power_clean()
        results.append(
            (
                harness.observation()["H001"].asDict(),
                sorted(row["message_id"] for row in clean.collect()),
                sorted(
                    (row["kafka_partition"], row["kafka_offset"])
                    for row in clean.collect()
                ),
            )
        )
    spark.conf.set("spark.sql.shuffle.partitions", "4")

    first, second = results
    for key in ("observed_slot_count", "valid_measurement_count", "duplicate_count"):
        assert first[0][key] == second[0][key]
    assert first[1] == second[1]
    assert first[2] == second[2]
