"""상위 재처리가 하위를 다시 계산하게 만드는지 확인한다.

power-silver 자신은 레이크 상위가 없다(Bronze에서 읽는다). 그래서 여기서는 하위 작업을
흉내 낸다 — `appliance_usage_daily`라는 소비자가 관측일을 읽고 확정했다고 기록한 뒤,
관측일을 재처리했을 때 그 날짜가 다시 계산 대상으로 잡히는지 본다.
"""

from __future__ import annotations

from datetime import date
from uuid import uuid4

from power_silver.catalog import (
    DIRTY_DEPENDENCY_UNKNOWN,
    DIRTY_NEVER_PROCESSED,
    DIRTY_UPSTREAM_CHANGED,
    SilverCatalog,
)
from power_silver.commit import SilverCommitRepository, dependency_entries
from power_silver.constants import DATASET_OBSERVATION

from test_commit import DATASETS, TARGET, build_manifest, start


CONSUMER_JOB = "appliance-usage-daily"
CONSUMER_DATASET = "appliance_usage_daily"


def consumer_manifest(run_id, depends_on):
    return {
        "job": CONSUMER_JOB,
        "run_id": str(run_id),
        "attempt": 1,
        "target_date": TARGET.isoformat(),
        "input_snapshot_id": "gold-snap-1",
        "rule_version": "appliance-usage-v1",
        "config_version": "c@1",
        "outputs": {
            CONSUMER_DATASET: {
                "path": f"/nilm/gold/{CONSUMER_DATASET}/usage_date={TARGET}/run_id={run_id}",
                "row_count": 18,
            }
        },
        "depends_on": dependency_entries(depends_on),
        "completed_at": "2026-09-20T02:00:00+00:00",
    }


def publish_consumer(session_factory, catalog):
    """하위 작업이 관측일 활성 버전을 고정해 읽고 결과를 확정한 상황."""

    upstream = catalog.active_versions(DATASET_OBSERVATION, [TARGET])
    repository = SilverCommitRepository(session_factory, job_name=CONSUMER_JOB)
    handle = repository.start(
        TARGET,
        input_snapshot_id="gold-snap-1",
        rule_version="appliance-usage-v1",
        config_version="c@1",
    )
    depends_on = list(upstream.values())
    repository.publish(consumer_manifest(handle.run_id, depends_on), depends_on)
    return handle, depends_on


def test_a_reprocessed_upstream_date_becomes_dirty(session_factory):
    silver = SilverCommitRepository(session_factory)
    catalog = SilverCatalog(session_factory)

    first = start(silver)
    silver.publish(build_manifest(first.run_id))

    # 아직 아무도 소비하지 않았다.
    dirty = catalog.dirty_dates(CONSUMER_DATASET, DATASET_OBSERVATION)
    assert [(item.target_date, item.reason) for item in dirty] == [
        (TARGET, DIRTY_NEVER_PROCESSED)
    ]

    publish_consumer(session_factory, catalog)
    assert catalog.dirty_dates(CONSUMER_DATASET, DATASET_OBSERVATION) == ()

    # 늦게 도착한 데이터로 관측일을 재처리한다.
    second = start(silver, snapshot_id="snap-2")
    silver.publish(build_manifest(second.run_id, snapshot_id="snap-2"))

    dirty = catalog.dirty_dates(CONSUMER_DATASET, DATASET_OBSERVATION)
    assert len(dirty) == 1
    assert dirty[0].reason == DIRTY_UPSTREAM_CHANGED
    assert dirty[0].upstream_run_id == second.run_id
    assert dirty[0].consumed_run_id == first.run_id


def test_a_result_without_a_recorded_dependency_is_dirty(session_factory):
    silver = SilverCommitRepository(session_factory)
    catalog = SilverCatalog(session_factory)
    handle = start(silver)
    silver.publish(build_manifest(handle.run_id))

    # 의존성을 남기지 않고 확정한 하위 결과(예전 구현으로 만든 것).
    consumer = SilverCommitRepository(session_factory, job_name=CONSUMER_JOB)
    run = consumer.start(
        TARGET,
        input_snapshot_id="gold-snap-1",
        rule_version="appliance-usage-v1",
        config_version="c@1",
    )
    consumer.publish(consumer_manifest(run.run_id, []))

    dirty = catalog.dirty_dates(CONSUMER_DATASET, DATASET_OBSERVATION)
    assert [item.reason for item in dirty] == [DIRTY_DEPENDENCY_UNKNOWN]


def test_recovery_restores_the_dependency_from_the_manifest(
    session_factory, lake, settings
):
    from power_silver.commit import manifest_path, write_manifest

    silver = SilverCommitRepository(session_factory)
    catalog = SilverCatalog(session_factory)
    handle = start(silver)
    silver.publish(build_manifest(handle.run_id))

    upstream = list(catalog.active_versions(DATASET_OBSERVATION, [TARGET]).values())
    consumer = SilverCommitRepository(session_factory, job_name=CONSUMER_JOB)
    run = consumer.start(
        TARGET,
        input_snapshot_id="gold-snap-1",
        rule_version="appliance-usage-v1",
        config_version="c@1",
    )
    # 파일·manifest는 확정됐지만 DB 반영 전에 죽었다. 의존성도 함께 롤백됐다.
    manifest = consumer_manifest(run.run_id, upstream)
    path = manifest_path("/nilm/manifests/job=appliance-usage-daily", TARGET, run.run_id)
    manifest["manifest_path"] = path
    write_manifest(lake, path, manifest)
    assert catalog.dirty_dates(CONSUMER_DATASET, DATASET_OBSERVATION)[0].reason == (
        DIRTY_NEVER_PROCESSED
    )

    assert consumer.recover(lake, "/nilm/manifests/job=appliance-usage-daily", TARGET) == 1
    # manifest에 실린 의존성으로 되살아나므로 DEPENDENCY_UNKNOWN이 되지 않는다.
    assert catalog.dirty_dates(CONSUMER_DATASET, DATASET_OBSERVATION) == ()


def test_dirty_dates_can_be_limited_to_a_window(session_factory):
    silver = SilverCommitRepository(session_factory)
    catalog = SilverCatalog(session_factory)
    other = date(2026, 9, 18)

    for target_date in (TARGET, other):
        run = silver.start(
            target_date,
            input_snapshot_id="snap-1",
            rule_version="power-silver-v1",
            config_version="c@1",
        )
        manifest = build_manifest(run.run_id)
        manifest["target_date"] = target_date.isoformat()
        silver.publish(manifest)

    everything = catalog.dirty_dates(CONSUMER_DATASET, DATASET_OBSERVATION)
    assert {item.target_date for item in everything} == {TARGET, other}

    window = catalog.dirty_dates(CONSUMER_DATASET, DATASET_OBSERVATION, dates=[other])
    assert [item.target_date for item in window] == [other]
    assert catalog.dirty_dates(CONSUMER_DATASET, DATASET_OBSERVATION, dates=[]) == ()


def test_active_versions_carries_the_ids_a_consumer_must_pin(session_factory):
    silver = SilverCommitRepository(session_factory)
    catalog = SilverCatalog(session_factory)
    handle = start(silver)
    silver.publish(build_manifest(handle.run_id))

    for dataset in DATASETS:
        ref = catalog.active_version(dataset, TARGET)
        assert ref.run_id == handle.run_id
        assert ref.version_id is not None
        assert ref.input_snapshot_id == "snap-1"
        assert ref.output_path.endswith(str(handle.run_id))
    assert catalog.active_path(DATASET_OBSERVATION, date(2026, 1, 1)) is None
    assert catalog.missing_dates(DATASET_OBSERVATION, [TARGET, date(2026, 1, 1)]) == (
        date(2026, 1, 1),
    )


def test_dependencies_survive_a_republish_of_the_same_run(session_factory):
    silver = SilverCommitRepository(session_factory)
    catalog = SilverCatalog(session_factory)
    handle = start(silver)
    silver.publish(build_manifest(handle.run_id))

    consumer_handle, depends_on = publish_consumer(session_factory, catalog)
    consumer = SilverCommitRepository(session_factory, job_name=CONSUMER_JOB)
    consumer.publish(
        consumer_manifest(consumer_handle.run_id, depends_on), depends_on
    )

    with session_factory() as session:
        from realtime_analysis.models import LakeDatasetDependency

        rows = session.query(LakeDatasetDependency).all()
    assert len(rows) == 1
    assert rows[0].upstream_run_id == handle.run_id


def test_an_unrelated_consumer_run_does_not_clear_the_dirty_flag(session_factory):
    silver = SilverCommitRepository(session_factory)
    catalog = SilverCatalog(session_factory)
    handle = start(silver)
    silver.publish(build_manifest(handle.run_id))

    # 다른 날짜를 확정한 하위 실행은 이 날짜의 상태를 바꾸지 않는다.
    consumer = SilverCommitRepository(session_factory, job_name=CONSUMER_JOB)
    other = date(2026, 9, 18)
    run = consumer.start(
        other,
        input_snapshot_id="gold-snap-0",
        rule_version="appliance-usage-v1",
        config_version="c@1",
    )
    manifest = consumer_manifest(run.run_id, [])
    manifest["target_date"] = other.isoformat()
    manifest["outputs"][CONSUMER_DATASET]["path"] = f"/nilm/gold/x/{uuid4()}"
    consumer.publish(manifest)

    dirty = catalog.dirty_dates(CONSUMER_DATASET, DATASET_OBSERVATION)
    assert [(item.target_date, item.reason) for item in dirty] == [
        (TARGET, DIRTY_NEVER_PROCESSED)
    ]
