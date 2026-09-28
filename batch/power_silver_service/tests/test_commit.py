from __future__ import annotations

from datetime import date
from uuid import uuid4

import pytest

from power_silver.commit import (
    ConflictingRunHistory,
    LockNotAcquired,
    SilverCommitRepository,
    manifest_path,
    write_manifest,
)
from power_silver.constants import (
    DATASET_OBSERVATION,
    DATASET_POWER_CLEAN,
    RUN_RUNNING,
    RUN_SUCCEEDED,
    VERSION_SUPERSEDED,
)


TARGET = date(2026, 9, 19)
DATASETS = (DATASET_POWER_CLEAN, DATASET_OBSERVATION)


def build_manifest(run_id, *, snapshot_id="snap-1", rule="power-silver-v1", config="c@1"):
    return {
        "job": "power-silver-daily",
        "run_id": str(run_id),
        "attempt": 1,
        "target_date": TARGET.isoformat(),
        "input_snapshot_id": snapshot_id,
        "rule_version": rule,
        "config_version": config,
        "outputs": {
            DATASET_POWER_CLEAN: {
                "path": f"/nilm/silver/power_clean/event_date={TARGET}/run_id={run_id}",
                "row_count": 10,
            },
            DATASET_OBSERVATION: {
                "path": (
                    f"/nilm/silver/household_observation_daily"
                    f"/observation_date={TARGET}/run_id={run_id}"
                ),
                "row_count": 3,
            },
        },
        "bytes_per_row": {DATASET_POWER_CLEAN: 21.5},
        "completed_at": "2026-09-20T01:00:00+00:00",
    }


def start(repository, **kwargs):
    return repository.start(
        TARGET,
        input_snapshot_id=kwargs.get("snapshot_id", "snap-1"),
        rule_version=kwargs.get("rule", "power-silver-v1"),
        config_version=kwargs.get("config", "c@1"),
    )


def test_publishing_makes_the_run_active_and_reusable(session_factory):
    repository = SilverCommitRepository(session_factory)
    handle = start(repository)
    repository.publish(build_manifest(handle.run_id))

    for dataset in DATASETS:
        version = repository.active_version(dataset, TARGET)
        assert version is not None and version.run_id == handle.run_id

    assert (
        repository.completed_run(
            TARGET,
            input_snapshot_id="snap-1",
            rule_version="power-silver-v1",
            config_version="c@1",
            dataset_names=DATASETS,
        )
        == handle.run_id
    )


def test_a_new_input_snapshot_supersedes_the_previous_version(session_factory):
    repository = SilverCommitRepository(session_factory)
    first = start(repository)
    repository.publish(build_manifest(first.run_id))

    second = start(repository, snapshot_id="snap-2")
    assert second.attempt == 2
    repository.publish(build_manifest(second.run_id, snapshot_id="snap-2"))

    active = repository.active_version(DATASET_POWER_CLEAN, TARGET)
    assert active.run_id == second.run_id
    with session_factory() as session:
        from realtime_analysis.models import LakeDatasetVersion

        statuses = {
            str(row.run_id): row.status
            for row in session.query(LakeDatasetVersion)
            .filter(LakeDatasetVersion.dataset_name == DATASET_POWER_CLEAN)
            .all()
        }
    assert statuses[str(first.run_id)] == VERSION_SUPERSEDED

    # 입력이 달라졌으므로 예전 스냅샷으로는 재사용할 수 없다.
    assert (
        repository.completed_run(
            TARGET,
            input_snapshot_id="snap-1",
            rule_version="power-silver-v1",
            config_version="c@1",
            dataset_names=DATASETS,
        )
        is None
    )


def test_a_changed_rule_version_is_not_reused(session_factory):
    repository = SilverCommitRepository(session_factory)
    handle = start(repository)
    repository.publish(build_manifest(handle.run_id))
    assert (
        repository.completed_run(
            TARGET,
            input_snapshot_id="snap-1",
            rule_version="power-silver-v2",
            config_version="c@1",
            dataset_names=DATASETS,
        )
        is None
    )


def test_publishing_twice_leaves_one_active_version(session_factory):
    repository = SilverCommitRepository(session_factory)
    handle = start(repository)
    manifest = build_manifest(handle.run_id)
    repository.publish(manifest)
    repository.publish(manifest)

    with session_factory() as session:
        from realtime_analysis.models import LakeDatasetVersion

        rows = (
            session.query(LakeDatasetVersion)
            .filter(LakeDatasetVersion.dataset_name == DATASET_POWER_CLEAN)
            .all()
        )
    assert len(rows) == 1


def test_recover_publishes_a_manifest_written_before_the_database_update(
    session_factory, lake, settings
):
    repository = SilverCommitRepository(session_factory)
    handle = start(repository)
    manifest = build_manifest(handle.run_id)
    path = manifest_path(settings.manifest_base, TARGET, handle.run_id)
    manifest["manifest_path"] = path
    write_manifest(lake, path, manifest)

    # 파일은 확정됐지만 DB에는 아직 RUNNING으로 남아 있다.
    with session_factory() as session:
        from realtime_analysis.models import LakeBatchRun

        assert session.get(LakeBatchRun, handle.run_id).status == RUN_RUNNING
    assert repository.active_version(DATASET_POWER_CLEAN, TARGET) is None

    assert repository.recover(lake, settings.manifest_base, TARGET) == 1
    assert repository.active_version(DATASET_POWER_CLEAN, TARGET).run_id == handle.run_id
    # 두 번째 복구는 아무것도 하지 않는다.
    assert repository.recover(lake, settings.manifest_base, TARGET) == 0


def test_recover_rebuilds_a_run_row_that_no_longer_exists(
    session_factory, lake, settings
):
    repository = SilverCommitRepository(session_factory)
    run_id = uuid4()
    manifest = build_manifest(run_id)
    path = manifest_path(settings.manifest_base, TARGET, run_id)
    manifest["manifest_path"] = path
    write_manifest(lake, path, manifest)

    assert repository.recover(lake, settings.manifest_base, TARGET) == 1
    with session_factory() as session:
        from realtime_analysis.models import LakeBatchRun

        assert session.get(LakeBatchRun, run_id).status == RUN_SUCCEEDED


def test_recovering_an_older_run_does_not_demote_the_newer_active_version(
    session_factory, lake, settings
):
    repository = SilverCommitRepository(session_factory)

    # 오래된 실행: 파일과 manifest는 확정됐지만 DB 반영 직전에 죽었다.
    stale = start(repository)
    stale_manifest = build_manifest(stale.run_id)
    stale_path = manifest_path(settings.manifest_base, TARGET, stale.run_id)
    stale_manifest["manifest_path"] = stale_path
    write_manifest(lake, stale_path, stale_manifest)

    # 그 사이 새 입력으로 다음 실행이 정상 확정됐다.
    fresh = start(repository, snapshot_id="snap-2")
    repository.publish(build_manifest(fresh.run_id, snapshot_id="snap-2"))
    assert repository.active_version(DATASET_POWER_CLEAN, TARGET).run_id == fresh.run_id

    # 뒤늦은 복구가 최신 결과를 활성 자리에서 밀어내면 안 된다.
    assert repository.recover(lake, settings.manifest_base, TARGET) == 1
    for dataset in DATASETS:
        assert repository.active_version(dataset, TARGET).run_id == fresh.run_id

    with session_factory() as session:
        from realtime_analysis.models import LakeBatchRun, LakeDatasetVersion

        stale_run = session.get(LakeBatchRun, stale.run_id)
        # 결과 자체는 확정됐으므로 성공이다. 다만 활성 자리는 가져가지 않았다.
        assert stale_run.status == RUN_SUCCEEDED
        assert stale_run.details["activated"] is False
        versions = {
            str(row.run_id): row.status
            for row in session.query(LakeDatasetVersion)
            .filter(LakeDatasetVersion.dataset_name == DATASET_POWER_CLEAN)
            .all()
        }
    assert versions == {
        str(stale.run_id): VERSION_SUPERSEDED,
        str(fresh.run_id): "ACTIVE",
    }


def test_a_manifest_claiming_another_runs_attempt_is_refused(
    session_factory, lake, settings
):
    repository = SilverCommitRepository(session_factory)
    start(repository)  # attempt 1을 이미 다른 실행이 쓰고 있다

    orphan = build_manifest(uuid4())
    orphan["attempt"] = 1
    with pytest.raises(ConflictingRunHistory):
        repository.publish(orphan)


def test_the_date_lock_refuses_a_second_writer(session_factory):
    first = SilverCommitRepository(session_factory)
    second = SilverCommitRepository(session_factory)

    with first.date_lock(TARGET):
        with pytest.raises(LockNotAcquired):
            with second.date_lock(TARGET):
                pass
        # 다른 날짜는 막지 않는다.
        with second.date_lock(date(2026, 9, 18)):
            pass

    # 풀린 뒤에는 잡을 수 있다.
    with second.date_lock(TARGET):
        pass


def test_the_date_lock_is_reentrant_within_one_repository(session_factory):
    repository = SilverCommitRepository(session_factory)
    with repository.date_lock(TARGET):
        # run_daily가 잠근 상태에서 recover가 다시 잠근다.
        with repository.date_lock(TARGET):
            pass
        handle = start(repository)
        repository.publish(build_manifest(handle.run_id))
    assert repository.active_version(DATASET_POWER_CLEAN, TARGET) is not None


def test_bytes_per_row_of_the_latest_successful_run_is_reused(session_factory):
    repository = SilverCommitRepository(session_factory)
    assert repository.latest_bytes_per_row(DATASET_POWER_CLEAN) is None

    handle = start(repository)
    repository.publish(build_manifest(handle.run_id))

    assert repository.latest_bytes_per_row(DATASET_POWER_CLEAN) == 21.5
