"""Loader behaviour on SQLite: batching, failures, resume, duplicates, ordering."""

from __future__ import annotations

import json
from uuid import UUID

import pytest

from session_lake_loader.lake_schema import read_parquet_rows
from session_lake_loader.loader import LakeIntegrityError, SessionLakeLoader
from session_lake_loader.repository import SessionLakeRepository
from session_lake_loader.storage import LocalLakeStorage
from session_lake_loader.verifier import SessionLakeVerifier, restore_latest_state

from conftest import BRONZE_BASE, FakeClock, SessionFactoryHelper


class FlakyStorage:
    """Delegates to a real storage but fails the N-th write."""

    def __init__(self, inner: LocalLakeStorage, fail_on_write: int | None = None) -> None:
        self._inner = inner
        self.fail_on_write = fail_on_write
        self.writes = 0

    def write_bytes(self, path: str, data: bytes) -> None:
        self.writes += 1
        if self.fail_on_write is not None and self.writes == self.fail_on_write:
            raise IOError("simulated HDFS upload failure")
        self._inner.write_bytes(path, data)

    def __getattr__(self, name: str):
        return getattr(self._inner, name)


def cycle(loader: SessionLakeLoader):
    return loader.run_incremental_cycle(batch_max_events=100, max_batches=10)


def lake_rows(storage: LocalLakeStorage, manifest_path: str) -> list[dict]:
    manifest = json.loads(storage.read_bytes(manifest_path))
    rows: list[dict] = []
    for entry in manifest["files"]:
        rows.extend(read_parquet_rows(storage.read_bytes(entry["path"])))
    return rows


def test_incremental_batch_delivers_pending_events_and_writes_manifest(
    loader: SessionLakeLoader,
    repository: SessionLakeRepository,
    storage: LocalLakeStorage,
    sessions: SessionFactoryHelper,
    clock: FakeClock,
) -> None:
    first = sessions.create()
    clock.advance(minutes=5)
    sessions.finish(first)
    second = sessions.create(appliance_type="KETTLE")

    results = cycle(loader)

    assert [r.status for r in results] == ["COMPLETED"]
    batch = repository.get_batch(UUID(results[0].batch_id))
    assert batch.status == "COMPLETED"
    assert batch.event_count == 3 and batch.row_count == 3
    assert batch.ingest_date.isoformat() == "2026-09-20"
    assert batch.manifest_path.startswith(
        "/nilm/manifests/job=session-lake-loader/date=2026-09-20/manifest-"
    )
    assert all(event.delivery_status == "DELIVERED" for event in sessions.outbox())

    manifest = json.loads(storage.read_bytes(batch.manifest_path))
    assert manifest["batch_kind"] == "INCREMENTAL"
    assert manifest["row_count"] == 3
    assert manifest["schema_version"] == 1
    assert manifest["files"][0]["path"].startswith(
        f"{BRONZE_BASE}/ingest_date=2026-09-20/batch_id={batch.batch_id}/part-"
    )
    rows = lake_rows(storage, batch.manifest_path)
    assert [(row["session_id"], row["session_version"], row["operation"]) for row in rows] == [
        (str(first), 1, "INSERT"),
        (str(first), 2, "UPDATE"),
        (str(second), 1, "INSERT"),
    ]
    assert rows[1]["ended_at"] is not None
    assert not storage.list(f"{BRONZE_BASE}/ingest_date=2026-09-20/batch_id={batch.batch_id}")[
        0
    ].endswith(".tmp")

    # 더 이상 전달할 이벤트가 없다.
    assert cycle(loader) == []


def test_upload_failure_keeps_batch_and_events_for_the_same_retry(
    repository: SessionLakeRepository,
    storage: LocalLakeStorage,
    sessions: SessionFactoryHelper,
    clock: FakeClock,
) -> None:
    flaky = FlakyStorage(storage, fail_on_write=1)
    loader = SessionLakeLoader(repository, flaky, retry_backoff_seconds=30, clock=clock)
    session_id = sessions.create()

    failed = cycle(loader)
    assert [r.status for r in failed] == ["FAILED"]
    assert failed[0].stage == "upload"
    batch = repository.get_batch(UUID(failed[0].batch_id))
    assert batch.status == "FAILED"
    assert batch.attempt_count == 1
    assert "simulated HDFS upload failure" in batch.last_error
    events = sessions.outbox()
    assert [e.delivery_status for e in events] == ["ASSIGNED"]
    assert events[0].batch_id == batch.batch_id

    # 백오프 전에는 재시도하지 않고 새 배치도 만들지 않는다.
    flaky.fail_on_write = None
    assert cycle(loader) == []

    clock.advance(seconds=31)
    retried = cycle(loader)
    assert [(r.batch_id, r.status) for r in retried] == [(str(batch.batch_id), "COMPLETED")]
    assert repository.get_batch(batch.batch_id).status == "COMPLETED"
    assert [e.delivery_status for e in sessions.outbox()] == ["DELIVERED"]
    rows = lake_rows(storage, batch.manifest_path)
    assert rows[0]["session_id"] == str(session_id)


def test_crash_after_manifest_before_db_completion_resumes_without_rewriting(
    repository: SessionLakeRepository,
    storage: LocalLakeStorage,
    sessions: SessionFactoryHelper,
    clock: FakeClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    counting = FlakyStorage(storage)
    loader = SessionLakeLoader(repository, counting, clock=clock)
    sessions.create()

    original = repository.mark_batch_completed
    calls = {"count": 0}

    def crash_once(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("database connection lost before completion")
        return original(*args, **kwargs)

    monkeypatch.setattr(repository, "mark_batch_completed", crash_once)

    failed = cycle(loader)
    assert failed[0].status == "FAILED" and failed[0].stage == "finalize"
    batch = repository.get_batch(UUID(failed[0].batch_id))
    assert storage.exists(batch.manifest_path)
    writes_before_resume = counting.writes

    clock.advance(minutes=1)
    resumed = cycle(loader)
    assert resumed[0].status == "COMPLETED"
    assert resumed[0].resumed_from_manifest is True
    assert counting.writes == writes_before_resume  # 파일·manifest를 다시 쓰지 않았다
    assert repository.get_batch(batch.batch_id).status == "COMPLETED"
    assert [e.delivery_status for e in sessions.outbox()] == ["DELIVERED"]


def test_existing_final_file_with_different_content_is_never_overwritten(
    loader: SessionLakeLoader,
    repository: SessionLakeRepository,
    storage: LocalLakeStorage,
    sessions: SessionFactoryHelper,
    clock: FakeClock,
) -> None:
    sessions.create()
    batch = repository.assign_incremental_batch(100, ingest_date=clock().date())
    directory = repository.batch_directory(batch)
    storage.write_bytes(f"{directory}/part-00000.parquet", b"not the same parquet")

    result = loader.process_batch(batch)

    assert result.status == "FAILED"
    assert LakeIntegrityError.__name__ in result.error
    assert storage.read_bytes(f"{directory}/part-00000.parquet") == b"not the same parquet"
    assert repository.get_batch(batch.batch_id).status == "FAILED"


def test_duplicate_and_out_of_order_delivery_restore_to_the_same_latest_state(
    loader: SessionLakeLoader,
    repository: SessionLakeRepository,
    storage: LocalLakeStorage,
    sessions: SessionFactoryHelper,
    clock: FakeClock,
) -> None:
    session_id = sessions.create()
    clock.advance(minutes=1)
    sessions.finish(session_id)
    deleted_id = sessions.create(appliance_type="IRON")
    cycle(loader)  # 배치 A: v1, v2, iron v1

    clock.advance(minutes=1)
    sessions.delete(deleted_id)
    cycle(loader)  # 배치 B: iron DELETE v2

    # 초기 적재를 다시 돌려 SNAPSHOT 중복(같은 세션·버전)을 만든다.
    initial = loader.run_initial_load(allow_repeat=True)
    assert initial.status == "COMPLETED"

    report = SessionLakeVerifier(repository, storage).run()

    assert report.ok, report.to_dict()
    assert report.restore.duplicate_rows == 1  # SNAPSHOT v2 == outbox v2
    assert report.restore.ignored_lower_versions >= 0
    assert report.restore.live_count == 1
    assert report.restore.deleted_count == 1
    assert report.comparison.matched_live == 1
    assert report.comparison.matched_deleted == 1

    # 역순 전달: 배치 B의 파일을 먼저, 배치 A를 나중에 읽어도 같은 결과다.
    manifests = sorted(
        (
            f"{repository.manifest_base}/{date_dir}/{name}"
            for date_dir in storage.list(repository.manifest_base)
            for name in storage.list(f"{repository.manifest_base}/{date_dir}")
        ),
        reverse=True,
    )
    rows: list[dict] = []
    for path in manifests:
        rows.extend(lake_rows(storage, path))
    restored = restore_latest_state(rows)
    assert restored.sessions[str(session_id)].version == 2
    assert restored.sessions[str(deleted_id)].is_deleted is True
    assert restored.conflicts == []


def test_initial_load_keeps_changes_made_during_the_load_in_the_outbox(
    loader: SessionLakeLoader,
    repository: SessionLakeRepository,
    storage: LocalLakeStorage,
    sessions: SessionFactoryHelper,
    clock: FakeClock,
) -> None:
    existing = sessions.create(capture=False)  # 트리거 도입 전부터 있던 세션
    created_during: dict[str, UUID] = {}

    def concurrent_change() -> None:
        created_during["id"] = sessions.create(appliance_type="KETTLE")
        sessions.finish(existing)

    result = loader.run_initial_load(after_snapshot_opened=concurrent_change)

    assert result.status == "COMPLETED"
    batch = repository.get_batch(UUID(result.batch_id))
    assert batch.batch_kind == "INITIAL"
    manifest = json.loads(storage.read_bytes(batch.manifest_path))
    assert manifest["batch_kind"] == "INITIAL"
    assert manifest["snapshot"]["attempt"] == 1
    # 초기 적재 중 발생한 변경은 outbox에 그대로 남아 있다.
    assert [e.delivery_status for e in sessions.outbox()] == ["PENDING", "PENDING"]

    # 초기 적재 직후: 전달 전 이벤트는 오류가 아니라 대기로 보고된다.
    report = SessionLakeVerifier(repository, storage).run()
    assert report.ok, report.to_dict()
    assert {item["reason"] for item in report.comparison.pending} <= {
        "NOT_LOADED_YET",
        "LAKE_BEHIND_PENDING",
    }

    # 증분 적재 뒤에는 최신 상태가 완전히 일치한다.
    incremental = cycle(loader)
    assert [r.status for r in incremental] == ["COMPLETED"]
    report = SessionLakeVerifier(repository, storage).run()
    assert report.ok, report.to_dict()
    assert report.comparison.pending == []
    assert report.comparison.matched_live == 2
    assert report.restore.sessions[str(existing)].version == 2

    with pytest.raises(RuntimeError):
        loader.run_initial_load()


def test_initial_load_retry_discards_unconfirmed_files_and_keeps_batch(
    repository: SessionLakeRepository,
    storage: LocalLakeStorage,
    sessions: SessionFactoryHelper,
    clock: FakeClock,
) -> None:
    for _ in range(3):
        sessions.create(capture=False, started_at=clock.advance(minutes=1))
    flaky = FlakyStorage(storage, fail_on_write=2)
    loader = SessionLakeLoader(repository, flaky, rows_per_file=1, clock=clock)

    failed = loader.run_initial_load()
    assert failed.status == "FAILED"
    batch = repository.get_batch(UUID(failed.batch_id))
    directory = repository.batch_directory(batch)
    leftovers = storage.list(directory)
    assert leftovers  # 첫 파일은 올라갔고 두 번째에서 실패했다

    flaky.fail_on_write = None
    resumed = loader.run_initial_load()
    assert resumed.batch_id == failed.batch_id
    assert resumed.status == "COMPLETED" and resumed.row_count == 3
    names = storage.list(directory)
    assert all(name.startswith("part-a02-") for name in names), names
    assert not any(name in names for name in leftovers)
    manifest = json.loads(storage.read_bytes(batch.manifest_path))
    assert manifest["snapshot"]["attempt"] == 2
    assert repository.get_batch(batch.batch_id).status == "COMPLETED"


def test_status_summary_reports_undelivered_backlog_and_failures(
    repository: SessionLakeRepository,
    storage: LocalLakeStorage,
    sessions: SessionFactoryHelper,
    clock: FakeClock,
) -> None:
    sessions.create()
    clock.advance(minutes=10)
    sessions.create(appliance_type="KETTLE")
    flaky = FlakyStorage(storage, fail_on_write=1)
    loader = SessionLakeLoader(repository, flaky, max_attempts=1, clock=clock)
    cycle(loader)

    status = repository.status_summary()

    assert status.undelivered_events == 2
    assert status.assigned_events == 2 and status.pending_events == 0
    assert status.oldest_undelivered_age_seconds == 600.0
    assert status.batches == {"INCREMENTAL": {"FAILED": 1}}
    assert len(status.failed_batches) == 1
    assert loader.is_stalled(status.failed_batches[0]) is True
    assert "simulated" in status.failed_batches[0].last_error
    assert status.to_dict()["undelivered_events"] == 2
