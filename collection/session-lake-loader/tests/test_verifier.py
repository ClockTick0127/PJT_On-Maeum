from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

from session_lake_loader.repository import UnloadedVersions, VerificationSnapshot
from session_lake_loader.verifier import (
    StateRestorer,
    compare_with_database,
    restore_latest_state,
)


T = datetime(2026, 9, 20, 9, 0, tzinfo=timezone.utc)


def lake_row(session_id, version, *, deleted=False, probability="0.8000", ended=None, event_id=None):
    return {
        "event_id": event_id,
        "operation": "DELETE" if deleted else ("SNAPSHOT" if event_id is None else "UPDATE"),
        "session_id": str(session_id),
        "session_version": version,
        "is_deleted": deleted,
        "changed_at": T,
        "activity_daily_id": "11111111-1111-1111-1111-111111111111",
        "household_id": None if deleted else "H001",
        "appliance_type": None if deleted else "MICROWAVE",
        "observation_date": None if deleted else date(2026, 9, 20),
        "started_at": T,
        "ended_at": ended,
        "max_probability": Decimal(probability),
        "decision_threshold": Decimal("0.5000"),
        "updated_at": T,
        "batch_id": "b",
        "batch_kind": "INCREMENTAL",
        "schema_version": 1,
    }


def db_record(session_id, version, *, probability="0.8000", ended=None):
    return SimpleNamespace(
        id=session_id,
        activity_daily_id="11111111-1111-1111-1111-111111111111",
        household_id="H001",
        appliance_type="MICROWAVE",
        observation_date=date(2026, 9, 20),
        started_at=T,
        ended_at=ended,
        max_probability=Decimal(probability),
        decision_threshold=Decimal("0.5000"),
        updated_at=T,
        lake_version=version,
    )


def snapshot(sessions=(), unloaded=None):
    return VerificationSnapshot(
        taken_at=T,
        snapshot_id=None,
        sessions={str(record.id): record for record in sessions},
        unloaded=unloaded or {},
    )


def test_restore_picks_highest_version_and_ignores_late_lower_versions() -> None:
    sid = uuid4()
    restorer = StateRestorer()
    restorer.add_rows([lake_row(sid, 2, ended=T, event_id=2)], source="batch-2")
    restorer.add_rows([lake_row(sid, 1, event_id=1)], source="batch-1-late")
    result = restorer.result()

    assert result.sessions[str(sid)].version == 2
    assert result.ignored_lower_versions == 1
    assert result.conflicts == []


def test_restore_keeps_delete_tombstone_as_final_state() -> None:
    sid = uuid4()
    result = restore_latest_state(
        [lake_row(sid, 1, event_id=1), lake_row(sid, 2, deleted=True, event_id=2)]
    )

    assert result.sessions[str(sid)].is_deleted is True
    assert result.deleted_count == 1
    assert result.live_count == 0


def test_restore_drops_identical_duplicates_but_flags_conflicting_content() -> None:
    sid = uuid4()
    result = restore_latest_state(
        [
            lake_row(sid, 1, event_id=1),
            lake_row(sid, 1),  # 초기 적재 SNAPSHOT 중복
            lake_row(sid, 1, probability="0.9000", event_id=99),  # 같은 버전, 다른 내용
        ]
    )

    assert result.duplicate_rows == 1
    assert len(result.conflicts) == 1
    assert result.conflicts[0]["session_version"] == 1


def test_compare_matches_live_and_deleted_sessions() -> None:
    live, gone = uuid4(), uuid4()
    restored = restore_latest_state(
        [lake_row(live, 2, ended=T, event_id=2), lake_row(gone, 3, deleted=True, event_id=3)]
    )
    result = compare_with_database(restored, snapshot([db_record(live, 2, ended=T)]))

    assert result.ok
    assert result.matched_live == 1
    assert result.matched_deleted == 1


def test_compare_reports_content_mismatch_and_missing_sessions_as_errors() -> None:
    mismatch, missing = uuid4(), uuid4()
    restored = restore_latest_state([lake_row(mismatch, 1, probability="0.7000", event_id=1)])
    result = compare_with_database(
        restored,
        snapshot([db_record(mismatch, 1), db_record(missing, 1)]),
    )

    reasons = sorted(error["reason"] for error in result.errors)
    assert reasons == ["CONTENT_MISMATCH", "MISSING_IN_LAKE"]


def test_compare_treats_unloaded_outbox_versions_as_pending_not_errors() -> None:
    behind, fresh, deleting = uuid4(), uuid4(), uuid4()
    restored = restore_latest_state(
        [lake_row(behind, 1, event_id=1), lake_row(deleting, 1, event_id=5)]
    )
    result = compare_with_database(
        restored,
        snapshot(
            [db_record(behind, 3, ended=T), db_record(fresh, 1)],
            unloaded={
                str(behind): UnloadedVersions(min_version=2, max_version=3, has_delete=False),
                str(fresh): UnloadedVersions(min_version=1, max_version=1, has_delete=False),
                str(deleting): UnloadedVersions(min_version=2, max_version=2, has_delete=True),
            },
        ),
    )

    assert result.ok
    assert sorted(item["reason"] for item in result.pending) == [
        "DELETE_PENDING",
        "LAKE_BEHIND_PENDING",
        "NOT_LOADED_YET",
    ]


def test_compare_flags_gap_that_no_unloaded_version_explains() -> None:
    sid = uuid4()
    restored = restore_latest_state([lake_row(sid, 1, event_id=1)])
    result = compare_with_database(
        restored,
        snapshot(
            [db_record(sid, 3, ended=T)],
            unloaded={str(sid): UnloadedVersions(min_version=3, max_version=3, has_delete=False)},
        ),
    )

    assert [error["reason"] for error in result.errors] == ["LAKE_BEHIND"]
