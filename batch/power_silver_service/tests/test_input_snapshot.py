from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json

import pytest

from power_silver.input_snapshot import InputChanged, build_snapshot, verify_unchanged

from conftest import DAY_END_EPOCH, TARGET_DATE


def utc(epoch: float) -> datetime:
    return datetime.fromtimestamp(epoch, timezone.utc)


def put_manifest(
    storage,
    settings,
    *,
    ingest_date: str,
    partition: int,
    name: str,
    business_dates,
    files,
    max_measured_at: datetime,
    committed_at: datetime,
    ok_count: int = 10,
):
    for path in files:
        storage.write_bytes(path, b"parquet-bytes")
    payload = {
        "topic": "power.raw.v1",
        "partition": partition,
        "start_offset": 0,
        "end_offset": ok_count - 1,
        "ok_count": ok_count,
        "quarantine_count": 1,
        "min_measured_at": (max_measured_at - timedelta(seconds=60)).isoformat(),
        "max_measured_at": max_measured_at.isoformat(),
        "business_dates": list(business_dates),
        "files": list(files),
        "committed_at": committed_at.isoformat(),
    }
    storage.write_bytes(
        f"{settings.bronze_manifest_base}/date={ingest_date}/manifest-{partition}-{name}.json",
        json.dumps(payload).encode(),
    )


def test_files_are_chosen_by_business_date_not_by_ingest_folder(lake, settings):
    wanted = f"{settings.bronze_base}/ingest_date=2026-09-20/hour=00/partition=0/late.parquet"
    other = f"{settings.bronze_base}/ingest_date=2026-09-20/hour=01/partition=0/other.parquet"
    put_manifest(
        lake,
        settings,
        ingest_date="2026-09-20",
        partition=0,
        name="late",
        business_dates=["2026-09-19", "2026-09-20"],
        files=[wanted],
        max_measured_at=utc(DAY_END_EPOCH + 600),
        committed_at=utc(DAY_END_EPOCH + 700),
    )
    put_manifest(
        lake,
        settings,
        ingest_date="2026-09-20",
        partition=0,
        name="other",
        business_dates=["2026-09-20"],
        files=[other],
        max_measured_at=utc(DAY_END_EPOCH + 900),
        committed_at=utc(DAY_END_EPOCH + 950),
    )

    snapshot = build_snapshot(
        lake, settings, TARGET_DATE, now=utc(DAY_END_EPOCH + 3600)
    )
    assert [item.path for item in snapshot.files] == [wanted]
    assert snapshot.ready is True


def test_quarantine_files_in_the_manifest_are_not_read_as_input(lake, settings):
    ok = f"{settings.bronze_base}/ingest_date=2026-09-19/hour=00/partition=0/a.parquet"
    quarantined = "/nilm/quarantine/power/ingest_date=2026-09-19/partition=0/a.parquet"
    put_manifest(
        lake,
        settings,
        ingest_date="2026-09-19",
        partition=0,
        name="a",
        business_dates=["2026-09-19"],
        files=[ok, quarantined],
        max_measured_at=utc(DAY_END_EPOCH + 10),
        committed_at=utc(DAY_END_EPOCH + 20),
    )
    snapshot = build_snapshot(
        lake, settings, TARGET_DATE, now=utc(DAY_END_EPOCH + 3600)
    )
    assert [item.path for item in snapshot.files] == [ok]


def test_a_partition_that_has_not_crossed_the_day_boundary_blocks_the_run(lake, settings):
    for partition, measured in ((0, DAY_END_EPOCH + 30), (1, DAY_END_EPOCH - 7200)):
        put_manifest(
            lake,
            settings,
            ingest_date="2026-09-19",
            partition=partition,
            name="a",
            business_dates=["2026-09-19"],
            files=[
                f"{settings.bronze_base}/ingest_date=2026-09-19/hour=00"
                f"/partition={partition}/a.parquet"
            ],
            max_measured_at=utc(measured),
            committed_at=utc(measured + 10),
        )

    snapshot = build_snapshot(
        lake, settings, TARGET_DATE, now=utc(DAY_END_EPOCH + 600)
    )
    assert snapshot.ready is False
    assert snapshot.wait_reason == "PARTITIONS_BEHIND:1"
    # 파일은 이미 고를 수 있다. 확정만 미룬다.
    assert len(snapshot.files) == 2


def test_a_lagging_partition_is_accepted_once_the_loader_keeps_flushing(lake, settings):
    put_manifest(
        lake,
        settings,
        ingest_date="2026-09-19",
        partition=1,
        name="a",
        business_dates=["2026-09-19"],
        files=[
            f"{settings.bronze_base}/ingest_date=2026-09-19/hour=00/partition=1/a.parquet"
        ],
        max_measured_at=utc(DAY_END_EPOCH - 7200),
        committed_at=utc(DAY_END_EPOCH + settings.input_ready_grace_seconds + 60),
    )
    snapshot = build_snapshot(
        lake, settings, TARGET_DATE, now=utc(DAY_END_EPOCH + 3600)
    )
    assert snapshot.ready is True


def test_waiting_ends_at_the_deadline_even_without_evidence(lake, settings):
    snapshot = build_snapshot(lake, settings, TARGET_DATE, now=utc(DAY_END_EPOCH + 60))
    assert (snapshot.ready, snapshot.wait_reason) == (False, "NO_BRONZE_MANIFEST")

    late = build_snapshot(
        lake,
        settings,
        TARGET_DATE,
        now=utc(DAY_END_EPOCH + settings.input_wait_deadline_seconds + 1),
    )
    assert late.ready is True
    assert late.files == ()


def test_the_day_is_never_closed_before_it_ends(lake, settings):
    snapshot = build_snapshot(lake, settings, TARGET_DATE, now=utc(DAY_END_EPOCH - 60))
    assert (snapshot.ready, snapshot.wait_reason) == (False, "TARGET_DATE_NOT_FINISHED")


def test_snapshot_id_changes_when_a_file_is_replaced_in_place(lake, settings):
    path = f"{settings.bronze_base}/ingest_date=2026-09-19/hour=00/partition=0/a.parquet"
    put_manifest(
        lake,
        settings,
        ingest_date="2026-09-19",
        partition=0,
        name="a",
        business_dates=["2026-09-19"],
        files=[path],
        max_measured_at=utc(DAY_END_EPOCH + 10),
        committed_at=utc(DAY_END_EPOCH + 20),
    )
    now = utc(DAY_END_EPOCH + 3600)
    first = build_snapshot(lake, settings, TARGET_DATE, now=now)
    assert build_snapshot(lake, settings, TARGET_DATE, now=now).snapshot_id == first.snapshot_id

    lake.write_bytes(path, b"different-bytes-entirely")
    second = build_snapshot(lake, settings, TARGET_DATE, now=now)
    assert second.snapshot_id != first.snapshot_id


def test_verify_unchanged_detects_a_replaced_input_file(lake, settings):
    path = f"{settings.bronze_base}/ingest_date=2026-09-19/hour=00/partition=0/a.parquet"
    put_manifest(
        lake,
        settings,
        ingest_date="2026-09-19",
        partition=0,
        name="a",
        business_dates=["2026-09-19"],
        files=[path],
        max_measured_at=utc(DAY_END_EPOCH + 10),
        committed_at=utc(DAY_END_EPOCH + 20),
    )
    snapshot = build_snapshot(
        lake, settings, TARGET_DATE, now=utc(DAY_END_EPOCH + 3600)
    )
    verify_unchanged(lake, snapshot)

    lake.write_bytes(path, b"grown")
    with pytest.raises(InputChanged):
        verify_unchanged(lake, snapshot)


def test_a_manifest_listing_a_missing_file_stops_the_run(lake, settings):
    storage_path = (
        f"{settings.bronze_base}/ingest_date=2026-09-19/hour=00/partition=0/gone.parquet"
    )
    put_manifest(
        lake,
        settings,
        ingest_date="2026-09-19",
        partition=0,
        name="gone",
        business_dates=["2026-09-19"],
        files=[storage_path],
        max_measured_at=utc(DAY_END_EPOCH + 10),
        committed_at=utc(DAY_END_EPOCH + 20),
    )
    lake.delete(storage_path)
    with pytest.raises(InputChanged):
        build_snapshot(lake, settings, TARGET_DATE, now=utc(DAY_END_EPOCH + 3600))
