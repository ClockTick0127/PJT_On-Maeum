"""Confirmed-manifest discovery must match both loaders' file naming."""

from __future__ import annotations

import json
from uuid import uuid4

import pytest

from power_silver.analysis_job import (
    _confirmed_files,
    select_analysis_input_snapshot,
)


def put_manifest(storage, path: str, files: list[str]) -> None:
    for file_path in files:
        storage.write_bytes(file_path, b"parquet-bytes")
    payload = {"files": [{"path": file_path, "sha256": "abc"} for file_path in files]}
    storage.write_bytes(path, json.dumps(payload).encode())


def test_session_manifest_named_after_its_batch_is_confirmed(lake, settings):
    # 세션 적재기는 날짜 디렉터리를 공유하므로 배치 id를 파일 이름에 넣는다.
    batch_id = uuid4()
    wanted = f"{settings.silver_base}/session/part-0.parquet"
    put_manifest(
        lake,
        f"{settings.session_manifest_base}/date=2026-09-19/manifest-{batch_id}.json",
        [wanted],
    )

    files, digest, count = _confirmed_files(lake, settings.session_manifest_base)

    assert files == [wanted]
    assert count == 1
    assert digest


def test_receipt_manifest_in_its_batch_directory_is_confirmed(lake, settings):
    # 영수증 적재기는 배치 디렉터리마다 manifest.json 하나를 쓴다.
    wanted = f"{settings.silver_base}/receipt/part-0.parquet"
    put_manifest(
        lake,
        f"{settings.receipt_manifest_base}/date=2026-09-19/batch_id={uuid4()}/manifest.json",
        [wanted],
    )

    files, _digest, count = _confirmed_files(lake, settings.receipt_manifest_base)

    assert files == [wanted]
    assert count == 1


def test_unconfirmed_leftovers_are_not_read_as_manifests(lake, settings):
    # 확정 전 잔여물이나 다른 메타데이터가 확정으로 오인되면 안 된다.
    lake.write_bytes(
        f"{settings.session_manifest_base}/date=2026-09-19/manifest-{uuid4()}.json.tmp",
        b"{}",
    )
    lake.write_bytes(
        f"{settings.session_manifest_base}/date=2026-09-19/summary.json",
        b"{}",
    )

    files, _digest, count = _confirmed_files(lake, settings.session_manifest_base)

    assert files == []
    assert count == 0


def test_selected_analysis_input_does_not_include_a_later_manifest(lake, settings):
    receipt = f"{settings.silver_base}/receipt/part-0.parquet"
    session = f"{settings.silver_base}/session/part-0.parquet"
    put_manifest(
        lake,
        f"{settings.receipt_manifest_base}/date=2026-09-19/batch_id=a/manifest.json",
        [receipt],
    )
    put_manifest(
        lake,
        f"{settings.session_manifest_base}/date=2026-09-19/manifest-a.json",
        [session],
    )
    selected = select_analysis_input_snapshot(settings, storage=lake)

    later = f"{settings.silver_base}/session/part-1.parquet"
    put_manifest(
        lake,
        f"{settings.session_manifest_base}/date=2026-09-20/manifest-b.json",
        [later],
    )

    assert [item.path for item in selected.session_files] == [session]
    selected.validate(lake)
    next_run = select_analysis_input_snapshot(settings, storage=lake)
    assert [item.path for item in next_run.session_files] == [session, later]
    assert next_run.snapshot_id != selected.snapshot_id


def test_selected_analysis_input_rejects_in_place_file_mutation(lake, settings):
    receipt = f"{settings.silver_base}/receipt/part-0.parquet"
    session = f"{settings.silver_base}/session/part-0.parquet"
    put_manifest(
        lake,
        f"{settings.receipt_manifest_base}/date=2026-09-19/batch_id=a/manifest.json",
        [receipt],
    )
    put_manifest(
        lake,
        f"{settings.session_manifest_base}/date=2026-09-19/manifest-a.json",
        [session],
    )
    selected = select_analysis_input_snapshot(settings, storage=lake)

    lake.write_bytes(session, b"changed-confirmed-file")

    with pytest.raises(RuntimeError, match="changed after selection"):
        selected.validate(lake)
