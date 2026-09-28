import json

import pytest

from household_report.input_snapshot import load_report_input
from power_silver.storage import LocalLakeStorage


def _document():
    return {
        "schema_version": 1,
        "period_start": "2026-09-22",
        "period_end": "2026-09-22",
        "gold_run_id": "gold-1",
        "report_rule_version": "household-report-v1",
        "assessment_cutoff": "2026-09-23T03:00:00+09:00",
        "assessment_delivery_complete": True,
        "sources": {
            name: {"version_id": f"{name}-1", "paths": [f"/input/{name}"]}
            for name in ("targets", "usage", "baseline", "statistics", "assessments")
        },
    }


def _store(tmp_path, document):
    storage = LocalLakeStorage(tmp_path)
    for source in document["sources"].values():
        storage.makedirs(source["paths"][0])
    storage.write_bytes("/manifest.json", json.dumps(document).encode())
    return storage


def test_report_identity_is_stable_when_path_order_differs(tmp_path):
    document = _document()
    document["sources"]["usage"]["paths"] *= 2
    storage = _store(tmp_path, document)
    first = load_report_input(storage, "/manifest.json")
    document["sources"]["usage"]["paths"].reverse()
    storage.write_bytes("/manifest.json", json.dumps(document).encode())
    second = load_report_input(storage, "/manifest.json")
    assert first.snapshot_id == second.snapshot_id
    assert first.paths("usage") == ["/input/usage"]


def test_incomplete_assessment_delivery_is_rejected(tmp_path):
    document = _document()
    document["assessment_delivery_complete"] = False
    storage = _store(tmp_path, document)
    with pytest.raises(RuntimeError, match="not complete"):
        load_report_input(storage, "/manifest.json")


def test_missing_source_path_is_rejected(tmp_path):
    document = _document()
    storage = _store(tmp_path, document)
    document["sources"]["usage"]["paths"] = ["/missing"]
    storage.write_bytes("/manifest.json", json.dumps(document).encode())
    with pytest.raises(FileNotFoundError):
        load_report_input(storage, "/manifest.json")
