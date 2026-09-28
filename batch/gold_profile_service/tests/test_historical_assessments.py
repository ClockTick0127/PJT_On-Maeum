import hashlib
import json

import pytest

from household_report.historical_assessments import (
    connect_report_input, import_assessments, load_completed_run,
)


def completed(tmp_path):
    row = {
        "assessment_id": "a1", "household_id": "H001",
        "assessed_at": "2026-09-19T15:00:00Z", "assessment_status": "LEARNING",
        "risk_score": None, "risk_level": None, "profile_version": None,
        "policy_version": "policy-test", "score_version": "monitoring-score-v1-MIA",
        "indicators": "[]", "backfill_run_id": "a" * 64,
        "assessment_mode": "EVENT_TIME_REASSESSMENT",
    }
    data = (json.dumps(row) + "\n").encode()
    (tmp_path / "assessments.jsonl").write_bytes(data)
    document = {
        "schema_version": 1, "household_id": "H001", "backfill_run_id": "a" * 64,
        "assessment_delivery_complete": True, "assessment_cutoff": "2026-09-20T00:00:00Z",
        "assessment_mode": "EVENT_TIME_REASSESSMENT", "record_count": 1,
        "data_sha256": hashlib.sha256(data).hexdigest(),
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def test_incomplete_and_modified_data_rejected(tmp_path):
    path = completed(tmp_path)
    document, _, _ = load_completed_run(path)
    document["assessment_delivery_complete"] = False
    path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(ValueError, match="not a completed"):
        load_completed_run(path)
    path = completed(tmp_path)
    (tmp_path / "assessments.jsonl").write_text("changed")
    with pytest.raises(ValueError, match="checksum"):
        load_completed_run(path)


def test_report_connection_preserves_other_sources():
    original = {"sources": {key: {"version_id": key, "paths": [key]} for key in
                            ("targets", "usage", "baseline", "statistics", "assessments")}}
    imported = {"sources": {"assessments": {"version_id": "new", "paths": ["new"]}},
                "assessment_cutoff": "2026-09-20T00:00:00Z", "assessment_mode": "EVENT_TIME_REASSESSMENT"}
    result = connect_report_input(original, imported)
    assert result["sources"]["usage"] == original["sources"]["usage"]
    assert original["sources"]["assessments"]["version_id"] == "assessments"
    assert result["sources"]["assessments"]["version_id"] == "new"


@pytest.mark.spark
def test_import_parquet_is_consumed_by_report_without_inventing_normal_score(tmp_path, spark):
    from power_silver.storage import LocalLakeStorage
    from household_report.assessment_summary import build_assessment_report
    from datetime import date
    path = completed(tmp_path)
    storage = LocalLakeStorage(tmp_path / "lake")
    result = import_assessments(path, spark=spark, storage=storage, output_base="/assessments")
    assessments = spark.read.parquet(storage.uri(result["sources"]["assessments"]["paths"][0]))
    targets = spark.createDataFrame([("H001",)], ["household_id"])
    output = build_assessment_report(assessments, targets, start=date(2026, 9, 20), end=date(2026, 9, 20))
    row = output["assessment_summary"].first()
    assert row.recorded_assessment_count == 1
    assert row.unavailable_record_count == 1
    assert row.max_recorded_valid_score is None
    with pytest.raises(FileExistsError):
        import_assessments(path, spark=spark, storage=storage, output_base="/assessments")
