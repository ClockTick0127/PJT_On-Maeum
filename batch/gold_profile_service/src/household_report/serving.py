from __future__ import annotations

import hashlib
import json

from sqlalchemy import text


INSERT_RUN = text("""
INSERT INTO reporting.report_runs (
    report_id, period_start, period_end, gold_run_id, input_snapshot_id,
    rule_version, manifest_path, generated_at, manifest
)
VALUES (
    :report_id, :period_start, :period_end, :gold_run_id, :input_snapshot_id,
    :rule_version, :manifest_path, :generated_at, CAST(:manifest AS jsonb)
)
""")

INSERT_ROW = text("""
INSERT INTO reporting.report_rows (
    report_id, household_id, dataset, row_key, payload
)
VALUES (
    :report_id, :household_id, :dataset, :row_key, CAST(:payload AS jsonb)
)
""")


def publish_report(engine, *, spark, storage, manifest_path: str, batch_size: int = 500):
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    manifest = json.loads(storage.read_bytes(manifest_path))
    if manifest.get("schema_version") != 1:
        raise ValueError("unsupported report manifest schema")
    report_id = manifest["report_id"]

    with engine.begin() as connection:
        connection.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
            {"key": f"household-report:{report_id}"},
        )
        existing = connection.execute(
            text(
                "SELECT input_snapshot_id FROM reporting.report_runs "
                "WHERE report_id = :report_id"
            ),
            {"report_id": report_id},
        ).scalar_one_or_none()
        if existing is not None:
            if existing != manifest["input_snapshot_id"]:
                raise RuntimeError("report identity conflict")
            return {"status": "REUSED", "report_id": report_id}

        connection.execute(
            INSERT_RUN,
            {
                "report_id": report_id,
                "period_start": manifest["period_start"],
                "period_end": manifest["period_end"],
                "gold_run_id": manifest["gold_run_id"],
                "input_snapshot_id": manifest["input_snapshot_id"],
                "rule_version": manifest["rule_version"],
                "manifest_path": manifest_path,
                "generated_at": manifest["completed_at"],
                "manifest": json.dumps(manifest, ensure_ascii=False),
            },
        )
        for dataset, output in manifest["outputs"].items():
            frame = spark.read.parquet(storage.uri(output["path"]))
            batch = []
            written = 0
            for raw in frame.toJSON().toLocalIterator():
                value = json.loads(raw)
                household_id = value.get("household_id")
                if household_id is None:
                    raise ValueError(f"missing household_id in {dataset}")
                canonical = json.dumps(
                    value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                )
                batch.append(
                    {
                        "report_id": report_id,
                        "household_id": household_id,
                        "dataset": dataset,
                        "row_key": hashlib.sha256(canonical.encode()).hexdigest(),
                        "payload": canonical,
                    }
                )
                if len(batch) >= batch_size:
                    connection.execute(INSERT_ROW, batch)
                    written += len(batch)
                    batch.clear()
            if batch:
                connection.execute(INSERT_ROW, batch)
                written += len(batch)
            if written != output["row_count"]:
                raise RuntimeError(f"row count mismatch for {dataset}")
    return {"status": "PUBLISHED", "report_id": report_id}
