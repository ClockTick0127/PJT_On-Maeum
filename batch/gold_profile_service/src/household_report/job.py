from __future__ import annotations

from datetime import datetime, timezone
import json
from uuid import uuid4

from power_silver.writer import write_parquet

from household_report.assessment_summary import build_assessment_report
from household_report.daily_trends import assessment_mode, build_daily_trends
from pyspark.sql import functions as F
from household_report.evidence import build_evidence
from household_report.input_snapshot import read_source
from household_report.usage_summary import build_usage_report
from household_report.validation import validate_inputs, validate_outputs


def build_report(
    inputs,
    *,
    spark,
    storage,
    partitions: int = 8,
    base: str = "/nilm/gold/household_report_daily",
):
    if partitions < 1:
        raise ValueError("partitions must be positive")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    sources = {
        name: read_source(spark, storage, inputs, name)
        for name in inputs.document["sources"]
    }
    validate_inputs(sources, inputs)
    mode = assessment_mode(sources["assessments"], inputs.document)
    sources["assessments"] = sources["assessments"].withColumn("assessment_mode", F.lit(mode))
    targets = sources["targets"].select("household_id")
    frames = build_usage_report(
        sources["usage"], targets, start=inputs.start, end=inputs.end
    )
    frames.update(
        build_assessment_report(
            sources["assessments"], targets, start=inputs.start, end=inputs.end
        )
    )
    frames["baseline"] = sources["baseline"].join(targets, "household_id", "inner")
    frames["statistics"] = sources["statistics"].join(
        targets, "household_id", "inner"
    )
    frames["evidence"] = build_evidence(
        frames["usage_summary"], frames["assessment_detail"]
    )
    frames.update(build_daily_trends(frames["usage_daily"], frames["assessment_detail"], targets,
                                    start=inputs.start, end=inputs.end, mode=mode))
    validate_outputs(frames)

    attempt_id = str(uuid4())
    staging = f"{base}/.staging/{attempt_id}"
    final = (
        f"{base}/report_end={inputs.end}/report_id={inputs.report_id}"
        f"/attempt_id={attempt_id}"
    )
    try:
        outputs = {}
        for name, frame in frames.items():
            staged_path = f"{staging}/{name}"
            result = write_parquet(frame, storage, staged_path, partitions=partitions)
            rows = spark.read.parquet(storage.uri(staged_path)).count()
            outputs[name] = {
                "path": f"{final}/{name}",
                "row_count": rows,
                "file_count": result.file_count,
                "byte_count": result.byte_count,
            }
        storage.rename(staging, final)
    except BaseException:
        storage.delete(staging, recursive=True)
        raise

    manifest = {
        "schema_version": 1,
        "assessment_mode": mode,
        "report_id": inputs.report_id,
        "attempt_id": attempt_id,
        "period_start": inputs.start.isoformat(),
        "period_end": inputs.end.isoformat(),
        "gold_run_id": inputs.gold_run_id,
        "rule_version": inputs.rule_version,
        "input_snapshot_id": inputs.snapshot_id,
        "input": inputs.document,
        "outputs": outputs,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    manifest_path = f"{final}/manifest.json"
    temporary_path = f"{final}/manifest.pending"
    storage.write_bytes(
        temporary_path,
        json.dumps(manifest, ensure_ascii=False, sort_keys=True).encode(),
    )
    storage.rename(temporary_path, manifest_path)
    return manifest_path
