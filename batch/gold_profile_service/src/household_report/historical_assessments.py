"""Import a completed offline Java reassessment into the report's Parquet contract."""
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
from uuid import uuid4


def load_completed_run(manifest_path):
    path = Path(manifest_path)
    document = json.loads(path.read_text(encoding="utf-8"))
    if (document.get("schema_version") != 1
            or document.get("assessment_delivery_complete") is not True
            or document.get("assessment_mode") != "EVENT_TIME_REASSESSMENT"
            or not re.fullmatch(r"[0-9a-f]{64}", str(document.get("backfill_run_id", "")))
            or not document.get("household_id")):
        raise ValueError("not a completed historical assessment run")
    cutoff = datetime.fromisoformat(document["assessment_cutoff"].replace("Z", "+00:00"))
    if cutoff.tzinfo is None:
        raise ValueError("assessment cutoff must have a timezone")
    data = path.parent / "assessments.jsonl"
    digest = hashlib.sha256()
    with data.open("rb") as source:
        for block in iter(lambda: source.read(65536), b""):
            digest.update(block)
    if digest.hexdigest() != document.get("data_sha256"):
        raise ValueError("assessment data checksum mismatch")
    return document, data, cutoff


def connect_report_input(report_input, imported):
    """Return a new report input document; never overwrite an existing snapshot."""
    from copy import deepcopy
    result = deepcopy(report_input)
    if set(result.get("sources", {})) != {"targets", "usage", "baseline", "statistics", "assessments"}:
        raise ValueError("report input must provide all five sources")
    result["sources"]["assessments"] = imported["sources"]["assessments"]
    result["assessment_cutoff"] = imported["assessment_cutoff"]
    result["assessment_delivery_complete"] = True
    result["assessment_mode"] = imported["assessment_mode"]
    return result


def import_assessments(manifest_path, *, spark, storage, output_base):
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType, TimestampType, IntegerType, DoubleType,
    )
    from household_report.validation import require_unique, reject_rows

    document, data, cutoff = load_completed_run(manifest_path)
    names = {
        "assessment_id": StringType(), "household_id": StringType(),
        "assessed_at": TimestampType(), "last_observed_at": TimestampType(),
        "assessment_status": StringType(), "risk_score": IntegerType(),
        "risk_level": StringType(), "confidence": DoubleType(),
        "profile_version": StringType(), "policy_version": StringType(),
        "score_version": StringType(), "indicators": StringType(),
        "backfill_run_id": StringType(), "assessment_mode": StringType(),
    }
    frame = spark.read.schema(StructType([
        StructField(name, kind, True) for name, kind in names.items()
    ])).option("mode", "FAILFAST").json(str(data.resolve())).cache()
    final = f"{output_base.rstrip('/')}/run_id={document['backfill_run_id']}"
    staging = f"{output_base.rstrip('/')}/.staging/{uuid4()}"
    try:
        if storage.exists(final):
            raise FileExistsError(f"immutable assessment output already exists: {final}")
        require_unique(frame, ["assessment_id"])
        count = frame.count()
        if count < 1 or count != document["record_count"]:
            raise ValueError("assessment record count mismatch")
        reject_rows(frame,
                    F.col("assessed_at").isNull() | (F.col("assessed_at") >= F.lit(cutoff))
                    | F.col("household_id").isNull()
                    | (F.col("household_id") != document["household_id"])
                    | F.col("backfill_run_id").isNull()
                    | (F.col("backfill_run_id") != document["backfill_run_id"])
                    | F.col("assessment_mode").isNull()
                    | (F.col("assessment_mode") != "EVENT_TIME_REASSESSMENT"),
                    "assessment provenance/cutoff mismatch")
        frame.write.mode("errorifexists").parquet(storage.uri(f"{staging}/data"))
        # Recheck after Spark's lazy read before declaring delivery complete.
        latest, _, _ = load_completed_run(manifest_path)
        if latest != document:
            raise ValueError("assessment manifest changed during import")
        result = {
            **document,
            "sources": {"assessments": {
                "version_id": document["backfill_run_id"], "paths": [f"{final}/data"],
            }},
        }
        storage.write_bytes(f"{staging}/manifest.json", json.dumps(result, ensure_ascii=False).encode())
        storage.rename(staging, final)
        return {"manifest_path": f"{final}/manifest.json", **result}
    finally:
        frame.unpersist()
        if storage.exists(staging):
            storage.delete(staging, recursive=True)
