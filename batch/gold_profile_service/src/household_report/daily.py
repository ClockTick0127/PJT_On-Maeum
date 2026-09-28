"""Pin committed lake inputs and a consistent monitoring DB snapshot, then publish."""
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
from uuid import uuid4

from sqlalchemy import text
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

from household_report.assessment_summary import KST
from household_report.input_snapshot import load_report_input
from household_report.job import build_report
from household_report.serving import publish_report

ASSESSMENT_SCHEMA = StructType([StructField(k, t, True) for k, t in [
    ("assessment_id", StringType()), ("household_id", StringType()),
    ("assessed_at", TimestampType()), ("assessment_status", StringType()),
    ("risk_score", IntegerType()), ("risk_level", StringType()),
    ("profile_version", StringType()), ("policy_version", StringType()),
    ("score_version", StringType()), ("indicators", StringType()),
    ("assessment_mode", StringType()),
]])


def export_live_assessments(engine, *, spark, storage, start, end, base):
    start_at = datetime.combine(start, time.min, KST)
    cutoff = datetime.combine(end + timedelta(days=1), time.min, KST)
    if cutoff > datetime.now(timezone.utc):
        raise ValueError("only completed KST dates can be reported")
    staging = f"{base}/.staging/{uuid4()}"
    digest = hashlib.sha256(f"live-v1:{start}:{end}".encode())
    try:
        # Server-side cursor bounds memory. All batches see the same committed DB snapshot.
        with engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn:
            with conn.begin():
                conn.execute(text("SET TRANSACTION READ ONLY"))
                result = conn.execution_options(stream_results=True).execute(text("""
                    SELECT id::text AS assessment_id, household_id, assessed_at,
                           assessment_status, risk_score, risk_level, profile_version,
                           policy_version, score_version, indicators,
                           'LIVE_RECORDED' AS assessment_mode
                    FROM risk_assessments
                    WHERE assessed_at >= :start AND assessed_at < :cutoff
                    ORDER BY id
                """), {"start": start_at, "cutoff": cutoff})
                spark.createDataFrame([], ASSESSMENT_SCHEMA).write.parquet(storage.uri(f"{staging}/data"))
                for batch in result.mappings().partitions(5000):
                    values = [dict(row) for row in batch]
                    for row in values:
                        digest.update(json.dumps(row, sort_keys=True, default=str, ensure_ascii=False).encode())
                        digest.update(b"\n")
                    spark.createDataFrame(values, ASSESSMENT_SCHEMA).write.mode("append").parquet(storage.uri(f"{staging}/data"))
        version = digest.hexdigest()
        final = f"{base}/snapshot={version}"
        if not storage.exists(final):
            storage.rename(staging, final)
        return {"version_id": version, "paths": [f"{final}/data"]}
    finally:
        if storage.exists(staging):
            storage.delete(staging, recursive=True)


def prepare_daily_input(*, engine, spark, storage, catalog, targets, end, window_days=90,
                        base="/nilm/report_inputs", historical_manifest=None):
    if not 1 <= window_days <= 365:
        raise ValueError("window_days must be in 1..365")
    start = end - timedelta(days=window_days - 1)
    if datetime.combine(end + timedelta(days=1), time.min, KST) > datetime.now(timezone.utc):
        raise ValueError("only completed KST dates can be reported")
    days = [start + timedelta(days=n) for n in range(window_days)]
    baseline = catalog.active_version("routine_baseline_history", end)
    statistics = catalog.active_version("household_statistical_profile", end)
    if baseline is None or statistics is None or baseline.run_id != statistics.run_id:
        raise ValueError("matching committed Gold baseline/statistics required")
    usage = catalog.active_versions("appliance_usage_daily", days)
    if end not in usage:
        raise ValueError("report date has no committed usage input")
    ids = targets.household_ids_for_date(end, 9 * 3600)
    if not ids:
        raise ValueError("no configured reporting households")
    target_version = hashlib.sha256(json.dumps([targets.fingerprint, end.isoformat(), sorted(ids)]).encode()).hexdigest()
    target_path = f"{base}/targets/{target_version}"
    if not storage.exists(target_path):
        staged = f"{base}/.staging/{uuid4()}"
        try:
            spark.createDataFrame([(x,) for x in ids], "household_id string").write.parquet(storage.uri(staged))
            storage.rename(staged, target_path)
        finally:
            if storage.exists(staged):
                storage.delete(staged, recursive=True)
    mode = "LIVE_RECORDED"
    cutoff = datetime.combine(end + timedelta(days=1), time.min, KST).isoformat()
    if historical_manifest:
        historical = json.loads(storage.read_bytes(historical_manifest))
        if historical.get("assessment_mode") != "EVENT_TIME_REASSESSMENT" or historical.get("assessment_delivery_complete") is not True:
            raise ValueError("completed historical assessment manifest required")
        if datetime.fromisoformat(historical["assessment_cutoff"]) < datetime.fromisoformat(cutoff):
            raise ValueError("historical assessment delivery does not cover report end")
        assessment = historical["sources"]["assessments"]
        mode = "EVENT_TIME_REASSESSMENT"
    else:
        assessment = export_live_assessments(engine, spark=spark, storage=storage,
                                            start=start, end=end, base=f"{base}/assessments")
    def ref(value):
        return {"version_id": str(value.version_id), "paths": [value.output_path]}
    document = {
        "schema_version": 1, "period_start": start.isoformat(), "period_end": end.isoformat(),
        "gold_run_id": str(baseline.run_id), "report_rule_version": "grafana-daily-v1",
        "assessment_mode": mode, "assessment_cutoff": cutoff, "assessment_delivery_complete": True,
        "assessment_completeness_scope": "COMMITTED_DATABASE_SNAPSHOT" if mode == "LIVE_RECORDED" else "COMPLETED_BACKFILL",
        "missing_usage_dates": [d.isoformat() for d in days if d not in usage],
        "sources": {"targets": {"version_id": target_version, "paths": [target_path]},
                    "baseline": ref(baseline), "statistics": ref(statistics), "assessments": assessment,
                    "usage": {"version_id": hashlib.sha256("|".join(str(usage[d].version_id) for d in sorted(usage)).encode()).hexdigest(),
                              "paths": [usage[d].output_path for d in sorted(usage)]}},
    }
    raw = json.dumps(document, ensure_ascii=False, sort_keys=True).encode()
    path = f"{base}/manifests/{hashlib.sha256(raw).hexdigest()}.json"
    if not storage.exists(path):
        storage.write_bytes(path, raw)
    return path


def run_daily(*, engine, spark, storage, catalog, targets, end, window_days=90, historical_manifest=None):
    path = prepare_daily_input(engine=engine, spark=spark, storage=storage, catalog=catalog,
                               targets=targets, end=end, window_days=window_days,
                               historical_manifest=historical_manifest)
    inputs = load_report_input(storage, path)
    with engine.connect() as conn:
        existing = conn.execute(text("SELECT report_id FROM reporting.report_runs WHERE report_id=:id"),
                                {"id": inputs.report_id}).scalar_one_or_none()
    if existing:
        return {"status": "REUSED", "report_id": existing}
    manifest = build_report(inputs, spark=spark, storage=storage)
    return publish_report(engine, spark=spark, storage=storage, manifest_path=manifest)
