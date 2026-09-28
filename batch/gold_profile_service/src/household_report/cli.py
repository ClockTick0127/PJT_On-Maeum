import argparse
import json
import os
from datetime import date

from sqlalchemy import create_engine

from gold_profile.config import get_settings
from power_silver.spark import build_session
from power_silver.storage import create_storage

from household_report.input_snapshot import load_report_input
from household_report.job import build_report
from household_report.serving import publish_report
from household_report.historical_assessments import import_assessments, connect_report_input


def main(argv=None):
    parser = argparse.ArgumentParser(prog="household-report")
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("build")
    build.add_argument("--input-manifest", required=True)
    build.add_argument("--partitions", type=int, default=8)
    publish = commands.add_parser("publish")
    publish.add_argument("--manifest", required=True)
    publish.add_argument("--batch-size", type=int, default=500)
    daily = commands.add_parser("daily", help="pin committed inputs, build and publish a daily report")
    daily.add_argument("--as-of", type=date.fromisoformat, required=True)
    daily.add_argument("--window-days", type=int, default=90)
    daily.add_argument("--historical-manifest", help="lake manifest produced by import-assessments")
    historical = commands.add_parser("import-assessments")
    historical.add_argument("--input-manifest", required=True)
    historical.add_argument("--output-base", required=True)
    historical.add_argument("--report-input", help="existing lake report input document to copy")
    historical.add_argument("--report-output", help="new lake report input document path")
    args = parser.parse_args(argv)
    if args.command == "import-assessments" and bool(args.report_input) != bool(args.report_output):
        parser.error("--report-input and --report-output must be supplied together")

    settings = get_settings()
    storage = create_storage(settings)
    spark = build_session(settings)
    try:
        if args.command == "daily":
            from household_report.daily import run_daily
            from power_silver.catalog import SilverCatalog
            from power_silver.targets import load_targets
            from realtime_analysis.database import create_session_factory
            database_url = os.environ.get("REPORT_DATABASE_URL")
            if not database_url:
                raise RuntimeError("REPORT_DATABASE_URL is required for daily reports")
            engine = create_engine(database_url, pool_pre_ping=True)
            try:
                result = run_daily(engine=engine, spark=spark, storage=storage,
                    catalog=SilverCatalog(create_session_factory(settings)),
                    targets=load_targets(settings.observation_targets_file), end=args.as_of,
                    window_days=args.window_days, historical_manifest=args.historical_manifest)
                print(json.dumps(result, ensure_ascii=False))
                return 0
            finally:
                engine.dispose()
        if args.command == "import-assessments":
            report_input = None
            if args.report_input:
                if storage.exists(args.report_output):
                    raise FileExistsError(args.report_output)
                report_input = json.loads(storage.read_bytes(args.report_input))
            result = import_assessments(args.input_manifest, spark=spark, storage=storage,
                                        output_base=args.output_base)
            if report_input is not None:
                connected = connect_report_input(report_input, result)
                storage.write_bytes(args.report_output, json.dumps(connected, ensure_ascii=False).encode())
                result["report_input_manifest"] = args.report_output
            print(json.dumps(result, ensure_ascii=False))
            return 0
        if args.command == "build":
            inputs = load_report_input(storage, args.input_manifest)
            manifest_path = build_report(
                inputs, spark=spark, storage=storage, partitions=args.partitions
            )
            print(
                json.dumps(
                    {
                        "status": "LAKE_COMMITTED",
                        "report_id": inputs.report_id,
                        "manifest_path": manifest_path,
                    },
                    ensure_ascii=False,
                )
            )
            return 0

        database_url = os.environ.get("REPORT_DATABASE_URL")
        if not database_url:
            raise RuntimeError("REPORT_DATABASE_URL is required for publish")
        engine = create_engine(database_url, pool_pre_ping=True)
        try:
            result = publish_report(
                engine,
                spark=spark,
                storage=storage,
                manifest_path=args.manifest,
                batch_size=args.batch_size,
            )
            print(json.dumps(result, ensure_ascii=False))
            return 0
        finally:
            engine.dispose()
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
