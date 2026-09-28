"""history-generator command line.

    history-generator plan SCENARIO [--from D] [--to D] [--household ID]
    history-generator write SCENARIO [--from D] [--to D] (--lake-local-root DIR | HDFS env)
                              [--waveform simulator|synthetic] [--simulator-dir DIR] [--force]
    history-generator snapshots SCENARIO --output DIR [--interval 60]
    history-generator targets SCENARIO --output FILE [--existing FILE] [--include-load]
    history-generator profiles --household ID --output FILE [--delivery-mode SHADOW|ACTIVE]
    history-generator run-dates --from D --to D [--compose-file F] [--service gold-profile] ...
"""

from __future__ import annotations

import argparse
from datetime import date
import json
import logging
import os
from pathlib import Path
import sys
import time

from history_generator.lake import HouseholdDay, LakePaths, LakeWriter
from history_generator.scenario import Scenario, ScenarioError, load_scenario
from history_generator.schedule import plan_day, plan_summary
from history_generator.waveform import WaveformPool, make_generator, seed_from


logger = logging.getLogger("history_generator")


def _iso_date(text: str) -> date:
    try:
        return date.fromisoformat(text)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"invalid date {text!r}") from error


def _date_range(scenario: Scenario, args) -> list[date]:
    start = args.from_date or scenario.start
    end = args.to_date or scenario.end
    if start < scenario.start or end > scenario.end or start > end:
        raise ScenarioError(f"date range {start}..{end} must lie inside {scenario.start}..{scenario.end}")
    return [day for day in scenario.dates() if start <= day <= end]


def _select_households(scenario: Scenario, args):
    households = list(scenario.households)
    if getattr(args, "household", None):
        wanted = set(args.household)
        households = [item for item in households if item.household_id in wanted]
        missing = wanted - {item.household_id for item in households}
        if missing:
            raise ScenarioError(f"unknown households: {sorted(missing)}")
    if getattr(args, "demo_only", False):
        households = [item for item in households if not item.is_load]
    if getattr(args, "load_only", False):
        households = [item for item in households if item.is_load]
    if not households:
        raise ScenarioError("no households selected")
    return households


def _storage(args):
    try:
        from power_silver.storage import HdfsLakeStorage, LocalLakeStorage
    except ImportError as error:  # pragma: no cover - environment problem
        raise SystemExit("power_silver.storage is not importable; install power-silver-service "
                         "or add batch/power_silver_service/src to PYTHONPATH") from error
    if args.lake_local_root:
        return LocalLakeStorage(args.lake_local_root)
    from hdfs import InsecureClient

    url = args.hdfs_url or os.environ.get("HDFS_URL", "http://namenode:9870")
    user = args.hdfs_user or os.environ.get("HDFS_USER", "root")
    fs_uri = args.lake_fs_uri or os.environ.get("LAKE_FS_URI", "hdfs://namenode:9000")
    return HdfsLakeStorage(InsecureClient(url, user=user), fs_uri)


# --- commands ---------------------------------------------------------------------


def cmd_plan(args) -> int:
    scenario = load_scenario(args.scenario)
    households = _select_households(scenario, args)
    days = _date_range(scenario, args)
    output = []
    for household in households:
        for day in days:
            output.append(plan_summary(plan_day(household, day)))
    if args.json:
        json.dump(output, sys.stdout, ensure_ascii=False, indent=1)
        sys.stdout.write("\n")
        return 0
    print(f"scenario {scenario.source_path}: {scenario.start} .. {scenario.end}, "
          f"{len(scenario.demo_households())} demo + {len(scenario.load_households())} load households")
    for item in output:
        uses = ", ".join(f"{use['appliance']} {use['start']}-{use['end']}"
                         + (f" x{use['sessions']}" if use['sessions'] > 1 else "") for use in item["uses"])
        periods = f" [{', '.join(item['periods'])}]" if item["periods"] else ""
        missing = f" missing={item['missing_seconds']}s" if item["missing_seconds"] else ""
        print(f"{item['household_id']} {item['date']}{periods}{missing}: {uses or '(no use)'}")
    return 0


def cmd_write(args) -> int:
    scenario = load_scenario(args.scenario)
    households = _select_households(scenario, args)
    days = _date_range(scenario, args)
    storage = _storage(args)
    paths = LakePaths.from_env()
    kind = args.waveform or ("simulator" if args.simulator_dir else "synthetic")
    generator = make_generator(kind, args.simulator_dir)
    pool = WaveformPool(generator, scenario.load.waveform_pool_size if scenario.load else 1)
    writer = LakeWriter(storage, paths, scenario, batch_prefix=args.batch_prefix)
    logger.info("writing %d dates for %d households with %s waveform to %s",
                len(days), len(households), generator.name,
                args.lake_local_root or "HDFS")
    totals = {"dates": 0, "skipped": 0, "power_rows": 0, "receipt_rows": 0, "session_rows": 0, "bytes": 0}
    started = time.perf_counter()
    for ordinal, day in enumerate(days):
        if not args.force and writer.day_written(day):
            totals["skipped"] += 1
            logger.info("%s already written, skipped", day)
            continue
        items: list[HouseholdDay] = []
        for household in households:
            plan = plan_day(household, day)
            if household.is_load and household.template_id:
                waveform = pool.waveform(plan, household.template_id, ordinal)
            else:
                waveform = generator.generate(plan, seed_from(household.seed, household.household_id, day))
            items.append(HouseholdDay(household, plan, waveform))
        result = writer.write_day(day, items, force=args.force)
        totals["dates"] += 1
        totals["power_rows"] += result.power_rows
        totals["receipt_rows"] += result.receipt_rows
        totals["session_rows"] += result.session_rows
        totals["bytes"] += result.bytes_written
        logger.info("%s: %d households, %d power rows, %d sessions, %.1f MB", day, result.households,
                    result.power_rows, result.session_rows, result.bytes_written / 1e6)
    totals["seconds"] = round(time.perf_counter() - started, 1)
    print(json.dumps(totals))
    return 0


def cmd_snapshots(args) -> int:
    from history_generator.snapshots import write_household_inputs

    scenario = load_scenario(args.scenario)
    households = [item for item in _select_households(scenario, args) if not item.is_load or args.include_load]
    kind = args.waveform or ("simulator" if args.simulator_dir else "synthetic")
    generator = make_generator(kind, args.simulator_dir)
    output = Path(args.output)
    results = []
    for household in households:
        def day_plans(day, household=household):
            plan = plan_day(household, day)
            return plan, generator.generate(plan, seed_from(household.seed, household.household_id, day))

        results.append(write_household_inputs(scenario, household, output, day_plans, interval=args.interval))
        logger.info("%s: %d snapshot lines", household.household_id, results[-1]["snapshots"])
    print(json.dumps(results, ensure_ascii=False))
    return 0


def cmd_targets(args) -> int:
    from history_generator.targets import write_targets

    scenario = load_scenario(args.scenario)
    count = write_targets(scenario, Path(args.output),
                          Path(args.existing) if args.existing else None, include_load=args.include_load)
    print(json.dumps({"output": args.output, "targets": count}))
    return 0


def cmd_profiles(args) -> int:
    from history_generator.profiles import database_url, fetch_payloads, treat_as_active, write_profiles

    payloads = fetch_payloads(args.database_url or database_url(), args.household, delivery_mode=args.delivery_mode)
    if args.treat_as_active:
        payloads = treat_as_active(payloads)
    if args.virtual_publish:
        from history_generator.profiles import virtual_publish

        payloads = virtual_publish(payloads, args.utc_offset_seconds)
    write_profiles(payloads, Path(args.output))
    print(json.dumps({"household_id": args.household, "profiles": len(payloads), "output": args.output,
                      "treated_as_active": bool(args.treat_as_active),
                      "virtual_publish": bool(args.virtual_publish)}))
    return 0


def cmd_run_dates(args) -> int:
    from history_generator.rundates import run_dates

    extra_env = dict(item.split("=", 1) for item in args.env or [])
    results = run_dates(
        args.from_date, args.to_date, log_path=Path(args.log), dry_run=args.dry_run, keep_going=args.keep_going,
        compose_file=args.compose_file, service=args.service, extra_env=extra_env,
        attempts=args.attempts, publish=args.publish, docker=args.docker,
        volumes=tuple(args.volume or ()),
    )
    if args.dry_run:
        return 0
    failed = [item for item in results if item.exit_code not in (0, 10, 11, 12)]
    print(json.dumps({"dates": len(results), "failed": len(failed),
                      "exit_codes": {str(code): sum(1 for item in results if item.exit_code == code)
                                     for code in sorted({item.exit_code for item in results})}}))
    return 1 if failed else 0


# --- parser -------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="history-generator", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--log-level", default="INFO")
    commands = parser.add_subparsers(dest="command", required=True)

    def scenario_args(sub, *, dates: bool = True):
        sub.add_argument("scenario", help="scenario JSON path")
        sub.add_argument("--household", action="append", help="limit to these household ids")
        sub.add_argument("--demo-only", action="store_true")
        sub.add_argument("--load-only", action="store_true")
        if dates:
            sub.add_argument("--from", dest="from_date", type=_iso_date)
            sub.add_argument("--to", dest="to_date", type=_iso_date)

    def waveform_args(sub):
        sub.add_argument("--waveform", choices=("simulator", "synthetic"))
        sub.add_argument("--simulator-dir", help="infrastructure/mqtt/simulator checkout")

    plan = commands.add_parser("plan", help="print the deterministic day plans (ground truth)")
    scenario_args(plan)
    plan.add_argument("--json", action="store_true")
    plan.set_defaults(func=cmd_plan)

    write = commands.add_parser("write", help="write Bronze power, receipts and sessions for a date range")
    scenario_args(write)
    waveform_args(write)
    write.add_argument("--lake-local-root", help="write to a local directory instead of HDFS")
    write.add_argument("--hdfs-url")
    write.add_argument("--hdfs-user")
    write.add_argument("--lake-fs-uri")
    write.add_argument("--batch-prefix", default="hist")
    write.add_argument("--force", action="store_true", help="rewrite dates whose manifests already exist")
    write.set_defaults(func=cmd_write)

    snapshots = commands.add_parser("snapshots", help="historicalAssessment inputs per household")
    scenario_args(snapshots, dates=False)
    waveform_args(snapshots)
    snapshots.add_argument("--output", required=True)
    snapshots.add_argument("--interval", type=int, default=60)
    snapshots.add_argument("--include-load", action="store_true")
    snapshots.set_defaults(func=cmd_snapshots)

    targets = commands.add_parser("targets", help="observation_targets.json with the scenario households")
    scenario_args(targets, dates=False)
    targets.add_argument("--output", required=True)
    targets.add_argument("--existing", help="merge into this existing targets file")
    targets.add_argument("--include-load", action="store_true")
    targets.set_defaults(func=cmd_targets)

    profiles = commands.add_parser("profiles", help="outbox payloads -> profiles.json")
    profiles.add_argument("--household", required=True)
    profiles.add_argument("--output", required=True)
    profiles.add_argument("--delivery-mode", choices=("SHADOW", "ACTIVE"))
    profiles.add_argument("--database-url")
    profiles.add_argument("--treat-as-active", action="store_true",
                          help="relabel SHADOW payloads as ACTIVE (annotated) so historicalAssessment accepts them")
    profiles.add_argument("--virtual-publish", action="store_true",
                          help="set effective_from/published_at to the KST midnight after as_of_date (annotated)")
    profiles.add_argument("--utc-offset-seconds", type=int, default=9 * 3600)
    profiles.set_defaults(func=cmd_profiles)

    run = commands.add_parser("run-dates", help="run gold-profile daily for each date, oldest first")
    run.add_argument("--from", dest="from_date", type=_iso_date, required=True)
    run.add_argument("--to", dest="to_date", type=_iso_date, required=True)
    run.add_argument("--compose-file")
    run.add_argument("--service", default="gold-profile")
    run.add_argument("--env", action="append", help="KEY=VALUE passed with -e")
    run.add_argument("--volume", action="append", help="HOST:CONTAINER[:ro] passed with -v (e.g. targets file)")
    run.add_argument("--attempts", type=int, default=3)
    run.add_argument("--publish", action="store_true", help="omit --no-publish")
    run.add_argument("--docker", default="docker")
    run.add_argument("--log", default="run-dates.jsonl")
    run.add_argument("--dry-run", action="store_true")
    run.add_argument("--keep-going", action="store_true")
    run.set_defaults(func=cmd_run_dates)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(message)s")
    try:
        return args.func(args)
    except ScenarioError as error:
        parser.exit(2, f"scenario error: {error}\n")
    except FileNotFoundError as error:
        parser.exit(2, f"{error}\n")
    return 0
