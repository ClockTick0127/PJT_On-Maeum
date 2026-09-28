"""Command line entry point for the rolling Gold profile batch."""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import json
import logging
import math
import signal
import threading

from power_silver.catalog import SilverCatalog
from power_silver.commit import SilverCommitRepository
from power_silver.constants import DATASET_APPLIANCE_USAGE_DAILY, DATASET_SESSION_SLICES
from power_silver.spark import build_session
from power_silver.storage import create_storage

from gold_profile.config import get_settings
from gold_profile.daily import (
    RUN_INPUT_INCOMPLETE,
    RUN_PUBLISH_PENDING,
    STATUS_FAILED,
    STATUS_SKIPPED,
    STATUS_SUCCEEDED,
    SparkDailyStages,
    run_daily_pipeline,
)
from gold_profile.delivery import GoldProfilePublisher, drain_outbox
from gold_profile.input_snapshot import build_profile_snapshot, window_dates
from gold_profile.job import (
    DATASET_ROUTINE_BASELINE, JOB_NAME, config_version_of, run_gold_profile,
)


EXIT_SUCCEEDED = 0
EXIT_FAILED = 1
EXIT_INVALID_ARGUMENT = 2
EXIT_INPUT_INCOMPLETE = 10
EXIT_SKIPPED = 11
EXIT_PUBLISH_PENDING = 12


def _iso_date(value: str) -> str:
    try:
        date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be an ISO date (YYYY-MM-DD)") from error
    return value


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


def _non_negative_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0:
        raise argparse.ArgumentTypeError("must be at least 0")
    return parsed


def _positive_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than 0")
    return parsed


def daily_exit_code(status: str) -> int:
    """Map the daily result contract to stable scheduler-facing exit codes."""

    return {
        STATUS_SUCCEEDED: EXIT_SUCCEEDED,
        RUN_INPUT_INCOMPLETE: EXIT_INPUT_INCOMPLETE,
        STATUS_SKIPPED: EXIT_SKIPPED,
        RUN_PUBLISH_PENDING: EXIT_PUBLISH_PENDING,
        STATUS_FAILED: EXIT_FAILED,
    }.get(status, EXIT_FAILED)


def default_as_of_date(settings, now: datetime | None = None) -> date:
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return (now + timedelta(seconds=settings.business_utc_offset_seconds)).date() - timedelta(days=1)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gold-profile", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="build one as-of-date profile")
    run.add_argument("--as-of", type=_iso_date)
    run.add_argument("--force", action="store_true")
    backfill = commands.add_parser("backfill", help="build an inclusive as-of range")
    backfill.add_argument("--from", dest="start", type=_iso_date, required=True)
    backfill.add_argument("--to", dest="end", type=_iso_date, required=True)
    backfill.add_argument("--force", action="store_true")
    status = commands.add_parser("status", help="show the active shadow profile")
    status.add_argument("--as-of", type=_iso_date)
    dirty = commands.add_parser("dirty", help="check whether active inputs changed")
    dirty.add_argument("--as-of", type=_iso_date)
    dirty.add_argument("--from", dest="start", type=_iso_date)
    dirty.add_argument("--to", dest="end", type=_iso_date)

    publish = commands.add_parser("publish", help="publish pending outbox rows")
    publish.add_argument(
        "--loop", action="store_true",
        help="keep draining until stopped; this is how the publisher service runs",
    )
    publish.add_argument(
        "--interval", type=_positive_float,
        help="seconds between sweeps when looping (default: the retry interval)",
    )

    daily = commands.add_parser(
        "daily", help="run one business day end to end: silver, usage, gold, publish"
    )
    daily.add_argument("--as-of", type=_iso_date)
    daily.add_argument(
        "--attempts", type=_positive_int, default=3,
        help="retries per stage (default: 3)",
    )
    daily.add_argument(
        "--retry-seconds", type=_non_negative_float, default=30.0
    )
    daily.add_argument(
        "--no-align", action="store_true",
        help="skip rebuilding window dates that sit on an older session snapshot",
    )
    daily.add_argument(
        "--no-publish", action="store_true",
        help="leave the outbox to the publisher service",
    )
    return parser


def _session_factory(settings):
    from realtime_analysis.database import create_session_factory

    return create_session_factory(settings)


def _snapshot(settings, catalog, as_of_date):
    dates = window_dates(as_of_date, settings.profile_window_days)
    return build_profile_snapshot(
        as_of_date, settings.profile_window_days,
        catalog.active_versions(DATASET_APPLIANCE_USAGE_DAILY, dates),
        catalog.active_versions(DATASET_SESSION_SLICES, dates),
        rule_version=settings.profile_rule_version,
        statistic_rule_version=settings.profile_statistic_rule_version,
        analysis_run_id=settings.analysis_run_id,
        timezone_name=f"UTC{settings.business_utc_offset_seconds:+d}s",
    )


def _stop_on_signal() -> threading.Event:
    """SIGTERM/SIGINT을 받으면 서는 신호. ``docker stop``이 깔끔히 끝나게 한다."""

    stop = threading.Event()
    for received in (signal.SIGINT, signal.SIGTERM):
        signal.signal(received, lambda *_: stop.set())
    return stop


def _daily(settings, session_factory, args) -> int:
    as_of_date = (
        date.fromisoformat(args.as_of) if args.as_of else default_as_of_date(settings)
    )
    spark = None
    try:
        storage = create_storage(settings)
        spark = build_session(settings)
        stages = SparkDailyStages(
            settings,
            storage=storage,
            session_factory=session_factory,
            spark=spark,
            # 보내지 않을 때 브로커 연결을 만들 이유가 없다.
            publisher=None if args.no_publish
            else GoldProfilePublisher(settings, session_factory),
        )
        report = run_daily_pipeline(
            stages, as_of_date,
            attempts=args.attempts,
            retry_seconds=args.retry_seconds,
            align_window=not args.no_align,
            publish=not args.no_publish,
        )
    except Exception as error:  # initialization failures also need a machine-readable result
        logging.exception("daily pipeline initialization failed")
        report = {
            "as_of_date": as_of_date.isoformat(),
            "status": STATUS_FAILED,
            "ok": False,
            "stages": [],
            "error": f"{type(error).__name__}: {error}",
        }
    finally:
        if spark is not None:
            spark.stop()
    exit_code = daily_exit_code(report["status"])
    report = {**report, "exit_code": exit_code}
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return exit_code


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO))
    session_factory = _session_factory(settings)
    if args.command == "publish":
        publisher = GoldProfilePublisher(settings, session_factory)
        if args.loop:
            interval = args.interval or settings.profile_publisher_retry_seconds
            return drain_outbox(publisher, interval, _stop_on_signal())
        published, failed = publisher.publish_pending()
        logging.info("gold profile outbox published=%s failed=%s", published, failed)
        return 1 if failed else 0

    if args.command == "daily":
        return _daily(settings, session_factory, args)

    repository = SilverCommitRepository(session_factory, job_name=JOB_NAME)
    catalog = SilverCatalog(session_factory)
    target = date.fromisoformat(args.as_of) if getattr(args, "as_of", None) else default_as_of_date(settings)

    if args.command in ("status", "dirty"):
        if args.command == "dirty" and (args.start or args.end):
            if not args.start or not args.end:
                raise SystemExit("dirty range requires both --from and --to")
            range_start, range_end = date.fromisoformat(args.start), date.fromisoformat(args.end)
            if range_end < range_start:
                raise SystemExit("--to must not be earlier than --from")
            inspected = [
                range_start + timedelta(days=i)
                for i in range((range_end - range_start).days + 1)
            ]
        else:
            inspected = [target]
        items = []
        for inspected_date in inspected:
            active = repository.active_version(DATASET_ROUTINE_BASELINE, inspected_date)
            current = _snapshot(settings, catalog, inspected_date)
            items.append({
                "as_of_date": inspected_date.isoformat(),
                "active_run_id": str(active.run_id) if active else None,
                "active_snapshot_id": active.input_snapshot_id if active else None,
                "current_snapshot_id": current.snapshot_id,
                "input_incomplete": current.incomplete,
                "missing_usage_dates": [item.isoformat() for item in current.missing_usage_dates],
                "missing_slice_dates": [item.isoformat() for item in current.missing_slice_dates],
                "dirty": active is None or active.input_snapshot_id != current.snapshot_id,
            })
        payload = {
            "config_version": config_version_of(settings),
            "profiles": items,
            "dirty_as_of_dates": [item["as_of_date"] for item in items if item["dirty"]],
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 1 if args.command == "dirty" and payload["dirty_as_of_dates"] else 0

    if args.command == "backfill":
        start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
        if end < start:
            raise SystemExit("--to must not be earlier than --from")
        wanted = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    else:
        wanted = [target]

    storage = create_storage(settings)
    spark = build_session(settings)
    try:
        for as_of_date in wanted:
            result = run_gold_profile(
                settings, as_of_date, storage=storage,
                session_factory=session_factory, spark=spark,
                force=getattr(args, "force", False),
            )
            logging.info(
                "%s %s status=%s run=%s reused=%s incomplete=%s",
                JOB_NAME, as_of_date, result.status, result.run_id,
                result.reused_run_id, result.incomplete,
            )
    finally:
        spark.stop()
    return 0
