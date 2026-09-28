"""Command line entrypoint for the session lake loader.

Commands
    run            poll analysis_db and load new outbox events continuously
    incremental    one loader cycle: retry incomplete batches, drain pending events
    initial-load   full load of the current sessions from one DB snapshot
    verify         restore the latest state from the lake and compare with the DB
    status         undelivered counts, oldest wait, batch counts, recent failures
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
from threading import Event

from hdfs import InsecureClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from realtime_analysis.database import build_database_url
from realtime_analysis.health_server import ObservabilityServer
from realtime_analysis.readiness import ReadinessProbe, database_readiness_check

from session_lake_loader.config import LoaderSettings, get_settings
from session_lake_loader.loader import SessionLakeLoader
from session_lake_loader.metrics import LoaderMetrics
from session_lake_loader.receipt_lake import AnalysisReceiptLakeLoader
from session_lake_loader.repository import (
    LoaderAlreadyRunning,
    SessionLakeRepository,
)
from session_lake_loader.storage import HdfsLakeStorage, LakeStorage, LocalLakeStorage
from session_lake_loader.verifier import SessionLakeVerifier


logger = logging.getLogger(__name__)


def build_session_factory(settings: LoaderSettings) -> sessionmaker[Session]:
    engine = create_engine(build_database_url(settings), pool_pre_ping=True)
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def build_storage(settings: LoaderSettings) -> LakeStorage:
    if settings.lake_local_root:
        logger.warning("LAKE_LOCAL_ROOT가 설정되어 HDFS 대신 로컬 디렉터리를 사용합니다: %s",
                       settings.lake_local_root)
        return LocalLakeStorage(settings.lake_local_root)
    return HdfsLakeStorage(InsecureClient(settings.hdfs_url, user=settings.hdfs_user))


def build_components(
    settings: LoaderSettings,
    metrics: LoaderMetrics | None = None,
) -> tuple[sessionmaker[Session], SessionLakeRepository, LakeStorage, SessionLakeLoader]:
    session_factory = build_session_factory(settings)
    repository = SessionLakeRepository(
        session_factory,
        bronze_base=settings.session_bronze_base,
        manifest_base=settings.session_manifest_base,
    )
    storage = build_storage(settings)
    loader = SessionLakeLoader(
        repository,
        storage,
        rows_per_file=settings.loader_rows_per_file,
        retry_backoff_seconds=settings.loader_retry_backoff_seconds,
        retry_max_backoff_seconds=settings.loader_retry_max_backoff_seconds,
        max_attempts=settings.loader_max_attempts,
        spool_dir=settings.loader_spool_dir or None,
        metrics=metrics,
    )
    return session_factory, repository, storage, loader


def _emit(payload: dict, as_json: bool, lines: list[str] | None = None) -> None:
    if as_json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    else:
        for line in lines or []:
            print(line)


def _status_lines(status_dict: dict, stalled: list[str]) -> list[str]:
    lines = [
        f"checked_at: {status_dict['checked_at']}",
        f"undelivered events: {status_dict['undelivered_events']} "
        f"(pending {status_dict['pending_events']}, assigned {status_dict['assigned_events']})",
        f"oldest undelivered: {status_dict['oldest_undelivered_changed_at']} "
        f"(age {status_dict['oldest_undelivered_age_seconds']} s)",
        f"last completed batch: {status_dict['last_completed_at']}",
        "batches:",
    ]
    for kind, statuses in sorted(status_dict["batches"].items()):
        lines.append(f"  {kind}: " + ", ".join(f"{k}={v}" for k, v in sorted(statuses.items())))
    if status_dict["incomplete_initial_batches"]:
        lines.append("incomplete initial batches (resume with initial-load):")
        for batch in status_dict["incomplete_initial_batches"]:
            lines.append(f"  {batch['batch_id']} status={batch['status']} attempts={batch['attempt_count']}")
    if status_dict["failed_batches"]:
        lines.append("recent failed batches:")
        for batch in status_dict["failed_batches"]:
            marker = " STALLED" if batch["batch_id"] in stalled else ""
            lines.append(
                f"  {batch['batch_id']} kind={batch['batch_kind']} attempts={batch['attempt_count']}"
                f" last_error_at={batch['last_error_at']}{marker}"
            )
            lines.append(f"    {batch['last_error']}")
    return lines


def command_status(args: argparse.Namespace, settings: LoaderSettings) -> int:
    _, repository, _, loader = build_components(settings)
    status = repository.status_summary()
    stalled = [str(b.batch_id) for b in status.failed_batches if loader.is_stalled(b)]
    payload = {**status.to_dict(), "stalled_batches": stalled}
    _emit(payload, args.json, _status_lines(payload, stalled))
    return 0


def command_incremental(args: argparse.Namespace, settings: LoaderSettings) -> int:
    _, repository, _, loader = build_components(settings)
    try:
        with repository.cycle_lock():
            results = loader.run_incremental_cycle(
                batch_max_events=settings.loader_batch_max_events,
                max_batches=settings.loader_max_batches_per_cycle,
            )
    except LoaderAlreadyRunning:
        print("다른 적재기 프로세스가 실행 중입니다.", file=sys.stderr)
        return 3
    payload = {"results": [r.to_dict() for r in results]}
    _emit(
        payload,
        args.json,
        [
            f"{r.batch_id} {r.batch_kind} {r.status} rows={r.row_count} files={r.file_count}"
            + (f" stage={r.stage} error={r.error}" if r.error else "")
            for r in results
        ]
        or ["처리할 이벤트가 없습니다."],
    )
    return 0 if all(r.status == "COMPLETED" for r in results) else 1


def command_receipt_incremental(args: argparse.Namespace, settings: LoaderSettings) -> int:
    session_factory = build_session_factory(settings)
    loader = AnalysisReceiptLakeLoader(
        session_factory,
        build_storage(settings),
        bronze_base=settings.receipt_bronze_base,
        manifest_base=settings.receipt_manifest_base,
    )
    result = loader.run_once(settings.loader_batch_max_events)
    payload = {"result": result.__dict__ if result is not None else None}
    _emit(
        payload,
        args.json,
        [
            "처리할 분석 증거가 없습니다."
            if result is None
            else f"{result.batch_id} {result.status} rows={result.row_count}"
        ],
    )
    return 0


def command_initial_load(args: argparse.Namespace, settings: LoaderSettings) -> int:
    _, repository, _, loader = build_components(settings)
    try:
        with repository.cycle_lock():
            result = loader.run_initial_load(allow_repeat=args.allow_repeat)
    except LoaderAlreadyRunning:
        print("다른 적재기 프로세스가 실행 중입니다. run 서비스를 잠시 멈추고 다시 실행하세요.",
              file=sys.stderr)
        return 3
    _emit(
        result.to_dict(),
        args.json,
        [f"{result.batch_id} {result.status} rows={result.row_count} files={result.file_count}"
         + (f" stage={result.stage} error={result.error}" if result.error else "")],
    )
    return 0 if result.status == "COMPLETED" else 1


def command_verify(args: argparse.Namespace, settings: LoaderSettings) -> int:
    _, repository, storage, _ = build_components(settings)
    report = SessionLakeVerifier(repository, storage).run()
    _emit(report.to_dict(), args.json, report.summary_lines())
    return 0 if report.ok else 1


def command_run(args: argparse.Namespace, settings: LoaderSettings) -> int:
    metrics = LoaderMetrics()
    session_factory, repository, storage, loader = build_components(settings, metrics)
    receipt_loader = AnalysisReceiptLakeLoader(
        session_factory,
        storage,
        bronze_base=settings.receipt_bronze_base,
        manifest_base=settings.receipt_manifest_base,
    )
    stop_event = Event()

    def request_shutdown(signum: int, frame: object) -> None:
        logger.info("종료 요청: signal=%s", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)

    readiness = ReadinessProbe(
        {
            "database": database_readiness_check(session_factory),
            "lake": lambda: storage.exists("/"),
        }
    )
    server = ObservabilityServer(settings.http_host, settings.http_port, readiness)
    try:
        server.start()
        logger.info(
            "session lake loader started: poll=%ss batch_max_events=%s bronze=%s",
            settings.loader_poll_seconds,
            settings.loader_batch_max_events,
            settings.session_bronze_base,
        )
        while not stop_event.is_set():
            try:
                with repository.cycle_lock():
                    loader.run_incremental_cycle(
                        batch_max_events=settings.loader_batch_max_events,
                        max_batches=settings.loader_max_batches_per_cycle,
                    )
                    receipt_loader.run_once(settings.loader_batch_max_events)
                    status = repository.status_summary()
                    stalled = sum(1 for b in status.failed_batches if loader.is_stalled(b))
                    metrics.observe_status(status, stalled=stalled)
            except LoaderAlreadyRunning:
                logger.info("다른 적재기가 실행 중이어서 이번 주기를 건너뜁니다.")
            except Exception:  # noqa: BLE001
                logger.exception("적재 주기 실패")
            stop_event.wait(settings.loader_poll_seconds)
    finally:
        stop_event.set()
        server.stop()
        logger.info("session lake loader stopped")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="session-lake-loader", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    subparsers = parser.add_subparsers(dest="command", required=True)

    run = subparsers.add_parser("run", help="주기 실행 서비스")
    run.set_defaults(handler=command_run)

    incremental = subparsers.add_parser("incremental", help="증분 적재 1회")
    incremental.add_argument("--json", action="store_true")
    incremental.set_defaults(handler=command_incremental)

    receipt_incremental = subparsers.add_parser(
        "receipt-incremental", help="분석 처리 증거 증분 적재 1회"
    )
    receipt_incremental.add_argument("--json", action="store_true")
    receipt_incremental.set_defaults(handler=command_receipt_incremental)

    initial = subparsers.add_parser("initial-load", help="초기 전체 적재")
    initial.add_argument("--allow-repeat", action="store_true",
                         help="완료된 초기 적재가 있어도 다시 실행한다")
    initial.add_argument("--json", action="store_true")
    initial.set_defaults(handler=command_initial_load)

    verify = subparsers.add_parser("verify", help="레이크 복원 상태와 DB 비교")
    verify.add_argument("--json", action="store_true")
    verify.set_defaults(handler=command_verify)

    status = subparsers.add_parser("status", help="미전달 건수·대기시간·배치 상태")
    status.add_argument("--json", action="store_true")
    status.set_defaults(handler=command_status)
    return parser


def main(argv: list[str] | None = None) -> int:
    settings = get_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = build_parser().parse_args(argv)
    return int(args.handler(args, settings))


if __name__ == "__main__":
    sys.exit(main())
