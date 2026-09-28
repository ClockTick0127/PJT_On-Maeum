"""power-silver-daily 실행 인자.

    power-silver run --date 2026-09-19
    power-silver backfill --from 2026-09-01 --to 2026-09-19
    power-silver status --date 2026-09-19
    power-silver recover --date 2026-09-19

운영에서는 standalone 클러스터에 제출한다.

    spark-submit --master spark://<host>:7077 \
      --conf spark.executor.cores=2 \
      $(python -c "import power_silver, os; print(os.path.join(os.path.dirname(power_silver.__file__), 'entrypoint.py'))") \
      run --date 2026-09-19
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import json
import logging
import sys

from realtime_analysis.models import LakeBatchRun
from sqlalchemy import select

from power_silver.commit import LockNotAcquired, SilverCommitRepository
from power_silver.analysis_job import run_analysis_daily
from power_silver.config import get_settings
from power_silver.constants import DATASETS, JOB_NAME, RUN_SKIPPED, RUN_SUCCEEDED
from power_silver.job import config_version_of, run_daily
from power_silver.spark import build_session
from power_silver.storage import create_storage
from power_silver.targets import load_targets


logger = logging.getLogger(__name__)


def default_target_date(settings, now: datetime | None = None) -> date:
    """업무 시간대 기준 어제."""

    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    business_now = now + timedelta(seconds=settings.business_utc_offset_seconds)
    return business_now.date() - timedelta(days=1)


def _session_factory(settings):
    # analysis_db 접속 변수 이름이 분석 서비스와 같으므로 그 헬퍼를 그대로 쓴다.
    from realtime_analysis.database import create_session_factory

    return create_session_factory(settings)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="power-silver", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run", help="하루치 Silver를 만든다")
    run.add_argument("--date", help="대상 업무 날짜(YYYY-MM-DD). 기본값은 어제")
    run.add_argument(
        "--force",
        action="store_true",
        help="입력이 준비되지 않았거나 같은 입력으로 이미 성공했어도 다시 계산한다",
    )

    backfill = commands.add_parser("backfill", help="날짜 구간을 순서대로 처리한다")
    backfill.add_argument("--from", dest="start", required=True)
    backfill.add_argument("--to", dest="end", required=True)
    backfill.add_argument("--force", action="store_true")

    status = commands.add_parser("status", help="활성 버전과 최근 실행을 보여준다")
    status.add_argument("--date")
    status.add_argument("--json", action="store_true")

    recover = commands.add_parser(
        "recover", help="파일은 확정됐는데 DB에 반영되지 않은 실행을 반영한다"
    )
    recover.add_argument("--date")

    usage = commands.add_parser(
        "usage-daily", help="분석 커버리지와 일별 가전 사용 요약을 만든다"
    )
    usage.add_argument("--date", help="대상 업무 날짜(YYYY-MM-DD). 기본값은 어제")
    usage.add_argument(
        "--force", action="store_true",
        help="같은 입력·규칙·정책으로 확정된 실행이 있어도 다시 계산한다",
    )

    return parser


def _dates(start: date, end: date) -> list[date]:
    if end < start:
        raise SystemExit("--to must not be earlier than --from")
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    settings = get_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    storage = create_storage(settings)
    session_factory = _session_factory(settings)
    repository = SilverCommitRepository(session_factory)

    if arguments.command == "status":
        return _status(settings, repository, session_factory, arguments)

    if arguments.command == "recover":
        target_date = (
            date.fromisoformat(arguments.date)
            if arguments.date
            else default_target_date(settings)
        )
        try:
            published = repository.recover(storage, settings.manifest_base, target_date)
        except LockNotAcquired:
            print(f"another writer already owns {target_date}")
            return 0
        print(f"recovered {published} run(s) for {target_date}")
        return 0

    if arguments.command == "usage-daily":
        target_date = (
            date.fromisoformat(arguments.date)
            if arguments.date
            else default_target_date(settings)
        )
        spark = build_session(settings)
        try:
            manifest = run_analysis_daily(
                settings,
                target_date,
                storage=storage,
                session_factory=session_factory,
                spark=spark,
                force=arguments.force,
            )
            logger.info(
                "analysis-usage-daily %s succeeded: run_id=%s reused=%s",
                target_date,
                manifest["run_id"],
                manifest.get("reused_run_id") is not None,
            )
            return 0
        finally:
            spark.stop()

    targets = load_targets(settings.observation_targets_file)
    if arguments.command == "backfill":
        wanted = _dates(
            date.fromisoformat(arguments.start), date.fromisoformat(arguments.end)
        )
    else:
        wanted = [
            date.fromisoformat(arguments.date)
            if arguments.date
            else default_target_date(settings)
        ]

    spark = build_session(settings)
    failures = 0
    try:
        for target_date in wanted:
            result = run_daily(
                settings,
                target_date,
                storage=storage,
                targets=targets,
                repository=repository,
                spark=spark,
                force=arguments.force,
            )
            logger.info(
                "%s %s: status=%s run_id=%s reused=%s wait=%s",
                JOB_NAME,
                target_date,
                result.status,
                result.run_id,
                result.reused_run_id,
                result.wait_reason,
            )
            # SKIPPED는 다른 작성자가 그 날짜를 맡았다는 뜻이라 실패가 아니다.
            # WAITING_INPUT은 다음 주기에 다시 시도해야 하므로 실패로 알린다.
            if result.status not in (RUN_SUCCEEDED, RUN_SKIPPED):
                failures += 1
    finally:
        spark.stop()
    return 1 if failures else 0


def _status(settings, repository, session_factory, arguments) -> int:
    target_date = (
        date.fromisoformat(arguments.date)
        if arguments.date
        else default_target_date(settings)
    )
    targets = load_targets(settings.observation_targets_file)
    payload = {
        "target_date": target_date.isoformat(),
        "config_version": config_version_of(targets),
        "rule_version": settings.rule_version,
        "active": {},
        "runs": [],
    }
    for dataset in DATASETS:
        version = repository.active_version(dataset, target_date)
        payload["active"][dataset] = (
            None
            if version is None
            else {
                "run_id": str(version.run_id),
                "path": version.output_path,
                "row_count": version.row_count,
                "input_snapshot_id": version.input_snapshot_id,
                "rule_version": version.rule_version,
                "config_version": version.config_version,
                "published_at": version.published_at.isoformat()
                if version.published_at
                else None,
            }
        )
    with session_factory() as session:
        runs = session.scalars(
            select(LakeBatchRun)
            .where(
                LakeBatchRun.job_name == JOB_NAME,
                LakeBatchRun.target_date == target_date,
            )
            .order_by(LakeBatchRun.attempt.desc())
            .limit(10)
        ).all()
        payload["runs"] = [
            {
                "run_id": str(run.run_id),
                "attempt": run.attempt,
                "status": run.status,
                "input_snapshot_id": run.input_snapshot_id,
                "error_message": run.error_message,
            }
            for run in runs
        ]

    if arguments.json:
        json.dump(payload, sys.stdout, ensure_ascii=False, indent=1)
        sys.stdout.write("\n")
    else:
        print(f"target_date={payload['target_date']}")
        for dataset, active in payload["active"].items():
            print(f"  {dataset}: {active}")
        for run in payload["runs"]:
            print(f"  run {run['attempt']}: {run['status']} {run['run_id']}")
    return 0
