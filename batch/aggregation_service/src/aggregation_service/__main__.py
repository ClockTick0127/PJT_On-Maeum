"""Entrypoint for daily aggregation jobs."""

from __future__ import annotations

import logging
import signal
from threading import Event

from confluent_kafka.admin import AdminClient
from hdfs import InsecureClient

from aggregation_service.baseline_updater import (
    RoutineBaselineUpdateService,
)
from aggregation_service.batch_runs import BatchRunRepository
from aggregation_service.daily_activity_index import (
    DailyActivityIndexRepository,
    DailyActivityIndexScheduler,
    DailyActivityIndexService,
)
from aggregation_service.routine_change_detector import (
    RoutineChangeDetectionService,
)
from aggregation_service.retention import BronzeRetentionService, RetentionConfig

from realtime_analysis.activity_publisher import ActivityIndexPublisher
from realtime_analysis.baseline import (
    BaselineRepository,
    SqlAlchemyBaselineRepository,
)
from realtime_analysis.config import get_settings
from realtime_analysis.database import create_session_factory
from realtime_analysis.event_producer import AnalysisEventPublisher
from realtime_analysis.health_server import ObservabilityServer
from realtime_analysis.policy import SqlAlchemyPolicyRepository
from realtime_analysis.readiness import (
    ReadinessProbe,
    database_readiness_check,
    kafka_readiness_check,
)


logger = logging.getLogger(__name__)


def main() -> None:
    settings = get_settings()

    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    stop_event = Event()

    def request_shutdown(signum: int, frame: object) -> None:
        logger.info(
            "Aggregation shutdown requested: signal=%s",
            signum,
        )
        stop_event.set()

    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)

    session_factory = create_session_factory(settings)

    # 기존과 동일한 설정 기반 집계 대상 가구를 사용한다.
    bootstrap_baselines = BaselineRepository.from_json_file(
        settings.baseline_file
    )

    # 마이그레이션과 초기 데이터 삽입은 분석 서비스에서 수행한다.
    # Compose에서 분석 서비스의 준비 완료 후 집계 서비스를 시작한다.
    baseline_repository = SqlAlchemyBaselineRepository(
        session_factory=session_factory,
        timezone_name=settings.analysis_timezone,
    )
    policy_repository = SqlAlchemyPolicyRepository(session_factory)

    service = DailyActivityIndexService(
        repository=DailyActivityIndexRepository(
            session_factory=session_factory,
            timezone_name=settings.analysis_timezone,
            expected_samples_per_day=(
                settings.analysis_expected_samples_per_day
            ),
            valid_coverage_ratio=(
                settings.analysis_observation_valid_coverage_ratio
            ),
        ),
        publisher=ActivityIndexPublisher(settings),
        configured_household_ids=bootstrap_baselines.household_ids,
        baseline_updater=RoutineBaselineUpdateService(
            session_factory=session_factory,
            baseline_repository=baseline_repository,
            timezone_name=settings.analysis_timezone,
            window_days=settings.routine_baseline_window_days,
            minimum_sample_days=(
                settings.routine_baseline_minimum_sample_days
            ),
            minimum_weekday_sample_days=(
                settings.routine_baseline_minimum_weekday_sample_days
            ),
            minimum_daily_use_probability=(
                settings.routine_baseline_minimum_daily_use_probability
            ),
        ),
        daily_event_detector=RoutineChangeDetectionService(
            session_factory=session_factory,
            policy_repository=policy_repository,
            publisher=AnalysisEventPublisher(settings),
            timezone_name=settings.analysis_timezone,
        ),
    )

    batch_runs = BatchRunRepository(session_factory)
    retention = None
    if settings.retention_enabled:
        retention = BronzeRetentionService(
            InsecureClient(settings.hdfs_url, user="root"),
            batch_runs,
            RetentionConfig(
                bronze_base=settings.bronze_base,
                bronze_manifest_base=settings.bronze_manifest_base,
                manifest_base=settings.retention_manifest_base,
                retention_days=settings.bronze_retention_days,
                grace_days=settings.bronze_grace_days,
                max_delete_bytes=settings.retention_max_delete_bytes,
                max_delete_dates=settings.retention_max_delete_dates,
                apply=settings.retention_apply,
                require_compaction=settings.retention_require_compaction,
            ),
        )

    scheduler = DailyActivityIndexScheduler(
        service=service,
        timezone_name=settings.analysis_timezone,
        publish_hour=settings.activity_index_publish_hour,
        publish_minute=settings.activity_index_publish_minute,
        poll_seconds=settings.activity_index_scheduler_poll_seconds,
        run_repository=batch_runs,
        completion_hook=(
            (lambda _activity_date: retention.run())
            if retention is not None
            else None
        ),
    )

    kafka_admin = AdminClient(
        {"bootstrap.servers": settings.kafka_bootstrap_servers}
    )

    readiness = ReadinessProbe(
        {
            "database": database_readiness_check(session_factory),
            "kafka_activity": kafka_readiness_check(
                kafka_admin,
                settings.kafka_analysis_activity_topic,
                settings.readiness_timeout_seconds,
            ),
            "kafka_event": kafka_readiness_check(
                kafka_admin,
                settings.kafka_analysis_event_topic,
                settings.readiness_timeout_seconds,
            ),
        }
    )

    observability_server = ObservabilityServer(
        settings.http_host,
        settings.http_port,
        readiness,
    )

    try:
        observability_server.start()

        logger.info(
            "Aggregation service started: timezone=%s schedule=%02d:%02d",
            settings.analysis_timezone,
            settings.activity_index_publish_hour,
            settings.activity_index_publish_minute,
        )

        while not stop_event.is_set():
            try:
                scheduler.run_due()
            except Exception:
                logger.exception("Daily aggregation failed")

            stop_event.wait(
                settings.activity_index_scheduler_poll_seconds
            )
    finally:
        stop_event.set()
        observability_server.stop()
        logger.info("Aggregation service stopped")


if __name__ == "__main__":
    main()
