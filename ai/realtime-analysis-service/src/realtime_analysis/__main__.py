"""실시간 분석 서비스의 실행 진입점."""

import logging
from queue import SimpleQueue
import signal
from threading import Event, Thread

from confluent_kafka.admin import AdminClient

from realtime_analysis.activity_repository import (
    SqlAlchemyApplianceActivityRepository,
)
from realtime_analysis.anomaly_detector import (
    RealtimeAnomalyDetector,
    SqlAlchemyEventDetectionRepository,
)
from realtime_analysis.baseline import (
    BaselineRepository,
    SqlAlchemyBaselineRepository,
)
from realtime_analysis.baseline_refresh import BaselineCacheRefresher
from realtime_analysis.buffer import HouseholdBuffer
from realtime_analysis.config import get_settings
from realtime_analysis.consumer import AnalysisConsumer
from realtime_analysis.database import create_session_factory
from realtime_analysis.data_quality_monitor import (
    DataQualityMonitor,
    DataQualityWatchdog,
)
from realtime_analysis.data_quality_publisher import DataQualityEventPublisher
from realtime_analysis.dlq import DlqPublisher
from realtime_analysis.event_emission_repository import (
    SqlAlchemyEventEmissionRepository,
)
from realtime_analysis.event_producer import AnalysisEventPublisher
from realtime_analysis.handler import MeasurementHandler
from realtime_analysis.health_server import ObservabilityServer
from realtime_analysis.metrics import METRICS
from realtime_analysis.model_manifest import ModelManifest
from realtime_analysis.outing_consumer import OutingEventConsumer
from realtime_analysis.outing_repository import SqlAlchemyOutingStateRepository
from realtime_analysis.predictor import FakePredictor
from realtime_analysis.preprocessing import StandardizingPredictor
from realtime_analysis.policy import (
    PolicyRepository,
    SqlAlchemyPolicyRepository,
)
from realtime_analysis.readiness import (
    ReadinessProbe,
    database_readiness_check,
    kafka_readiness_check,
)
from realtime_analysis.snapshot_publisher import AnalysisSnapshotPublisher
from realtime_analysis.state_tracker import DailyActivityTracker
from realtime_analysis.state_decider import ApplianceStateDecider
from realtime_analysis.state_transition import ApplianceStateTransitionDetector

logger = logging.getLogger(__name__)


def main() -> None:
    settings = get_settings()
    METRICS.set_e2e_clock_skew_tolerance(
        settings.analysis_e2e_clock_skew_tolerance_seconds
    )
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    stop_event = Event()

    def request_shutdown(signum: int, frame: object) -> None:
        logger.info("Shutdown requested: signal=%s", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)

    if settings.model_backend == "selected_scene":
        from realtime_analysis.scene_pipeline import run_selected_scene
        run_selected_scene(settings, stop_event)
        return

    # 이상 탐지기 생성
    tracker = DailyActivityTracker()
    manifest = ModelManifest.from_json_file(settings.model_manifest_file)
    model_version = manifest.version
    if settings.model_backend == "real":
        import torch
        from realtime_analysis.real_predictor import RealtimeModelPredictor

        torch.set_num_threads(settings.model_threads)
        predictor = RealtimeModelPredictor(
            settings.model_asset_root,
            device=settings.model_device,
            dtype=settings.model_dtype,
        )
        state_decider = ApplianceStateDecider(predictor.on_thresholds)
        state_transition_detector = ApplianceStateTransitionDetector(
            on_confirmation_samples=predictor.confirmation_samples,
            off_confirmation_samples=predictor.confirmation_samples,
            off_threshold_margin=0,
            off_thresholds=predictor.off_thresholds,
        )
        model_name = "nilm-r3-six-appliance"
        model_version = predictor.model_version
    else:
        predictor = StandardizingPredictor(
            FakePredictor(settings.fake_on_appliance_types),
            manifest,
        )
        state_decider = ApplianceStateDecider.from_manifest(manifest)
        state_transition_detector = ApplianceStateTransitionDetector(
            on_confirmation_samples=settings.appliance_on_confirmation_samples,
            off_confirmation_samples=settings.appliance_off_confirmation_samples,
            off_threshold_margin=settings.appliance_off_threshold_margin,
        )
        model_name = manifest.model_name
    METRICS.set_model_info(model_name, model_version)
    session_factory = create_session_factory(settings)
    bootstrap_policies = PolicyRepository.from_json_file(
        settings.analysis_policy_file
    )
    policy_repository = SqlAlchemyPolicyRepository(session_factory)
    outing_state_repository = SqlAlchemyOutingStateRepository(session_factory)
    seeded_policies = policy_repository.seed_missing(
        bootstrap_policies.policies
    )
    if seeded_policies:
        logger.info(
            "Bootstrap analysis policies inserted: rows=%s",
            seeded_policies,
        )
    detector = RealtimeAnomalyDetector(
        repository=SqlAlchemyEventDetectionRepository(
            session_factory=session_factory,
            timezone_name=settings.analysis_timezone,
        ),
        policy_repository=policy_repository,
        timezone_name=settings.analysis_timezone,
        emission_repository=SqlAlchemyEventEmissionRepository(session_factory),
        outing_state_provider=outing_state_repository,
    )
    bootstrap_baselines = BaselineRepository.from_json_file(
        settings.baseline_file
    )
    baseline_repository = SqlAlchemyBaselineRepository(
        session_factory=session_factory,
        timezone_name=settings.analysis_timezone,
    )
    seeded_baselines = baseline_repository.seed_missing(
        bootstrap_baselines.baselines
    )
    if seeded_baselines:
        logger.info(
            "Bootstrap routine baselines inserted: rows=%s",
            seeded_baselines,
        )
    activity_repository = SqlAlchemyApplianceActivityRepository(
        session_factory=session_factory,
        timezone_name=settings.analysis_timezone,
        expected_samples_per_day=settings.analysis_expected_samples_per_day,
    )
    event_publisher = AnalysisEventPublisher(settings)
    data_quality_monitor = DataQualityMonitor(
        publisher=DataQualityEventPublisher(settings),
        gap_threshold_seconds=(
            settings.analysis_data_gap_threshold_seconds
        ),
        recovery_confirmation_samples=(
            settings.analysis_data_recovery_confirmation_samples
        ),
        on_gap=activity_repository.close_open_sessions,
    )
    handler = MeasurementHandler(
        buffer=HouseholdBuffer(settings.model_window_size),
        predictor=predictor,
        state_decider=state_decider,
        state_transition_detector=state_transition_detector,
        # 환경변수의 analysis_db 접속 정보로 SessionFactory를 만들고
        # 실제 SQLAlchemy Repository를 Handler에 주입한다.
        activity_repository=activity_repository,
        baseline_repository=baseline_repository,
        tracker=tracker,
        detector=detector,
        event_publisher=event_publisher,
        snapshot_publisher=AnalysisSnapshotPublisher(settings),
        timezone_name=settings.analysis_timezone,
        data_quality_monitor=data_quality_monitor,
        analysis_run_id=settings.analysis_run_id,
        model_version=model_version,
        pipeline_version=settings.analysis_pipeline_version,
    )
    consumer = AnalysisConsumer(
        settings=settings,
        dlq_publisher=DlqPublisher(settings),
        handler=handler,
    )
    outing_consumer = OutingEventConsumer(
        settings=settings,
        repository=outing_state_repository,
        dlq_publisher=DlqPublisher(settings),
    )
    outing_consumer_errors: SimpleQueue[BaseException] = SimpleQueue()

    def run_outing_consumer() -> None:
        try:
            outing_consumer.run(stop_event)
        except BaseException as error:
            logger.exception("Outing event consumer failed")
            outing_consumer_errors.put(error)
            stop_event.set()

    outing_consumer_thread = Thread(
        target=run_outing_consumer,
        name="outing-event-consumer",
        daemon=True,
    )
    kafka_admin = AdminClient(
        {"bootstrap.servers": settings.kafka_bootstrap_servers}
    )
    readiness = ReadinessProbe(
        {
            "kafka_input": kafka_readiness_check(
                kafka_admin,
                settings.kafka_input_topic,
                settings.readiness_timeout_seconds,
            ),
            "kafka_data_quality": kafka_readiness_check(
                kafka_admin,
                settings.kafka_analysis_data_quality_topic,
                settings.readiness_timeout_seconds,
            ),
            "kafka_outing": kafka_readiness_check(
                kafka_admin,
                settings.kafka_outing_event_topic,
                settings.readiness_timeout_seconds,
            ),
            "database": database_readiness_check(session_factory),
            # The current MVP loads its runtime model from the packaged manifest.
            # The ACTIVE model loader can replace this check without changing HTTP.
            "model": lambda: manifest is not None,
        }
    )
    observability_server = ObservabilityServer(
        settings.http_host,
        settings.http_port,
        readiness,
    )
    baseline_cache_refresher = BaselineCacheRefresher(
        repository=baseline_repository,
        interval_seconds=settings.routine_baseline_refresh_seconds,
    )
    data_quality_watchdog = DataQualityWatchdog(
        monitor=data_quality_monitor,
        poll_seconds=settings.analysis_data_quality_poll_seconds,
    )
    try:
        observability_server.start()
        baseline_cache_refresher.start()
        data_quality_watchdog.start(stop_event)
        outing_consumer_thread.start()

        consumer.run(stop_event)
    finally:
        stop_event.set()
        if outing_consumer_thread.ident is not None:
            outing_consumer_thread.join(timeout=5)
            if outing_consumer_thread.is_alive():
                logger.error("Outing event consumer did not stop within timeout")
        data_quality_watchdog.stop()
        baseline_cache_refresher.stop()
        observability_server.stop()

    if not outing_consumer_errors.empty():
        raise RuntimeError("Outing event consumer stopped unexpectedly") from (
            outing_consumer_errors.get()
        )


if __name__ == "__main__":
    main()
