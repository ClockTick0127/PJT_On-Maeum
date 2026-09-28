"""Prometheus metrics emitted by the realtime analysis pipeline."""

from __future__ import annotations

from collections.abc import Mapping
from threading import Lock

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, Info, REGISTRY


STAGE_DURATION_BUCKETS = (
    0.0001,
    0.0005,
    0.001,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
)

E2E_DURATION_BUCKETS = (
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
    120.0,
    300.0,
)

DAILY_JOB_DURATION_BUCKETS = (
    0.001,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
    120.0,
    300.0,
    600.0,
)

MESSAGE_STATUSES = ("processed", "dlq", "failed")
DLQ_REASONS = ("INVALID_JSON", "VALIDATION_ERROR")
REALTIME_PATTERNS = (
    "ROUTINE_MISSED",
    "PROLONGED_INACTIVITY",
    "PROLONGED_APPLIANCE_USE",
)
PATTERNS = (*REALTIME_PATTERNS, "ROUTINE_CHANGED")
PATTERN_RESULTS = ("detected", "not_detected", "error")
DAILY_JOBS = ("activity_index", "routine_changed", "baseline_update")
DAILY_JOB_STATUSES = ("success", "error")
REBALANCE_EVENTS = ("assign", "revoke", "lost")


class AnalysisMetrics:
    """Owns the bounded-cardinality metrics exposed by ``/metrics``."""

    def __init__(
        self,
        registry: CollectorRegistry = REGISTRY,
        e2e_clock_skew_tolerance_seconds: float = 0.1,
    ) -> None:
        self.set_e2e_clock_skew_tolerance(
            e2e_clock_skew_tolerance_seconds
        )
        self.stage_duration = Histogram(
            "nilm_analysis_stage_duration_seconds",
            "Realtime analysis pipeline stage duration in seconds.",
            ("stage",),
            buckets=STAGE_DURATION_BUCKETS,
            registry=registry,
        )
        self.e2e_duration = Histogram(
            "nilm_analysis_e2e_duration_seconds",
            "Duration from measurement observation to snapshot publication.",
            buckets=E2E_DURATION_BUCKETS,
            registry=registry,
        )
        self.messages = Counter(
            "nilm_analysis_messages_total",
            "Kafka input messages by terminal processing status.",
            ("status",),
            registry=registry,
        )
        self.dlq_messages = Counter(
            "nilm_analysis_dlq_messages_total",
            "Messages successfully published to the analysis DLQ.",
            ("reason",),
            registry=registry,
        )
        self.errors = Counter(
            "nilm_analysis_errors_total",
            "Realtime analysis errors by stage and exception type.",
            ("stage", "error_type"),
            registry=registry,
        )
        self.consumer_lag = Gauge(
            "nilm_analysis_consumer_lag_messages",
            "Difference between the Kafka high watermark and consumer position.",
            ("topic", "partition"),
            registry=registry,
        )
        self.consumer_rebalances = Counter(
            "nilm_analysis_consumer_rebalances_total",
            "Kafka consumer rebalance callbacks by event.",
            ("event",),
            registry=registry,
        )
        self.assigned_partitions = Gauge(
            "nilm_analysis_consumer_assigned_partitions",
            "Kafka partitions currently assigned to this consumer instance.",
            registry=registry,
        )
        self.household_state_resets = Counter(
            "nilm_analysis_household_state_resets_total",
            "Household volatile states reset after partition revocation.",
            registry=registry,
        )
        self.warmup_households = Gauge(
            "nilm_analysis_warmup_households",
            "Households currently refilling their model input window.",
            registry=registry,
        )
        self.model = Info(
            "nilm_analysis_model",
            "Currently loaded analysis model.",
            registry=registry,
        )
        self.pattern_detection_duration = Histogram(
            "nilm_pattern_detection_duration_seconds",
            "Pattern detection algorithm duration in seconds.",
            ("pattern",),
            buckets=STAGE_DURATION_BUCKETS,
            registry=registry,
        )
        self.pattern_detections = Counter(
            "nilm_pattern_detection_total",
            "Pattern detection evaluations by pattern and result.",
            ("pattern", "result"),
            registry=registry,
        )
        self.pattern_events = Counter(
            "nilm_pattern_events_total",
            "Pattern events successfully published to Kafka by event type.",
            ("event_type",),
            registry=registry,
        )
        self.daily_job_duration = Histogram(
            "nilm_daily_job_duration_seconds",
            "Daily analysis job duration in seconds.",
            ("job",),
            buckets=DAILY_JOB_DURATION_BUCKETS,
            registry=registry,
        )
        self.daily_job_runs = Counter(
            "nilm_daily_job_runs_total",
            "Daily analysis job runs by job and terminal status.",
            ("job", "status"),
            registry=registry,
        )
        self._initialize_fixed_counter_labels()
        self._lag_labels: set[tuple[str, str]] = set()
        self._lag_lock = Lock()

    def _initialize_fixed_counter_labels(self) -> None:
        """Expose zero-valued series before their first increment.

        Prometheus cannot infer a counter increase when the first scraped sample
        already has value 1. Initializing every bounded label combination makes
        the first real increment visible to ``rate`` and ``increase`` queries.
        """

        for status in MESSAGE_STATUSES:
            self.messages.labels(status=status)
        for reason in DLQ_REASONS:
            self.dlq_messages.labels(reason=reason)
        for event in REBALANCE_EVENTS:
            self.consumer_rebalances.labels(event=event)
        for pattern in PATTERNS:
            for result in PATTERN_RESULTS:
                self.pattern_detections.labels(pattern=pattern, result=result)
        self.pattern_detections.labels(
            pattern="ROUTINE_CHANGED",
            result="skipped",
        )
        for event_type in PATTERNS:
            self.pattern_events.labels(event_type=event_type)
        for job in DAILY_JOBS:
            for status in DAILY_JOB_STATUSES:
                self.daily_job_runs.labels(job=job, status=status)

    def observe_stage(self, stage: str, duration_ns: int) -> None:
        self.stage_duration.labels(stage=stage).observe(duration_ns / 1_000_000_000)

    def set_e2e_clock_skew_tolerance(self, tolerance_seconds: float) -> None:
        """Configure how much negative E2E time is treated as clock jitter."""

        if tolerance_seconds < 0:
            raise ValueError("E2E clock skew tolerance must be non-negative")
        self._e2e_clock_skew_tolerance_seconds = tolerance_seconds

    def observe_e2e(self, duration_seconds: float) -> None:
        if duration_seconds < -self._e2e_clock_skew_tolerance_seconds:
            self.record_error("e2e", "ClockSkew")
            return
        self.e2e_duration.observe(max(duration_seconds, 0.0))

    def record_message(self, status: str) -> None:
        self.messages.labels(status=status).inc()

    def record_dlq(self, reason: str) -> None:
        self.dlq_messages.labels(reason=reason).inc()

    def record_error(self, stage: str, error_type: str) -> None:
        self.errors.labels(stage=stage, error_type=error_type).inc()

    def replace_consumer_lag(
        self,
        lags: Mapping[tuple[str, int], int],
    ) -> None:
        current_labels = {(topic, str(partition)) for topic, partition in lags}
        with self._lag_lock:
            for stale_topic, stale_partition in self._lag_labels - current_labels:
                self.consumer_lag.remove(stale_topic, stale_partition)
            for (topic, partition), lag in lags.items():
                self.consumer_lag.labels(
                    topic=topic,
                    partition=str(partition),
                ).set(max(lag, 0))
            self._lag_labels = current_labels

    def record_consumer_rebalance(self, event: str) -> None:
        self.consumer_rebalances.labels(event=event).inc()

    def set_assigned_partitions(self, count: int) -> None:
        self.assigned_partitions.set(max(count, 0))

    def record_household_state_resets(self, count: int) -> None:
        if count > 0:
            self.household_state_resets.inc(count)

    def set_warmup_households(self, count: int) -> None:
        self.warmup_households.set(max(count, 0))

    def set_model_info(self, name: str, version: str) -> None:
        self.model.info({"name": name, "version": version})

    def observe_pattern_detection(self, pattern: str, duration_seconds: float) -> None:
        self.pattern_detection_duration.labels(pattern=pattern).observe(
            duration_seconds
        )

    def record_pattern_detection(self, pattern: str, result: str) -> None:
        self.pattern_detections.labels(pattern=pattern, result=result).inc()

    def record_pattern_event(self, event_type: str) -> None:
        self.pattern_events.labels(event_type=event_type).inc()

    def observe_daily_job(self, job: str, duration_seconds: float) -> None:
        self.daily_job_duration.labels(job=job).observe(duration_seconds)

    def record_daily_job(self, job: str, status: str) -> None:
        self.daily_job_runs.labels(job=job, status=status).inc()


METRICS = AnalysisMetrics()
