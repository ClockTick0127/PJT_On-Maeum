"""Prometheus metrics for the session lake loader."""

from __future__ import annotations

from prometheus_client import REGISTRY, CollectorRegistry, Counter, Gauge

from session_lake_loader.repository import LoaderStatus


class LoaderMetrics:
    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        self.undelivered_events = Gauge(
            "session_lake_outbox_undelivered_events",
            "outbox events not yet delivered to the lake (PENDING + ASSIGNED)",
            registry=registry,
        )
        self.pending_events = Gauge(
            "session_lake_outbox_pending_events",
            "outbox events not yet assigned to a batch",
            registry=registry,
        )
        self.oldest_undelivered_seconds = Gauge(
            "session_lake_outbox_oldest_undelivered_seconds",
            "age of the oldest undelivered outbox event",
            registry=registry,
        )
        self.batches = Gauge(
            "session_lake_batches",
            "session lake batches by kind and status",
            labelnames=("kind", "status"),
            registry=registry,
        )
        self.stalled_batches = Gauge(
            "session_lake_batches_stalled",
            "failed batches that exhausted LOADER_MAX_ATTEMPTS",
            registry=registry,
        )
        self.batches_processed_total = Counter(
            "session_lake_batches_processed_total",
            "batch attempts by kind and result",
            labelnames=("kind", "result"),
            registry=registry,
        )
        self.batch_failures_total = Counter(
            "session_lake_batch_failures_total",
            "batch failures by kind and stage",
            labelnames=("kind", "stage"),
            registry=registry,
        )
        self.rows_written_total = Counter(
            "session_lake_rows_written_total",
            "lake rows confirmed by kind",
            labelnames=("kind",),
            registry=registry,
        )
        self.last_cycle_timestamp = Gauge(
            "session_lake_loader_last_cycle_timestamp_seconds",
            "unix time of the last completed loader cycle",
            registry=registry,
        )

    def observe_status(self, status: LoaderStatus, stalled: int = 0) -> None:
        self.undelivered_events.set(status.undelivered_events)
        self.pending_events.set(status.pending_events)
        self.oldest_undelivered_seconds.set(status.oldest_undelivered_age_seconds or 0.0)
        for kind, statuses in status.batches.items():
            for batch_status, count in statuses.items():
                self.batches.labels(kind=kind, status=batch_status).set(count)
        self.stalled_batches.set(stalled)
        self.last_cycle_timestamp.set(status.checked_at.timestamp())

    def observe_result(self, result) -> None:
        self.batches_processed_total.labels(
            kind=result.batch_kind, result=result.status
        ).inc()
        if result.status == "FAILED":
            self.batch_failures_total.labels(
                kind=result.batch_kind, stage=result.stage or "unknown"
            ).inc()
        elif not result.resumed_from_manifest:
            self.rows_written_total.labels(kind=result.batch_kind).inc(result.row_count)
