import json
from urllib.error import HTTPError
from urllib.request import urlopen

import pytest
from prometheus_client import CollectorRegistry, generate_latest

import realtime_analysis.pipeline_timing as pipeline_timing_module
from realtime_analysis.health_server import ObservabilityServer
from realtime_analysis.metrics import AnalysisMetrics
from realtime_analysis.pipeline_timing import pipeline_timing, stage
from realtime_analysis.readiness import ReadinessProbe


def request(server: ObservabilityServer, path: str) -> tuple[int, str, bytes]:
    try:
        response = urlopen(  # noqa: S310 - test only targets loopback
            f"http://127.0.0.1:{server.port}{path}",
            timeout=2,
        )
    except HTTPError as error:
        return error.code, error.headers.get("Content-Type", ""), error.read()
    with response:
        return (
            response.status,
            response.headers.get("Content-Type", ""),
            response.read(),
        )


def test_health_is_up_even_when_a_dependency_is_not_ready() -> None:
    server = ObservabilityServer(
        "127.0.0.1",
        0,
        ReadinessProbe({"kafka": lambda: False}),
        CollectorRegistry(),
    )
    server.start()
    try:
        status, content_type, payload = request(server, "/health")
    finally:
        server.stop()

    assert status == 200
    assert content_type.startswith("application/json")
    assert json.loads(payload) == {"status": "UP"}


def test_ready_reports_each_dependency_and_returns_503() -> None:
    server = ObservabilityServer(
        "127.0.0.1",
        0,
        ReadinessProbe(
            {
                "kafka": lambda: True,
                "database": lambda: False,
                "model": lambda: True,
            }
        ),
        CollectorRegistry(),
    )
    server.start()
    try:
        status, _, payload = request(server, "/ready")
    finally:
        server.stop()

    assert status == 503
    assert json.loads(payload) == {
        "status": "DOWN",
        "checks": {"kafka": "UP", "database": "DOWN", "model": "UP"},
    }


def test_metrics_endpoint_exposes_dashboard_metrics() -> None:
    registry = CollectorRegistry()
    metrics = AnalysisMetrics(registry)
    metrics.observe_stage("inference", 25_000_000)
    metrics.observe_e2e(1.25)
    metrics.record_message("processed")
    metrics.replace_consumer_lag({("power.raw.v1", 3): 17})
    metrics.set_model_info("nilm-tcn", "v1.0.0")
    metrics.observe_pattern_detection("ROUTINE_MISSED", 0.025)
    metrics.record_pattern_detection("ROUTINE_MISSED", "detected")
    metrics.record_pattern_event("ROUTINE_MISSED")
    metrics.observe_daily_job("activity_index", 0.5)
    metrics.record_daily_job("activity_index", "success")

    server = ObservabilityServer(
        "127.0.0.1",
        0,
        ReadinessProbe({"model": lambda: True}),
        registry,
    )
    server.start()
    try:
        status, content_type, payload = request(server, "/metrics")
    finally:
        server.stop()

    output = payload.decode("utf-8")
    assert status == 200
    assert content_type.startswith("text/plain")
    assert 'nilm_analysis_stage_duration_seconds_count{stage="inference"} 1.0' in output
    assert "nilm_analysis_e2e_duration_seconds_count 1.0" in output
    assert 'nilm_analysis_messages_total{status="processed"} 1.0' in output
    assert (
        'nilm_analysis_consumer_lag_messages{partition="3",topic="power.raw.v1"} 17.0'
        in output
    )
    assert 'nilm_analysis_model_info{name="nilm-tcn",version="v1.0.0"} 1.0' in output
    assert (
        'nilm_pattern_detection_duration_seconds_count{pattern="ROUTINE_MISSED"} 1.0'
        in output
    )
    assert (
        'nilm_pattern_detection_total{pattern="ROUTINE_MISSED",result="detected"} 1.0'
        in output
    )
    assert 'nilm_pattern_events_total{event_type="ROUTINE_MISSED"} 1.0' in output
    assert (
        'nilm_daily_job_duration_seconds_count{job="activity_index"} 1.0'
        in output
    )
    assert (
        'nilm_daily_job_runs_total{job="activity_index",status="success"} 1.0'
        in output
    )


def test_fixed_counter_labels_are_exposed_at_zero_before_first_increment() -> None:
    registry = CollectorRegistry()
    AnalysisMetrics(registry)

    assert registry.get_sample_value(
        "nilm_analysis_messages_total",
        {"status": "processed"},
    ) == 0
    assert registry.get_sample_value(
        "nilm_analysis_dlq_messages_total",
        {"reason": "INVALID_JSON"},
    ) == 0
    assert registry.get_sample_value(
        "nilm_pattern_detection_total",
        {"pattern": "ROUTINE_MISSED", "result": "detected"},
    ) == 0
    assert registry.get_sample_value(
        "nilm_pattern_detection_total",
        {"pattern": "ROUTINE_CHANGED", "result": "skipped"},
    ) == 0
    assert registry.get_sample_value(
        "nilm_pattern_events_total",
        {"event_type": "ROUTINE_MISSED"},
    ) == 0
    assert registry.get_sample_value(
        "nilm_daily_job_runs_total",
        {"job": "activity_index", "status": "success"},
    ) == 0


def test_pipeline_timing_updates_histogram_and_throughput(monkeypatch) -> None:
    registry = CollectorRegistry()
    metrics = AnalysisMetrics(registry)
    monkeypatch.setattr(pipeline_timing_module, "METRICS", metrics)

    with pipeline_timing() as timer:
        with stage("preprocess"):
            pass
        timer.mark("processed")

    output = generate_latest(registry).decode("utf-8")
    assert 'nilm_analysis_stage_duration_seconds_count{stage="preprocess"} 1.0' in output
    assert 'nilm_analysis_messages_total{status="processed"} 1.0' in output


def test_negative_e2e_within_tolerance_is_clamped_to_zero() -> None:
    registry = CollectorRegistry()
    metrics = AnalysisMetrics(
        registry,
        e2e_clock_skew_tolerance_seconds=0.1,
    )

    metrics.observe_e2e(-0.05)

    assert (
        registry.get_sample_value("nilm_analysis_e2e_duration_seconds_count")
        == 1
    )
    assert registry.get_sample_value("nilm_analysis_e2e_duration_seconds_sum") == 0
    assert registry.get_sample_value(
        "nilm_analysis_errors_total",
        {"stage": "e2e", "error_type": "ClockSkew"},
    ) is None


def test_negative_e2e_exceeding_tolerance_is_counted_as_clock_skew() -> None:
    registry = CollectorRegistry()
    metrics = AnalysisMetrics(
        registry,
        e2e_clock_skew_tolerance_seconds=0.1,
    )

    metrics.observe_e2e(-0.5)

    assert registry.get_sample_value("nilm_analysis_e2e_duration_seconds_count") == 0
    assert registry.get_sample_value(
        "nilm_analysis_errors_total",
        {"stage": "e2e", "error_type": "ClockSkew"},
    ) == 1


def test_negative_e2e_tolerance_must_be_non_negative() -> None:
    with pytest.raises(ValueError, match="must be non-negative"):
        AnalysisMetrics(
            CollectorRegistry(),
            e2e_clock_skew_tolerance_seconds=-0.1,
        )
