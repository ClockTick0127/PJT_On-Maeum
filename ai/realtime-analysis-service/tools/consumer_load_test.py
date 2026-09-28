"""Automate local 1/2/4 consumer load comparisons.

The runner intentionally uses only the Python standard library. It controls the
local Docker Compose project, starts the existing MQTT simulator, queries the
Prometheus HTTP API, and writes JSON/CSV/Markdown reports.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from importlib.util import find_spec
import json
import math
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
from typing import Callable, Iterable, Mapping, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen


MIB = 1024 * 1024
IMPORTANT_STAGES = (
    "observation_db",
    "inference",
    "activity_db",
    "snapshot_publish_ack",
    "anomaly_detection",
    "event_publish_ack",
    "offset_store",
)


class LoadTestError(RuntimeError):
    """Raised when the load test cannot produce a trustworthy comparison."""


@dataclass(frozen=True)
class LoadTestConfig:
    repo_root: Path
    consumers: tuple[int, ...] = (1, 2, 4)
    houses: int = 40
    hz: float = 1.0
    seed: int | None = None
    measurement_seconds: int = 300
    model_window_size: int = 299
    warmup_timeout_seconds: int = 1800
    recovery_timeout_seconds: int = 1800
    assignment_timeout_seconds: int = 180
    poll_seconds: float = 5.0
    scrape_grace_seconds: float = 3.0
    prometheus_url: str = "http://localhost:19090"
    output_dir: Path | None = None
    skip_build: bool = False
    restore_consumers: int | None = 1

    @property
    def compose_dir(self) -> Path:
        return self.repo_root / "infrastructure" / "local"

    @property
    def simulator_dir(self) -> Path:
        return self.repo_root / "infrastructure" / "mqtt" / "simulator"

    @property
    def simulator_file(self) -> Path:
        return self.simulator_dir / "simulator.py"

    @property
    def expected_input_rate(self) -> float:
        return self.houses * self.hz

    def resolved_output_dir(self) -> Path:
        if self.output_dir is not None:
            return self.output_dir.resolve()
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        return (
            self.repo_root
            / "ai"
            / "realtime-analysis-service"
            / "load-test-results"
            / stamp
        )


@dataclass
class BenchmarkResult:
    consumer_count: int
    instances: list[str]
    expected_input_rate: float
    started_at: str
    measurement_started_at: str
    measurement_finished_at: str
    measurement_seconds: float
    rebalance_seconds: float
    warmup_seconds: float
    warmup_households_peak: float
    household_state_resets: float
    rebalance_events: dict[str, float]
    assigned_partitions: float
    processed_messages: float
    throughput_avg: float
    throughput_peak: float
    observed_ingress_rate: float
    throughput_by_instance: dict[str, float]
    lag_avg: float
    lag_max: float
    lag_at_measurement_start: float
    lag_at_measurement_end: float
    lag_recovery_seconds: float
    e2e_p95_ms: float | None
    stage_avg_ms: dict[str, float]
    stage_p95_ms: dict[str, float]
    failed_messages: float
    dlq_messages: float
    errors: float
    errors_by_type: dict[str, float]
    cpu_cores_avg: float
    memory_mib_avg: float
    memory_mib_max: float
    simulator_log: str


class PrometheusClient:
    """Minimal Prometheus HTTP API client."""

    def __init__(self, base_url: str, timeout_seconds: float = 10.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def _get(self, path: str, params: Mapping[str, object]) -> dict[str, object]:
        url = f"{self.base_url}{path}?{urlencode(params)}"
        try:
            with urlopen(url, timeout=self.timeout_seconds) as response:
                payload = json.load(response)
        except (HTTPError, URLError, TimeoutError) as error:
            raise LoadTestError(f"Prometheus request failed: {url}: {error}") from error
        if payload.get("status") != "success":
            raise LoadTestError(f"Prometheus query failed: {payload}")
        return payload

    def query(self, expression: str, *, at: float | None = None) -> list[dict[str, object]]:
        params: dict[str, object] = {"query": expression}
        if at is not None:
            params["time"] = at
        payload = self._get("/api/v1/query", params)
        return list(payload["data"]["result"])  # type: ignore[index]

    def query_range(
        self,
        expression: str,
        *,
        start: float,
        end: float,
        step: float,
    ) -> list[dict[str, object]]:
        payload = self._get(
            "/api/v1/query_range",
            {"query": expression, "start": start, "end": end, "step": step},
        )
        return list(payload["data"]["result"])  # type: ignore[index]

    def scalar(
        self,
        expression: str,
        *,
        default: float = 0.0,
        at: float | None = None,
    ) -> float:
        result = self.query(expression, at=at)
        if not result:
            return default
        return float(result[0]["value"][1])  # type: ignore[index]

    def vector(
        self,
        expression: str,
        label: str,
        *,
        at: float | None = None,
    ) -> dict[str, float]:
        values: dict[str, float] = {}
        for item in self.query(expression, at=at):
            metric = item.get("metric", {})
            key = str(metric.get(label, "unknown"))  # type: ignore[union-attr]
            value = float(item["value"][1])  # type: ignore[index]
            # Empty histogram windows are returned as NaN. They are omitted so
            # JSON reports remain standards-compliant and Markdown shows N/A.
            if math.isfinite(value):
                values[key] = value
        return values

    def labeled_vector(
        self,
        expression: str,
        labels: Sequence[str],
        *,
        at: float | None = None,
    ) -> dict[tuple[str, ...], float]:
        values: dict[tuple[str, ...], float] = {}
        for item in self.query(expression, at=at):
            metric = item.get("metric", {})
            key = tuple(str(metric.get(label, "unknown")) for label in labels)  # type: ignore[union-attr]
            value = float(item["value"][1])  # type: ignore[index]
            if math.isfinite(value):
                values[key] = value
        return values

    def live_instances(self) -> list[str]:
        result = self.query('up{job="ai-analysis"} == 1')
        return sorted(str(item["metric"]["instance"]) for item in result)  # type: ignore[index]

    def range_values(
        self,
        expression: str,
        *,
        start: float,
        end: float,
        step: float,
    ) -> list[float]:
        values: list[float] = []
        for series in self.query_range(expression, start=start, end=end, step=step):
            values.extend(float(sample[1]) for sample in series.get("values", []))
        return values


def parse_consumers(raw: str) -> tuple[int, ...]:
    try:
        parsed = tuple(int(value.strip()) for value in raw.split(",") if value.strip())
    except ValueError as error:
        raise argparse.ArgumentTypeError("consumers must be comma-separated integers") from error
    if not parsed or any(value <= 0 for value in parsed):
        raise argparse.ArgumentTypeError("consumers must contain positive integers")
    if len(set(parsed)) != len(parsed):
        raise argparse.ArgumentTypeError("consumers must not contain duplicates")
    return parsed


def prom_duration(seconds: float) -> str:
    return f"{max(2, math.ceil(seconds))}s"


def utc_iso(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def metric_selector(instances: Sequence[str], *extra_labels: str) -> str:
    if not instances:
        raise LoadTestError("no live AI consumer instances were discovered")
    pattern = "|".join(re.escape(instance) for instance in instances)
    pattern = pattern.replace("\\", "\\\\").replace('"', '\\"')
    # Prometheus uses RE2, which does not support non-capturing groups.
    labels = ['job="ai-analysis"', f'instance=~"({pattern})"', *extra_labels]
    return "{" + ",".join(labels) + "}"


def max_or_zero(values: Iterable[float]) -> float:
    values = list(values)
    return max(values) if values else 0.0


def average_or_zero(values: Iterable[float]) -> float:
    values = list(values)
    return sum(values) / len(values) if values else 0.0


def throughput_comparison(
    baseline: BenchmarkResult,
    current: BenchmarkResult,
) -> tuple[float, float]:
    """Return speedup and scaling efficiency relative to the first result."""

    if baseline.throughput_avg <= 0:
        return 0.0, 0.0
    speedup = current.throughput_avg / baseline.throughput_avg
    replica_ratio = current.consumer_count / baseline.consumer_count
    efficiency = (speedup / replica_ratio) * 100 if replica_ratio > 0 else 0.0
    return speedup, efficiency


def correctness_issues(result: BenchmarkResult) -> list[str]:
    issues: list[str] = []
    if round(result.assigned_partitions) != 24:
        issues.append(f"partitions={result.assigned_partitions:.0f}")
    if result.rebalance_events.get("lost", 0.0) >= 0.5:
        issues.append(f"lost={result.rebalance_events['lost']:.0f}")
    if result.failed_messages >= 0.5:
        issues.append(f"failed={result.failed_messages:.0f}")
    if result.errors >= 0.5:
        issues.append(f"errors={result.errors:.0f}")
    if result.dlq_messages >= 0.5:
        issues.append(f"dlq={result.dlq_messages:.0f}")
    return issues


def counter_delta(
    before: Mapping[tuple[str, ...], float],
    after: Mapping[tuple[str, ...], float],
) -> dict[tuple[str, ...], float]:
    """Calculate counter increments while tolerating container counter resets."""

    deltas: dict[tuple[str, ...], float] = {}
    for key, current in after.items():
        previous = before.get(key, 0.0)
        deltas[key] = current - previous if current >= previous else current
    return deltas


def run_checked(command: Sequence[str], *, cwd: Path) -> None:
    print(f"$ {' '.join(command)}", flush=True)
    try:
        subprocess.run(command, cwd=cwd, check=True)
    except FileNotFoundError as error:
        raise LoadTestError(f"command not found: {command[0]}") from error
    except subprocess.CalledProcessError as error:
        raise LoadTestError(
            f"command failed with exit code {error.returncode}: {' '.join(command)}"
        ) from error


def wait_until(
    description: str,
    *,
    timeout_seconds: float,
    poll_seconds: float,
    probe: Callable[[], tuple[bool, str]],
) -> float:
    started = time.monotonic()
    last_detail: str | None = None
    while True:
        ready, detail = probe()
        if detail != last_detail:
            print(f"[{description}] {detail}", flush=True)
            last_detail = detail
        if ready:
            return time.monotonic() - started
        if time.monotonic() - started >= timeout_seconds:
            raise LoadTestError(f"timed out waiting for {description}: {detail}")
        time.sleep(poll_seconds)


class ConsumerLoadTest:
    def __init__(self, config: LoadTestConfig, prometheus: PrometheusClient) -> None:
        self.config = config
        self.prometheus = prometheus
        self.output_dir = config.resolved_output_dir()
        self.results: list[BenchmarkResult] = []
        self._simulator: subprocess.Popen[str] | None = None
        self._simulator_log_handle = None

    def compose_command(self, *args: str) -> list[str]:
        return [
            "docker",
            "compose",
            "-f",
            "compose.yaml",
            "-f",
            "compose.scale.yaml",
            *args,
        ]

    def scale(self, consumers: int, *, recreate: bool = False) -> None:
        args = [
            "up",
            "-d",
            "--scale",
            f"realtime-analysis-service={consumers}",
        ]
        if recreate:
            args.extend(["--build", "--force-recreate"])
        else:
            args.append("--no-recreate")
        args.append("realtime-analysis-service")
        run_checked(self.compose_command(*args), cwd=self.config.compose_dir)

    def wait_for_assignment(self, expected_consumers: int) -> tuple[float, list[str]]:
        def probe() -> tuple[bool, str]:
            instances = self.prometheus.live_instances()
            assigned = self.prometheus.scalar(
                'sum(nilm_analysis_consumer_assigned_partitions{job="ai-analysis"} '
                'and on(instance) (up{job="ai-analysis"} == 1))'
            )
            ready = len(instances) == expected_consumers and round(assigned) == 24
            return ready, f"consumers={len(instances)}/{expected_consumers}, partitions={assigned:.0f}/24"

        elapsed = wait_until(
            "assignment",
            timeout_seconds=self.config.assignment_timeout_seconds,
            poll_seconds=self.config.poll_seconds,
            probe=probe,
        )
        return elapsed, self.prometheus.live_instances()

    def capture_lifecycle_counters(
        self,
        *,
        instances: Sequence[str] | None = None,
        at: float | None = None,
    ) -> tuple[dict[tuple[str, ...], float], dict[tuple[str, ...], float]]:
        live_instances = list(instances) if instances is not None else self.prometheus.live_instances()
        if not live_instances:
            return {}, {}
        selector = metric_selector(live_instances)
        rebalances = self.prometheus.labeled_vector(
            "sum by(instance,event) "
            f"(nilm_analysis_consumer_rebalances_total{selector})",
            ("instance", "event"),
            at=at,
        )
        resets = self.prometheus.labeled_vector(
            "sum by(instance) "
            f"(nilm_analysis_household_state_resets_total{selector})",
            ("instance",),
            at=at,
        )
        return rebalances, resets

    def start_simulator(self, consumers: int) -> Path:
        log_path = self.output_dir / f"simulator-consumers-{consumers}.log"
        self._simulator_log_handle = log_path.open("w", encoding="utf-8")
        command = [
            sys.executable,
            str(self.config.simulator_file),
            "--scenario",
            "random",
            "--houses",
            str(self.config.houses),
            "--hz",
            str(self.config.hz),
            "--count",
            "0",
            "--quiet",
        ]
        if self.config.seed is not None:
            command.extend(["--seed", str(self.config.seed)])
        print(f"$ {' '.join(command)}", flush=True)
        try:
            self._simulator = subprocess.Popen(
                command,
                cwd=self.config.simulator_dir,
                stdout=self._simulator_log_handle,
                stderr=subprocess.STDOUT,
                text=True,
            )
        except FileNotFoundError as error:
            self._simulator_log_handle.close()
            self._simulator_log_handle = None
            raise LoadTestError(f"Python executable not found: {sys.executable}") from error
        return log_path

    def stop_simulator(self) -> None:
        process = self._simulator
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        self._simulator = None
        if self._simulator_log_handle is not None:
            self._simulator_log_handle.close()
            self._simulator_log_handle = None

    def ensure_simulator_running(self) -> None:
        if self._simulator is None:
            raise LoadTestError("simulator process was not started")
        return_code = self._simulator.poll()
        if return_code is not None:
            raise LoadTestError(
                f"simulator exited unexpectedly with code {return_code}; check its log"
            )

    def wait_for_warmup(self, instances: Sequence[str], baseline: float) -> float:
        selector = metric_selector(instances, 'status="processed"')
        minimum_messages = self.config.houses * self.config.model_window_size

        def probe() -> tuple[bool, str]:
            self.ensure_simulator_running()
            processed = self.prometheus.scalar(
                f"sum(nilm_analysis_messages_total{selector})"
            )
            delta = max(0.0, processed - baseline)
            warmup = self.prometheus.scalar(
                f"sum(nilm_analysis_warmup_households{metric_selector(instances)})"
            )
            ready = delta >= minimum_messages and warmup == 0
            return ready, (
                f"processed={delta:.0f}/{minimum_messages}, "
                f"warmup_households={warmup:.0f}"
            )

        return wait_until(
            "warm-up",
            timeout_seconds=self.config.warmup_timeout_seconds,
            poll_seconds=self.config.poll_seconds,
            probe=probe,
        )

    def monitor_measurement(self, instances: Sequence[str]) -> tuple[float, float]:
        started = time.time()
        deadline = time.monotonic() + self.config.measurement_seconds
        throughput_selector = metric_selector(instances, 'status="processed"')
        lag_selector = metric_selector(instances)
        while True:
            self.ensure_simulator_running()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            throughput = self.prometheus.scalar(
                f"sum(rate(nilm_analysis_messages_total{throughput_selector}[30s]))"
            )
            lag = self.prometheus.scalar(
                f"sum(nilm_analysis_consumer_lag_messages{lag_selector})"
            )
            print(
                f"[measurement] remaining={remaining:.0f}s "
                f"throughput={throughput:.1f} msg/s lag={lag:.0f}",
                flush=True,
            )
            time.sleep(min(self.config.poll_seconds, max(remaining, 0.0)))
        return started, time.time()

    def collect_metrics(
        self,
        *,
        instances: Sequence[str],
        measurement_start: float,
        measurement_end: float,
        simulator_start: float,
        warmup_end: float,
        evaluation_time: float,
        rebalance_before: Mapping[tuple[str, ...], float],
        reset_before: Mapping[tuple[str, ...], float],
    ) -> dict[str, object]:
        measure_window = prom_duration(evaluation_time - measurement_start)
        measure_duration = max(measurement_end - measurement_start, 0.001)
        selector = metric_selector(instances)
        processed_selector = metric_selector(instances, 'status="processed"')
        failed_selector = metric_selector(instances, 'status="failed"')

        processed = self.prometheus.scalar(
            f"sum(increase(nilm_analysis_messages_total{processed_selector}[{measure_window}]))",
            at=evaluation_time,
        )
        rate_start = min(measurement_end, measurement_start + 30.0)
        throughput_series = self.prometheus.range_values(
            f"sum(rate(nilm_analysis_messages_total{processed_selector}[30s]))",
            start=rate_start,
            end=measurement_end,
            step=max(1.0, min(self.config.poll_seconds, 5.0)),
        )
        lag_series = self.prometheus.range_values(
            f"sum(nilm_analysis_consumer_lag_messages{selector})",
            start=measurement_start,
            end=measurement_end,
            step=max(1.0, min(self.config.poll_seconds, 5.0)),
        )
        memory_series = self.prometheus.range_values(
            f"sum(process_resident_memory_bytes{selector}) / {MIB}",
            start=measurement_start,
            end=measurement_end,
            step=max(1.0, min(self.config.poll_seconds, 5.0)),
        )
        warmup_series = self.prometheus.range_values(
            f"sum(nilm_analysis_warmup_households{selector})",
            start=simulator_start,
            end=warmup_end,
            step=max(1.0, min(self.config.poll_seconds, 5.0)),
        )

        e2e = self.prometheus.scalar(
            "histogram_quantile(0.95, sum by(le) "
            f"(increase(nilm_analysis_e2e_duration_seconds_bucket{selector}[{measure_window}])))",
            default=math.nan,
            at=evaluation_time,
        )
        stage_avg = self.prometheus.vector(
            "1000 * sum by(stage) "
            f"(increase(nilm_analysis_stage_duration_seconds_sum{selector}[{measure_window}])) "
            "/ sum by(stage) "
            f"(increase(nilm_analysis_stage_duration_seconds_count{selector}[{measure_window}]))",
            "stage",
            at=evaluation_time,
        )
        stage_p95 = self.prometheus.vector(
            "1000 * histogram_quantile(0.95, sum by(le,stage) "
            f"(increase(nilm_analysis_stage_duration_seconds_bucket{selector}[{measure_window}])))",
            "stage",
            at=evaluation_time,
        )
        throughput_by_instance = self.prometheus.vector(
            "sum by(instance) "
            f"(increase(nilm_analysis_messages_total{processed_selector}[{measure_window}])) "
            f"/ {measure_duration}",
            "instance",
            at=evaluation_time,
        )
        errors_by_labels = self.prometheus.labeled_vector(
            "sum by(stage,error_type) "
            f"(increase(nilm_analysis_errors_total{selector}[{measure_window}]))",
            ("stage", "error_type"),
            at=evaluation_time,
        )
        rebalance_after, reset_after = self.capture_lifecycle_counters(
            instances=instances,
            at=evaluation_time,
        )
        rebalance_deltas = counter_delta(rebalance_before, rebalance_after)
        reset_deltas = counter_delta(reset_before, reset_after)
        rebalance_events: dict[str, float] = {}
        for (_, event), value in rebalance_deltas.items():
            rebalance_events[event] = rebalance_events.get(event, 0.0) + value
        errors_by_type = {
            f"{stage}:{error_type}": value
            for (stage, error_type), value in errors_by_labels.items()
        }

        lag_start = lag_series[0] if lag_series else 0.0
        lag_end = lag_series[-1] if lag_series else 0.0
        throughput_avg = processed / measure_duration
        observed_ingress_rate = max(
            0.0,
            throughput_avg + ((lag_end - lag_start) / measure_duration),
        )
        return {
            "processed_messages": processed,
            "throughput_avg": throughput_avg,
            "throughput_peak": max_or_zero(throughput_series),
            "observed_ingress_rate": observed_ingress_rate,
            "throughput_by_instance": throughput_by_instance,
            "lag_avg": average_or_zero(lag_series),
            "lag_max": max_or_zero(lag_series),
            "lag_at_measurement_start": lag_start,
            "lag_at_measurement_end": lag_end,
            "e2e_p95_ms": None if not math.isfinite(e2e) else e2e * 1000,
            "stage_avg_ms": stage_avg,
            "stage_p95_ms": stage_p95,
            "failed_messages": self.prometheus.scalar(
                f"sum(increase(nilm_analysis_messages_total{failed_selector}[{measure_window}]))",
                at=evaluation_time,
            ),
            "dlq_messages": self.prometheus.scalar(
                "sum(increase(nilm_analysis_dlq_messages_total"
                f"{selector}[{measure_window}]))",
                at=evaluation_time,
            ),
            "errors": sum(errors_by_type.values()),
            "errors_by_type": errors_by_type,
            "cpu_cores_avg": self.prometheus.scalar(
                "sum(increase(process_cpu_seconds_total"
                f"{selector}[{measure_window}])) / {measure_duration}",
                at=evaluation_time,
            ),
            "memory_mib_avg": self.prometheus.scalar(
                f"sum(avg_over_time(process_resident_memory_bytes{selector}[{measure_window}])) / {MIB}",
                at=evaluation_time,
            ),
            "memory_mib_max": max_or_zero(memory_series),
            "warmup_households_peak": max_or_zero(warmup_series),
            "household_state_resets": sum(reset_deltas.values()),
            "rebalance_events": rebalance_events,
            "assigned_partitions": self.prometheus.scalar(
                f"sum(nilm_analysis_consumer_assigned_partitions{selector})",
                at=evaluation_time,
            ),
        }

    def wait_for_lag_recovery(
        self,
        instances: Sequence[str],
        *,
        recovery_started_at: float,
    ) -> float:
        selector = metric_selector(instances)
        wait_until(
            "lag recovery",
            timeout_seconds=self.config.recovery_timeout_seconds,
            poll_seconds=self.config.poll_seconds,
            probe=lambda: self._lag_probe(selector),
        )
        return time.time() - recovery_started_at

    def ensure_zero_lag_before_run(self, instances: Sequence[str]) -> None:
        # Assignment can become visible before the consumer's first lag refresh.
        # Waiting one poll interval avoids accepting the gauge's initial absence
        # as a trustworthy zero.
        time.sleep(self.config.poll_seconds)
        selector = metric_selector(instances)
        wait_until(
            "pre-run lag",
            timeout_seconds=self.config.recovery_timeout_seconds,
            poll_seconds=self.config.poll_seconds,
            probe=lambda: self._lag_probe(selector),
        )

    def _lag_probe(self, selector: str) -> tuple[bool, str]:
        lag = self.prometheus.scalar(
            f"sum(nilm_analysis_consumer_lag_messages{selector})"
        )
        return lag == 0, f"lag={lag:.0f}"

    def run_case(self, consumers: int, *, recreate: bool) -> BenchmarkResult:
        print(f"\n=== Consumer {consumers} benchmark ===", flush=True)
        rebalance_before, reset_before = self.capture_lifecycle_counters()
        scale_start = time.time()
        self.scale(consumers, recreate=recreate)
        rebalance_seconds, instances = self.wait_for_assignment(consumers)
        self.ensure_zero_lag_before_run(instances)
        selector = metric_selector(instances, 'status="processed"')
        baseline = self.prometheus.scalar(
            f"sum(nilm_analysis_messages_total{selector})"
        )

        simulator_log = self.start_simulator(consumers)
        simulator_start = time.time()
        warmup_seconds = self.wait_for_warmup(instances, baseline)
        warmup_end = time.time()
        measurement_start, measurement_end = self.monitor_measurement(instances)
        self.stop_simulator()
        recovery_started_at = time.time()
        time.sleep(self.config.scrape_grace_seconds)
        evaluation_time = time.time()
        recovery_seconds = self.wait_for_lag_recovery(
            instances,
            recovery_started_at=recovery_started_at,
        )
        metrics = self.collect_metrics(
            instances=instances,
            measurement_start=measurement_start,
            measurement_end=measurement_end,
            simulator_start=simulator_start,
            warmup_end=warmup_end,
            evaluation_time=evaluation_time,
            rebalance_before=rebalance_before,
            reset_before=reset_before,
        )
        current_instances = self.prometheus.live_instances()
        if current_instances != list(instances):
            raise LoadTestError(
                "consumer instances changed during the measurement: "
                f"before={instances}, after={current_instances}"
            )

        return BenchmarkResult(
            consumer_count=consumers,
            instances=list(instances),
            expected_input_rate=self.config.expected_input_rate,
            started_at=utc_iso(scale_start),
            measurement_started_at=utc_iso(measurement_start),
            measurement_finished_at=utc_iso(measurement_end),
            measurement_seconds=measurement_end - measurement_start,
            rebalance_seconds=rebalance_seconds,
            warmup_seconds=warmup_seconds,
            lag_recovery_seconds=recovery_seconds,
            simulator_log=str(simulator_log.relative_to(self.output_dir)),
            **metrics,
        )

    def run(self) -> list[BenchmarkResult]:
        self.validate()
        self.output_dir.mkdir(parents=True, exist_ok=False)
        self.write_metadata()
        try:
            for index, consumers in enumerate(self.config.consumers):
                result = self.run_case(
                    consumers,
                    recreate=index == 0 and not self.config.skip_build,
                )
                self.results.append(result)
                write_reports(self.output_dir, self.config, self.results)
        finally:
            self.stop_simulator()
            if self.config.restore_consumers is not None:
                print(
                    f"\nRestoring realtime-analysis-service to "
                    f"{self.config.restore_consumers} consumer(s)...",
                    flush=True,
                )
                self.scale(self.config.restore_consumers)
        return self.results

    def validate(self) -> None:
        required = (
            self.config.compose_dir / "compose.yaml",
            self.config.compose_dir / "compose.scale.yaml",
            self.config.simulator_file,
        )
        missing = [str(path) for path in required if not path.is_file()]
        if missing:
            raise LoadTestError(f"required files are missing: {', '.join(missing)}")
        if shutil.which("docker") is None:
            raise LoadTestError("docker CLI was not found on PATH")
        if find_spec("aiomqtt") is None:
            raise LoadTestError(
                "aiomqtt is not installed for this Python interpreter. Run "
                "'python -m pip install -r infrastructure/mqtt/simulator/requirements.txt'."
            )
        try:
            self.prometheus.query("up")
        except LoadTestError as error:
            raise LoadTestError(
                "Prometheus is not reachable. Start the local Compose stack first."
            ) from error

    def write_metadata(self) -> None:
        data = {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "repo_root": str(self.config.repo_root),
            "consumers": self.config.consumers,
            "houses": self.config.houses,
            "hz": self.config.hz,
            "seed": self.config.seed,
            "expected_input_rate": self.config.expected_input_rate,
            "measurement_seconds": self.config.measurement_seconds,
            "model_window_size": self.config.model_window_size,
            "prometheus_url": self.config.prometheus_url,
        }
        (self.output_dir / "metadata.json").write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


def write_reports(
    output_dir: Path,
    config: LoadTestConfig,
    results: Sequence[BenchmarkResult],
) -> None:
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "configuration": {
            "consumers": config.consumers,
            "houses": config.houses,
            "hz": config.hz,
            "expected_input_rate": config.expected_input_rate,
            "measurement_seconds": config.measurement_seconds,
            "model_window_size": config.model_window_size,
        },
        "results": [asdict(result) for result in results],
    }
    (output_dir / "results.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False),
        encoding="utf-8",
    )

    csv_fields = [
        "consumer_count",
        "correctness",
        "expected_input_rate",
        "throughput_avg",
        "throughput_peak",
        "observed_ingress_rate",
        "throughput_speedup",
        "scaling_efficiency_percent",
        "lag_avg",
        "lag_max",
        "lag_at_measurement_start",
        "lag_at_measurement_end",
        "lag_recovery_seconds",
        "e2e_p95_ms",
        "failed_messages",
        "dlq_messages",
        "errors",
        "errors_by_type",
        "cpu_cores_avg",
        "memory_mib_avg",
        "memory_mib_max",
        "rebalance_seconds",
        "warmup_seconds",
        "warmup_households_peak",
        "household_state_resets",
        "assigned_partitions",
        *[f"{stage}_p95_ms" for stage in IMPORTANT_STAGES],
    ]
    with (output_dir / "summary.csv").open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=csv_fields)
        writer.writeheader()
        baseline = results[0]
        for result in results:
            row = {field: getattr(result, field, "") for field in csv_fields}
            speedup, efficiency = throughput_comparison(baseline, result)
            issues = correctness_issues(result)
            row["correctness"] = "PASS" if not issues else f"WARN: {', '.join(issues)}"
            row["throughput_speedup"] = speedup
            row["scaling_efficiency_percent"] = efficiency
            row["errors_by_type"] = json.dumps(
                result.errors_by_type,
                ensure_ascii=False,
                sort_keys=True,
            )
            for stage in IMPORTANT_STAGES:
                row[f"{stage}_p95_ms"] = result.stage_p95_ms.get(stage, "")
            writer.writerow(row)

    (output_dir / "report.md").write_text(
        render_markdown_report(config, results),
        encoding="utf-8",
    )


def render_markdown_report(
    config: LoadTestConfig,
    results: Sequence[BenchmarkResult],
) -> str:
    def number(value: float | None, digits: int = 2) -> str:
        return "N/A" if value is None else f"{value:.{digits}f}"

    lines = [
        "# Realtime Analysis Consumer Load Test",
        "",
        f"- 입력: {config.houses}가구 × {config.hz:g}Hz = "
        f"{config.expected_input_rate:g} msg/s",
        f"- 안정 구간 측정 시간: {config.measurement_seconds}초",
        f"- 모델 입력 윈도우: {config.model_window_size}개",
        "",
        "## 요약",
        "",
        "| Consumers | Result | Observed input | Throughput avg | Speedup | Efficiency | "
        "Max lag | Lag recovery | E2E p95 | Errors | DLQ | CPU | RSS avg |",
        "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    if not results:
        return "\n".join(lines + ["", "완료된 측정이 없습니다.", ""])
    baseline = results[0]
    for result in results:
        speedup, efficiency = throughput_comparison(baseline, result)
        issues = correctness_issues(result)
        verdict = "PASS" if not issues else "WARN: " + ", ".join(issues)
        lines.append(
            f"| {result.consumer_count} | {verdict} | "
            f"{number(result.observed_ingress_rate)} msg/s | "
            f"{number(result.throughput_avg)} msg/s | "
            f"{number(speedup)}x | {number(efficiency)}% | "
            f"{number(result.lag_max, 0)} | "
            f"{number(result.lag_recovery_seconds)}s | {number(result.e2e_p95_ms)}ms | "
            f"{number(result.errors, 0)} | {number(result.dlq_messages, 0)} | "
            f"{number(result.cpu_cores_avg)} cores | {number(result.memory_mib_avg)} MiB |"
        )

    error_details = [result for result in results if result.errors_by_type]
    if error_details:
        lines.extend(["", "### 오류 상세", ""])
        for result in error_details:
            details = ", ".join(
                f"`{key}`={number(value, 0)}"
                for key, value in sorted(result.errors_by_type.items())
            )
            lines.append(f"- Consumer {result.consumer_count}: {details}")

    lines.extend(
        [
            "",
            "## 리밸런싱과 warm-up",
            "",
            "| Consumers | Assignment stable | Warm-up | Peak households | Resets | "
            "Partitions | assign/revoke/lost |",
            "| ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for result in results:
        events = result.rebalance_events
        lines.append(
            f"| {result.consumer_count} | {number(result.rebalance_seconds)}s | "
            f"{number(result.warmup_seconds)}s | "
            f"{number(result.warmup_households_peak, 0)} | "
            f"{number(result.household_state_resets, 0)} | "
            f"{number(result.assigned_partitions, 0)} | "
            f"{number(events.get('assign', 0), 0)}/"
            f"{number(events.get('revoke', 0), 0)}/"
            f"{number(events.get('lost', 0), 0)} |"
        )

    lines.extend(["", "## 단계별 지연 p95", ""])
    header = "| Stage | " + " | ".join(
        f"{result.consumer_count} consumers" for result in results
    ) + " |"
    lines.append(header)
    lines.append("| --- | " + " | ".join("---:" for _ in results) + " |")
    stages = sorted({stage for result in results for stage in result.stage_p95_ms})
    for stage in stages:
        lines.append(
            f"| `{stage}` | "
            + " | ".join(
                f"{number(result.stage_p95_ms.get(stage))}ms" for result in results
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## 판정 시 주의사항",
            "",
            "- 입력 부하가 단일 Consumer 처리 한도보다 낮으면 Consumer를 늘려도 전체 "
            "처리량은 입력 속도 이상 증가하지 않습니다.",
            "- `lost`, 오류, DLQ는 정상 random 부하에서 0이어야 합니다.",
            "- E2E는 시뮬레이터 `measured_at`부터 Snapshot 발행까지이므로 Kafka backlog를 "
            "포함합니다.",
            "- 이벤트 중복의 완전한 검증은 `analysis.event.v1`을 별도 수집해 `event_id`를 "
            "비교해야 합니다. 발행 성공 직후 DB 저장 전 장애는 Outbox 적용 전 남는 범위입니다.",
            "",
        ]
    )
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare local realtime analysis performance with 1/2/4 consumers.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--consumers", type=parse_consumers, default=(1, 2, 4))
    parser.add_argument("--houses", type=int, default=40)
    parser.add_argument("--hz", type=float, default=1.0)
    parser.add_argument("--seed", type=int, help="simulator random seed (0..2147483647)")
    parser.add_argument("--duration", type=int, default=300, dest="measurement_seconds")
    parser.add_argument("--model-window-size", type=int, default=299)
    parser.add_argument("--warmup-timeout", type=int, default=1800)
    parser.add_argument("--recovery-timeout", type=int, default=1800)
    parser.add_argument("--assignment-timeout", type=int, default=180)
    parser.add_argument("--poll-seconds", type=float, default=5.0)
    parser.add_argument("--prometheus-url", default="http://localhost:19090")
    parser.add_argument(
        "--repo-root",
        type=Path,
        help="repository root holding infrastructure/; inferred from this file's location when omitted",
    )
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument(
        "--restore-consumers",
        type=int,
        default=1,
        help="consumer count restored after success or failure; use 0 to disable",
    )
    return parser


def default_repo_root(tool_path: Path | None = None) -> Path:
    """Infer the repository root from this file's location inside the repo.

    The tool lives at <repo>/ai/realtime-analysis-service/tools/, so the root is three
    directories up. Outside that layout (e.g. copied into a container as /app/tools)
    there is no such ancestor, so fail with a hint instead of an IndexError.
    """
    here = (tool_path or Path(__file__)).resolve()
    parents = here.parents
    if len(parents) <= 3 or not (parents[3] / "infrastructure").is_dir():
        raise LoadTestError(
            "cannot infer the repository root from "
            f"{here}; pass --repo-root explicitly"
        )
    return parents[3]


def config_from_args(args: argparse.Namespace) -> LoadTestConfig:
    positive_fields = {
        "houses": args.houses,
        "hz": args.hz,
        "duration": args.measurement_seconds,
        "model-window-size": args.model_window_size,
        "warmup-timeout": args.warmup_timeout,
        "recovery-timeout": args.recovery_timeout,
        "assignment-timeout": args.assignment_timeout,
        "poll-seconds": args.poll_seconds,
    }
    invalid = [name for name, value in positive_fields.items() if value <= 0]
    if invalid:
        raise LoadTestError(f"options must be greater than zero: {', '.join(invalid)}")
    if args.seed is not None and not 0 <= args.seed <= 2**31 - 1:
        raise LoadTestError("seed must be between 0 and 2147483647")
    if args.restore_consumers < 0:
        raise LoadTestError("restore-consumers must be zero or greater")
    repo_root = args.repo_root.resolve() if args.repo_root is not None else default_repo_root()
    return LoadTestConfig(
        repo_root=repo_root,
        consumers=args.consumers,
        houses=args.houses,
        hz=args.hz,
        seed=args.seed,
        measurement_seconds=args.measurement_seconds,
        model_window_size=args.model_window_size,
        warmup_timeout_seconds=args.warmup_timeout,
        recovery_timeout_seconds=args.recovery_timeout,
        assignment_timeout_seconds=args.assignment_timeout,
        poll_seconds=args.poll_seconds,
        prometheus_url=args.prometheus_url,
        output_dir=args.output_dir,
        skip_build=args.skip_build,
        restore_consumers=args.restore_consumers or None,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        config = config_from_args(args)
        runner = ConsumerLoadTest(
            config,
            PrometheusClient(config.prometheus_url),
        )
        results = runner.run()
    except (LoadTestError, KeyboardInterrupt) as error:
        message = "cancelled by user" if isinstance(error, KeyboardInterrupt) else str(error)
        print(f"ERROR: {message}", file=sys.stderr)
        return 1
    print(f"\nCompleted {len(results)} benchmark(s): {runner.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
