from __future__ import annotations

import argparse
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from tools.consumer_load_test import (
    BenchmarkResult,
    ConsumerLoadTest,
    LoadTestError,
    counter_delta,
    LoadTestConfig,
    PrometheusClient,
    build_parser,
    config_from_args,
    default_repo_root,
    metric_selector,
    parse_consumers,
    prom_duration,
    render_markdown_report,
    throughput_comparison,
    write_reports,
)


def result_fixture() -> BenchmarkResult:
    return BenchmarkResult(
        consumer_count=2,
        instances=["172.22.0.5:8000", "172.22.0.6:8000"],
        expected_input_rate=40.0,
        started_at="2026-09-21T00:00:00+00:00",
        measurement_started_at="2026-09-21T00:05:00+00:00",
        measurement_finished_at="2026-09-21T00:10:00+00:00",
        measurement_seconds=300.0,
        rebalance_seconds=3.0,
        warmup_seconds=299.0,
        warmup_households_peak=20.0,
        household_state_resets=20.0,
        rebalance_events={"assign": 2.0, "revoke": 1.0, "lost": 0.0},
        assigned_partitions=24.0,
        processed_messages=12000.0,
        throughput_avg=40.0,
        throughput_peak=42.0,
        observed_ingress_rate=40.0,
        throughput_by_instance={"172.22.0.5:8000": 20.0, "172.22.0.6:8000": 20.0},
        lag_avg=1.0,
        lag_max=8.0,
        lag_at_measurement_start=0.0,
        lag_at_measurement_end=0.0,
        lag_recovery_seconds=2.0,
        e2e_p95_ms=125.0,
        stage_avg_ms={"observation_db": 8.0},
        stage_p95_ms={"observation_db": 15.0, "inference": 1.0},
        failed_messages=0.0,
        dlq_messages=0.0,
        errors=0.0,
        errors_by_type={},
        cpu_cores_avg=0.8,
        memory_mib_avg=210.0,
        memory_mib_max=230.0,
        simulator_log="simulator-consumers-2.log",
    )


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1", (1,)),
        ("1,2,4", (1, 2, 4)),
        (" 2, 4 ", (2, 4)),
    ],
)
def test_parse_consumers(raw: str, expected: tuple[int, ...]) -> None:
    assert parse_consumers(raw) == expected


@pytest.mark.parametrize("raw", ["", "0", "1,-2", "1,1", "one"])
def test_parse_consumers_rejects_invalid_values(raw: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        parse_consumers(raw)


def test_seed_option_passes_to_simulator_and_metadata(tmp_path: Path) -> None:
    args = build_parser().parse_args(["--seed", "0", "--repo-root", str(tmp_path)])
    config = config_from_args(args)
    assert config.seed == 0
    assert config.repo_root == tmp_path.resolve()

    config = LoadTestConfig(repo_root=tmp_path, output_dir=tmp_path, seed=config.seed)
    runner = ConsumerLoadTest(config, PrometheusClient("http://prometheus.invalid"))
    runner.write_metadata()
    assert json.loads((tmp_path / "metadata.json").read_text(encoding="utf-8"))["seed"] == 0

    with patch("tools.consumer_load_test.subprocess.Popen") as popen:
        popen.return_value.poll.return_value = 0
        runner.start_simulator(2)
        command = popen.call_args.args[0]
        assert command[-2:] == ["--seed", "0"]
        runner.stop_simulator()


@pytest.mark.parametrize("seed", ["-1", str(2**31)])
def test_seed_option_rejects_out_of_range_values(seed: str, tmp_path: Path) -> None:
    args = build_parser().parse_args(["--seed", seed, "--repo-root", str(tmp_path)])
    with pytest.raises(LoadTestError, match="seed must be between"):
        config_from_args(args)


def test_default_repo_root_is_inferred_from_the_repository_layout(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    (repo / "infrastructure").mkdir(parents=True)
    tool = repo / "ai" / "realtime-analysis-service" / "tools" / "consumer_load_test.py"
    tool.parent.mkdir(parents=True)
    tool.touch()
    assert default_repo_root(tool) == repo.resolve()


def test_default_repo_root_fails_clearly_outside_the_repository(tmp_path: Path) -> None:
    # e.g. the Docker test image copies the tool to /app/tools without the repo above it
    tool = tmp_path / "tools" / "consumer_load_test.py"
    tool.parent.mkdir()
    tool.touch()
    with pytest.raises(LoadTestError, match="--repo-root"):
        default_repo_root(tool)


def test_metric_selector_escapes_instance_regex_characters() -> None:
    selector = metric_selector(
        ["172.22.0.5:8000", "host+blue:8000"],
        'status="processed"',
    )

    assert 'job="ai-analysis"' in selector
    assert 'status="processed"' in selector
    assert "(?:" not in selector
    assert "172\\\\.22\\\\.0\\\\.5:8000" in selector
    assert "host\\\\+blue:8000" in selector


def test_prom_duration_rounds_up_and_has_minimum() -> None:
    assert prom_duration(0.1) == "2s"
    assert prom_duration(10.1) == "11s"


def test_prometheus_vector_omits_nan_values() -> None:
    client = PrometheusClient("http://prometheus.invalid")
    client.query = lambda expression, at=None: [  # type: ignore[method-assign]
        {"metric": {"stage": "inference"}, "value": [1, "NaN"]},
        {"metric": {"stage": "observation_db"}, "value": [1, "0.012"]},
    ]

    assert client.vector("ignored", "stage") == {"observation_db": 0.012}


def test_reports_include_summary_and_machine_readable_results(tmp_path: Path) -> None:
    config = LoadTestConfig(repo_root=tmp_path, consumers=(2,), houses=40, hz=1.0)
    result = result_fixture()

    write_reports(tmp_path, config, [result])

    payload = json.loads((tmp_path / "results.json").read_text(encoding="utf-8"))
    assert payload["results"][0]["throughput_avg"] == 40.0
    csv_text = (tmp_path / "summary.csv").read_text(
        encoding="utf-8-sig"
    )
    assert "observation_db_p95_ms" in csv_text
    assert "scaling_efficiency_percent" in csv_text
    assert "PASS" in csv_text
    report = (tmp_path / "report.md").read_text(encoding="utf-8")
    assert "| 2 | PASS | 40.00 msg/s | 40.00 msg/s" in report
    assert "`observation_db`" in report


def test_markdown_report_calls_out_outbox_limitation(tmp_path: Path) -> None:
    config = LoadTestConfig(repo_root=tmp_path, consumers=(2,))

    report = render_markdown_report(config, [result_fixture()])

    assert "Outbox" in report
    assert "lost" in report


def test_throughput_comparison_uses_consumer_ratio() -> None:
    baseline = result_fixture()
    baseline.consumer_count = 1
    baseline.throughput_avg = 20.0
    scaled = result_fixture()
    scaled.consumer_count = 4
    scaled.throughput_avg = 60.0

    speedup, efficiency = throughput_comparison(baseline, scaled)

    assert speedup == 3.0
    assert efficiency == 75.0


def test_counter_delta_handles_new_and_reset_instances() -> None:
    before = {
        ("old", "assign"): 4.0,
        ("reset", "assign"): 9.0,
    }
    after = {
        ("old", "assign"): 5.0,
        ("new", "assign"): 1.0,
        ("reset", "assign"): 1.0,
    }

    assert counter_delta(before, after) == {
        ("old", "assign"): 1.0,
        ("new", "assign"): 1.0,
        ("reset", "assign"): 1.0,
    }
