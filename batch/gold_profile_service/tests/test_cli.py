import json
from types import SimpleNamespace

import pytest

import gold_profile.cli as cli
from gold_profile.daily import (
    RUN_INPUT_INCOMPLETE,
    RUN_PUBLISH_PENDING,
    STATUS_FAILED,
    STATUS_SKIPPED,
    STATUS_SUCCEEDED,
)


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (STATUS_SUCCEEDED, cli.EXIT_SUCCEEDED),
        (RUN_INPUT_INCOMPLETE, cli.EXIT_INPUT_INCOMPLETE),
        (STATUS_SKIPPED, cli.EXIT_SKIPPED),
        (RUN_PUBLISH_PENDING, cli.EXIT_PUBLISH_PENDING),
        (STATUS_FAILED, cli.EXIT_FAILED),
    ],
)
def test_daily_json_and_exit_code_follow_result_contract(
    monkeypatch, capsys, status, expected
):
    spark = SimpleNamespace(stopped=False)
    spark.stop = lambda: setattr(spark, "stopped", True)
    report = {
        "as_of_date": "2026-09-20",
        "status": status,
        "ok": status == STATUS_SUCCEEDED,
        "stages": [],
    }
    monkeypatch.setattr(cli, "create_storage", lambda _settings: object())
    monkeypatch.setattr(cli, "build_session", lambda _settings: spark)
    monkeypatch.setattr(cli, "SparkDailyStages", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(cli, "run_daily_pipeline", lambda *_args, **_kwargs: report)
    args = cli.build_parser().parse_args([
        "daily", "--as-of", "2026-09-20", "--no-publish"
    ])

    result = cli._daily(SimpleNamespace(business_utc_offset_seconds=32400), object(), args)

    payload = json.loads(capsys.readouterr().out)
    assert result == expected
    assert payload["status"] == status
    assert payload["exit_code"] == expected
    assert spark.stopped is True


def test_daily_initialization_failure_is_json_failure(monkeypatch, capsys):
    monkeypatch.setattr(cli, "create_storage", lambda _settings: object())
    monkeypatch.setattr(
        cli,
        "build_session",
        lambda _settings: (_ for _ in ()).throw(RuntimeError("spark unavailable")),
    )
    args = cli.build_parser().parse_args([
        "daily", "--as-of", "2026-09-20", "--no-publish"
    ])

    result = cli._daily(SimpleNamespace(business_utc_offset_seconds=32400), object(), args)

    payload = json.loads(capsys.readouterr().out)
    assert result == cli.EXIT_FAILED
    assert payload["status"] == STATUS_FAILED
    assert payload["exit_code"] == cli.EXIT_FAILED
    assert payload["error"] == "RuntimeError: spark unavailable"


@pytest.mark.parametrize(
    "arguments",
    [
        ["daily", "--attempts", "0"],
        ["daily", "--attempts", "-1"],
        ["daily", "--retry-seconds", "-0.1"],
        ["daily", "--retry-seconds", "nan"],
        ["daily", "--retry-seconds", "inf"],
        ["daily", "--as-of", "2026-02-30"],
        ["publish", "--loop", "--interval", "0"],
    ],
)
def test_invalid_numeric_arguments_are_rejected(arguments):
    with pytest.raises(SystemExit) as error:
        cli.build_parser().parse_args(arguments)

    assert error.value.code == cli.EXIT_INVALID_ARGUMENT
