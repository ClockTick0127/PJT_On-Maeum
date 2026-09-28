from datetime import date
import json
from pathlib import Path

from history_generator.rundates import daily_command, last_json, run_dates
from history_generator.scenario import load_scenario
from history_generator.schedule import plan_day
from history_generator.snapshots import snapshot_lines, write_household_inputs
from history_generator.targets import merged_targets
from history_generator.waveform import SyntheticWaveform


def scenario_path(tmp_path: Path) -> Path:
    document = {
        "range": {"start": "2026-06-25", "end": "2026-06-26"},
        "households": [
            {"household_id": "T001", "seed": 1,
             "schedule": [{"appliance": "kettle", "median": "08:50", "probability": 1.0,
                           "duration_seconds": [150, 210]}],
             "periods": [{"name": "missing", "start": "2026-06-26", "missing_windows": [["09:00", "15:00"]]}],
             "away": [{"start": "2026-06-25T09:00:00+09:00", "end": "2026-06-25T18:00:00+09:00"}]},
        ],
        "load_households": {"count": 2, "templates": ["T001"], "seed": 3},
    }
    path = tmp_path / "s.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def test_snapshot_lines_follow_the_contract_and_skip_missing_windows(tmp_path):
    scenario = load_scenario(scenario_path(tmp_path))
    household = scenario.household("T001")
    plan = plan_day(household, date(2026, 6, 26))
    waveform = SyntheticWaveform().generate(plan, 1)
    lines = [json.loads(line) for line in snapshot_lines(scenario, plan, waveform, interval=60)]
    assert len(lines) == 1440 - 6 * 60
    first = lines[0]
    assert first["schema_version"] == 1 and first["household_id"] == "T001"
    assert first["observed_at"] == "2026-06-26T00:00:00+09:00"
    assert [item["appliance_type"] for item in first["appliances"]] == [
        "KETTLE", "INDUCTION", "IRON", "MICROWAVE", "HAIR_DRYER", "VACUUM_CLEANER"]
    kettle_on = [line for line in lines if line["appliances"][0]["is_on"]]
    assert kettle_on, "the kettle use must appear in at least one 60 s snapshot"
    observed = [line["observed_at"] for line in lines]
    assert observed == sorted(observed) and len(set(observed)) == len(observed)


def test_write_household_inputs_creates_config_with_away_periods(tmp_path):
    scenario = load_scenario(scenario_path(tmp_path))
    household = scenario.household("T001")
    generator = SyntheticWaveform()

    def day_plans(day):
        plan = plan_day(household, day)
        return plan, generator.generate(plan, 1)

    summary = write_household_inputs(scenario, household, tmp_path / "out", day_plans, interval=300)
    directory = Path(summary["directory"])
    config = json.loads((directory / "config.json").read_text(encoding="utf-8"))
    assert config["householdId"] == "T001"
    assert config["start"] == "2026-06-25T00:00:00+09:00" and config["end"] == "2026-06-27T00:00:00+09:00"
    assert config["awayPeriods"] == [{"start": "2026-06-25T09:00:00+09:00", "end": "2026-06-25T18:00:00+09:00"}]
    assert (directory / "policy.json").exists()
    assert summary["snapshots"] == sum(1 for _ in (directory / "snapshots.jsonl").open(encoding="utf-8"))


def test_targets_merge_keeps_existing_and_adds_scenario_households(tmp_path):
    scenario = load_scenario(scenario_path(tmp_path))
    existing = {"config_version": "observation-targets-v1", "targets": [
        {"household_id": "H001", "device_id": "main", "effective_from": "2026-09-01T00:00:00+09:00",
         "effective_to": None, "sampling_interval_seconds": 1, "observation_enabled": True}]}
    demo = merged_targets(scenario, existing, include_load=False)
    assert [item["household_id"] for item in demo["targets"]] == ["H001", "T001"]
    assert demo["targets"][1]["effective_from"] == "2026-06-25T00:00:00+09:00"
    with_load = merged_targets(scenario, existing, include_load=True)
    assert [item["household_id"] for item in with_load["targets"]] == ["H001", "T001", "L0001", "L0002"]
    assert with_load["targets"][-1]["sampling_interval_seconds"] == 10


def test_run_dates_records_reports_and_stops_on_failure(tmp_path):
    calls = []

    class Completed:
        def __init__(self, code, stdout):
            self.returncode, self.stdout, self.stderr = code, stdout, ""

    def fake_runner(command, **_):
        calls.append(command)
        day = command[command.index("--as-of") + 1]
        if day == "2026-06-26":
            return Completed(10, json.dumps({"status": "INPUT_INCOMPLETE", "exit_code": 10}))
        if day == "2026-06-27":
            return Completed(1, "boom\n" + json.dumps({"status": "FAILED", "exit_code": 1}))
        return Completed(12, json.dumps({"status": "PUBLISH_PENDING", "exit_code": 12}))

    results = run_dates(date(2026, 6, 25), date(2026, 6, 28), log_path=tmp_path / "log.jsonl", dry_run=False,
                        keep_going=False, runner=fake_runner, compose_file="c.yaml", service="gold-profile",
                        extra_env={"OBSERVATION_TARGETS_FILE": "/t.json"}, attempts=2, publish=False)
    assert [item.exit_code for item in results] == [12, 10, 1]
    assert results[0].command[:6] == ["docker", "compose", "-f", "c.yaml", "run", "--rm"]
    assert "--no-publish" in results[0].command and "-e" in results[0].command
    logged = [json.loads(line) for line in (tmp_path / "log.jsonl").read_text(encoding="utf-8").splitlines()]
    assert logged[1]["status"] == "INPUT_INCOMPLETE" and logged[2]["report"]["status"] == "FAILED"
    assert last_json("noise\n{\"a\": 1}\n") == {"a": 1}
    assert last_json("WARN x\n{\n  \"status\": \"SUCCEEDED\",\n  \"stages\": [{\"stage\": \"a\"}]\n}\n") == {
        "status": "SUCCEEDED", "stages": [{"stage": "a"}]}
    pretty = "INFO\n{\n \"status\": \"PUBLISH_PENDING\",\n \"stages\": [\n  {\n   \"stage\": \"publish\",\n   \"status\": \"SKIPPED\"\n  }\n ]\n}"
    assert last_json(pretty)["status"] == "PUBLISH_PENDING"
    assert last_json("{\"a\": 1}\n{\"b\": 2}") == {"b": 2}
    command = daily_command(date(2026, 1, 1), compose_file=None, service="s", extra_env={}, attempts=1,
                            publish=True, volumes=("/w:/work:ro",))
    assert command == ["docker", "compose", "run", "--rm", "--no-deps", "-v", "/w:/work:ro", "s", "gold-profile",
                       "daily", "--as-of", "2026-01-01", "--attempts", "1"]


def test_treat_as_active_relabels_only_shadow_and_keeps_time_fields():
    from history_generator.profiles import TREATED_AS_ACTIVE_NOTE, sort_key, treat_as_active

    payloads = [
        {"household_id": "H1", "delivery_mode": "SHADOW", "as_of_date": "2026-07-23", "profile_revision": 1,
         "published_at": "2026-09-24T13:00:00Z"},
        {"household_id": "H1", "delivery_mode": "ACTIVE", "as_of_date": "2026-07-22", "profile_revision": 2},
    ]
    result = treat_as_active(payloads)
    assert [p["delivery_mode"] for p in result] == ["ACTIVE", "ACTIVE"]
    assert result[0]["history_generator_note"] == TREATED_AS_ACTIVE_NOTE and "history_generator_note" not in result[1]
    assert result[0]["published_at"] == "2026-09-24T13:00:00Z"
    assert payloads[0]["delivery_mode"] == "SHADOW"  # input untouched
    assert sorted(result, key=sort_key)[0]["as_of_date"] == "2026-07-22"


def test_virtual_publish_uses_the_day_after_as_of_and_keeps_originals():
    from history_generator.profiles import virtual_publish

    payloads = [{"household_id": "H1", "as_of_date": "2026-07-22", "profile_revision": 1,
                 "effective_from": "2026-09-24T13:40:00+09:00", "published_at": "2026-09-24T04:40:00Z"}]
    result = virtual_publish(payloads)
    assert result[0]["effective_from"] == "2026-07-23T00:00:00+09:00"
    assert result[0]["published_at"] == "2026-07-22T15:00:00Z"
    assert result[0]["history_generator_original"]["published_at"] == "2026-09-24T04:40:00Z"
    assert payloads[0]["effective_from"] == "2026-09-24T13:40:00+09:00"
