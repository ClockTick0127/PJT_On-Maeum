from datetime import date
import json
from pathlib import Path

import pytest

from history_generator.scenario import (
    MERGE_GAP_SECONDS,
    ScenarioError,
    load_scenario,
    parse_clock,
)
from history_generator.schedule import plan_day


SCENARIOS = Path(__file__).resolve().parents[1] / "scenarios"


def write_scenario(tmp_path: Path, document: dict) -> Path:
    path = tmp_path / "scenario.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def base_document(**overrides) -> dict:
    document = {
        "range": {"start": "2026-06-25", "end": "2026-07-04"},
        "households": [
            {
                "household_id": "T001",
                "seed": 1,
                "schedule": [
                    {"appliance": "kettle", "median": "08:50", "jitter_minutes": 15,
                     "probability": 1.0, "duration_seconds": [150, 210]},
                    {"appliance": "induction", "median": "19:00", "jitter_minutes": 20,
                     "probability": 1.0, "duration_seconds": [600, 840],
                     "segments": [3, 5], "segment_gap_seconds": [20, 60]},
                    {"appliance": "iron", "median": "12:30", "probability": 1.0,
                     "duration_seconds": [300, 420], "weekdays": ["MON", "WED", "FRI"]},
                ],
                "periods": [
                    {"name": "change", "start": "2026-06-28", "end": "2026-06-29",
                     "shift_minutes": {"kettle": 150}},
                    {"name": "missing", "start": "2026-06-30",
                     "missing_windows": [["09:00", "15:00"]]},
                    {"name": "reduced", "start": "2026-07-01", "end": "2026-07-01",
                     "keep_first_use_only": True},
                    {"name": "inactive", "start": "2026-07-02", "end": "2026-07-03",
                     "probability_override": {"*": 0.0}},
                    {"name": "long", "start": "2026-07-04", "end": "2026-07-04",
                     "extend_first_use_seconds": {"induction": 10800}},
                ],
            }
        ],
    }
    document.update(overrides)
    return document


def test_parse_clock_accepts_midnight_and_rejects_garbage():
    assert parse_clock("08:50") == 8 * 3600 + 50 * 60
    assert parse_clock("24:00") == 86_400
    with pytest.raises(ScenarioError):
        parse_clock("25:00")


def test_demo_scenario_file_loads_and_is_deterministic():
    scenario = load_scenario(SCENARIOS / "demo_3households.json")
    assert [item.household_id for item in scenario.demo_households()] == ["H008", "H009", "H010"]
    assert len(scenario.dates()) == 91
    household = scenario.household("H008")
    first = plan_day(household, date(2026, 7, 1))
    again = plan_day(household, date(2026, 7, 1))
    assert first == again
    assert first.first_use_second("kettle") is not None


def test_plan_applies_shift_keep_first_inactive_and_missing(tmp_path):
    scenario = load_scenario(write_scenario(tmp_path, base_document()))
    household = scenario.household("T001")

    normal = plan_day(household, date(2026, 6, 26))
    kettle_start = normal.first_use_second("kettle")
    assert 8 * 3600 + 35 * 60 <= kettle_start <= 9 * 3600 + 5 * 60
    induction = [use for use in normal.uses if use.appliance == "induction"][0]
    assert 3 <= len(induction.sessions) <= 5
    gaps = [b.start_second - a.end_second for a, b in zip(induction.sessions, induction.sessions[1:])]
    assert all(0 < gap <= MERGE_GAP_SECONDS["induction"] for gap in gaps)
    assert all(session.duration >= 10 for session in normal.sessions)

    shifted = plan_day(household, date(2026, 6, 28))
    assert 11 * 3600 + 5 * 60 <= shifted.first_use_second("kettle") <= 11 * 3600 + 35 * 60

    missing = plan_day(household, date(2026, 6, 30))
    assert missing.missing_windows == ((9 * 3600, 15 * 3600),)
    assert missing.is_missing(10 * 3600) and not missing.is_missing(8 * 3600)

    reduced = plan_day(household, date(2026, 7, 1))  # Wednesday -> iron applies
    assert reduced.use_count("kettle") == 1 and reduced.use_count("iron") == 1

    inactive = plan_day(household, date(2026, 7, 2))
    assert inactive.uses == ()

    long_use = plan_day(household, date(2026, 7, 4))
    induction = [use for use in long_use.uses if use.appliance == "induction"][0]
    assert induction.end_second - induction.start_second == 10800
    assert len(induction.sessions) == 1


def test_weekday_rule_only_fires_on_listed_days(tmp_path):
    scenario = load_scenario(write_scenario(tmp_path, base_document()))
    household = scenario.household("T001")
    monday, tuesday = date(2026, 6, 29), date(2026, 6, 30)
    assert monday.weekday() == 0 and tuesday.weekday() == 1
    assert plan_day(household, monday).use_count("iron") == 1
    assert plan_day(household, tuesday).use_count("iron") == 0


def test_period_edit_does_not_reshuffle_other_rules(tmp_path):
    document = base_document()
    plain = load_scenario(write_scenario(tmp_path, document))
    document["households"][0]["periods"] = [
        {"name": "inactive-induction", "start": "2026-06-26", "end": "2026-06-26",
         "probability_override": {"induction": 0.0}}
    ]
    edited = load_scenario(write_scenario(tmp_path, document))
    day = date(2026, 6, 26)
    before = plan_day(plain.household("T001"), day)
    after = plan_day(edited.household("T001"), day)
    assert before.use_count("induction") == 1 and after.use_count("induction") == 0
    assert [use for use in before.uses if use.appliance != "induction"] == list(after.uses)


def test_segment_gap_wider_than_merge_gap_is_rejected(tmp_path):
    document = base_document()
    document["households"][0]["schedule"][1]["segment_gap_seconds"] = [20, 300]
    with pytest.raises(ScenarioError, match="merge gap"):
        load_scenario(write_scenario(tmp_path, document))


def test_load_households_expand_deterministically(tmp_path):
    document = base_document(load_households={
        "count": 12, "prefix": "L", "sampling_interval_seconds": 10, "templates": ["T001"],
        "weights": [1], "seed": 7, "inactive_fraction": 0.5, "inactive_days": 2,
    })
    first = load_scenario(write_scenario(tmp_path, document))
    second = load_scenario(write_scenario(tmp_path, document))
    load = first.load_households()
    assert len(load) == 12 and load[0].household_id == "L0001" and load[-1].household_id == "L0012"
    assert all(item.sampling_interval_seconds == 10 and item.template_id == "T001" for item in load)
    assert [item.rules for item in load] == [item.rules for item in second.load_households()]
    assert any(item.periods for item in load)
    for item in load:
        for period in item.periods:
            assert first.start <= period.start <= period.end <= first.end
