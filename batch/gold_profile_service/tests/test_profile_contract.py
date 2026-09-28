from datetime import date
from types import SimpleNamespace
from uuid import UUID

import pytest

from gold_profile.catalog import affected_as_of_dates
from gold_profile.cli import build_parser
from gold_profile.input_snapshot import build_profile_snapshot, window_dates
from gold_profile.config import GoldProfileSettings
from gold_profile.job import config_version_of
from gold_profile.routine_baseline import nearest_rank_value


def _ref(dataset, day, seed):
    return SimpleNamespace(
        dataset_name=dataset,
        target_date=day,
        run_id=UUID(int=seed),
        version_id=UUID(int=seed + 100),
        output_path=f"/{dataset}/{day}",
        manifest_path=f"/manifest/{seed}",
        input_snapshot_id=f"snapshot-{seed}",
        rule_version="upstream-rule",
        config_version="upstream-config",
    )


def test_window_is_inclusive_and_exactly_28_days():
    days = window_dates(date(2026, 9, 19), 28)
    assert len(days) == 28
    assert days[0] == date(2026, 8, 23)
    assert days[-1] == date(2026, 9, 19)


def test_snapshot_records_missing_dates_and_is_deterministic():
    days = window_dates(date(2026, 9, 19), 3)
    usage = {days[0]: _ref("usage", days[0], 1), days[2]: _ref("usage", days[2], 2)}
    slices = {day: _ref("slices", day, 10 + index) for index, day in enumerate(days)}
    first = build_profile_snapshot(
        days[-1], 3, usage, slices, rule_version="r1",
        statistic_rule_version="s1", analysis_run_id="a1", timezone_name="UTC+32400s",
    )
    second = build_profile_snapshot(
        days[-1], 3, usage, slices, rule_version="r1",
        statistic_rule_version="s1", analysis_run_id="a1", timezone_name="UTC+32400s",
    )
    assert first.snapshot_id == second.snapshot_id
    assert first.incomplete
    assert first.missing_usage_dates == (days[1],)
    assert first.missing_slice_dates == ()


def test_nearest_rank_has_no_interpolation():
    values = [100, 200, 300, 400]
    assert nearest_rank_value(values, 0.10) == 100
    assert nearest_rank_value(values, 0.50) == 200
    assert nearest_rank_value(values, 0.90) == 400
    assert nearest_rank_value([], 0.90) is None
    with pytest.raises(ValueError):
        nearest_rank_value(values, 0)


def test_changed_day_invalidates_every_containing_rolling_window():
    changed = date(2026, 9, 1)
    affected = affected_as_of_dates(
        changed, window_days=28, through_date=date(2026, 9, 4)
    )
    assert affected == tuple(date(2026, 9, day) for day in range(1, 5))


def test_cli_contract():
    parser = build_parser()
    assert parser.parse_args(["run", "--as-of", "2026-09-19"]).command == "run"
    assert parser.parse_args(["dirty", "--as-of", "2026-09-19"]).command == "dirty"


def test_delivery_mode_is_part_of_gold_run_configuration():
    shadow = GoldProfileSettings(profile_delivery_mode="SHADOW")
    active = shadow.model_copy(update={"profile_delivery_mode": "ACTIVE"})

    assert config_version_of(shadow) != config_version_of(active)
