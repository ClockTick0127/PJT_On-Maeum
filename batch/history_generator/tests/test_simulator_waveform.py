from datetime import date
import json
from pathlib import Path

import numpy as np
import pytest

from history_generator.scenario import load_scenario
from history_generator.schedule import plan_day
from history_generator.waveform import SimulatorWaveform, WaveformPool, make_generator

SIMULATOR = Path(__file__).resolve().parents[3] / "infrastructure" / "mqtt" / "simulator"

pytestmark = pytest.mark.simulator


@pytest.fixture
def scenario(tmp_path):
    document = {
        "range": {"start": "2026-06-25", "end": "2026-06-25"},
        "households": [{"household_id": "T001", "seed": 5, "schedule": [
            {"appliance": "kettle", "median": "08:50", "probability": 1.0, "duration_seconds": [180, 180]},
            {"appliance": "induction", "median": "19:00", "probability": 1.0, "duration_seconds": [600, 600],
             "segments": [3, 3], "segment_gap_seconds": [30, 30]},
        ]}],
    }
    path = tmp_path / "s.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return load_scenario(path)


def test_engine_waveform_follows_the_plan_and_is_deterministic(scenario):
    if not (SIMULATOR / "engine" / "power_model.py").exists():
        pytest.skip("simulator checkout not available")
    generator = make_generator("simulator", SIMULATOR)
    assert isinstance(generator, SimulatorWaveform)
    plan = plan_day(scenario.household("T001"), date(2026, 6, 25))
    first = generator.generate(plan, 11)
    second = generator.generate(plan, 11)
    assert first.shape == (86_400, 4) and np.array_equal(first, second)
    assert np.all(first[:, 0] >= 0) and np.all((first[:, 2] >= 0) & (first[:, 2] <= 1))
    kettle = [s for s in plan.sessions if s.appliance == "kettle"][0]
    during = first[kettle.start_second + 5:kettle.end_second, 0].mean()
    before = first[kettle.start_second - 300:kettle.start_second - 5, 0].mean()
    after = first[kettle.end_second + 5:kettle.end_second + 300, 0].mean()
    assert during - before > 1400 and during - after > 1400  # kettle median 1,657 W
    induction = [u for u in plan.uses if u.appliance == "induction"][0]
    gap = induction.sessions[0].end_second, induction.sessions[1].start_second
    assert first[gap[0] + 2:gap[1] - 2, 0].mean() < first[induction.sessions[0].start_second + 2:gap[0] - 2, 0].mean()


def test_pool_reuses_generated_days(scenario):
    calls = []

    class Counting:
        name = "counting"

        def generate(self, plan, seed):
            calls.append(seed)
            return np.full((86_400, 4), float(seed))

    pool = WaveformPool(Counting(), size=2)
    plan = plan_day(scenario.household("T001"), date(2026, 6, 25))
    a = pool.waveform(plan, "T001", 0)
    b = pool.waveform(plan, "T001", 2)
    c = pool.waveform(plan, "T001", 1)
    assert np.array_equal(a, b) and not np.array_equal(a, c) and len(calls) == 2
