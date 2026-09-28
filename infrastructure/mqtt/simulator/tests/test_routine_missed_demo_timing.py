"""시연용 미사용 발행 구간이 평소 기록 그래프와 일치하는지 검증한다."""

import os
import sys
import unittest
from datetime import datetime, timedelta


SIMULATOR_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SIMULATOR_DIR not in sys.path:
    sys.path.insert(0, SIMULATOR_DIR)

import scenarios


class RoutineMissedDemoTimingTest(unittest.TestCase):
    def test_published_window_matches_replay_window(self):
        definition = scenarios.RoutineMissedDemoScenario
        start = datetime(2026, 9, 26, 12, 34, 56, tzinfo=scenarios.KST)
        end = start + timedelta(seconds=definition.TOTAL_CYCLES - 1)

        self.assertEqual(scenarios.get_scenario_target_cycles("routine_missed_demo"), 330)
        self.assertEqual(end.strftime("%H:%M:%S"), "12:40:25")
        self.assertEqual((start + timedelta(seconds=30)).strftime("%H:%M:%S"), "12:35:26")
        self.assertEqual((start + timedelta(seconds=90)).strftime("%H:%M:%S"), "12:36:26")
        self.assertFalse(hasattr(definition, "DEFAULT_START_TIME"))

    def test_default_start_uses_first_measurement_clock(self):
        now = datetime(2026, 9, 26, 12, 34, 56, tzinfo=scenarios.KST)
        self.assertIsNone(scenarios.resolve_simulation_start_time("manual", now=now))
        selected_date = scenarios.resolve_simulation_start_time(
            "manual", simulation_date="2026-09-25", now=now
        )
        self.assertEqual(selected_date.isoformat(), "2026-09-25T12:34:56+09:00")


if __name__ == "__main__":
    unittest.main()
