"""
실시간 경로 seed 재현 검증

같은 seed + 같은 가구/시나리오로 시작하면 틱별 계측값이 동일하게 재현되는지,
seed 미지정 경로는 기존 전역 random 동작(seed42 fixture 호환)을 유지하는지 확인한다.
MQTT 없이 init_simulation_states + calculate_main_panel_metrics를 직접 호출한다.
"""

import io
import json
import os
import queue
import random
import sys
import time
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SIMULATOR_DIR = os.path.dirname(TESTS_DIR)
if SIMULATOR_DIR not in sys.path:
    sys.path.insert(0, SIMULATOR_DIR)

import scenarios
import simulator
from engine import state as engine_state
from server.manager import SimulatorManager
from server.request_handler import RequestHandler

TICKS = 1800


def run_ticks(houses, seed, ticks=TICKS, allow_random=True):
    """주어진 가구 구성으로 초기화 후 ticks만큼 계측값을 수집한다. {house: [metrics, ...]}"""
    simulator.init_simulation_states(houses, seed=seed)
    result = {house: [] for house in houses}
    for _ in range(ticks):
        for house in houses:
            result[house].append(simulator.calculate_main_panel_metrics(house, allow_random=allow_random))
    return result


class TestSeededMetricsReplay(unittest.TestCase):
    def tearDown(self):
        # 다른 테스트에 가구별 rng가 남지 않도록 seed 미지정 상태로 되돌린다.
        simulator.init_simulation_states(simulator.DEFAULT_HOUSES)

    def test_a_same_seed_same_sequence(self):
        first = run_ticks(["H001"], seed=12345)
        second = run_ticks(["H001"], seed=12345)
        self.assertEqual(first["H001"], second["H001"])

    def test_b_global_random_pollution_does_not_affect_replay(self):
        random.seed(1)
        [random.random() for _ in range(37)]
        first = run_ticks(["H001"], seed=12345)
        random.seed(999)
        [random.random() for _ in range(5)]
        second = run_ticks(["H001"], seed=12345)
        self.assertEqual(first["H001"], second["H001"])

    def test_c_different_seed_differs(self):
        a = run_ticks(["H001"], seed=1)
        b = run_ticks(["H001"], seed=2)
        self.assertNotEqual(a["H001"], b["H001"])

    def test_d_household_independence(self):
        alone = run_ticks(["H002"], seed=777)
        together = run_ticks(["H001", "H002"], seed=777)
        self.assertEqual(alone["H002"], together["H002"])
        # 가구마다 다른 난수열을 쓰는지도 확인
        self.assertNotEqual(together["H001"], together["H002"])

    def test_e_random_mode_still_turns_devices_on(self):
        seq = run_ticks(["H001"], seed=12345)["H001"]
        self.assertTrue(any(m["active_devices"] for m in seq), "1800틱 동안 가전이 한 번도 켜지지 않음")

    def test_f_seed_none_uses_global_random(self):
        simulator.init_simulation_states(["H001"])
        self.assertEqual(engine_state.house_rngs, {})
        self.assertIs(engine_state.get_house_rng("H001"), random)

        random.seed(42)
        a = run_ticks(["H001"], seed=None, ticks=200)
        random.seed(42)
        b = run_ticks(["H001"], seed=None, ticks=200)
        self.assertEqual(a["H001"], b["H001"])

    def test_seeded_init_clears_previous_rngs(self):
        simulator.init_simulation_states(["H001", "H002"], seed=5)
        self.assertEqual(set(engine_state.house_rngs), {"H001", "H002"})
        simulator.init_simulation_states(["H003"])
        self.assertEqual(engine_state.house_rngs, {})

    def test_resolve_seed_validation(self):
        self.assertEqual(simulator.resolve_seed(0), 0)
        self.assertEqual(simulator.resolve_seed(2**31 - 1), 2**31 - 1)
        generated = simulator.resolve_seed(None)
        self.assertIs(type(generated), int)
        self.assertTrue(0 <= generated <= 2**31 - 1)
        for bad in (True, False, "1", 1.0, -1, 2**31, [1]):
            with self.assertRaises(ValueError, msg=repr(bad)):
                simulator.resolve_seed(bad)


class TestManagerSeed(unittest.TestCase):
    def setUp(self):
        self.manager = SimulatorManager()

    def tearDown(self):
        if self.manager.is_running:
            self.manager.stop()
        simulator.init_simulation_states(simulator.DEFAULT_HOUSES)

    def test_g_start_without_seed_returns_generated_seed(self):
        with patch("server.manager.threading.Thread") as thread_cls:
            thread_cls.return_value.is_alive.return_value = False
            res = self.manager.start(households=[{"house": "H001", "scenario": "random"}])
        self.assertIs(type(res["seed"]), int)
        self.assertEqual(self.manager.get_status()["seed"], res["seed"])
        self.assertIn("H001", engine_state.house_rngs)

    def test_g_start_with_seed_echoes_it_and_reset_clears(self):
        with patch("server.manager.threading.Thread") as thread_cls:
            thread_cls.return_value.is_alive.return_value = False
            res = self.manager.start(households=[{"house": "H001", "scenario": "random"}], seed=4242)
        self.assertEqual(res["seed"], 4242)
        self.assertEqual(self.manager.get_status()["seed"], 4242)
        self.manager.reset()
        self.assertIsNone(self.manager.get_status()["seed"])

    def test_g_start_rejects_invalid_seed(self):
        for bad in (True, "1", -1, 2**31):
            with self.assertRaises(ValueError, msg=repr(bad)):
                self.manager.start(households=[{"house": "H001", "scenario": "random"}], seed=bad)
        self.assertFalse(self.manager.is_running)

    def test_start_time_fixes_resolved_start(self):
        with patch("server.manager.threading.Thread") as thread_cls:
            thread_cls.return_value.is_alive.return_value = False
            res = self.manager.start(
                households=[{"house": "H001", "scenario": "random"}],
                simulation_date="2026-09-10",
                start_time="09:00:00",
                seed=1,
            )
        self.assertEqual(res["resolved_start_time"], scenarios.format_iso_utc(
            scenarios.resolve_fixed_start_time("09:00:00", simulation_date="2026-09-10")))
        self.assertTrue(res["resolved_start_time"].startswith("2026-09-10T00:00:00"))

    def test_start_time_rejected_for_routine_scenarios(self):
        for sc in ("normal_routine", "routine_missed"):
            with self.assertRaises(ValueError, msg=sc):
                self.manager.start(households=[{"house": "H001", "scenario": sc}], start_time="09:00")
        self.assertFalse(self.manager.is_running)

    def _collect_totals(self, seed, ticks, start_time=None):
        q = queue.Queue()
        self.manager.add_subscriber(q)
        try:
            with patch("aiomqtt.Client"), patch("simulator.publish_house_power", new_callable=AsyncMock):
                self.manager.start(
                    households=[{"house": "H001", "scenario": "random"}, {"house": "H002", "scenario": "random"}],
                    interval=0.1,
                    seed=seed,
                    simulation_date="2026-09-10" if start_time else None,
                    start_time=start_time,
                )
                values = []
                deadline = time.monotonic() + 30
                while len(values) < ticks * 2 and time.monotonic() < deadline:
                    try:
                        item = q.get(timeout=1)
                    except queue.Empty:
                        continue
                    if "totalP" in item and item.get("house") in ("H001", "H002"):
                        values.append((item["house"], item["sec"], item["totalP"], item["totalQ"],
                                       item["voltage"], item["currentA"], tuple(item["activeNames"]),
                                       item["now_iso"] if start_time else None))
                self.manager.stop()
        finally:
            self.manager.remove_subscriber(q)
        return sorted(values[: ticks * 2])

    def test_manager_worker_replays_same_values(self):
        first = self._collect_totals(seed=2024, ticks=15)
        second = self._collect_totals(seed=2024, ticks=15)
        self.assertEqual(len(first), 30)
        self.assertEqual(first, second)

    def test_manager_worker_replays_same_values_and_timestamps_with_start_time(self):
        first = self._collect_totals(seed=2024, ticks=10, start_time="09:00:00")
        second = self._collect_totals(seed=2024, ticks=10, start_time="09:00:00")
        self.assertEqual(len(first), 20)
        self.assertEqual(first, second)
        h001_times = [row[7] for row in first if row[0] == "H001"]
        self.assertTrue(h001_times[0].startswith("2026-09-10T00:00:00"))
        self.assertTrue(h001_times[1].startswith("2026-09-10T00:00:01"))


class TestResolveFixedStartTime(unittest.TestCase):
    def test_valid_formats(self):
        dt = scenarios.resolve_fixed_start_time("09:00:05", simulation_date="2026-09-10")
        self.assertEqual((dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second), (2026, 9, 10, 9, 0, 5))
        self.assertEqual(dt.utcoffset().total_seconds(), 9 * 3600)
        dt2 = scenarios.resolve_fixed_start_time("09:00", simulation_date="2026-09-10")
        self.assertEqual(dt2.second, 0)

    def test_without_date_uses_today_kst(self):
        from datetime import datetime
        fixed_now = datetime(2026, 9, 23, 23, 30, tzinfo=scenarios.KST)
        dt = scenarios.resolve_fixed_start_time("08:00:00", now=fixed_now)
        self.assertEqual(dt.date().isoformat(), "2026-09-23")

    def test_invalid(self):
        for bad in ("9:00", "25:00:00", "09:60", 900, None, "2026-09-10T09:00:00", "09:00:00:00", ""):
            with self.assertRaises(ValueError, msg=repr(bad)):
                scenarios.resolve_fixed_start_time(bad, simulation_date="2026-09-10")


class TestApiStartSeed(unittest.TestCase):
    def _make_handler(self, body):
        handler = RequestHandler.__new__(RequestHandler)
        handler.command = "POST"
        handler.path = "/api/start"
        body_bytes = json.dumps(body).encode("utf-8")
        handler.headers = {"Content-Length": str(len(body_bytes))}
        handler.rfile = io.BytesIO(body_bytes)
        handler.wfile = io.BytesIO()
        handler.manager = MagicMock()
        handler.manager.start.return_value = {
            "status": "started",
            "scenario": "random",
            "house": "H001",
            "households": [{"house": "H001", "scenario": "random"}],
            "simulation_date": None,
            "resolved_start_time": None,
            "seed": body.get("seed") if isinstance(body.get("seed"), int) else 99,
        }
        handler.e2e_manager = None
        handler.send_response = MagicMock()
        handler.send_header = MagicMock()
        handler.end_headers = MagicMock()
        return handler

    def _body(self, handler):
        return json.loads(handler.wfile.getvalue().decode("utf-8"))

    def test_seed_passed_through_and_returned(self):
        handler = self._make_handler({"households": [{"house": "H001", "scenario": "random"}], "seed": 123})
        handler.do_POST()
        handler.send_response.assert_called_with(200)
        self.assertEqual(handler.manager.start.call_args.kwargs["seed"], 123)
        self.assertEqual(self._body(handler)["seed"], 123)

    def test_seed_omitted_not_passed(self):
        handler = self._make_handler({"households": [{"house": "H001", "scenario": "random"}]})
        handler.do_POST()
        handler.send_response.assert_called_with(200)
        self.assertNotIn("seed", handler.manager.start.call_args.kwargs)
        self.assertEqual(self._body(handler)["seed"], 99)

    def test_start_time_passed_through(self):
        handler = self._make_handler({"households": [{"house": "H001", "scenario": "random"}],
                                      "simulation_date": "2026-09-10", "start_time": "09:00:00", "seed": 5})
        handler.do_POST()
        handler.send_response.assert_called_with(200)
        self.assertEqual(handler.manager.start.call_args.kwargs["start_time"], "09:00:00")

    def test_invalid_start_time_rejected_400(self):
        for bad in ("9:00", "24:00", 900, "2026-09-10T09:00"):
            handler = self._make_handler({"households": [{"house": "H001", "scenario": "random"}], "start_time": bad})
            handler.do_POST()
            handler.send_response.assert_called_with(400)
            handler.manager.start.assert_not_called()

    def test_invalid_seed_rejected_400(self):
        for bad in (True, "1", -1, 2**31, 1.5):
            handler = self._make_handler({"households": [{"house": "H001", "scenario": "random"}], "seed": bad})
            handler.do_POST()
            handler.send_response.assert_called_with(400)
            handler.manager.start.assert_not_called()


if __name__ == "__main__":
    unittest.main()
