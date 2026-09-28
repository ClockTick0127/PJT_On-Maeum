"""
H001 전자레인지 사용시간 초과 시나리오(prolonged_use) 단위 테스트

검증 항목:
1. prolonged_use가 server/config.ALLOWED_SCENARIOS에 포함되고 타임라인 상수가 계약과 같은지
2. 기본 시작 시각이 11:55:00 KST이고 cycle별 가상 시각표(12:00 ON, 12:02 2분 경과, 12:05 OFF)가 맞는지
3. 다중 실행에서 normal_routine·routine_missed와 함께 쓰면 ValueError 및 HTTP 400이 반환되는지
4. H001 외 가구, start_time 지정이 API·Manager 양쪽에서 거절되는지
5. cycle 1~300 OFF, cycle 301~600 정확히 300개 ON, cycle 601~630 OFF이며 다른 가전은 켜지지 않는지
6. 전자레인지가 켜진 5분 동안 전력이 끊김 없이 유지되는지
7. Manager 시작 시 SSE 첫 표본이 prolonged_use / 11:55:00 / 전자레인지 상태를 포함하는지
시뮬레이터는 MQTT 원천 데이터만 검증한다. AI 사용시간 초과 판정 결과는 검증하지 않는다.
"""

import io
import json
import os
import queue
import sys
import unittest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SIMULATOR_DIR = os.path.dirname(TESTS_DIR)
if SIMULATOR_DIR not in sys.path:
    sys.path.insert(0, SIMULATOR_DIR)

import scenarios
from scenarios import (
    KST,
    ProlongedUseScenario,
    PROLONGED_USE_SCHEDULE,
    inject_prolonged_use_scenario_event,
    resolve_simulation_start_time,
    resolve_multi_simulation_start_time,
)
import simulator
import server.config as srv_config
from server.manager import SimulatorManager
from server.request_handler import RequestHandler


class DummyRequestHandler(RequestHandler):
    """테스트용 RequestHandler Mock"""
    def __init__(self, manager, method="POST", path="/api/start", body_dict=None):
        self.manager = manager
        self.e2e_manager = None
        self.path = path
        self.command = method
        self.request_version = "HTTP/1.1"
        self.requestline = f"{method} {path} HTTP/1.1"
        self.close_connection = False
        body_bytes = json.dumps(body_dict).encode("utf-8") if body_dict is not None else b""
        self.rfile = io.BytesIO(body_bytes)
        self.wfile = io.BytesIO()
        self.headers = {
            "Content-Length": str(len(body_bytes)),
            "Content-Type": "application/json"
        }

    def get_response_json(self):
        output = self.wfile.getvalue().decode("utf-8").strip()
        if "\r\n\r\n" in output:
            output = output.split("\r\n\r\n", 1)[1]
        return json.loads(output) if output else {}

    def get_status_code(self):
        output = self.wfile.getvalue().decode("utf-8").strip()
        lines = output.split("\r\n")
        if lines and len(lines[0].split()) >= 2:
            return int(lines[0].split()[1])
        return 0


class TestProlongedUseConfig(unittest.TestCase):
    """1. 시나리오 등록 및 상수"""

    def test_registered_in_allowed_scenarios(self):
        self.assertIn("prolonged_use", srv_config.ALLOWED_SCENARIOS)

    def test_timeline_constants(self):
        self.assertEqual(ProlongedUseScenario.TARGET_HOUSE, "H001")
        self.assertEqual(ProlongedUseScenario.APPLIANCE, "microwave")
        self.assertEqual(ProlongedUseScenario.DEFAULT_START_TIME, "11:55:00")
        self.assertEqual(ProlongedUseScenario.APPLIANCE_ON_CYCLE, 301)
        self.assertEqual(ProlongedUseScenario.ALLOWED_DURATION_SEC, 120)
        self.assertEqual(ProlongedUseScenario.ALLOWED_DURATION_ELAPSED_CYCLE, 421)
        self.assertEqual(ProlongedUseScenario.APPLIANCE_OFF_CYCLE, 601)
        self.assertEqual(ProlongedUseScenario.APPLIANCE_DURATION_SEC, 300)
        self.assertEqual(ProlongedUseScenario.TOTAL_CYCLES, 630)
        self.assertEqual(scenarios.get_scenario_target_cycles("prolonged_use"), 630)

    def test_schedule_events(self):
        self.assertEqual(PROLONGED_USE_SCHEDULE[301]["event"], "MICROWAVE_START")
        self.assertEqual(PROLONGED_USE_SCHEDULE[421]["event"], "ALLOWED_DURATION_ELAPSED")
        self.assertEqual(PROLONGED_USE_SCHEDULE[601]["event"], "MICROWAVE_STOP")
        self.assertEqual(PROLONGED_USE_SCHEDULE[630]["event"], "PROLONGED_USE_COMPLETE")
        self.assertTrue(PROLONGED_USE_SCHEDULE[301]["actions"]["microwave"]["manual_hold"])


class TestProlongedUseStartTime(unittest.TestCase):
    """2. 가상 시작 시각과 cycle별 시각표"""

    def setUp(self):
        self.fixed_now = datetime(2026, 9, 24, 9, 0, 0, tzinfo=KST)

    def test_default_start_time_today(self):
        dt = resolve_simulation_start_time("prolonged_use", now=self.fixed_now)
        self.assertEqual((dt.year, dt.month, dt.day), (2026, 9, 24))
        self.assertEqual(dt.strftime("%H:%M:%S"), "11:55:00")
        self.assertEqual(dt.tzinfo, KST)

    def test_date_only_uses_default_time(self):
        dt = resolve_simulation_start_time("prolonged_use", simulation_date="2026-10-01", now=self.fixed_now)
        self.assertEqual((dt.year, dt.month, dt.day), (2026, 10, 1))
        self.assertEqual(dt.strftime("%H:%M:%S"), "11:55:00")

    def test_cycle_timetable(self):
        base_dt = resolve_simulation_start_time("prolonged_use", simulation_date="2026-09-24", now=self.fixed_now)

        def time_for(cycle):
            return (base_dt + timedelta(seconds=cycle - 1)).strftime("%H:%M:%S")

        self.assertEqual(time_for(1), "11:55:00")
        self.assertEqual(time_for(300), "11:59:59")
        self.assertEqual(time_for(ProlongedUseScenario.APPLIANCE_ON_CYCLE), ProlongedUseScenario.APPLIANCE_ON_TIME)
        self.assertEqual(time_for(ProlongedUseScenario.ALLOWED_DURATION_ELAPSED_CYCLE), ProlongedUseScenario.ALLOWED_DURATION_ELAPSED_TIME)
        self.assertEqual(time_for(ProlongedUseScenario.APPLIANCE_OFF_CYCLE), ProlongedUseScenario.APPLIANCE_OFF_TIME)
        self.assertEqual(time_for(ProlongedUseScenario.TOTAL_CYCLES), "12:05:29")

    def test_multi_run_with_random_houses_uses_scenario_start(self):
        households = [
            {"house": "H001", "scenario": "prolonged_use"},
            {"house": "H003", "scenario": "random"},
        ]
        dt = resolve_multi_simulation_start_time(households, simulation_date="2026-09-24", now=self.fixed_now)
        self.assertEqual(dt.strftime("%H:%M:%S"), "11:55:00")


class TestProlongedUseConflictsAndRestrictions(unittest.TestCase):
    """3 & 4. 고정 타임라인 충돌, H001 전용, start_time 거절"""

    def setUp(self):
        self.manager = SimulatorManager(host="localhost", port=1883, tls_enabled=False)

    def tearDown(self):
        self.manager.stop()
        self.manager.reset()

    def _post(self, body):
        handler = DummyRequestHandler(self.manager, body_dict=body)
        handler.do_POST()
        return handler.get_status_code(), handler.get_response_json()

    def test_multi_conflict_with_other_fixed_scenarios(self):
        for other_house, other in (("H002", "routine_missed"),):
            households = [{"house": "H001", "scenario": "prolonged_use"}, {"house": other_house, "scenario": other}]
            with self.subTest(other=other):
                with self.assertRaises(ValueError):
                    resolve_multi_simulation_start_time(households)
                status, resp = self._post({"households": households})
                self.assertEqual(status, 400)
                self.assertIn("prolonged_use는 normal_routine·routine_missed와", resp.get("message", ""))
                with self.assertRaises(ValueError):
                    self.manager.start(households=households)

    def test_h002_rejected_single_and_multi(self):
        message = "prolonged_use 시나리오는 H001 가구에서만 실행할 수 있습니다."
        status, resp = self._post({"house": "H002", "scenario": "prolonged_use"})
        self.assertEqual((status, resp.get("message")), (400, message))
        status, resp = self._post({"households": [{"house": "H002", "scenario": "prolonged_use"}]})
        self.assertEqual((status, resp.get("message")), (400, message))
        with self.assertRaises(ValueError) as ctx:
            self.manager.start(house="H002", scenario="prolonged_use")
        self.assertEqual(str(ctx.exception), message)

    def test_start_time_rejected(self):
        with self.assertRaises(ValueError) as ctx:
            self.manager.start(house="H001", scenario="prolonged_use", start_time="10:00:00")
        self.assertIn("prolonged_use", str(ctx.exception))
        status, _ = self._post({"house": "H001", "scenario": "prolonged_use", "start_time": "10:00:00"})
        self.assertEqual(status, 400)


class TestProlongedUsePhysicsTimeline(unittest.TestCase):
    """5 & 6. 물리 엔진 630 tick 타임라인"""

    def _run_timeline(self, seed):
        simulator.init_simulation_states(["H001"], seed=seed)
        rows = []
        for cycle in range(1, ProlongedUseScenario.TOTAL_CYCLES + 1):
            simulator.inject_prolonged_use_scenario_event(cycle, "H001")
            metrics = simulator.calculate_main_panel_metrics("H001", allow_random=False)
            states = {name: dev["state"] for name, dev in simulator.device_states["H001"].items()}
            rows.append((cycle, metrics["active_power"], states))
        return rows

    def test_exact_300_microwave_on_samples(self):
        rows = self._run_timeline(seed=7)
        on_cycles = [cycle for cycle, _, states in rows if states["microwave"] != "OFF"]
        self.assertEqual(on_cycles, list(range(301, 601)), "전자레인지 ON은 cycle 301~600, 정확히 300개여야 합니다.")
        for cycle, _, states in rows:
            for device, state in states.items():
                if device != "microwave":
                    self.assertEqual(state, "OFF", f"cycle {cycle}에서 {device}는 OFF여야 합니다.")

    def test_power_is_continuous_while_microwave_is_on(self):
        rows = self._run_timeline(seed=11)
        before = [p for cycle, p, _ in rows if cycle <= 300]
        during = [p for cycle, p, _ in rows if 301 <= cycle <= 600]
        after = [p for cycle, p, _ in rows if cycle >= 601]
        self.assertLess(max(before), 300.0, "데우기 전에는 대기전력만 흘러야 합니다.")
        self.assertLess(max(after), 300.0, "끈 뒤에는 대기전력으로 돌아와야 합니다.")
        self.assertGreater(min(during), 850.0, "켜진 5분 동안 전력(약 940W)이 한 번도 끊기지 않아야 합니다.")
        self.assertLess(max(during), 1600.0)
        self.assertGreater(max(during[:2]), 1150.0, "켤 때 변압기 돌입 전류 피크가 나타나야 합니다.")

    def test_event_injection_is_noop_for_other_house(self):
        simulator.init_simulation_states(["H001", "H002"])
        desc = inject_prolonged_use_scenario_event(301, "H009", simulator.device_states)
        self.assertIsNotNone(desc)
        self.assertEqual(simulator.device_states["H001"]["microwave"]["state"], "OFF")
        self.assertEqual(simulator.device_states["H002"]["microwave"]["state"], "OFF")


class TestProlongedUseManagerAndSSE(unittest.TestCase):
    """7. Manager 시작 응답과 SSE 첫 표본"""

    def setUp(self):
        self.manager = SimulatorManager(host="localhost", port=1883, tls_enabled=False)

    def tearDown(self):
        self.manager.stop()
        self.manager.reset()

    def test_start_and_first_sse_sample(self):
        with patch("aiomqtt.Client"), patch("simulator.publish_house_power", new_callable=AsyncMock):
            q = queue.Queue(maxsize=100)
            self.manager.add_subscriber(q)
            res = self.manager.start(house="H001", scenario="prolonged_use", simulation_date="2026-09-24")
            self.assertEqual(res.get("status"), "started")
            # 11:55:00 KST = 02:55:00 UTC
            self.assertEqual(self.manager.resolved_start_time, "2026-09-24T02:55:00.000Z")

            event = q.get(timeout=3.0)
            self.assertEqual(event["house"], "H001")
            self.assertEqual(event["scenario"], "prolonged_use")
            self.assertEqual(event["sec"], 1)
            self.assertEqual(event["simTimeKst"], "11:55:00")
            self.assertEqual(event["simDateKst"], "2026-09-24")
            self.assertIn("microwave", event["devices"])
            self.assertFalse(event["devices"]["microwave"]["enabled"])
            self.manager.remove_subscriber(q)


if __name__ == "__main__":
    unittest.main()
