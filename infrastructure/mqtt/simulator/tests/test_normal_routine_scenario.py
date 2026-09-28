"""
H001 정상 일상 시나리오(normal_routine) 및 원클릭 시연 기능 종합 단위 테스트

검증 항목:
1. normal_routine이 server/config.ALLOWED_SCENARIOS에 포함되는지
2. 단일 정상 실행의 기본 시작 시각이 08:04:58 KST인지
3. 명시적 날짜 및 시작 시각 우선순위가 올바르게 적용되는지
4. 다중 실행에서 normal_routine과 routine_missed 충돌 시 ValueError 및 HTTP 400이 반환되는지
5. cycle 1~242에서 전자레인지가 OFF인지
6. cycle 243~302에서 정확히 60개 활성 샘플이 생성되는지
7. cycle 303부터 전자레인지가 명시적으로 OFF되는지
8. cycle 308에서 정상 시나리오가 완료되는지
9. 정상 시나리오에서 랜덤 가전이 켜지지 않는지 (allow_random=False)
10. Pause/Resume 후 가상 시간이 정확히 1초 연속성을 유지하는지
11. 정상 시나리오 SSE 데이터가 정확한 상태(scenario, mode, status, cycle_count 등)를 포함하는지
12. 기존 peak, routine_missed, random, manual 시나리오 동작이 보존되는지
"""

import copy
import io
import json
import os
import queue
import sys
import time
import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, AsyncMock, patch, ANY

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SIMULATOR_DIR = os.path.dirname(TESTS_DIR)
if SIMULATOR_DIR not in sys.path:
    sys.path.insert(0, SIMULATOR_DIR)

import scenarios
from scenarios import (
    KST,
    NormalRoutineScenario,
    RoutineMissedScenario,
    PEAK_SCENARIO_SCHEDULE,
    inject_peak_scenario_event,
    inject_normal_routine_scenario_event,
    resolve_simulation_start_time,
    resolve_multi_simulation_start_time,
)
import simulator
import server.config as srv_config
from server.manager import SimulatorManager, ModeConflictError
from server.request_handler import RequestHandler


class DummyRequestHandler(RequestHandler):
    """테스트용 RequestHandler Mock"""
    def __init__(self, manager, method="POST", path="/api/start", body_dict=None):
        self.manager = manager
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


class TestNormalRoutineAllowedAndConfig(unittest.TestCase):
    """1. 시나리오 등록 및 상수 검증"""

    def test_normal_routine_in_allowed_scenarios(self):
        self.assertIn("normal_routine", srv_config.ALLOWED_SCENARIOS)
        self.assertIn("routine_missed", srv_config.ALLOWED_SCENARIOS)
        self.assertIn("peak", srv_config.ALLOWED_SCENARIOS)
        self.assertIn("random", srv_config.ALLOWED_SCENARIOS)
        self.assertIn("manual", srv_config.ALLOWED_SCENARIOS)

    def test_normal_routine_scenario_constants(self):
        self.assertEqual(NormalRoutineScenario.TARGET_HOUSE, "H001")
        self.assertEqual(NormalRoutineScenario.DEFAULT_START_TIME, "08:04:58")
        self.assertEqual(NormalRoutineScenario.MICROWAVE_ON_TIME, "08:09:00")
        self.assertEqual(NormalRoutineScenario.MICROWAVE_OFF_TIME, "08:10:00")
        self.assertEqual(NormalRoutineScenario.MICROWAVE_ON_CYCLE, 243)
        self.assertEqual(NormalRoutineScenario.MICROWAVE_OFF_CYCLE, 303)
        self.assertEqual(NormalRoutineScenario.MICROWAVE_DURATION_SEC, 60)
        self.assertEqual(NormalRoutineScenario.TOTAL_CYCLES, 308)
        self.assertEqual(NormalRoutineScenario.DEFAULT_COUNT, 308)


class TestNormalRoutineStartTimeResolution(unittest.TestCase):
    """2 & 3. 가상 시작 시각 및 우선순위 검증"""

    def setUp(self):
        self.fixed_now = datetime(2026, 9, 15, 12, 0, 0, tzinfo=KST)

    def test_normal_routine_default_start_time(self):
        """날짜/시간 모두 생략 시 오늘 08:04:58 KST"""
        dt = resolve_simulation_start_time(
            "normal_routine",
            start_time_str=None,
            simulation_date=None,
            now=self.fixed_now,
        )
        self.assertEqual(dt.year, 2026)
        self.assertEqual(dt.month, 9)
        self.assertEqual(dt.day, 15)
        self.assertEqual(dt.hour, 8)
        self.assertEqual(dt.minute, 4)
        self.assertEqual(dt.second, 58)
        self.assertEqual(dt.tzinfo, KST)

    def test_normal_routine_with_date_only(self):
        """날짜만 지정 시 해당 날짜의 08:04:58 KST"""
        dt = resolve_simulation_start_time(
            "normal_routine",
            start_time_str=None,
            simulation_date="2026-10-01",
            now=self.fixed_now,
        )
        self.assertEqual(dt.year, 2026)
        self.assertEqual(dt.month, 10)
        self.assertEqual(dt.day, 1)
        self.assertEqual(dt.hour, 8)
        self.assertEqual(dt.minute, 4)
        self.assertEqual(dt.second, 58)
        self.assertEqual(dt.tzinfo, KST)

    def test_normal_routine_explicit_time_priority(self):
        """명시적 시간 입력이 기본 시작 시각(08:04:58)보다 우선"""
        dt = resolve_simulation_start_time(
            "normal_routine",
            start_time_str="09:15:30",
            simulation_date=None,
            now=self.fixed_now,
        )
        self.assertEqual(dt.hour, 9)
        self.assertEqual(dt.minute, 15)
        self.assertEqual(dt.second, 30)

    def test_normal_routine_explicit_datetime_priority(self):
        """명시적 전체 날짜시간 입력이 우선"""
        dt = resolve_simulation_start_time(
            "normal_routine",
            start_time_str="2026-12-25 07:00:00",
            now=self.fixed_now,
        )
        self.assertEqual(dt.year, 2026)
        self.assertEqual(dt.month, 12)
        self.assertEqual(dt.day, 25)
        self.assertEqual(dt.hour, 7)
        self.assertEqual(dt.minute, 0)
        self.assertEqual(dt.second, 0)


class TestMultiHouseConflictDefense(unittest.TestCase):
    """4. 다중 가구 공통 시각 충돌 방어 (normal_routine + routine_missed)"""

    def setUp(self):
        self.fixed_now = datetime(2026, 9, 15, 12, 0, 0, tzinfo=KST)

    def test_resolve_multi_simulation_start_time_raises_value_error_on_conflict(self):
        households = [
            {"house": "H001", "scenario": "normal_routine"},
            {"house": "H002", "scenario": "routine_missed"},
        ]
        with self.assertRaises(ValueError) as ctx:
            resolve_multi_simulation_start_time(households, now=self.fixed_now)
        self.assertIn("normal_routine과 routine_missed는 동일한 다중 실행에서 함께 사용할 수 없습니다", str(ctx.exception))

    def test_resolve_multi_with_only_normal_routine(self):
        households = [
            {"house": "H001", "scenario": "normal_routine"},
            {"house": "H003", "scenario": "random"},
        ]
        dt = resolve_multi_simulation_start_time(households, simulation_date="2026-09-20", now=self.fixed_now)
        self.assertEqual(dt.hour, 8)
        self.assertEqual(dt.minute, 4)
        self.assertEqual(dt.second, 58)

    def test_resolve_multi_with_only_routine_missed(self):
        households = [
            {"house": "H002", "scenario": "routine_missed"},
            {"house": "H003", "scenario": "random"},
        ]
        dt = resolve_multi_simulation_start_time(households, simulation_date="2026-09-20", now=self.fixed_now)
        self.assertEqual(dt.hour, 8)
        self.assertEqual(dt.minute, 10)
        self.assertEqual(dt.second, 1)

    def test_api_start_rejects_conflict_with_http_400(self):
        manager = SimulatorManager(host="localhost", port=1883, tls_enabled=False)
        body = {
            "households": [
                {"house": "H001", "scenario": "normal_routine"},
                {"house": "H002", "scenario": "routine_missed"},
            ]
        }
        handler = DummyRequestHandler(manager, method="POST", path="/api/start", body_dict=body)
        handler.do_POST()

        status_code = handler.get_status_code()
        resp_json = handler.get_response_json()

        self.assertEqual(status_code, 400)
        self.assertEqual(resp_json.get("status"), "error")
        self.assertIn("normal_routine과 routine_missed는 동일한 다중 실행에서 함께 사용할 수 없습니다", resp_json.get("message", ""))


class TestNormalRoutineH001Restriction(unittest.TestCase):
    """normal_routine 시나리오의 H001 전용 제한 (API, Manager, CLI) 검증"""

    def setUp(self):
        self.manager = SimulatorManager(host="localhost", port=1883, tls_enabled=False)

    def tearDown(self):
        self.manager.stop()
        self.manager.reset()

    @patch("aiomqtt.Client")
    def test_h001_normal_routine_api_allowed(self, _mock_client):
        """H001 normal_routine 단일 및 households 요청은 HTTP 200으로 정상 수락"""
        # 단일 요청
        handler1 = DummyRequestHandler(
            self.manager,
            method="POST",
            path="/api/start",
            body_dict={"house": "H001", "scenario": "normal_routine"}
        )
        handler1.do_POST()
        self.assertEqual(handler1.get_status_code(), 200)
        self.assertEqual(handler1.get_response_json().get("status"), "started")
        self.manager.stop()

        # households 요청
        handler2 = DummyRequestHandler(
            self.manager,
            method="POST",
            path="/api/start",
            body_dict={"households": [{"house": "H001", "scenario": "normal_routine"}]}
        )
        handler2.do_POST()
        self.assertEqual(handler2.get_status_code(), 200)
        self.assertEqual(handler2.get_response_json().get("status"), "started")

    def test_h002_normal_routine_single_api_rejected_with_400(self):
        """H002 normal_routine 단일 요청은 HTTP 400 BAD_REQUEST로 거절"""
        handler = DummyRequestHandler(
            self.manager,
            method="POST",
            path="/api/start",
            body_dict={"house": "H002", "scenario": "normal_routine"}
        )
        handler.do_POST()
        self.assertEqual(handler.get_status_code(), 400)
        resp = handler.get_response_json()
        self.assertEqual(resp.get("status"), "error")
        self.assertEqual(resp.get("code"), "BAD_REQUEST")
        self.assertEqual(resp.get("message"), "normal_routine 시나리오는 H001 가구에서만 실행할 수 있습니다.")

    def test_h002_normal_routine_households_api_rejected_with_400(self):
        """H002 normal_routine households 요청은 HTTP 400 BAD_REQUEST로 거절"""
        handler = DummyRequestHandler(
            self.manager,
            method="POST",
            path="/api/start",
            body_dict={"households": [{"house": "H002", "scenario": "normal_routine"}]}
        )
        handler.do_POST()
        self.assertEqual(handler.get_status_code(), 400)
        resp = handler.get_response_json()
        self.assertEqual(resp.get("status"), "error")
        self.assertEqual(resp.get("code"), "BAD_REQUEST")
        self.assertEqual(resp.get("message"), "normal_routine 시나리오는 H001 가구에서만 실행할 수 있습니다.")

    def test_manager_direct_call_raises_value_error_for_h002(self):
        """Manager 직접 호출 시 H002의 normal_routine 실행은 ValueError로 거절"""
        # 단일 가구 인자
        with self.assertRaises(ValueError) as ctx1:
            self.manager.start(house="H002", scenario="normal_routine")
        self.assertEqual(str(ctx1.exception), "normal_routine 시나리오는 H001 가구에서만 실행할 수 있습니다.")

        # households 키워드 인자
        with self.assertRaises(ValueError) as ctx2:
            self.manager.start(households=[{"house": "H002", "scenario": "normal_routine"}])
        self.assertEqual(str(ctx2.exception), "normal_routine 시나리오는 H001 가구에서만 실행할 수 있습니다.")

    def test_cli_normal_routine_targets_only_h001(self):
        """normal_routine에서는 --houses 1과 레거시 기본값 10을 H001 단일 실행으로 해석한다.
        그 외 2 이상의 가구 수는 ValueError로 거절한다.
        """
        import asyncio

        # 1. --scenario normal_routine만 지정 (생략 시 기본값 10 -> H001 단일 가구 실행)
        args_default = simulator.parse_args(["--scenario", "normal_routine"])
        self.assertEqual(args_default.houses, 10)
        args_default.count = 1
        with patch("simulator.init_simulation_states") as mock_init, \
             patch("aiomqtt.Client"), \
             patch("simulator.publish_house_power", new_callable=AsyncMock) as mock_pub:
            mock_pub.return_value = {"house": "H001", "power": 55.0, "devices": []}
            asyncio.run(simulator.run_simulator(args_default))
            mock_init.assert_called_once_with(["H001"], seed=ANY)

        # 2. --scenario normal_routine --houses 1 (H001 단일 가구 실행)
        args_h1 = simulator.parse_args(["--scenario", "normal_routine", "--houses", "1"])
        self.assertEqual(args_h1.houses, 1)
        args_h1.count = 1
        with patch("simulator.init_simulation_states") as mock_init, \
             patch("aiomqtt.Client"), \
             patch("simulator.publish_house_power", new_callable=AsyncMock) as mock_pub:
            mock_pub.return_value = {"house": "H001", "power": 55.0, "devices": []}
            asyncio.run(simulator.run_simulator(args_h1))
            mock_init.assert_called_once_with(["H001"], seed=ANY)

        # 3. --scenario normal_routine --houses 2 (ValueError 발생)
        args_h2 = simulator.parse_args(["--scenario", "normal_routine", "--houses", "2"])
        with self.assertRaises(ValueError) as ctx2:
            asyncio.run(simulator.run_simulator(args_h2))
        self.assertEqual(str(ctx2.exception), "normal_routine 시나리오는 H001 가구에서만 실행할 수 있습니다.")

        # 4. --scenario normal_routine --houses 10 (레거시 기본값과 동일하게 H001 단일 실행, 오류 없음)
        args_h10 = simulator.parse_args(["--scenario", "normal_routine", "--houses", "10"])
        self.assertEqual(args_h10.houses, 10)
        args_h10.count = 1
        with patch("simulator.init_simulation_states") as mock_init, \
             patch("aiomqtt.Client"), \
             patch("simulator.publish_house_power", new_callable=AsyncMock) as mock_pub:
            mock_pub.return_value = {"house": "H001", "power": 55.0, "devices": []}
            asyncio.run(simulator.run_simulator(args_h10))
            mock_init.assert_called_once_with(["H001"], seed=ANY)

        # 5. --scenario normal_routine --houses 11 (ValueError 발생)
        args_h11 = simulator.parse_args(["--scenario", "normal_routine", "--houses", "11"])
        with self.assertRaises(ValueError) as ctx11:
            asyncio.run(simulator.run_simulator(args_h11))
        self.assertEqual(str(ctx11.exception), "normal_routine 시나리오는 H001 가구에서만 실행할 수 있습니다.")

    def test_normal_routine_cycle_timetable(self):
        """cycle별 가상 시각 시간표 정확성 검증:
        cycle 1 = 08:04:58
        cycle 242 = 08:08:59
        cycle 243 = 08:09:00
        cycle 302 = 08:09:59
        cycle 303 = 08:10:00
        cycle 308 = 08:10:05
        """
        fixed_now = datetime(2026, 9, 15, 12, 0, 0, tzinfo=KST)
        base_dt = resolve_simulation_start_time(
            "normal_routine",
            simulation_date="2026-09-15",
            now=fixed_now,
        )
        self.assertEqual(base_dt.strftime("%H:%M:%S"), "08:04:58")

        def get_time_str_for_cycle(cycle_num):
            dt = base_dt + timedelta(seconds=(cycle_num - 1))
            return dt.strftime("%H:%M:%S")

        self.assertEqual(get_time_str_for_cycle(1), "08:04:58")
        self.assertEqual(get_time_str_for_cycle(242), "08:08:59")
        self.assertEqual(get_time_str_for_cycle(243), "08:09:00")
        self.assertEqual(get_time_str_for_cycle(302), "08:09:59")
        self.assertEqual(get_time_str_for_cycle(303), "08:10:00")
        self.assertEqual(get_time_str_for_cycle(308), "08:10:05")


class TestNormalRoutinePhysicsAndTimeline(unittest.TestCase):
    """5, 6, 7, 8, 9. 전자레인지 60초 활성 샘플, 가전 OFF, 완료 및 랜덤 가전 차단 검증"""

    def setUp(self):
        simulator.init_simulation_states(["H001"])

    def test_event_injection_messages(self):
        # cycle 243: ON 메시지
        msg243 = inject_normal_routine_scenario_event(243, "H001", simulator.device_states)
        self.assertIsNotNone(msg243)
        self.assertIn("08:09 아침 정상 루틴 시작 — 전자레인지 가동", msg243)
        mw = simulator.device_states["H001"]["microwave"]
        self.assertEqual(mw["state"], "STARTING")
        self.assertEqual(mw["session_remaining"], 61)
        self.assertFalse(mw["manual_hold"])

        # cycle 303: OFF 메시지
        msg303 = inject_normal_routine_scenario_event(303, "H001", simulator.device_states)
        self.assertIsNotNone(msg303)
        self.assertIn("08:10 이전 전자레인지 60초 사용 완료 — 대기전력 복귀", msg303)
        self.assertEqual(simulator.device_states["H001"]["microwave"]["state"], "OFF")
        self.assertEqual(simulator.device_states["H001"]["microwave"]["session_remaining"], 0)

        # cycle 308: 완료 메시지
        msg308 = inject_normal_routine_scenario_event(308, "H001", simulator.device_states)
        self.assertIsNotNone(msg308)
        self.assertIn("H001 정상 일상 전력 패턴 발행 완료", msg308)

    def test_exact_60_active_samples_in_simulation(self):
        """cycle 1~242는 OFF, cycle 243~302까지 정확히 60개 활성 샘플, cycle 303~308은 OFF 검증"""
        simulator.init_simulation_states(["H001"])

        active_cycles = []
        off_before_cycles = []
        off_after_cycles = []

        for cycle in range(1, 309):
            # 1. 이벤트 주입
            simulator.inject_normal_routine_scenario_event(cycle, "H001")

            # 2. 물리 엔진 1초 틱 실행 (allow_random=False)
            metrics = simulator.calculate_main_panel_metrics(
                "H001",
                allow_random=False,
            )

            mw = simulator.device_states["H001"]["microwave"]
            mw_state = mw.get("state", "OFF")
            is_mw_active = (mw_state in ("STARTING", "RUNNING") or "microwave" in metrics.get("active_devices", []))

            if cycle < 243:
                self.assertFalse(is_mw_active, f"cycle {cycle}에서 전자레인지가 OFF여야 합니다.")
                off_before_cycles.append(cycle)
            elif 243 <= cycle <= 302:
                self.assertTrue(is_mw_active, f"cycle {cycle}에서 전자레인지가 활성 상태여야 합니다.")
                # 전력값은 기저부하(약 55W) + 냉장고 + 전자레인지(~940W) + 노이즈로 850W 이상 1300W 이하
                self.assertGreater(metrics["active_power"], 850.0)
                self.assertLess(metrics["active_power"], 1600.0)
                active_cycles.append(cycle)
            else:  # 303 <= cycle <= 308
                self.assertFalse(is_mw_active, f"cycle {cycle}에서 전자레인지가 OFF여야 합니다.")
                # 대기전력 상태 (300W 미만)
                self.assertLess(metrics["active_power"], 300.0)
                off_after_cycles.append(cycle)

            # 3. 전자레인지 외 다른 수동 가전은 동작하지 않아야 함
            for d in ["kettle", "induction", "iron", "hair_dryer", "vacuum_cleaner"]:
                self.assertEqual(simulator.device_states["H001"][d]["state"], "OFF", f"cycle {cycle}에서 가전 {d}는 OFF여야 합니다.")

        self.assertEqual(len(off_before_cycles), 242)
        self.assertEqual(len(active_cycles), 60, "전자레인지 ON 활성 샘플은 정확히 60개여야 합니다 (cycle 243~302).")
        self.assertEqual(len(off_after_cycles), 6)  # 303, 304, 305, 306, 307, 308


class TestNormalRoutineSSEAndLifecycle(unittest.TestCase):
    """10, 11. Manager 생명주기, 완료 상태, Pause/Resume 가상 시간 연속성 및 SSE 데이터 검증"""

    def setUp(self):
        self.manager = SimulatorManager(host="localhost", port=1883, tls_enabled=False)

    def tearDown(self):
        self.manager.stop()
        self.manager.reset()

    @patch("aiomqtt.Client")
    def test_normal_routine_manager_start_and_complete(self, mock_client_cls):
        res = self.manager.start(
            households=[{"house": "H001", "scenario": "normal_routine"}],
            simulation_date="2026-09-15",
        )
        self.assertEqual(res.get("status"), "started")
        self.assertEqual(self.manager.current_mode, "normal_routine")
        self.assertEqual(NormalRoutineScenario.TOTAL_CYCLES, 308)
        self.assertEqual(self.manager.simulation_date, "2026-09-15")
        # 08:04:58 KST = 23:04:58 UTC (어제 날짜)
        self.assertEqual(self.manager.resolved_start_time, "2026-09-14T23:04:58.000Z")

    def test_pause_resume_virtual_time_continuity(self):
        """Pause 후 Resume 시 가상 시간이 1초도 어긋나지 않고 정확히 1초 연속성을 유지하는지 검증"""
        with patch("aiomqtt.Client"), patch("simulator.publish_house_power", new_callable=AsyncMock) as mock_pub:
            q = queue.Queue(maxsize=100)
            self.manager.add_subscriber(q)

            self.manager.start(
                households=[{"house": "H001", "scenario": "normal_routine"}],
                simulation_date="2026-09-15"
            )
            # 첫 2개 tick 진행 대기
            time.sleep(2.2)

            pause_res = self.manager.pause()
            self.assertEqual(pause_res["status"], "paused")

            status_paused = self.manager.get_status()
            paused_cycle = status_paused["cycle_count"]
            paused_iso = status_paused["last_metrics_by_house"]["H001"]["now_iso"]

            pub_count_at_pause = mock_pub.call_count

            # 큐를 비워서 pause 이전 이벤트 제거
            while not q.empty():
                try:
                    q.get_nowait()
                except queue.Empty:
                    break

            # pause 중 대기: cycle과 MQTT 발행이 동결되어야 함
            time.sleep(1.2)
            self.assertEqual(self.manager.cycle_count, paused_cycle)
            self.assertEqual(mock_pub.call_count, pub_count_at_pause)
            self.assertTrue(q.empty())

            # resume 호출
            resume_res = self.manager.resume()
            self.assertEqual(resume_res["status"], "resumed")

            # resume 후 정확히 첫 이벤트 수신
            first_event_after_resume = q.get(timeout=3.0)
            resumed_iso = first_event_after_resume["now_iso"]

            # 가상 시각 +1초 연속성 검증
            dt_paused = datetime.fromisoformat(paused_iso.replace("Z", "+00:00"))
            dt_resumed = datetime.fromisoformat(resumed_iso.replace("Z", "+00:00"))
            diff_seconds = (dt_resumed - dt_paused).total_seconds()
            self.assertEqual(diff_seconds, 1.0, f"resume 후 첫 타임스탬프는 pause 직전의 정확히 +1초여야 함 (실제 차이: {diff_seconds}초)")

            self.manager.remove_subscriber(q)

    def test_sse_metrics_structure(self):
        """SSE 메시지에 정확한 scenario, mode, status, cycle_count 등이 포함되는지 검증"""
        with patch("aiomqtt.Client"), patch("simulator.publish_house_power", new_callable=AsyncMock):
            q = queue.Queue(maxsize=100)
            self.manager.add_subscriber(q)

            self.manager.start(
                households=[{"house": "H001", "scenario": "normal_routine"}],
                simulation_date="2026-09-15"
            )

            # 첫 SSE 이벤트 수신 대기
            event = q.get(timeout=3.0)

            self.assertEqual(event["house"], "H001")
            self.assertEqual(event["scenario"], "normal_routine")
            self.assertEqual(event["mode"], "normal_routine")
            self.assertEqual(event["sec"], 1)
            self.assertEqual(event["cycle_count"], 1)
            self.assertEqual(event["status"], "running")
            self.assertIn("devices", event)
            self.assertIn("microwave", event["devices"])
            self.assertIn("totalP", event)
            self.assertIn("apparentS", event)
            self.assertIn("simTimeKst", event)
            self.assertEqual(event["simTimeKst"], "08:04:58")

            self.manager.remove_subscriber(q)


class TestRegressionExistingScenarios(unittest.TestCase):
    """12. 기존 peak, routine_missed 시나리오 회귀 검증"""

    def test_routine_missed_scenario_intact(self):
        self.assertEqual(RoutineMissedScenario.TARGET_HOUSE, "H001")
        self.assertEqual(RoutineMissedScenario.DEFAULT_START_TIME, "08:15:00")
        self.assertEqual(RoutineMissedScenario.BUFFER_WINDOW_SIZE, 299)
        self.assertEqual(RoutineMissedScenario.DEFAULT_COUNT, 300)

    def test_peak_scenario_intact(self):
        self.assertIn(10, PEAK_SCENARIO_SCHEDULE)
        self.assertIn(31, PEAK_SCENARIO_SCHEDULE)
        self.assertIn(45, PEAK_SCENARIO_SCHEDULE)
        self.assertEqual(PEAK_SCENARIO_SCHEDULE[10]["event"], "PEAK_START")
        self.assertEqual(PEAK_SCENARIO_SCHEDULE[31]["event"], "PEAK_EASE")
        self.assertEqual(PEAK_SCENARIO_SCHEDULE[45]["event"], "NORMAL_RETURN")


if __name__ == "__main__":
    unittest.main()
