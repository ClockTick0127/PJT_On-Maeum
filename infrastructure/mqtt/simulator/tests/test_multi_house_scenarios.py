import io
import json
import os
import queue
import subprocess
import sys
import threading
import time
import unittest
from datetime import datetime, date
from unittest.mock import MagicMock, AsyncMock, patch

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SIMULATOR_DIR = os.path.dirname(TESTS_DIR)
if SIMULATOR_DIR not in sys.path:
    sys.path.insert(0, SIMULATOR_DIR)

import scenarios
from scenarios import (
    KST,
    parse_simulation_date,
    resolve_multi_simulation_start_time,
    resolve_simulation_start_time,
    format_iso_utc,
)
import simulator
from server.manager import SimulatorManager, ModeConflictError
from server.request_handler import RequestHandler


class DummyRequestHandler(RequestHandler):
    """테스트용 Mock RequestHandler"""
    def __init__(self, manager, method="POST", path="/api/start", body_dict=None):
        self.manager = manager
        self.path = path
        self.command = method
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


class TestMultiHouseStartTimeResolution(unittest.TestCase):
    """다중 가구 공통 base_dt 결정 규칙 테스트 (규칙 3)"""

    def setUp(self):
        # 고정된 기준 KST 시각: 2026-09-14 14:30:15 KST
        self.fixed_now = datetime(2026, 9, 14, 14, 30, 15, tzinfo=KST)

    def test_multi_with_routine_missed_shares_08_10_01_kst(self):
        """실행 가구 중 routine_missed가 하나라도 있으면 08:10:01 KST 공유"""
        households = [
            {"house": "H001", "scenario": "peak"},
            {"house": "H002", "scenario": "routine_missed"},
            {"house": "H003", "scenario": "random"},
        ]
        # 1. 날짜가 지정된 경우
        base_dt = resolve_multi_simulation_start_time(
            households,
            simulation_date="2026-09-20",
            now=self.fixed_now,
        )
        self.assertEqual(base_dt.year, 2026)
        self.assertEqual(base_dt.month, 9)
        self.assertEqual(base_dt.day, 20)
        self.assertEqual(base_dt.hour, 8)
        self.assertEqual(base_dt.minute, 10)
        self.assertEqual(base_dt.second, 1)
        self.assertEqual(base_dt.tzinfo, KST)

        # 2. 날짜가 미지정된 경우 (now의 날짜 + 08:10:01)
        base_dt_none = resolve_multi_simulation_start_time(
            households,
            simulation_date=None,
            now=self.fixed_now,
        )
        self.assertEqual(base_dt_none.year, 2026)
        self.assertEqual(base_dt_none.month, 9)
        self.assertEqual(base_dt_none.day, 14)
        self.assertEqual(base_dt_none.hour, 8)
        self.assertEqual(base_dt_none.minute, 10)
        self.assertEqual(base_dt_none.second, 1)
        self.assertEqual(base_dt_none.tzinfo, KST)

    def test_multi_without_routine_missed_shares_date_plus_current_kst(self):
        """routine_missed가 없으면 선택 날짜 + 현재 KST 시각 결합"""
        households = [
            {"house": "H001", "scenario": "peak"},
            {"house": "H002", "scenario": "random"},
            {"house": "H003", "scenario": "manual"},
        ]
        # 1. 날짜 지정 시: 2026-09-25 + 14:30:15
        base_dt = resolve_multi_simulation_start_time(
            households,
            simulation_date="2026-09-25",
            now=self.fixed_now,
        )
        self.assertEqual(base_dt.year, 2026)
        self.assertEqual(base_dt.month, 9)
        self.assertEqual(base_dt.day, 25)
        self.assertEqual(base_dt.hour, 14)
        self.assertEqual(base_dt.minute, 30)
        self.assertEqual(base_dt.second, 15)
        self.assertEqual(base_dt.tzinfo, KST)

        # 2. 날짜 미지정 시: 현재 KST 시각 그대로
        base_dt_none = resolve_multi_simulation_start_time(
            households,
            simulation_date=None,
            now=self.fixed_now,
        )
        self.assertEqual(base_dt_none, self.fixed_now)

    def test_single_routine_missed_backward_compatibility(self):
        """기존 단일 가구 routine_missed도 08:10:01 KST 유지"""
        res = resolve_simulation_start_time(
            "routine_missed",
            simulation_date="2026-09-10",
            now=self.fixed_now,
        )
        self.assertEqual(res, datetime(2026, 9, 10, 8, 10, 1, tzinfo=KST))


class TestMultiHouseApiRequestHandler(unittest.TestCase):
    """API 요청 유효성 검증 및 하위 호환성 테스트 (규칙 1, 5)"""

    def _make_handler(self, method="POST", path="/api/start", body=None):
        handler = RequestHandler.__new__(RequestHandler)
        handler.command = method
        handler.path = path
        handler.headers = {}
        handler.manager = MagicMock()
        handler.manager.start.return_value = {
            "status": "started",
            "scenario": "peak",
            "house": "H001",
            "households": [{"house": "H001", "scenario": "peak"}],
            "simulation_date": None,
            "resolved_start_time": None,
        }
        handler.wfile = io.BytesIO()

        if body is not None:
            body_bytes = json.dumps(body).encode("utf-8")
            handler.headers["Content-Length"] = str(len(body_bytes))
            handler.rfile = io.BytesIO(body_bytes)
        else:
            handler.headers["Content-Length"] = "0"
            handler.rfile = io.BytesIO(b"")

        handler.send_response = MagicMock()
        handler.send_header = MagicMock()
        handler.end_headers = MagicMock()
        return handler

    def _get_response_body(self, handler):
        return json.loads(handler.wfile.getvalue().decode("utf-8"))

    def test_start_valid_multi_households(self):
        """다중 가구 정상 시작 요청"""
        req_body = {
            "households": [
                {"house": "H001", "scenario": "peak"},
                {"house": "H002", "scenario": "routine_missed"},
            ],
            "simulation_date": "2026-09-15",
        }
        handler = self._make_handler(body=req_body)
        handler.do_POST()

        handler.send_response.assert_called_with(200)
        handler.manager.start.assert_called_once_with(
            simulation_date="2026-09-15",
            households=[
                {"house": "H001", "scenario": "peak"},
                {"house": "H002", "scenario": "routine_missed"},
            ],
        )

    def test_start_single_household_backward_compatibility(self):
        """단일 가구 하위 호환 요청 (scenario + house)"""
        req_body = {
            "scenario": "peak",
            "house": "H001",
            "simulation_date": "2026-09-15",
        }
        handler = self._make_handler(body=req_body)
        handler.do_POST()

        handler.send_response.assert_called_with(200)
        # 단일 가구는 기존 인자 형태로 manager.start 호출
        handler.manager.start.assert_called_once_with(
            scenario="peak",
            house="H001",
            simulation_date="2026-09-15",
        )

    def test_start_empty_households_rejected(self):
        """빈 가구 목록은 400 거절"""
        req_body = {"households": []}
        handler = self._make_handler(body=req_body)
        handler.do_POST()

        handler.send_response.assert_called_with(400)
        body = self._get_response_body(handler)
        self.assertIn("최대 가구 수", body["message"]) if "최대" in body["message"] else self.assertIn("비어있을 수 없습니다", body["message"])

    def test_start_duplicate_house_rejected(self):
        """중복 가구 ID는 400 거절"""
        req_body = {
            "households": [
                {"house": "H001", "scenario": "peak"},
                {"house": "H001", "scenario": "random"},
            ]
        }
        handler = self._make_handler(body=req_body)
        handler.do_POST()

        handler.send_response.assert_called_with(400)
        body = self._get_response_body(handler)
        self.assertIn("중복된 가구 ID", body["message"])

    def test_start_conflicting_house_and_households_rejected(self):
        """house/scenario와 households 동시 지정 시 400 거절"""
        req_body = {
            "house": "H001",
            "households": [{"house": "H001", "scenario": "peak"}],
        }
        handler = self._make_handler(body=req_body)
        handler.do_POST()

        handler.send_response.assert_called_with(400)
        body = self._get_response_body(handler)
        self.assertIn("동시에 전달할 수 없습니다", body["message"])

    def test_start_invalid_scenario_rejected(self):
        """유효하지 않은 시나리오는 400 거절"""
        req_body = {
            "households": [{"house": "H001", "scenario": "invalid_mode"}]
        }
        handler = self._make_handler(body=req_body)
        handler.do_POST()

        handler.send_response.assert_called_with(400)
        body = self._get_response_body(handler)
        self.assertIn("지원하지 않는 시나리오", body["message"])

    def test_start_invalid_house_format_rejected(self):
        """유효하지 않은 가구 번호는 400 거절"""
        req_body = {
            "households": [{"house": "H999", "scenario": "peak"}]
        }
        handler = self._make_handler(body=req_body)
        handler.do_POST()

        handler.send_response.assert_called_with(400)
        body = self._get_response_body(handler)
        self.assertIn("유효하지 않은 house ID", body["message"])

    def test_start_too_many_households_rejected(self):
        """10개 초과 가구 요청 시 400 거절"""
        req_body = {
            "households": [{"house": f"H{i:03d}", "scenario": "peak"} for i in range(1, 12)]
        }
        handler = self._make_handler(body=req_body)
        handler.do_POST()

        handler.send_response.assert_called_with(400)
        body = self._get_response_body(handler)
        self.assertIn("최대 가구 수", body["message"])

    def test_put_device_validation_in_handler(self):
        """PUT /api/device는 형식 검증 후 manager.set_device에 위임 (규칙 5)"""
        # 1. 필수값 누락
        handler = self._make_handler(method="PUT", path="/api/device", body={"device": "kettle"})
        handler.do_PUT()
        handler.send_response.assert_called_with(400)

        # 2. 올바른 형식 -> manager.set_device 호출
        handler_ok = self._make_handler(
            method="PUT",
            path="/api/device",
            body={"house": "H002", "device": "kettle", "enabled": True},
        )
        handler_ok.manager.set_device.return_value = {"state": "ON"}
        handler_ok.do_PUT()
        handler_ok.send_response.assert_called_with(200)
        handler_ok.manager.set_device.assert_called_once_with(
            "H002",
            "kettle",
            True
        )

    def test_pause_endpoint(self):
        """POST /api/pause 정상 처리"""
        handler = self._make_handler(path="/api/pause", body={})
        handler.manager.pause = MagicMock(return_value={"status": "paused"})
        handler.do_POST()
        handler.send_response.assert_called_with(200)
        body = self._get_response_body(handler)
        self.assertEqual(body["status"], "paused")
        handler.manager.pause.assert_called_once()

    def test_resume_endpoint(self):
        """POST /api/resume 정상 처리"""
        handler = self._make_handler(path="/api/resume", body={})
        handler.manager.resume = MagicMock(return_value={"status": "resumed"})
        handler.do_POST()
        handler.send_response.assert_called_with(200)
        body = self._get_response_body(handler)
        self.assertEqual(body["status"], "resumed")
        handler.manager.resume.assert_called_once()

    def test_reset_endpoint(self):
        """POST /api/reset 정상 처리"""
        handler = self._make_handler(path="/api/reset", body={})
        handler.manager.reset = MagicMock(return_value={"status": "reset"})
        handler.do_POST()
        handler.send_response.assert_called_with(200)
        body = self._get_response_body(handler)
        self.assertEqual(body["status"], "reset")
        handler.manager.reset.assert_called_once()


class TestSimulatorManagerMultiHouse(unittest.TestCase):
    """SimulatorManager 다중 가구 실행, 상태 전이, 락 검증 (규칙 1, 2, 5, 6)"""

    def setUp(self):
        self.manager = SimulatorManager()

    def tearDown(self):
        if self.manager.is_running:
            self.manager.stop()

    def test_01_manager_start_positional_args_backward_compatibility(self):
        """1. manager.start('peak', 'H001') 위치 인자 호출 하위 호환성 (규칙 1)"""
        with patch("aiomqtt.Client"), patch("simulator.publish_house_power", new_callable=AsyncMock):
            res = self.manager.start("peak", "H001", "2026-09-10")
            self.assertTrue(self.manager.is_running)
            self.assertEqual(self.manager.current_mode, "peak")
            self.assertEqual(self.manager.house, "H001")
            self.assertIn("H001", self.manager.active_households)
            self.assertEqual(self.manager.active_households["H001"]["scenario"], "peak")
            self.manager.stop()
            self.assertFalse(self.manager.is_running)

    def test_02_peak_60th_final_sse_status_is_completed(self):
        """2. peak 60번째 최종 SSE의 status가 반드시 completed인지 검증 (규칙 2)"""
        q = queue.Queue()
        self.manager.add_subscriber(q)

        with patch("aiomqtt.Client"), patch("simulator.publish_house_power", new_callable=AsyncMock):
            self.manager.start(
                households=[{"house": "H001", "scenario": "peak"}],
                simulation_date="2026-09-10",
            )
            # cycle_count를 59로 사전 설정하여 다음 틱이 60번째(마지막)가 되도록 유도
            with self.manager.lock:
                self.manager.active_households["H001"]["cycle_count"] = 59
                self.manager.global_cycle_count = 59
                self.manager.cycle_count = 59

            # 60번째 틱 브로드캐스트 대기 (최대 2.5초)
            received_event = None
            start_wait = time.time()
            while time.time() - start_wait < 3.0:
                try:
                    event = q.get(timeout=0.5)
                    if event.get("house") == "H001" and event.get("cycle_count") == 60:
                        received_event = event
                        break
                except queue.Empty:
                    pass

            self.manager.stop()

        self.assertIsNotNone(received_event, "60번째 SSE 이벤트가 수신되어야 함")
        self.assertEqual(received_event["house"], "H001")
        self.assertEqual(received_event["cycle_count"], 60)
        self.assertEqual(received_event["status"], "completed", "60번째 최종 SSE의 status는 completed이어야 함")
        self.assertEqual(self.manager.active_households["H001"]["status"], "completed")

    def test_03_routine_missed_300th_final_sse_status_is_completed(self):
        """3. routine_missed 300번째 최종 SSE의 status가 completed인지 검증 (규칙 2)"""
        q = queue.Queue()
        self.manager.add_subscriber(q)

        with patch("aiomqtt.Client"), patch("simulator.publish_house_power", new_callable=AsyncMock):
            self.manager.start(
                households=[{"house": "H001", "scenario": "routine_missed"}],
                simulation_date="2026-09-10",
            )
            with self.manager.lock:
                self.manager.active_households["H001"]["cycle_count"] = 299
                self.manager.global_cycle_count = 299
                self.manager.cycle_count = 299

            received_event = None
            start_wait = time.time()
            while time.time() - start_wait < 3.0:
                try:
                    event = q.get(timeout=0.5)
                    if event.get("house") == "H001" and event.get("cycle_count") == 300:
                        received_event = event
                        break
                except queue.Empty:
                    pass

            self.manager.stop()

        self.assertIsNotNone(received_event, "300번째 SSE 이벤트가 수신되어야 함")
        self.assertEqual(received_event["house"], "H001")
        self.assertEqual(received_event["cycle_count"], 300)
        self.assertEqual(received_event["status"], "completed", "300번째 최종 SSE의 status는 completed이어야 함")
        self.assertEqual(self.manager.active_households["H001"]["status"], "completed")

    def test_04_h001_completed_while_h002_continues_publishing(self):
        """4. H001 완료 후에도 H002가 계속 MQTT publish되는지 검증 (규칙 2, 7)"""
        published_houses = []

        async def mock_pub(client, house, **kwargs):
            published_houses.append(house)
            return {"house": house, "power": 50.0}

        with patch("aiomqtt.Client"), patch("simulator.publish_house_power", side_effect=mock_pub):
            self.manager.start(
                households=[
                    {"house": "H001", "scenario": "peak"},
                    {"house": "H002", "scenario": "random"},
                ],
                simulation_date="2026-09-10",
            )
            # H001을 59틱으로 사전 설정 -> 1틱 후 60틱 완료
            with self.manager.lock:
                self.manager.active_households["H001"]["cycle_count"] = 59

            # 2.2초 동안 실행하여 2번의 틱을 진행
            time.sleep(2.3)

            with self.manager.lock:
                h001_status = self.manager.active_households["H001"]["status"]
                h002_status = self.manager.active_households["H002"]["status"]

            self.manager.stop()

        self.assertEqual(h001_status, "completed")
        self.assertEqual(h002_status, "running")

        h001_count = published_houses.count("H001")
        h002_count = published_houses.count("H002")

        # H001은 완료 후 더 이상 발행되지 않으므로 1회 발행, H002는 2회 이상 발행
        self.assertEqual(h001_count, 1, "H001은 60틱 완료 후 추가 발행되지 않아야 함")
        self.assertGreaterEqual(h002_count, 2, "H002는 2회 이상 계속 발행되어야 함")

    def test_05_all_households_share_exact_same_timestamps_in_tick(self):
        """5. 동일 tick에서 모든 가구의 MQTT measured_at, ts 및 SSE now_iso 일치 (규칙 3)"""
        captured_mqtt = []
        q = queue.Queue()
        self.manager.add_subscriber(q)

        async def mock_pub(client, house, **kwargs):
            captured_mqtt.append({
                "house": house,
                "now_iso": kwargs.get("now_iso"),
            })
            return {"house": house, "power": 50.0}

        with patch("aiomqtt.Client"), patch("simulator.publish_house_power", side_effect=mock_pub):
            self.manager.start(
                households=[
                    {"house": "H001", "scenario": "peak"},
                    {"house": "H002", "scenario": "routine_missed"},
                    {"house": "H003", "scenario": "random"},
                ],
                simulation_date="2026-09-15",
            )
            # 1틱 실행 대기
            time.sleep(1.3)
            self.manager.stop()

        # 첫 번째 틱(3개 가구) 수집 확인
        first_tick_mqtt = captured_mqtt[:3]
        self.assertEqual(len(first_tick_mqtt), 3)

        # 모든 가구의 now_iso 일치 검증
        now_isos = [m["now_iso"] for m in first_tick_mqtt]
        self.assertEqual(len(set(now_isos)), 1, "동일 tick에서 모든 가구의 now_iso는 완전히 동일해야 함")

        # SSE now_iso 및 simTimeKst 일치 확인
        sse_events = []
        while not q.empty():
            sse_events.append(q.get_nowait())
        self.assertGreater(len(sse_events), 0)
        self.assertEqual(sse_events[0]["now_iso"], now_isos[0])
        # routine_missed가 포함되어 있으므로 시작 시각은 08:10:01 KST
        self.assertEqual(sse_events[0]["simTimeKst"], "08:10:01")

    def test_06_manual_validation_and_lock_atomicity(self):
        """6. manual 상태 검증과 변경이 Manager 락 안에서 원자적으로 처리되는지 검증 (규칙 5)"""
        # 1. 미실행 상태에서 set_device 호출 -> ModeConflictError
        with self.assertRaises(ModeConflictError) as ctx:
            self.manager.set_device("H001", "kettle", True)
        self.assertIn("정지 상태", str(ctx.exception))

        # 2. 실행 상태 (H001은 peak, H002는 manual)
        with patch("aiomqtt.Client"), patch("simulator.publish_house_power", new_callable=AsyncMock):
            self.manager.start(
                households=[
                    {"house": "H001", "scenario": "peak"},
                    {"house": "H002", "scenario": "manual"},
                ]
            )

            # (1) active_households에 없는 가구 제어 시도 -> ModeConflictError
            with self.assertRaises(ModeConflictError) as ctx:
                self.manager.set_device("H003", "kettle", True)
            self.assertIn("포함되어 있지 않습니다", str(ctx.exception))

            # (2) manual이 아닌 가구(H001: peak) 제어 시도 -> ModeConflictError
            with self.assertRaises(ModeConflictError) as ctx:
                self.manager.set_device("H001", "kettle", True)
            self.assertIn("manual' 시나리오 가구에서만", str(ctx.exception))

            # (3) manual 가구(H002) 정상 제어 -> 성공
            res = self.manager.set_device("H002", "kettle", True)
            self.assertIsNotNone(res)
            self.assertEqual(res.get("state"), "STARTING")

            self.manager.stop()

    def test_07_state_fields_semantics(self):
        """7. 다중 실행 시 하위 호환 상태 필드 정의 검증 (규칙 6)"""
        with patch("aiomqtt.Client"), patch("simulator.publish_house_power", new_callable=AsyncMock):
            self.manager.start(
                households=[
                    {"house": "H001", "scenario": "peak"},
                    {"house": "H002", "scenario": "random"},
                ]
            )
            self.assertTrue(self.manager.is_running)
            self.assertEqual(self.manager.current_mode, "multi")
            self.assertEqual(self.manager.cycle_count, self.manager.global_cycle_count)
            self.assertIn("H001", self.manager.active_households)
            self.assertIn("H002", self.manager.active_households)

            status = self.manager.get_status()
            self.assertTrue(status["is_running"])
            self.assertEqual(status["current_mode"], "multi")
            self.assertEqual(status["cycle_count"], self.manager.global_cycle_count)
            self.assertIn("active_households", status)
            self.assertIn("last_metrics_by_house", status)

            self.manager.stop()
            self.assertFalse(self.manager.is_running)

    def test_08_pause_and_resume_invariance_and_no_publish(self):
        """8. pause 시 MQTT/SSE 무발행 및 timestamp/cycle 동결, resume 후 다음 1초 tick으로 정상 연결 검증"""
        q = queue.Queue()
        self.manager.add_subscriber(q)
        published_calls = []

        async def mock_pub(client, house, **kwargs):
            published_calls.append({
                "house": house,
                "now_iso": kwargs.get("now_iso"),
                "time": time.time()
            })
            return {"house": house, "power": 100.0}

        with patch("aiomqtt.Client"), patch("simulator.publish_house_power", side_effect=mock_pub):
            # 1. 2026-09-10 날짜로 시작 (routine_missed 포함 -> 08:10:01 KST -> 2026-09-09T23:10:01.000Z 시작)
            self.manager.start(
                households=[{"house": "H001", "scenario": "routine_missed"}],
                simulation_date="2026-09-10",
            )
            # 첫 번째 틱 실행 대기
            time.sleep(1.3)
            with self.manager.lock:
                cycle_before_pause = self.manager.cycle_count
                self.assertGreaterEqual(cycle_before_pause, 1)

            # 2. pause() 호출 (Manager 락 + tick_lock 보호)
            pause_res = self.manager.pause()
            self.assertEqual(pause_res["status"], "paused")
            with self.manager.lock:
                self.assertTrue(self.manager.is_paused)

            # pause 응답 반환 시점의 발행 횟수 및 이벤트 수 기록
            mqtt_count_at_pause = len(published_calls)
            sse_count_at_pause = q.qsize()

            # 0.6초 대기 동안 추가 발행 및 cycle, timestamp 증가가 전혀 없어야 함
            time.sleep(0.6)
            with self.manager.lock:
                self.assertTrue(self.manager.is_paused)
                self.assertEqual(self.manager.cycle_count, cycle_before_pause, "pause 중에는 cycle이 증가하지 않아야 함")
            self.assertEqual(len(published_calls), mqtt_count_at_pause, "pause 응답 후 추가 MQTT 발행이 없어야 함")
            self.assertEqual(q.qsize(), sse_count_at_pause, "pause 응답 후 추가 SSE 브로드캐스트가 없어야 함")

            # 3. resume() 호출
            resume_res = self.manager.resume()
            self.assertEqual(resume_res["status"], "resumed")
            with self.manager.lock:
                self.assertFalse(self.manager.is_paused)

            # resume 후 다음 1초 tick 실행 대기
            time.sleep(1.3)
            self.manager.stop()

            # resume 후 정확히 cycle_before_pause + 1 틱이 실행되었는지 검증
            with self.manager.lock:
                cycle_after_resume = self.manager.cycle_count
                self.assertGreaterEqual(cycle_after_resume, cycle_before_pause + 1)

            # 마지막 발행된 이벤트의 timestamp가 이전 틱의 1초 후인지 검증
            self.assertGreater(len(published_calls), mqtt_count_at_pause)
            last_before_pause = published_calls[mqtt_count_at_pause - 1]["now_iso"]
            next_after_resume = published_calls[mqtt_count_at_pause]["now_iso"]
            dt_before = datetime.fromisoformat(last_before_pause.replace("Z", "+00:00"))
            dt_after = datetime.fromisoformat(next_after_resume.replace("Z", "+00:00"))
            self.assertEqual((dt_after - dt_before).total_seconds(), 1.0, "resume 후 다음 1초 tick으로 정확히 이어져야 함")

    def test_09_stop_while_paused(self):
        """9. pause 상태에서도 stop이 행업 없이 정상 동작하는지 검증"""
        with patch("aiomqtt.Client"), patch("simulator.publish_house_power", new_callable=AsyncMock):
            self.manager.start(households=[{"house": "H001", "scenario": "peak"}])
            time.sleep(0.5)

            # pause 실행
            self.manager.pause()
            with self.manager.lock:
                self.assertTrue(self.manager.is_paused)
                self.assertTrue(self.manager.is_running)

            # pause 상태에서 stop() 호출
            start_stop_time = time.time()
            stopped = self.manager.stop()
            stop_duration = time.time() - start_stop_time

            self.assertTrue(stopped, "stop()이 True를 반환해야 함")
            self.assertLess(stop_duration, 2.0, "pause 상태에서 stop()이 2초 이내로 즉각 완료되어야 함")
            with self.manager.lock:
                self.assertFalse(self.manager.is_running)
                self.assertFalse(self.manager.is_paused)
                self.assertIsNone(self.manager.worker_thread)

    def test_10_retention_rules_natural_completion_vs_stop_vs_reset(self):
        """10. 자연 완료, 명시적 stop, reset의 상태 보존 규칙 분리 검증"""
        # (A) 자연 완료 규칙: running이던 가구가 목표 도달 시 completed가 되고 워커는 종료되지만
        #     active_households의 completed 상태, last_metrics_by_house, simulation_date 보존
        with patch("aiomqtt.Client"), patch("simulator.publish_house_power", new_callable=AsyncMock):
            self.manager.start(
                households=[{"house": "H001", "scenario": "peak"}],
                simulation_date="2026-09-10"
            )
            # cycle을 59로 주입하여 1틱 후 자연 완료 유도
            with self.manager.lock:
                self.manager.active_households["H001"]["cycle_count"] = 59
                self.manager.global_cycle_count = 59
                self.manager.cycle_count = 59

            time.sleep(2.0)
            status_natural = self.manager.get_status()
            self.assertFalse(status_natural["is_running"], "모든 가구 완주 후 워커는 자동 정지(is_running: False)되어야 함")
            self.assertEqual(status_natural["active_households"]["H001"]["status"], "completed", "완주 가구는 completed 상태로 보존되어야 함")
            self.assertIn("H001", status_natural["last_metrics_by_house"], "완주 가구의 last_metrics_by_house는 보존되어야 함")

        # (B) 명시적 stop 규칙:
        #     H001(completed)과 H002(running) 중 stop 호출 시
        #     H001은 completed 유지, running이던 H002만 stopped로 변경, 메트릭 보존
        with patch("aiomqtt.Client"), patch("simulator.publish_house_power", new_callable=AsyncMock):
            self.manager.start(
                households=[
                    {"house": "H001", "scenario": "peak"},
                    {"house": "H002", "scenario": "random"},
                ],
                simulation_date="2026-09-20"
            )
            time.sleep(0.5)
            with self.manager.lock:
                self.manager.active_households["H001"]["status"] = "completed"

            self.manager.stop()
            status_stop = self.manager.get_status()
            self.assertFalse(status_stop["is_running"])
            self.assertEqual(status_stop["active_households"]["H001"]["status"], "completed", "기존 completed 가구는 stop 후에도 completed 유지")
            self.assertEqual(status_stop["active_households"]["H002"]["status"], "stopped", "running이던 가구는 stopped로 전이")
            self.assertIn("H001", status_stop["last_metrics_by_house"], "명시적 stop 후에도 메트릭 보존")

        # (C) reset 규칙:
        #     모든 가구 상태, 메트릭, 일시정지, 날짜 정보를 완전히 비움
        res = self.manager.reset()
        self.assertEqual(res["status"], "reset")
        status_reset = self.manager.get_status()
        self.assertFalse(status_reset["is_running"])
        self.assertFalse(status_reset["is_paused"])
        self.assertEqual(status_reset["active_households"], {}, "reset 후 active_households는 비어있어야 함")
        self.assertEqual(status_reset["last_metrics_by_house"], {}, "reset 후 last_metrics_by_house는 비어있어야 함")
        self.assertIsNone(status_reset["simulation_date"], "reset 후 simulation_date는 None이어야 함")
        self.assertIsNone(status_reset["resolved_start_time"], "reset 후 resolved_start_time은 None이어야 함")

    def test_12_reset_failure_handling_and_state_retention(self):
        """12. reset 시 실제 _stop_and_join() 실패 경로에서 예외 발생, HTTP 503 반환 및 데이터 보존 검증"""
        # _stop_and_join() 자체는 mock하지 않고, join() 호출 후에도 살아있는 fake worker thread 구성
        class FakeHungWorker:
            def __init__(self):
                self.join_called = False
                self.join_timeout = None

            def is_alive(self):
                return True

            def join(self, timeout=None):
                self.join_called = True
                self.join_timeout = timeout
                # 10초 동안 대기하지 않고 즉시 반환하여 테스트 지연 방지

        fake_worker = FakeHungWorker()
        real_stop_event = threading.Event()

        # 사전 상태 주입 (일시정지 상태 포함)
        with self.manager.lock:
            self.manager.active_households = {
                "H001": {"house": "H001", "scenario": "peak", "status": "running", "cycle_count": 5}
            }
            self.manager.last_metrics_by_house = {"H001": {"apparentS": 120.0, "power": 120.0}}
            self.manager.last_metrics = {"apparentS": 120.0, "power": 120.0}
            self.manager.simulation_date = "2026-09-10"
            self.manager.resolved_start_time = "2026-09-10T08:10:01+09:00"
            self.manager.cycle_count = 5
            self.manager.global_cycle_count = 5
            self.manager.is_paused = True  # reset 호출 시 false 전이 검증을 위해 True 설정
            self.manager.is_running = True
            self.manager.stop_event = real_stop_event
            self.manager.worker_thread = fake_worker

        try:
            # 1. Manager.reset() 호출 시 RuntimeError 발생 확인 (_stop_and_join() 직접 실행)
            with self.assertRaises(RuntimeError) as ctx:
                self.manager.reset()
            self.assertIn("시뮬레이터 워커 종료에 실패", str(ctx.exception))

            # 2. _stop_and_join() 내부 동작 및 상태 전이 검증
            self.assertTrue(real_stop_event.is_set(), "종료 요청(stop_event.set())이 워커에 전달되어야 함")
            self.assertFalse(self.manager.is_paused, "is_paused는 false로 전이되어야 함")
            self.assertTrue(fake_worker.join_called, "worker_thread.join()이 호출되어야 함")
            self.assertEqual(fake_worker.join_timeout, 10.0, "join 타임아웃 10초가 인자로 전달되어야 함")

            # 3. reset 초기화가 실행되지 않고 데이터 필드가 보존되는지 검증
            status = self.manager.get_status()
            self.assertIn("H001", status["active_households"], "active_households가 보존되어야 함")
            self.assertEqual(status["active_households"]["H001"]["cycle_count"], 5)
            self.assertEqual(status["last_metrics_by_house"], {"H001": {"apparentS": 120.0, "power": 120.0}}, "last_metrics_by_house 보존")
            self.assertEqual(status["last_metrics"], {"apparentS": 120.0, "power": 120.0}, "last_metrics 보존")
            self.assertEqual(status["simulation_date"], "2026-09-10", "simulation_date 보존")
            self.assertEqual(status["resolved_start_time"], "2026-09-10T08:10:01+09:00", "resolved_start_time 보존")
            self.assertEqual(status["cycle_count"], 5, "cycle_count 보존")
            self.assertEqual(status["global_cycle_count"], 5, "global_cycle_count 보존")

            # 4. RequestHandler /api/reset 호출 시 HTTP 503 및 RESET_FAILED 반환 유지 확인
            handler = DummyRequestHandler(self.manager, method="POST", path="/api/reset")
            handler.send_response = MagicMock()
            handler.send_header = MagicMock()
            handler.end_headers = MagicMock()

            handler.do_POST()
            handler.send_response.assert_called_with(503)
            resp = handler.get_response_json()
            self.assertEqual(resp.get("code"), "RESET_FAILED")
            self.assertIn("워커 종료에 실패", resp.get("message", ""))

        finally:
            # 5. 테스트 종료 후 fake worker 상태가 다른 테스트에 영향을 주지 않도록 완벽 정리
            with self.manager.lock:
                self.manager.worker_thread = None
                self.manager.stop_event = None
                self.manager.is_running = False
                self.manager.is_paused = False
            # 정상 reset 수행으로 초기화 상태 복구
            reset_res = self.manager.reset()
            self.assertEqual(reset_res["status"], "reset")

    def test_13_pause_resume_timestamp_continuity_without_simulation_date(self):
        """13. simulation_date가 없는 실행에서 pause/resume 시 timestamp 연속성 (+1s) 및 무발행 검증"""
        with patch("aiomqtt.Client"), patch("simulator.publish_house_power", new_callable=AsyncMock) as mock_pub:
            q = queue.Queue(maxsize=100)
            self.manager.add_subscriber(q)

            self.manager.start(
                households=[
                    {"house": "H001", "scenario": "peak"},
                    {"house": "H002", "scenario": "random"}
                ],
                simulation_date=None
            )
            # 첫 2개 tick 진행 대기
            time.sleep(2.2)

            # pause 호출
            pause_res = self.manager.pause()
            self.assertEqual(pause_res["status"], "paused")

            status_paused = self.manager.get_status()
            paused_cycle = status_paused["cycle_count"]
            last_metrics_h001 = status_paused["last_metrics_by_house"]["H001"]
            paused_iso = last_metrics_h001["now_iso"]

            # 동일 tick에서 모든 가구가 동일한 timestamp를 공유하는지 검증
            self.assertEqual(
                status_paused["last_metrics_by_house"]["H001"]["now_iso"],
                status_paused["last_metrics_by_house"]["H002"]["now_iso"],
                "동일 tick의 모든 가구는 동일한 now_iso를 공유해야 함"
            )

            pub_count_at_pause = mock_pub.call_count

            # 큐를 비워서 pause 이전 이벤트 제거
            while not q.empty():
                try:
                    q.get_nowait()
                except queue.Empty:
                    break

            # pause 중 1.5초 대기: 추가 발행 및 cycle 증가가 없어야 함
            time.sleep(1.5)
            self.assertEqual(self.manager.cycle_count, paused_cycle, "pause 중에는 cycle이 증가하지 않아야 함")
            self.assertEqual(mock_pub.call_count, pub_count_at_pause, "pause 중에는 추가 MQTT 발행이 없어야 함")
            self.assertTrue(q.empty(), "pause 중에는 SSE 브로드캐스트가 없어야 함")

            # resume 호출
            resume_res = self.manager.resume()
            self.assertEqual(resume_res["status"], "resumed")

            # resume 후 발행되는 정확히 첫 번째 이벤트 수신
            first_event_after_resume = q.get(timeout=3.0)
            resumed_iso = first_event_after_resume["now_iso"]

            # pause 직전과 resume 후 첫 timestamp 비교 (+1초 연속성)
            from datetime import datetime
            dt_paused = datetime.fromisoformat(paused_iso.replace("Z", "+00:00"))
            dt_resumed = datetime.fromisoformat(resumed_iso.replace("Z", "+00:00"))
            diff_seconds = (dt_resumed - dt_paused).total_seconds()
            self.assertEqual(diff_seconds, 1.0, f"resume 후 첫 타임스탬프는 pause 직전의 정확히 +1초여야 함 (실제 차이: {diff_seconds}초)")

            self.manager.remove_subscriber(q)
            self.manager.stop()
            self.manager.reset()

    def test_14_natural_completion_retains_dates_and_status(self):
        """14. 자연 완료 후 GET /api/status에서 날짜, 시작시각, completed 상태, 메트릭 보존 검증"""
        with patch("aiomqtt.Client"), patch("simulator.publish_house_power", new_callable=AsyncMock):
            start_info = self.manager.start(
                households=[{"house": "H001", "scenario": "peak"}],
                simulation_date="2026-09-10"
            )
            expected_start_time = start_info["resolved_start_time"]

            # 59회로 설정하여 1틱 후 자연 완료 유도
            with self.manager.lock:
                self.manager.active_households["H001"]["cycle_count"] = 59
                self.manager.global_cycle_count = 59
                self.manager.cycle_count = 59

            time.sleep(2.0)

            # 1. Manager.get_status() 검증
            status = self.manager.get_status()
            self.assertFalse(status["is_running"], "자연 완료 후 is_running은 False여야 함")
            self.assertEqual(status["active_households"]["H001"]["status"], "completed", "자연 완료 가구는 completed 유지")
            self.assertEqual(status["simulation_date"], "2026-09-10", "완료 후에도 simulation_date 보존")
            self.assertEqual(status["resolved_start_time"], expected_start_time, "완료 후에도 resolved_start_time 보존")
            self.assertIn("H001", status["last_metrics_by_house"], "마지막 메트릭 보존")

            # 2. 실제 RequestHandler GET /api/status 검증
            handler = DummyRequestHandler(self.manager, method="GET", path="/api/status")
            handler.send_response = MagicMock()
            handler.send_header = MagicMock()
            handler.end_headers = MagicMock()

            handler.do_GET()
            handler.send_response.assert_called_with(200)
            body_json = handler.get_response_json()

            self.assertFalse(body_json["is_running"])
            self.assertEqual(body_json["simulation_date"], "2026-09-10", "HTTP 응답에서도 완료 후 simulation_date를 숨기지 않아야 함")
            self.assertEqual(body_json["resolved_start_time"], expected_start_time)
            self.assertEqual(body_json["active_households"]["H001"]["status"], "completed")

            # 3. reset 이후에만 초기화 확인
            self.manager.reset()
            status_after_reset = self.manager.get_status()
            self.assertIsNone(status_after_reset["simulation_date"])
            self.assertIsNone(status_after_reset["resolved_start_time"])
            self.assertEqual(status_after_reset["active_households"], {})
            self.assertEqual(status_after_reset["last_metrics_by_house"], {})

    def test_15_get_status_atomic_snapshot_and_deepcopy(self):
        """15. get_status의 deepcopy 불변성 및 tick 중 원자적 스냅샷 정합성 검증"""
        with patch("aiomqtt.Client"), patch("simulator.publish_house_power", new_callable=AsyncMock):
            self.manager.start(
                households=[
                    {"house": "H001", "scenario": "peak"},
                    {"house": "H002", "scenario": "random"}
                ],
                simulation_date="2026-09-10"
            )
            time.sleep(1.0)

            # deepcopy 검증: 반환된 객체를 외부에서 수정해도 Manager 내부 상태 불변
            status = self.manager.get_status()
            status["active_households"]["H001"]["status"] = "MUTATED"
            status["last_metrics_by_house"]["H001"] = {"mutated": True}

            status_fresh = self.manager.get_status()
            self.assertNotEqual(status_fresh["active_households"]["H001"]["status"], "MUTATED")
            self.assertNotIn("mutated", status_fresh["last_metrics_by_house"].get("H001", {}))

            # 워커 실행 중 50회 연속 get_status 호출 시 cycle 정합성(H001과 H002의 cycle 차이가 0) 검증
            for _ in range(50):
                st = self.manager.get_status()
                h1_c = st["active_households"].get("H001", {}).get("cycle_count", 0)
                h2_c = st["active_households"].get("H002", {}).get("cycle_count", 0)
                self.assertEqual(h1_c, h2_c, f"동일 틱에서 가구별 cycle_count가 어긋나서는 안 됨 (H001: {h1_c}, H002: {h2_c})")
                self.assertEqual(st["global_cycle_count"], h1_c, "global_cycle_count와 가구별 cycle_count가 일치해야 함")
                time.sleep(0.02)

            self.manager.stop()
            self.manager.reset()

    def test_11_node_ui_real_js_suite(self):
        """11. 운영 waveform_viewer.html의 실제 JS를 Node.js로 실행하는 테스트 스위트 연동"""
        test_script_path = os.path.join(TESTS_DIR, "test_ui_real_js.js")
        proc = subprocess.run(
            ["node", test_script_path],
            capture_output=True,
            text=True,
            encoding="utf-8",
            cwd=SIMULATOR_DIR
        )
        if proc.returncode != 0:
            print("Node.js Test STDOUT:\n", proc.stdout)
            print("Node.js Test STDERR:\n", proc.stderr)
        self.assertEqual(proc.returncode, 0, f"Node.js UI 테스트 실패: {proc.stderr}")


class TestWebUiBehavior(unittest.TestCase):
    """웹 실행 UX 검증 (규칙 4)"""

    def setUp(self):
        html_path = os.path.join(SIMULATOR_DIR, "waveform_viewer.html")
        with open(html_path, "r", encoding="utf-8") as f:
            self.html_content = f.read()
        assets_dir = os.path.join(SIMULATOR_DIR, "assets")
        with open(os.path.join(assets_dir, "controls.js"), "r", encoding="utf-8") as f:
            self.controls_js = f.read()
        with open(os.path.join(assets_dir, "households.js"), "r", encoding="utf-8") as f:
            self.households_js = f.read()

    def test_no_local_fallback_on_api_failure(self):
        """다중 시작 API 실패 시 브라우저가 로컬 모드로 자동 폴백하지 않는지 검증 (규칙 4)"""
        self.assertIn("startMultiSimulation", self.controls_js)
        fn_start = self.controls_js.find("async function startMultiSimulation")
        fn_end = self.controls_js.find("function startPeakDemo", fn_start)
        fn_body = self.controls_js[fn_start:fn_end]

        self.assertNotIn("startLocalSimulation", fn_body, "다중 시작 API 실패 시 로컬 모드로 폴백하면 안 됨")
        self.assertIn("showNoticeError(", fn_body, "API 실패 시 사용자에게 오류 알림을 표시해야 함")

    def test_observed_house_dropdown_retains_completed_households(self):
        """관찰 가구 드롭다운에는 running뿐 아니라 completed 가구도 유지 (규칙 4)"""
        self.assertIn("ensureHouseInObservedSelect", self.households_js)
        self.assertIn("observedHouseSelect", self.html_content)
        self.assertIn("updateHouseholdTableRow", self.households_js)
        self.assertIn("completed", self.households_js)

    def test_single_start_button_and_presets(self):
        """실제 실행 버튼은 '설정한 가구 실행' 하나로 통일, 기존 버튼은 프리셋 (규칙 4)"""
        self.assertIn('id="btnStartSimulation"', self.html_content)
        self.assertIn("설정한 가구 실행", self.html_content)
        self.assertIn("applyPreset", self.html_content)


if __name__ == "__main__":
    unittest.main(verbosity=2)
