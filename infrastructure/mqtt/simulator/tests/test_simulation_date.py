import io
import json
import os
import sys
import unittest
from datetime import datetime, date, timedelta, timezone
from unittest.mock import MagicMock, AsyncMock, patch

# 상위 시뮬레이터 디렉터리 import 경로 등록
TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SIMULATOR_DIR = os.path.dirname(TESTS_DIR)
if SIMULATOR_DIR not in sys.path:
    sys.path.insert(0, SIMULATOR_DIR)

import scenarios
from scenarios import (
    KST,
    parse_simulation_date,
    resolve_simulation_start_time,
    format_iso_utc,
)
import simulator
from server.manager import SimulatorManager
from server.request_handler import RequestHandler


class TestSimulationDateParsing(unittest.TestCase):
    """1. YYYY-MM-DD 날짜 파싱 및 유효성 검증 테스트"""

    def test_valid_dates(self):
        self.assertEqual(parse_simulation_date("2026-09-10"), "2026-09-10")
        self.assertEqual(parse_simulation_date("2025-01-01"), "2025-01-01")
        self.assertEqual(parse_simulation_date("2026-12-31"), "2026-12-31")

    def test_leap_year(self):
        # 2024년은 윤년 (2월 29일 존재)
        self.assertEqual(parse_simulation_date("2024-02-29"), "2024-02-29")
        # 2023년은 평년 (2월 29일 없음) -> ValueError
        with self.assertRaises(ValueError):
            parse_simulation_date("2023-02-29")

    def test_nonexistent_dates_rejected(self):
        invalid_dates = [
            "2026-02-30",
            "2026-04-31",
            "2026-13-01",
            "2026-00-10",
            "2026-01-32",
        ]
        for d in invalid_dates:
            with self.subTest(date=d):
                with self.assertRaises(ValueError) as ctx:
                    parse_simulation_date(d)
                self.assertIn("simulation_date", str(ctx.exception))

    def test_invalid_formats_and_whitespace_rejected(self):
        # 엄격한 YYYY-MM-DD 검증: 공백 포함 값도 거절되어야 함
        invalid_formats = [
            " 2026-09-10",
            "2026-09-10 ",
            " 2026-09-10 ",
            "2026-9-1",
            "2026-9-10",
            "2026/09/10",
            "2026.09.10",
            "20260910",
            "2026-09-10T12:00:00",
            "hello",
            "",
            "   ",
        ]
        for f in invalid_formats:
            with self.subTest(format=f):
                with self.assertRaises(ValueError) as ctx:
                    parse_simulation_date(f)
                self.assertIn("simulation_date", str(ctx.exception))

    def test_non_string_types_rejected(self):
        invalid_types = [12345, 20260910, ["2026-09-10"], {"date": "2026-09-10"}, None, True, False]
        for val in invalid_types:
            with self.subTest(val=val):
                with self.assertRaises(ValueError) as ctx:
                    parse_simulation_date(val)
                self.assertIn("simulation_date는 YYYY-MM-DD 형식의 문자열이어야 합니다", str(ctx.exception))


class TestVirtualStartTimeResolution(unittest.TestCase):
    """2. 시나리오별 가상 시작 시각 및 --start-time 우선순위 결정 규칙 테스트"""

    def setUp(self):
        # 고정된 기준 현재 시각: 2026-09-14 14:30:15 KST
        self.fixed_now = datetime(2026, 9, 14, 14, 30, 15, tzinfo=KST)

    def test_01_routine_missed_with_time_only_start_time(self):
        # 1. routine_missed + start_time_str="09:00:00" -> 오늘 날짜 09:00:00 KST
        res = resolve_simulation_start_time(
            "routine_missed",
            simulation_date=None,
            start_time_str="09:00:00",
            now=self.fixed_now
        )
        expected = datetime(2026, 9, 14, 9, 0, 0, tzinfo=KST)
        self.assertEqual(res, expected)

    def test_02_random_with_simulation_date_and_time_only_start_time(self):
        # 2. random + simulation_date="2026-09-10" + start_time_str="09:00:00" -> 2026-09-10 09:00:00 KST
        res = resolve_simulation_start_time(
            "random",
            simulation_date="2026-09-10",
            start_time_str="09:00:00",
            now=self.fixed_now
        )
        expected = datetime(2026, 9, 10, 9, 0, 0, tzinfo=KST)
        self.assertEqual(res, expected)

    def test_03_routine_missed_with_simulation_date_and_time_only_start_time(self):
        # 3. routine_missed + simulation_date="2026-09-10" + start_time_str="09:00:00"
        #    -> 명시적 시작 시간이 시나리오 기본 시간(08:10:01)보다 우선해야 함 -> 2026-09-10 09:00:00 KST
        res = resolve_simulation_start_time(
            "routine_missed",
            simulation_date="2026-09-10",
            start_time_str="09:00:00",
            now=self.fixed_now,
            routine_default_time="08:10:01"
        )
        expected = datetime(2026, 9, 10, 9, 0, 0, tzinfo=KST)
        self.assertEqual(res, expected)

    def test_04_full_iso_datetime_start_time_without_date(self):
        # 4. 완전한 ISO datetime start_time만 지정 -> 기존 날짜와 시간이 유지되는지 검증
        iso_str = "2026-10-01T15:30:00"
        res = resolve_simulation_start_time(
            "routine_missed",
            simulation_date=None,
            start_time_str=iso_str,
            now=self.fixed_now
        )
        expected = datetime(2026, 10, 1, 15, 30, 0, tzinfo=KST)
        self.assertEqual(res, expected)

        # timezone offset 포함 ISO
        iso_with_tz = "2026-10-01T06:30:00+00:00"
        res_tz = resolve_simulation_start_time(
            "peak",
            simulation_date=None,
            start_time_str=iso_with_tz,
            now=self.fixed_now
        )
        expected_tz = datetime(2026, 10, 1, 6, 30, 0, tzinfo=timezone.utc)
        self.assertEqual(res_tz, expected_tz)

    def test_05_date_and_full_iso_datetime_conflict_raises_error(self):
        # 5. --date와 완전한 ISO datetime start_time 동시 사용 -> 명확한 ValueError 발생
        with self.assertRaises(ValueError) as ctx:
            resolve_simulation_start_time(
                "routine_missed",
                simulation_date="2026-09-10",
                start_time_str="2026-09-10T09:00:00",
                now=self.fixed_now
            )
        self.assertIn("--date와 완전한 ISO --start-time을 함께 사용할 수 없습니다", str(ctx.exception))

    def test_06_cli_routine_missed_default_time_08_15_00(self):
        # 6. CLI routine_missed에서 아무 날짜/시간도 지정하지 않음 -> CLI 기본값 오늘 08:15:00 KST
        res = resolve_simulation_start_time(
            "routine_missed",
            simulation_date=None,
            start_time_str=None,
            now=self.fixed_now,
            routine_default_time="08:15:00"
        )
        expected = datetime(2026, 9, 14, 8, 15, 0, tzinfo=KST)
        self.assertEqual(res, expected)

    def test_07_web_routine_missed_default_time_08_10_01(self):
        # 7. 웹 Manager routine_missed에서 날짜를 지정하지 않음 -> 웹 기본값 오늘 08:10:01 KST
        res = resolve_simulation_start_time(
            "routine_missed",
            simulation_date=None,
            start_time_str=None,
            now=self.fixed_now,
            routine_default_time="08:10:01"
        )
        expected = datetime(2026, 9, 14, 8, 10, 1, tzinfo=KST)
        self.assertEqual(res, expected)

    def test_08_routine_missed_with_simulation_date_uses_routine_default(self):
        # simulation_date만 지정 시: routine_default_time(08:10:01) 적용
        res = resolve_simulation_start_time(
            "routine_missed",
            simulation_date="2026-09-10",
            now=self.fixed_now,
            routine_default_time="08:10:01"
        )
        expected = datetime(2026, 9, 10, 8, 10, 1, tzinfo=KST)
        self.assertEqual(res, expected)

    def test_09_other_scenarios_with_simulation_date_combines_with_current_time(self):
        # peak, random, manual + simulation_date -> 선택 날짜 + 현재 시각(14:30:15)
        for sc in ["peak", "random", "manual"]:
            with self.subTest(scenario=sc):
                res = resolve_simulation_start_time(sc, simulation_date="2026-09-10", now=self.fixed_now)
                expected = datetime(2026, 9, 10, 14, 30, 15, tzinfo=KST)
                self.assertEqual(res, expected)

    def test_10_other_scenarios_without_simulation_date_returns_none(self):
        # 날짜 생략 시: peak, random, manual은 None 반환 (실시간 wall-clock 모드 유지)
        for sc in ["peak", "random", "manual"]:
            with self.subTest(scenario=sc):
                res = resolve_simulation_start_time(sc, simulation_date=None, now=self.fixed_now)
                self.assertIsNone(res)


class TestTimestampFormattingAndProgression(unittest.TestCase):
    """3. KST -> UTC 변환, 1초 증가 및 자정 넘김 테스트"""

    def test_kst_to_utc_format(self):
        dt_kst = datetime(2026, 9, 10, 14, 30, 15, tzinfo=KST)
        # 14:30:15 KST = 05:30:15 UTC
        formatted = format_iso_utc(dt_kst)
        self.assertEqual(formatted, "2026-09-10T05:30:15.000Z")

        # 08:10:01 KST = 전날 23:10:01 UTC
        dt_morning = datetime(2026, 9, 10, 8, 10, 1, tzinfo=KST)
        formatted_morning = format_iso_utc(dt_morning)
        self.assertEqual(formatted_morning, "2026-09-09T23:10:01.000Z")

    def test_exact_one_second_progression(self):
        base_dt = datetime(2026, 9, 10, 14, 30, 15, tzinfo=KST)
        timestamps = [format_iso_utc(base_dt + timedelta(seconds=sec)) for sec in range(5)]
        expected = [
            "2026-09-10T05:30:15.000Z",
            "2026-09-10T05:30:16.000Z",
            "2026-09-10T05:30:17.000Z",
            "2026-09-10T05:30:18.000Z",
            "2026-09-10T05:30:19.000Z",
        ]
        self.assertEqual(timestamps, expected)

    def test_midnight_rollover(self):
        # 2026-09-10 23:59:58 KST부터 4초간 진행
        base_dt = datetime(2026, 9, 10, 23, 59, 58, tzinfo=KST)
        dt_plus_0 = base_dt + timedelta(seconds=0)
        dt_plus_1 = base_dt + timedelta(seconds=1)
        dt_plus_2 = base_dt + timedelta(seconds=2)  # 2026-09-11 00:00:00 KST
        dt_plus_3 = base_dt + timedelta(seconds=3)  # 2026-09-11 00:00:01 KST

        self.assertEqual(dt_plus_0.strftime("%Y-%m-%d"), "2026-09-10")
        self.assertEqual(dt_plus_1.strftime("%Y-%m-%d"), "2026-09-10")
        self.assertEqual(dt_plus_2.strftime("%Y-%m-%d"), "2026-09-11")
        self.assertEqual(dt_plus_3.strftime("%Y-%m-%d"), "2026-09-11")

        self.assertEqual(format_iso_utc(dt_plus_2), "2026-09-10T15:00:00.000Z")
        self.assertEqual(format_iso_utc(dt_plus_3), "2026-09-10T15:00:01.000Z")


class TestPayloadConsistency(unittest.IsolatedAsyncioTestCase):
    """4. measured_at, ts, now_iso 일치성 테스트"""

    async def test_publisher_timestamp_consistency(self):
        fake_client = AsyncMock()
        target_iso = "2026-09-10T05:30:15.000Z"
        res = await simulator.publish_house_power(
            client=fake_client,
            house="H001",
            now_iso=target_iso,
            qos=1,
            allow_random=False
        )
        fake_client.publish.assert_called_once()
        topic, payload_str = fake_client.publish.call_args[0]
        self.assertEqual(topic, "v1/power/sim/H001/main")

        payload = json.loads(payload_str)
        self.assertEqual(payload["measured_at"], target_iso)
        self.assertEqual(payload["ts"], target_iso)
        self.assertEqual(payload["measured_at"], payload["ts"])


class DummyRequestHandler(RequestHandler):
    """HTTP 요청 파싱 및 라우팅 테스트를 위한 Mock RequestHandler"""
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


class TestApiStartValidation(unittest.TestCase):
    """5. /api/start 및 /api/status 엔드포인트 검증 테스트"""

    def setUp(self):
        self.mock_manager = MagicMock(spec=SimulatorManager)
        self.mock_manager.is_running = False
        self.mock_manager.current_mode = "idle"
        self.mock_manager.cycle_count = 0
        self.mock_manager.last_metrics = None
        self.mock_manager.simulation_date = None
        self.mock_manager.resolved_start_time = None
        self.mock_manager.get_connection_config.return_value = {
            "host": "localhost",
            "port": 1883,
            "tls_enabled": False
        }

    def test_start_with_invalid_date_returns_400(self):
        invalid_payloads = [
            {"scenario": "peak", "simulation_date": "2026-02-30"},
            {"scenario": "peak", "simulation_date": "2026-9-1"},
            {"scenario": "peak", "simulation_date": " 2026-09-10"},
            {"scenario": "peak", "simulation_date": "2026-09-10 "},
            {"scenario": "peak", "simulation_date": 12345},
            {"scenario": "peak", "simulation_date": ["2026-09-10"]},
            {"scenario": "peak", "simulation_date": "invalid-str"},
        ]

        for payload in invalid_payloads:
            with self.subTest(payload=payload):
                handler = DummyRequestHandler(self.mock_manager, method="POST", path="/api/start", body_dict=payload)
                handler.send_response = MagicMock()
                handler.send_header = MagicMock()
                handler.end_headers = MagicMock()

                handler.do_POST()
                # 400 Bad Request 확인
                handler.send_response.assert_called_with(400)
                resp = handler.get_response_json()
                self.assertEqual(resp.get("status"), "error")
                self.assertEqual(resp.get("code"), "BAD_REQUEST")
                self.assertIn("simulation_date", resp.get("message", ""))
                # manager.start는 호출되지 않아야 함
                self.mock_manager.start.assert_not_called()

    def test_start_with_valid_date_calls_manager_and_returns_200(self):
        self.mock_manager.start.return_value = {
            "status": "started",
            "scenario": "routine_missed",
            "house": "H001",
            "simulation_date": "2026-09-10",
            "resolved_start_time": "2026-09-09T23:10:01.000Z"
        }

        payload = {"scenario": "routine_missed", "house": "H001", "simulation_date": "2026-09-10"}
        handler = DummyRequestHandler(self.mock_manager, method="POST", path="/api/start", body_dict=payload)
        handler.send_response = MagicMock()
        handler.send_header = MagicMock()
        handler.end_headers = MagicMock()

        handler.do_POST()
        handler.send_response.assert_called_with(200)
        self.mock_manager.start.assert_called_once_with(
            scenario="routine_missed",
            house="H001",
            simulation_date="2026-09-10"
        )
        resp = handler.get_response_json()
        self.assertEqual(resp.get("status"), "started")
        self.assertEqual(resp.get("simulation_date"), "2026-09-10")
        self.assertEqual(resp.get("resolved_start_time"), "2026-09-09T23:10:01.000Z")

    def test_start_without_date_preserves_backwards_compatibility(self):
        self.mock_manager.start.return_value = {
            "status": "started",
            "scenario": "peak",
            "house": "H001",
            "simulation_date": None,
            "resolved_start_time": None
        }

        payload = {"scenario": "peak", "house": "H001"}
        handler = DummyRequestHandler(self.mock_manager, method="POST", path="/api/start", body_dict=payload)
        handler.send_response = MagicMock()
        handler.send_header = MagicMock()
        handler.end_headers = MagicMock()

        handler.do_POST()
        handler.send_response.assert_called_with(200)
        self.mock_manager.start.assert_called_once_with(
            scenario="peak",
            house="H001",
            simulation_date=None
        )

    def test_status_endpoint_reflects_simulation_date(self):
        self.mock_manager.get_status.return_value = {
            "is_running": True,
            "is_paused": False,
            "current_mode": "routine_missed",
            "scenario": "routine_missed",
            "house": "H001",
            "cycle_count": 0,
            "global_cycle_count": 0,
            "active_households": {"H001": {"scenario": "routine_missed", "status": "running", "cycle_count": 0}},
            "last_metrics_by_house": {},
            "last_metrics": None,
            "simulation_date": "2026-09-10",
            "resolved_start_time": "2026-09-09T23:10:01.000Z",
        }

        handler = DummyRequestHandler(self.mock_manager, method="GET", path="/api/status")
        handler.send_response = MagicMock()
        handler.send_header = MagicMock()
        handler.end_headers = MagicMock()

        handler.do_GET()
        handler.send_response.assert_called_with(200)
        resp = handler.get_response_json()
        self.assertEqual(resp.get("simulation_date"), "2026-09-10")
        self.assertEqual(resp.get("resolved_start_time"), "2026-09-09T23:10:01.000Z")

        # 정지(is_running: False) 후에도 다음 실행/reset 전까지 날짜 및 시작 시각 보존 확인
        self.mock_manager.get_status.return_value["is_running"] = False
        handler2 = DummyRequestHandler(self.mock_manager, method="GET", path="/api/status")
        handler2.send_response = MagicMock()
        handler2.send_header = MagicMock()
        handler2.end_headers = MagicMock()

        handler2.do_GET()
        resp2 = handler2.get_response_json()
        self.assertEqual(resp2.get("simulation_date"), "2026-09-10")
        self.assertEqual(resp2.get("resolved_start_time"), "2026-09-09T23:10:01.000Z")

        # reset 시에만 None 확인
        self.mock_manager.get_status.return_value["simulation_date"] = None
        self.mock_manager.get_status.return_value["resolved_start_time"] = None
        handler3 = DummyRequestHandler(self.mock_manager, method="GET", path="/api/status")
        handler3.send_response = MagicMock()
        handler3.send_header = MagicMock()
        handler3.end_headers = MagicMock()

        handler3.do_GET()
        resp3 = handler3.get_response_json()
        self.assertIsNone(resp3.get("simulation_date"))
        self.assertIsNone(resp3.get("resolved_start_time"))


class TestManagerDateIntegration(unittest.TestCase):
    """6. SimulatorManager가 날짜를 전달받아 실제 worker 타임스탬프를 올바르게 적용하는지 검증"""

    @patch("aiomqtt.Client")
    def test_manager_applies_simulation_date(self, mock_client_cls):
        manager = SimulatorManager(host="localhost", port=1883, tls_enabled=False)
        result = manager.start(scenario="routine_missed", house="H001", simulation_date="2026-09-10")

        self.assertEqual(result["status"], "started")
        self.assertEqual(result["simulation_date"], "2026-09-10")
        self.assertEqual(result["resolved_start_time"], "2026-09-09T23:10:01.000Z")

        self.assertEqual(manager.simulation_date, "2026-09-10")
        self.assertEqual(manager.resolved_start_time, "2026-09-09T23:10:01.000Z")

        manager.stop()
        # stop 후에도 날짜 및 시작 시각 보존 검증
        self.assertEqual(manager.simulation_date, "2026-09-10")
        self.assertEqual(manager.resolved_start_time, "2026-09-09T23:10:01.000Z")

        # reset 시에만 초기화 검증
        manager.reset()
        self.assertIsNone(manager.simulation_date)
        self.assertIsNone(manager.resolved_start_time)


if __name__ == "__main__":
    unittest.main()
