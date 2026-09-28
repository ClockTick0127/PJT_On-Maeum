"""
NILM 시뮬레이터 배속(Speed Multiplier) 기능 단위 및 통합 테스트 모듈

요구사항 검증 항목:
1. /api/start에 interval=0.1 전달 시 manager.start()로 전달
2. interval 미전달 시 1.0초 적용
3. /api/speed의 interval 형식 성공
4. /api/speed의 speed 형식 성공
5. interval과 speed 동시 전달 시 400
6. 두 필드 모두 없으면 400
7. 알 수 없는 필드가 있으면 400
8. 문자열, null, bool, 0, 음수, NaN, Infinity 거절
9. 0.1 미만 및 10.0 초과 거절
10. 실행 중이 아닐 때 /api/speed 호출 시 409
11. manager.set_interval()이 실행 중 원자적으로 값을 변경
12. Pause 중 변경 후 Resume 시 새 배속 적용
13. Reset 후 interval=1.0
14. interval 없는 새 실행은 이전 배속을 상속하지 않고 1.0
15. 잘못된 start interval이 기존 실행 워커를 중지하지 않음
16. 1x -> 10x 및 10x -> 1x 변경 시 다음 사이클 주기 변경 (결정론적 검증)
17. 배속 변경 전후 measured_at/ts/now_iso가 정확히 +1초
18. Pause/Resume 가상 시각 연속성 회귀 검증
19. 다중 가구 동일 tick timestamp 일치
20. 단일 워커 및 Reset 503 기존 회귀 테스트 유지
"""

import asyncio
from datetime import datetime, timezone, timedelta
import io
import json
import math
import os
import sys
import threading
import time
import unittest
from unittest.mock import MagicMock, patch, AsyncMock

# 상위 디렉터리 import 경로 등록
PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PARENT_DIR not in sys.path:
    sys.path.append(PARENT_DIR)

from server.config import validate_interval, MIN_INTERVAL, MAX_INTERVAL, DEFAULT_INTERVAL
from server.manager import SimulatorManager, ModeConflictError
from server.request_handler import RequestHandler
import scenarios
import simulator


class DummyRequestHandler(RequestHandler):
    """HTTP 요청 파싱 및 라우팅 테스트용 Mock RequestHandler"""
    def __init__(self, manager, method="POST", path="/api/speed", body_dict=None):
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


class TestSpeedMultiplierApi(unittest.TestCase):
    """REST API 엔드포인트 (/api/start, /api/speed) 배속 검증 테스트"""

    def setUp(self):
        self.mock_manager = MagicMock(spec=SimulatorManager)
        self.mock_manager.is_running = True
        self.mock_manager.interval = 1.0
        self.mock_manager.set_interval.return_value = {
            "status": "speed_updated",
            "interval": 0.2,
            "speed": 5.0
        }

    def test_01_api_start_passes_interval(self):
        """1. /api/start에 interval=0.1 전달 시 manager.start()로 전달 검증"""
        self.mock_manager.start.return_value = {
            "status": "started",
            "scenario": "peak",
            "house": "H001",
            "households": [{"house": "H001", "scenario": "peak"}],
            "interval": 0.1,
            "speed": 10.0
        }
        handler = DummyRequestHandler(
            self.mock_manager,
            method="POST",
            path="/api/start",
            body_dict={"scenario": "peak", "house": "H001", "interval": 0.1}
        )
        handler.send_response = MagicMock()
        handler.send_header = MagicMock()
        handler.end_headers = MagicMock()

        handler.do_POST()

        handler.send_response.assert_called_with(200)
        self.mock_manager.start.assert_called_once_with(
            scenario="peak",
            house="H001",
            simulation_date=None,
            interval=0.1
        )
        resp = handler.get_response_json()
        self.assertEqual(resp["interval"], 0.1)
        self.assertEqual(resp["speed"], 10.0)

    def test_02_api_start_omitted_interval_defaults(self):
        """2. /api/start에 interval 미전달 시 None(기본 1.0초) 전달 검증"""
        self.mock_manager.start.return_value = {
            "status": "started",
            "scenario": "peak",
            "house": "H001",
            "households": [{"house": "H001", "scenario": "peak"}],
            "interval": 1.0,
            "speed": 1.0
        }
        handler = DummyRequestHandler(
            self.mock_manager,
            method="POST",
            path="/api/start",
            body_dict={"scenario": "peak", "house": "H001"}
        )
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

    def test_03_api_speed_interval_format_success(self):
        """3. /api/speed의 interval 형식 성공 검증"""
        self.mock_manager.set_interval.return_value = {
            "status": "speed_updated",
            "interval": 0.2,
            "speed": 5.0
        }
        handler = DummyRequestHandler(
            self.mock_manager,
            method="POST",
            path="/api/speed",
            body_dict={"interval": 0.2}
        )
        handler.send_response = MagicMock()
        handler.send_header = MagicMock()
        handler.end_headers = MagicMock()

        handler.do_POST()

        handler.send_response.assert_called_with(200)
        self.mock_manager.set_interval.assert_called_once_with(0.2)
        resp = handler.get_response_json()
        self.assertEqual(resp["status"], "speed_updated")
        self.assertEqual(resp["interval"], 0.2)
        self.assertEqual(resp["speed"], 5.0)

    def test_04_api_speed_speed_format_success(self):
        """4. /api/speed의 speed 형식 성공 및 interval 변환 검증"""
        self.mock_manager.set_interval.return_value = {
            "status": "speed_updated",
            "interval": 0.2,
            "speed": 5.0
        }
        handler = DummyRequestHandler(
            self.mock_manager,
            method="POST",
            path="/api/speed",
            body_dict={"speed": 5}
        )
        handler.send_response = MagicMock()
        handler.send_header = MagicMock()
        handler.end_headers = MagicMock()

        handler.do_POST()

        handler.send_response.assert_called_with(200)
        # speed: 5 -> interval = 1.0 / 5 = 0.2
        self.mock_manager.set_interval.assert_called_once_with(0.2)
        resp = handler.get_response_json()
        self.assertEqual(resp["status"], "speed_updated")
        self.assertEqual(resp["interval"], 0.2)
        self.assertEqual(resp["speed"], 5.0)

    def test_05_api_speed_both_fields_rejected_400(self):
        """5. interval과 speed 동시 전달 시 400 거절 검증"""
        handler = DummyRequestHandler(
            self.mock_manager,
            method="POST",
            path="/api/speed",
            body_dict={"interval": 0.2, "speed": 5}
        )
        handler.send_response = MagicMock()
        handler.send_header = MagicMock()
        handler.end_headers = MagicMock()

        handler.do_POST()

        handler.send_response.assert_called_with(400)
        self.mock_manager.set_interval.assert_not_called()
        resp = handler.get_response_json()
        self.assertEqual(resp["code"], "BAD_REQUEST")

    def test_06_api_speed_neither_field_rejected_400(self):
        """6. 두 필드 모두 없으면 400 거절 검증"""
        handler = DummyRequestHandler(
            self.mock_manager,
            method="POST",
            path="/api/speed",
            body_dict={}
        )
        handler.send_response = MagicMock()
        handler.send_header = MagicMock()
        handler.end_headers = MagicMock()

        handler.do_POST()

        handler.send_response.assert_called_with(400)
        self.mock_manager.set_interval.assert_not_called()
        resp = handler.get_response_json()
        self.assertEqual(resp["code"], "BAD_REQUEST")

    def test_07_api_speed_unknown_fields_rejected_400(self):
        """7. 알 수 없는 필드가 있으면 400 거절 검증"""
        handler = DummyRequestHandler(
            self.mock_manager,
            method="POST",
            path="/api/speed",
            body_dict={"interval": 0.2, "extra": "invalid"}
        )
        handler.send_response = MagicMock()
        handler.send_header = MagicMock()
        handler.end_headers = MagicMock()

        handler.do_POST()

        handler.send_response.assert_called_with(400)
        self.mock_manager.set_interval.assert_not_called()

    def test_08_api_speed_invalid_types_rejected_400(self):
        """8. 문자열, null, bool, 0, 음수, NaN, Infinity 거절 검증"""
        invalid_bodies = [
            {"interval": "fast"},
            {"interval": True},   # bool은 int의 서브클래스이므로 엄격 방어 필수
            {"interval": False},
            {"interval": None},
            {"interval": 0},
            {"interval": -1.0},
            {"interval": float("nan")},
            {"interval": float("inf")},
            {"speed": "fast"},
            {"speed": True},
            {"speed": False},
            {"speed": None},
            {"speed": 0},
            {"speed": -5},
            {"speed": float("nan")},
            {"speed": float("inf")},
        ]
        for body in invalid_bodies:
            with self.subTest(body=body):
                handler = DummyRequestHandler(
                    self.mock_manager,
                    method="POST",
                    path="/api/speed",
                    body_dict=body
                )
                handler.send_response = MagicMock()
                handler.send_header = MagicMock()
                handler.end_headers = MagicMock()

                handler.do_POST()

                handler.send_response.assert_called_with(400)
                self.mock_manager.set_interval.assert_not_called()

    def test_09_api_speed_out_of_range_rejected_400(self):
        """9. 0.1 미만 및 10.0 초과 거절 검증"""
        out_of_range = [
            {"interval": 0.09},
            {"interval": 0.05},
            {"interval": 10.1},
            {"interval": 20.0},
            {"speed": 11.0},   # interval = 1/11 = 0.0909 < 0.1
            {"speed": 20.0},   # interval = 1/20 = 0.05 < 0.1
            {"speed": 0.05},   # interval = 1/0.05 = 20.0 > 10.0
        ]
        for body in out_of_range:
            with self.subTest(body=body):
                handler = DummyRequestHandler(
                    self.mock_manager,
                    method="POST",
                    path="/api/speed",
                    body_dict=body
                )
                handler.send_response = MagicMock()
                handler.send_header = MagicMock()
                handler.end_headers = MagicMock()

                handler.do_POST()

                handler.send_response.assert_called_with(400)
                self.mock_manager.set_interval.assert_not_called()

    def test_10_api_speed_when_not_running_returns_409(self):
        """10. 실행 중이 아닐 때 /api/speed 호출 시 409 거절 검증"""
        self.mock_manager.set_interval.side_effect = ModeConflictError("시뮬레이터가 실행 중이 아닙니다.")
        handler = DummyRequestHandler(
            self.mock_manager,
            method="POST",
            path="/api/speed",
            body_dict={"interval": 0.2}
        )
        handler.send_response = MagicMock()
        handler.send_header = MagicMock()
        handler.end_headers = MagicMock()

        handler.do_POST()

        handler.send_response.assert_called_with(409)
        resp = handler.get_response_json()
        self.assertEqual(resp["code"], "INVALID_MODE")


class TestSpeedMultiplierManager(unittest.TestCase):
    """SimulatorManager 배속 및 동시성·동적 스케줄링 검증 테스트"""

    def setUp(self):
        self.manager = SimulatorManager(host="localhost", port=1883, tls_enabled=False)

    def tearDown(self):
        self.manager.stop()

    def test_11_manager_set_interval_atomic_update(self):
        """11. manager.set_interval()이 실행 중 원자적으로 값을 변경하는지 검증"""
        # 정지 상태에서는 ModeConflictError
        with self.assertRaises(ModeConflictError):
            self.manager.set_interval(0.2)

        # 가상 실행 상태 모킹
        with self.manager.lock:
            self.manager.is_running = True

        res = self.manager.set_interval(0.2)
        self.assertEqual(res["status"], "speed_updated")
        self.assertEqual(res["interval"], 0.2)
        self.assertEqual(res["speed"], 5.0)
        self.assertEqual(self.manager.interval, 0.2)

        status = self.manager.get_status()
        self.assertEqual(status["interval"], 0.2)
        self.assertEqual(status["speed"], 5.0)

    def test_12_pause_state_speed_change_allowed(self):
        """12. Pause 중 변경 허용 및 Resume 후 새 배속 적용 검증"""
        with self.manager.lock:
            self.manager.is_running = True
            self.manager.is_paused = True

        res = self.manager.set_interval(0.1)
        self.assertEqual(res["interval"], 0.1)
        self.assertEqual(self.manager.interval, 0.1)

        # resume 호출 후에도 interval 유지
        self.manager.resume()
        self.assertEqual(self.manager.interval, 0.1)
        self.assertFalse(self.manager.is_paused)

    def test_13_reset_restores_interval_to_1_0(self):
        """13. Reset 후 interval=1.0으로 초기화 검증"""
        with self.manager.lock:
            self.manager.interval = 0.1

        self.manager.reset()
        self.assertEqual(self.manager.interval, 1.0)
        self.assertEqual(self.manager.get_status()["interval"], 1.0)
        self.assertEqual(self.manager.get_status()["speed"], 1.0)

    @patch("aiomqtt.Client")
    def test_14_new_execution_without_interval_does_not_inherit_previous_speed(self, mock_client_cls):
        """14. interval 없는 새 실행은 이전 배속을 상속하지 않고 1.0초로 시작 검증"""
        mock_client = AsyncMock()
        mock_client.__aenter__.return_value = mock_client
        mock_client_cls.return_value = mock_client

        # 1. interval=0.1로 시작
        res1 = self.manager.start(scenario="peak", house="H001", interval=0.1)
        self.assertEqual(res1["interval"], 0.1)
        self.assertEqual(self.manager.interval, 0.1)
        self.manager.stop()

        # 2. interval 없이 레거시 시작 호출 -> 1.0으로 시작해야 함
        res2 = self.manager.start(scenario="peak", house="H001")
        self.assertEqual(res2["interval"], 1.0)
        self.assertEqual(res2["speed"], 1.0)
        self.assertEqual(self.manager.interval, 1.0)
        self.manager.stop()

    @patch("aiomqtt.Client")
    def test_15_invalid_start_interval_does_not_stop_running_worker(self, mock_client_cls):
        """15. 잘못된 start interval이 기존 실행 워커를 중지하지 않음 검증"""
        mock_client = AsyncMock()
        mock_client.__aenter__.return_value = mock_client
        mock_client_cls.return_value = mock_client

        # 정상 시작
        self.manager.start(scenario="peak", house="H001", interval=1.0)
        orig_worker = self.manager.worker_thread
        self.assertTrue(self.manager.is_running)

        # 잘못된 interval로 재시작 시도 (0.05 < 0.1)
        with self.assertRaises(ValueError):
            self.manager.start(scenario="peak", house="H001", interval=0.05)

        # 기존 워커가 여전히 실행 중이어야 함
        self.assertTrue(self.manager.is_running)
        self.assertIs(self.manager.worker_thread, orig_worker)
        self.manager.stop()

    def test_16_dynamic_worker_cycle_timing_adjustment(self):
        """16. 1x -> 10x 및 10x -> 1x 변경 시 다음 사이클 주기 변경 (운영 _sleep_until_next_cycle 검증)"""
        stop_event = threading.Event()

        # Case 1: 1.0초(1x) 대기 중 100ms 시점에 0.1초(10x)로 변경 -> 잔여 시간이 0 이하가 되어 즉시 루프 탈출
        fake_time = [100.0]
        sleep_durations = []

        async def fake_sleep_accelerate(duration):
            sleep_durations.append(duration)
            fake_time[0] += duration
            # 100ms 경과(2회 50ms sleep) 시점에 10배속(0.1s)으로 변경
            if len(sleep_durations) == 2:
                self.manager.interval = 0.1

        with patch("time.monotonic", side_effect=lambda: fake_time[0]), \
             patch("asyncio.sleep", side_effect=fake_sleep_accelerate):
            self.manager.interval = 1.0
            self.manager.is_paused = False
            asyncio.run(self.manager._sleep_until_next_cycle(cycle_started=100.0, stop_event=stop_event))

        # 원래 1.0초 대기였다면 20회의 50ms sleep(총 1.0초)이 발생해야 하나,
        # 10배속 변경 즉시 감지되어 2회(0.1초) 만에 루프를 종료하고 다음 사이클을 앞당김
        self.assertEqual(len(sleep_durations), 2)
        self.assertAlmostEqual(sum(sleep_durations), 0.1, places=3)

        # Case 2: 0.1초(10x) 대기 중 50ms 시점에 1.0초(1x)로 변경 -> 마감 시각이 1.0초로 즉시 연장
        fake_time2 = [200.0]
        sleep_durations2 = []

        async def fake_sleep_decelerate(duration):
            sleep_durations2.append(duration)
            fake_time2[0] += duration
            # 50ms 경과(1회 sleep) 시점에 1배속(1.0s)으로 감속
            if len(sleep_durations2) == 1:
                self.manager.interval = 1.0

        with patch("time.monotonic", side_effect=lambda: fake_time2[0]), \
             patch("asyncio.sleep", side_effect=fake_sleep_decelerate):
            self.manager.interval = 0.1
            self.manager.is_paused = False
            asyncio.run(self.manager._sleep_until_next_cycle(cycle_started=200.0, stop_event=stop_event))

        # 0.1초 마감에서 1.0초 마감으로 연장되어 총 1.0초 분량(20회 sleep) 동안 대기
        self.assertEqual(len(sleep_durations2), 20)
        self.assertAlmostEqual(sum(sleep_durations2), 1.0, places=3)

    @patch("aiomqtt.Client")
    def test_17_virtual_time_invariant_plus_one_sec_per_cycle(self, mock_client_cls):
        """17. 배속(0.1s, 0.2s) 변경 전후 가상 시각(MQTT measured_at/ts 및 SSE now_iso) 일치 및 정확히 +1초씩 증가하는지 실제 워커 검증"""
        mock_client = AsyncMock()
        mock_client.__aenter__.return_value = mock_client
        mock_client_cls.return_value = mock_client

        published_payloads = []
        broadcast_events = []
        publish_event = threading.Event()
        target_count = 3

        async def fake_mqtt_publish(topic, payload_str, qos=1):
            data = json.loads(payload_str)
            published_payloads.append(data)
            if len(published_payloads) >= target_count:
                publish_event.set()

        mock_client.publish.side_effect = fake_mqtt_publish

        orig_broadcast = self.manager.broadcast
        def capturing_broadcast(data):
            broadcast_events.append(data)
            orig_broadcast(data)
        self.manager.broadcast = capturing_broadcast

        try:
            # 1. 0.1초 주기로 시작하여 3사이클 실행 대기
            self.manager.start(scenario="peak", house="H001", interval=0.1)
            if not publish_event.wait(timeout=2.0):
                self.fail(f"0.1s 주기 실행 중 3회 발행 대기 시간 초과 (현재 발행 횟수: {len(published_payloads)}, 목표: {target_count})")

            # 2. 실행 도중 0.2초 주기로 변경 후 2사이클 추가 실행 대기 (총 5회)
            publish_event.clear()
            target_count = len(published_payloads) + 2
            self.manager.set_interval(0.2)
            if not publish_event.wait(timeout=2.0):
                self.fail(f"0.2s 주기 변경 후 추가 2회 발행 대기 시간 초과 (현재 발행 횟수: {len(published_payloads)}, 목표: {target_count})")
        finally:
            self.manager.stop()

        self.assertFalse(self.manager.is_running, "테스트 완료 후 시뮬레이터는 정지 상태여야 함")
        if self.manager.worker_thread:
            self.assertFalse(self.manager.worker_thread.is_alive(), "테스트 완료 후 워커 스레드가 남아있지 않아야 함")

        self.assertGreaterEqual(len(published_payloads), 5, f"최소 5회 이상 발행되어야 함 (실제: {len(published_payloads)})")
        self.assertEqual(len(published_payloads), len(broadcast_events), "MQTT 발행 건수와 SSE 브로드캐스트 건수는 일치해야 함")

        # MQTT payload(measured_at, ts)와 SSE broadcast(now_iso) 정합성 및 연속 cycle 간 +1초 검증
        for i in range(len(published_payloads)):
            p = published_payloads[i]
            b = broadcast_events[i]

            # 1. MQTT 페이로드 필드 검증: measured_at == ts
            self.assertIn("measured_at", p)
            self.assertIn("ts", p)
            self.assertEqual(p["measured_at"], p["ts"], "MQTT payload의 measured_at과 ts는 일치해야 합니다.")

            # 2. SSE 브로드캐스트 필드 검증: now_iso
            self.assertIn("now_iso", b)
            self.assertEqual(p["measured_at"], b["now_iso"], "MQTT measured_at == MQTT ts == SSE now_iso 일치해야 합니다.")

        # 3. 연속된 사이클 간의 가상 시각이 정확히 1초씩 증가하는지 검증
        for i in range(len(published_payloads) - 1):
            p1 = published_payloads[i]
            p2 = published_payloads[i + 1]
            dt1 = datetime.fromisoformat(p1["measured_at"].replace("Z", "+00:00"))
            dt2 = datetime.fromisoformat(p2["measured_at"].replace("Z", "+00:00"))
            diff = (dt2 - dt1).total_seconds()
            self.assertEqual(diff, 1.0, f"Cycle {i+1}과 {i+2}의 가상 시각 차이는 정확히 1.0초여야 합니다 (실제 차이: {diff}초)")

    @patch("aiomqtt.Client")
    def test_18_pause_resume_virtual_time_continuity(self, mock_client_cls):
        """18. Pause/Resume 가상 시각 연속성 및 Pause 중 배속 변경 시 실제 발행 데이터 검증"""
        mock_client = AsyncMock()
        mock_client.__aenter__.return_value = mock_client
        mock_client_cls.return_value = mock_client

        captured_calls = []
        publish_event = threading.Event()
        target_count = 2

        async def fake_publish(*args, **kwargs):
            captured_calls.append(kwargs)
            if len(captured_calls) >= target_count:
                publish_event.set()
            return {"house": kwargs.get("house"), "status": "ok"}

        with patch("simulator.publish_house_power", side_effect=fake_publish):
            try:
                # 1. 0.1초 주기로 시작하여 2사이클 발행 대기
                self.manager.start(scenario="peak", house="H001", interval=0.1)
                if not publish_event.wait(timeout=2.0):
                    self.fail(f"시작 후 2회 발행 대기 시간 초과 (현재 발행 횟수: {len(captured_calls)}, 목표: {target_count})")

                # 2. Pause 호출 및 마지막 발행 타임스탬프 기록
                # manager.pause()는 내부적으로 tick_lock을 획득하여 진행 중인 tick의 발행이 완료된 후 반환됨을 보장
                self.manager.pause()
                self.assertTrue(self.manager.is_paused)
                count_at_pause = len(captured_calls)
                self.assertGreaterEqual(count_at_pause, 2)
                last_iso_before_pause = captured_calls[-1]["now_iso"]

                # 3. Pause 상태에서 추가 발행이 없음을 Event 대기(0.1초 bounded wait)로 검증
                publish_event.clear()
                self.assertFalse(publish_event.wait(timeout=0.1), "Pause 중에는 추가 MQTT 발행이 없어야 합니다.")
                self.assertEqual(len(captured_calls), count_at_pause, "Pause 중에는 발행 횟수가 증가하지 않아야 합니다.")

                # 4. Pause 상태에서 배속 변경 (0.1 -> 0.15)
                res_speed = self.manager.set_interval(0.15)
                self.assertEqual(res_speed["interval"], 0.15)
                self.assertEqual(self.manager.interval, 0.15)

                # 5. Resume 호출 및 새 tick 발행 대기
                target_count = count_at_pause + 1
                self.manager.resume()
                self.assertFalse(self.manager.is_paused)

                if not publish_event.wait(timeout=2.0):
                    self.fail(f"Resume 후 추가 발행 대기 시간 초과 (현재 발행 횟수: {len(captured_calls)}, 목표: {target_count})")
            finally:
                self.manager.stop()

        self.assertFalse(self.manager.is_running, "테스트 완료 후 시뮬레이터는 정지 상태여야 함")
        if self.manager.worker_thread:
            self.assertFalse(self.manager.worker_thread.is_alive(), "테스트 완료 후 워커 스레드가 남아있지 않아야 함")

        self.assertGreater(len(captured_calls), count_at_pause, "Resume 후 새 tick이 발행되어야 합니다.")
        first_iso_after_resume = captured_calls[count_at_pause]["now_iso"]

        # 가상 시각 연속성 검증: Pause 직전의 정확히 +1초 (publish 호출 인자 now_iso 기준)
        dt_before = datetime.fromisoformat(last_iso_before_pause.replace("Z", "+00:00"))
        dt_after = datetime.fromisoformat(first_iso_after_resume.replace("Z", "+00:00"))
        self.assertEqual((dt_after - dt_before).total_seconds(), 1.0, "Resume 후 첫 타임스탬프는 pause 직전의 정확히 +1초여야 합니다.")

    @patch("aiomqtt.Client")
    def test_19_multi_households_share_same_tick_timestamp(self, mock_client_cls):
        """19. 실제 다중 가구 워커 실행 시 동일 tick 내 모든 가구의 가상 시각 일치 및 +1초 증가 검증"""
        mock_client = AsyncMock()
        mock_client.__aenter__.return_value = mock_client
        mock_client_cls.return_value = mock_client

        captured_calls = []
        publish_event = threading.Event()
        target_count = 6  # 2가구 * 3사이클 = 6회

        async def fake_publish(*args, **kwargs):
            captured_calls.append(kwargs)
            if len(captured_calls) >= target_count:
                publish_event.set()
            return {"house": kwargs.get("house"), "status": "ok"}

        with patch("simulator.publish_house_power", side_effect=fake_publish):
            try:
                households = [
                    {"house": "H001", "scenario": "peak"},
                    {"house": "H002", "scenario": "random"}
                ]
                self.manager.start(households=households, interval=0.1)
                if not publish_event.wait(timeout=2.0):
                    self.fail(f"다중 가구 실행 시 목표 발행 대기 시간 초과 (현재 발행 횟수: {len(captured_calls)}, 목표: {target_count})")
            finally:
                self.manager.stop()

        self.assertFalse(self.manager.is_running, "테스트 완료 후 시뮬레이터는 정지 상태여야 함")
        if self.manager.worker_thread:
            self.assertFalse(self.manager.worker_thread.is_alive(), "테스트 완료 후 워커 스레드가 남아있지 않아야 함")

        h1_calls = [c for c in captured_calls if c["house"] == "H001"]
        h2_calls = [c for c in captured_calls if c["house"] == "H002"]

        min_cycles = min(len(h1_calls), len(h2_calls))
        self.assertGreaterEqual(min_cycles, 2, f"다중 가구 실행 시 최소 2사이클 이상 수집되어야 함 (H001: {len(h1_calls)}, H002: {len(h2_calls)})")

        for c in range(min_cycles):
            t_h1 = h1_calls[c]["now_iso"]
            t_h2 = h2_calls[c]["now_iso"]
            self.assertEqual(t_h1, t_h2, f"Cycle {c+1}에서 H001과 H002의 now_iso는 완전히 일치해야 합니다.")

            if c > 0:
                prev_t = h1_calls[c - 1]["now_iso"]
                dt_prev = datetime.fromisoformat(prev_t.replace("Z", "+00:00"))
                dt_cur = datetime.fromisoformat(t_h1.replace("Z", "+00:00"))
                self.assertEqual((dt_cur - dt_prev).total_seconds(), 1.0, f"Cycle {c}와 {c+1}의 시각 차이는 정확히 1초여야 합니다.")

    @patch("aiomqtt.Client")
    def test_20_reset_503_defense_on_worker_stop_failure(self, mock_client_cls):
        """20. 워커 종료 실패 시 Reset 503 방어: 상태 보존 및 HTTP 503 RESET_FAILED 반환 검증"""
        mock_client = AsyncMock()
        mock_client.__aenter__.return_value = mock_client
        mock_client_cls.return_value = mock_client

        # 1. 0.2초 배속으로 실행 시작
        self.manager.start(scenario="peak", house="H001", interval=0.2)
        orig_worker = self.manager.worker_thread
        self.assertTrue(self.manager.is_running)
        self.assertEqual(self.manager.interval, 0.2)
        self.assertIn("H001", self.manager.active_households)

        # 2. _stop_and_join()이 실패(False 반환)하도록 mock
        with patch.object(self.manager, "_stop_and_join", return_value=False):
            # manager.reset()은 RuntimeError를 발생시켜야 함
            with self.assertRaises(RuntimeError) as ctx:
                self.manager.reset()
            self.assertIn("시뮬레이터 워커 종료에 실패하여 리셋할 수 없습니다", str(ctx.exception))

            # 실패 후 기존 상태 보존 검증
            self.assertTrue(self.manager.is_running, "reset 실패 시 is_running은 True를 유지해야 합니다.")
            self.assertIs(self.manager.worker_thread, orig_worker, "기존 워커 스레드 참조가 유지되어야 합니다.")
            self.assertEqual(self.manager.interval, 0.2, "기존 interval(0.2)이 1.0으로 초기화되지 않고 유지되어야 합니다.")
            self.assertIn("H001", self.manager.active_households, "기존 가구 상태가 유지되어야 합니다.")

            # RequestHandler를 통한 HTTP 503 응답 검증
            handler = DummyRequestHandler(self.manager, method="POST", path="/api/reset")
            handler.send_response = MagicMock()
            handler.send_header = MagicMock()
            handler.end_headers = MagicMock()

            handler.do_POST()
            handler.send_response.assert_called_with(503)
            resp = handler.get_response_json()
            self.assertEqual(resp["code"], "RESET_FAILED")

        # 3. 정상 종료
        self.manager.stop()

    @patch("aiomqtt.Client")
    def test_21_reset_success_restores_defaults(self, mock_client_cls):
        """21. 워커 종료 성공 시 정상 리셋: interval=1.0, is_running=False 초기화 검증"""
        mock_client = AsyncMock()
        mock_client.__aenter__.return_value = mock_client
        mock_client_cls.return_value = mock_client

        self.manager.start(scenario="peak", house="H001", interval=0.2)
        self.assertEqual(self.manager.interval, 0.2)

        res = self.manager.reset()
        self.assertEqual(res["status"], "reset")
        self.assertFalse(self.manager.is_running)
        self.assertIsNone(self.manager.worker_thread)
        self.assertEqual(self.manager.interval, 1.0)
        self.assertEqual(self.manager.get_status()["interval"], 1.0)
        self.assertEqual(self.manager.get_status()["speed"], 1.0)


if __name__ == "__main__":
    unittest.main()

