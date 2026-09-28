"""
H001~H010 센서 고장 시나리오(sensor_fault) 종합 단위 테스트

검증 항목:
1. sensor_fault가 server/config.ALLOWED_SCENARIOS에 포함되는지
2. SensorFaultScenario 상수 및 타임라인 검증 (140 cycles, 11~130 고장, 120초 결측)
3. is_fault_cycle(cycle) 및 get_gap_metrics(cycle) 검증
4. cycle 11("센서 고장 시작"), cycle 131("센서 복구"), cycle 140("시나리오 완료") 명확한 이벤트 생성 검증
5. H001~H010 모든 가구에서 sensor_fault 시나리오 지원 검증
6. 단일 가구 MQTT 발행 검증:
   - 140 틱 완주 시 총 20회만 발행
   - cycle 11~130 구간(120초) 동안 해당 가구 topic 및 measured_at 메시지가 0건인지 엄격 검증
   - 0W 허위 데이터가 발행되지 않는지 검증
   - cycle 10과 cycle 131 간의 measured_at 타임스탬프 차이가 정확히 121초인지 검증
7. 다중 가구 동시 실행 시 격리 검증:
   - H001(sensor_fault)은 20회 발행 및 11~130 결측
   - H002(정상)는 140회 연속 발행
8. SimulatorManager SSE 이벤트 및 메트릭 검증:
   - cycle 11~130 동안 SSE 메트릭에 measurementAvailable: False, sensorFault: True,
     gapElapsedSec(1~120), gapRemainingSec(120~1) 포함 및 전력/전압/전류 필드가 None인지 검증
   - cycle 140 완료 시 status: 'completed' 전이 검증
9. 터미널 로깅 및 CLI 검증:
   - sensor_fault 시나리오의 target_count가 140으로 설정되는지 검증
"""

import asyncio
import copy
import io
import json
import os
import queue
import sys
import time
import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, AsyncMock, patch

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SIMULATOR_DIR = os.path.dirname(TESTS_DIR)
if SIMULATOR_DIR not in sys.path:
    sys.path.insert(0, SIMULATOR_DIR)

import scenarios
from scenarios import (
    KST,
    SensorFaultScenario,
    get_scenario_target_cycles,
)
import simulator
import server.config as srv_config
from server.manager import SimulatorManager
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


class TestSensorFaultAllowedAndConfig(unittest.TestCase):
    """1. 시나리오 등록 및 상수 검증"""

    def test_sensor_fault_in_allowed_scenarios(self):
        self.assertIn("sensor_fault", srv_config.ALLOWED_SCENARIOS)

    def test_sensor_fault_constants(self):
        self.assertEqual(SensorFaultScenario.NAME, "sensor_fault")
        self.assertEqual(SensorFaultScenario.TOTAL_CYCLES, 140)
        self.assertEqual(SensorFaultScenario.NORMAL_BEFORE_FAULT_CYCLES, 10)
        self.assertEqual(SensorFaultScenario.FAULT_START_CYCLE, 11)
        self.assertEqual(SensorFaultScenario.FAULT_END_CYCLE, 130)
        self.assertEqual(SensorFaultScenario.FAULT_DURATION_SEC, 120)
        self.assertEqual(SensorFaultScenario.RECOVERED_START_CYCLE, 131)
        self.assertEqual(SensorFaultScenario.RECOVERED_END_CYCLE, 140)
        self.assertEqual(SensorFaultScenario.RECOVERED_DURATION_CYCLES, 10)
        self.assertEqual(SensorFaultScenario.TOTAL_PUBLISH_COUNT, 20)

    def test_target_cycles_helper(self):
        self.assertEqual(get_scenario_target_cycles("sensor_fault"), 140)


class TestSensorFaultTimelineLogic(unittest.TestCase):
    """2. 타임라인 및 메트릭 산출 로직 검증"""

    def test_is_fault_cycle(self):
        # 1~10: 정상
        for c in range(1, 11):
            self.assertFalse(SensorFaultScenario.is_fault_cycle(c), f"cycle {c} should not be fault")

        # 11~130: 결측 구간
        for c in range(11, 131):
            self.assertTrue(SensorFaultScenario.is_fault_cycle(c), f"cycle {c} should be fault")

        # 131~140: 복구 후 정상
        for c in range(131, 141):
            self.assertFalse(SensorFaultScenario.is_fault_cycle(c), f"cycle {c} should not be fault")

    def test_get_gap_metrics(self):
        # 고장 전 정상 (cycle 1~10)
        is_fault, elapsed, rem = SensorFaultScenario.get_gap_metrics(1)
        self.assertFalse(is_fault)
        self.assertEqual(elapsed, 0)
        self.assertEqual(rem, 120)

        is_fault, elapsed, rem = SensorFaultScenario.get_gap_metrics(10)
        self.assertFalse(is_fault)
        self.assertEqual(elapsed, 0)
        self.assertEqual(rem, 120)

        # 고장 첫 주기 (cycle 11)
        is_fault, elapsed, rem = SensorFaultScenario.get_gap_metrics(11)
        self.assertTrue(is_fault)
        self.assertEqual(elapsed, 1)
        self.assertEqual(rem, 119)

        # 고장 중간 (cycle 70)
        is_fault, elapsed, rem = SensorFaultScenario.get_gap_metrics(70)
        self.assertTrue(is_fault)
        self.assertEqual(elapsed, 60)
        self.assertEqual(rem, 60)

        # 고장 마지막 주기 (cycle 130)
        is_fault, elapsed, rem = SensorFaultScenario.get_gap_metrics(130)
        self.assertTrue(is_fault)
        self.assertEqual(elapsed, 120)
        self.assertEqual(rem, 0)

        # 복구 구간 (cycle 131, 140)
        is_fault, elapsed, rem = SensorFaultScenario.get_gap_metrics(131)
        self.assertFalse(is_fault)
        self.assertEqual(elapsed, 120)
        self.assertEqual(rem, 0)

        is_fault, elapsed, rem = SensorFaultScenario.get_gap_metrics(140)
        self.assertFalse(is_fault)
        self.assertEqual(elapsed, 120)
        self.assertEqual(rem, 0)

    def test_get_cycle_status_events(self):
        # cycle 11: 고장 시작 이벤트
        status_tag, notice = SensorFaultScenario.get_cycle_status(11)
        self.assertEqual(status_tag, "센서 고장")
        self.assertIsNotNone(notice)
        self.assertIn("센서 고장 시작", notice)
        self.assertIn("120초", notice)

        # cycle 131: 복구 이벤트
        status_tag, notice = SensorFaultScenario.get_cycle_status(131)
        self.assertEqual(status_tag, "정상 계측")
        self.assertIsNotNone(notice)
        self.assertIn("센서 복구", notice)

        # cycle 140: 시나리오 완료 이벤트
        status_tag, notice = SensorFaultScenario.get_cycle_status(140)
        self.assertEqual(status_tag, "시나리오 완료")
        self.assertIsNotNone(notice)
        self.assertIn("시나리오 완료", notice)


class TestSensorFaultAllHouseholdsSupport(unittest.TestCase):
    """3. H001~H010 가구 전체에서 sensor_fault 시나리오 지원 검증"""

    def setUp(self):
        self.manager = SimulatorManager(host="localhost", port=1883, tls_enabled=False)

    def tearDown(self):
        self.manager.stop()
        self.manager.reset()

    @patch("aiomqtt.Client")
    def test_all_houses_supported_in_manager(self, _mock_client):
        for house_idx in range(1, 11):
            house = f"H{house_idx:03d}"
            try:
                self.manager.start(house=house, scenario="sensor_fault")
                self.assertEqual(self.manager.current_mode, "sensor_fault")
                self.assertIn(house, self.manager.active_households)
                self.assertEqual(self.manager.active_households[house]["scenario"], "sensor_fault")
            finally:
                self.manager.stop()
                self.manager.reset()

    def test_cli_accepts_sensor_fault_with_house(self):
        for house_idx in range(1, 11):
            house = f"H{house_idx:03d}"
            args = simulator.parse_args(["--scenario", "sensor_fault", "--houses", str(house_idx)])
            self.assertEqual(args.scenario, "sensor_fault")
            self.assertEqual(args.houses, house_idx)


class TestSensorFaultMqttPublishZeroBlackout(unittest.TestCase):
    """4. MQTT 발행 검증: cycle 11~130 완전 결측, topic별 measured_at 수집 검증"""

    def test_single_house_mqtt_publish_exact_20_and_zero_in_fault_gap(self):
        """
        단일 가구(H001) sensor_fault 140 틱 실행 시:
        - 총 발행 건수 정확히 20건
        - cycle 11~130 동안 해당 가구 topic의 메시지가 전혀 발행되지 않음
        - cycle 10 메시지와 cycle 131 메시지의 measured_at 타임스탬프 차이가 정확히 121초
        - 허위 0W 데이터가 발행되지 않음
        """
        manager = SimulatorManager(host="localhost", port=1883, tls_enabled=False)

        published_messages = []

        async def mock_publish(client, house, now_iso=None, qos=1, allow_random=False, metrics=None, **kwargs):
            topic = f"v1/power/sim/{house}/main"
            measured_at = now_iso or datetime.now(timezone.utc).isoformat()
            power = (metrics or {}).get("active_power", 55.0)
            msg_record = {
                "house": house,
                "topic": topic,
                "measured_at": measured_at,
                "active_power": power,
            }
            published_messages.append(msg_record)
            return {"house": house, "power": power, "devices": []}

        try:
            with patch("simulator.publish_house_power", side_effect=mock_publish), \
                 patch("aiomqtt.Client"), \
                 patch("server.manager.validate_interval", side_effect=lambda x: float(x)):
                manager.start(house="H001", scenario="sensor_fault", interval=0.0001)

                # 워커 루프가 cycle 140에 도달하여 자동 완료될 때까지 대기
                max_wait = 5.0
                start_time = time.time()
                while time.time() - start_time < max_wait:
                    if not manager.is_running:
                        break
                    time.sleep(0.01)

                self.assertFalse(manager.is_running, "시뮬레이터가 140 틱 완주 후 자동으로 종료되어야 함")
                h001_metrics = manager.last_metrics_by_house.get("H001")
                self.assertIsNotNone(h001_metrics)
                self.assertEqual(h001_metrics.get("status"), "completed")
                self.assertEqual(h001_metrics.get("sec"), 140)

                # 1. 총 발행 건수는 정확히 20건이어야 함
                self.assertEqual(len(published_messages), 20, f"예상 20건이나 실제 {len(published_messages)}건 발행됨")

                # 2. cycle 1~10 메시지 10건, cycle 131~140 메시지 10건 검증
                before_fault = published_messages[:10]
                after_fault = published_messages[10:]
                self.assertEqual(len(before_fault), 10)
                self.assertEqual(len(after_fault), 10)

                # 3. 허위 0W가 아닌 정상 전력값(대기전력 30W 이상)인지 검증
                for m in published_messages:
                    self.assertGreater(m["active_power"], 30.0, f"결측 중 0W나 비정상 전력이 발행되면 안 됨: {m}")

                # 4. 타임스탬프 연속성 검증: cycle 10과 cycle 131 간의 measured_at 차이가 121초여야 함
                dt_c10 = datetime.fromisoformat(before_fault[-1]["measured_at"])
                dt_c131 = datetime.fromisoformat(after_fault[0]["measured_at"])
                diff_sec = (dt_c131 - dt_c10).total_seconds()
                self.assertEqual(diff_sec, 121.0, f"cycle 10과 cycle 131의 시각 차이는 121초여야 합니다 (실제 {diff_sec}초)")

        finally:
            manager.stop()
            manager.reset()

    def test_multi_house_fault_isolation(self):
        """
        다중 가구 실행 (H001: sensor_fault, H002: random):
        - H001: 20건 발행, cycle 11~130 결측
        - H002: 140건 정상 연속 발행
        """
        manager = SimulatorManager(host="localhost", port=1883, tls_enabled=False)

        published_by_house = {"H001": [], "H002": []}

        async def mock_publish(client, house, now_iso=None, qos=1, allow_random=False, metrics=None, **kwargs):
            topic = f"v1/power/sim/{house}/main"
            record = {
                "house": house,
                "topic": topic,
                "measured_at": now_iso,
                "power": (metrics or {}).get("active_power", 55.0),
            }
            if house in published_by_house:
                published_by_house[house].append(record)
            return {"house": house, "power": record["power"], "devices": []}

        try:
            with patch("simulator.publish_house_power", side_effect=mock_publish), \
                 patch("aiomqtt.Client"), \
                 patch("server.manager.validate_interval", side_effect=lambda x: float(x)):
                manager.start(
                    households=[
                        {"house": "H001", "scenario": "sensor_fault"},
                        {"house": "H002", "scenario": "random"}
                    ],
                    interval=0.0001
                )

                start_time = time.time()
                while time.time() - start_time < 5.0:
                    h1_status = manager.active_households.get("H001", {}).get("status")
                    if h1_status == "completed" and len(published_by_house["H002"]) >= 140:
                        break
                    time.sleep(0.01)

                manager.stop()
                self.assertEqual(len(published_by_house["H001"]), 20)
                self.assertGreaterEqual(len(published_by_house["H002"]), 140)

                # H001 메시지의 topic 및 timestamp 검증:
                # cycle 10 직후 cycle 131까지 H001 토픽의 발행이 전무해야 함
                h001_msgs = published_by_house["H001"]
                for m in h001_msgs:
                    self.assertEqual(m["topic"], "v1/power/sim/H001/main")

                dt_10 = datetime.fromisoformat(h001_msgs[9]["measured_at"])
                dt_11 = datetime.fromisoformat(h001_msgs[10]["measured_at"])
                self.assertEqual((dt_11 - dt_10).total_seconds(), 121.0)

        finally:
            manager.stop()
            manager.reset()


class TestSensorFaultSseAndMetrics(unittest.TestCase):
    """5. SSE 스트림 및 메트릭(None 값, gapElapsedSec, sensorFault) 검증"""

    def test_sse_event_payload_structure(self):
        manager = SimulatorManager(host="localhost", port=1883, tls_enabled=False)

        sse_events = []
        orig_broadcast = manager.broadcast

        def mock_broadcast(payload):
            if payload.get("house") == "H001":
                sse_events.append(copy.deepcopy(payload))
            orig_broadcast(payload)

        manager.broadcast = mock_broadcast

        try:
            with patch("simulator.publish_house_power", new_callable=AsyncMock) as mock_pub, \
                 patch("aiomqtt.Client"), \
                 patch("server.manager.validate_interval", side_effect=lambda x: float(x)):
                mock_pub.return_value = {"house": "H001", "power": 55.0, "devices": []}
                manager.start(house="H001", scenario="sensor_fault", interval=0.0001)

                start_time = time.time()
                while time.time() - start_time < 5.0:
                    if not manager.is_running:
                        break
                    time.sleep(0.01)

                self.assertEqual(len(sse_events), 140)

                # 1. cycle 1 검증
                ev1 = sse_events[0]
                self.assertEqual(ev1["sec"], 1)
                self.assertTrue(ev1["measurementAvailable"])
                self.assertFalse(ev1["sensorFault"])
                self.assertEqual(ev1["gapElapsedSec"], 0)
                self.assertEqual(ev1["gapRemainingSec"], 120)
                self.assertIsNotNone(ev1["totalP"])
                self.assertEqual(ev1["status"], "running")

                # 2. cycle 10 검증
                ev10 = sse_events[9]
                self.assertEqual(ev10["sec"], 10)
                self.assertTrue(ev10["measurementAvailable"])
                self.assertFalse(ev10["sensorFault"])
                self.assertEqual(ev10["gapElapsedSec"], 0)
                self.assertEqual(ev10["gapRemainingSec"], 120)
                self.assertIsNotNone(ev10["totalP"])
                self.assertEqual(ev10["status"], "running")

                # 3. cycle 11 검증 (고장 시작)
                ev11 = sse_events[10]
                self.assertEqual(ev11["sec"], 11)
                self.assertFalse(ev11["measurementAvailable"])
                self.assertTrue(ev11["sensorFault"])
                self.assertEqual(ev11["gapElapsedSec"], 1)
                self.assertEqual(ev11["gapRemainingSec"], 119)
                self.assertIsNone(ev11["totalP"])
                self.assertIsNone(ev11["totalQ"])
                self.assertIsNone(ev11["apparentS"])
                self.assertIsNone(ev11["voltage"])
                self.assertIsNone(ev11["currentA"])
                self.assertIsNone(ev11["pf"])
                self.assertIn("센서 고장 시작", ev11["eventNoticeText"])
                self.assertEqual(ev11["status"], "running")

                # 4. cycle 130 검증 (고장 마지막 tick)
                ev130 = sse_events[129]
                self.assertEqual(ev130["sec"], 130)
                self.assertFalse(ev130["measurementAvailable"])
                self.assertTrue(ev130["sensorFault"])
                self.assertEqual(ev130["gapElapsedSec"], 120)
                self.assertEqual(ev130["gapRemainingSec"], 0)
                self.assertIsNone(ev130["totalP"])
                self.assertEqual(ev130["status"], "running")

                # 5. cycle 131 검증 (센서 복구 첫 tick)
                ev131 = sse_events[130]
                self.assertEqual(ev131["sec"], 131)
                self.assertTrue(ev131["measurementAvailable"])
                self.assertFalse(ev131["sensorFault"])
                self.assertEqual(ev131["gapElapsedSec"], 120)
                self.assertEqual(ev131["gapRemainingSec"], 0)
                self.assertIsNotNone(ev131["totalP"])
                self.assertIn("센서 복구", ev131["eventNoticeText"])
                self.assertEqual(ev131["status"], "running")

                # 6. cycle 140 검증 (시나리오 완료)
                ev140 = sse_events[139]
                self.assertEqual(ev140["sec"], 140)
                self.assertTrue(ev140["measurementAvailable"])
                self.assertFalse(ev140["sensorFault"])
                self.assertEqual(ev140["gapElapsedSec"], 120)
                self.assertEqual(ev140["gapRemainingSec"], 0)
                self.assertIsNotNone(ev140["totalP"])
                self.assertIn("시나리오 완료", ev140["eventNoticeText"])
                self.assertEqual(ev140["status"], "completed")

                # 전체 cycle 11~130 결측 메트릭 정합성 검증
                for i in range(10, 130):
                    ev = sse_events[i]
                    c = i + 1
                    self.assertFalse(ev["measurementAvailable"], f"cycle {c} measurementAvailable must be False")
                    self.assertTrue(ev["sensorFault"], f"cycle {c} sensorFault must be True")
                    self.assertIsNone(ev["totalP"], f"cycle {c} totalP must be None")
                    self.assertIsNone(ev["totalQ"], f"cycle {c} totalQ must be None")
                    self.assertIsNone(ev["apparentS"], f"cycle {c} apparentS must be None")
                    self.assertIsNone(ev["currentA"], f"cycle {c} currentA must be None")
                    self.assertIsNone(ev["voltage"], f"cycle {c} voltage must be None")
                    self.assertIsNone(ev["pf"], f"cycle {c} pf must be None")
                    self.assertEqual(ev["gapElapsedSec"], c - 10)
                    self.assertEqual(ev["gapRemainingSec"], 130 - c)

        finally:
            manager.stop()
            manager.reset()


class TestSensorFaultCliRunner(unittest.TestCase):
    """6. CLI simulator.py sensor_fault 실행 및 터미널 로깅 검증"""

    def test_cli_target_count_is_140(self):
        args = simulator.parse_args(["--scenario", "sensor_fault"])
        self.assertEqual(args.scenario, "sensor_fault")
        # count 미지정 시 140 틱
        effective_count = scenarios.get_scenario_target_cycles("sensor_fault")
        self.assertEqual(effective_count, 140)

    def test_cli_run_simulator_skips_mqtt_in_fault_cycles(self):
        args = simulator.parse_args(["--scenario", "sensor_fault", "--houses", "1", "--interval", "0.0001", "--count", "140"])
        published = []

        async def mock_pub(client, house, now_iso=None, qos=1, allow_random=False, metrics=None, **kwargs):
            published.append(house)
            return {"house": house, "power": 55.0, "devices": []}

        with patch("simulator.publish_house_power", side_effect=mock_pub), \
             patch("aiomqtt.Client"):
            asyncio.run(simulator.run_simulator(args))

        self.assertEqual(len(published), 20)

    def test_cli_sensor_fault_fixes_base_dt_when_unspecified(self):
        """--date / --start-time 미지정 시 base_dt 1회 고정, 가상 1초 증가, 고장 구간 0건"""
        args = simulator.parse_args([
            "--scenario", "sensor_fault",
            "--houses", "1",
            "--interval", "0.0001",
            "--count", "140",
        ])

        published_timestamps = []

        async def mock_pub(client, house, now_iso=None, qos=1, allow_random=False, metrics=None, **kwargs):
            published_timestamps.append(now_iso)
            return {"house": house, "power": 55.0, "devices": []}

        with patch("simulator.publish_house_power", side_effect=mock_pub), \
             patch("aiomqtt.Client"):
            asyncio.run(simulator.run_simulator(args))

        # 총 20건 발행 (10 정상 + 10 복구)
        self.assertEqual(len(published_timestamps), 20)

        # 정상 구간 10개: 1초 간격 증가
        before_fault = published_timestamps[:10]
        for i in range(1, len(before_fault)):
            dt_prev = datetime.fromisoformat(before_fault[i - 1].replace("Z", "+00:00"))
            dt_curr = datetime.fromisoformat(before_fault[i].replace("Z", "+00:00"))
            diff = (dt_curr - dt_prev).total_seconds()
            self.assertEqual(diff, 1.0, f"정상 구간 cycle {i}: 1초 간격이어야 함 (실제: {diff}초)")

        # 복구 구간 10개: 역시 1초 간격
        after_fault = published_timestamps[10:]
        for i in range(1, len(after_fault)):
            dt_prev = datetime.fromisoformat(after_fault[i - 1].replace("Z", "+00:00"))
            dt_curr = datetime.fromisoformat(after_fault[i].replace("Z", "+00:00"))
            diff = (dt_curr - dt_prev).total_seconds()
            self.assertEqual(diff, 1.0, f"복구 구간 cycle {i}: 1초 간격이어야 함 (실제: {diff}초)")

        # cycle 10과 cycle 131 사이 시각 차이 = 121초 (가상 시각 기준)
        dt_c10 = datetime.fromisoformat(before_fault[-1].replace("Z", "+00:00"))
        dt_c131 = datetime.fromisoformat(after_fault[0].replace("Z", "+00:00"))
        gap_diff = (dt_c131 - dt_c10).total_seconds()
        self.assertEqual(gap_diff, 121.0, f"고장 구간 measured_at 차이 121초 (실제: {gap_diff}초)")


class TestSensorFaultConfigurableDuration(unittest.TestCase):
    """7. D=30 및 D=180 가변 결측 시간 및 타임라인 경계값 / 발행 검증"""

    def test_d30_timeline_and_gap_metrics(self):
        tl = SensorFaultScenario.get_timeline(30)
        self.assertEqual(tl.duration_sec, 30)
        self.assertEqual(tl.fault_start, 11)
        self.assertEqual(tl.fault_end, 40)
        self.assertEqual(tl.recovery_start, 41)
        self.assertEqual(tl.recovery_end, 50)
        self.assertEqual(tl.total_cycles, 50)
        self.assertEqual(tl.publish_count, 20)

        # cycle 10 (고장 직전)
        is_fault, elapsed, rem = SensorFaultScenario.get_gap_metrics(10, duration_sec=30)
        self.assertFalse(is_fault)
        self.assertEqual(elapsed, 0)
        self.assertEqual(rem, 30)

        # cycle 11 (고장 시작)
        is_fault, elapsed, rem = SensorFaultScenario.get_gap_metrics(11, duration_sec=30)
        self.assertTrue(is_fault)
        self.assertEqual(elapsed, 1)
        self.assertEqual(rem, 29)

        # cycle 40 (고장 마지막)
        is_fault, elapsed, rem = SensorFaultScenario.get_gap_metrics(40, duration_sec=30)
        self.assertTrue(is_fault)
        self.assertEqual(elapsed, 30)
        self.assertEqual(rem, 0)

        # cycle 41 (복구 첫 사이클)
        is_fault, elapsed, rem = SensorFaultScenario.get_gap_metrics(41, duration_sec=30)
        self.assertFalse(is_fault)
        self.assertEqual(elapsed, 30)
        self.assertEqual(rem, 0)

        # cycle 50 (완료)
        is_fault, elapsed, rem = SensorFaultScenario.get_gap_metrics(50, duration_sec=30)
        self.assertFalse(is_fault)
        self.assertEqual(elapsed, 30)
        self.assertEqual(rem, 0)

    def test_d180_timeline_and_gap_metrics(self):
        tl = SensorFaultScenario.get_timeline(180)
        self.assertEqual(tl.duration_sec, 180)
        self.assertEqual(tl.fault_start, 11)
        self.assertEqual(tl.fault_end, 190)
        self.assertEqual(tl.recovery_start, 191)
        self.assertEqual(tl.recovery_end, 200)
        self.assertEqual(tl.total_cycles, 200)
        self.assertEqual(tl.publish_count, 20)

        # cycle 10 (고장 직전)
        is_fault, elapsed, rem = SensorFaultScenario.get_gap_metrics(10, duration_sec=180)
        self.assertFalse(is_fault)
        self.assertEqual(elapsed, 0)
        self.assertEqual(rem, 180)

        # cycle 11 (고장 시작)
        is_fault, elapsed, rem = SensorFaultScenario.get_gap_metrics(11, duration_sec=180)
        self.assertTrue(is_fault)
        self.assertEqual(elapsed, 1)
        self.assertEqual(rem, 179)

        # cycle 190 (고장 마지막)
        is_fault, elapsed, rem = SensorFaultScenario.get_gap_metrics(190, duration_sec=180)
        self.assertTrue(is_fault)
        self.assertEqual(elapsed, 180)
        self.assertEqual(rem, 0)

        # cycle 191 (복구 첫 사이클)
        is_fault, elapsed, rem = SensorFaultScenario.get_gap_metrics(191, duration_sec=180)
        self.assertFalse(is_fault)
        self.assertEqual(elapsed, 180)
        self.assertEqual(rem, 0)

        # cycle 200 (완료)
        is_fault, elapsed, rem = SensorFaultScenario.get_gap_metrics(200, duration_sec=180)
        self.assertFalse(is_fault)
        self.assertEqual(elapsed, 180)
        self.assertEqual(rem, 0)

    def test_d30_mqtt_publish_20_and_31s_gap(self):
        manager = SimulatorManager(host="localhost", port=1883, tls_enabled=False)
        published_messages = []

        async def mock_publish(client, house, now_iso=None, qos=1, allow_random=False, metrics=None, **kwargs):
            topic = f"v1/power/sim/{house}/main"
            measured_at = now_iso or datetime.now(timezone.utc).isoformat()
            power = (metrics or {}).get("active_power", 55.0)
            msg_record = {"house": house, "topic": topic, "measured_at": measured_at, "active_power": power}
            published_messages.append(msg_record)
            return {"house": house, "power": power, "devices": []}

        try:
            with patch("simulator.publish_house_power", side_effect=mock_publish), \
                 patch("aiomqtt.Client"), \
                 patch("server.manager.validate_interval", side_effect=lambda x: float(x)):
                manager.start(house="H001", scenario="sensor_fault", fault_duration_sec=30, interval=0.0001)

                max_wait = 5.0
                start_time = time.time()
                while time.time() - start_time < max_wait:
                    if not manager.is_running:
                        break
                    time.sleep(0.01)

                self.assertFalse(manager.is_running)
                h001_metrics = manager.last_metrics_by_house.get("H001")
                self.assertEqual(h001_metrics.get("status"), "completed")
                self.assertEqual(h001_metrics.get("sec"), 50)
                self.assertEqual(len(published_messages), 20)

                dt_c10 = datetime.fromisoformat(published_messages[9]["measured_at"].replace("Z", "+00:00"))
                dt_c41 = datetime.fromisoformat(published_messages[10]["measured_at"].replace("Z", "+00:00"))
                gap_sec = (dt_c41 - dt_c10).total_seconds()
                self.assertEqual(gap_sec, 31.0)
        finally:
            manager.stop()
            manager.reset()

    def test_d180_mqtt_publish_20_and_181s_gap(self):
        manager = SimulatorManager(host="localhost", port=1883, tls_enabled=False)
        published_messages = []

        async def mock_publish(client, house, now_iso=None, qos=1, allow_random=False, metrics=None, **kwargs):
            topic = f"v1/power/sim/{house}/main"
            measured_at = now_iso or datetime.now(timezone.utc).isoformat()
            power = (metrics or {}).get("active_power", 55.0)
            msg_record = {"house": house, "topic": topic, "measured_at": measured_at, "active_power": power}
            published_messages.append(msg_record)
            return {"house": house, "power": power, "devices": []}

        try:
            with patch("simulator.publish_house_power", side_effect=mock_publish), \
                 patch("aiomqtt.Client"), \
                 patch("server.manager.validate_interval", side_effect=lambda x: float(x)):
                manager.start(house="H001", scenario="sensor_fault", fault_duration_sec=180, interval=0.0001)

                max_wait = 5.0
                start_time = time.time()
                while time.time() - start_time < max_wait:
                    if not manager.is_running:
                        break
                    time.sleep(0.01)

                self.assertFalse(manager.is_running)
                h001_metrics = manager.last_metrics_by_house.get("H001")
                self.assertEqual(h001_metrics.get("status"), "completed")
                self.assertEqual(h001_metrics.get("sec"), 200)
                self.assertEqual(len(published_messages), 20)

                dt_c10 = datetime.fromisoformat(published_messages[9]["measured_at"].replace("Z", "+00:00"))
                dt_c191 = datetime.fromisoformat(published_messages[10]["measured_at"].replace("Z", "+00:00"))
                gap_sec = (dt_c191 - dt_c10).total_seconds()
                self.assertEqual(gap_sec, 181.0)
        finally:
            manager.stop()
            manager.reset()


class TestSensorFaultCliCountConflict(unittest.TestCase):
    """8. CLI --count 및 fault_duration_sec 충돌 사전 검증 테스트"""

    def test_cli_count_conflict_rejected_before_connection(self):
        # 1. D=30, count=-1 -> TLS/MQTT 연결 전 ValueError 및 mock TLS 미호출 검증
        args_neg = simulator.parse_args(["--scenario", "sensor_fault", "--fault-duration-sec", "30", "--count", "-1"])
        with patch("simulator.get_mqtt_tls_context") as mock_tls:
            with self.assertRaises(ValueError) as ctx:
                asyncio.run(simulator.run_simulator(args_neg))
            self.assertIn("일치해야 합니다", str(ctx.exception))
            self.assertIn("50", str(ctx.exception))
            self.assertIn("-1", str(ctx.exception))
            mock_tls.assert_not_called()

        # 2. D=30, count=140 -> 거절 및 mock TLS 미호출 검증
        args_conflict_140 = simulator.parse_args(["--scenario", "sensor_fault", "--fault-duration-sec", "30", "--count", "140"])
        with patch("simulator.get_mqtt_tls_context") as mock_tls:
            with self.assertRaises(ValueError) as ctx:
                asyncio.run(simulator.run_simulator(args_conflict_140))
            self.assertIn("일치해야 합니다", str(ctx.exception))
            self.assertIn("50", str(ctx.exception))
            self.assertIn("140", str(ctx.exception))
            mock_tls.assert_not_called()

        # 3. D=120, count=100 -> 거절 및 mock TLS 미호출 검증
        args_conflict_120 = simulator.parse_args(["--scenario", "sensor_fault", "--fault-duration-sec", "120", "--count", "100"])
        with patch("simulator.get_mqtt_tls_context") as mock_tls:
            with self.assertRaises(ValueError) as ctx:
                asyncio.run(simulator.run_simulator(args_conflict_120))
            self.assertIn("140", str(ctx.exception))
            self.assertIn("100", str(ctx.exception))
            mock_tls.assert_not_called()

    def test_cli_count_zero_and_matching_count_allowed(self):
        # D=30, count=0 -> 허용 및 expected_total(50) 사용 (20건 정상 발행)
        args_zero = simulator.parse_args(["--scenario", "sensor_fault", "--fault-duration-sec", "30", "--count", "0", "--interval", "0.0001"])
        published = []
        async def mock_pub(client, house, now_iso=None, qos=1, allow_random=False, metrics=None, **kwargs):
            published.append(house)
            return {"house": house, "power": 55.0, "devices": []}

        with patch("simulator.publish_house_power", side_effect=mock_pub), patch("aiomqtt.Client"):
            asyncio.run(simulator.run_simulator(args_zero))
        self.assertEqual(len(published), 20)

        # D=30, count=50 -> 허용 및 일치하는 총 사이클로 정상 실행 (20건 정상 발행)
        published.clear()
        args_match = simulator.parse_args(["--scenario", "sensor_fault", "--fault-duration-sec", "30", "--count", "50", "--interval", "0.0001"])
        with patch("simulator.publish_house_power", side_effect=mock_pub), patch("aiomqtt.Client"):
            asyncio.run(simulator.run_simulator(args_match))
        self.assertEqual(len(published), 20)


class TestSensorFaultApiValidationAndProtection(unittest.TestCase):
    """9. 단일/다중 API 검증 및 기존 실행 무중단 보호 테스트"""

    def setUp(self):
        self.manager = SimulatorManager(host="localhost", port=1883, tls_enabled=False)

    def tearDown(self):
        self.manager.stop()
        self.manager.reset()

    def test_api_single_and_multi_start_success(self):
        # 키 생략 시 기본값 120 적용 검증
        with patch.object(self.manager, "_run_async_worker"):
            handler_omit = DummyRequestHandler(
                self.manager,
                path="/api/start",
                body_dict={"scenario": "sensor_fault", "house": "H001"}
            )
            handler_omit.do_POST()
            self.assertEqual(handler_omit.get_status_code(), 200)
            res_omit = handler_omit.get_response_json()
            self.assertEqual(res_omit.get("status"), "started")
            self.assertEqual(res_omit.get("fault_duration_sec"), 120)

        # 단일 가구 sensor_fault + fault_duration_sec: 30
        with patch.object(self.manager, "_run_async_worker"):
            handler = DummyRequestHandler(
                self.manager,
                path="/api/start",
                body_dict={"scenario": "sensor_fault", "house": "H001", "fault_duration_sec": 30}
            )
            handler.do_POST()
            self.assertEqual(handler.get_status_code(), 200)
            res = handler.get_response_json()
            self.assertEqual(res.get("status"), "started")
            self.assertEqual(res.get("fault_duration_sec"), 30)

        # 다중 가구 sensor_fault + fault_duration_sec: 180
        with patch.object(self.manager, "_run_async_worker"):
            handler = DummyRequestHandler(
                self.manager,
                path="/api/start",
                body_dict={
                    "households": [
                        {"house": "H001", "scenario": "sensor_fault"},
                        {"house": "H002", "scenario": "peak"}
                    ],
                    "fault_duration_sec": 180
                }
            )
            handler.do_POST()
            self.assertEqual(handler.get_status_code(), 200)
            res = handler.get_response_json()
            self.assertEqual(res.get("status"), "started")
            self.assertEqual(res.get("fault_duration_sec"), 180)

    def test_api_validation_rejects_invalid_duration(self):
        # None (null), 0, -5, 3601, True, False, "30", 12.5 거절
        invalid_cases = [None, 0, -5, 3601, True, False, "30", 12.5]
        for bad_val in invalid_cases:
            handler = DummyRequestHandler(
                self.manager,
                path="/api/start",
                body_dict={"scenario": "sensor_fault", "house": "H001", "fault_duration_sec": bad_val}
            )
            handler.do_POST()
            self.assertEqual(handler.get_status_code(), 400, f"fault_duration_sec={bad_val} must return 400")

    def test_api_validation_rejects_duration_when_no_sensor_fault(self):
        # sensor_fault가 없는 요청에 fault_duration_sec 포함 시 400 거절
        handler = DummyRequestHandler(
            self.manager,
            path="/api/start",
            body_dict={"scenario": "peak", "house": "H001", "fault_duration_sec": 30}
        )
        handler.do_POST()
        self.assertEqual(handler.get_status_code(), 400)
        self.assertIn("sensor_fault", handler.get_response_json().get("message", ""))

        # sensor_fault가 없는 요청에 fault_duration_sec: None (null) 포함 시에도 400 거절
        handler_null = DummyRequestHandler(
            self.manager,
            path="/api/start",
            body_dict={"scenario": "peak", "house": "H001", "fault_duration_sec": None}
        )
        handler_null.do_POST()
        self.assertEqual(handler_null.get_status_code(), 400)
        self.assertIn("sensor_fault", handler_null.get_response_json().get("message", ""))

        # 다중 가구에도 sensor_fault가 없을 때
        handler_multi = DummyRequestHandler(
            self.manager,
            path="/api/start",
            body_dict={
                "households": [
                    {"house": "H001", "scenario": "peak"},
                    {"house": "H002", "scenario": "random"}
                ],
                "fault_duration_sec": 30
            }
        )
        handler_multi.do_POST()
        self.assertEqual(handler_multi.get_status_code(), 400)

    def test_non_destructive_protection_when_invalid_request_arrives(self):
        """기존 실행 중인 시뮬레이터가 잘못된 파라미터 요청(음수, null 등)에도 중단되지 않고 계속 보호되는지 검증"""
        with patch.object(self.manager, "_run_async_worker"):
            # 1. 정상 peak 시뮬레이션 시작
            handler_start = DummyRequestHandler(
                self.manager,
                path="/api/start",
                body_dict={"scenario": "peak", "house": "H001"}
            )
            handler_start.do_POST()
            self.assertEqual(handler_start.get_status_code(), 200)
            self.assertTrue(self.manager.is_running)
            self.assertEqual(self.manager.current_mode, "peak")

            # 2. 잘못된 fault_duration_sec 요청 전송 (-10)
            handler_bad_neg = DummyRequestHandler(
                self.manager,
                path="/api/start",
                body_dict={"scenario": "sensor_fault", "house": "H001", "fault_duration_sec": -10}
            )
            handler_bad_neg.do_POST()
            self.assertEqual(handler_bad_neg.get_status_code(), 400)
            self.assertTrue(self.manager.is_running, "잘못된 요청(-10)으로 기존 시뮬레이터가 중단되면 안 됨")

            # 3. 명시적 null(None) 요청 전송
            handler_bad_null = DummyRequestHandler(
                self.manager,
                path="/api/start",
                body_dict={"scenario": "sensor_fault", "house": "H001", "fault_duration_sec": None}
            )
            handler_bad_null.do_POST()
            self.assertEqual(handler_bad_null.get_status_code(), 400)
            self.assertTrue(self.manager.is_running, "잘못된 요청(null)으로 기존 시뮬레이터가 중단되면 안 됨")
            self.assertEqual(self.manager.current_mode, "peak")


class TestSensorFaultSseDurationField(unittest.TestCase):
    """10. SSE faultDurationSec 필드 전 구간 검증"""

    def test_sse_fault_duration_sec_included_in_all_phases(self):
        manager = SimulatorManager(host="localhost", port=1883, tls_enabled=False)
        sse_events = []
        q = queue.Queue()
        manager.add_subscriber(q)

        def subscriber_listener():
            while True:
                try:
                    data = q.get(timeout=0.2)
                    sse_events.append(data)
                except queue.Empty:
                    if not manager.is_running and len(sse_events) >= 50:
                        break

        import threading
        listener_th = threading.Thread(target=subscriber_listener, daemon=True)
        listener_th.start()

        async def mock_publish(client, house, now_iso=None, qos=1, allow_random=False, metrics=None, **kwargs):
            return {"house": house, "power": 55.0, "devices": []}

        try:
            with patch("simulator.publish_house_power", side_effect=mock_publish), \
                 patch("aiomqtt.Client"), \
                 patch("server.manager.validate_interval", side_effect=lambda x: float(x)):
                manager.start(house="H001", scenario="sensor_fault", fault_duration_sec=30, interval=0.0001)

                max_wait = 5.0
                start_time = time.time()
                while time.time() - start_time < max_wait:
                    if not manager.is_running:
                        break
                    time.sleep(0.01)

                listener_th.join(timeout=2.0)
                self.assertFalse(manager.is_running)
                self.assertEqual(len(sse_events), 50)

                # cycle 1, 10 (고장 전 정상 계측 구간)
                self.assertEqual(sse_events[0]["faultDurationSec"], 30)
                self.assertEqual(sse_events[9]["faultDurationSec"], 30)

                # cycle 11, 40 (고장 결측 구간)
                self.assertEqual(sse_events[10]["faultDurationSec"], 30)
                self.assertEqual(sse_events[39]["faultDurationSec"], 30)

                # cycle 41, 50 (복구 후 및 완료 시점)
                self.assertEqual(sse_events[40]["faultDurationSec"], 30)
                self.assertEqual(sse_events[49]["faultDurationSec"], 30)
        finally:
            manager.remove_subscriber(q)
            manager.stop()
            manager.reset()


if __name__ == "__main__":
    unittest.main()
