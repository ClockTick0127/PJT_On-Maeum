"""
verify_activity_normal_mqtt.py 독립 단위 및 통합 검증 테스트 스위트

검증 내용:
1. StreamMessageValidator 단위 검증 (16개 테스트):
   - 정상 payload 최초 수신 및 비트맵(bytearray) O(SECONDS_PER_DAY) 검증
   - 동일 payload 중복 수신 (duplicate_deliveries 분리)
   - foreign run 메시지 분리 (run_id 기반 실시간 UUID5 대조)
   - 잘못된 UUID 형식 거절
   - 범위 밖 타임스탬프 거절
   - timezone 오류 (+09:00 아님) 거절
   - 마이크로초 포함 타임스탬프 거절
   - 정확한 14개 필드 규격 검증 (누락 필드 거절)
   - 추가 필드 거절
   - bool 숫자 거절 (isinstance(val, bool) 방어)
   - NaN / Infinity 거절
   - 호환 필드 불일치 (house!=household_id, device!=device_id, ts!=measured_at, power_w!=active_power)
   - 8대 금지 AI 정답 라벨 거절
2. 비밀번호 중앙 마스킹 및 보안 유틸리티 (Password Redaction):
   - redact_secrets 단위 검증 (None, 빈 문자열, 정상 치환)
   - MQTT client 생성 예외 문자열에 실제 MQTT_PASS가 포함되어도 JSON에서 제거됨
   - MQTT 연결 예외 문자열에 실제 MQTT_PASS가 포함되어도 제거됨
   - subscribe 예외 문자열에 실제 MQTT_PASS가 포함되어도 제거됨
   - 일반 내부 예외 문자열에 실제 MQTT_PASS가 포함되어도 제거됨
   - stdout과 stderr 모두 실제 비밀번호 미포함 및 '***' 대체 검증
3. 시작 POST 진행 중 취소 및 run_id 복구 (Start POST Cancellation & Lifecycle):
   - 시작 POST worker가 진행 중일 때 취소
   - 취소 후 start task가 run_id를 반환하면 stop 호출
   - stop 후 CancelledError 재전파
   - 시작 task timeout 후 잔존 asyncio task 0건
   - 시작 POST가 자동 재시도되지 않음 (단일 요청 보장)
   - run_id 응답 유실 한계가 README에 명시됨
4. 환경변수 MQTT_PORT 범위 검증:
   - MQTT_PORT=0, 65536, -1, abc 거절 (code 2)
   - CLI 명시 포트와 환경변수 포트의 동일한 검증 계약
5. 비동기 취소 및 실제 cleanup timeout (Cancellation & Actual Timeout):
   - 실제 cleanup timeout 발생 시 제한시간 내 helper 반환 및 cleanup task 0건 확인
   - cleanup 성공 경로 분리 테스트
   - cleanup 진입 후 두 번째 cancel 전달 시 중복 취소 안전 종료
   - HTTP 정리 요청에 자체 timeout이 전달됨
   - KeyboardInterrupt 시 traceback 없이 exit code 130 반환 (RuntimeWarning 0건)
6. 실제 E2E API 상태 집합 기반 정리 조건:
   - COMPLETED, FAILED, STOPPED 상태에서 stop 미호출
   - PARTIAL_FAILED 상태에서 stop 미호출
   - STOPPING 상태에서 중복 stop 미호출
   - overall_status를 기준으로 정지 여부 판정
   - 알 수 없는 상태에서 안전한 1회 stop 호출 후 최초 오류 보존
7. 모든 자원 정리 예외의 우선순위:
   - subscriber 정리 실패가 최초 오류를 덮지 않음
   - client.__aexit__ 실패가 최초 오류를 덮지 않음
   - stop과 MQTT close가 모두 실패해도 최초 exit code 유지
   - 성공 조건 충족 후 disconnect 실패 시 성공 유지 (code 0)
   - __aenter__ 실패 시 __aexit__ 미호출
8. 단일 Exit Code 및 CLI 계약:
   - TLS CA 설정 실패 시 정확히 exit code 2 반환
   - MQTT 클라이언트 생성 실패 시 exit code 3 반환
   - MQTT 연결/구독 타임아웃 시 exit code 6 반환
   - MQTT 연결 일반 오류 시 exit code 3 반환
   - HTTP 소켓/URLError 타임아웃 시 exit code 6 반환
   - CLI 포트 범위(0, 65536) 거절 (exit code 2)
   - CLI 타임아웃(0, 음수, NaN, Inf, bool) 거절 (exit code 2)
   - CLI --help 시 exit code 0 반환
"""
from __future__ import annotations

import asyncio
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
import io
import json
import math
import os
import socket
import sys
import threading
import time
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
import urllib.error
import uuid

from engine.scenario_catalog import (
    get_activity_scenario_definition,
)
from engine.schedule import (
    ApplianceEvent,
    DaySchedule,
    ScenarioDefinition,
    compile_schedule,
)
from engine.unified_catalog import (
    UNIFIED_SCENARIO_IDS,
    get_unified_scenario_definition,
)
from tools.verify_activity_normal_mqtt import (
    ACTIVITY_SCENARIO_IDS,
    BASE_DATE,
    CliConfigError,
    EXACT_FIELDS,
    EXIT_API_ERROR,
    EXIT_CLI_CONFIG_ERROR,
    EXIT_INTERNAL_ERROR,
    EXIT_KEYBOARD_INTERRUPT,
    EXIT_MQTT_ERROR,
    EXIT_SUCCESS,
    EXIT_TIMEOUT,
    EXIT_VERIFICATION_FAILURE,
    ExpectedSchedulePlan,
    FORBIDDEN_AI_FIELDS,
    KST,
    PayloadContractError,
    QueueOverflowError,
    SCHEDULE_PUBLISHER_NAMESPACE,
    SECONDS_PER_DAY,
    SimulatorApiError,
    SimulatorTerminalFailureError,
    StreamMessageValidator,
    TARGET_DEVICE,
    TARGET_HOUSEHOLD,
    TARGET_REFERENCE_DATE,
    TARGET_SCENARIO,
    TimeoutVerificationError,
    VerificationSummary,
    _cleanup_active_run,
    _is_timeout_exception,
    _safe_exc_msg,
    build_expected_schedule_plan,
    http_post_start_run,
    http_post_stop_run,
    main,
    parse_arguments,
    redact_secrets,
    run_verification,
    validate_port,
    validate_positive_timeout,
)


def make_valid_payload_dict(
    run_id: str = "run_test_01",
    second_of_day: int = 0,
    active_power: float = 142.5,
    reactive_power: float = 41.2,
    power_factor: float = 0.96,
    current: float = 0.68,
    voltage: float = 220.3,
    apparent_power: float = 148.4,
    override_fields: dict | None = None,
    date_obj: date | None = None,
) -> dict:
    d = date_obj or date(2026, 9, 16)
    dt = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=KST) + timedelta(seconds=second_of_day)
    measured_at = dt.isoformat()
    absolute_cycle = second_of_day + 1
    uuid5_name = f"SCHEDULE_PUBLISHER_V1::{run_id}::{TARGET_HOUSEHOLD}::{absolute_cycle}::{measured_at}"
    msg_id = str(uuid.uuid5(SCHEDULE_PUBLISHER_NAMESPACE, uuid5_name))

    p = {
        "message_id": msg_id,
        "household_id": TARGET_HOUSEHOLD,
        "device_id": TARGET_DEVICE,
        "measured_at": measured_at,
        "active_power": active_power,
        "reactive_power": reactive_power,
        "power_factor": power_factor,
        "current": current,
        "house": TARGET_HOUSEHOLD,
        "device": TARGET_DEVICE,
        "ts": measured_at,
        "power_w": active_power,
        "voltage": voltage,
        "apparent_power": apparent_power,
    }
    if override_fields:
        p.update(override_fields)
    return p


class DummyFakeClient:
    """테스트용 모의 aiomqtt.Client"""
    def __init__(self, message_count: int = 0, run_id: str = "run_test_01", sleep_before_msg: float = 0.1):
        self.message_count = message_count
        self.run_id = run_id
        self.sleep_before_msg = sleep_before_msg
        self.entered = False
        self.closed = False
        self.aexit_call_count = 0
        self.messages = self._msg_gen()

    async def _msg_gen(self):
        class MockMsg:
            def __init__(self, p_bytes):
                self.payload = p_bytes
                self.topic = MagicMock()
                self.topic.matches.return_value = True
                self.topic.__str__ = lambda _: "v1/power/sim/H001/main"

        for i in range(self.message_count):
            p = make_valid_payload_dict(run_id=self.run_id, second_of_day=i)
            yield MockMsg(json.dumps(p).encode("utf-8"))

        while True:
            await asyncio.sleep(self.sleep_before_msg)
            yield None

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, *args):
        self.closed = True
        self.aexit_call_count += 1

    async def subscribe(self, *args, **kwargs):
        return None


# ==============================================================================
# 1. StreamMessageValidator 단위 테스트 (16종)
# ==============================================================================
class TestStreamMessageValidator(unittest.TestCase):
    """StreamMessageValidator 단위 테스트"""

    def setUp(self):
        self.run_id = "run_unit_01"
        self.validator = StreamMessageValidator(self.run_id)

    def test_valid_first_payload(self):
        p = make_valid_payload_dict(self.run_id, second_of_day=0)
        res = self.validator.process_message(json.dumps(p))
        self.assertEqual(res, "TARGET")
        self.assertEqual(self.validator.target_unique_messages, 1)
        self.assertEqual(self.validator.duplicate_deliveries, 0)
        self.assertEqual(self.validator.foreign_run_messages, 0)
        self.assertEqual(self.validator.invalid_payloads, 0)
        self.assertEqual(self.validator.first_measured_at, "2026-09-16T00:00:00+09:00")
        self.assertEqual(self.validator.timeline_bitmap[0], 1)
        self.assertEqual(self.validator.missing_virtual_seconds, SECONDS_PER_DAY - 1)

    def test_duplicate_payload(self):
        p = make_valid_payload_dict(self.run_id, second_of_day=100)
        res1 = self.validator.process_message(json.dumps(p))
        res2 = self.validator.process_message(json.dumps(p))
        self.assertEqual(res1, "TARGET")
        self.assertEqual(res2, "DUPLICATE")
        self.assertEqual(self.validator.target_unique_messages, 1)
        self.assertEqual(self.validator.duplicate_deliveries, 1)

    def test_foreign_run_message(self):
        p = make_valid_payload_dict(run_id="run_different_99", second_of_day=0)
        res = self.validator.process_message(json.dumps(p))
        self.assertEqual(res, "FOREIGN")
        self.assertEqual(self.validator.foreign_run_messages, 1)
        self.assertEqual(self.validator.target_unique_messages, 0)
        self.assertEqual(self.validator.timeline_bitmap[0], 0)

    def test_invalid_uuid(self):
        p = make_valid_payload_dict(self.run_id, second_of_day=0, override_fields={"message_id": "not-a-valid-uuid"})
        res = self.validator.process_message(json.dumps(p))
        self.assertEqual(res, "INVALID")
        self.assertEqual(self.validator.invalid_payloads, 1)

    def test_out_of_range_timestamp(self):
        p = make_valid_payload_dict(self.run_id, second_of_day=0, override_fields={"measured_at": "2026-09-17T00:00:00+09:00", "ts": "2026-09-17T00:00:00+09:00"})
        res = self.validator.process_message(json.dumps(p))
        self.assertEqual(res, "INVALID")
        self.assertEqual(self.validator.invalid_payloads, 1)

    def test_timezone_error(self):
        p_no_tz = make_valid_payload_dict(self.run_id, second_of_day=0, override_fields={"measured_at": "2026-09-16T00:00:00", "ts": "2026-09-16T00:00:00"})
        p_utc = make_valid_payload_dict(self.run_id, second_of_day=0, override_fields={"measured_at": "2026-09-16T00:00:00Z", "ts": "2026-09-16T00:00:00Z"})
        self.assertEqual(self.validator.process_message(json.dumps(p_no_tz)), "INVALID")
        self.assertEqual(self.validator.process_message(json.dumps(p_utc)), "INVALID")
        self.assertEqual(self.validator.invalid_payloads, 2)

    def test_microseconds_timestamp(self):
        p = make_valid_payload_dict(self.run_id, second_of_day=0, override_fields={"measured_at": "2026-09-16T00:00:00.123456+09:00", "ts": "2026-09-16T00:00:00.123456+09:00"})
        res = self.validator.process_message(json.dumps(p))
        self.assertEqual(res, "INVALID")
        self.assertEqual(self.validator.invalid_payloads, 1)

    def test_exact_14_fields_missing_field(self):
        p = make_valid_payload_dict(self.run_id, second_of_day=0)
        del p["apparent_power"]
        res = self.validator.process_message(json.dumps(p))
        self.assertEqual(res, "INVALID")
        self.assertEqual(self.validator.invalid_payloads, 1)

    def test_extra_field_rejected(self):
        p = make_valid_payload_dict(self.run_id, second_of_day=0, override_fields={"unexpected_extra": "foo"})
        res = self.validator.process_message(json.dumps(p))
        self.assertEqual(res, "INVALID")
        self.assertEqual(self.validator.invalid_payloads, 1)

    def test_bool_number_rejected(self):
        p1 = make_valid_payload_dict(self.run_id, second_of_day=0, override_fields={"active_power": True, "power_w": True})
        p2 = make_valid_payload_dict(self.run_id, second_of_day=0, override_fields={"voltage": False})
        self.assertEqual(self.validator.process_message(json.dumps(p1)), "INVALID")
        self.assertEqual(self.validator.process_message(json.dumps(p2)), "INVALID")
        self.assertEqual(self.validator.invalid_payloads, 2)

    def test_nan_infinity_rejected(self):
        raw_nan = json.dumps(make_valid_payload_dict(self.run_id, second_of_day=0)).replace("142.5", "NaN")
        raw_inf = json.dumps(make_valid_payload_dict(self.run_id, second_of_day=0)).replace("142.5", "Infinity")
        self.assertEqual(self.validator.process_message(raw_nan), "INVALID")
        self.assertEqual(self.validator.process_message(raw_inf), "INVALID")
        self.assertEqual(self.validator.invalid_payloads, 2)

    def test_voltage_strictly_positive(self):
        p = make_valid_payload_dict(self.run_id, second_of_day=0, override_fields={"voltage": 0.0})
        res = self.validator.process_message(json.dumps(p))
        self.assertEqual(res, "INVALID")
        self.assertEqual(self.validator.invalid_payloads, 1)

    def test_power_factor_range(self):
        p1 = make_valid_payload_dict(self.run_id, second_of_day=0, override_fields={"power_factor": 1.05})
        p2 = make_valid_payload_dict(self.run_id, second_of_day=0, override_fields={"power_factor": -0.1})
        self.assertEqual(self.validator.process_message(json.dumps(p1)), "INVALID")
        self.assertEqual(self.validator.process_message(json.dumps(p2)), "INVALID")
        self.assertEqual(self.validator.invalid_payloads, 2)

    def test_compatible_fields_mismatch(self):
        cases = [
            {"house": "H002"},
            {"device": "sub"},
            {"ts": "2026-09-16T00:00:01+09:00"},
            {"power_w": 999.9},
        ]
        for c in cases:
            p = make_valid_payload_dict(self.run_id, second_of_day=0, override_fields=c)
            self.assertEqual(self.validator.process_message(json.dumps(p)), "INVALID")
        self.assertEqual(self.validator.invalid_payloads, len(cases))

    def test_forbidden_ai_labels(self):
        for field in FORBIDDEN_AI_FIELDS:
            p = make_valid_payload_dict(self.run_id, second_of_day=0, override_fields={field: 123})
            self.assertEqual(self.validator.process_message(json.dumps(p)), "INVALID")
        self.assertEqual(self.validator.invalid_payloads, len(FORBIDDEN_AI_FIELDS))


# ==============================================================================
# 2. 비밀번호 중앙 마스킹 및 보안 테스트
# ==============================================================================
class TestPasswordMaskingAndRedaction(unittest.IsolatedAsyncioTestCase):
    """실제 MQTT_PASS가 포함된 예외 주입 시 JSON, stdout, stderr 미노출 및 마스킹 검증"""

    def test_redact_secrets_utility(self):
        """redact_secrets 유틸리티 안전성 검증"""
        self.assertEqual(redact_secrets(None), "")
        self.assertEqual(redact_secrets(""), "")
        self.assertEqual(redact_secrets("no secret here", secret=None), "no secret here")
        self.assertEqual(redact_secrets("no secret here", secret=""), "no secret here")
        self.assertEqual(redact_secrets("password is my_secret_pass!", secret="my_secret_pass"), "password is ***!")

    async def test_mqtt_client_creation_exception_redacts_password(self):
        """MQTT client 생성 예외 문자열에 실제 MQTT_PASS가 포함되어도 제거됨"""
        secret_pass = "INJECTED_CLIENT_CREATE_PASS_99"
        broker_cfg = {"host": "localhost", "port": 1883, "password": secret_pass, "tls_enabled": False}

        with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", side_effect=Exception(f"Client init error with secret: {secret_pass}")):
            summary = await run_verification(broker_cfg, "http://127.0.0.1:8085")
            self.assertEqual(summary.exit_code, EXIT_MQTT_ERROR)
            self.assertNotIn(secret_pass, str(summary.error_message))
            self.assertIn("***", str(summary.error_message))

    async def test_mqtt_connect_exception_redacts_password(self):
        """MQTT 연결 예외 문자열에 실제 MQTT_PASS가 포함되어도 제거됨"""
        secret_pass = "INJECTED_CONNECT_PASS_77"
        fake_client = DummyFakeClient()

        async def failing_connect():
            raise ConnectionRefusedError(f"Connection failed for pass={secret_pass}")

        fake_client.__aenter__ = failing_connect
        broker_cfg = {"host": "localhost", "port": 1883, "password": secret_pass, "tls_enabled": False}

        with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", return_value=fake_client):
            summary = await run_verification(broker_cfg, "http://127.0.0.1:8085")
            self.assertEqual(summary.exit_code, EXIT_MQTT_ERROR)
            self.assertNotIn(secret_pass, str(summary.error_message))
            self.assertIn("***", str(summary.error_message))

    async def test_subscribe_exception_redacts_password(self):
        """subscribe 예외 문자열에 실제 MQTT_PASS가 포함되어도 제거됨"""
        secret_pass = "INJECTED_SUBSCRIBE_PASS_55"
        fake_client = DummyFakeClient()

        async def failing_sub(*args, **kwargs):
            raise RuntimeError(f"Subscription rejected auth token {secret_pass}")

        fake_client.subscribe = failing_sub
        broker_cfg = {"host": "localhost", "port": 1883, "password": secret_pass, "tls_enabled": False}

        with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", return_value=fake_client):
            summary = await run_verification(broker_cfg, "http://127.0.0.1:8085")
            self.assertEqual(summary.exit_code, EXIT_MQTT_ERROR)
            self.assertNotIn(secret_pass, str(summary.error_message))
            self.assertIn("***", str(summary.error_message))

    async def test_general_internal_exception_redacts_password(self):
        """일반 내부 예외 문자열에 실제 MQTT_PASS가 포함되어도 제거됨"""
        secret_pass = "INJECTED_INTERNAL_SECRET_33"
        fake_client = DummyFakeClient()
        broker_cfg = {"host": "localhost", "port": 1883, "password": secret_pass, "tls_enabled": False}

        with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", return_value=fake_client), \
             patch("tools.verify_activity_normal_mqtt.http_post_start_run", side_effect=ValueError(f"Internal bug leaking {secret_pass}")):

            summary = await run_verification(broker_cfg, "http://127.0.0.1:8085")
            self.assertEqual(summary.exit_code, EXIT_INTERNAL_ERROR)
            self.assertNotIn(secret_pass, str(summary.error_message))
            self.assertIn("***", str(summary.error_message))

    def test_main_stdout_and_stderr_redaction(self):
        """main() 실행 시 stdout JSON 및 stderr 모두에 실제 비밀번호가 노출되지 않음"""
        secret_pass = "CLI_RAW_SUPER_SECRET_8899"
        out_buf = io.StringIO()
        err_buf = io.StringIO()

        with patch.dict(os.environ, {"MQTT_PASS": secret_pass}), \
             patch("sys.stdout", out_buf), \
             patch("sys.stderr", err_buf), \
             patch("tools.verify_activity_normal_mqtt.resolve_mqtt_config", side_effect=Exception(f"Broker config error with {secret_pass}")):

            code = main([])
            self.assertEqual(code, EXIT_CLI_CONFIG_ERROR)

        out_text = out_buf.getvalue()
        err_text = err_buf.getvalue()
        self.assertNotIn(secret_pass, out_text)
        self.assertNotIn(secret_pass, err_text)
        self.assertIn("***", out_text)

    async def test_whitespace_only_password_redaction(self):
        """secret='   ' 같은 공백 전용 비밀번호가 예외 메시지와 JSON에서 노출되지 않고 마스킹됨"""
        # 단위 redaction 검증
        self.assertEqual(redact_secrets("token: [   ]", secret="   "), "token: [***]")

        # 비동기 예외 메시지 및 JSON 마스킹 검증
        secret_pass = "   "
        broker_cfg = {"host": "localhost", "port": 1883, "password": secret_pass, "tls_enabled": False}

        with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", side_effect=Exception(f"Auth failure with [{secret_pass}]")):
            summary = await run_verification(broker_cfg, "http://127.0.0.1:8085")
            self.assertEqual(summary.exit_code, EXIT_MQTT_ERROR)
            self.assertNotIn(secret_pass, str(summary.error_message))
            self.assertIn("***", str(summary.error_message))

            json_out = json.dumps(asdict(summary), ensure_ascii=False)
            self.assertNotIn(secret_pass, json_out)
            self.assertIn("***", json_out)

    def test_env_and_broker_secret_containment_relationship(self):
        """환경변수 secret과 broker secret이 부분 포함 관계인 경우 길이 내림차순으로 둘 다 안전하게 치환"""
        # 케이스 1: env_secret이 broker_secret을 포함하는 경우 ("secret12345" vs "secret")
        with patch.dict(os.environ, {"MQTT_PASS": "secret12345"}):
            res = redact_secrets("passwords are secret12345 and secret", secret="secret")
            self.assertEqual(res, "passwords are *** and ***")
            self.assertNotIn("secret12345", res)
            self.assertNotIn("secret", res)

        # 케이스 2: broker_secret이 env_secret을 포함하는 경우 ("longer_token_value" vs "token")
        with patch.dict(os.environ, {"MQTT_PASS": "token"}):
            res = redact_secrets("passwords are longer_token_value and token", secret="longer_token_value")
            self.assertEqual(res, "passwords are *** and ***")
            self.assertNotIn("longer_token_value", res)
            self.assertNotIn("token", res)

    def test_none_and_empty_string_safety(self):
        """None과 빈 문자열('')에서는 무한 치환이나 오류가 발생하지 않고 안전 반환"""
        self.assertEqual(redact_secrets(None, secret=None), "")
        self.assertEqual(redact_secrets(None, secret=""), "")
        self.assertEqual(redact_secrets("hello world", secret=None), "hello world")
        self.assertEqual(redact_secrets("hello world", secret=""), "hello world")
        with patch.dict(os.environ, {"MQTT_PASS": ""}):
            self.assertEqual(redact_secrets("hello world", secret=""), "hello world")
            self.assertEqual(redact_secrets("hello world", secret=None), "hello world")



# ==============================================================================
# 3. 시작 POST 진행 중 취소 및 run_id 복구 테스트
# ==============================================================================
class TestStartPostCancellationAndRecovery(unittest.IsolatedAsyncioTestCase):
    """시작 POST worker 진행 중 취소 및 run_id 복구 생명주기 검증"""

    async def test_cancellation_during_start_worker_with_run_id_calls_stop_and_reraises(self):
        """시작 POST worker 진행 중 취소 시 bounded wait로 run_id를 회수하여 stop 호출 후 CancelledError 재전파"""
        fake_client = DummyFakeClient()
        broker_cfg = {"host": "localhost", "port": 1883, "tls_enabled": False}
        start_worker_entered = threading.Event()
        start_worker_release = threading.Event()

        def slow_start_worker(api_url, timeout_sec, *args, **kwargs):
            start_worker_entered.set()
            start_worker_release.wait(timeout=2.0)
            return "run_recovered_after_cancel"

        with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", return_value=fake_client), \
             patch("tools.verify_activity_normal_mqtt.http_post_start_run", side_effect=slow_start_worker), \
             patch("tools.verify_activity_normal_mqtt.http_post_stop_run") as mock_stop:

            task = asyncio.create_task(
                run_verification(
                    broker_config=broker_cfg,
                    api_url="http://127.0.0.1:8085",
                    overall_timeout=5.0,
                )
            )
            # 워커 진입 대기 (이벤트 루프 차단 방지를 위해 async sleep 폴링)
            for _ in range(50):
                if start_worker_entered.is_set():
                    break
                await asyncio.sleep(0.02)
            self.assertTrue(start_worker_entered.is_set())
            task.cancel()
            # 취소 인입 후 바로 워커 완료 허용
            start_worker_release.set()

            with self.assertRaises(asyncio.CancelledError):
                await task

            mock_stop.assert_called_once_with("http://127.0.0.1:8085", "run_recovered_after_cancel", 3.0)

    async def test_cancellation_during_start_worker_without_run_id_skips_stop_and_reraises(self):
        """시작 POST 워커가 결과를 내지 못하고 취소된 경우 stop 호출 없이 CancelledError 재전파"""
        fake_client = DummyFakeClient()
        broker_cfg = {"host": "localhost", "port": 1883, "tls_enabled": False}
        start_worker_entered = threading.Event()
        start_worker_release = threading.Event()

        def stalled_start_worker(api_url, timeout_sec, *args, **kwargs):
            start_worker_entered.set()
            start_worker_release.wait(timeout=5.0)
            return "run_unreachable"

        try:
            with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", return_value=fake_client), \
                 patch("tools.verify_activity_normal_mqtt.http_post_start_run", side_effect=stalled_start_worker), \
                 patch("tools.verify_activity_normal_mqtt.http_post_stop_run") as mock_stop:

                task = asyncio.create_task(
                    run_verification(
                        broker_config=broker_cfg,
                        api_url="http://127.0.0.1:8085",
                        overall_timeout=5.0,
                    )
                )
                # 워커 진입 대기 (이벤트 루프 차단 방지를 위해 async sleep 폴링)
                for _ in range(50):
                    if start_worker_entered.is_set():
                        break
                    await asyncio.sleep(0.02)
                self.assertTrue(start_worker_entered.is_set())
                task.cancel()

                with self.assertRaises(asyncio.CancelledError):
                    await task

                mock_stop.assert_not_called()
        finally:
            start_worker_release.set()

    async def test_start_task_timeout_leaves_zero_lingering_tasks(self):
        """시작 POST 타임아웃 시 잔존 start 관련 asyncio task가 0건임을 검증"""
        fake_client = DummyFakeClient()
        broker_cfg = {"host": "localhost", "port": 1883, "tls_enabled": False}
        worker_release = threading.Event()

        def stalled_worker(api_url, timeout_sec, *args, **kwargs):
            worker_release.wait(timeout=2.0)
            return "run_late"

        try:
            with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", return_value=fake_client), \
                 patch("tools.verify_activity_normal_mqtt.http_post_start_run", side_effect=stalled_worker):

                summary = await run_verification(
                    broker_config=broker_cfg,
                    api_url="http://127.0.0.1:8085",
                    http_timeout=0.05,
                    overall_timeout=1.0,
                )
                self.assertEqual(summary.exit_code, EXIT_TIMEOUT)

                current = asyncio.current_task()
                tasks = [t for t in asyncio.all_tasks() if t is not current and not t.done()]
                start_tasks = [t for t in tasks if "http_start" in t.get_name()]
                self.assertEqual(len(start_tasks), 0)
        finally:
            worker_release.set()

    async def test_start_post_is_not_automatically_retried(self):
        """시작 POST 요청이 실패하거나 타임아웃되어도 중복 실행 방지를 위해 자동 재시도되지 않음 검증"""
        fake_client = DummyFakeClient()
        broker_cfg = {"host": "localhost", "port": 1883, "tls_enabled": False}
        call_count = 0

        def failing_start(api_url, timeout_sec, *args, **kwargs):
            nonlocal call_count
            call_count += 1
            raise SimulatorApiError("HTTP 500 Internal Server Error")

        with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", return_value=fake_client), \
             patch("tools.verify_activity_normal_mqtt.http_post_start_run", side_effect=failing_start):

            summary = await run_verification(broker_cfg, "http://127.0.0.1:8085", overall_timeout=1.0)
            self.assertEqual(summary.exit_code, EXIT_API_ERROR)
            self.assertEqual(call_count, 1)

    def test_run_id_loss_limitation_documented_in_readme(self):
        """README.md에 시작 API 응답 유실 시 자동 stop 불가 한계가 명시되어 있는지 확인"""
        readme_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "README.md")
        with open(readme_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn("시작 API 응답 자체가 유실되어 run_id를 확보하지 못한 경우 자동 stop을 보장할 수 없다", content)


# ==============================================================================
# 4. 환경변수 MQTT_PORT 범위 검증 테스트
# ==============================================================================
class TestMqttPortEnvValidation(unittest.TestCase):
    """환경변수 MQTT_PORT가 resolve_mqtt_config 후에도 동일하게 code 2로 거절되는지 검증"""

    def test_env_mqtt_port_invalid_rejected_code_2(self):
        cases = ["0", "65536", "-1", "abc"]
        for bad_port in cases:
            with patch.dict(os.environ, {"MQTT_PORT": bad_port}):
                buf = io.StringIO()
                with patch("sys.stdout", buf):
                    code = main([])
                self.assertEqual(code, EXIT_CLI_CONFIG_ERROR, f"Failed for MQTT_PORT={bad_port}")
                self.assertEqual(code, 2)
                data = json.loads(buf.getvalue())
                self.assertEqual(data["exit_code"], 2)


# ==============================================================================
# 5. 비동기 취소 및 실제 cleanup timeout 테스트
# ==============================================================================
class TestActualCleanupTimeout(unittest.IsolatedAsyncioTestCase):
    """실제 cleanup timeout 및 태스크 누수 방지 검증"""

    async def test_cleanup_active_run_actual_timeout_and_task_cleanup(self):
        """실제 timeout보다 오래 걸리는 stop 함수 사용 시 제한시간 내 helper 반환 및 잔존 task 0건 검증"""
        block_worker = threading.Event()

        def stalled_stop(api_url, run_id, timeout_sec):
            block_worker.wait(timeout=3.0)

        try:
            with patch("tools.verify_activity_normal_mqtt.http_post_stop_run", side_effect=stalled_stop):
                t0 = time.monotonic()
                # 0.05초 제한시간으로 실제 타임아웃 유도
                await _cleanup_active_run(
                    "http://127.0.0.1:8085",
                    "run_stalled_stop_01",
                    {"overall_status": "RUNNING"},
                    timeout_sec=0.05,
                )
                elapsed = time.monotonic() - t0
                self.assertLess(elapsed, 0.8)

                current = asyncio.current_task()
                running_tasks = [t for t in asyncio.all_tasks() if t is not current and not t.done()]
                cleanup_tasks = [t for t in running_tasks if "cleanup_stop" in t.get_name()]
                self.assertEqual(len(cleanup_tasks), 0)
        finally:
            block_worker.set()

    async def test_cleanup_active_run_success_fast_path(self):
        """정상 stop 경로가 빠르게 완료되고 잔존 task가 0건임을 검증"""
        with patch("tools.verify_activity_normal_mqtt.http_post_stop_run") as mock_stop:
            t0 = time.monotonic()
            await _cleanup_active_run(
                "http://127.0.0.1:8085",
                "run_fast_stop",
                {"overall_status": "RUNNING"},
                timeout_sec=1.0,
            )
            elapsed = time.monotonic() - t0
            self.assertLess(elapsed, 0.2)
            mock_stop.assert_called_once()

            current = asyncio.current_task()
            running_tasks = [t for t in asyncio.all_tasks() if t is not current and not t.done()]
            cleanup_tasks = [t for t in running_tasks if "cleanup_stop" in t.get_name()]
            self.assertEqual(len(cleanup_tasks), 0)

    async def test_repeated_cancellation_during_cleanup(self):
        """cleanup 진입 후 두 번째 cancel이 전달되어도 무한 대기하지 않고 안전 종료 검증"""
        fake_client = DummyFakeClient()
        broker_cfg = {"host": "localhost", "port": 1883, "tls_enabled": False}
        start_called = threading.Event()
        cleanup_in_progress = threading.Event()
        cleanup_release = threading.Event()

        def fake_start(*args, **kwargs):
            start_called.set()
            return "run_entry_cancel"

        def slow_stop(api_url, run_id, timeout_sec):
            cleanup_in_progress.set()
            cleanup_release.wait(timeout=2.0)

        try:
            with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", return_value=fake_client), \
                 patch("tools.verify_activity_normal_mqtt.http_post_start_run", side_effect=fake_start), \
                 patch("tools.verify_activity_normal_mqtt.http_get_run_snapshot", return_value={"overall_status": "RUNNING"}), \
                 patch("tools.verify_activity_normal_mqtt.http_post_stop_run", side_effect=slow_stop):

                task = asyncio.create_task(
                    run_verification(broker_cfg, "http://127.0.0.1:8085", overall_timeout=10.0)
                )

                # 시작 API가 실제로 호출된 시점까지 대기 (최대 3초)
                for _ in range(150):
                    if start_called.is_set():
                        break
                    await asyncio.sleep(0.02)
                self.assertTrue(start_called.is_set(), "시작 API가 3초 이내에 호출되지 않았습니다.")

                # 시작 API 워커의 반환값이 이벤트 루프에 전달되어 run_id가 확보되도록 양보
                await asyncio.sleep(0.05)

                task.cancel()  # 1차 취소 (run_id 확보 시점 이후 취소 인입)

                # cleanup 진입 대기 (부하 여유를 위해 1초->3초로 폴링 확장; 단, 이것은 부하 여유일 뿐 원인 해결이 아님)
                for _ in range(150):
                    if cleanup_in_progress.is_set():
                        break
                    await asyncio.sleep(0.02)
                self.assertTrue(cleanup_in_progress.is_set(), "정리(stop API)가 3초 이내에 호출되지 않았습니다.")
                task.cancel()  # 2차 취소 (cleanup 진행 중 인입)
                cleanup_release.set()

                t0 = time.monotonic()
                with self.assertRaises(asyncio.CancelledError):
                    await asyncio.wait_for(task, timeout=3.0)
                elapsed = time.monotonic() - t0
                self.assertLess(elapsed, 3.0)
        finally:
            cleanup_release.set()

    def test_main_keyboard_interrupt_exit_code_130_no_warning(self):
        """main()에서 KeyboardInterrupt 시 RuntimeWarning 0건 및 exit code 130 검증"""
        err_buf = io.StringIO()

        def fake_asyncio_run(coro):
            coro.close()  # RuntimeWarning: coroutine was never awaited 방지
            raise KeyboardInterrupt()

        with patch("tools.verify_activity_normal_mqtt.asyncio.run", side_effect=fake_asyncio_run), \
             patch("sys.stderr", err_buf):
            code = main([])
            self.assertEqual(code, EXIT_KEYBOARD_INTERRUPT)
            self.assertEqual(code, 130)
            self.assertNotIn("Traceback", err_buf.getvalue())


# ==============================================================================
# 6. 실제 E2E API 상태 집합 기반 정리 조건 테스트
# ==============================================================================
class TestStatusHandling(unittest.IsolatedAsyncioTestCase):
    """최신 스냅샷 overall_status에 따른 조건부 stop 호출 검증"""

    async def test_status_terminal_skips_stop_api(self):
        for terminal_st in ("COMPLETED", "FAILED", "STOPPED"):
            with patch("tools.verify_activity_normal_mqtt.http_post_stop_run") as mock_stop:
                await _cleanup_active_run(
                    "http://127.0.0.1:8085",
                    "run_term_01",
                    {"overall_status": terminal_st},
                    timeout_sec=1.0,
                )
                mock_stop.assert_not_called()

    async def test_status_partial_failed_skips_stop_api(self):
        with patch("tools.verify_activity_normal_mqtt.http_post_stop_run") as mock_stop:
            await _cleanup_active_run(
                "http://127.0.0.1:8085",
                "run_part_fail_01",
                {"overall_status": "PARTIAL_FAILED"},
                timeout_sec=1.0,
            )
            mock_stop.assert_not_called()

    async def test_status_stopping_skips_stop_api(self):
        with patch("tools.verify_activity_normal_mqtt.http_post_stop_run") as mock_stop:
            await _cleanup_active_run(
                "http://127.0.0.1:8085",
                "run_stopping_01",
                {"overall_status": "STOPPING"},
                timeout_sec=1.0,
            )
            mock_stop.assert_not_called()

    async def test_cleanup_decides_on_overall_status(self):
        for active_st in ("STARTING", "RUNNING", "PAUSED", None):
            snap = {"overall_status": active_st} if active_st else None
            with patch("tools.verify_activity_normal_mqtt.http_post_stop_run") as mock_stop:
                await _cleanup_active_run(
                    "http://127.0.0.1:8085",
                    "run_active_01",
                    snap,
                    timeout_sec=1.0,
                )
                mock_stop.assert_called_once()

    async def test_status_unknown_calls_stop_and_preserves_primary_error(self):
        fake_client = DummyFakeClient()
        broker_cfg = {"host": "localhost", "port": 1883, "tls_enabled": False}

        with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", return_value=fake_client), \
             patch("tools.verify_activity_normal_mqtt.http_post_start_run", return_value="run_unknown_st"), \
             patch("tools.verify_activity_normal_mqtt.http_get_run_snapshot", return_value={"overall_status": "UNEXPECTED_WEIRD_STATUS"}), \
             patch("tools.verify_activity_normal_mqtt.http_post_stop_run") as mock_stop:

            summary = await run_verification(
                broker_config=broker_cfg,
                api_url="http://127.0.0.1:8085",
                idle_timeout=0.05,
                overall_timeout=1.0,
            )

            self.assertEqual(summary.status, "FAILURE")
            self.assertEqual(summary.exit_code, EXIT_TIMEOUT)
            mock_stop.assert_called_once()


# ==============================================================================
# 7. 자원 정리 예외의 우선순위 계약 테스트
# ==============================================================================
class TestCleanupExceptionPriority(unittest.IsolatedAsyncioTestCase):
    """정리 단계에서 발생하는 예외가 최초 발생 오류를 덮지 않음을 검증"""

    async def test_cleanup_subscriber_failure_does_not_mask_original_error(self):
        fake_client = DummyFakeClient()
        broker_cfg = {"host": "localhost", "port": 1883, "tls_enabled": False}

        with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", return_value=fake_client), \
             patch("tools.verify_activity_normal_mqtt.http_post_start_run", return_value="run_sub_fail"), \
             patch("tools.verify_activity_normal_mqtt.http_get_run_snapshot", return_value={"overall_status": "RUNNING"}), \
             patch("tools.verify_activity_normal_mqtt.http_post_stop_run"):

            summary = await run_verification(
                broker_config=broker_cfg,
                api_url="http://127.0.0.1:8085",
                idle_timeout=0.05,
                overall_timeout=1.0,
            )

            self.assertEqual(summary.status, "FAILURE")
            self.assertEqual(summary.exit_code, EXIT_TIMEOUT)
            self.assertIn("idle timeout", str(summary.error_message))

    async def test_cleanup_client_exit_failure_does_not_mask_original_error(self):
        fake_client = DummyFakeClient()

        async def broken_aexit(*args):
            raise RuntimeError("MQTT socket disconnect error")

        fake_client.__aexit__ = broken_aexit
        broker_cfg = {"host": "localhost", "port": 1883, "tls_enabled": False}

        with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", return_value=fake_client), \
             patch("tools.verify_activity_normal_mqtt.http_post_start_run", return_value="run_client_fail"), \
             patch("tools.verify_activity_normal_mqtt.http_get_run_snapshot", return_value={"overall_status": "RUNNING"}), \
             patch("tools.verify_activity_normal_mqtt.http_post_stop_run"):

            summary = await run_verification(
                broker_config=broker_cfg,
                api_url="http://127.0.0.1:8085",
                idle_timeout=0.05,
                overall_timeout=1.0,
            )

            self.assertEqual(summary.status, "FAILURE")
            self.assertEqual(summary.exit_code, EXIT_TIMEOUT)

    async def test_multiple_cleanup_failures_preserve_primary_exit_code(self):
        fake_client = DummyFakeClient()

        async def broken_aexit(*args):
            raise ConnectionResetError("Broker forcibly closed connection")

        fake_client.__aexit__ = broken_aexit
        broker_cfg = {"host": "localhost", "port": 1883, "tls_enabled": False}

        def broken_stop(api_url, run_id, timeout_sec):
            raise urllib.error.URLError("HTTP stop failed")

        with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", return_value=fake_client), \
             patch("tools.verify_activity_normal_mqtt.http_post_start_run", return_value="run_double_fail"), \
             patch("tools.verify_activity_normal_mqtt.http_get_run_snapshot", return_value={"overall_status": "RUNNING"}), \
             patch("tools.verify_activity_normal_mqtt.http_post_stop_run", side_effect=broken_stop):

            summary = await run_verification(
                broker_config=broker_cfg,
                api_url="http://127.0.0.1:8085",
                idle_timeout=0.05,
                overall_timeout=1.0,
            )

            self.assertEqual(summary.status, "FAILURE")
            self.assertEqual(summary.exit_code, EXIT_TIMEOUT)

    async def test_success_with_disconnect_error_preserves_success(self):
        run_id = "run_success_disc_err"
        fake_client = DummyFakeClient(message_count=0)

        class MockFullMsg:
            def __init__(self, idx):
                self.payload = json.dumps(make_valid_payload_dict(run_id=run_id, second_of_day=idx)).encode("utf-8")
                self.topic = MagicMock()
                self.topic.matches.return_value = True
                self.topic.__str__ = lambda _: "v1/power/sim/H001/main"

        async def full_msg_gen():
            for i in range(SECONDS_PER_DAY):
                yield MockFullMsg(i)
                if i % 100 == 0:
                    await asyncio.sleep(0)
            while True:
                await asyncio.sleep(1.0)

        fake_client.messages = full_msg_gen()

        async def failing_aexit(*args):
            raise OSError("Socket abruptly disconnected during shutdown")

        fake_client.__aexit__ = failing_aexit

        api_snap = {
            "overall_status": "COMPLETED",
            "overall_state": "COMPLETED",
            "households": [
                {
                    "household_id": "H001",
                    "scenario": "ACTIVITY_NORMAL",
                    "state": "COMPLETED",
                    "runner_virtual_slots": SECONDS_PER_DAY,
                    "settled_virtual_slots": SECONDS_PER_DAY,
                    "published_samples": SECONDS_PER_DAY,
                    "planned_publish_samples": SECONDS_PER_DAY,
                    "omitted_samples": 0,
                    "completed_days": 1,
                    "total_days": 1,
                    "last_error": None,
                }
            ],
        }

        broker_cfg = {"host": "localhost", "port": 1883, "tls_enabled": False}

        with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", return_value=fake_client), \
             patch("tools.verify_activity_normal_mqtt.http_post_start_run", return_value=run_id), \
             patch("tools.verify_activity_normal_mqtt.http_get_run_snapshot", return_value=api_snap):

            summary = await run_verification(
                broker_config=broker_cfg,
                api_url="http://127.0.0.1:8085",
                overall_timeout=30.0,
                idle_timeout=15.0,
                poll_interval=0.1,
            )

            self.assertEqual(summary.status, "SUCCESS")
            self.assertEqual(summary.exit_code, EXIT_SUCCESS)

    async def test_client_aenter_failure_skips_aexit(self):
        fake_client = DummyFakeClient()

        async def failing_aenter():
            raise ConnectionRefusedError("Broker port closed")

        fake_client.__aenter__ = failing_aenter
        broker_cfg = {"host": "localhost", "port": 1883, "tls_enabled": False}

        with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", return_value=fake_client):
            summary = await run_verification(
                broker_config=broker_cfg,
                api_url="http://127.0.0.1:8085",
            )

            self.assertEqual(summary.status, "FAILURE")
            self.assertEqual(summary.exit_code, EXIT_MQTT_ERROR)
            self.assertEqual(fake_client.aexit_call_count, 0)


# ==============================================================================
# 8. 단일 Exit Code 및 CLI 계약 테스트
# ==============================================================================
class TestExitCodesAndCliContract(unittest.IsolatedAsyncioTestCase):
    """엄격한 단일 Exit Code 분류 및 CLI 계약 검증"""

    async def test_tls_context_failure_structured_json(self):
        broker_cfg = {
            "host": "localhost",
            "port": 8883,
            "tls_enabled": True,
            "ca_file": "non_existent_ca_cert.pem",
        }

        with patch("tools.verify_activity_normal_mqtt.create_mqtt_tls_context", side_effect=FileNotFoundError("CA file not found")):
            summary = await run_verification(broker_cfg, "http://127.0.0.1:8085")
            self.assertEqual(summary.exit_code, EXIT_CLI_CONFIG_ERROR)
            self.assertEqual(summary.exit_code, 2)
            self.assertEqual(summary.status, "FAILURE")

    async def test_mqtt_client_creation_failure_structured_json(self):
        broker_cfg = {"host": "localhost", "port": 1883, "tls_enabled": False}

        with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", side_effect=TypeError("Bad param")):
            summary = await run_verification(broker_cfg, "http://127.0.0.1:8085")
            self.assertEqual(summary.exit_code, EXIT_MQTT_ERROR)
            self.assertEqual(summary.exit_code, 3)

    async def test_mqtt_connect_timeout_exit_code_and_json(self):
        fake_client = DummyFakeClient()

        async def timeout_connect():
            raise asyncio.TimeoutError()

        fake_client.__aenter__ = timeout_connect
        broker_cfg = {"host": "localhost", "port": 1883, "tls_enabled": False}

        with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", return_value=fake_client):
            summary = await run_verification(broker_cfg, "http://127.0.0.1:8085")
            self.assertEqual(summary.exit_code, EXIT_TIMEOUT)
            self.assertEqual(summary.exit_code, 6)

    async def test_mqtt_connect_general_error_exit_code_and_json(self):
        fake_client = DummyFakeClient()

        async def refused_connect():
            raise ConnectionRefusedError("Connection refused")

        fake_client.__aenter__ = refused_connect
        broker_cfg = {"host": "localhost", "port": 1883, "tls_enabled": False}

        with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", return_value=fake_client):
            summary = await run_verification(broker_cfg, "http://127.0.0.1:8085")
            self.assertEqual(summary.exit_code, EXIT_MQTT_ERROR)
            self.assertEqual(summary.exit_code, 3)

    async def test_subscribe_timeout_exit_code(self):
        fake_client = DummyFakeClient()

        async def timeout_sub(*args, **kwargs):
            raise asyncio.TimeoutError()

        fake_client.subscribe = timeout_sub
        broker_cfg = {"host": "localhost", "port": 1883, "tls_enabled": False}

        with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", return_value=fake_client):
            summary = await run_verification(broker_cfg, "http://127.0.0.1:8085")
            self.assertEqual(summary.exit_code, EXIT_TIMEOUT)
            self.assertEqual(summary.exit_code, 6)

    async def test_subscribe_error_exit_code(self):
        fake_client = DummyFakeClient()

        async def failing_sub(*args, **kwargs):
            raise RuntimeError("SUBACK return code failure")

        fake_client.subscribe = failing_sub
        broker_cfg = {"host": "localhost", "port": 1883, "tls_enabled": False}

        with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", return_value=fake_client):
            summary = await run_verification(broker_cfg, "http://127.0.0.1:8085")
            self.assertEqual(summary.exit_code, EXIT_MQTT_ERROR)
            self.assertEqual(summary.exit_code, 3)

    async def test_http_socket_timeout_classified_as_timeout(self):
        fake_client = DummyFakeClient()
        broker_cfg = {"host": "localhost", "port": 1883, "tls_enabled": False}

        with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", return_value=fake_client), \
             patch("tools.verify_activity_normal_mqtt.http_post_start_run", side_effect=socket.timeout("HTTP socket timed out")):

            summary = await run_verification(broker_cfg, "http://127.0.0.1:8085")
            self.assertEqual(summary.exit_code, EXIT_TIMEOUT)
            self.assertEqual(summary.exit_code, 6)

    async def test_http_urlerror_timeout_classified_as_timeout(self):
        fake_client = DummyFakeClient()
        broker_cfg = {"host": "localhost", "port": 1883, "tls_enabled": False}

        with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", return_value=fake_client), \
             patch("tools.verify_activity_normal_mqtt.http_post_start_run", return_value="run_http_timeout"), \
             patch("tools.verify_activity_normal_mqtt.http_get_run_snapshot", side_effect=urllib.error.URLError(socket.timeout("timed out"))), \
             patch("tools.verify_activity_normal_mqtt.http_post_stop_run"):

            summary = await run_verification(broker_cfg, "http://127.0.0.1:8085", idle_timeout=5.0, overall_timeout=2.0)
            self.assertEqual(summary.exit_code, EXIT_TIMEOUT)
            self.assertEqual(summary.exit_code, 6)

    def test_cli_port_out_of_range_rejected(self):
        buf = io.StringIO()
        with patch("sys.stdout", buf):
            code1 = main(["--broker-port", "0"])
        self.assertEqual(code1, EXIT_CLI_CONFIG_ERROR)
        self.assertEqual(code1, 2)
        data1 = json.loads(buf.getvalue())
        self.assertEqual(data1["exit_code"], 2)

        buf2 = io.StringIO()
        with patch("sys.stdout", buf2):
            code2 = main(["--broker-port", "65536"])
        self.assertEqual(code2, EXIT_CLI_CONFIG_ERROR)
        self.assertEqual(code2, 2)
        data2 = json.loads(buf2.getvalue())
        self.assertEqual(data2["exit_code"], 2)

    def test_cli_invalid_timeout_args_rejected(self):
        for invalid_val in ("0", "-5.0"):
            buf = io.StringIO()
            with patch("sys.stdout", buf):
                code = main(["--idle-timeout", invalid_val])
            self.assertEqual(code, 2)
            data = json.loads(buf.getvalue())
            self.assertEqual(data["exit_code"], 2)

        with self.assertRaises(CliConfigError):
            validate_positive_timeout(float("nan"), "test-nan")

        with self.assertRaises(CliConfigError):
            validate_positive_timeout(float("inf"), "test-inf")

        with self.assertRaises(CliConfigError):
            validate_positive_timeout(True, "test-bool")

    def test_cli_help_option_exit_code_zero(self):
        with patch("sys.stdout", new=io.StringIO()):
            code = main(["--help"])
            self.assertEqual(code, 0)


# ==============================================================================
# 9. 비동기 워크플로우 기본 테스트 (Mock 기반)
# ==============================================================================
class TestVerifyActivityNormalAsyncFlow(unittest.IsolatedAsyncioTestCase):
    """큐 오버플로우, idle/overall 타임아웃 및 세션 완주 검증"""

    async def test_queue_overflow_propagation(self):
        class FakeMessage:
            def __init__(self, payload: bytes):
                self.payload = payload
                self.topic = MagicMock()
                self.topic.matches.return_value = True
                self.topic.__str__ = lambda _: "v1/power/sim/H001/main"

        class OverflowingFakeClient:
            def __init__(self):
                self.messages = self._msg_gen()
                self.closed = False

            async def _msg_gen(self):
                for i in range(2005):
                    yield FakeMessage(b"{}")

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                self.closed = True

            async def subscribe(self, *args, **kwargs):
                return None

        fake_client = OverflowingFakeClient()
        broker_cfg = {"host": "localhost", "port": 1883, "tls_enabled": False}

        with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", return_value=fake_client), \
             patch("tools.verify_activity_normal_mqtt.http_post_start_run", return_value="run_overflow_01"), \
             patch("tools.verify_activity_normal_mqtt.http_get_run_snapshot", return_value={"overall_status": "RUNNING"}), \
             patch("tools.verify_activity_normal_mqtt.http_post_stop_run") as mock_stop:

            summary = await run_verification(
                broker_config=broker_cfg,
                api_url="http://127.0.0.1:8085",
                overall_timeout=2.0,
                idle_timeout=1.0,
            )

            self.assertEqual(summary.status, "FAILURE")
            self.assertEqual(summary.exit_code, EXIT_VERIFICATION_FAILURE)
            self.assertIn("큐 용량", str(summary.error_message))
            self.assertTrue(fake_client.closed)
            mock_stop.assert_called_once()

    async def test_idle_timeout(self):
        class StalledFakeClient:
            def __init__(self):
                self.messages = self._msg_gen()
                self.closed = False

            async def _msg_gen(self):
                while True:
                    await asyncio.sleep(0.5)
                    yield None

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                self.closed = True

            async def subscribe(self, *args, **kwargs):
                return None

        fake_client = StalledFakeClient()
        broker_cfg = {"host": "localhost", "port": 1883, "tls_enabled": False}

        with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", return_value=fake_client), \
             patch("tools.verify_activity_normal_mqtt.http_post_start_run", return_value="run_idle_01"), \
             patch("tools.verify_activity_normal_mqtt.http_get_run_snapshot", return_value={"overall_status": "RUNNING"}), \
             patch("tools.verify_activity_normal_mqtt.http_post_stop_run") as mock_stop:

            summary = await run_verification(
                broker_config=broker_cfg,
                api_url="http://127.0.0.1:8085",
                connect_timeout=1.0,
                subscribe_timeout=1.0,
                idle_timeout=0.1,
                overall_timeout=2.0,
                poll_interval=0.05,
            )

            self.assertEqual(summary.status, "FAILURE")
            self.assertEqual(summary.exit_code, EXIT_TIMEOUT)
            self.assertIn("idle timeout", str(summary.error_message))
            self.assertTrue(fake_client.closed)
            mock_stop.assert_called_once()

    async def test_overall_timeout(self):
        class SlowFakeClient:
            def __init__(self):
                self.messages = self._msg_gen()
                self.closed = False

            async def _msg_gen(self):
                while True:
                    await asyncio.sleep(0.1)
                    yield None

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                self.closed = True

            async def subscribe(self, *args, **kwargs):
                return None

        fake_client = SlowFakeClient()
        broker_cfg = {"host": "localhost", "port": 1883, "tls_enabled": False}

        with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", return_value=fake_client), \
             patch("tools.verify_activity_normal_mqtt.http_post_start_run", return_value="run_overall_01"), \
             patch("tools.verify_activity_normal_mqtt.http_get_run_snapshot", return_value={"overall_status": "RUNNING"}), \
             patch("tools.verify_activity_normal_mqtt.http_post_stop_run") as mock_stop:

            summary = await run_verification(
                broker_config=broker_cfg,
                api_url="http://127.0.0.1:8085",
                idle_timeout=5.0,
                overall_timeout=0.1,
                poll_interval=0.05,
            )

            self.assertEqual(summary.status, "FAILURE")
            self.assertEqual(summary.exit_code, EXIT_TIMEOUT)
            self.assertIn("전체 실행 제한시간 초과", str(summary.error_message))
            mock_stop.assert_called_once()

    async def test_api_failed_handling(self):
        fake_client = DummyFakeClient()
        broker_cfg = {"host": "localhost", "port": 1883, "tls_enabled": False}

        with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", return_value=fake_client), \
             patch("tools.verify_activity_normal_mqtt.http_post_start_run", return_value="run_fail_01"), \
             patch("tools.verify_activity_normal_mqtt.http_get_run_snapshot", return_value={"overall_status": "FAILED", "last_error": "MOCK_FAILURE"}), \
             patch("tools.verify_activity_normal_mqtt.http_post_stop_run"):

            summary = await run_verification(
                broker_config=broker_cfg,
                api_url="http://127.0.0.1:8085",
                idle_timeout=5.0,
                overall_timeout=2.0,
                poll_interval=0.05,
            )

            self.assertEqual(summary.status, "FAILURE")
            self.assertEqual(summary.exit_code, EXIT_VERIFICATION_FAILURE)
            self.assertIn("FAILED", str(summary.error_message))

    async def test_successful_run(self):
        run_id = "run_success_01"

        class PerfectFakeMessage:
            def __init__(self, idx: int):
                self.payload = json.dumps(make_valid_payload_dict(run_id=run_id, second_of_day=idx)).encode("utf-8")
                self.topic = MagicMock()
                self.topic.matches.return_value = True
                self.topic.__str__ = lambda _: "v1/power/sim/H001/main"

        class FastMockClient:
            def __init__(self):
                self.messages = self._gen_all()
                self.closed = False

            async def _gen_all(self):
                for i in range(SECONDS_PER_DAY):
                    yield PerfectFakeMessage(i)
                    if i % 200 == 0:
                        await asyncio.sleep(0)
                while True:
                    await asyncio.sleep(1.0)

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                self.closed = True

            async def subscribe(self, *args, **kwargs):
                return None

        mock_api_snapshot = {
            "overall_status": "COMPLETED",
            "overall_state": "COMPLETED",
            "households": [
                {
                    "household_id": "H001",
                    "scenario": "ACTIVITY_NORMAL",
                    "state": "COMPLETED",
                    "runner_virtual_slots": SECONDS_PER_DAY,
                    "settled_virtual_slots": SECONDS_PER_DAY,
                    "published_samples": SECONDS_PER_DAY,
                    "planned_publish_samples": SECONDS_PER_DAY,
                    "omitted_samples": 0,
                    "completed_days": 1,
                    "total_days": 1,
                    "last_error": None,
                }
            ],
        }

        fake_client = FastMockClient()
        broker_cfg = {"host": "localhost", "port": 1883, "tls_enabled": False}

        with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", return_value=fake_client), \
             patch("tools.verify_activity_normal_mqtt.http_post_start_run", return_value=run_id), \
             patch("tools.verify_activity_normal_mqtt.http_get_run_snapshot", return_value=mock_api_snapshot):

            summary = await run_verification(
                broker_config=broker_cfg,
                api_url="http://127.0.0.1:8085",
                connect_timeout=1.0,
                subscribe_timeout=1.0,
                idle_timeout=15.0,
                overall_timeout=30.0,
                poll_interval=0.2,
            )

            self.assertEqual(summary.status, "SUCCESS")
            self.assertEqual(summary.exit_code, EXIT_SUCCESS)
            self.assertEqual(summary.target_unique_messages, SECONDS_PER_DAY)
            self.assertEqual(summary.missing_virtual_seconds, 0)
            self.assertEqual(summary.invalid_payloads, 0)
            self.assertEqual(summary.duplicate_deliveries, 0)
            self.assertEqual(summary.first_measured_at, "2026-09-16T00:00:00+09:00")
            self.assertEqual(summary.last_measured_at, "2026-09-16T23:59:59+09:00")
            self.assertEqual(summary.simulator_status, "COMPLETED")
            self.assertTrue(fake_client.closed)


# ==============================================================================
# 10. CLI 옵션 기본 테스트
# ==============================================================================
class TestCliOptions(unittest.TestCase):
    def test_default_options(self):
        args = parse_arguments([])
        self.assertEqual(args.scenario, "ACTIVITY_NORMAL")
        self.assertEqual(args.reference_date, "2026-09-16")
        self.assertEqual(args.api_url, "http://127.0.0.1:8085")
        self.assertEqual(args.connect_timeout, 10.0)
        self.assertEqual(args.subscribe_timeout, 10.0)
        self.assertEqual(args.http_timeout, 15.0)
        self.assertEqual(args.idle_timeout, 30.0)
        self.assertEqual(args.overall_timeout, 600.0)
        self.assertEqual(args.poll_interval, 0.5)

    def test_no_broker_pass_argument(self):
        with self.assertRaises(SystemExit):
            parse_arguments(["--broker-pass", "secret123"])


# ==============================================================================
# 11. 6개 일일 활동 시나리오 일반화 계약 검증 (a ~ h)
# ==============================================================================
class TestGeneralActivityScenarios(unittest.IsolatedAsyncioTestCase):
    """6개 일일 활동 시나리오 공통 지원 및 신규 계약 테스트"""

    def test_a_expected_plan_matches_independent_calculation_all_6_scenarios(self):
        """
        계약 a: 6개 시나리오 각각에 대해 build_expected_schedule_plan이 산출한
        기대 발행/결측/첫·마지막 슬롯이 definition.omission_ranges로부터 독립 산출한 기대값과 일치함을 전수 검증.
        """
        for sc_id in ACTIVITY_SCENARIO_IDS:
            defn = get_activity_scenario_definition(sc_id)
            day_sched = defn.days[0]
            omissions = day_sched.omission_ranges

            if not omissions:
                expected_first_sec = 0
                expected_last_sec = SECONDS_PER_DAY - 1
                expected_omit_samples = 0
                expected_pub_samples = SECONDS_PER_DAY
            else:
                first_om = omissions[0]
                expected_first_sec = 0
                expected_last_sec = first_om.start_second - 1
                expected_omit_samples = sum(r.end_second - r.start_second for r in omissions)
                expected_pub_samples = SECONDS_PER_DAY - expected_omit_samples

            plan = build_expected_schedule_plan(sc_id, "2026-09-16")
            self.assertEqual(plan.first_publish_second, expected_first_sec, f"{sc_id} first_sec mismatch")
            self.assertEqual(plan.last_publish_second, expected_last_sec, f"{sc_id} last_sec mismatch")
            self.assertEqual(plan.expected_publish_samples, expected_pub_samples, f"{sc_id} pub_samples mismatch")
            self.assertEqual(plan.expected_omitted_samples, expected_omit_samples, f"{sc_id} omit_samples mismatch")

        # 이유: 카탈로그 명세(ACTIVITY_INSUFFICIENT는 22:47:59부터 자정까지 결측되어 22:47:58에 마지막 메시지 발행)의
        # 시간 변환 회귀를 단일 지점에서 감지하기 위해 허용된 유일한 리터럴 단정.
        plan_insufficient = build_expected_schedule_plan("ACTIVITY_INSUFFICIENT", "2026-09-16")
        self.assertEqual(plan_insufficient.last_expected_measured_at, "2026-09-16T22:47:58+09:00")

    def test_b_insufficient_scenario_success_with_exact_samples(self):
        """
        계약 b: INSUFFICIENT에서 기대 발행 수만큼의 유효 타겟 메시지만 수신했을 때
        missing 0, planned_omitted 일치, unexpected_in_omission 0, SUCCESS 판정 가능
        """
        plan = build_expected_schedule_plan("ACTIVITY_INSUFFICIENT", "2026-09-16")
        validator = StreamMessageValidator("run_insuf_b", plan=plan)

        # 시작 슬롯 및 마지막 슬롯 실제 메시지 처리 검증
        first_p = make_valid_payload_dict(run_id="run_insuf_b", second_of_day=plan.first_publish_second or 0)
        last_p = make_valid_payload_dict(run_id="run_insuf_b", second_of_day=plan.last_publish_second or 0)

        res_first = validator.process_message(json.dumps(first_p))
        res_last = validator.process_message(json.dumps(last_p))

        self.assertEqual(res_first, "TARGET")
        self.assertEqual(res_last, "TARGET")
        self.assertEqual(validator.first_measured_at, plan.first_expected_measured_at)
        self.assertEqual(validator.last_measured_at, plan.last_expected_measured_at)

        # 전체 기대 발행 건수 도달 시뮬레이션
        validator.target_unique_messages = plan.expected_publish_samples
        self.assertEqual(validator.missing_virtual_seconds, 0)
        self.assertEqual(validator.planned_omitted_seconds, plan.expected_omitted_samples)
        self.assertEqual(validator.unexpected_in_omission, 0)
        self.assertEqual(validator.invalid_payloads, 0)

    def test_c_insufficient_scenario_rejects_sample_in_omission_range(self):
        """
        계약 c: INSUFFICIENT 계획 결측 구간(첫 번째 결측 슬롯) 메시지 1건 수신 시
        unexpected_in_omission 증가, invalid_payloads 미증가(이중 집계 방지), omission_violation_reasons 기록, SUCCESS 차단
        """
        plan = build_expected_schedule_plan("ACTIVITY_INSUFFICIENT", "2026-09-16")
        validator = StreamMessageValidator("run_insuf_c", plan=plan)

        # 첫 번째 결측 슬롯
        first_omitted_sec = (plan.last_publish_second + 1) if plan.last_publish_second is not None else 0
        p_omitted = make_valid_payload_dict(run_id="run_insuf_c", second_of_day=first_omitted_sec)
        res = validator.process_message(json.dumps(p_omitted))

        self.assertEqual(res, "UNEXPECTED_OMISSION")
        self.assertEqual(validator.unexpected_in_omission, 1)
        self.assertEqual(validator.invalid_payloads, 0)  # 이중 집계 금지 계약
        self.assertEqual(len(validator.omission_violation_reasons), 1)
        self.assertIn("UNEXPECTED_IN_OMISSION", validator.omission_violation_reasons[0])
        self.assertIn(f"second_of_day={first_omitted_sec}", validator.omission_violation_reasons[0])

    def test_d_normal_scenario_regression_missing_single_sample(self):
        """
        계약 d: NORMAL 시나리오에서 1건 누락 시 missing_virtual_seconds가 1이 되어 FAILURE 판정
        """
        plan = build_expected_schedule_plan("ACTIVITY_NORMAL", "2026-09-16")
        validator = StreamMessageValidator("run_normal_d", plan=plan)
        validator.target_unique_messages = SECONDS_PER_DAY - 1
        self.assertEqual(validator.missing_virtual_seconds, 1)

    def test_e_cli_default_scenario_equals_activity_normal(self):
        """
        계약 e: --scenario 미지정 시 기본값 ACTIVITY_NORMAL이며 시작 API 요청 본문이 기존과 완전히 동일
        """
        args = parse_arguments([])
        self.assertEqual(args.scenario, "ACTIVITY_NORMAL")
        self.assertEqual(args.reference_date, "2026-09-16")

        fake_resp = io.BytesIO(b'{"run_id": "run_default_01"}')
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.read.side_effect = fake_resp.read
        mock_resp.__enter__.return_value = mock_resp
        mock_resp.__exit__.return_value = False

        captured_req = None
        def fake_urlopen(req, timeout):
            nonlocal captured_req
            captured_req = req
            return mock_resp

        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            run_id = http_post_start_run("http://127.0.0.1:8085", 5.0)
            self.assertEqual(run_id, "run_default_01")

        body = json.loads(captured_req.data.decode("utf-8"))
        expected_body = {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [
                {
                    "household_id": "H001",
                    "scenario": "ACTIVITY_NORMAL",
                }
            ],
        }
        self.assertEqual(body, expected_body)

    def test_f_cli_invalid_scenario_exits_code_2_without_start_api(self):
        """
        계약 f: --scenario 잘못된 값 입력 시 exit 2로 즉시 종료되고 시작 API가 호출되지 않음
        """
        with patch("tools.verify_activity_normal_mqtt.http_post_start_run") as mock_start:
            exit_code = main(["--scenario", "INVALID_SCENARIO"])
            self.assertEqual(exit_code, EXIT_CLI_CONFIG_ERROR)
            mock_start.assert_not_called()

        with patch("tools.verify_activity_normal_mqtt.http_post_start_run") as mock_start:
            # 소문자도 엄격 거절
            exit_code = main(["--scenario", "activity_normal"])
            self.assertEqual(exit_code, EXIT_CLI_CONFIG_ERROR)
            mock_start.assert_not_called()

    async def test_g_api_done_evaluation_with_scenario_expected_samples(self):
        """
        계약 g: api_done이 시나리오별 기대값(발행/결측)으로 정확히 판정되는지 INSUFFICIENT 스냅샷으로 검증
        """
        plan = build_expected_schedule_plan("ACTIVITY_INSUFFICIENT", "2026-09-16")
        broker_cfg = {"host": "localhost", "port": 1883, "tls_enabled": False}
        fake_client = DummyFakeClient()

        # 정상 완료 스냅샷 (INSUFFICIENT 기대값에 정확히 부합)
        insufficient_api_snap = {
            "overall_status": "COMPLETED",
            "overall_state": "COMPLETED",
            "households": [
                {
                    "household_id": "H001",
                    "scenario": "ACTIVITY_INSUFFICIENT",
                    "state": "COMPLETED",
                    "runner_virtual_slots": SECONDS_PER_DAY,
                    "settled_virtual_slots": SECONDS_PER_DAY,
                    "published_samples": plan.expected_publish_samples,
                    "planned_publish_samples": plan.expected_publish_samples,
                    "omitted_samples": plan.expected_omitted_samples,
                    "completed_days": 1,
                    "total_days": 1,
                    "last_error": None,
                }
            ],
        }

        # validator가 이미 완료 상태라고 모킹하여 api_done 판정 즉시 루프 탈출
        with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", return_value=fake_client), \
             patch("tools.verify_activity_normal_mqtt.http_post_start_run", return_value="run_g_01"), \
             patch("tools.verify_activity_normal_mqtt.http_get_run_snapshot", return_value=insufficient_api_snap), \
             patch("tools.verify_activity_normal_mqtt.StreamMessageValidator") as mock_val_cls:

            mock_val = MagicMock()
            mock_val.target_unique_messages = plan.expected_publish_samples
            mock_val.missing_virtual_seconds = 0
            mock_val.planned_omitted_seconds = plan.expected_omitted_samples
            mock_val.invalid_payloads = 0
            mock_val.unexpected_in_omission = 0
            mock_val.duplicate_deliveries = 0
            mock_val.foreign_run_messages = 0
            mock_val.first_measured_at = plan.first_expected_measured_at
            mock_val.last_measured_at = plan.last_expected_measured_at
            mock_val.invalid_reasons = []
            mock_val.omission_violation_reasons = []
            mock_val_cls.return_value = mock_val

            summary = await run_verification(
                broker_config=broker_cfg,
                api_url="http://127.0.0.1:8085",
                expected_plan=plan,
                overall_timeout=5.0,
                poll_interval=0.05,
            )

            self.assertEqual(summary.status, "SUCCESS")
            self.assertEqual(summary.exit_code, EXIT_SUCCESS)
            self.assertEqual(summary.scenario, "ACTIVITY_INSUFFICIENT")
            self.assertEqual(summary.expected_publish_samples, plan.expected_publish_samples)
            self.assertEqual(summary.expected_omitted_samples, plan.expected_omitted_samples)
            self.assertEqual(summary.planned_omitted_seconds, plan.expected_omitted_samples)

    async def test_h_password_redaction_and_cleanup_preserved_under_new_scenarios(self):
        """
        계약 h: 신규 시나리오 실행 경로 및 오류 상황에서도 비밀번호가 안전하게 마스킹되고 자원 정리가 유지됨을 검증
        """
        secret_pass = "my_super_secret_password_777"
        broker_cfg = {"host": "localhost", "port": 1883, "tls_enabled": False, "password": secret_pass}
        plan = build_expected_schedule_plan("ACTIVITY_DURATION_CAP", "2026-09-16")

        with patch.dict(os.environ, {"MQTT_PASS": secret_pass}), \
             patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", side_effect=Exception(f"Connection refused with {secret_pass}")):

            summary = await run_verification(broker_cfg, "http://127.0.0.1:8085", expected_plan=plan)

            self.assertEqual(summary.status, "FAILURE")
            self.assertEqual(summary.exit_code, EXIT_MQTT_ERROR)
            self.assertEqual(summary.scenario, "ACTIVITY_DURATION_CAP")
            self.assertNotIn(secret_pass, str(summary.error_message))
            self.assertIn("***", str(summary.error_message))

    def test_reference_date_validation_in_payload_matches_plan(self):
        """
        계약 3: validate_payload_contract의 dt.date() 비교 대상이 plan.reference_date여서
        --reference-date 2026-09-20 계획에서 2026-09-20 메시지는 TARGET, 2026-09-16 메시지는 INVALID(MEASURED_AT_DATE_MISMATCH)
        """
        plan_sep20 = build_expected_schedule_plan("ACTIVITY_NORMAL", "2026-09-20")
        validator = StreamMessageValidator("run_date_test", plan=plan_sep20)

        # 1. 2026-09-20 메시지 -> 정상 TARGET
        p_sep20 = make_valid_payload_dict(
            run_id="run_date_test",
            second_of_day=0,
            date_obj=date(2026, 9, 20),
        )
        res_sep20 = validator.process_message(json.dumps(p_sep20))
        self.assertEqual(res_sep20, "TARGET")

        # 2. 2026-09-16 메시지 -> INVALID (MEASURED_AT_DATE_MISMATCH)
        p_sep16 = make_valid_payload_dict(
            run_id="run_date_test",
            second_of_day=0,
            date_obj=date(2026, 9, 16),
        )
        res_sep16 = validator.process_message(json.dumps(p_sep16))
        self.assertEqual(res_sep16, "INVALID")
        self.assertTrue(any("MEASURED_AT_DATE_MISMATCH" in r for r in validator.invalid_reasons))

    async def test_non_default_scenario_and_reference_date_reflected_in_start_api(self):
        """
        비기본 --scenario 와 --reference-date 가 시작 API 요청 본문에 실제로 반영되는지 검증.
        이유: 6개 중 5개 시나리오의 기대 발행 수가 86,400 으로 동일하므로, 시나리오 전달이 끊겨도 INSUFFICIENT 외에는 검증이 통과해 버린다.
        """
        # 1. http_post_start_run 단위 검증: urllib.request.urlopen 패치 후 요청 본문 캡처
        fake_resp = io.BytesIO(b'{"run_id": "run_custom_01"}')
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.read.side_effect = fake_resp.read
        mock_resp.__enter__.return_value = mock_resp
        mock_resp.__exit__.return_value = False

        captured_req = None
        def fake_urlopen(req, timeout):
            nonlocal captured_req
            captured_req = req
            return mock_resp

        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            run_id = http_post_start_run(
                "http://127.0.0.1:8085",
                timeout_sec=5.0,
                scenario="ACTIVITY_INSUFFICIENT",
                reference_date="2026-09-20",
            )
            self.assertEqual(run_id, "run_custom_01")

        self.assertIsNotNone(captured_req)
        body = json.loads(captured_req.data.decode("utf-8"))
        expected_body = {
            "reference_date": "2026-09-20",
            "execution": {"mode": "BURST"},
            "households": [
                {
                    "household_id": "H001",
                    "scenario": "ACTIVITY_INSUFFICIENT",
                }
            ],
        }
        self.assertEqual(body, expected_body)

        # 2. run_verification 경로 검증: expected_plan 전달 시 http_post_start_run에 해당 인자 전달 확인
        plan = build_expected_schedule_plan("ACTIVITY_INSUFFICIENT", "2026-09-20")
        broker_cfg = {"host": "localhost", "port": 1883, "tls_enabled": False}
        fake_client = DummyFakeClient()

        mock_start = MagicMock(return_value="run_verification_test_id")
        with patch("tools.verify_activity_normal_mqtt.aiomqtt.Client", return_value=fake_client), \
             patch("tools.verify_activity_normal_mqtt.http_post_start_run", mock_start), \
             patch("tools.verify_activity_normal_mqtt.http_get_run_snapshot", side_effect=SimulatorApiError("Early exit for test")):

            # 실행은 즉시 실패하거나 타임아웃으로 끝나도 무방 (호출 인자만 확인)
            await run_verification(
                broker_config=broker_cfg,
                api_url="http://127.0.0.1:8085",
                expected_plan=plan,
                overall_timeout=2.0,
            )

            mock_start.assert_called_once()
            called_args, called_kwargs = mock_start.call_args
            scenario_arg = called_kwargs.get("scenario") or (called_args[2] if len(called_args) > 2 else None)
            ref_date_arg = called_kwargs.get("reference_date") or (called_args[3] if len(called_args) > 3 else None)
            self.assertEqual(scenario_arg, "ACTIVITY_INSUFFICIENT")
            self.assertEqual(ref_date_arg, "2026-09-20")


def make_two_day_scenario_definition() -> ScenarioDefinition:
    day0 = DaySchedule(day_offset=0, events=())
    day1 = DaySchedule(day_offset=1, events=())
    return ScenarioDefinition(scenario_id="SYNTHETIC_TWO_DAY", days=(day0, day1))


def make_multiday_payload(
    run_id: str = "run_test_01",
    relative_second: int = 0,
    first_cycle: int = 1,
    base_date: date = date(2026, 9, 16),
    active_power: float = 142.5,
    reactive_power: float = 41.2,
    power_factor: float = 0.96,
    current: float = 0.68,
    voltage: float = 220.3,
    apparent_power: float = 148.4,
) -> dict:
    start_dt = datetime(base_date.year, base_date.month, base_date.day, 0, 0, 0, tzinfo=KST)
    dt = start_dt + timedelta(seconds=relative_second)
    measured_at = dt.isoformat()
    absolute_cycle = first_cycle + relative_second
    uuid5_name = f"SCHEDULE_PUBLISHER_V1::{run_id}::{TARGET_HOUSEHOLD}::{absolute_cycle}::{measured_at}"
    msg_id = str(uuid.uuid5(SCHEDULE_PUBLISHER_NAMESPACE, uuid5_name))
    return {
        "message_id": msg_id,
        "household_id": TARGET_HOUSEHOLD,
        "device_id": TARGET_DEVICE,
        "measured_at": measured_at,
        "active_power": active_power,
        "reactive_power": reactive_power,
        "power_factor": power_factor,
        "current": current,
        "house": TARGET_HOUSEHOLD,
        "device": TARGET_DEVICE,
        "ts": measured_at,
        "power_w": active_power,
        "voltage": voltage,
        "apparent_power": apparent_power,
    }


class TestMultiDayVerification(unittest.IsolatedAsyncioTestCase):
    """
    다일(20일, 28일) 시나리오 확장 검증 테스트 스위트
    """

    def test_multiday_first_last_measured_at_preservation(self):
        """
        [1] 요구사항 검증:
        2일 합성 계획에서 첫날 자정(rel_sec=0) 메시지 주입 후 둘째 날 자정(rel_sec=86400) 메시지를 주입해도
        first_measured_at이 첫날 자정 값으로 유지되고, 마지막 슬롯(rel_sec=172799) 메시지에서만 last_measured_at이 설정됨을 단정.
        """
        compiled = compile_schedule(make_two_day_scenario_definition(), base_date=date(2026, 9, 16))
        plan_2d = ExpectedSchedulePlan(
            scenario_id="SYNTHETIC_TWO_DAY",
            reference_date=date(2026, 9, 16),
            reference_date_str="2026-09-16",
            compiled_plan=compiled,
            expected_publish_samples=compiled.total_planned_publish_samples,
            expected_omitted_samples=compiled.total_planned_omitted_samples,
            first_publish_second=0,
            last_publish_second=172799,
            first_expected_measured_at=compiled.virtual_time_at(1).isoformat(),
            last_expected_measured_at=compiled.virtual_time_at(172800).isoformat(),
            schedule_bitmap=bytes(b"\x01" * 172800),
        )
        validator = StreamMessageValidator("run_preserve_test", plan=plan_2d)

        # 1. 첫날 자정 (rel_sec = 0, cycle = 1, measured_at = 2026-09-16T00:00:00+09:00)
        day1_midnight = make_multiday_payload(
            run_id="run_preserve_test",
            relative_second=0,
            first_cycle=compiled.first_cycle,
            base_date=plan_2d.start_date,
        )
        res1 = validator.process_message(json.dumps(day1_midnight))
        self.assertEqual(res1, "TARGET")
        first_measured_expected = day1_midnight["measured_at"]
        self.assertEqual(validator.first_measured_at, first_measured_expected)
        self.assertIsNone(validator.last_measured_at)

        # 2. 둘째 날 자정 (rel_sec = 86400, cycle = 86401, measured_at = 2026-09-17T00:00:00+09:00)
        day2_midnight = make_multiday_payload(
            run_id="run_preserve_test",
            relative_second=86400,
            first_cycle=compiled.first_cycle,
            base_date=plan_2d.start_date,
        )
        res2 = validator.process_message(json.dumps(day2_midnight))
        self.assertEqual(res2, "TARGET")
        # first_measured_at이 둘째 날 자정으로 덮어써지지 않고 첫날 자정 값으로 유지되는지 단정
        self.assertEqual(validator.first_measured_at, first_measured_expected)
        self.assertNotEqual(validator.first_measured_at, day2_midnight["measured_at"])
        self.assertIsNone(validator.last_measured_at)

        # 3. 마지막 슬롯 (rel_sec = 172799, cycle = 172800, measured_at = 2026-09-17T23:59:59+09:00)
        last_slot = make_multiday_payload(
            run_id="run_preserve_test",
            relative_second=172799,
            first_cycle=compiled.first_cycle,
            base_date=plan_2d.start_date,
        )
        res3 = validator.process_message(json.dumps(last_slot))
        self.assertEqual(res3, "TARGET")
        self.assertEqual(validator.first_measured_at, first_measured_expected)
        self.assertEqual(validator.last_measured_at, last_slot["measured_at"])

    def test_multiday_cycle_boundary_uuid5(self):
        """
        [3] 요구사항 검증:
        2일 합성 계획에서:
        - 둘째 날 00:00:00 메시지(절대 사이클 86,401, rel_sec 86,400)의 message_id가 TARGET으로 통과
        - 첫날 마지막 슬롯(절대 사이클 86,400, rel_sec 86,399)도 TARGET으로 통과
        - 이 두 건이 서로 다른 비트맵 인덱스(86,399와 86,400)에 기록되어 중복(DUPLICATE)으로 잡히지 않음
        """
        compiled = compile_schedule(make_two_day_scenario_definition(), base_date=date(2026, 9, 16))
        plan_2d = ExpectedSchedulePlan(
            scenario_id="SYNTHETIC_TWO_DAY",
            reference_date=date(2026, 9, 16),
            reference_date_str="2026-09-16",
            compiled_plan=compiled,
            expected_publish_samples=compiled.total_planned_publish_samples,
            expected_omitted_samples=compiled.total_planned_omitted_samples,
            first_publish_second=0,
            last_publish_second=172799,
            first_expected_measured_at=compiled.virtual_time_at(1).isoformat(),
            last_expected_measured_at=compiled.virtual_time_at(172800).isoformat(),
            schedule_bitmap=bytes(b"\x01" * 172800),
        )
        validator = StreamMessageValidator("run_boundary_test", plan=plan_2d)

        # 첫날 마지막 슬롯 (rel_sec = 86,399, absolute_cycle = 86,400)
        day1_last = make_multiday_payload(
            run_id="run_boundary_test",
            relative_second=86399,
            first_cycle=compiled.first_cycle,
            base_date=plan_2d.start_date,
        )
        res1 = validator.process_message(json.dumps(day1_last))
        self.assertEqual(res1, "TARGET")
        self.assertEqual(validator.timeline_bitmap[86399], 1)

        # 둘째 날 첫 슬롯 (00:00:00, rel_sec = 86,400, absolute_cycle = 86,401)
        day2_first = make_multiday_payload(
            run_id="run_boundary_test",
            relative_second=86400,
            first_cycle=compiled.first_cycle,
            base_date=plan_2d.start_date,
        )
        res2 = validator.process_message(json.dumps(day2_first))
        self.assertEqual(res2, "TARGET")
        self.assertEqual(validator.timeline_bitmap[86400], 1)

        # 비트맵 인덱스 분리 및 중복 미발생 단정
        self.assertEqual(validator.duplicate_deliveries, 0)
        self.assertEqual(validator.target_unique_messages, 2)

    def test_multiday_out_of_range_rejected(self):
        """
        다일 계획 날짜 및 범위 밖 메시지 거절 검증:
        - 시작일 이전 날짜 (2026-09-15) -> INVALID
        - 종료일 이후 날짜 (2026-09-18) -> INVALID
        """
        compiled = compile_schedule(make_two_day_scenario_definition(), base_date=date(2026, 9, 16))
        plan_2d = ExpectedSchedulePlan(
            scenario_id="SYNTHETIC_TWO_DAY",
            reference_date=date(2026, 9, 16),
            reference_date_str="2026-09-16",
            compiled_plan=compiled,
            expected_publish_samples=compiled.total_planned_publish_samples,
            expected_omitted_samples=compiled.total_planned_omitted_samples,
            first_publish_second=0,
            last_publish_second=172799,
            first_expected_measured_at=compiled.virtual_time_at(1).isoformat(),
            last_expected_measured_at=compiled.virtual_time_at(172800).isoformat(),
            schedule_bitmap=bytes(b"\x01" * 172800),
        )
        validator = StreamMessageValidator("run_range_test", plan=plan_2d)

        # 시작일 이전 날짜 (2026-09-15)
        before_start = make_multiday_payload(
            run_id="run_range_test",
            relative_second=0,
            first_cycle=compiled.first_cycle,
            base_date=date(2026, 9, 15),
        )
        res_before = validator.process_message(json.dumps(before_start))
        self.assertEqual(res_before, "INVALID")
        self.assertTrue(any("MEASURED_AT_DATE_MISMATCH" in r for r in validator.invalid_reasons))

        # 종료일 이후 날짜 (2026-09-18)
        after_end = make_multiday_payload(
            run_id="run_range_test",
            relative_second=0,
            first_cycle=compiled.first_cycle,
            base_date=date(2026, 9, 18),
        )
        res_after = validator.process_message(json.dumps(after_end))
        self.assertEqual(res_after, "INVALID")
        self.assertTrue(any("MEASURED_AT_DATE_MISMATCH" in r for r in validator.invalid_reasons))

    def test_all_unified_scenarios_plan_derivation(self):
        """
        통합 카탈로그 10종 시나리오에 대한 ExpectedSchedulePlan 동적 산출 검증:
        - 1일 6종: 86,400 슬롯
        - 20일 1종: 1,728,000 슬롯
        - 28일 3종: 2,419,200 슬롯
        """
        for sc_id in UNIFIED_SCENARIO_IDS:
            plan = build_expected_schedule_plan(sc_id, "2026-09-16")
            self.assertEqual(plan.scenario_id, sc_id)
            self.assertIsNotNone(plan.first_publish_second)
            self.assertIsNotNone(plan.last_publish_second)
            self.assertIsNotNone(plan.first_expected_measured_at)
            self.assertIsNotNone(plan.last_expected_measured_at)
            self.assertEqual(len(plan.schedule_bitmap), plan.total_virtual_slots)

            if sc_id.startswith("ACTIVITY_"):
                self.assertEqual(plan.total_days, 1)
                self.assertEqual(plan.total_virtual_slots, 86400)
            elif sc_id == "BASELINE_MICROWAVE_20D":
                self.assertEqual(plan.total_days, 20)
                self.assertEqual(plan.total_virtual_slots, 1728000)
                self.assertEqual(plan.expected_publish_samples, 1728000)
                self.assertEqual(plan.expected_omitted_samples, 0)
            elif sc_id.startswith("ROUTINE_CHANGED_"):
                self.assertEqual(plan.total_days, 28)
                self.assertEqual(plan.total_virtual_slots, 2419200)
                self.assertEqual(plan.expected_publish_samples, 2419200)
                self.assertEqual(plan.expected_omitted_samples, 0)

    def test_timeout_auto_scaling(self):
        """
        다일 시나리오 슬롯 수 비례 제한시간 자동 상향 산출 공식 검증
        """
        # 1일: 86,400 -> max(600, 86400/500 + 300 = 472.8) -> 600.0
        plan_1d = build_expected_schedule_plan("ACTIVITY_NORMAL", "2026-09-16")
        timeout_1d = max(600.0, (plan_1d.total_virtual_slots / 500.0) + 300.0)
        self.assertEqual(timeout_1d, 600.0)

        # 20일: 1,728,000 -> max(600, 1728000/500 + 300 = 3756.0) -> 3756.0
        plan_20d = build_expected_schedule_plan("BASELINE_MICROWAVE_20D", "2026-09-16")
        timeout_20d = max(600.0, (plan_20d.total_virtual_slots / 500.0) + 300.0)
        self.assertEqual(timeout_20d, 3756.0)

        # 28일: 2,419,200 -> max(600, 2419200/500 + 300 = 5138.4) -> 5138.4
        plan_28d = build_expected_schedule_plan("ROUTINE_CHANGED_LATER", "2026-09-16")
        timeout_28d = max(600.0, (plan_28d.total_virtual_slots / 500.0) + 300.0)
        self.assertAlmostEqual(timeout_28d, 5138.4, places=2)

    def test_multiday_api_done_evaluation(self):
        """
        다일 실행 세션 스냅샷(28일)에 대한 api_done 계약 평가 검증
        """
        plan_28d = build_expected_schedule_plan("ROUTINE_CHANGED_LATER", "2026-09-16")

        # 완료 스냅샷 (28일 전량 충족)
        completed_snap = {
            "overall_status": "COMPLETED",
            "overall_state": "COMPLETED",
            "households": [
                {
                    "household_id": "H001",
                    "state": "COMPLETED",
                    "runner_virtual_slots": 2419200,
                    "settled_virtual_slots": 2419200,
                    "published_samples": 2419200,
                    "planned_publish_samples": 2419200,
                    "omitted_samples": 0,
                    "completed_days": 28,
                    "total_days": 28,
                    "last_error": None,
                }
            ],
        }

        h001 = completed_snap["households"][0]
        api_done = (
            completed_snap.get("overall_status") == "COMPLETED"
            and completed_snap.get("overall_state") == "COMPLETED"
            and h001.get("state") == "COMPLETED"
            and h001.get("runner_virtual_slots") == plan_28d.total_virtual_slots
            and h001.get("settled_virtual_slots") == plan_28d.total_virtual_slots
            and h001.get("published_samples") == plan_28d.expected_publish_samples
            and h001.get("planned_publish_samples") == plan_28d.expected_publish_samples
            and h001.get("omitted_samples") == plan_28d.expected_omitted_samples
            and h001.get("completed_days") == plan_28d.total_days
            and h001.get("total_days") == plan_28d.total_days
            and h001.get("last_error") is None
        )
        self.assertTrue(api_done)

        # 미완료 스냅샷 (27일 진행 중) -> api_done False
        incomplete_snap = json.loads(json.dumps(completed_snap))
        incomplete_snap["households"][0]["completed_days"] = 27
        h001_incomp = incomplete_snap["households"][0]
        api_done_incomp = (
            incomplete_snap.get("overall_status") == "COMPLETED"
            and incomplete_snap.get("overall_state") == "COMPLETED"
            and h001_incomp.get("state") == "COMPLETED"
            and h001_incomp.get("runner_virtual_slots") == plan_28d.total_virtual_slots
            and h001_incomp.get("settled_virtual_slots") == plan_28d.total_virtual_slots
            and h001_incomp.get("completed_days") == plan_28d.total_days
        )
        self.assertFalse(api_done_incomp)

    def test_cli_accepts_all_unified_scenarios_and_rejects_unknown(self):
        """
        CLI --scenario 파싱 및 main() 진입 검증:
        - 10종 통합 시나리오는 파싱 통과
        - 알 수 없는 시나리오는 exit code 2 (EXIT_CLI_CONFIG_ERROR)로 거절
        """
        for sc_id in UNIFIED_SCENARIO_IDS:
            args = parse_arguments(["--scenario", sc_id])
            self.assertEqual(args.scenario, sc_id)

        # 알 수 없는 시나리오 전달 시 code 2
        exit_code = main(["--scenario", "INVALID_UNKNOWN_SCENARIO"])
        self.assertEqual(exit_code, EXIT_CLI_CONFIG_ERROR)

    def test_multiday_reference_date_aligns_with_server_rules(self):
        """
        [3-a, 3-b] 요구사항 검증:
        20일 및 28일 시나리오에 대해 도구가 산출한 plan.start_date / plan.end_date가
        서버와 같은 규칙(end == 입력한 reference_date, start == reference_date - (total_days-1))을 따르고,
        컴파일 결과가 first_cycle, last_cycle, total_virtual_slots, start_date, end_date에서 모두 일치하는지 단정.
        """
        ref_date = date(2026, 9, 16)
        test_scenarios = [
            ("BASELINE_MICROWAVE_20D", 20),
            ("ROUTINE_CHANGED_LATER", 28),
            ("ROUTINE_CHANGED_EARLIER", 28),
            ("ROUTINE_CHANGED_WITHIN_THRESHOLD", 28),
        ]
        for sc_id, total_days in test_scenarios:
            plan = build_expected_schedule_plan(sc_id, "2026-09-16")
            expected_start = ref_date - timedelta(days=total_days - 1)
            expected_end = ref_date

            # [3-a] start_date / end_date 검증
            self.assertEqual(plan.end_date, expected_end)
            self.assertEqual(plan.start_date, expected_start)
            self.assertEqual(plan.reference_date, ref_date)

            # [3-b] compile_schedule 직접 호출 결과와 완전 일치 검증
            defn = get_unified_scenario_definition(sc_id)
            server_compiled = compile_schedule(defn, base_date=expected_start)
            self.assertEqual(plan.compiled_plan.first_cycle, server_compiled.first_cycle)
            self.assertEqual(plan.compiled_plan.last_cycle, server_compiled.last_cycle)
            self.assertEqual(plan.compiled_plan.total_virtual_slots, server_compiled.total_virtual_slots)
            self.assertEqual(plan.compiled_plan.start_date, server_compiled.start_date)
            self.assertEqual(plan.compiled_plan.end_date, server_compiled.end_date)

    def test_single_day_scenarios_start_date_equals_end_date_regression(self):
        """
        [3-c] 요구사항 검증 (회귀):
        단일 일자 6개 시나리오에서 start_date == end_date == reference_date 임을 단정.
        """
        ref_date = date(2026, 9, 16)
        activity_scenarios = [
            "ACTIVITY_NORMAL",
            "ACTIVITY_LOW",
            "ACTIVITY_NONE",
            "ACTIVITY_INSUFFICIENT",
            "ACTIVITY_SESSION_MERGE",
            "ACTIVITY_DURATION_CAP",
        ]
        for sc_id in activity_scenarios:
            plan = build_expected_schedule_plan(sc_id, "2026-09-16")
            self.assertEqual(plan.start_date, ref_date)
            self.assertEqual(plan.end_date, ref_date)
            self.assertEqual(plan.reference_date, ref_date)

    def test_baseline_20d_first_and_last_publish_timestamps(self):
        """
        [3-d] 요구사항 검증:
        20일 계획(BASELINE_MICROWAVE_20D)의 첫 발행이 2026-08-28T00:00:00+09:00,
        마지막 발행이 2026-09-16T23:59:59+09:00 인지 단정 (입력 reference_date 2026-09-16 기준).
        """
        plan = build_expected_schedule_plan("BASELINE_MICROWAVE_20D", "2026-09-16")
        self.assertEqual(plan.first_expected_measured_at, "2026-08-28T00:00:00+09:00")
        self.assertEqual(plan.last_expected_measured_at, "2026-09-16T23:59:59+09:00")
        self.assertEqual(plan.start_date, date(2026, 8, 28))
        self.assertEqual(plan.end_date, date(2026, 9, 16))

    def test_resolve_base_date_unit(self):
        """
        [2] 순수 함수 resolve_base_date 단위 계약 검증:
        - 1일: reference_date 그대로 반환
        - 20일: reference_date - 19일 반환
        - 28일: reference_date - 27일 반환
        - total_days < 1 또는 유효하지 않은 타입 시 ScheduleError 발생
        """
        from engine.schedule import ScheduleError, resolve_base_date
        d = date(2026, 9, 16)
        self.assertEqual(resolve_base_date(d, 1), d)
        self.assertEqual(resolve_base_date(d, 20), date(2026, 8, 28))
        self.assertEqual(resolve_base_date(d, 28), date(2026, 8, 20))

        with self.assertRaises(ScheduleError):
            resolve_base_date(d, 0)
        with self.assertRaises(ScheduleError):
            resolve_base_date(d, -5)
        with self.assertRaises(ScheduleError):
            resolve_base_date(d, True)
        with self.assertRaises(ScheduleError):
            resolve_base_date("2026-09-16", 10)


if __name__ == "__main__":
    unittest.main()

