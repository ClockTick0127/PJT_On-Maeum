"""
NILM 전력 시뮬레이터 MQTT TLS 암호화 연결 및 설정 단위 테스트 스위트

검증 항목:
1. MQTT_TLS_ENABLED 파서: 다양한 불리언 표현 지원 및 오타/알 수 없는 값에 대한 ValueError 방어
2. MQTT 포트 우선순위: CLI 명시 > MQTT_PORT 환경변수 > TLS(8883) > 평문(1883)
3. 설정 해석 시점 및 회귀 검증: 잘못된 MQTT_PORT 환경변수가 있어도 CLI --port 명시 시 정상 실행
4. TLS 비활성화 시 평문(None) 설정 유지
5. TLS 활성화 시 유효한 CA PEM fixture 기반 SSLContext 생성 및 CERT_REQUIRED, check_hostname 강제 검증
6. CA 파일 누락, 미존재, 디렉터리, 손상된 PEM에 대한 즉각적이고 명확한 예외 발생 검증
7. CLI simulator.parse_args(): 옵션 우선순위 및 환경변수 연동 검증
8. CLI run_simulator() 실행 시 aiomqtt.Client에 tls_context 전달 검증 (Mock)
9. 웹 시뮬레이터 SimulatorManager 실행 시 aiomqtt.Client에 tls_context 전달 검증 (Mock)
10. 웹 시뮬레이터 SimulatorManager.start()의 런타임 동기 검증 (잘못된 CA 설정 시 즉각 실패 및 워커 미가동)
11. 비밀번호 실제 미노출 검증: 환경변수 설정 상태에서 실제 --help 출력 캡처 및 평문 미노출 검증
12. 웹 API /api/start 호출 시 동기 오류(400 CONFIG_ERROR) 반환 검증
13. 웹 서버 바인딩 호스트: 기본값 127.0.0.1 및 --bind-host 옵션 검증
"""

import asyncio
import importlib
import io
import os
import ssl
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch, MagicMock, AsyncMock

# 상위 시뮬레이터 디렉터리 import 경로 등록
TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SIMULATOR_DIR = os.path.dirname(TESTS_DIR)
if SIMULATOR_DIR not in sys.path:
    sys.path.insert(0, SIMULATOR_DIR)

import simulator
import web_server
import engine.config
from engine.tls import (
    parse_tls_enabled,
    resolve_mqtt_port,
    resolve_mqtt_config,
    mask_password,
    create_mqtt_tls_context,
    get_mqtt_tls_context,
)
from server.manager import SimulatorManager

FIXTURE_CA_PATH = os.path.join(TESTS_DIR, "fixtures", "test_ca.crt")


class TestMqttTlsConfiguration(unittest.TestCase):
    def setUp(self):
        # 환경변수 격리를 위해 원본 환경변수 백업
        self._orig_env = os.environ.copy()

    def tearDown(self):
        # 환경변수 복원
        os.environ.clear()
        os.environ.update(self._orig_env)

    # -------------------------------------------------------------
    # 1. MQTT_TLS_ENABLED 파서 검증
    # -------------------------------------------------------------
    def test_01_parse_tls_enabled_valid_values(self):
        """다양한 유효 불리언 표현에 대해 올바른 True/False 파싱 검증"""
        # True 계열
        for val in ("true", "True", "TRUE", "1", 1, True, "yes", "YES", "y", "on", "ON"):
            with self.subTest(val=val):
                self.assertTrue(parse_tls_enabled(val))

        # False 계열
        for val in ("false", "False", "FALSE", "0", 0, False, "no", "NO", "n", "off", "OFF", None, ""):
            with self.subTest(val=val):
                self.assertFalse(parse_tls_enabled(val))

    def test_02_parse_tls_enabled_rejects_typos_and_unknown_values(self):
        """오타(ture, treu) 또는 알 수 없는 설정값 지정 시 평문으로 다운그레이드되지 않고 ValueError 발생 검증"""
        invalid_values = ("ture", "treu", "flase", "enable", "disable", "unknown", 2, -1)
        for invalid in invalid_values:
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError) as ctx:
                    parse_tls_enabled(invalid)
                self.assertIn("올바르지 않은 MQTT_TLS_ENABLED", str(ctx.exception))

    # -------------------------------------------------------------
    # 2. MQTT 포트 결정 우선순위 검증
    # -------------------------------------------------------------
    def test_03_resolve_mqtt_port_priority(self):
        """
        포트 우선순위 검증:
        CLI 명시 > MQTT_PORT 환경변수 > TLS 활성화 8883 > 평문 1883
        """
        # Case A: 명시적 포트 지정 (환경변수나 TLS 상태보다 최우선)
        os.environ["MQTT_PORT"] = "1883"
        self.assertEqual(resolve_mqtt_port(explicit_port=9999, tls_enabled=True), 9999)
        self.assertEqual(resolve_mqtt_port(explicit_port=8884, tls_enabled=False), 8884)

        # Case B: 명시적 포트 없음, MQTT_PORT 환경변수 있음
        os.environ["MQTT_PORT"] = "9883"
        self.assertEqual(resolve_mqtt_port(explicit_port=None, tls_enabled=True), 9883)
        self.assertEqual(resolve_mqtt_port(explicit_port=None, tls_enabled=False), 9883)

        # Case C: 환경변수 없음, TLS 활성화 -> 8883
        os.environ.pop("MQTT_PORT", None)
        self.assertEqual(resolve_mqtt_port(explicit_port=None, tls_enabled=True), 8883)

        # Case D: 환경변수 없음, 평문 -> 1883
        self.assertEqual(resolve_mqtt_port(explicit_port=None, tls_enabled=False), 1883)

        # Case E: MQTT_PORT가 숫자가 아닌 경우 ValueError
        os.environ["MQTT_PORT"] = "not_a_number"
        with self.assertRaises(ValueError):
            resolve_mqtt_port()

    def test_03b_cli_port_overrides_invalid_env_port(self):
        """
        회귀 테스트: 잘못된 MQTT_PORT 환경변수가 설정되어 있어도
        CLI --port가 명시되면 오류 없이 CLI 포트가 최우선 적용되어야 함.
        (반대로 CLI 포트가 없으면 환경변수 오류로 즉각 실패)
        """
        os.environ["MQTT_PORT"] = "corrupt_port_xyz"

        # 1. simulator CLI에서 --port 명시 시 정상 통과
        args = simulator.parse_args(["--port", "1883"])
        self.assertEqual(args.port, 1883)

        # 2. web_server 설정 해석에서 port 명시 시 정상 통과
        cfg = resolve_mqtt_config(port=8883)
        self.assertEqual(cfg["port"], 8883)

        # 3. CLI --port 미지정 시에는 환경변수 파싱 오류로 즉시 실패해야 함
        with self.assertRaises(ValueError) as ctx:
            simulator.parse_args([])
        self.assertIn("유효하지 않은 MQTT_PORT", str(ctx.exception))

    def test_03c_config_import_resilient_to_invalid_env_port(self):
        """
        회귀 테스트: config.py 모듈 import 시점에는 포트를 조기 해석하지 않으므로,
        잘못된 MQTT_PORT 환경변수가 있어도 import가 실패하지 않아야 함.
        """
        os.environ["MQTT_PORT"] = "invalid_garbage_port"
        # 모듈 reload 시 예외가 발생하지 않아야 함
        try:
            importlib.reload(engine.config)
        except Exception as err:
            self.fail(f"잘못된 MQTT_PORT 환경변수로 인해 engine.config import 실패: {err}")

        # 정적 기본값 유지 확인
        self.assertEqual(engine.config.DEFAULT_BROKER_PORT, 1883)

    # -------------------------------------------------------------
    # 3. TLS 비활성화 시 평문(None) 설정 유지 검증
    # -------------------------------------------------------------
    def test_04_plaintext_connection_returns_none_context(self):
        """TLS 비활성화 시 SSLContext는 None이어야 함 (로컬 개발 1883 평문 완벽 보존)"""
        ctx = get_mqtt_tls_context(tls_enabled=False, ca_file=None)
        self.assertIsNone(ctx)

        ctx_with_ca = get_mqtt_tls_context(tls_enabled=False, ca_file=FIXTURE_CA_PATH)
        self.assertIsNone(ctx_with_ca, "TLS가 비활성화된 경우 ca_file이 있어도 컨텍스트는 None이어야 합니다.")

    # -------------------------------------------------------------
    # 4. TLS 활성화 시 유효한 CA PEM fixture 기반 SSLContext 생성 검증
    # -------------------------------------------------------------
    def test_05_tls_creates_valid_ssl_context_with_strict_verification(self):
        """유효한 CA 파일로 SSLContext 생성 시 CERT_REQUIRED 및 check_hostname 강제 검증"""
        self.assertTrue(os.path.exists(FIXTURE_CA_PATH), f"테스트용 CA fixture 미존재: {FIXTURE_CA_PATH}")

        context = create_mqtt_tls_context(FIXTURE_CA_PATH)
        self.assertIsInstance(context, ssl.SSLContext)
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED, "서버 인증서 검증(CERT_REQUIRED)이 강제되어야 합니다.")
        self.assertTrue(context.check_hostname, "Hostname/SAN 검증(check_hostname=True)이 강제되어야 합니다.")

    # -------------------------------------------------------------
    # 5. CA 파일 누락, 미존재, 디렉터리, 손상 PEM에 대한 예외 처리 검증
    # -------------------------------------------------------------
    def test_06_missing_or_empty_ca_file_raises_value_error(self):
        """TLS 활성화 시 CA 파일이 누락되거나 빈 문자열이면 명확한 ValueError 발생"""
        for empty_ca in (None, "", "   "):
            with self.subTest(empty_ca=empty_ca):
                with self.assertRaises(ValueError) as ctx:
                    create_mqtt_tls_context(empty_ca)
                self.assertIn("CA 인증서 파일 경로", str(ctx.exception))

    def test_07_nonexistent_ca_file_raises_file_not_found(self):
        """존재하지 않는 CA 파일 경로 지정 시 명확한 FileNotFoundError 발생"""
        invalid_path = os.path.join(TESTS_DIR, "nonexistent_ca.crt")
        with self.assertRaises(FileNotFoundError) as ctx:
            create_mqtt_tls_context(invalid_path)
        self.assertIn("찾을 수 없습니다", str(ctx.exception))

    def test_08_directory_as_ca_file_raises_value_error(self):
        """디렉터리 경로를 CA 파일로 지정 시 ValueError 발생"""
        with self.assertRaises(ValueError) as ctx:
            create_mqtt_tls_context(TESTS_DIR)
        self.assertIn("일반 파일이 아닙니다", str(ctx.exception))

    def test_09_corrupted_pem_raises_ssl_error(self):
        """손상되거나 유효하지 않은 PEM 파일 로드 시 ssl.SSLError 발생"""
        with tempfile.NamedTemporaryFile("w", suffix=".crt", delete=False, encoding="utf-8") as tf:
            tf.write("-----BEGIN CERTIFICATE-----\nINVALID_BASE64_GARBAGE\n-----END CERTIFICATE-----\n")
            temp_path = tf.name

        try:
            with self.assertRaises(ssl.SSLError) as ctx:
                create_mqtt_tls_context(temp_path)
            self.assertIn("로드 실패", str(ctx.exception))
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    # -------------------------------------------------------------
    # 6. CLI simulator.parse_args() 인자 및 환경변수 연동 검증
    # -------------------------------------------------------------
    def test_10_simulator_cli_args_parsing(self):
        """CLI 인자 파싱 및 기본값/환경변수 오버라이드 검증"""
        # 기본 실행 (인자 없음): 평문 1883, TLS 비활성화
        args = simulator.parse_args([])
        self.assertFalse(args.tls)
        self.assertEqual(args.port, 1883)
        self.assertIsNone(args.ca_file)

        # --tls 지정 시: 포트 미지정이면 자동으로 8883 적용
        args_tls = simulator.parse_args(["--tls", "--ca-file", FIXTURE_CA_PATH])
        self.assertTrue(args_tls.tls)
        self.assertEqual(args_tls.port, 8883)
        self.assertEqual(args_tls.ca_file, FIXTURE_CA_PATH)

        # --tls와 명시적 --port 9883 지정 시: 9883 적용
        args_custom_port = simulator.parse_args(["--tls", "--ca-file", FIXTURE_CA_PATH, "-p", "9883"])
        self.assertEqual(args_custom_port.port, 9883)

        # 환경변수로 MQTT_TLS_ENABLED=true 설정 후 CLI에서 --no-tls로 오버라이드
        os.environ["MQTT_TLS_ENABLED"] = "true"
        args_override = simulator.parse_args(["--no-tls"])
        self.assertFalse(args_override.tls)
        self.assertEqual(args_override.port, 1883)

    # -------------------------------------------------------------
    # 7. CLI run_simulator()의 aiomqtt.Client tls_context 전달 검증 (Mock)
    # -------------------------------------------------------------
    def test_11_cli_run_simulator_passes_tls_context(self):
        """CLI run_simulator()가 aiomqtt.Client에 올바른 tls_context를 전달하는지 Mock 검증"""
        # Case A: TLS 활성화
        args = simulator.parse_args(["--tls", "--ca-file", FIXTURE_CA_PATH, "-c", "1"])
        mock_client_instance = MagicMock()
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=None)
        mock_client_instance.pending_calls_threshold = 100

        with patch("simulator.publish_house_power", new_callable=AsyncMock) as mock_pub, \
             patch("aiomqtt.Client", return_value=mock_client_instance) as mock_aiomqtt:
            mock_pub.return_value = {"house": "H001", "power": 60.0, "devices": []}
            asyncio.run(simulator.run_simulator(args))

            mock_aiomqtt.assert_called_once()
            _, kwargs = mock_aiomqtt.call_args
            self.assertIsNotNone(kwargs.get("tls_context"))
            self.assertIsInstance(kwargs["tls_context"], ssl.SSLContext)
            self.assertEqual(kwargs["port"], 8883)

        # Case B: TLS 비활성화 (평문)
        args_plain = simulator.parse_args(["--no-tls", "-c", "1"])
        mock_client_instance_plain = MagicMock()
        mock_client_instance_plain.__aenter__ = AsyncMock(return_value=mock_client_instance_plain)
        mock_client_instance_plain.__aexit__ = AsyncMock(return_value=None)
        mock_client_instance_plain.pending_calls_threshold = 100

        with patch("simulator.publish_house_power", new_callable=AsyncMock) as mock_pub, \
             patch("aiomqtt.Client", return_value=mock_client_instance_plain) as mock_aiomqtt_plain:
            mock_pub.return_value = {"house": "H001", "power": 60.0, "devices": []}
            asyncio.run(simulator.run_simulator(args_plain))

            mock_aiomqtt_plain.assert_called_once()
            _, kwargs = mock_aiomqtt_plain.call_args
            self.assertIsNone(kwargs.get("tls_context"))
            self.assertEqual(kwargs["port"], 1883)

    # -------------------------------------------------------------
    # 8. 웹 시뮬레이터 SimulatorManager의 aiomqtt.Client tls_context 전달 검증 (Mock)
    # -------------------------------------------------------------
    def test_12_web_simulator_manager_passes_tls_context(self):
        """웹 컨트롤러 SimulatorManager가 aiomqtt.Client에 올바른 tls_context를 전달하는지 Mock 검증"""
        manager = SimulatorManager(
            host="127.0.0.1",
            port=8883,
            username="test_user",
            password="test_password",
            tls_enabled=True,
            ca_file=FIXTURE_CA_PATH
        )

        mock_client_instance = MagicMock()
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=None)
        mock_client_instance.pending_calls_threshold = 100

        with patch("aiomqtt.Client", return_value=mock_client_instance) as mock_aiomqtt, \
             patch("simulator.publish_house_power", new_callable=AsyncMock) as mock_pub:
            mock_pub.return_value = {"house": "H001", "power": 55.0, "devices": []}

            stop_event = threading.Event()
            cfg = manager.get_connection_config()
            tls_context = get_mqtt_tls_context(tls_enabled=cfg["tls_enabled"], ca_file=cfg["ca_file"])

            # 1회만 루프를 돌고 종료하도록 stop_event 트리거 설정
            def stop_manager_side_effect(*args, **kwargs):
                stop_event.set()
                return {"house": "H001", "power": 55.0, "devices": []}

            mock_pub.side_effect = stop_manager_side_effect

            asyncio.run(manager._worker_loop("peak", "H001", stop_event, cfg, tls_context))

            mock_aiomqtt.assert_called_once()
            _, kwargs = mock_aiomqtt.call_args
            self.assertIsNotNone(kwargs.get("tls_context"))
            self.assertIsInstance(kwargs["tls_context"], ssl.SSLContext)
            self.assertEqual(kwargs["port"], 8883)

    # -------------------------------------------------------------
    # 9. 웹 시뮬레이터 start()의 런타임 동기 검증
    # -------------------------------------------------------------
    def test_13_web_manager_start_synchronous_tls_validation(self):
        """웹 시뮬레이터 manager.start() 호출 시 유효하지 않은 TLS 설정이면 동기적으로 예외 발생 및 워커 미가동"""
        # Case A: 존재하지 않는 CA 파일
        manager_invalid = SimulatorManager(
            tls_enabled=True,
            ca_file="/nonexistent/path/to/ca.crt"
        )
        with self.assertRaises(FileNotFoundError) as ctx:
            manager_invalid.start("peak", "H001")
        self.assertIn("찾을 수 없습니다", str(ctx.exception))
        self.assertFalse(manager_invalid.is_running, "오류 발생 시 is_running이 False여야 합니다.")
        self.assertIsNone(manager_invalid.worker_thread, "오류 발생 시 워커 스레드가 생성되지 않아야 합니다.")

        # Case B: CA 파일 미지정 (None)
        manager_missing = SimulatorManager(
            tls_enabled=True,
            ca_file=None
        )
        with self.assertRaises(ValueError) as ctx:
            manager_missing.start("peak", "H001")
        self.assertIn("CA 인증서 파일 경로", str(ctx.exception))
        self.assertFalse(manager_missing.is_running)

    # -------------------------------------------------------------
    # 10. 비밀번호 마스킹 및 출력 미노출 검증
    # -------------------------------------------------------------
    def test_14_password_not_exposed_in_logs_and_helpers(self):
        """비밀번호 마스킹 헬퍼 동작 및 설정/출력 시 비밀번호 평문 미노출 검증"""
        secret = "very_secret_production_password_1234!"
        self.assertEqual(mask_password(secret), "***")
        self.assertEqual(mask_password(""), "")
        self.assertEqual(mask_password(None), "")

        # resolve_mqtt_config의 결과는 dict에 보존되지만, 출력용 문자열에는 포함되지 않아야 함
        cfg = resolve_mqtt_config(password=secret, tls_enabled=True, ca_file=FIXTURE_CA_PATH)
        self.assertEqual(cfg["password"], secret)

        # 로그 출력 시뮬레이션
        tls_desc = f" (TLS ON | CA: {cfg['ca_file']})" if cfg["tls_enabled"] else " (평문)"
        log_line = f"[WebSimulator] MQTT 브로커({cfg['host']}:{cfg['port']}{tls_desc}) 연결 중..."
        self.assertNotIn(secret, log_line)

    def test_14b_password_not_exposed_in_cli_help(self):
        """
        환경변수(MQTT_PASS)에 테스트 비밀번호가 설정된 상태에서
        실제 CLI --help 출력을 캡처하여 비밀번호가 노출되지 않는지 엄격 검증
        """
        secret = "UltraSecretProductionPassword999!#$"
        with patch.dict(os.environ, {"MQTT_PASS": secret}):
            # 1. simulator.py --help 검증
            stdout_buf_sim = io.StringIO()
            with patch("sys.stdout", stdout_buf_sim):
                with self.assertRaises(SystemExit) as cm_sim:
                    simulator.parse_args(["--help"])
                self.assertEqual(cm_sim.exception.code, 0)
            help_output_sim = stdout_buf_sim.getvalue()
            self.assertNotIn(secret, help_output_sim, "simulator.py --help 출력에 비밀번호가 노출되었습니다!")
            self.assertIn("(default: None)", help_output_sim)

            # 2. web_server.py --help 검증
            stdout_buf_web = io.StringIO()
            with patch("sys.stdout", stdout_buf_web):
                with self.assertRaises(SystemExit) as cm_web:
                    web_server.parse_args(["--help"])
                self.assertEqual(cm_web.exception.code, 0)
            help_output_web = stdout_buf_web.getvalue()
            self.assertNotIn(secret, help_output_web, "web_server.py --help 출력에 비밀번호가 노출되었습니다!")
            self.assertIn("(default: None)", help_output_web)

            # 3. 실제 parse_args 실행 시에는 비밀번호가 정상 바인딩되는지 검증
            parsed = simulator.parse_args([])
            self.assertEqual(parsed.password, secret)

    # -------------------------------------------------------------
    # 11. 웹 API /api/start 호출 시 동기 오류(400 CONFIG_ERROR) 반환 검증
    # -------------------------------------------------------------
    def test_15_web_handler_returns_400_on_invalid_ca_at_start(self):
        """웹 API /api/start 호출 시 TLS 설정 오류(CA 누락/미존재)가 발생하면 HTTP 400 및 CONFIG_ERROR 반환 검증"""
        from server.request_handler import create_request_handler
        import json

        manager = SimulatorManager(tls_enabled=True, ca_file="/invalid/ca.crt")
        handler_cls = create_request_handler(manager, None)

        handler = handler_cls.__new__(handler_cls)
        handler.manager = manager
        handler.path = "/api/start"
        handler.headers = {"Content-Length": "2"}
        handler.rfile = io.BytesIO(b"{}")
        handler.wfile = io.BytesIO()
        handler.close_connection = False

        handler.send_response = MagicMock()
        handler.send_header = MagicMock()
        handler.end_headers = MagicMock()

        handler.do_POST()

        handler.send_response.assert_called_with(400)
        output_bytes = handler.wfile.getvalue()
        output_json = json.loads(output_bytes.decode("utf-8"))
        self.assertEqual(output_json["status"], "error")
        self.assertEqual(output_json["code"], "CONFIG_ERROR")
        self.assertIn("찾을 수 없습니다", output_json["message"])

    # -------------------------------------------------------------
    # 12. 웹 서버 바인딩 호스트 옵션 검증
    # -------------------------------------------------------------
    def test_16_web_server_bind_host_options(self):
        """웹 서버 기본 바인딩 호스트가 127.0.0.1이고 --bind-host 옵션이 정상 동작하는지 검증"""
        # 1. 기본 인자 파싱 시 127.0.0.1
        args_default = web_server.parse_args([])
        self.assertEqual(args_default.bind_host, "127.0.0.1")

        # 2. --bind-host 0.0.0.0 명시 파싱
        args_custom = web_server.parse_args(["--bind-host", "0.0.0.0"])
        self.assertEqual(args_custom.bind_host, "0.0.0.0")


if __name__ == "__main__":
    unittest.main(verbosity=2)
