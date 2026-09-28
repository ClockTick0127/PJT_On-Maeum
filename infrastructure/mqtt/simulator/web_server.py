"""
NILM 스마트홈 전력 시뮬레이터 경량 웹 컨트롤러 서버 (Web Controller Server)

브라우저 화면(waveform_viewer.html)의 버튼 조작에 따라 실제 Mosquitto MQTT 브로커로
전력 데이터를 실시간 발행(Publish)하고, 브라우저 차트와 완벽히 동기화(SSE)합니다.

실행 방법:
    python web_server.py
    python web_server.py 8089
    (브라우저가 자동으로 http://127.0.0.1:8085 로 열립니다)
"""

import argparse
import asyncio
import os
import sys
import threading
import webbrowser
from typing import Optional

# Windows SelectorLoop 호환성 설정
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.append(CURRENT_DIR)

import simulator
from server.config import DEFAULT_PORT, ALLOWED_SCENARIOS
from server.manager import SimulatorManager, ModeConflictError
from server.e2e_manager import E2EScheduleSessionManager
from server.request_handler import ThreadedHTTPServer, RequestHandler, create_request_handler
from engine.tls import resolve_mqtt_config, create_mqtt_tls_context

# waveform_viewer.html 절대 경로 계산
HTML_PATH = os.path.join(CURRENT_DIR, "waveform_viewer.html")

# 전역 SimulatorManager 및 RequestHandler 핸들
# 모듈 import 시점에는 지연 생성(None)하여 잘못된 환경변수가 있어도 CLI 옵션이 우선 적용되도록 합니다.
manager: Optional[SimulatorManager] = None
handler_class: Optional[type] = None


def parse_args(args=None):
    """웹 서버 실행 커맨드라인 인자 파싱"""
    parser = argparse.ArgumentParser(
        description="NILM 스마트홈 전력 시뮬레이터 경량 웹 컨트롤러 서버",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    # 위치 인자 포트 지원 (기존 python web_server.py 8089 호환)
    parser.add_argument(
        "port_pos",
        nargs="?",
        type=int,
        default=None,
        help="웹 서버 바인딩 포트 (위치 인자)"
    )
    parser.add_argument(
        "--web-port",
        type=int,
        default=DEFAULT_PORT,
        help="웹 서버 바인딩 포트"
    )
    parser.add_argument(
        "--bind-host",
        default="127.0.0.1",
        help="웹 서버 바인딩 호스트 (기본값: 127.0.0.1. 0.0.0.0 바인딩 시 인증되지 않은 제어 API가 외부에 노출되므로 주의)"
    )
    parser.add_argument(
        "--host",
        default=None,
        help="MQTT 브로커 호스트 주소 (미지정 시 MQTT_HOST 환경변수 또는 localhost)"
    )
    parser.add_argument(
        "--port", "-p",
        type=int,
        default=None,
        help="MQTT 브로커 포트 번호 (미지정 시 MQTT_PORT 환경변수 또는 TLS 여부에 따라 8883/1883)"
    )
    parser.add_argument(
        "--user", "-u",
        default=None,
        help="MQTT 인증 사용자명 (미지정 시 MQTT_USER 환경변수 또는 simulator_user)"
    )
    parser.add_argument(
        "--password",
        default=None,
        help="MQTT 인증 비밀번호 (미지정 시 MQTT_PASS 환경변수 또는 test1234)"
    )
    parser.add_argument(
        "--tls",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="MQTT TLS 활성화 여부 (--tls 또는 --no-tls, 미지정 시 MQTT_TLS_ENABLED 환경변수)"
    )
    parser.add_argument(
        "--ca-file",
        default=None,
        help="MQTT TLS CA 인증서 파일 경로 (미지정 시 MQTT_CA_FILE 환경변수)"
    )
    return parser.parse_args(args)


def main(args=None):
    global manager, handler_class

    parsed = parse_args(args)
    web_port = parsed.port_pos if parsed.port_pos is not None else parsed.web_port
    bind_host = parsed.bind_host

    # 0.0.0.0 바인딩 시 보안 경고 출력
    if bind_host in ("0.0.0.0", "::"):
        print(
            "[보안 경고] 웹 서버가 모든 네트워크 인터페이스(0.0.0.0)에 바인딩되었습니다.\n"
            "  인증되지 않은 제어 API(/api/start, /api/stop 등)가 외부에 노출될 수 있습니다.\n"
            "  운영 환경에서는 기본값(127.0.0.1)과 SSH 터널 포트 포워딩을 사용하거나,\n"
            "  EC2 보안 그룹에서 해당 포트 접근을 엄격히 제한하십시오.",
            file=sys.stderr,
            flush=True
        )

    # 1. 공통 MQTT 설정 해석 (포트 우선순위: --port > MQTT_PORT > TLS 8883 > 평문 1883)
    cfg = resolve_mqtt_config(
        host=parsed.host,
        port=parsed.port,
        username=parsed.user,
        password=parsed.password,
        tls_enabled=parsed.tls,
        ca_file=parsed.ca_file,
    )

    # 2. 기동 시점 TLS 설정 사전 검증 (잘못된 설정 시 서버 시작 전 즉각 실패)
    if cfg["tls_enabled"]:
        try:
            create_mqtt_tls_context(cfg["ca_file"])
        except Exception as err:
            print(f"[WebSimulator 오류] MQTT TLS 설정 검증 실패: {err}", file=sys.stderr, flush=True)
            sys.exit(1)

    # 3. 파싱된 MQTT 설정으로 SimulatorManager 및 RequestHandler 생성
    manager = SimulatorManager(
        host=cfg["host"],
        port=cfg["port"],
        username=cfg["username"],
        password=cfg["password"],
        tls_enabled=cfg["tls_enabled"],
        ca_file=cfg["ca_file"],
    )
    e2e_manager = E2EScheduleSessionManager(
        broker_config=cfg,
        broadcast_callback=manager.broadcast_external,
    )
    shared_start_lock = threading.Lock()
    handler_class = create_request_handler(
        manager,
        html_path=HTML_PATH,
        e2e_manager=e2e_manager,
        shared_start_lock=shared_start_lock,
    )

    server_address = (bind_host, web_port)
    httpd = ThreadedHTTPServer(server_address, handler_class)

    url = f"http://{bind_host}:{web_port}"
    tls_desc = f" (TLS ON | CA: {cfg['ca_file']})" if cfg["tls_enabled"] else " (TLS OFF (평문))"
    print(f"============================================================")
    print(f" NILM 전력 시뮬레이터 인터랙티브 웹 서버 가동")
    print(f" - 대시보드 URL : {url}")
    print(f" - 바인딩 호스트: {bind_host}")
    print(f" - MQTT 브로커  : {cfg['host']}:{cfg['port']}{tls_desc}")
    print(f" - 기능: 화면 버튼 클릭 시 실제 MQTT 발행 및 차트 실시간 렌더링")
    print(f" - 서버 종료: Ctrl + C")
    print(f"============================================================", flush=True)

    # 기본 브라우저 자동 오픈 (로컬 루프백 접속일 때만 오픈)
    if os.getenv("BROWSER") != "none" and bind_host in ("127.0.0.1", "localhost"):
        try:
            webbrowser.open(url)
        except Exception:
            pass

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n웹 서버를 정지합니다.", flush=True)
    finally:
        if e2e_manager is not None:
            e2e_manager.shutdown(timeout_sec=5.0)
        manager.stop()
        httpd.server_close()


if __name__ == "__main__":
    main()
