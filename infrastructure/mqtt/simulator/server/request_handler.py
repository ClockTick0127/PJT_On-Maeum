"""
NILM 스마트홈 전력 시뮬레이터 HTTP 요청 핸들러 모듈

ThreadedHTTPServer 및 BaseHTTPRequestHandler 기반의 RequestHandler 클래스를 정의합니다.
웹 프런트엔드 정적 파일(waveform_viewer.html) 서빙, REST API 제어 엔드포인트(/api/status,
/api/start, /api/stop, /api/device), Server-Sent Events(SSE) 실시간 스트리밍(/api/stream)을 처리합니다.
"""

from datetime import date, datetime
import json
import math
import os
import queue
import re
import sys
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from typing import Any

# 상위 디렉터리(infrastructure/mqtt/simulator) import 경로 등록
PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PARENT_DIR not in sys.path:
    sys.path.append(PARENT_DIR)

VIEWER_ASSETS = {
    "waveform.css": "text/css; charset=utf-8",
    "routine_demo.css": "text/css; charset=utf-8",
    "routine_demo.js": "text/javascript; charset=utf-8",
    "panels.js": "text/javascript; charset=utf-8",
    "state.js": "text/javascript; charset=utf-8",
    "households.js": "text/javascript; charset=utf-8",
    "devices.js": "text/javascript; charset=utf-8",
    "powerflow.js": "text/javascript; charset=utf-8",
    "charts.js": "text/javascript; charset=utf-8",
    "physics.js": "text/javascript; charset=utf-8",
    "realtime.js": "text/javascript; charset=utf-8",
    "server_stream.js": "text/javascript; charset=utf-8",
    "controls.js": "text/javascript; charset=utf-8",
    "events.js": "text/javascript; charset=utf-8",
    "e2e.js": "text/javascript; charset=utf-8",
    # waveform.css가 url()로 참조하는 패널 토글 아이콘.
    # HTML의 href/src가 아니라 CSS 안에서만 참조되므로 빠뜨리기 쉽다.
    "image/empty_electricity.png": "image/png",
    "image/full_electricity.png": "image/png",
    "image/On_house.png": "image/png",
    "image/Off_house.png": "image/png",
}

import simulator
import scenarios
from .config import ALLOWED_SCENARIOS, validate_interval
from .manager import SimulatorManager, ModeConflictError
from server.e2e_manager import (
    E2EError,
    E2EConflictError,
    E2ERunNotFoundError,
    E2EHouseholdNotFoundError,
    E2EStartTimeoutError,
    E2EStartError,
    E2EOperationTimeoutError,
    E2ESnapshotTimeoutError,
)
from engine.unified_catalog import list_scenario_api_summaries, list_unified_scenario_ids
from engine.schedule_executor import ExecutionMode


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


class RequestHandler(BaseHTTPRequestHandler):
    manager: SimulatorManager = None
    html_path: str = None
    e2e_manager: Any = None
    shared_start_lock: threading.Lock = threading.Lock()

    def log_message(self, format, *args):
        # 불필요한 표준 콘솔 로그 억제
        pass

    def handle(self):
        try:
            super().handle()
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            pass

    def do_GET(self):
        url_path = self.path.split('?')[0]

        if url_path in ("/", "/index.html", "/waveform_viewer.html"):
            # 1. waveform_viewer.html 파일 제공
            target_html = self.html_path or os.path.join(PARENT_DIR, "waveform_viewer.html")
            try:
                with open(target_html, "r", encoding="utf-8") as f:
                    content = f.read().encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)
            except Exception as e:
                self.send_error(500, f"HTML 파일 로드 실패: {e}")

        elif url_path in ("/routine-demo", "/routine-demo.html", "/routine_demo.html"):
            # 발표용 정상/루틴 누락 비교 페이지 제공
            viewer_path = self.html_path or os.path.join(PARENT_DIR, "waveform_viewer.html")
            target_html = os.path.join(os.path.dirname(viewer_path), "routine_demo.html")
            try:
                with open(target_html, "r", encoding="utf-8") as f:
                    content = f.read().encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)
            except Exception as e:
                self.send_error(500, f"시연 페이지 로드 실패: {e}")

        elif url_path.startswith("/assets/"):
            asset_name = url_path.removeprefix("/assets/")
            content_type = VIEWER_ASSETS.get(asset_name)
            if content_type is None:
                self.send_error(404, "Not Found")
                return
            html_path = self.html_path or os.path.join(PARENT_DIR, "waveform_viewer.html")
            asset_path = os.path.join(os.path.dirname(html_path), "assets", asset_name)
            try:
                with open(asset_path, "rb") as asset_file:
                    content = asset_file.read()
                self.send_response(200)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)
            except OSError as err:
                self.send_error(500, f"정적 파일 로드 실패: {err}")

        elif url_path == "/api/status":
            # 2. 현재 시뮬레이터 실행 상태 확인 (원자적 스냅샷 조회)
            cfg = self.manager.get_connection_config() if hasattr(self.manager, "get_connection_config") else {
                "host": simulator.DEFAULT_BROKER_HOST,
                "port": simulator.DEFAULT_BROKER_PORT,
                "tls_enabled": False
            }
            tls_desc = " (TLS)" if cfg.get("tls_enabled") else ""

            status_snap = self.manager.get_status()
            status_snap["broker"] = f"{cfg['host']}:{cfg['port']}{tls_desc}"

            self.send_json(200, status_snap)

        elif url_path == "/api/stream":
            # 3. Server-Sent Events (SSE) 실시간 데이터 스트리밍
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            q = queue.Queue(maxsize=100)
            self.manager.add_subscriber(q)

            try:
                # 초기 연결 확인 핑
                self.wfile.write(b": keepalive\n\n")
                self.wfile.flush()

                while True:
                    try:
                        # 1초 타임아웃으로 대기
                        data = q.get(timeout=1.5)
                        msg = f"data: {json.dumps(data)}\n\n".encode("utf-8")
                        self.wfile.write(msg)
                        self.wfile.flush()
                    except queue.Empty:
                        # 하트비트 핑
                        self.wfile.write(b": heartbeat\n\n")
                        self.wfile.flush()
            except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
                pass
            finally:
                self.close_connection = True
                self.manager.remove_subscriber(q)

        elif url_path == "/api/e2e/scenarios":
            if self.e2e_manager is None:
                self.send_error_json(503, "E2E_MANAGER_UNAVAILABLE", "E2E 세션 관리자가 주입되지 않았습니다.")
                return
            summaries = list_scenario_api_summaries()
            self.send_json(200, {
                "status": "success",
                "count": len(summaries),
                "scenarios": summaries
            })

        elif url_path.startswith("/api/e2e/"):
            if self.e2e_manager is None:
                self.send_error_json(503, "E2E_MANAGER_UNAVAILABLE", "E2E 세션 관리자가 주입되지 않았습니다.")
                return

            parts = url_path.strip("/").split("/")
            if len(parts) == 4 and parts[2] == "runs":
                run_id = parts[3]
                try:
                    snap = self.e2e_manager.get_session_snapshot(run_id, timeout_sec=1.0)
                    self.send_json(200, snap)
                except E2ERunNotFoundError as err:
                    self.send_error_json(404, "RUN_NOT_FOUND", str(err))
                except E2ESnapshotTimeoutError as err:
                    self.send_error_json(503, "E2E_SNAPSHOT_TIMEOUT", str(err))
                except RuntimeError as err:
                    self.send_error_json(503, "E2E_MANAGER_UNAVAILABLE", str(err))
                except Exception as err:
                    self.send_error_json(500, "INTERNAL_ERROR", str(err))
            elif len(parts) == 6 and parts[2] == "runs" and parts[4] == "households":
                run_id = parts[3]
                h_id = parts[5]
                try:
                    h_snap = self.e2e_manager.get_household_snapshot(run_id, h_id, timeout_sec=1.0)
                    self.send_json(200, {
                        "status": "success",
                        "household": h_snap
                    })
                except E2ERunNotFoundError as err:
                    self.send_error_json(404, "RUN_NOT_FOUND", str(err))
                except E2EHouseholdNotFoundError as err:
                    self.send_error_json(404, "HOUSEHOLD_NOT_FOUND", str(err))
                except E2ESnapshotTimeoutError as err:
                    self.send_error_json(503, "E2E_SNAPSHOT_TIMEOUT", str(err))
                except RuntimeError as err:
                    self.send_error_json(503, "E2E_MANAGER_UNAVAILABLE", str(err))
                except Exception as err:
                    self.send_error_json(500, "INTERNAL_ERROR", str(err))
            else:
                self.send_error(404, "Not Found")

        else:
            self.send_error(404, "Not Found")

    def send_json(self, status_code: int, data: dict):
        content = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(content)))
        if status_code >= 400:
            self.send_header("Connection", "close")
            self.close_connection = True
        self.end_headers()
        self.wfile.write(content)
        try:
            self.wfile.flush()
        except Exception:
            pass

    def send_error_json(self, status_code: int, error_code: str, message: str):
        self.send_json(status_code, {
            "status": "error",
            "code": error_code,
            "message": message
        })

    def read_json_body(self, allow_empty: bool = False):
        """Content-Length 및 JSON 본문을 안전하게 파싱하고 검증한다.
        성공 시 (data, True)를 반환하고, 실패 시 400 JSON 응답을 전송하고 (None, False)를 반환한다.
        """
        raw_length = self.headers.get("Content-Length")
        if raw_length is None:
            if allow_empty:
                return {}, True
            self.send_error_json(400, "BAD_REQUEST", "Content-Length 헤더가 누락되었거나 요청 본문이 비어있습니다.")
            return None, False

        try:
            content_length = int(raw_length.strip())
        except (ValueError, TypeError, AttributeError):
            self.send_error_json(400, "BAD_REQUEST", f"올바르지 않은 Content-Length 값입니다: '{raw_length}'")
            return None, False

        if content_length < 0:
            self.send_error_json(400, "BAD_REQUEST", f"Content-Length는 음수일 수 없습니다: {content_length}")
            return None, False

        if content_length == 0:
            if allow_empty:
                return {}, True
            self.send_error_json(400, "BAD_REQUEST", "요청 본문이 비어있습니다.")
            return None, False

        try:
            raw_body = self.rfile.read(content_length)
        except Exception as err:
            self.send_error_json(400, "BAD_REQUEST", f"요청 본문 읽기 실패: {err}")
            return None, False

        try:
            body_str = raw_body.decode("utf-8")
        except UnicodeDecodeError:
            self.send_error_json(400, "BAD_REQUEST", "UTF-8 디코딩에 실패했습니다.")
            return None, False

        if not body_str.strip():
            if allow_empty:
                return {}, True
            self.send_error_json(400, "BAD_REQUEST", "요청 본문이 비어있습니다.")
            return None, False

        def reject_constant(value):
            raise ValueError(f"비표준 JSON 숫자: {value}")

        try:
            params = json.loads(body_str, parse_constant=reject_constant)
        except Exception:
            self.send_error_json(400, "BAD_REQUEST", "유효하지 않은 JSON 형식입니다.")
            return None, False

        if not isinstance(params, dict):
            self.send_error_json(400, "BAD_REQUEST", "JSON 객체(dictionary)여야 합니다.")
            return None, False

        return params, True

    def do_POST(self):
        url_path = self.path.split('?')[0]

        if url_path == "/api/start":
            # 1. 시뮬레이션 시작 (MQTT 발행 + 실시간 스트림)
            params, ok = self.read_json_body(allow_empty=True)
            if not ok:
                return

            has_households = ("households" in params)
            has_single_fields = ("house" in params or "scenario" in params)

            # house/scenario 단일 지정과 households 다중 지정 동시 입력 거절
            if has_households and has_single_fields:
                self.send_error_json(400, "BAD_REQUEST", "house/scenario 단일 지정과 households 다중 지정은 동시에 전달할 수 없습니다.")
                return

            if has_households:
                raw_households = params["households"]
                if not isinstance(raw_households, list):
                    self.send_error_json(400, "BAD_REQUEST", "households 필드는 배열(list)이어야 합니다.")
                    return
                if len(raw_households) == 0:
                    self.send_error_json(400, "BAD_REQUEST", "households 배열은 비어있을 수 없습니다.")
                    return
                if len(raw_households) > len(simulator.DEFAULT_HOUSES):
                    self.send_error_json(400, "BAD_REQUEST", f"최대 가구 수({len(simulator.DEFAULT_HOUSES)}개)를 초과할 수 없습니다.")
                    return

                seen_houses = set()
                normalized_households = []
                allowed_item_keys = {"house", "scenario"}
                for item in raw_households:
                    if not isinstance(item, dict):
                        self.send_error_json(400, "BAD_REQUEST", "households 배열의 각 원소는 객체(dict)여야 합니다.")
                        return
                    if "house" not in item or "scenario" not in item:
                        self.send_error_json(400, "BAD_REQUEST", "households 원소에는 'house'와 'scenario' 필드가 필수입니다.")
                        return
                    extra_keys = set(item.keys()) - allowed_item_keys
                    if extra_keys:
                        self.send_error_json(400, "BAD_REQUEST", f"households 원소에 허용되지 않은 필드가 포함되어 있습니다: {sorted(extra_keys)}")
                        return

                    h_id = item["house"]
                    sc = item["scenario"]
                    if not isinstance(h_id, str) or not isinstance(sc, str):
                        self.send_error_json(400, "BAD_REQUEST", "house와 scenario는 문자열이어야 합니다.")
                        return
                    if h_id not in simulator.DEFAULT_HOUSES:
                        self.send_error_json(400, "BAD_REQUEST", f"유효하지 않은 house ID입니다: '{h_id}'. 허용 목록: {simulator.DEFAULT_HOUSES}")
                        return
                    if sc not in ALLOWED_SCENARIOS:
                        self.send_error_json(400, "BAD_REQUEST", f"지원하지 않는 시나리오입니다: '{sc}'. 허용 목록: {sorted(ALLOWED_SCENARIOS)}")
                        return
                    if h_id in seen_houses:
                        self.send_error_json(400, "BAD_REQUEST", f"중복된 가구 ID가 존재합니다: '{h_id}'")
                        return

                    seen_houses.add(h_id)
                    normalized_households.append({"house": h_id, "scenario": sc})

                normalized_households = sorted(normalized_households, key=lambda x: x["house"])

                if any(item["scenario"] == "normal_routine" and item["house"] != "H001" for item in normalized_households):
                    self.send_error_json(400, "BAD_REQUEST", "normal_routine 시나리오는 H001 가구에서만 실행할 수 있습니다.")
                    return
                if any(item["scenario"] == "prolonged_use" and item["house"] != "H001" for item in normalized_households):
                    self.send_error_json(400, "BAD_REQUEST", "prolonged_use 시나리오는 H001 가구에서만 실행할 수 있습니다.")
                    return
                if any(item["scenario"] == "routine_missed_demo" for item in normalized_households):
                    self.send_error_json(400, "BAD_REQUEST", "routine_missed_demo 시나리오는 단일 가구 요청에서만 실행할 수 있습니다.")
                    return

                has_normal = any(item["scenario"] == "normal_routine" for item in normalized_households)
                has_missed = any(item["scenario"] == "routine_missed" for item in normalized_households)
                if has_normal and has_missed:
                    self.send_error_json(400, "BAD_REQUEST", "normal_routine과 routine_missed는 동일한 다중 실행에서 함께 사용할 수 없습니다.")
                    return
                has_prolonged = any(item["scenario"] == "prolonged_use" for item in normalized_households)
                if has_prolonged and (has_normal or has_missed):
                    self.send_error_json(400, "BAD_REQUEST", "prolonged_use는 normal_routine·routine_missed와 동일한 다중 실행에서 함께 사용할 수 없습니다.")
                    return
            else:
                house = params.get("house", "H001")
                scenario = params.get("scenario", "peak")

                if not isinstance(house, str) or house not in simulator.DEFAULT_HOUSES:
                    self.send_error_json(400, "BAD_REQUEST", f"유효하지 않은 house ID입니다: '{house}'. 허용 목록: {simulator.DEFAULT_HOUSES}")
                    return
                if not isinstance(scenario, str) or scenario not in ALLOWED_SCENARIOS:
                    self.send_error_json(400, "BAD_REQUEST", f"지원하지 않는 시나리오입니다: '{scenario}'. 허용 목록: {sorted(ALLOWED_SCENARIOS)}")
                    return
                if scenario == "normal_routine" and house != "H001":
                    self.send_error_json(400, "BAD_REQUEST", "normal_routine 시나리오는 H001 가구에서만 실행할 수 있습니다.")
                    return
                if scenario == "prolonged_use" and house != "H001":
                    self.send_error_json(400, "BAD_REQUEST", "prolonged_use 시나리오는 H001 가구에서만 실행할 수 있습니다.")
                    return
                if scenario == "routine_missed_demo" and house != "H001":
                    self.send_error_json(400, "BAD_REQUEST", "routine_missed_demo 시나리오는 H001 가구에서만 실행할 수 있습니다.")
                    return

                normalized_households = [{"house": house, "scenario": scenario}]

            # simulation_date 선택 필드 엄격 검증
            simulation_date = None
            if "simulation_date" in params and params["simulation_date"] is not None:
                raw_sim_date = params["simulation_date"]
                if not isinstance(raw_sim_date, str):
                    self.send_error_json(
                        400,
                        "BAD_REQUEST",
                        "잘못된 simulation_date 필드입니다: simulation_date는 YYYY-MM-DD 형식의 문자열이어야 합니다."
                    )
                    return
                try:
                    simulation_date = scenarios.parse_simulation_date(raw_sim_date)
                except ValueError as err:
                    self.send_error_json(
                        400,
                        "BAD_REQUEST",
                        f"잘못된 simulation_date 필드입니다: {err}"
                    )
                    return

            # interval 선택 필드 엄격 검증
            interval = None
            if "interval" in params:
                raw_interval = params["interval"]
                try:
                    interval = validate_interval(raw_interval)
                except ValueError as err:
                    self.send_error_json(
                        400,
                        "BAD_REQUEST",
                        f"잘못된 interval 필드입니다: {err}"
                    )
                    return

            # fault_duration_sec 선택 필드 엄격 검증
            fault_duration_sec = None
            if "fault_duration_sec" in params:
                has_sf = any(h["scenario"] == "sensor_fault" for h in normalized_households)
                if not has_sf:
                    self.send_error_json(
                        400,
                        "BAD_REQUEST",
                        "sensor_fault 시나리오가 포함되지 않은 요청에는 fault_duration_sec를 지정할 수 없습니다."
                    )
                    return
                raw_fds = params["fault_duration_sec"]
                if type(raw_fds) is not int or isinstance(raw_fds, bool):
                    self.send_error_json(
                        400,
                        "BAD_REQUEST",
                        "fault_duration_sec는 1~3600 사이의 정수여야 합니다."
                    )
                    return
                if not (1 <= raw_fds <= 3600):
                    self.send_error_json(
                        400,
                        "BAD_REQUEST",
                        "fault_duration_sec는 1~3600 범위의 정수여야 합니다."
                    )
                    return
                fault_duration_sec = raw_fds

            # seed 선택 필드 엄격 검증 (null/미지정이면 서버가 생성)
            seed = None
            if "seed" in params and params["seed"] is not None:
                try:
                    seed = simulator.resolve_seed(params["seed"])
                except ValueError as err:
                    self.send_error_json(400, "BAD_REQUEST", f"잘못된 seed 필드입니다: {err}")
                    return

            # start_time 선택 필드 엄격 검증 ("HH:MM" / "HH:MM:SS", KST)
            start_time = None
            if "start_time" in params and params["start_time"] is not None:
                try:
                    scenarios.resolve_fixed_start_time(params["start_time"], simulation_date=simulation_date)
                except ValueError as err:
                    self.send_error_json(400, "BAD_REQUEST", f"잘못된 start_time 필드입니다: {err}")
                    return
                start_time = params["start_time"].strip()

            kwargs = {}
            if interval is not None:
                kwargs["interval"] = interval
            if fault_duration_sec is not None:
                kwargs["fault_duration_sec"] = fault_duration_sec
            if seed is not None:
                kwargs["seed"] = seed
            if start_time is not None:
                kwargs["start_time"] = start_time

            with self.shared_start_lock:
                if self.e2e_manager is not None and self.e2e_manager.is_active():
                    self.send_error_json(
                        409,
                        "E2E_SIMULATOR_RUNNING",
                        "E2E 시뮬레이터 세션이 실행 중이므로 레거시 시뮬레이터를 시작할 수 없습니다."
                    )
                    return

                try:
                    if has_households:
                        started_info = self.manager.start(
                            simulation_date=simulation_date,
                            households=normalized_households,
                            **kwargs
                        )
                    else:
                        started_info = self.manager.start(
                            scenario=scenario,
                            house=house,
                            simulation_date=simulation_date,
                            **kwargs
                        )
                    resp_payload = {
                        "status": "started",
                        "scenario": started_info.get("scenario"),
                        "house": started_info.get("house"),
                        "households": started_info.get("households"),
                        "simulation_date": started_info.get("simulation_date"),
                        "resolved_start_time": started_info.get("resolved_start_time"),
                        "interval": started_info.get("interval"),
                        "speed": started_info.get("speed")
                    }
                    if "fault_duration_sec" in started_info:
                        resp_payload["fault_duration_sec"] = started_info["fault_duration_sec"]
                    if "seed" in started_info:
                        resp_payload["seed"] = started_info["seed"]
                    self.send_json(200, resp_payload)
                except ValueError as err:
                    err_msg = str(err)
                    code = "CONFIG_ERROR" if ("TLS" in err_msg or "CA" in err_msg) else "BAD_REQUEST"
                    self.send_error_json(400, code, err_msg)
                except (FileNotFoundError, PermissionError) as err:
                    self.send_error_json(400, "CONFIG_ERROR", str(err))
                except RuntimeError as err:
                    self.send_error_json(409, "START_FAILED", str(err))
                except Exception as err:
                    self.send_error_json(500, "START_FAILED", str(err))

        elif url_path == "/api/speed":
            # 2. 배속(발행 주기) 동적 변경
            params, ok = self.read_json_body(allow_empty=False)
            if not ok:
                return

            allowed_keys = {"interval", "speed"}
            extra_keys = set(params.keys()) - allowed_keys
            if extra_keys:
                self.send_error_json(400, "BAD_REQUEST", f"허용되지 않은 필드가 포함되어 있습니다: {sorted(extra_keys)}")
                return

            has_interval = ("interval" in params)
            has_speed = ("speed" in params)

            if has_interval and has_speed:
                self.send_error_json(400, "BAD_REQUEST", "interval과 speed 필드는 동시에 전달할 수 없습니다.")
                return

            if not has_interval and not has_speed:
                self.send_error_json(400, "BAD_REQUEST", "interval 또는 speed 필드 중 하나는 필수입니다.")
                return

            if has_interval:
                raw_interval = params["interval"]
                try:
                    target_interval = validate_interval(raw_interval)
                except ValueError as err:
                    self.send_error_json(400, "BAD_REQUEST", f"잘못된 interval 값입니다: {err}")
                    return
            else:
                raw_speed = params["speed"]
                if raw_speed is None:
                    self.send_error_json(400, "BAD_REQUEST", "speed 필드는 필수입니다.")
                    return
                if isinstance(raw_speed, bool):
                    self.send_error_json(400, "BAD_REQUEST", "speed는 boolean 타입일 수 없습니다.")
                    return
                if not isinstance(raw_speed, (int, float)):
                    self.send_error_json(400, "BAD_REQUEST", f"speed는 숫자여야 합니다. (전달된 타입: {type(raw_speed).__name__})")
                    return
                val_float = float(raw_speed)
                if not math.isfinite(val_float) or val_float <= 0:
                    self.send_error_json(400, "BAD_REQUEST", "speed는 0보다 큰 유한한 숫자여야 합니다.")
                    return
                converted_interval = 1.0 / val_float
                try:
                    target_interval = validate_interval(converted_interval)
                except ValueError as err:
                    self.send_error_json(400, "BAD_REQUEST", f"환산된 interval 값이 유효하지 않습니다: {err}")
                    return

            try:
                res = self.manager.set_interval(target_interval)
                self.send_json(200, res)
            except ModeConflictError as err:
                self.send_error_json(409, "INVALID_MODE", str(err))
            except ValueError as err:
                self.send_error_json(400, "BAD_REQUEST", str(err))
            except Exception as err:
                self.send_error_json(500, "INTERNAL_ERROR", f"서버 내부 오류: {err}")

        elif url_path == "/api/stop":
            # 3. 시뮬레이션 중지
            stopped = self.manager.stop()
            status_code = 200 if stopped else 503
            resp = {"status": "stopped" if stopped else "stop_timeout"}
            self.send_json(status_code, resp)

        elif url_path == "/api/pause":
            # 4. 시뮬레이션 일시정지
            try:
                res = self.manager.pause()
                self.send_json(200, res)
            except ModeConflictError as err:
                self.send_error_json(409, "INVALID_MODE", str(err))
            except Exception as err:
                self.send_error_json(500, "PAUSE_FAILED", str(err))

        elif url_path == "/api/resume":
            # 5. 시뮬레이션 재개
            try:
                res = self.manager.resume()
                self.send_json(200, res)
            except ModeConflictError as err:
                self.send_error_json(409, "INVALID_MODE", str(err))
            except Exception as err:
                self.send_error_json(500, "RESUME_FAILED", str(err))

        elif url_path == "/api/reset":
            # 6. 시뮬레이터 완전 초기화
            try:
                res = self.manager.reset()
                self.send_json(200, res)
            except RuntimeError as err:
                self.send_error_json(503, "RESET_FAILED", str(err))
            except Exception as err:
                self.send_error_json(500, "RESET_FAILED", str(err))

        elif url_path == "/api/e2e/runs":
            if self.e2e_manager is None:
                self.send_error_json(503, "E2E_MANAGER_UNAVAILABLE", "E2E 세션 관리자가 주입되지 않았습니다.")
                return

            params, ok = self.read_json_body(allow_empty=False)
            if not ok:
                return

            if not isinstance(params, dict):
                self.send_error_json(400, "BAD_REQUEST", "요청 본문은 JSON 객체여야 합니다.")
                return

            allowed_top_keys = {"reference_date", "start_time", "execution", "households", "timezone"}
            extra_keys = set(params.keys()) - allowed_top_keys
            if extra_keys:
                self.send_error_json(400, "BAD_REQUEST", f"허용되지 않은 필드가 포함되어 있습니다: {sorted(extra_keys)}")
                return

            for req_key in ("reference_date", "execution", "households"):
                if req_key not in params:
                    self.send_error_json(400, "BAD_REQUEST", f"필수 필드가 누락되었습니다: '{req_key}'")
                    return

            ref_date_raw = params["reference_date"]
            if not isinstance(ref_date_raw, str) or not re.match(r"^\d{4}-\d{2}-\d{2}$", ref_date_raw):
                self.send_error_json(400, "BAD_REQUEST", f"잘못된 reference_date 형식입니다: '{ref_date_raw}'. YYYY-MM-DD여야 합니다.")
                return
            try:
                parsed_date = datetime.strptime(ref_date_raw, "%Y-%m-%d").date()
                if parsed_date.strftime("%Y-%m-%d") != ref_date_raw:
                    self.send_error_json(400, "BAD_REQUEST", f"존재하지 않는 날짜입니다: '{ref_date_raw}'")
                    return
            except ValueError:
                self.send_error_json(400, "BAD_REQUEST", f"존재하지 않는 날짜입니다: '{ref_date_raw}'")
                return

            if "start_time" in params:
                st_raw = params["start_time"]
                if st_raw is not None:
                    if not isinstance(st_raw, str):
                        self.send_error_json(400, "BAD_REQUEST", "start_time 필드는 문자열이어야 합니다.")
                        return
                    clean_st = st_raw.strip()
                    is_valid_time = False
                    if re.match(r"^\d{2}:\d{2}(?::\d{2})?$", clean_st):
                        parts = clean_st.split(":")
                        h = int(parts[0])
                        m = int(parts[1])
                        s = int(parts[2]) if len(parts) == 3 else 0
                        if 0 <= h <= 23 and 0 <= m <= 59 and 0 <= s <= 59:
                            is_valid_time = True
                    else:
                        try:
                            datetime.fromisoformat(clean_st)
                            is_valid_time = True
                        except ValueError:
                            pass

                    if not is_valid_time:
                        self.send_error_json(400, "BAD_REQUEST", f"유효하지 않은 start_time 형식입니다: '{st_raw}'. HH:MM[:SS] 또는 ISO 형식이어야 합니다.")
                        return

            if "timezone" in params:
                tz_raw = params["timezone"]
                if not isinstance(tz_raw, str) or tz_raw != "Asia/Seoul":
                    self.send_error_json(400, "BAD_REQUEST", f"유효하지 않은 timezone입니다: '{tz_raw}'. 'Asia/Seoul'만 지원합니다.")
                    return

            exec_raw = params["execution"]
            if not isinstance(exec_raw, dict):
                self.send_error_json(400, "BAD_REQUEST", "execution 필드는 객체여야 합니다.")
                return
            allowed_exec_keys = {"mode", "speed"}
            extra_exec_keys = set(exec_raw.keys()) - allowed_exec_keys
            if extra_exec_keys:
                self.send_error_json(400, "BAD_REQUEST", f"execution에 허용되지 않은 필드가 있습니다: {sorted(extra_exec_keys)}")
                return
            if "mode" not in exec_raw:
                self.send_error_json(400, "BAD_REQUEST", "execution.mode 필드는 필수입니다.")
                return
            mode_str = exec_raw["mode"]
            if mode_str not in ("BURST", "REALTIME", "ACCELERATED"):
                self.send_error_json(400, "BAD_REQUEST", f"유효하지 않은 execution.mode입니다: '{mode_str}'. 허용값: BURST, REALTIME, ACCELERATED")
                return

            if mode_str == "ACCELERATED":
                # speed 생략 시 가구 수 기반 안전 배속을 매니저가 자동 산출한다.
                # (DEFAULT_SAFE_AGGREGATE_RATE / 가구 수)
                if "speed" in exec_raw and exec_raw["speed"] is not None:
                    speed = exec_raw["speed"]
                    if (
                        type(speed) not in (int, float)
                        or isinstance(speed, bool)
                        or not math.isfinite(speed)
                        or speed <= 0
                    ):
                        self.send_error_json(400, "BAD_REQUEST", f"speed는 0보다 큰 유한한 숫자여야 합니다: {speed}")
                        return
            else:
                if "speed" in exec_raw and exec_raw["speed"] is not None:
                    self.send_error_json(400, "BAD_REQUEST", f"{mode_str} 모드에서는 speed 필드를 지정할 수 없습니다.")
                    return

            households_raw = params["households"]
            if not isinstance(households_raw, list) or len(households_raw) < 1 or len(households_raw) > 10:
                self.send_error_json(400, "BAD_REQUEST", "households는 1개 이상 10개 이하의 가구를 포함해야 합니다.")
                return

            valid_scenario_ids = set(list_unified_scenario_ids())
            allowed_house_ids = {f"H{i:03d}" for i in range(1, 11)}
            seen_h_ids = set()

            for idx, h_item in enumerate(households_raw):
                if not isinstance(h_item, dict):
                    self.send_error_json(400, "BAD_REQUEST", f"households[{idx}] 항목은 객체여야 합니다.")
                    return
                extra_h_keys = set(h_item.keys()) - {"household_id", "scenario"}
                if extra_h_keys:
                    self.send_error_json(400, "BAD_REQUEST", f"households[{idx}]에 허용되지 않은 필드가 있습니다: {sorted(extra_h_keys)}")
                    return
                if "household_id" not in h_item or "scenario" not in h_item:
                    self.send_error_json(400, "BAD_REQUEST", f"households[{idx}]에 household_id와 scenario는 필수입니다.")
                    return
                h_id = h_item["household_id"]
                sc_id = h_item["scenario"]
                if not isinstance(h_id, str) or h_id not in allowed_house_ids:
                    self.send_error_json(400, "BAD_REQUEST", f"유효하지 않은 household_id입니다: '{h_id}'. 허용 범위: H001~H010")
                    return
                if h_id in seen_h_ids:
                    self.send_error_json(400, "BAD_REQUEST", f"중복된 household_id가 존재합니다: '{h_id}'")
                    return
                seen_h_ids.add(h_id)
                if not isinstance(sc_id, str) or sc_id not in valid_scenario_ids:
                    self.send_error_json(400, "BAD_REQUEST", f"유효하지 않은 시나리오입니다: '{sc_id}'. 허용 목록: {sorted(valid_scenario_ids)}")
                    return

            with self.shared_start_lock:
                if self.manager is not None:
                    legacy_status = self.manager.get_status()
                    if bool(legacy_status.get("is_running", False)):
                        self.send_error_json(
                            409,
                            "LEGACY_SIMULATOR_RUNNING",
                            "레거시 시뮬레이터가 실행 중이므로 E2E 시뮬레이터를 시작할 수 없습니다."
                        )
                        return

                if self.e2e_manager.is_active():
                    self.send_error_json(
                        409,
                        "E2E_SIMULATOR_RUNNING",
                        "이미 실행 중인 활성 E2E 세션이 존재합니다."
                    )
                    return

                try:
                    resp = self.e2e_manager.create_and_start_session(params)
                    self.send_json(202, resp)
                except E2EConflictError as err:
                    self.send_error_json(409, "E2E_SIMULATOR_RUNNING", str(err))
                except E2EStartTimeoutError as err:
                    self.send_error_json(503, "E2E_START_TIMEOUT", str(err))
                except E2EStartError as err:
                    self.send_error_json(500, "START_FAILED", str(err))
                except RuntimeError as err:
                    self.send_error_json(503, "E2E_MANAGER_UNAVAILABLE", str(err))
                except Exception as err:
                    self.send_error_json(500, "START_FAILED", f"E2E 세션 시작 실패: {err}")

        elif url_path.startswith("/api/e2e/"):
            if self.e2e_manager is None:
                self.send_error_json(503, "E2E_MANAGER_UNAVAILABLE", "E2E 세션 관리자가 주입되지 않았습니다.")
                return

            parts = url_path.strip("/").split("/")
            if len(parts) == 5 and parts[2] == "runs" and parts[4] == "stop":
                run_id = parts[3]
                try:
                    res = self.e2e_manager.stop_session(run_id)
                    self.send_json(202, res)
                except E2ERunNotFoundError as err:
                    self.send_error_json(404, "RUN_NOT_FOUND", str(err))
                except E2EOperationTimeoutError as err:
                    self.send_error_json(503, "E2E_OPERATION_TIMEOUT", str(err))
                except RuntimeError as err:
                    self.send_error_json(503, "E2E_MANAGER_UNAVAILABLE", str(err))
                except Exception as err:
                    self.send_error_json(500, "STOP_FAILED", str(err))

            elif len(parts) == 7 and parts[2] == "runs" and parts[4] == "households":
                run_id = parts[3]
                h_id = parts[5]
                action = parts[6]

                try:
                    if action == "pause":
                        res = self.e2e_manager.pause_household(run_id, h_id)
                    elif action == "resume":
                        res = self.e2e_manager.resume_household(run_id, h_id)
                    elif action == "stop":
                        res = self.e2e_manager.stop_household(run_id, h_id)
                    else:
                        self.send_error(404, "Not Found")
                        return
                    status_code = 200 if res.get("status") == "success" else 202
                    self.send_json(status_code, res)
                except E2ERunNotFoundError as err:
                    self.send_error_json(404, "RUN_NOT_FOUND", str(err))
                except E2EHouseholdNotFoundError as err:
                    self.send_error_json(404, "HOUSEHOLD_NOT_FOUND", str(err))
                except E2EOperationTimeoutError as err:
                    self.send_error_json(503, "E2E_OPERATION_TIMEOUT", str(err))
                except ValueError as err:
                    val_err = str(err).strip("'")
                    code = val_err if val_err == "TASK_ALREADY_TERMINAL" else "INVALID_STATE"
                    self.send_error_json(409, code, f"태스크 상태 오류: {err}")
                except RuntimeError as err:
                    self.send_error_json(503, "E2E_MANAGER_UNAVAILABLE", str(err))
                except Exception as err:
                    self.send_error_json(500, "ACTION_FAILED", str(err))
            else:
                self.send_error(404, "Not Found")

        else:
            self.send_error(404, "Not Found")

    def do_PUT(self):
        url_path = self.path.split('?')[0]

        if url_path == "/api/device":
            params, ok = self.read_json_body(allow_empty=False)
            if not ok:
                return

            house = params.get("house")
            device = params.get("device")
            enabled = params.get("enabled")

            if house is None or device is None or enabled is None:
                self.send_error_json(400, "BAD_REQUEST", "house, device, enabled 필드는 필수입니다.")
                return

            if not isinstance(house, str) or not isinstance(device, str) or type(enabled) is not bool:
                self.send_error_json(400, "BAD_REQUEST", "house와 device는 문자열, enabled는 정확히 bool 타입이어야 합니다.")
                return

            if house not in simulator.DEFAULT_HOUSES:
                self.send_error_json(400, "BAD_REQUEST", f"지원하지 않는 가구 ID입니다: '{house}'")
                return

            if device not in simulator.DEVICE_PROFILES:
                self.send_error_json(400, "BAD_REQUEST", f"지원하지 않는 가전입니다: '{device}'")
                return

            try:
                snapshot = self.manager.set_device(house, device, enabled)
                resp = {
                    "status": "success",
                    "house": house,
                    "device": device,
                    "enabled": enabled,
                    "device_state": snapshot
                }
                self.send_json(200, resp)
            except ModeConflictError as err:
                self.send_error_json(409, "INVALID_MODE", str(err))
            except ValueError as err:
                self.send_error_json(400, "BAD_REQUEST", str(err))
            except Exception as err:
                self.send_error_json(500, "INTERNAL_ERROR", f"서버 내부 오류: {err}")

        else:
            self.send_error(404, "Not Found")

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()


def create_request_handler(
    manager: SimulatorManager,
    html_path: str = None,
    e2e_manager: Any = None,
    shared_start_lock: threading.Lock = None,
) -> type:
    """SimulatorManager 인스턴스와 HTML 경로, E2E 관리자가 주입된 RequestHandler 클래스를 생성합니다."""
    class InjectedRequestHandler(RequestHandler):
        pass

    InjectedRequestHandler.manager = manager
    InjectedRequestHandler.html_path = html_path
    InjectedRequestHandler.e2e_manager = e2e_manager
    InjectedRequestHandler.shared_start_lock = shared_start_lock if shared_start_lock is not None else threading.Lock()
    return InjectedRequestHandler
