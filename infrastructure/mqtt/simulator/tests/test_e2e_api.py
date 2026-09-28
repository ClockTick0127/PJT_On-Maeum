"""
NILM 스마트홈 시뮬레이터 E2E 시나리오 엔진 5A REST API 단위/통합 테스트 (test_e2e_api.py)

검증 대상 (API 8종 및 하위 호환성):
1. GET  /api/e2e/scenarios (통합 카탈로그 10종 요약 반환)
2. POST /api/e2e/runs (단일 세션 시작 202, 엄격한 입력 검증 400, 상호 배제 409)
3. GET  /api/e2e/runs/{run_id} (세션 전체 불변 스냅샷 조회 200, 404, 타임아웃 503)
4. GET  /api/e2e/runs/{run_id}/households/{household_id} (가구별 상세 메트릭 200, 404, 타임아웃 503)
5. POST /api/e2e/runs/{run_id}/households/{household_id}/pause (가구 일시정지 202, 멱등 200, 404, 409)
6. POST /api/e2e/runs/{run_id}/households/{household_id}/resume (가구 재개 202, 멱등 200, 404, 409)
7. POST /api/e2e/runs/{run_id}/households/{household_id}/stop (가구 중단 202, 멱등 200, 404, 409)
8. POST /api/e2e/runs/{run_id}/stop (세션 일괄 중단 202, 404)
9. e2e_manager 미주입 시 503 E2E_MANAGER_UNAVAILABLE 및 기존 레거시 API 정상 동작
10. shared_start_lock 및 SimulatorManager.get_status() 계약을 통한 레거시/E2E 상호 배제
11. 실제 SimulatorManager 인스턴스를 주입한 E2E 시작 호환성 검증
"""
from __future__ import annotations

import json
import re
import threading
import time
import unittest
import urllib.error
import urllib.request
from unittest.mock import MagicMock, patch

from server.config import DEFAULT_SAFE_AGGREGATE_RATE, resolve_auto_speed
from server.manager import SimulatorManager
from server.e2e_manager import E2EScheduleSessionManager, E2ESnapshotTimeoutError
from server.request_handler import ThreadedHTTPServer, create_request_handler
from tests.test_e2e_manager import FakeMqttFactory


class TestE2EApi(unittest.TestCase):
    """E2E REST API 8개 엔드포인트 및 하위 호환성 검증 테스트"""

    @classmethod
    def setUpClass(cls):
        cls.fake_factory = FakeMqttFactory()
        cls.e2e_manager = E2EScheduleSessionManager(
            broker_config={"host": "localhost", "port": 1883},
            mqtt_client_factory=cls.fake_factory,
        )
        # 실제 SimulatorManager 계약과 호환되는 spec 기반 mock
        cls.mock_manager = MagicMock(spec=SimulatorManager)
        cls.mock_manager.is_running = False
        cls.mock_manager.get_status.return_value = {"is_running": False, "mode": "idle"}
        cls.shared_lock = threading.Lock()

        cls.handler_cls = create_request_handler(
            manager=cls.mock_manager,
            html_path=None,
            e2e_manager=cls.e2e_manager,
            shared_start_lock=cls.shared_lock,
        )
        cls.httpd = ThreadedHTTPServer(("127.0.0.1", 0), cls.handler_cls)
        cls.port = cls.httpd.server_address[1]
        cls.server_thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.server_thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls.e2e_manager.shutdown(timeout_sec=2.0)

    def setUp(self):
        self.mock_manager.is_running = False
        self.mock_manager.get_status.return_value = {"is_running": False, "mode": "idle"}
        cur = self.e2e_manager.get_current_session()
        if cur is not None and cur.is_active():
            self.e2e_manager.stop_session(cur.run_id)
            for _ in range(50):
                if not self.e2e_manager.is_active():
                    break
                time.sleep(0.02)

    def tearDown(self):
        self.mock_manager.is_running = False
        self.mock_manager.get_status.return_value = {"is_running": False, "mode": "idle"}
        cur = self.e2e_manager.get_current_session()
        if cur is not None and cur.is_active():
            self.e2e_manager.stop_session(cur.run_id)
            for _ in range(50):
                if not self.e2e_manager.is_active():
                    break
                time.sleep(0.02)

    def request(self, method: str, path: str, body: dict | None = None, raw_body: str | None = None) -> tuple[int, dict]:
        """HTTP 요청 전송 및 응답 (status_code, response_json) 반환 헬퍼"""
        url = f"http://127.0.0.1:{self.port}{path}"
        if raw_body is not None:
            data = raw_body.encode("utf-8")
        elif body is not None:
            data = json.dumps(body).encode("utf-8")
        else:
            data = None

        headers = {"Content-Type": "application/json"} if (body is not None or raw_body is not None) else {}
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req) as resp:
                status = resp.status
                content = json.loads(resp.read().decode("utf-8"))
                return status, content
        except urllib.error.HTTPError as err:
            status = err.code
            try:
                content = json.loads(err.read().decode("utf-8"))
            except Exception:
                content = {"raw": err.read().decode("utf-8")}
            return status, content

    def test_viewer_assets_are_served(self):
        """HTML이 참조하는 CSS/JS가 HTTP로 제공되는지 확인한다."""
        base = f"http://127.0.0.1:{self.port}"
        with urllib.request.urlopen(base + "/waveform_viewer.html") as response:
            html = response.read().decode("utf-8")
        asset_urls = re.findall(r'(?:href|src)="(assets/[a-z0-9_.]+)"', html)
        self.assertEqual(len(asset_urls), 13)
        for asset_url in asset_urls:
            with self.subTest(asset=asset_url):
                with urllib.request.urlopen(base + "/" + asset_url) as response:
                    self.assertEqual(response.status, 200)
                    self.assertTrue(response.read())
                    expected_type = "text/css" if asset_url.endswith(".css") else "text/javascript"
                    self.assertIn(expected_type, response.headers["Content-Type"])
        with self.assertRaises(urllib.error.HTTPError) as unknown:
            urllib.request.urlopen(base + "/assets/unknown.js")
        self.assertEqual(unknown.exception.code, 404)

    def test_routine_comparison_demo_is_served_with_local_assets(self):
        """정상/이상 비교 시연 페이지와 전용 로컬 자산을 HTTP로 제공한다."""
        base = f"http://127.0.0.1:{self.port}"
        for page_path in ("/routine-demo", "/routine-demo.html"):
            with self.subTest(path=page_path):
                with urllib.request.urlopen(base + page_path) as response:
                    self.assertEqual(response.status, 200)
                    self.assertIn("text/html", response.headers["Content-Type"])
                    html = response.read().decode("utf-8")
                    self.assertIn("평소보다 오래 켜진 전자레인지", html)
                    self.assertIn('id="normalChart"', html)
                    self.assertIn('id="anomalyChart"', html)

        for asset_path, expected_type in (
            ("/assets/routine_demo.css", "text/css"),
            ("/assets/routine_demo.js", "text/javascript"),
        ):
            with self.subTest(asset=asset_path):
                with urllib.request.urlopen(base + asset_path) as response:
                    self.assertEqual(response.status, 200)
                    self.assertIn(expected_type, response.headers["Content-Type"])
                    self.assertTrue(response.read())

    def test_css_referenced_images_are_served(self):
        """CSS가 url()로 참조하는 이미지도 HTTP로 제공되는지 확인한다.

        href/src 스캔으로는 잡히지 않아 정적 파일 허용 목록에서 누락되기 쉬운 경로다.
        """
        base = f"http://127.0.0.1:{self.port}"
        with urllib.request.urlopen(base + "/assets/waveform.css") as response:
            css = response.read().decode("utf-8")
        image_urls = sorted(set(re.findall(r"url\('([A-Za-z0-9_./]+)'\)", css)))
        self.assertTrue(image_urls, "CSS에서 url() 참조를 찾지 못했다")
        for image_url in image_urls:
            with self.subTest(image=image_url):
                with urllib.request.urlopen(base + "/assets/" + image_url) as response:
                    self.assertEqual(response.status, 200)
                    self.assertTrue(response.read())
                    self.assertIn("image/", response.headers["Content-Type"])

    def test_accelerated_speed_omitted_resolves_to_safe_auto_speed(self):
        """POST /api/e2e/runs: ACCELERATED에서 speed 생략 시 가구 수 기반 안전 배속 자동 산출"""
        # 1. 순수 함수 계약: 합계가 항상 DEFAULT_SAFE_AGGREGATE_RATE로 유지된다
        for count in (1, 2, 3, 10):
            resolved = resolve_auto_speed(count)
            self.assertAlmostEqual(resolved * count, DEFAULT_SAFE_AGGREGATE_RATE, places=9)
        for bad_count in (0, -1, True, 1.5, None):
            with self.assertRaises(ValueError):
                resolve_auto_speed(bad_count)

        # 2. 단일 가구: speed 생략 -> 202, 스냅샷에 해결된 배속이 그대로 노출된다
        status, body = self.request("POST", "/api/e2e/runs", {
            "reference_date": "2026-09-16",
            "execution": {"mode": "ACCELERATED"},
            "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
        })
        self.assertEqual(status, 202)
        run_id = body["run_id"]

        status_s, snap = self.request("GET", f"/api/e2e/runs/{run_id}")
        self.assertEqual(status_s, 200)
        self.assertEqual(snap["execution_mode"], "ACCELERATED")
        self.assertAlmostEqual(snap["speed_multiplier"], DEFAULT_SAFE_AGGREGATE_RATE, places=9)

        self.request("POST", f"/api/e2e/runs/{run_id}/stop")
        for _ in range(50):
            if not self.e2e_manager.is_active():
                break
            time.sleep(0.02)

        # 3. 2가구: 한계는 합계 기준이므로 가구당 배속이 절반이어야 한다
        status2, body2 = self.request("POST", "/api/e2e/runs", {
            "reference_date": "2026-09-16",
            "execution": {"mode": "ACCELERATED"},
            "households": [
                {"household_id": "H001", "scenario": "ACTIVITY_NORMAL"},
                {"household_id": "H002", "scenario": "ACTIVITY_NORMAL"},
            ],
        })
        self.assertEqual(status2, 202)
        run_id2 = body2["run_id"]

        status_s2, snap2 = self.request("GET", f"/api/e2e/runs/{run_id2}")
        self.assertEqual(status_s2, 200)
        self.assertAlmostEqual(
            snap2["speed_multiplier"], DEFAULT_SAFE_AGGREGATE_RATE / 2, places=9
        )
        self.assertAlmostEqual(
            snap2["speed_multiplier"] * 2, DEFAULT_SAFE_AGGREGATE_RATE, places=9
        )

        self.request("POST", f"/api/e2e/runs/{run_id2}/stop")

    def test_explicit_speed_is_not_overridden_by_auto_resolution(self):
        """POST /api/e2e/runs: speed를 명시하면 자동 산출이 개입하지 않는다 (하위 호환)"""
        status, body = self.request("POST", "/api/e2e/runs", {
            "reference_date": "2026-09-16",
            "execution": {"mode": "ACCELERATED", "speed": 12.5},
            "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
        })
        self.assertEqual(status, 202)
        run_id = body["run_id"]

        status_s, snap = self.request("GET", f"/api/e2e/runs/{run_id}")
        self.assertEqual(status_s, 200)
        self.assertEqual(snap["speed_multiplier"], 12.5)

        self.request("POST", f"/api/e2e/runs/{run_id}/stop")

    def test_burst_still_rejects_speed_and_stays_unpaced(self):
        """BURST는 자동 배속 도입 이후에도 speed를 거절하고 무제한으로 유지된다"""
        status, body = self.request("POST", "/api/e2e/runs", {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST", "speed": 700},
            "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
        })
        self.assertEqual(status, 400)
        self.assertIn("speed", body["message"])

        status_ok, body_ok = self.request("POST", "/api/e2e/runs", {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
        })
        self.assertEqual(status_ok, 202)
        run_id = body_ok["run_id"]

        status_s, snap = self.request("GET", f"/api/e2e/runs/{run_id}")
        self.assertEqual(status_s, 200)
        self.assertEqual(snap["execution_mode"], "BURST")
        self.assertIsNone(snap["speed_multiplier"])

        self.request("POST", f"/api/e2e/runs/{run_id}/stop")

    def test_get_scenarios_returns_exact_10_summaries(self):
        """GET /api/e2e/scenarios: 10종 시나리오 요약 반환 확인"""
        status, body = self.request("GET", "/api/e2e/scenarios")
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "success")
        self.assertEqual(body["count"], 10)
        self.assertEqual(len(body["scenarios"]), 10)

        for sc in body["scenarios"]:
            self.assertIn("scenario_id", sc)
            self.assertIn("category", sc)
            self.assertIn("total_days", sc)
            self.assertIn("planned_virtual_slots", sc)
            self.assertIn("planned_publish_samples", sc)
            # 불필요한 필드 배제 확인
            self.assertNotIn("description", sc)
            self.assertNotIn("ai_labels", sc)

    def test_post_runs_strict_input_validation(self):
        """POST /api/e2e/runs: 잘못된 입력에 대해 명확한 400 BAD_REQUEST 반환 검증"""
        # 1. 미허용 top-level 키
        status, body = self.request("POST", "/api/e2e/runs", {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
            "invalid_key": 123,
        })
        self.assertEqual(status, 400)
        self.assertEqual(body["code"], "BAD_REQUEST")
        self.assertIn("허용되지 않은 필드", body["message"])

        # 2. 필수 필드 누락
        status, body = self.request("POST", "/api/e2e/runs", {
            "execution": {"mode": "BURST"},
            "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
        })
        self.assertEqual(status, 400)
        self.assertIn("reference_date", body["message"])

        # 3. 잘못된 reference_date 형식 및 비정규 날짜
        for bad_date in ("2026/09/16", "2026-9-1", "2026-02-30", "2026-13-01", "invalid"):
            status, body = self.request("POST", "/api/e2e/runs", {
                "reference_date": bad_date,
                "execution": {"mode": "BURST"},
                "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
            })
            self.assertEqual(status, 400, f"Expected 400 for bad date '{bad_date}'")

        # 4. timezone 검증 (Asia/Seoul 외 모든 문자열 거절)
        for bad_tz in ("UTC", "KST", "America/New_York", "", 123):
            status, body = self.request("POST", "/api/e2e/runs", {
                "reference_date": "2026-09-16",
                "timezone": bad_tz,
                "execution": {"mode": "BURST"},
                "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
            })
            self.assertEqual(status, 400, f"Expected 400 for bad timezone '{bad_tz}'")

        # 5. 잘못된 execution mode
        status, body = self.request("POST", "/api/e2e/runs", {
            "reference_date": "2026-09-16",
            "execution": {"mode": "INVALID_MODE"},
            "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
        })
        self.assertEqual(status, 400)
        self.assertIn("execution.mode", body["message"])

        # 6. ACCELERATED 모드에서 유효하지 않은 speed (0, 음수, bool, string) 거절
        #    (speed 누락은 400이 아니라 자동 배속으로 해결된다:
        #     test_accelerated_speed_omitted_resolves_to_safe_auto_speed 참조)
        for bad_speed in (0, -5, True, False, "fast"):
            status, body = self.request("POST", "/api/e2e/runs", {
                "reference_date": "2026-09-16",
                "execution": {"mode": "ACCELERATED", "speed": bad_speed},
                "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
            })
            self.assertEqual(status, 400, f"Expected 400 for bad speed '{bad_speed}'")

        # 7. 비표준 JSON 상수(NaN, Infinity, -Infinity, 1e309) 거절
        for const_val in ("NaN", "Infinity", "-Infinity", "1e309"):
            raw = f'{{"reference_date": "2026-09-16", "execution": {{"mode": "ACCELERATED", "speed": {const_val}}}, "households": [{{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}}]}}'
            status, body = self.request("POST", "/api/e2e/runs", raw_body=raw)
            self.assertEqual(status, 400, f"Expected 400 for constant '{const_val}'")

        # 8. BURST 모드에서 speed 지정 금지
        status, body = self.request("POST", "/api/e2e/runs", {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST", "speed": 10.0},
            "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
        })
        self.assertEqual(status, 400)
        self.assertIn("speed", body["message"])

        # 9. 허용 범위 초과 household_id
        status, body = self.request("POST", "/api/e2e/runs", {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [{"household_id": "H099", "scenario": "ACTIVITY_NORMAL"}],
        })
        self.assertEqual(status, 400)
        self.assertIn("household_id", body["message"])

        # 10. 중복 household_id
        status, body = self.request("POST", "/api/e2e/runs", {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [
                {"household_id": "H001", "scenario": "ACTIVITY_NORMAL"},
                {"household_id": "H001", "scenario": "ACTIVITY_NORMAL"},
            ],
        })
        self.assertEqual(status, 400)
        self.assertIn("중복된 household_id", body["message"])

        # 11. 존재하지 않는 시나리오
        status, body = self.request("POST", "/api/e2e/runs", {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [{"household_id": "H001", "scenario": "NONEXISTENT_SCENARIO"}],
        })
        self.assertEqual(status, 400)
        self.assertIn("유효하지 않은 시나리오", body["message"])

    def test_post_runs_mutual_exclusion_with_legacy_and_active_session(self):
        """레거시 실행 중이거나 이미 E2E 활성 세션 존재 시 409 충돌 반환 검증"""
        # 1. 레거시 시뮬레이터 실행 중인 경우
        self.mock_manager.is_running = True
        self.mock_manager.get_status.return_value = {"is_running": True, "mode": "peak"}
        status, body = self.request("POST", "/api/e2e/runs", {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
        })
        self.assertEqual(status, 409)
        self.assertEqual(body["code"], "LEGACY_SIMULATOR_RUNNING")
        self.mock_manager.is_running = False
        self.mock_manager.get_status.return_value = {"is_running": False, "mode": "idle"}

        # 2. 정상 E2E 세션 시작 (202)
        status, body = self.request("POST", "/api/e2e/runs", {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
        })
        self.assertEqual(status, 202, f"Expected 202 but got {status}: {body}")
        self.assertEqual(body["status"], "accepted")
        self.assertEqual(body["state"], "STARTING")
        run_id = body["run_id"]

        # 3. E2E 세션 실행 중 중복 E2E 시작 시도 -> 409
        status2, body2 = self.request("POST", "/api/e2e/runs", {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [{"household_id": "H002", "scenario": "ACTIVITY_NORMAL"}],
        })
        self.assertEqual(status2, 409)
        self.assertEqual(body2["code"], "E2E_SIMULATOR_RUNNING")

        # 4. E2E 세션 실행 중 레거시 /api/start 시작 시도 -> 409
        status3, body3 = self.request("POST", "/api/start", {
            "house": "H001",
            "scenario": "peak",
        })
        self.assertEqual(status3, 409)
        self.assertEqual(body3["code"], "E2E_SIMULATOR_RUNNING")

        # 세션 중단
        self.request("POST", f"/api/e2e/runs/{run_id}/stop")

    def test_get_runs_and_households_snapshots(self):
        """GET /api/e2e/runs/{run_id} 및 GET .../households/{household_id} 정상 및 404 조회"""
        # 세션 시작
        status, body = self.request("POST", "/api/e2e/runs", {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [
                {"household_id": "H001", "scenario": "ACTIVITY_NORMAL"},
                {"household_id": "H002", "scenario": "ROUTINE_CHANGED_LATER"},
            ],
        })
        self.assertEqual(status, 202)
        run_id = body["run_id"]

        time.sleep(0.05)

        # 세션 전체 스냅샷 조회
        status_s, body_s = self.request("GET", f"/api/e2e/runs/{run_id}")
        self.assertEqual(status_s, 200)
        self.assertEqual(body_s["run_id"], run_id)
        self.assertEqual(body_s["reference_date"], "2026-09-16")
        self.assertEqual(body_s["execution_mode"], "BURST")
        self.assertEqual(len(body_s["households"]), 2)

        # 미존재 run_id 조회 -> 404
        status_nf, body_nf = self.request("GET", "/api/e2e/runs/run_nonexistent")
        self.assertEqual(status_nf, 404)
        self.assertEqual(body_nf["code"], "RUN_NOT_FOUND")

        # 개별 가구 H001 스냅샷 조회
        status_h, body_h = self.request("GET", f"/api/e2e/runs/{run_id}/households/H001")
        self.assertEqual(status_h, 200)
        h_info = body_h["household"]
        self.assertEqual(h_info["household_id"], "H001")
        self.assertEqual(h_info["scenario"], "ACTIVITY_NORMAL")
        self.assertIn("settled_virtual_slots", h_info)
        self.assertIn("published_samples", h_info)

        # 미존재 가구 H999 조회 -> 404
        status_h_nf, body_h_nf = self.request("GET", f"/api/e2e/runs/{run_id}/households/H999")
        self.assertEqual(status_h_nf, 404)
        self.assertEqual(body_h_nf["code"], "HOUSEHOLD_NOT_FOUND")

        # 세션 중지
        self.request("POST", f"/api/e2e/runs/{run_id}/stop")

    def test_household_pause_resume_stop_endpoints(self):
        """가구별 pause, resume, stop 제어 및 멱등 200 / 전이 202 검증"""
        status, body = self.request("POST", "/api/e2e/runs", {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [
                {"household_id": "H001", "scenario": "ACTIVITY_NORMAL"},
            ],
        })
        self.assertEqual(status, 202)
        run_id = body["run_id"]

        time.sleep(0.05)

        # 1. 일시정지 요청 (202 또는 200)
        p_status, p_body = self.request("POST", f"/api/e2e/runs/{run_id}/households/H001/pause")
        self.assertIn(p_status, (200, 202))
        self.assertEqual(p_body["requested_action"], "PAUSE")

        # PAUSED 수렴 대기
        for _ in range(50):
            _, snap = self.request("GET", f"/api/e2e/runs/{run_id}/households/H001")
            if snap.get("household", {}).get("state") == "PAUSED":
                break
            time.sleep(0.05)

        # 2. 이미 PAUSED 상태에서 중복 pause -> 200 OK 멱등
        p_status2, p_body2 = self.request("POST", f"/api/e2e/runs/{run_id}/households/H001/pause")
        self.assertEqual(p_status2, 200)
        self.assertEqual(p_body2["status"], "success")
        self.assertEqual(p_body2["current_state"], "PAUSED")

        # 3. 재개 요청 -> 202 Accepted
        r_status, r_body = self.request("POST", f"/api/e2e/runs/{run_id}/households/H001/resume")
        self.assertIn(r_status, (200, 202))
        self.assertEqual(r_body["requested_action"], "RESUME")

        # 4. 개별 가구 stop -> 202 Accepted
        s_status, s_body = self.request("POST", f"/api/e2e/runs/{run_id}/households/H001/stop")
        self.assertIn(s_status, (200, 202))
        self.assertEqual(s_body["requested_action"], "STOP")

        # STOPPED 수렴 대기
        for _ in range(50):
            _, snap = self.request("GET", f"/api/e2e/runs/{run_id}/households/H001")
            if snap.get("household", {}).get("state") == "STOPPED":
                break
            time.sleep(0.05)

    def test_snapshot_timeout_returns_503(self):
        """get_session_snapshot 타임아웃 발생 시 503 E2E_SNAPSHOT_TIMEOUT 응답 검증"""
        with patch.object(self.e2e_manager, "get_session_snapshot", side_effect=E2ESnapshotTimeoutError("Mock timeout")):
            status, body = self.request("GET", "/api/e2e/runs/run_dummy")
            self.assertEqual(status, 503)
            self.assertEqual(body["code"], "E2E_SNAPSHOT_TIMEOUT")

    def test_post_runs_with_real_simulator_manager_instance(self):
        """실제 SimulatorManager() 인스턴스를 주입하여 POST /api/e2e/runs가 202를 반환함을 검증"""
        real_mgr = SimulatorManager()
        handler_cls = create_request_handler(
            manager=real_mgr,
            html_path=None,
            e2e_manager=self.e2e_manager,
            shared_start_lock=threading.Lock(),
        )
        httpd = ThreadedHTTPServer(("127.0.0.1", 0), handler_cls)
        port = httpd.server_address[1]
        t = threading.Thread(target=httpd.serve_forever, daemon=True)
        t.start()
        try:
            url = f"http://127.0.0.1:{port}/api/e2e/runs"
            payload = {
                "reference_date": "2026-09-16",
                "execution": {"mode": "BURST"},
                "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
            }
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
            with urllib.request.urlopen(req) as resp:
                self.assertEqual(resp.status, 202)
                res_body = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(res_body["status"], "accepted")
        finally:
            httpd.shutdown()
            httpd.server_close()

    def test_e2e_manager_unavailable_returns_503(self):
        """e2e_manager=None 주입 시 /api/e2e/* 요청은 503 반환, 레거시 API는 정상 동작 검증"""
        handler_cls = create_request_handler(
            manager=self.mock_manager,
            html_path=None,
            e2e_manager=None,
            shared_start_lock=self.shared_lock,
        )
        httpd = ThreadedHTTPServer(("127.0.0.1", 0), handler_cls)
        port = httpd.server_address[1]
        t = threading.Thread(target=httpd.serve_forever, daemon=True)
        t.start()

        try:
            # 1. /api/e2e/scenarios -> 503
            url_e2e = f"http://127.0.0.1:{port}/api/e2e/scenarios"
            req_e2e = urllib.request.Request(url_e2e, method="GET")
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                urllib.request.urlopen(req_e2e)
            self.assertEqual(ctx.exception.code, 503)
            err_body = json.loads(ctx.exception.read().decode("utf-8"))
            self.assertEqual(err_body["code"], "E2E_MANAGER_UNAVAILABLE")

            # 2. /api/e2e/runs -> 503
            url_runs = f"http://127.0.0.1:{port}/api/e2e/runs"
            req_runs = urllib.request.Request(
                url_runs,
                data=json.dumps({"reference_date": "2026-09-16"}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                urllib.request.urlopen(req_runs)
            self.assertEqual(ctx.exception.code, 503)

            # 3. 레거시 /api/status -> 200 정상 동작
            url_status = f"http://127.0.0.1:{port}/api/status"
            with urllib.request.urlopen(url_status) as resp:
                self.assertEqual(resp.status, 200)
        finally:
            httpd.shutdown()
            httpd.server_close()

    def test_post_runs_start_time_validation(self):
        """POST /api/e2e/runs: start_time 입력 형식 검증 및 스냅샷 보존 테스트"""
        # 1. 유효하지 않은 형식들 -> 400 거절
        invalid_cases = [
            "25:00:00",
            "12:60:00",
            "invalid_time",
            "12:00:99",
            12345,
            True,
        ]
        for bad_st in invalid_cases:
            payload = {
                "reference_date": "2026-09-16",
                "start_time": bad_st,
                "execution": {"mode": "BURST"},
                "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
            }
            status, body = self.request("POST", "/api/e2e/runs", payload)
            self.assertEqual(status, 400, f"Expected 400 for bad start_time '{bad_st}' but got {status}")
            self.assertIn("start_time", body["message"])

        # 2. 유효한 형식들 -> 202 수락 및 snapshot에 start_time 보존
        valid_cases = [
            "09:00:00",
            "09:00",
            "2026-09-16T09:00:00",
            "2026-09-16T09:00:00+09:00",
        ]
        for good_st in valid_cases:
            payload = {
                "reference_date": "2026-09-16",
                "start_time": good_st,
                "execution": {"mode": "BURST"},
                "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
            }
            status, body = self.request("POST", "/api/e2e/runs", payload)
            self.assertEqual(status, 202, f"Expected 202 for valid start_time '{good_st}'")
            run_id = body["run_id"]

            time.sleep(0.05)
            s_status, s_body = self.request("GET", f"/api/e2e/runs/{run_id}")
            self.assertEqual(s_status, 200)
            self.assertEqual(s_body.get("start_time"), good_st)
            if s_body.get("households"):
                self.assertEqual(s_body["households"][0].get("start_time"), good_st)

            self.request("POST", f"/api/e2e/runs/{run_id}/stop")
            for _ in range(50):
                if not self.e2e_manager.is_active():
                    break
                time.sleep(0.02)

    def test_day_results_queryable_in_run_and_household_snapshot(self):
        """GET /api/e2e/runs/{run_id}: 완료된 세션에서 날짜별 결과(day_results)의 5개 필수 필드 및 무결성 검증"""
        status, body = self.request("POST", "/api/e2e/runs", {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
        })
        self.assertEqual(status, 202)
        run_id = body["run_id"]

        # 완료 대기 (BURST 모드 완료 시 즉시 break)
        for _ in range(200):
            _, snap = self.request("GET", f"/api/e2e/runs/{run_id}")
            if snap.get("overall_status") == "COMPLETED":
                break
            time.sleep(0.1)

        # 1. 전체 세션 스냅샷 확인
        status_s, snap_s = self.request("GET", f"/api/e2e/runs/{run_id}")
        self.assertEqual(status_s, 200)
        self.assertEqual(snap_s["overall_status"], "COMPLETED")

        h001 = snap_s["households"][0]
        self.assertEqual(h001["completed_days"], 1)
        self.assertIn("day_results", h001)
        day_results = h001["day_results"]
        self.assertEqual(len(day_results), 1)

        dr = day_results[0]
        self.assertEqual(dr["scenario"], "ACTIVITY_NORMAL")
        self.assertEqual(dr["household_id"], "H001")
        self.assertEqual(dr["activity_date"], "2026-09-16")
        self.assertEqual(dr["published_samples"], 86400)
        self.assertEqual(dr["status"], "COMPLETED")
        self.assertEqual(dr["run_id"], run_id)

        # 2. 개별 가구 스냅샷 확인
        status_h, snap_h = self.request("GET", f"/api/e2e/runs/{run_id}/households/H001")
        self.assertEqual(status_h, 200)
        h_info = snap_h["household"]
        self.assertEqual(h_info["completed_days"], 1)
        self.assertEqual(h_info["day_results"], day_results)
        self.assertEqual(h_info["daily_results"], day_results)

    def test_day_results_stopped_session_omits_incomplete_day(self):
        """중단(STOPPED)된 세션에서 미완료 날짜가 day_results에 COMPLETED로 포함되지 않음 검증"""
        status, body = self.request("POST", "/api/e2e/runs", {
            "reference_date": "2026-09-16",
            "execution": {"mode": "REALTIME"},
            "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
        })
        self.assertEqual(status, 202)
        run_id = body["run_id"]

        time.sleep(0.05)

        # 시작 직후 즉시 중단 (1일 미완료 상태)
        self.request("POST", f"/api/e2e/runs/{run_id}/stop")

        for _ in range(50):
            _, snap = self.request("GET", f"/api/e2e/runs/{run_id}")
            if snap.get("overall_status") in ("STOPPED", "FAILED"):
                break
            time.sleep(0.05)

        status_s, snap_s = self.request("GET", f"/api/e2e/runs/{run_id}")
        self.assertEqual(status_s, 200)
        self.assertIn(snap_s["overall_status"], ("STOPPED", "FAILED"))

        h001 = snap_s["households"][0]
        self.assertEqual(h001["completed_days"], 0)
        self.assertEqual(h001["day_results"], [])
        self.assertEqual(h001["daily_results"], [])

    def test_day_results_idempotency_no_duplicates_on_repeated_queries(self):
        """동일 세션 스냅샷을 반복 재조회해도 day_results 결과가 중복 누적되지 않음 검증"""
        status, body = self.request("POST", "/api/e2e/runs", {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
        })
        run_id = body["run_id"]

        for _ in range(200):
            _, snap = self.request("GET", f"/api/e2e/runs/{run_id}")
            if snap.get("overall_status") == "COMPLETED":
                break
            time.sleep(0.1)

        # 5회 연속 재조회 시에도 day_results 원소 수는 항상 정확히 1개 유지
        for i in range(5):
            _, snap = self.request("GET", f"/api/e2e/runs/{run_id}")
            h001 = snap["households"][0]
            self.assertEqual(len(h001["day_results"]), 1, f"Iteration {i} resulted in duplicates!")
            self.assertEqual(h001["completed_days"], 1)

    def test_schedule_engine_preserves_86400_slots_and_fixed_event_times(self):
        """기존 86,400개 슬롯 및 고정 가전 이벤트 시각 보존 검증"""
        from engine.schedule import compile_schedule
        from engine.unified_catalog import get_unified_scenario_definition
        import datetime as dt

        defn = get_unified_scenario_definition("ACTIVITY_NORMAL")
        plan = compile_schedule(defn, base_date=dt.date(2026, 9, 16))

        # 1. 86,400 슬롯 보존
        self.assertEqual(plan.total_virtual_slots, 86400)
        self.assertEqual(plan.first_cycle, 1)
        self.assertEqual(plan.last_cycle, 86400)
        self.assertEqual(plan.virtual_time_at(1).isoformat(), "2026-09-16T00:00:00+09:00")
        self.assertEqual(plan.virtual_time_at(86400).isoformat(), "2026-09-16T23:59:59+09:00")

        # 2. 고정 이벤트 시각 보존 (07:00=25200s, 09:00=32400s, 12:00=43200s, 15:00=54000s, 18:00=64800s, 21:00=75600s)
        t_25201 = plan.transitions_at(25201)  # 07:00:00 ON (cycle = 1 + 25200)
        self.assertTrue(any(t.appliance == "kettle" and t.transition_type.name == "ON" for t in t_25201))

        t_32401 = plan.transitions_at(32401)  # 09:00:00 ON (cycle = 1 + 32400)
        self.assertTrue(any(t.appliance == "microwave" and t.transition_type.name == "ON" for t in t_32401))


if __name__ == "__main__":
    unittest.main()
