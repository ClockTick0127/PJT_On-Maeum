"""
NILM 스마트홈 시뮬레이터 수동 가전 제어 기능 라이브 통합 검증 테스트

검증 항목:
1. MQTT와 SSE가 동일한 tick 데이터를 공유하는지 검증 (타임스탬프 일치 기반 6대 물리량 비교)
2. 인덕션 전체 duty cycle 검증 (STARTING -> RUNNING 가열 -> RUNNING 휴지 -> RUNNING 재가열 -> OFF)
3. 비수동 모드(peak, random, routine_missed)의 제어 거부 검증 (409 INVALID_MODE)
4. 빠른 재시작 후 단일 워커 검증 (중복 타임스탬프 없음, sec 단조증가, 종료 후 중단)
"""

import os
import sys
import time
import json
import math
import socket
import select
import urllib.request
import urllib.error
import urllib.parse
import http.client
import subprocess
import threading
import unittest
import asyncio
from datetime import datetime

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import aiomqtt

# 상위 디렉터리 import 경로 등록
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SIMULATOR_DIR = os.path.dirname(CURRENT_DIR)
if SIMULATOR_DIR not in sys.path:
    sys.path.insert(0, SIMULATOR_DIR)

from engine.tls import parse_tls_enabled, resolve_mqtt_port, get_mqtt_tls_context

# 테스트 설정
TEST_PORT = 8089
BASE_URL = f"http://127.0.0.1:{TEST_PORT}"
MQTT_TLS_ENABLED = parse_tls_enabled(os.getenv("MQTT_TLS_ENABLED", "false"))
MQTT_PORT = resolve_mqtt_port(tls_enabled=MQTT_TLS_ENABLED)
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_USER = os.getenv("MQTT_USER", "simulator_user")
MQTT_PASS = os.getenv("MQTT_PASS", "test1234")
MQTT_CA_FILE = os.getenv("MQTT_CA_FILE", None)

# 인증정보 마스킹 헬퍼
def mask_credentials(user: str, password: str) -> str:
    masked_pw = "***" if password else ""
    return f"user='{user}', password='{masked_pw}'"


def check_port_open(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, ConnectionRefusedError):
        return False


def safe_stop_and_join(*threads, timeout: float = 5.0):
    """모든 구독 스레드에 stop을 요청하고 순차 join하며 잔존 스레드 경고를 출력한다."""
    for t in threads:
        if t is not None:
            t.stop()
    for t in threads:
        if t is not None:
            t.join(timeout=timeout)
            if t.is_alive():
                print(f" [경고] 스레드 '{t.name}' (Ident: {t.ident})가 {timeout}초 내에 종료되지 않고 여전히 살아있습니다!", flush=True)


class MqttSubscriberThread(threading.Thread):
    """실제 MQTT 브로커로부터 토픽을 수집하는 스레드 (명시적 종료 및 join 지원)"""

    def __init__(self, topic: str = "v1/power/sim/+/main"):
        super().__init__(name="MqttSubscriberThread")
        self.topic = topic
        self.messages = []
        self.lock = threading.Lock()
        self._stop_event = threading.Event()
        self.connected_event = threading.Event()
        self.error = None

    def run(self):
        asyncio.run(self._async_loop())

    async def _async_loop(self):
        try:
            tls_context = get_mqtt_tls_context(tls_enabled=MQTT_TLS_ENABLED, ca_file=MQTT_CA_FILE)
            async with aiomqtt.Client(
                MQTT_HOST,
                MQTT_PORT,
                username=MQTT_USER,
                password=MQTT_PASS,
                tls_context=tls_context
            ) as client:
                await client.subscribe(self.topic)
                self.connected_event.set()
                while not self._stop_event.is_set():
                    try:
                        async with asyncio.timeout(0.5):
                            async for message in client.messages:
                                try:
                                    payload = json.loads(message.payload.decode("utf-8"))
                                    with self.lock:
                                        self.messages.append((time.time(), payload))
                                except Exception:
                                    pass
                                if self._stop_event.is_set():
                                    break
                    except TimeoutError:
                        pass
        except Exception as ex:
            self.error = ex
        finally:
            self.connected_event.set()

    def get_messages(self):
        with self.lock:
            return list(self.messages)

    def clear(self):
        with self.lock:
            self.messages.clear()

    def stop(self):
        self._stop_event.set()


class SseSubscriberThread(threading.Thread):
    """/api/stream SSE 스트림을 수집하는 스레드 (명시적 종료 및 join 지원)"""

    def __init__(self, url: str):
        super().__init__(name="SseSubscriberThread")
        self.url = url
        self.events = []
        self.lock = threading.Lock()
        self._stop_event = threading.Event()
        self.connected_event = threading.Event()
        self._conn = None
        self._sock = None
        self.error = None

    def run(self):
        parsed = urllib.parse.urlparse(self.url)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or 80
        path = parsed.path or "/"

        try:
            self._conn = http.client.HTTPConnection(host, port, timeout=5.0)
            self._conn.connect()
            self._sock = self._conn.sock
            self._conn.request("GET", path)
            resp = self._conn.getresponse()
            self.connected_event.set()

            buffer = ""
            while not self._stop_event.is_set():
                if not self._sock:
                    break
                try:
                    r, _, _ = select.select([self._sock], [], [], 0.5)
                except (ValueError, OSError):
                    break

                if not r:
                    continue

                try:
                    chunk = resp.fp.read1(1024) if hasattr(resp.fp, "read1") else resp.read(1024)
                except (OSError, ConnectionError):
                    break

                if not chunk:
                    break

                buffer += chunk.decode("utf-8", errors="replace")
                while "\n\n" in buffer:
                    part, buffer = buffer.split("\n\n", 1)
                    for line in part.split("\n"):
                        if line.startswith("data: "):
                            raw_data = line[6:].strip()
                            try:
                                payload = json.loads(raw_data)
                                with self.lock:
                                    self.events.append((time.time(), payload))
                            except Exception:
                                pass
        except (OSError, ConnectionResetError, ConnectionAbortedError, http.client.HTTPException):
            pass
        except Exception as ex:
            self.error = ex
        finally:
            self.connected_event.set()
            if self._conn:
                try:
                    self._conn.close()
                except Exception:
                    pass

    def get_events(self):
        with self.lock:
            return list(self.events)

    def clear(self):
        with self.lock:
            self.events.clear()

    def stop(self):
        self._stop_event.set()
        # 소켓 shutdown으로 select/read 즉각 해제
        if self._sock:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                self._sock.close()
            except Exception:
                pass
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass


class TestManualLiveIntegration(unittest.TestCase):
    server_process = None
    server_log_file = None
    watchdog_timer = None
    current_running_test = None

    @classmethod
    def setUpClass(cls):
        tls_info = f", TLS ON (CA: {MQTT_CA_FILE})" if MQTT_TLS_ENABLED else ", TLS OFF"
        print(f"\n============================================================")
        print(f" [{datetime.now().strftime('%H:%M:%S')}] [통합 검증] 수동 가전 제어 최종 보강 검증 시작")
        print(f" - 대상 서버 포트: {TEST_PORT}")
        print(f" - MQTT 브로커   : {MQTT_HOST}:{MQTT_PORT} ({mask_credentials(MQTT_USER, MQTT_PASS)}{tls_info})")
        print(f" - 정상 예상 시간: 약 95초 (최대 제한 시간: 180초)")
        print(f"============================================================", flush=True)

        # 0. 전체 테스트 180초(3분) 워치독 타이머 가동
        def watchdog_action():
            print(f"\n\n{'!'*60}", flush=True)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [FATAL TIMEOUT] 전체 테스트 제한 시간(180초) 초과!", flush=True)
            print(f" - 타임아웃 시점 실행 중인 테스트: {cls.current_running_test}", flush=True)
            print(f" - 현재 활성 스레드 목록:", flush=True)
            for th in threading.enumerate():
                print(f"   * Thread '{th.name}' (ident={th.ident}, daemon={th.daemon}, alive={th.is_alive()})", flush=True)
            print(f"{'!'*60}\n", flush=True)
            if cls.server_process:
                try:
                    cls.server_process.kill()
                except Exception:
                    pass
            os._exit(1)

        cls.watchdog_timer = threading.Timer(180.0, watchdog_action)
        cls.watchdog_timer.daemon = True
        cls.watchdog_timer.start()

        # 1. Mosquitto 브로커 활성 상태 확인 (기존 서비스 유지, 재시작 금지)
        if not check_port_open(MQTT_HOST, MQTT_PORT, timeout=3.0):
            raise unittest.SkipTest(f"MQTT 브로커({MQTT_HOST}:{MQTT_PORT})에 접속할 수 없습니다. 서비스가 가동 중인지 확인하세요.")

        # 2. web_server.py 실행 경로 탐색
        current_dir = os.path.dirname(os.path.abspath(__file__))
        simulator_dir = os.path.dirname(current_dir)
        web_server_py = os.path.join(simulator_dir, "web_server.py")
        if not os.path.isfile(web_server_py):
            raise FileNotFoundError(f"web_server.py를 찾을 수 없습니다: {web_server_py}")

        # 3. web_server.py 서브프로세스 시작 (파이프 버퍼 포화 방지를 위해 테스트 로그 파일로 연결)
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        env["BROWSER"] = "none"  # 자동 브라우저 팝업 방지

        log_path = os.path.join(simulator_dir, "tests", "test_server_output.log")
        cls.server_log_file = open(log_path, "w", encoding="utf-8")

        cls.server_process = subprocess.Popen(
            [sys.executable, web_server_py, str(TEST_PORT)],
            cwd=simulator_dir,
            env=env,
            stdout=cls.server_log_file,
            stderr=subprocess.STDOUT,
            text=True
        )

        # 4. 서버 기동 대기 (최대 10초)
        start_wait = time.time()
        server_ready = False
        while time.time() - start_wait < 10.0:
            if check_port_open("127.0.0.1", TEST_PORT, timeout=0.5):
                try:
                    req = urllib.request.Request(f"{BASE_URL}/api/status")
                    with urllib.request.urlopen(req, timeout=1.0) as resp:
                        if resp.status == 200:
                            server_ready = True
                            break
                except Exception:
                    pass
            time.sleep(0.3)

        if not server_ready:
            cls.tearDownClass()
            raise RuntimeError(f"테스트 웹 서버(포트 {TEST_PORT}) 기동에 실패했습니다.")

        print(f" -> 테스트 웹 서버(PID: {cls.server_process.pid}, Port: {TEST_PORT}) 정상 가동 확인", flush=True)

    @classmethod
    def tearDownClass(cls):
        # 0. 워치독 타이머 취소
        if cls.watchdog_timer:
            cls.watchdog_timer.cancel()
            cls.watchdog_timer = None

        # 1. 실행 중인 시뮬레이터 중지
        try:
            req = urllib.request.Request(f"{BASE_URL}/api/stop", data=b"{}", method="POST")
            req.add_header("Content-Type", "application/json")
            with urllib.request.urlopen(req, timeout=2.0):
                pass
        except Exception:
            pass

        # 2. 테스트가 기동한 web_server.py 프로세스 종료
        if cls.server_process:
            print(f" -> 테스트 웹 서버 프로세스(PID: {cls.server_process.pid}) 종료 중...", flush=True)
            try:
                cls.server_process.terminate()
                cls.server_process.wait(timeout=5.0)
            except Exception:
                try:
                    cls.server_process.kill()
                    cls.server_process.wait(timeout=2.0)
                except Exception:
                    pass
            cls.server_process = None

        # 3. 로그 파일 핸들 닫기
        if cls.server_log_file:
            try:
                cls.server_log_file.close()
            except Exception:
                pass
            cls.server_log_file = None

        # 4. 테스트 종료 후 사용한 포트가 LISTEN 상태가 아닌지 확인
        time.sleep(1.0)
        is_port_still_open = check_port_open("127.0.0.1", TEST_PORT, timeout=1.0)
        if is_port_still_open:
            print(f" [경고] 테스트 포트 {TEST_PORT}가 아직 LISTEN 상태로 남아 있습니다.", flush=True)
        else:
            print(f" -> 테스트 포트 {TEST_PORT} 정상 해제 확인 (LISTEN 아님)", flush=True)

    def setUp(self):
        TestManualLiveIntegration.current_running_test = self._testMethodName
        self._test_start_time = time.time()
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] >>> {self._testMethodName} 시작", flush=True)

    def tearDown(self):
        elapsed = time.time() - getattr(self, "_test_start_time", time.time())
        print(f"[{datetime.now().strftime('%H:%M:%S')}] <<< {self._testMethodName} 종료 (소요: {elapsed:.2f}초)", flush=True)

    def http_post(self, path: str, data: dict = None) -> tuple[int, dict]:
        url = f"{BASE_URL}{path}"
        body = json.dumps(data).encode("utf-8") if data is not None else b"{}"
        req = urllib.request.Request(url, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode("utf-8"))

    def http_put(self, path: str, data: dict) -> tuple[int, dict]:
        url = f"{BASE_URL}{path}"
        body = json.dumps(data).encode("utf-8")
        req = urllib.request.Request(url, data=body, method="PUT")
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode("utf-8"))

    def http_get(self, path: str) -> tuple[int, dict]:
        url = f"{BASE_URL}{path}"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))

    def test_01_mqtt_and_sse_share_identical_tick_data(self):
        """
        검증 1. MQTT와 SSE가 동일한 tick 데이터를 공유하는지 검증
        - manual 모드로 H001 시작
        - MQTT measured_at == SSE now_iso 정확 일치 메시지만 매칭
        - 6대 전력 물리량 완전 비교
        - 최소 5개 이상의 동일 tick 연속 검증
        """
        print("\n--- [검증 1] MQTT와 SSE 동일 tick 데이터 동기화 검증 시작 ---", flush=True)
        mqtt_sub = MqttSubscriberThread(topic="v1/power/sim/+/main")
        sse_sub = SseSubscriberThread(url=f"{BASE_URL}/api/stream")

        try:
            mqtt_sub.start()
            sse_sub.start()
            self.assertTrue(mqtt_sub.connected_event.wait(timeout=5.0), "MQTT 구독자 연결 실패")
            self.assertTrue(sse_sub.connected_event.wait(timeout=5.0), "SSE 구독자 연결 실패")

            # manual 모드로 H001 시작
            status, res = self.http_post("/api/start", {"scenario": "manual", "house": "H001"})
            self.assertEqual(status, 200, f"/api/start 실패: {res}")
            self.assertEqual(res.get("status"), "started")

            # 최대 30초 동안 수집하며, SSE sec 기준 정확히 연속된 5개 tick 매칭 즉시 성공 종료
            matched_dict = {}
            matched_consecutive = []
            start_obs = time.time()
            last_report = start_obs

            while time.time() - start_obs < 30.0:
                time.sleep(0.5)
                elapsed = time.time() - start_obs

                mqtt_msgs = mqtt_sub.get_messages()
                sse_evts = sse_sub.get_events()

                if time.time() - last_report >= 5.0:
                    last_report = time.time()
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] [test_01 진행중] 경과: {elapsed:.1f}s/30s | 수집 MQTT: {len(mqtt_msgs)}개, SSE: {len(sse_evts)}개", flush=True)

                # H001 대상 MQTT 메시지만 맵 구성 (measured_at -> payload)
                mqtt_by_ts = {}
                for _, m in mqtt_msgs:
                    if m.get("household_id") == "H001" or m.get("house") == "H001":
                        ts = m.get("measured_at") or m.get("ts")
                        if ts:
                            mqtt_by_ts[ts] = m

                # SSE 이벤트와 MQTT 매칭 (6대 물리량 비교 및 즉시 assertion 검증)
                for _, sse in sse_evts:
                    if sse.get("house") != "H001":
                        continue
                    now_iso = sse.get("now_iso")
                    sec = sse.get("sec")
                    if not now_iso or sec is None:
                        continue

                    if now_iso in mqtt_by_ts:
                        if sec in matched_dict:
                            continue
                        mqtt = mqtt_by_ts[now_iso]

                        # 필수 필드 추출 및 float 변환 검증 (누락 또는 변환 실패 시 즉시 fail)
                        try:
                            p_mqtt = float(mqtt["active_power"])
                            q_mqtt = float(mqtt["reactive_power"])
                            s_mqtt = float(mqtt["apparent_power"])
                            pf_mqtt = float(mqtt["power_factor"])
                            v_mqtt = float(mqtt["voltage"])
                            i_mqtt = float(mqtt["current"])
                        except Exception as e:
                            self.fail(f"MQTT 페이로드 필수 필드 누락 또는 숫자 변환 실패 at {now_iso}: {e} (MQTT={mqtt})")

                        try:
                            p_sse = float(sse["totalP"])
                            q_sse = float(sse["totalQ"])
                            s_sse = float(sse["apparentS"])
                            pf_sse = float(sse["pf"])
                            v_sse = float(sse["voltage"])
                            i_sse = float(sse["currentA"])
                        except Exception as e:
                            self.fail(f"SSE 이벤트 필수 필드 누락 또는 숫자 변환 실패 at {now_iso}: {e} (SSE={sse})")

                        # 6대 물리량 즉시 검증 (rel_tol=0.0 명시, 기존 abs_tol 유지, 불일치 시 타임스탬프와 양쪽 값 출력 후 즉시 실패)
                        self.assertTrue(
                            math.isclose(p_mqtt, p_sse, rel_tol=0.0, abs_tol=0.01),
                            f"Active power mismatch at {now_iso}: MQTT={p_mqtt}W != SSE={p_sse}W"
                        )
                        self.assertTrue(
                            math.isclose(q_mqtt, q_sse, rel_tol=0.0, abs_tol=0.01),
                            f"Reactive power mismatch at {now_iso}: MQTT={q_mqtt}var != SSE={q_sse}var"
                        )
                        self.assertTrue(
                            math.isclose(s_mqtt, s_sse, rel_tol=0.0, abs_tol=0.01),
                            f"Apparent power mismatch at {now_iso}: MQTT={s_mqtt}VA != SSE={s_sse}VA"
                        )
                        self.assertTrue(
                            math.isclose(pf_mqtt, pf_sse, rel_tol=0.0, abs_tol=0.005),
                            f"Power factor mismatch at {now_iso}: MQTT={pf_mqtt} != SSE={pf_sse}"
                        )
                        self.assertTrue(
                            math.isclose(v_mqtt, v_sse, rel_tol=0.0, abs_tol=0.01),
                            f"Voltage mismatch at {now_iso}: MQTT={v_mqtt}V != SSE={v_sse}V"
                        )
                        self.assertTrue(
                            math.isclose(i_mqtt, i_sse, rel_tol=0.0, abs_tol=0.01),
                            f"Current mismatch at {now_iso}: MQTT={i_mqtt}A != SSE={i_sse}A"
                        )

                        # 모든 assertion을 통과한 tick만 matched_dict에 추가
                        matched_dict[sec] = {
                            "sec": sec,
                            "ts": now_iso,
                            "mqtt": {
                                "P": p_mqtt, "Q": q_mqtt, "S": s_mqtt,
                                "PF": pf_mqtt, "V": v_mqtt, "I": i_mqtt
                            },
                            "sse": {
                                "P": p_sse, "Q": q_sse, "S": s_sse,
                                "PF": pf_sse, "V": v_sse, "I": i_sse
                            }
                        }

                # 매칭된 데이터를 sec 오름차순으로 정렬
                sorted_ticks = [matched_dict[s] for s in sorted(matched_dict.keys())]

                # sec 값이 정확히 +1씩 증가하는 연속 구간 탐색
                current_streak = []
                best_streak = []
                for item in sorted_ticks:
                    if not current_streak:
                        current_streak = [item]
                    elif item["sec"] == current_streak[-1]["sec"] + 1:
                        current_streak.append(item)
                    elif item["sec"] == current_streak[-1]["sec"]:
                        pass
                    else:
                        if len(current_streak) > len(best_streak):
                            best_streak = current_streak
                        current_streak = [item]
                if len(current_streak) > len(best_streak):
                    best_streak = current_streak

                if len(best_streak) >= 5:
                    matched_consecutive = best_streak[:5]
                    print(f" -> 연속 5개 tick 매칭 조기 달성! (경과: {elapsed:.1f}s, sec: {[x['sec'] for x in matched_consecutive]})", flush=True)
                    break

            mqtt_msgs = mqtt_sub.get_messages()
            sse_evts = sse_sub.get_events()
            print(f" -> 수집 최종 완료: MQTT {len(mqtt_msgs)}개, SSE {len(sse_evts)}개", flush=True)

            self.assertGreaterEqual(
                len(matched_consecutive), 5,
                f"SSE sec 기준 정확히 연속된 5개 tick(sec+1) 매칭 조건을 만족하지 못했습니다 ({len(matched_consecutive)}개)."
            )

            print(f" -> 타임스탬프 일치 tick 검증 완료: 연속 5개 tick 완전 일치 (sec: {[x['sec'] for x in matched_consecutive]})", flush=True)
            for sample in matched_consecutive:
                m = sample["mqtt"]
                s = sample["sse"]
                print(f"    [Tick #{sample['sec']} @ {sample['ts']}]", flush=True)
                print(f"    - active_power  : MQTT={m['P']:.2f}W, SSE={s['P']:.2f}W", flush=True)
                print(f"    - reactive_power: MQTT={m['Q']:.2f}var, SSE={s['Q']:.2f}var", flush=True)
                print(f"    - apparent_power: MQTT={m['S']:.2f}VA, SSE={s['S']:.2f}VA", flush=True)
                print(f"    - power_factor  : MQTT={m['PF']:.4f}, SSE={s['PF']:.4f}", flush=True)
                print(f"    - voltage       : MQTT={m['V']:.2f}V, SSE={s['V']:.2f}V", flush=True)
                print(f"    - current       : MQTT={m['I']:.2f}A, SSE={s['I']:.2f}A", flush=True)

        finally:
            self.http_post("/api/stop")
            safe_stop_and_join(mqtt_sub, sse_sub, timeout=5.0)
            self.assertFalse(mqtt_sub.is_alive(), "MQTT 구독자 스레드가 정상 종료되지 않았습니다.")
            self.assertFalse(sse_sub.is_alive(), "SSE 구독자 스레드가 정상 종료되지 않았습니다.")

    def test_02_induction_full_duty_cycle_and_transitions(self):
        """
        검증 2. 인덕션 전체 duty cycle 검증
        - manual 모드에서 induction ON
        - 최소 50초~65초 관찰
        - STARTING -> RUNNING 고출력 -> RUNNING 휴지 -> RUNNING 고출력 재진입 확인
        - STARTING/RUNNING 순서 assertion
        - 휴지 구간에도 state=="RUNNING", enabled=True, manualHold=True 유지 확인
        - 중복 ON 요청 시 파라미터 초기화 없음 확인
        - OFF 전환 및 상태 확인
        """
        print("\n--- [검증 2] 인덕션 서모스탯 듀티 사이클 및 상태 전이 검증 시작 ---", flush=True)
        sse_sub = SseSubscriberThread(url=f"{BASE_URL}/api/stream")

        try:
            sse_sub.start()
            self.assertTrue(sse_sub.connected_event.wait(timeout=5.0), "SSE 연결 실패")

            # 1. manual 모드 시작
            st, res = self.http_post("/api/start", {"scenario": "manual", "house": "H001"})
            self.assertEqual(st, 200)
            time.sleep(1.0)

            # 2. induction ON 요청
            st, put_res = self.http_put("/api/device", {"house": "H001", "device": "induction", "enabled": True})
            self.assertEqual(st, 200)
            self.assertEqual(put_res.get("enabled"), True)
            initial_snapshot = put_res.get("device_state", {})
            self.assertEqual(initial_snapshot.get("state"), "STARTING")
            self.assertTrue(initial_snapshot.get("manual_hold"))
            initial_nominal_w = initial_snapshot.get("nominal_w")

            print(f" -> 인덕션 ON 요청 성공: state={initial_snapshot.get('state')}, nominal_w={initial_nominal_w:.1f}W", flush=True)

            # 3. 55초 동안 실시간 SSE 관찰 (인덕션 주기: ON 14~22초, OFF 6~14초)
            observed_states = []
            observed_powers = []
            heating_powers = []
            pause_powers = []
            pause_checks = []
            re_heating_observed = False

            phase = "WAIT_HEATING"
            start_obs_time = time.time()
            last_progress_report = start_obs_time
            last_event_count = 0

            while time.time() - start_obs_time < 58.0:
                events = sse_sub.get_events()

                if time.time() - last_progress_report >= 5.0:
                    last_progress_report = time.time()
                    elapsed_obs = time.time() - start_obs_time
                    recent_states = observed_states[-3:] if observed_states else ["NONE"]
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] [test_02 진행중] 경과: {elapsed_obs:.1f}s/58s | phase: {phase} | 수집 SSE: {len(events)}개 | 최근상태: {recent_states}", flush=True)

                if len(events) > last_event_count:
                    for _, ev in events[last_event_count:]:
                        dev_info = ev.get("devices", {}).get("induction", {})
                        st_name = dev_info.get("state")
                        enabled = dev_info.get("enabled")
                        manual_hold = dev_info.get("manualHold")
                        total_p = ev.get("totalP", 0.0)

                        if st_name:
                            observed_states.append(st_name)
                            observed_powers.append(total_p)

                        # 상태 전이 및 전력 구간 추적
                        if phase == "WAIT_HEATING":
                            if st_name == "STARTING":
                                print(f"    [T+{len(observed_states):02d}s] STARTING 관측 (전력: {total_p:.1f}W)", flush=True)
                            elif st_name == "RUNNING" and total_p > 1200.0:
                                heating_powers.append(total_p)
                                if len(heating_powers) >= 3:
                                    phase = "WAIT_PAUSE"
                                    print(f"    [T+{len(observed_states):02d}s] 1. RUNNING 고출력 가열 구간 진입 (전력: {total_p:.1f}W)", flush=True)
                        elif phase == "WAIT_PAUSE":
                            if st_name == "RUNNING":
                                # 인덕션 가열 휴지 구간: 인덕션 자체는 8~18W, 기저부하 합산 시 < 250W
                                if total_p < 250.0:
                                    pause_powers.append(total_p)
                                    pause_checks.append((st_name, enabled, manual_hold))
                                    if len(pause_powers) >= 2:
                                        phase = "WAIT_REHEAT"
                                        print(f"    [T+{len(observed_states):02d}s] 2. RUNNING 서모스탯 휴지 구간 진입 (전력: {total_p:.1f}W, 상태={st_name}, enabled={enabled}, manualHold={manual_hold})", flush=True)
                                else:
                                    heating_powers.append(total_p)
                        elif phase == "WAIT_REHEAT":
                            if st_name == "RUNNING" and total_p > 1200.0:
                                re_heating_observed = True
                                print(f"    [T+{len(observed_states):02d}s] 3. RUNNING 고출력 재진입 관측 (전력: {total_p:.1f}W)", flush=True)
                                break

                    last_event_count = len(events)
                    if re_heating_observed:
                        break
                time.sleep(0.5)

            # 요구사항 단언:
            # 1) 상태 전환 순서: PUT 응답 STARTING -> 이후 SSE RUNNING 검증 (inrush_sec=1 고려)
            self.assertEqual(initial_snapshot.get("state"), "STARTING", "PUT 응답에서 STARTING 상태가 확인되지 않았습니다.")
            self.assertIn("RUNNING", observed_states, "SSE 스트림에서 RUNNING 상태가 관측되지 않았습니다.")
            print(f" -> 상태 전이 검증 성공: PUT 응답(STARTING) -> SSE 스트림(RUNNING)", flush=True)

            # 2) 고출력 가열 구간 확인 (>1200W)
            self.assertGreaterEqual(len(heating_powers), 3, f"고출력 가열 구간이 충분히 관측되지 않았습니다: {len(heating_powers)}회")
            print(f" -> 고출력 가열 구간 검증 성공: {len(heating_powers)}회 관측 (평균: {sum(heating_powers)/len(heating_powers):.1f}W)", flush=True)

            # 3) 휴지 구간 확인 (<250W) 및 상태 유지 확인
            self.assertGreaterEqual(len(pause_powers), 2, f"서모스탯 휴지 구간이 관측되지 않았습니다: {len(pause_powers)}회")
            for st_val, en_val, mh_val in pause_checks:
                self.assertEqual(st_val, "RUNNING", f"휴지 구간 중 state가 RUNNING이 아닙니다: {st_val}")
                self.assertTrue(en_val, f"휴지 구간 중 enabled가 true가 아닙니다: {en_val}")
                self.assertTrue(mh_val, f"휴지 구간 중 manualHold가 true가 아닙니다: {mh_val}")
            print(f" -> 서모스탯 휴지 구간 검증 성공: {len(pause_powers)}회 관측 (평균: {sum(pause_powers)/len(pause_powers):.1f}W, state=RUNNING, enabled=True, manualHold=True 유지)", flush=True)

            # 4) 재가열 구간 진입 확인
            self.assertTrue(re_heating_observed, "휴지 구간 후 고출력 재가열 진입이 관측되지 않았습니다.")
            print(f" -> 고출력 재가열 진입 검증 성공", flush=True)

            # 4. 중복 ON 요청 검증 (파라미터 불필요 초기화 없음)
            print(" -> 가동 중인 인덕션에 동일 ON 요청 전송 (멱등성 검증)...", flush=True)
            st_dup, dup_res = self.http_put("/api/device", {"house": "H001", "device": "induction", "enabled": True})
            self.assertEqual(st_dup, 200)
            dup_state = dup_res.get("device_state", {})
            self.assertEqual(dup_state.get("state"), "RUNNING", "중복 ON 후 state가 RUNNING이 아닙니다.")
            self.assertTrue(dup_state.get("manual_hold"), "중복 ON 후 manual_hold가 True가 아닙니다.")
            self.assertEqual(dup_state.get("nominal_w"), initial_nominal_w, "중복 ON 후 nominal_w가 재설정되었습니다.")

            # 5. induction OFF 요청 및 확인
            print(" -> 인덕션 OFF 요청 전송...", flush=True)
            st_off, off_res = self.http_put("/api/device", {"house": "H001", "device": "induction", "enabled": False})
            self.assertEqual(st_off, 200)

            # 다음 SSE에서 OFF 확정 검증
            time.sleep(1.5)
            latest_events = sse_sub.get_events()
            self.assertGreater(len(latest_events), 0)
            latest_ind = latest_events[-1][1].get("devices", {}).get("induction", {})
            self.assertEqual(latest_ind.get("state"), "OFF", f"OFF 요청 후 state가 OFF가 아닙니다: {latest_ind}")
            self.assertFalse(latest_ind.get("enabled"), f"OFF 요청 후 enabled가 false가 아닙니다: {latest_ind}")
            self.assertFalse(latest_ind.get("manualHold"), f"OFF 요청 후 manualHold가 false가 아닙니다: {latest_ind}")
            print(" -> 인덕션 OFF 상태 확정 확인 완료", flush=True)

        finally:
            self.http_post("/api/stop")
            safe_stop_and_join(sse_sub, timeout=5.0)
            self.assertFalse(sse_sub.is_alive(), "SSE 구독자 스레드가 정상 종료되지 않았습니다.")

    def test_03_non_manual_modes_reject_device_control(self):
        """
        검증 3. 비수동 모드(peak, random, routine_missed)의 제어 거부 검증
        - 각 모드별로 개별 시뮬레이션 시작
        - PUT /api/device 요청 시 HTTP 409 확인
        - error code가 INVALID_MODE인지 확인
        - 가전 상태가 바뀌지 않았는지 전후 확인
        - 각 모드 종료 확인
        """
        print("\n--- [검증 3] 비수동 모드 제어 거부(409 INVALID_MODE) 검증 시작 ---", flush=True)
        modes_to_test = ["peak", "random", "routine_missed"]

        for mode in modes_to_test:
            with self.subTest(mode=mode):
                print(f"[{datetime.now().strftime('%H:%M:%S')}] -> [{mode} 모드] 시작 및 제어 거절 검증...", flush=True)
                try:
                    # 1. 시뮬레이터 시작
                    st, start_res = self.http_post("/api/start", {"scenario": mode, "house": "H001"})
                    self.assertEqual(st, 200, f"{mode} 모드 시작 실패: {start_res}")
                    time.sleep(0.5)

                    # 2. 상태 확인
                    st, status_res = self.http_get("/api/status")
                    self.assertEqual(st, 200)
                    self.assertEqual(status_res.get("current_mode"), mode)
                    self.assertTrue(status_res.get("is_running"))

                    # 3. PUT /api/device 제어 시도 (kettle ON)
                    st_put, put_res = self.http_put("/api/device", {"house": "H001", "device": "kettle", "enabled": True})
                    print(f"    PUT 응답: HTTP {st_put}, Body={put_res}", flush=True)

                    # 4. 검증: HTTP 409 및 INVALID_MODE
                    self.assertEqual(st_put, 409, f"{mode} 모드에서 409 대신 {st_put} 반환")
                    self.assertEqual(put_res.get("code"), "INVALID_MODE", f"error code가 INVALID_MODE가 아닙니다: {put_res}")
                    self.assertIn("manual", put_res.get("message", "").lower())

                    # 5. 상태 변경 없음 확인
                    st_after, status_after = self.http_get("/api/status")
                    self.assertEqual(st_after, 200)
                    last_m = status_after.get("last_metrics")
                    if last_m and "devices" in last_m:
                        kettle_st = last_m["devices"].get("kettle", {})
                        # peak 모드 초기 1초 시점에는 kettle이 OFF여야 함
                        if mode == "routine_missed":
                            self.assertEqual(kettle_st.get("state"), "OFF")
                            self.assertFalse(kettle_st.get("enabled"))

                finally:
                    # 6. 각 모드 종료
                    st_stop, stop_res = self.http_post("/api/stop")
                    self.assertEqual(st_stop, 200, f"{mode} 모드 정지 실패: {stop_res}")
                    time.sleep(0.5)
                    print(f"    [{mode} 모드] 검증 완료 및 정상 정지", flush=True)

    def test_04_rapid_restarts_single_worker_verification(self):
        """
        검증 4. 빠른 재시작 후 단일 워커 검증
        - manual 모드 시작과 stop/start를 빠르게 4회 반복
        - 마지막 start 이후 최소 16초 동안 MQTT/SSE 수집
        - measured_at 중복 없음 검증
        - now_iso 중복 없음 검증
        - sec가 1씩 단조 증가함 검증
        - 정상적인 1초 tick 기준 2배 발행 패턴 없음 검증
        - stop 완료 후 추가 데이터 발생 중단 검증
        """
        print("\n--- [검증 4] 빠른 재시작 후 단일 워커 검증 시작 ---", flush=True)
        mqtt_sub = MqttSubscriberThread(topic="v1/power/sim/+/main")
        sse_sub = SseSubscriberThread(url=f"{BASE_URL}/api/stream")

        try:
            mqtt_sub.start()
            sse_sub.start()
            self.assertTrue(mqtt_sub.connected_event.wait(timeout=5.0))
            self.assertTrue(sse_sub.connected_event.wait(timeout=5.0))

            # 1. 빠른 start / stop / start 반복 (4회)
            print(" -> 급속 start/stop/start 반복 실행 (4회)...", flush=True)
            for i in range(4):
                self.http_post("/api/start", {"scenario": "manual", "house": "H001"})
                time.sleep(0.15)
                self.http_post("/api/stop")
                time.sleep(0.15)

            # 마지막 확정 start
            st, res = self.http_post("/api/start", {"scenario": "manual", "house": "H001"})
            self.assertEqual(st, 200)
            print(" -> 최종 start 호출 완료, 16.5초간 스트림 수집...", flush=True)

            mqtt_sub.clear()
            sse_sub.clear()

            # 2. 최소 15초 이상 (16.5초) 관찰 (5초 간격 진행 출력)
            start_obs = time.time()
            last_obs_report = start_obs
            while time.time() - start_obs < 16.5:
                time.sleep(0.5)
                if time.time() - last_obs_report >= 5.0:
                    last_obs_report = time.time()
                    elapsed_obs = time.time() - start_obs
                    cur_mqtt = len([m[1] for m in mqtt_sub.get_messages() if m[1].get("household_id") == "H001" or m[1].get("house") == "H001"])
                    cur_sse = len([e[1] for e in sse_sub.get_events() if e[1].get("house") == "H001"])
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] [test_04 진행중] 경과: {elapsed_obs:.1f}s/16.5s | 수집 MQTT: {cur_mqtt}개, SSE: {cur_sse}개", flush=True)

            mqtt_msgs = [m[1] for m in mqtt_sub.get_messages() if m[1].get("household_id") == "H001" or m[1].get("house") == "H001"]
            sse_evts = [e[1] for e in sse_sub.get_events() if e[1].get("house") == "H001"]

            print(f" -> 관찰 결과: MQTT 수집={len(mqtt_msgs)}개, SSE 수집={len(sse_evts)}개", flush=True)
            self.assertGreaterEqual(len(mqtt_msgs), 14, f"16초간 수집된 MQTT 메시지 부족: {len(mqtt_msgs)}")
            self.assertGreaterEqual(len(sse_evts), 14, f"16초간 수집된 SSE 이벤트 부족: {len(sse_evts)}")

            # 3. MQTT 중복 measured_at 검증
            mqtt_ts_list = [m.get("measured_at") for m in mqtt_msgs if m.get("measured_at")]
            self.assertEqual(len(mqtt_ts_list), len(set(mqtt_ts_list)), "MQTT 메시지에 중복된 measured_at이 존재합니다 (워커 중복 실행 의심).")

            # 4. SSE 중복 now_iso 검증
            sse_ts_list = [e.get("now_iso") for e in sse_evts if e.get("now_iso")]
            self.assertEqual(len(sse_ts_list), len(set(sse_ts_list)), "SSE 이벤트에 중복된 now_iso가 존재합니다 (워커 중복 실행 의심).")

            # 5. SSE sec 단조 증가 검증 (sec[i] == sec[i-1] + 1)
            sec_list = [e.get("sec") for e in sse_evts if "sec" in e]
            for i in range(1, len(sec_list)):
                self.assertEqual(sec_list[i], sec_list[i - 1] + 1, f"SSE sec가 단조 증가하지 않습니다: sec[{i-1}]={sec_list[i-1]}, sec[{i}]={sec_list[i]}")

            # 6. 비정상적인 2배 발행 패턴 검증 (16초 관찰 시 30개 이상 수집되면 실패)
            self.assertLessEqual(len(mqtt_msgs), 20, f"비정상적인 2배 발행 속도 감지: 16초간 {len(mqtt_msgs)}개 수집")
            self.assertLessEqual(len(sse_evts), 20, f"비정상적인 2배 발행 속도 감지: 16초간 {len(sse_evts)}개 수집")
            print(" -> 단일 워커 1초 주기 발행 검증 통과 (중복 타임스탬프 0건, sec 단조증가)", flush=True)

            # 7. stop 완료 후 새로운 데이터 발생 중단 검증
            print(" -> stop 호출 후 스트림 중단 검증...", flush=True)
            st_stop, _ = self.http_post("/api/stop")
            self.assertEqual(st_stop, 200)
            time.sleep(1.5)

            # 큐 초기화 후 3초간 추가 수집 여부 확인
            mqtt_sub.clear()
            sse_sub.clear()
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [test_04 stop 대기] 3초간 잔여 데이터 관찰 중...", flush=True)
            time.sleep(3.0)

            post_stop_mqtt = [m[1] for m in mqtt_sub.get_messages() if m[1].get("household_id") == "H001" or m[1].get("house") == "H001"]
            post_stop_sse = [e[1] for e in sse_sub.get_events() if e[1].get("house") == "H001"]

            print(f" -> stop 후 3초간 추가 수집: MQTT={len(post_stop_mqtt)}개, SSE={len(post_stop_sse)}개", flush=True)
            self.assertEqual(len(post_stop_mqtt), 0, f"stop 이후에도 MQTT 발행이 계속되고 있습니다: {len(post_stop_mqtt)}개")
            self.assertEqual(len(post_stop_sse), 0, f"stop 이후에도 SSE 이벤트가 계속되고 있습니다: {len(post_stop_sse)}개")
            print(" -> stop 후 발행 완전 중단 확인 통과", flush=True)

        finally:
            self.http_post("/api/stop")
            safe_stop_and_join(mqtt_sub, sse_sub, timeout=5.0)
            self.assertFalse(mqtt_sub.is_alive(), "MQTT 구독자 스레드가 정상 종료되지 않았습니다.")
            self.assertFalse(sse_sub.is_alive(), "SSE 구독자 스레드가 정상 종료되지 않았습니다.")


if __name__ == "__main__":
    unittest.main(verbosity=2)
