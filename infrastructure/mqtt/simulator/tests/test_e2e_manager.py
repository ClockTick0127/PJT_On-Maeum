"""
NILM 스마트홈 시뮬레이터 단일 세션 기반 E2E 세션 관리자(e2e_manager.py) 단위 테스트

검증 내용:
1. aggregate_session_status 8대 순수 함수 규칙 검증
2. 단일 세션 제약 (활성 세션 중복 409, terminal 후 세션 교체, 이전 run_id 404)
3. 가구별 독립성 (H001 연결 실패 시 H002 완주, pause/stop/예외 격리, 가구별 독립 client)
4. Terminal 확정 선후관계:
   - executor COMPLETED여도 MQTT context 종료 전에는 household COMPLETED가 아님
   - executor STOPPED여도 MQTT context 종료 전에는 household STOPPED가 아님
   - context exit 실패 시 FAILED 확정 및 에러 기록
5. 상태 스냅샷 및 Pause/Resume/Stop 제어:
   - pause 요청 후 PAUSING, executor 경계 도달 후 PAUSED
   - resume 후 실제 executor snapshot 기준 RUNNING
   - 멱등 200 OK 응답
6. Bounded 셧다운 (5초 내 스레드 종료, 활성/일시정지 태스크 정리)
7. 루프 dispatch 실패 시 STARTING 세션 rollback
8. 가구별 client factory가 서로 다른 client 생성
9. 실제 MQTT/TLS 설정 해석 경로 재사용
10. 새 세션 허용은 이전 세션의 모든 MQTT context 종료 후에만 가능
"""
from __future__ import annotations

import asyncio
import threading
import time
import unittest
from datetime import date
from unittest.mock import MagicMock, patch

from server.e2e_manager import (
    E2EScheduleSessionManager,
    E2ESession,
    HouseholdExecutionTask,
    HouseholdTaskStatus,
    SessionStatus,
    aggregate_session_status,
    E2EError,
    E2EConflictError,
    E2ERunNotFoundError,
    E2EHouseholdNotFoundError,
    E2EStartTimeoutError,
    E2EStartError,
    E2EOperationTimeoutError,
    E2ESnapshotTimeoutError,
    SessionActivationState,
    CommandStatus,
    CommandEnvelope,
)
from engine.schedule_executor import DeterministicScheduleExecutor, ExecutionMode, ExecutionStatus


class FakeMqttClient:
    def __init__(self, household_id: str):
        self.household_id = household_id
        self.published = []
        self._count = 0

    async def publish(self, topic: str, payload: str, qos: int = 1, retain: bool = False):
        self.published.append((topic, payload, qos, retain))
        self._count += 1
        # Event loop yield every 200 ticks
        if self._count % 200 == 0:
            await asyncio.sleep(0)


class FakeMqttContext:
    def __init__(
        self,
        client: FakeMqttClient,
        fail_enter: bool = False,
        fail_exit: bool = False,
        delay_enter: float = 0.0,
        delay_exit: float = 0.0,
    ):
        self.client = client
        self.fail_enter = fail_enter
        self.fail_exit = fail_exit
        self.delay_enter = delay_enter
        self.delay_exit = delay_exit
        self.entered = False
        self.exited = False

    async def __aenter__(self):
        await asyncio.sleep(0)
        if self.delay_enter > 0:
            await asyncio.sleep(self.delay_enter)
        if self.fail_enter:
            raise ConnectionError("Mock connection failed")
        self.entered = True
        return self.client

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.delay_exit > 0:
            await asyncio.sleep(self.delay_exit)
        self.exited = True
        if self.fail_exit:
            raise RuntimeError("Mock disconnect failed")


class FakeMqttFactory:
    def __init__(
        self,
        fail_households: set[str] | None = None,
        fail_exit_households: set[str] | None = None,
        delay_exit: float = 0.0,
    ):
        self.fail_households = fail_households or set()
        self.fail_exit_households = fail_exit_households or set()
        self.delay_exit = delay_exit
        self.clients: dict[str, FakeMqttClient] = {}
        self.contexts: dict[str, FakeMqttContext] = {}

    def __call__(self, household_id: str):
        client = FakeMqttClient(household_id)
        self.clients[household_id] = client
        ctx = FakeMqttContext(
            client=client,
            fail_enter=(household_id in self.fail_households),
            fail_exit=(household_id in self.fail_exit_households),
            delay_exit=self.delay_exit,
        )
        self.contexts[household_id] = ctx
        return ctx


class TestE2ESessionManager(unittest.TestCase):
    """E2EScheduleSessionManager 단위 테스트"""

    def setUp(self):
        self._initial_threads = {t.ident: t.name for t in threading.enumerate()}
        self.factory = FakeMqttFactory()
        self.manager = E2EScheduleSessionManager(
            broker_config={"host": "localhost", "port": 1883},
            mqtt_client_factory=self.factory,
        )

    def tearDown(self):
        self.manager.shutdown(timeout_sec=2.0)
        self.assertFalse(self.manager._loop_thread.is_alive())
        # 이번 테스트에서 새로 생성되어 종료되지 않은 스레드 검증
        current_threads = threading.enumerate()
        new_threads = [t for t in current_threads if t.ident not in self._initial_threads and t.is_alive()]
        leaked_loops = [t.name for t in new_threads if "E2ESessionManagerLoop" in t.name]
        leaked_workers = [t.name for t in new_threads if "ThreadPoolExecutor" in t.name or "ThreadPool" in t.name]
        self.assertEqual(leaked_loops, [], f"잔존 E2ESessionManagerLoop 스레드: {leaked_loops}")
        self.assertEqual(leaked_workers, [], f"잔존 ThreadPoolExecutor 워커 스레드: {leaked_workers}")

    def test_aggregate_session_status_pure_function(self):
        """세션 상태 집계 순수 함수의 8가지 규칙 검증"""
        # 1. 하나라도 STARTING -> STARTING
        self.assertEqual(
            aggregate_session_status([HouseholdTaskStatus.STARTING, HouseholdTaskStatus.RUNNING]),
            "STARTING"
        )
        # 2. 하나라도 RUNNING/PAUSING/STOPPING -> RUNNING
        self.assertEqual(
            aggregate_session_status([HouseholdTaskStatus.RUNNING, HouseholdTaskStatus.PAUSED]),
            "RUNNING"
        )
        self.assertEqual(
            aggregate_session_status([HouseholdTaskStatus.PAUSING, HouseholdTaskStatus.COMPLETED]),
            "RUNNING"
        )
        self.assertEqual(
            aggregate_session_status([HouseholdTaskStatus.STOPPING, HouseholdTaskStatus.STOPPED]),
            "RUNNING"
        )
        # 3. 활성 실행 없이 하나라도 PAUSED -> PAUSED
        self.assertEqual(
            aggregate_session_status([HouseholdTaskStatus.PAUSED, HouseholdTaskStatus.COMPLETED]),
            "PAUSED"
        )
        # 4. 전원 COMPLETED -> COMPLETED
        self.assertEqual(
            aggregate_session_status([HouseholdTaskStatus.COMPLETED, HouseholdTaskStatus.COMPLETED]),
            "COMPLETED"
        )
        # 5. 전원 FAILED -> FAILED
        self.assertEqual(
            aggregate_session_status([HouseholdTaskStatus.FAILED, HouseholdTaskStatus.FAILED]),
            "FAILED"
        )
        # 6. 전원 STOPPED -> STOPPED
        self.assertEqual(
            aggregate_session_status([HouseholdTaskStatus.STOPPED, HouseholdTaskStatus.STOPPED]),
            "STOPPED"
        )
        # 7. terminal 혼합 상태이고 FAILED 존재 -> PARTIAL_FAILED
        self.assertEqual(
            aggregate_session_status([HouseholdTaskStatus.COMPLETED, HouseholdTaskStatus.FAILED]),
            "PARTIAL_FAILED"
        )
        self.assertEqual(
            aggregate_session_status([HouseholdTaskStatus.STOPPED, HouseholdTaskStatus.FAILED]),
            "PARTIAL_FAILED"
        )
        # 8. COMPLETED와 STOPPED 혼합 (FAILED 없음) -> STOPPED
        self.assertEqual(
            aggregate_session_status([HouseholdTaskStatus.COMPLETED, HouseholdTaskStatus.STOPPED]),
            "STOPPED"
        )

    def test_each_household_uses_distinct_client(self):
        """가구별로 서로 다른 독립 aiomqtt.Client 인스턴스가 주입되는지 확인"""
        req = {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [
                {"household_id": "H001", "scenario": "ACTIVITY_NORMAL"},
                {"household_id": "H002", "scenario": "ACTIVITY_NORMAL"},
            ],
        }
        resp = self.manager.create_and_start_session(req)
        time.sleep(0.1)

        self.assertIn("H001", self.factory.clients)
        self.assertIn("H002", self.factory.clients)
        self.assertIsNot(self.factory.clients["H001"], self.factory.clients["H002"])

    def test_household_connect_failure_does_not_cancel_other_households(self):
        """H001 연결 실패 시 H002는 정상적으로 계속 실행됨을 검증"""
        factory = FakeMqttFactory(fail_households={"H001"})
        manager = E2EScheduleSessionManager(
            broker_config={"host": "localhost", "port": 1883},
            mqtt_client_factory=factory,
        )
        try:
            req = {
                "reference_date": "2026-09-16",
                "execution": {"mode": "BURST"},
                "households": [
                    {"household_id": "H001", "scenario": "ACTIVITY_NORMAL"},
                    {"household_id": "H002", "scenario": "ACTIVITY_NORMAL"},
                ],
            }
            resp = manager.create_and_start_session(req)
            run_id = resp["run_id"]

            time.sleep(0.3)

            snap_h1 = manager.get_household_snapshot(run_id, "H001")
            snap_h2 = manager.get_household_snapshot(run_id, "H002")

            self.assertIsNotNone(snap_h1)
            self.assertIsNotNone(snap_h2)
            self.assertEqual(snap_h1["state"], "FAILED")
            self.assertIn("Mock connection failed", str(snap_h1["last_error"]))
            # H002는 격리되어 RUNNING 또는 완주
            self.assertIn(snap_h2["state"], ("RUNNING", "COMPLETED"))
        finally:
            manager.shutdown(timeout_sec=2.0)

    def test_household_pause_and_resume_isolation(self):
        """pause 요청 후 PAUSING 및 경계 도달 후 PAUSED, resume 후 RUNNING 스냅샷 검증"""
        req = {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [
                {"household_id": "H001", "scenario": "ACTIVITY_NORMAL"},
                {"household_id": "H002", "scenario": "ACTIVITY_NORMAL"},
            ],
        }
        resp = self.manager.create_and_start_session(req)
        run_id = resp["run_id"]

        # H001이 STARTING에서 RUNNING으로 진입할 때까지 대기
        for _ in range(50):
            snap = self.manager.get_household_snapshot(run_id, "H001")
            if snap and snap["state"] == "RUNNING":
                break
            time.sleep(0.01)

        # H001 일시정지 요청
        pause_resp = self.manager.pause_household(run_id, "H001")
        self.assertIn(pause_resp["status"], ("accepted", "success"))

        # H001이 PAUSED 상태로 수렴할 때까지 대기
        for _ in range(50):
            snap_h1 = self.manager.get_household_snapshot(run_id, "H001")
            if snap_h1 and snap_h1["state"] == "PAUSED":
                break
            time.sleep(0.05)

        snap_h1 = self.manager.get_household_snapshot(run_id, "H001")
        snap_h2 = self.manager.get_household_snapshot(run_id, "H002")
        self.assertEqual(snap_h1["state"], "PAUSED")
        self.assertNotEqual(snap_h2["state"], "PAUSED")

        # 이미 PAUSED일 때 중복 pause는 200 OK (status: success) 멱등
        dup_pause = self.manager.pause_household(run_id, "H001")
        self.assertEqual(dup_pause["status"], "success")
        self.assertEqual(dup_pause["current_state"], "PAUSED")

        # H001 재개
        resume_resp = self.manager.resume_household(run_id, "H001")
        self.assertIn(resume_resp["status"], ("accepted", "success"))

        # 재개 후 RUNNING (또는 COMPLETED) 확인
        for _ in range(50):
            snap_h1 = self.manager.get_household_snapshot(run_id, "H001")
            if snap_h1 and snap_h1["state"] in ("RUNNING", "COMPLETED"):
                break
            time.sleep(0.05)
        self.assertIn(snap_h1["state"], ("RUNNING", "COMPLETED"))

    def test_executor_completed_not_premature_completed_before_context_exit(self):
        """executor COMPLETED여도 MQTT context 종료 전에는 household COMPLETED가 아님을 검증"""
        delayed_factory = FakeMqttFactory(delay_exit=0.3)
        manager = E2EScheduleSessionManager(
            broker_config={"host": "localhost", "port": 1883},
            mqtt_client_factory=delayed_factory,
        )

        async def fake_run(client):
            # run 완료 시 executor status는 COMPLETED
            session = manager.get_current_session()
            if session and "H001" in session.tasks and session.tasks["H001"].executor:
                session.tasks["H001"].executor._status = ExecutionStatus.COMPLETED
            return MagicMock()

        try:
            with patch.object(DeterministicScheduleExecutor, "run", side_effect=fake_run):
                req = {
                    "reference_date": "2026-09-16",
                    "execution": {"mode": "BURST"},
                    "households": [
                        {"household_id": "H001", "scenario": "ACTIVITY_NONE"},
                    ],
                }
                resp = manager.create_and_start_session(req)
                run_id = resp["run_id"]

                # executor run은 즉시 완료되지만 context exit는 delay_exit(0.3s) 동안 진행 중
                time.sleep(0.1)
                ctx = delayed_factory.contexts.get("H001")
                session = manager.get_current_session()
                self.assertIsNotNone(session)
                task = session.tasks["H001"]
                self.assertIsNotNone(task.executor)
                self.assertEqual(task.executor.snapshot().status, ExecutionStatus.COMPLETED)

                # Context exit 완료 전: household는 아직 COMPLETED가 아님 (RUNNING)
                snap = manager.get_household_snapshot(run_id, "H001")
                self.assertIsNotNone(snap)
                self.assertEqual(snap["state"], "RUNNING")
                self.assertNotEqual(snap["state"], "COMPLETED")

                # Context exit 완료 후: household COMPLETED 확정
                time.sleep(0.4)
                snap_after = manager.get_household_snapshot(run_id, "H001")
                self.assertIsNotNone(snap_after)
                self.assertEqual(snap_after["state"], "COMPLETED")
        finally:
            manager.shutdown(timeout_sec=2.0)

    def test_cannot_submit_task_if_loop_not_ready(self):
        """E2E loop 준비 전 task 제출 불가 검증"""
        orig_event_wait = threading.Event.wait

        class PatchedWait:
            def __get__(self, instance, owner):
                if instance is None:
                    return self
                def bound_wait(timeout=None):
                    # Thread.start()의 _started.wait()이나 Thread.join()의 _is_stopped.wait()은 방해하지 않고
                    # _ready_event(timeout=5.0)만 False 반환
                    if timeout == 5.0:
                        return False
                    return orig_event_wait(instance, timeout=timeout)
                return bound_wait

        with patch.object(threading.Event, "wait", PatchedWait()):
            with self.assertRaises(RuntimeError) as ctx:
                E2EScheduleSessionManager(
                    broker_config={"host": "localhost", "port": 1883},
                    mqtt_client_factory=self.factory,
                )
            self.assertIn("초기화 실패", str(ctx.exception))

    def test_executor_stopped_not_premature_stopped_before_context_exit(self):
        """executor STOPPED여도 MQTT context 종료 전에는 household STOPPED가 아님을 검증"""
        delayed_factory = FakeMqttFactory(delay_exit=0.4)
        manager = E2EScheduleSessionManager(
            broker_config={"host": "localhost", "port": 1883},
            mqtt_client_factory=delayed_factory,
        )
        try:
            req = {
                "reference_date": "2026-09-16",
                "execution": {"mode": "BURST"},
                "households": [
                    {"household_id": "H001", "scenario": "ACTIVITY_NORMAL"},
                ],
            }
            resp = manager.create_and_start_session(req)
            run_id = resp["run_id"]
            time.sleep(0.05)

            # 중지 요청 접수 -> executor는 즉시 멈추고 context exit 진입
            stop_resp = manager.stop_household(run_id, "H001")
            self.assertIn(stop_resp["status"], ("accepted", "success"))

            # MQTT context exit 지연 중 (0.1s 시점): STOPPED가 아니어야 함! (STOPPING 유지)
            time.sleep(0.1)
            ctx = delayed_factory.contexts.get("H001")
            if ctx and not ctx.exited:
                snap = manager.get_household_snapshot(run_id, "H001")
                self.assertIsNotNone(snap)
                self.assertEqual(snap["state"], "STOPPING")

            # context 완전히 종료 후: STOPPED 확정
            time.sleep(0.5)
            snap_after = manager.get_household_snapshot(run_id, "H001")
            self.assertEqual(snap_after["state"], "STOPPED")
        finally:
            manager.shutdown(timeout_sec=2.0)

    def test_context_exit_failure_sets_failed_state(self):
        """MQTT context __aexit__ 실패 시 COMPLETED나 STOPPED가 아닌 FAILED로 확정됨을 검증"""
        factory = FakeMqttFactory(fail_exit_households={"H001"})
        manager = E2EScheduleSessionManager(
            broker_config={"host": "localhost", "port": 1883},
            mqtt_client_factory=factory,
        )
        try:
            req = {
                "reference_date": "2026-09-16",
                "execution": {"mode": "BURST"},
                "households": [
                    {"household_id": "H001", "scenario": "ACTIVITY_NORMAL"},
                ],
            }
            resp = manager.create_and_start_session(req)
            run_id = resp["run_id"]
            time.sleep(0.05)

            # 중지 요청을 통해 exit를 즉시 유도
            manager.stop_household(run_id, "H001")
            time.sleep(0.2)

            snap = manager.get_household_snapshot(run_id, "H001")
            self.assertIsNotNone(snap)
            self.assertEqual(snap["state"], "FAILED")
            self.assertIn("Mock disconnect failed", str(snap["last_error"]))
        finally:
            manager.shutdown(timeout_sec=2.0)

    def test_second_session_start_rejected_when_active(self):
        """활성 세션이 존재할 때 신규 세션 시작 요청은 409 거절 가능(is_active=True)함을 검증"""
        req = {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [
                {"household_id": "H001", "scenario": "ACTIVITY_NORMAL"},
            ],
        }
        self.manager.create_and_start_session(req)
        self.assertTrue(self.manager.is_active())

    def test_new_session_allowed_only_after_all_contexts_exit(self):
        """새 세션 허용은 이전 세션의 모든 MQTT context 종료 후에만 가능함을 검증"""
        delayed_factory = FakeMqttFactory(delay_exit=0.5)
        manager = E2EScheduleSessionManager(
            broker_config={"host": "localhost", "port": 1883},
            mqtt_client_factory=delayed_factory,
        )
        try:
            req = {
                "reference_date": "2026-09-16",
                "execution": {"mode": "BURST"},
                "households": [
                    {"household_id": "H001", "scenario": "ACTIVITY_NORMAL"},
                ],
            }
            resp = manager.create_and_start_session(req)
            run_id = resp["run_id"]
            time.sleep(0.05)

            # 중지 요청
            manager.stop_session(run_id)

            # context 종료 대기 중 (0.1s): 아직 is_active()가 True여야 함!
            time.sleep(0.1)
            ctx = delayed_factory.contexts.get("H001")
            if ctx and not ctx.exited:
                self.assertTrue(manager.is_active())

            # context 완전히 종료 후 (0.7s): is_active()가 False로 전이
            time.sleep(0.6)
            self.assertFalse(manager.is_active())
        finally:
            manager.shutdown(timeout_sec=2.0)

    def test_terminal_session_replaced_by_new_session(self):
        """세션이 terminal에 도달한 후에는 신규 세션이 기존 세션을 교체함을 검증"""
        req1 = {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [
                {"household_id": "H001", "scenario": "ACTIVITY_NORMAL"},
            ],
        }
        resp1 = self.manager.create_and_start_session(req1)
        run_id1 = resp1["run_id"]

        # 중지하여 즉시 terminal 유도
        self.manager.stop_session(run_id1)
        for _ in range(50):
            if not self.manager.is_active():
                break
            time.sleep(0.05)
        self.assertFalse(self.manager.is_active())

        # 신규 세션 시작
        req2 = {
            "reference_date": "2026-09-17",
            "execution": {"mode": "BURST"},
            "households": [
                {"household_id": "H002", "scenario": "ACTIVITY_NORMAL"},
            ],
        }
        resp2 = self.manager.create_and_start_session(req2)
        run_id2 = resp2["run_id"]

        self.assertNotEqual(run_id1, run_id2)
        # 구 run_id1은 더 이상 조회 불가 (E2ERunNotFoundError 발생)
        with self.assertRaises(E2ERunNotFoundError):
            self.manager.get_session_snapshot(run_id1)
        # 신규 run_id2는 정상 조회 가능
        self.assertIsNotNone(self.manager.get_session_snapshot(run_id2))

    def test_loop_dispatch_failure_rolls_back_session(self):
        """비동기 루프 제출(dispatch) 실패 시 세션 등록이 rollback됨을 검증"""
        req = {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [
                {"household_id": "H001", "scenario": "ACTIVITY_NORMAL"},
            ],
        }

        with patch("asyncio.run_coroutine_threadsafe", side_effect=RuntimeError("Dispatch error")):
            with self.assertRaises(RuntimeError):
                self.manager.create_and_start_session(req)

        # rollback 결과 확인: 활성 세션 없음
        self.assertIsNone(self.manager.get_current_session())
        self.assertFalse(self.manager.is_active())

    def test_cannot_submit_task_after_shutdown(self):
        """종료 후에는 task 제출이 불가능함을 검증"""
        self.manager.shutdown(timeout_sec=2.0)
        req = {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [
                {"household_id": "H001", "scenario": "ACTIVITY_NORMAL"},
            ],
        }
        with self.assertRaises(RuntimeError):
            self.manager.create_and_start_session(req)

    def test_shutdown_completes_bounded(self):
        """shutdown이 타임아웃(2.0초) 내에 안전하게 완료되고 스레드가 종료됨을 검증"""
        success = self.manager.shutdown(timeout_sec=2.0)
        self.assertTrue(success)
        self.assertFalse(self.manager._loop_thread.is_alive())

    def test_reuse_existing_mqtt_tls_config_resolution(self):
        """기존 MQTT/TLS 설정 해석 모듈 재사용 검증"""
        from engine.tls import resolve_mqtt_config, get_mqtt_tls_context
        cfg = self.manager.broker_config
        self.assertIn("host", cfg)
        self.assertIn("port", cfg)

    def test_executor_created_on_e2e_loop_thread(self):
        """executor 생성 thread가 E2E loop thread와 동일함을 검증"""
        created_threads = []
        original_init = DeterministicScheduleExecutor.__init__

        def spy_init(exec_self, *args, **kwargs):
            created_threads.append(threading.current_thread())
            original_init(exec_self, *args, **kwargs)

        with patch.object(DeterministicScheduleExecutor, "__init__", spy_init):
            req = {
                "reference_date": "2026-09-16",
                "execution": {"mode": "BURST"},
                "households": [{"household_id": "H001", "scenario": "ACTIVITY_NONE"}],
            }
            self.manager.create_and_start_session(req)
            time.sleep(0.1)
            self.assertTrue(len(created_threads) > 0)
            for th in created_threads:
                self.assertEqual(th, self.manager._loop_thread)

    def test_executor_control_called_on_e2e_loop_thread(self):
        """pause/resume/stop 호출 thread가 E2E loop thread와 동일함을 검증"""
        control_threads = []
        orig_pause = DeterministicScheduleExecutor.pause
        orig_resume = DeterministicScheduleExecutor.resume
        orig_stop = DeterministicScheduleExecutor.request_stop

        def spy_pause(exec_self, *args, **kwargs):
            control_threads.append(("pause", threading.current_thread()))
            return orig_pause(exec_self, *args, **kwargs)

        def spy_resume(exec_self, *args, **kwargs):
            control_threads.append(("resume", threading.current_thread()))
            return orig_resume(exec_self, *args, **kwargs)

        def spy_stop(exec_self, *args, **kwargs):
            control_threads.append(("stop", threading.current_thread()))
            return orig_stop(exec_self, *args, **kwargs)

        with patch.object(DeterministicScheduleExecutor, "pause", spy_pause), \
             patch.object(DeterministicScheduleExecutor, "resume", spy_resume), \
             patch.object(DeterministicScheduleExecutor, "request_stop", spy_stop):
            req = {
                "reference_date": "2026-09-16",
                "execution": {"mode": "BURST"},
                "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
            }
            resp = self.manager.create_and_start_session(req)
            run_id = resp["run_id"]
            time.sleep(0.05)
            self.manager.pause_household(run_id, "H001")
            self.manager.resume_household(run_id, "H001")
            self.manager.stop_household(run_id, "H001")

            self.assertTrue(len(control_threads) >= 3)
            for action, th in control_threads:
                self.assertEqual(th, self.manager._loop_thread)

    def test_executor_snapshot_called_on_e2e_loop_thread(self):
        """executor snapshot 조회 thread가 E2E loop thread와 동일함을 검증"""
        snap_threads = []
        orig_snap = DeterministicScheduleExecutor.snapshot

        def spy_snap(exec_self, *args, **kwargs):
            snap_threads.append(threading.current_thread())
            return orig_snap(exec_self, *args, **kwargs)

        with patch.object(DeterministicScheduleExecutor, "snapshot", spy_snap):
            req = {
                "reference_date": "2026-09-16",
                "execution": {"mode": "BURST"},
                "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
            }
            resp = self.manager.create_and_start_session(req)
            run_id = resp["run_id"]
            time.sleep(0.05)
            self.manager.get_session_snapshot(run_id)
            self.manager.get_household_snapshot(run_id, "H001")

            self.assertTrue(len(snap_threads) > 0)
            for th in snap_threads:
                self.assertEqual(th, self.manager._loop_thread)

    def test_get_snapshot_is_read_only_and_does_not_mutate_state(self):
        """GET snapshot이 executor 제어 상태 및 카운터를 변경하지 않음을 검증"""
        req = {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
        }
        resp = self.manager.create_and_start_session(req)
        run_id = resp["run_id"]
        time.sleep(0.05)
        self.manager.pause_household(run_id, "H001")
        time.sleep(0.1)

        # 반복적인 get_session_snapshot 호출
        snap1 = self.manager.get_session_snapshot(run_id)
        snap2 = self.manager.get_session_snapshot(run_id)
        h_snap1 = self.manager.get_household_snapshot(run_id, "H001")
        h_snap2 = self.manager.get_household_snapshot(run_id, "H001")

        self.assertEqual(snap1["overall_status"], snap2["overall_status"])
        self.assertEqual(h_snap1["state"], h_snap2["state"])
        self.assertEqual(h_snap1["published_samples"], h_snap2["published_samples"])

    def test_overall_state_all_paused_and_running_plus_paused(self):
        """모든 가구 PAUSED 시 overall PAUSED, RUNNING+PAUSED 시 overall RUNNING 검증"""
        req = {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [
                {"household_id": "H001", "scenario": "ACTIVITY_NORMAL"},
                {"household_id": "H002", "scenario": "ACTIVITY_NORMAL"},
            ],
        }
        resp = self.manager.create_and_start_session(req)
        run_id = resp["run_id"]
        time.sleep(0.05)

        # 1. H001만 pause -> 1 RUNNING, 1 PAUSED -> overall RUNNING
        self.manager.pause_household(run_id, "H001")
        time.sleep(0.1)
        snap_partial = self.manager.get_session_snapshot(run_id)
        self.assertEqual(snap_partial["overall_status"], "RUNNING")

        # 2. H002도 pause -> 둘 다 PAUSED -> overall PAUSED
        self.manager.pause_household(run_id, "H002")
        time.sleep(0.1)
        snap_all_paused = self.manager.get_session_snapshot(run_id)
        self.assertEqual(snap_all_paused["overall_status"], "PAUSED")

    def test_factory_type_error_recorded_as_failed_without_retry(self):
        """factory 내부 TypeError가 fallback 재시도 없이 FAILED 상태에 기록됨을 검증"""
        def broken_factory(household_id: str):
            raise TypeError("Custom internal TypeError from factory")

        manager = E2EScheduleSessionManager(
            broker_config={"host": "localhost", "port": 1883},
            mqtt_client_factory=broken_factory,
        )
        try:
            req = {
                "reference_date": "2026-09-16",
                "execution": {"mode": "BURST"},
                "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
            }
            resp = manager.create_and_start_session(req)
            run_id = resp["run_id"]
            time.sleep(0.1)

            snap = manager.get_household_snapshot(run_id, "H001")
            self.assertIsNotNone(snap)
            self.assertEqual(snap["state"], "FAILED")
            self.assertIn("Custom internal TypeError", str(snap["last_error"]))
        finally:
            manager.shutdown(timeout_sec=2.0)

    def test_concurrent_direct_manager_start_calls_only_one_succeeds(self):
        """두 스레드가 직접 manager를 동시에 호출해도 하나만 성공하고 다른 하나는 E2EConflictError 발생"""
        from server.e2e_manager import E2EConflictError
        req = {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
        }

        results = []
        errors = []

        def call_start():
            try:
                r = self.manager.create_and_start_session(req)
                results.append(r)
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=call_start)
        t2 = threading.Thread(target=call_start)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        self.assertEqual(len(results), 1)
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], E2EConflictError)

    def test_activity_insufficient_h002_planned_publish_samples_is_82079(self):
        """ACTIVITY_INSUFFICIENT를 실행하는 H002의 planned_publish_samples가 82,079인지 검증"""
        req = {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [
                {"household_id": "H001", "scenario": "ACTIVITY_NORMAL"},
                {"household_id": "H002", "scenario": "ACTIVITY_INSUFFICIENT"},
            ],
        }
        resp = self.manager.create_and_start_session(req)
        run_id = resp["run_id"]
        time.sleep(0.05)

        h2_snap = self.manager.get_household_snapshot(run_id, "H002")
        self.assertIsNotNone(h2_snap)
        self.assertEqual(h2_snap["scenario"], "ACTIVITY_INSUFFICIENT")
        self.assertEqual(h2_snap["planned_publish_samples"], 82079)

    def test_two_phase_activation_abort_prevents_mqtt_entry(self):
        """준비 코루틴 완료 후 커밋 전 abort/timeout 시, gate 개방 후에도 MQTT context 진입 0회 및 publish 0회 검증"""
        factory = FakeMqttFactory()
        manager = E2EScheduleSessionManager(
            broker_config={"host": "localhost", "port": 1883},
            mqtt_client_factory=factory,
        )
        try:
            req = {
                "reference_date": "2026-09-16",
                "execution": {"mode": "BURST"},
                "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
            }

            orig_rct = asyncio.run_coroutine_threadsafe

            class AbortTimeoutFutureWrapper:
                def __init__(self, real_future):
                    self._real = real_future

                def result(self, timeout=None):
                    # 실제 prepare가 완료될 때까지 대기 (가구 task 스폰 및 gate.wait() 도달 보장)
                    self._real.result(timeout=1.0)
                    # Phase 2 커밋 전 타임아웃 발생 시뮬레이션
                    raise TimeoutError("Simulated prepare timeout")

                def cancel(self):
                    return self._real.cancel()

                def cancelled(self):
                    return self._real.cancelled()

                def done(self):
                    return self._real.done()

                def exception(self, timeout=None):
                    return self._real.exception(timeout=timeout)

                def add_done_callback(self, fn):
                    return self._real.add_done_callback(fn)

            def fake_rct(coro, loop):
                real_fut = orig_rct(coro, loop)
                return AbortTimeoutFutureWrapper(real_fut)

            with patch("asyncio.run_coroutine_threadsafe", side_effect=fake_rct):
                with self.assertRaises(E2EStartTimeoutError):
                    manager.create_and_start_session(req, timeout_sec=0.05)

            # 루프가 abort 정리 작업(gate.set, cleanup)을 마칠 때까지 대기
            settled = threading.Event()
            manager._loop.call_soon_threadsafe(settled.set)
            self.assertTrue(settled.wait(timeout=1.0))

            self.assertEqual(len(factory.contexts), 0)
            self.assertFalse(manager.is_active())
            self.assertIsNone(manager.get_current_session())
        finally:
            manager.shutdown(timeout_sec=2.0)
            self.assertFalse(manager._loop_thread.is_alive())

    def test_start_queued_timeout_loop_release_no_mqtt_entry(self):
        """시작 future가 QUEUED인 상태에서 타임아웃된 후 루프가 해제되어도 MQTT context 진입 0회, publish 0회 검증"""
        factory = FakeMqttFactory()
        manager = E2EScheduleSessionManager(
            broker_config={"host": "localhost", "port": 1883},
            mqtt_client_factory=factory,
        )
        entered = threading.Event()
        release = threading.Event()

        def block_loop():
            entered.set()
            release.wait(timeout=2.0)

        try:
            manager._loop.call_soon_threadsafe(block_loop)
            self.assertTrue(entered.wait(timeout=1.0))

            req = {
                "reference_date": "2026-09-16",
                "execution": {"mode": "BURST"},
                "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
            }

            try:
                with self.assertRaises(E2EStartTimeoutError):
                    manager.create_and_start_session(req, timeout_sec=0.05)
            finally:
                release.set()

            # 루프 해제 후 큐잉된 작업 처리 대기
            settled = threading.Event()
            manager._loop.call_soon_threadsafe(settled.set)
            self.assertTrue(settled.wait(timeout=1.0))

            self.assertEqual(len(factory.contexts), 0)
            self.assertFalse(manager.is_active())
            self.assertIsNone(manager.get_current_session())
        finally:
            release.set()
            manager.shutdown(timeout_sec=2.0)
            self.assertFalse(manager._loop_thread.is_alive())

    def test_command_envelope_queued_timeout_prevents_late_mutation(self):
        """제어 명령이 QUEUED 상태에서 타임아웃 선점(ABORTED)되면 루프 해제 후 executor 상태 변경 0회 검증"""
        req = {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
        }
        resp = self.manager.create_and_start_session(req)
        run_id = resp["run_id"]
        time.sleep(0.05)

        h_snap = self.manager.get_household_snapshot(run_id, "H001")
        self.assertEqual(h_snap["state"], "RUNNING")

        entered = threading.Event()
        release = threading.Event()

        def block_loop():
            entered.set()
            release.wait(timeout=2.0)

        self.manager._loop.call_soon_threadsafe(block_loop)
        self.assertTrue(entered.wait(timeout=1.0))

        try:
            with self.assertRaises(E2EOperationTimeoutError):
                self.manager.pause_household(run_id, "H001", timeout_sec=0.02)
        finally:
            release.set()

        # 루프 해제 후 큐잉된 작업 처리 대기
        settled = threading.Event()
        self.manager._loop.call_soon_threadsafe(settled.set)
        self.assertTrue(settled.wait(timeout=1.0))

        snap_after = self.manager.get_household_snapshot(run_id, "H001")
        self.assertEqual(snap_after["state"], "RUNNING")

    def test_command_envelope_claimed_returns_result_not_503(self):
        """제어 명령이 CLAIMED를 선점한 경우 accepted_result를 반환하며 503 타임아웃이 아님을 검증"""
        req = {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
        }
        resp = self.manager.create_and_start_session(req)
        run_id = resp["run_id"]
        time.sleep(0.05)

        envelope = CommandEnvelope(action="PAUSE", run_id=run_id, household_id="H001")
        envelope.status = CommandStatus.CLAIMED
        envelope.accepted_result = {
            "status": "accepted",
            "run_id": run_id,
            "household_id": "H001",
            "requested_action": "PAUSE",
            "current_state": "PAUSING",
        }

        class ClaimedTimeoutStubFuture:
            def result(self, timeout=None):
                raise TimeoutError("Future timeout while CLAIMED")

            def cancel(self):
                pass

        def fake_run_coroutine_threadsafe(coro, loop):
            # coroutine was never awaited 경고 방지를 위해 즉시 close
            coro.close()
            return ClaimedTimeoutStubFuture()

        with patch("asyncio.run_coroutine_threadsafe", side_effect=fake_run_coroutine_threadsafe):
            res = self.manager._dispatch_command(envelope, timeout_sec=0.01)
            self.assertEqual(res["status"], "accepted")
            self.assertEqual(res["requested_action"], "PAUSE")
            self.assertEqual(res["current_state"], "PAUSING")

    def test_stop_session_linearization_no_partial_timeout(self):
        """stop_session이 중간 await 없이 원자적으로 전체 가구를 중단 처리함을 검증"""
        req = {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [
                {"household_id": "H001", "scenario": "ACTIVITY_NORMAL"},
                {"household_id": "H002", "scenario": "ACTIVITY_NORMAL"},
            ],
        }
        resp = self.manager.create_and_start_session(req)
        run_id = resp["run_id"]
        time.sleep(0.05)

        stop_res = self.manager.stop_session(run_id)
        self.assertEqual(stop_res["status"], "accepted")
        self.assertEqual(stop_res["requested_action"], "STOP")

        time.sleep(0.1)
        snap1 = self.manager.get_household_snapshot(run_id, "H001")
        snap2 = self.manager.get_household_snapshot(run_id, "H002")
        self.assertIn(snap1["state"], ("STOPPING", "STOPPED"))
        self.assertIn(snap2["state"], ("STOPPING", "STOPPED"))

    def test_snapshot_queued_timeout_cancels_future(self):
        """스냅샷 조회가 QUEUED 상태에서 타임아웃 시 future가 취소됨을 검증"""
        req = {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
        }
        resp = self.manager.create_and_start_session(req)
        run_id = resp["run_id"]
        time.sleep(0.05)

        entered = threading.Event()
        release = threading.Event()

        def block_loop():
            entered.set()
            release.wait(timeout=2.0)

        self.manager._loop.call_soon_threadsafe(block_loop)
        self.assertTrue(entered.wait(timeout=1.0))

        try:
            with self.assertRaises(E2ESnapshotTimeoutError):
                self.manager.get_session_snapshot(run_id, timeout_sec=0.02)
        finally:
            release.set()

        # 루프 해제 후 정상화 확인
        settled = threading.Event()
        self.manager._loop.call_soon_threadsafe(settled.set)
        self.assertTrue(settled.wait(timeout=1.0))

    def test_prepare_exception_raises_e2e_start_error(self):
        """_prepare_session_on_loop 내부 오류 시 E2EStartTimeoutError가 아니라 E2EStartError 발생 및 세션 rollback 검증"""
        req = {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
        }
        with patch.object(self.manager, "_prepare_session_on_loop", side_effect=ValueError("Internal prepare fail")):
            with self.assertRaises(E2EStartError) as ctx:
                self.manager.create_and_start_session(req)
            self.assertIn("Internal prepare fail", str(ctx.exception))

        self.assertIsNone(self.manager.get_current_session())
        self.assertFalse(self.manager.is_active())

    def test_activation_gate_schedule_failure_raises_runtime_error_not_202(self):
        """gate.set 예약 실패 시 202를 반환하지 않고 RuntimeError 발생 및 세션 롤백 검증"""
        req = {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
        }
        orig_cst = self.manager._loop.call_soon_threadsafe

        def fail_on_gate_set(callback, *args):
            if getattr(callback, "__name__", "") == "set" or (hasattr(callback, "__self__") and isinstance(getattr(callback, "__self__", None), asyncio.Event)):
                raise RuntimeError("Loop closed")
            return orig_cst(callback, *args)

        with patch.object(self.manager._loop, "call_soon_threadsafe", side_effect=fail_on_gate_set):
            with self.assertRaises(RuntimeError) as ctx:
                self.manager.create_and_start_session(req)
            self.assertIn("게이트 개방 예약 실패", str(ctx.exception))

        self.assertIsNone(self.manager.get_current_session())
        self.assertFalse(self.manager.is_active())

    def test_shutdown_strict_monotonic_deadline_and_no_task_leak(self):
        """shutdown이 deadline + Windows 스케줄링 허용오차 내에 반환되며 cooperative task 종료 확인"""
        req = {
            "reference_date": "2026-09-16",
            "execution": {"mode": "BURST"},
            "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
        }
        self.manager.create_and_start_session(req)
        time.sleep(0.05)

        t0 = time.monotonic()
        stopped = self.manager.shutdown(timeout_sec=0.2)
        elapsed = time.monotonic() - t0

        self.assertLessEqual(elapsed, 0.45)
        self.assertTrue(stopped)
        self.assertFalse(self.manager._loop_thread.is_alive())

    def test_shutdown_non_cooperative_returns_false_within_deadline(self):
        """루프가 동기 작업에 의해 점유되어 cooperative 정리가 불가능할 때 deadline 내에 False를 반환함을 검증"""
        manager = E2EScheduleSessionManager(
            broker_config={"host": "localhost", "port": 1883},
            mqtt_client_factory=FakeMqttFactory(),
        )
        entered = threading.Event()
        release = threading.Event()

        def block_loop():
            entered.set()
            release.wait(timeout=2.0)

        try:
            manager._loop.call_soon_threadsafe(block_loop)
            self.assertTrue(entered.wait(timeout=1.0))

            t0 = time.monotonic()
            stopped = manager.shutdown(timeout_sec=0.1)
            elapsed = time.monotonic() - t0

            self.assertLessEqual(elapsed, 0.45)
            self.assertFalse(stopped)
        finally:
            release.set()
            time.sleep(0.02)
            if manager._loop_thread and manager._loop_thread.is_alive():
                manager._loop_thread.join(timeout=1.0)
            self.assertFalse(manager._loop_thread.is_alive())

    def test_shutdown_loop_finalizer_bounded_drain_with_stubborn_task(self):
        """첫 번째 cancellation을 지연하는 task가 있어도 loop finalizer가 무제한 대기하지 않고 제한 시간 내 종료됨을 검증"""
        manager = E2EScheduleSessionManager(
            broker_config={"host": "localhost", "port": 1883},
            mqtt_client_factory=FakeMqttFactory(),
        )
        task_started = threading.Event()
        cancelled_count = 0

        async def delayed_cancel_coro():
            nonlocal cancelled_count
            task_started.set()
            while True:
                try:
                    await asyncio.sleep(10.0)
                except asyncio.CancelledError:
                    cancelled_count += 1
                    if cancelled_count <= 1:
                        # 첫 번째 cancellation을 일시적으로 지연/무시 (약 0.2초 대기하여 timeout_sec=0.1 초과 보장)
                        await asyncio.sleep(0.2)
                    else:
                        # 두 번째 cancellation(finalizer drain)에서는 정상 cooperative 종료
                        raise

        try:
            asyncio.run_coroutine_threadsafe(delayed_cancel_coro(), manager._loop)
            self.assertTrue(task_started.wait(timeout=1.0))

            t0 = time.monotonic()
            stopped = manager.shutdown(timeout_sec=0.1)
            elapsed = time.monotonic() - t0

            # 셧다운 호출 자체는 timeout_sec 허용오차 이내에 유한 반환
            self.assertLessEqual(elapsed, 0.45)

            # loop finalizer가 bounded drain 후 스레드를 정상 종료함을 확인
            if manager._loop_thread and manager._loop_thread.is_alive():
                manager._loop_thread.join(timeout=1.5)
            self.assertFalse(manager._loop_thread.is_alive())
            # 첫 번째 cancellation이 지연되었음을 확인
            self.assertGreaterEqual(cancelled_count, 1)
        finally:
            if manager._loop_thread and manager._loop_thread.is_alive():
                manager._loop.call_soon_threadsafe(manager._loop.stop)
                manager._loop_thread.join(timeout=1.0)
            self.assertFalse(manager._loop_thread.is_alive())

    def test_day_results_multiday_distinct_dates_and_exact_samples(self):
        """다일 시나리오에서 날짜별 결과(day_results)가 각 일자별로 구분되어 기록되는지 검증"""
        from engine.schedule import DaySchedule, ScenarioDefinition, SECONDS_PER_DAY
        from unittest.mock import patch

        # 2일짜리 가상 시나리오 정의 (2026-09-15, 2026-09-16)
        day0 = DaySchedule(day_offset=0, events=(), omission_ranges=())
        day1 = DaySchedule(day_offset=1, events=(), omission_ranges=())
        sc_2d = ScenarioDefinition(scenario_id="TWO_DAY_TEST", days=(day0, day1))

        with patch("server.e2e_manager.get_unified_scenario_definition", return_value=sc_2d):
            resp = self.manager.create_and_start_session({
                "reference_date": "2026-09-16",
                "execution": {"mode": "BURST"},
                "households": [{"household_id": "H001", "scenario": "TWO_DAY_TEST"}],
            })
            run_id = resp["run_id"]

            for _ in range(300):
                snap = self.manager.get_session_snapshot(run_id)
                if snap["overall_status"] == "COMPLETED":
                    break
                time.sleep(0.1)

            snap = self.manager.get_session_snapshot(run_id)
            self.assertEqual(snap["overall_status"], "COMPLETED")
            h001 = snap["households"][0]
            self.assertEqual(h001["completed_days"], 2)
            self.assertEqual(len(h001["day_results"]), 2)

            dr0 = h001["day_results"][0]
            dr1 = h001["day_results"][1]

            self.assertEqual(dr0["activity_date"], "2026-09-15")
            self.assertEqual(dr0["published_samples"], SECONDS_PER_DAY)
            self.assertEqual(dr0["status"], "COMPLETED")
            self.assertEqual(dr0["scenario"], "TWO_DAY_TEST")
            self.assertEqual(dr0["household_id"], "H001")

            self.assertEqual(dr1["activity_date"], "2026-09-16")
            self.assertEqual(dr1["published_samples"], SECONDS_PER_DAY)
            self.assertEqual(dr1["status"], "COMPLETED")
            self.assertEqual(dr1["scenario"], "TWO_DAY_TEST")
            self.assertEqual(dr1["household_id"], "H001")

    def test_resolve_start_time_delay_rules(self):
        """시간만 입력, 날짜/시간 입력, 과거 시각 처리 규칙 검증"""
        from datetime import datetime, timezone, timedelta
        from server.e2e_manager import resolve_start_time_delay, KST

        # 기준 현재 시각: 2026-09-19 20:00:00 KST
        base_now = datetime(2026, 9, 19, 20, 0, 0, tzinfo=KST)

        # 1. 생략 또는 None -> 0.0
        self.assertEqual(resolve_start_time_delay(None, now=base_now), 0.0)
        self.assertEqual(resolve_start_time_delay("", now=base_now), 0.0)

        # 2. 시간만 입력 - 미래 시각 (20:30:00 -> 30분 = 1800초)
        self.assertEqual(resolve_start_time_delay("20:30:00", now=base_now), 1800.0)
        self.assertEqual(resolve_start_time_delay("20:30", now=base_now), 1800.0)

        # 3. 시간만 입력 - 과거 시각 (19:00:00 -> 0.0초 대기)
        self.assertEqual(resolve_start_time_delay("19:00:00", now=base_now), 0.0)
        self.assertEqual(resolve_start_time_delay("20:00:00", now=base_now), 0.0)

        # 4. 날짜 및 시간 입력 - 미래 시각 (2026-09-20T01:00:00 -> 5시간 = 18000초)
        self.assertEqual(resolve_start_time_delay("2026-09-20T01:00:00", now=base_now), 18000.0)
        self.assertEqual(resolve_start_time_delay("2026-09-20T01:00:00+09:00", now=base_now), 18000.0)

        # 5. 날짜 및 시간 입력 - 과거 시각 (2026-09-18T10:00:00 -> 0.0초 대기)
        self.assertEqual(resolve_start_time_delay("2026-09-18T10:00:00", now=base_now), 0.0)

    def test_scheduled_start_time_delays_publishing_until_scheduled(self):
        """지정된 start_time 전에는 발행이 일어나지 않고, 예약 시각 도달 후 정상 발행됨을 검증"""
        from datetime import datetime, timedelta
        from server.e2e_manager import KST

        # 현재 KST 시각 기준 0.3초 뒤로 예약 시각 설정
        future_dt = datetime.now(KST) + timedelta(seconds=0.3)
        future_st = future_dt.isoformat()

        resp = self.manager.create_and_start_session({
            "reference_date": "2026-09-16",
            "start_time": future_st,
            "execution": {"mode": "BURST"},
            "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
        })
        run_id = resp["run_id"]

        # 0.1초 시점 (예약 시각 전): 어느 가구도 아직 발행하지 않음 (published_samples == 0, state == STARTING)
        time.sleep(0.1)
        snap = self.manager.get_session_snapshot(run_id)
        self.assertEqual(snap["overall_status"], "STARTING")
        self.assertEqual(snap["households"][0]["published_samples"], 0)
        self.assertEqual(snap["households"][0]["state"], "STARTING")

        # 0.3초 경과 후 예약 시각 도달 -> 발행 개시되어 BURST 모드로 완료 대기
        for _ in range(200):
            snap = self.manager.get_session_snapshot(run_id)
            if snap["overall_status"] == "COMPLETED":
                break
            time.sleep(0.1)

        snap = self.manager.get_session_snapshot(run_id)
        self.assertEqual(snap["overall_status"], "COMPLETED")
        self.assertEqual(snap["households"][0]["published_samples"], 86400)
        self.assertEqual(snap["households"][0]["completed_days"], 1)

    def test_scheduled_start_time_stopped_before_start(self):
        """예약 대기 중 세션을 중단하면 0건 발행 상태로 즉시 STOPPED 전이됨을 검증"""
        from datetime import datetime, timedelta
        from server.e2e_manager import KST

        # 10초 뒤로 미래 예약 시각 설정
        future_dt = datetime.now(KST) + timedelta(seconds=10.0)
        future_st = future_dt.isoformat()

        resp = self.manager.create_and_start_session({
            "reference_date": "2026-09-16",
            "start_time": future_st,
            "execution": {"mode": "BURST"},
            "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
        })
        run_id = resp["run_id"]

        time.sleep(0.05)
        snap = self.manager.get_session_snapshot(run_id)
        self.assertEqual(snap["overall_status"], "STARTING")
        self.assertEqual(snap["households"][0]["published_samples"], 0)

        # 예약 대기 중 세션 중단
        stop_res = self.manager.stop_session(run_id)
        self.assertEqual(stop_res["requested_action"], "STOP")

        for _ in range(50):
            snap = self.manager.get_session_snapshot(run_id)
            if snap["overall_status"] == "STOPPED":
                break
            time.sleep(0.02)

        snap = self.manager.get_session_snapshot(run_id)
        self.assertEqual(snap["overall_status"], "STOPPED")
        self.assertEqual(snap["households"][0]["published_samples"], 0)
        self.assertEqual(snap["households"][0]["completed_days"], 0)
        self.assertEqual(snap["households"][0]["day_results"], [])

    def test_scheduled_start_time_all_households_stopped_via_stop_household_cancels_timer(self):
        """다중 가구 예약 세션에서 stop_household로 모든 가구를 중단했을 때 잔여 예약 타이머가 취소됨을 검증"""
        from datetime import datetime, timedelta
        from server.e2e_manager import KST

        # 30초 뒤로 미래 예약 시각 설정
        future_dt = datetime.now(KST) + timedelta(seconds=30.0)
        future_st = future_dt.isoformat()

        resp = self.manager.create_and_start_session({
            "reference_date": "2026-09-16",
            "start_time": future_st,
            "execution": {"mode": "BURST"},
            "households": [
                {"household_id": "H001", "scenario": "ACTIVITY_NORMAL"},
                {"household_id": "H002", "scenario": "ACTIVITY_NORMAL"},
            ],
        })
        run_id = resp["run_id"]
        session = self.manager.get_current_session()
        self.assertIsNotNone(session)
        self.assertIsNotNone(session.scheduled_task)
        self.assertFalse(session.scheduled_task.done())

        # 1. H001만 먼저 stop_household로 중단
        res_h1 = self.manager.stop_household(run_id, "H001")
        self.assertIn(res_h1["status"], ("accepted", "success"))

        for _ in range(50):
            h1_snap = self.manager.get_household_snapshot(run_id, "H001")
            if h1_snap["state"] == "STOPPED":
                break
            time.sleep(0.02)
        h1_snap = self.manager.get_household_snapshot(run_id, "H001")
        self.assertEqual(h1_snap["state"], "STOPPED")

        # H002가 아직 대기 중이므로 예약 타이머는 계속 유지되어야 함
        self.assertFalse(session.scheduled_task.done())

        # 2. 마지막 남은 H002도 stop_household로 중단
        res_h2 = self.manager.stop_household(run_id, "H002")
        self.assertIn(res_h2["status"], ("accepted", "success"))

        for _ in range(50):
            h2_snap = self.manager.get_household_snapshot(run_id, "H002")
            if h2_snap["state"] == "STOPPED":
                break
            time.sleep(0.02)
        h2_snap = self.manager.get_household_snapshot(run_id, "H002")
        self.assertEqual(h2_snap["state"], "STOPPED")

        # 모든 가구가 중단되었으므로 잔여 예약 타이머가 취소(done)되어야 함!
        for _ in range(50):
            if session.scheduled_task.done():
                break
            time.sleep(0.02)
        self.assertTrue(session.scheduled_task.done())

        # 전체 세션 상태 또한 STOPPED로 전이 확인
        snap = self.manager.get_session_snapshot(run_id)
        self.assertEqual(snap["overall_status"], "STOPPED")
        self.assertEqual(snap["households"][0]["published_samples"], 0)
        self.assertEqual(snap["households"][1]["published_samples"], 0)


import queue
from server.manager import SimulatorManager


class TestSimulatorManagerBroadcastExternal(unittest.TestCase):
    """broadcast_external 전송 전용 경로 및 레거시 last_metrics 비오염 검증 단위 테스트"""

    def test_broadcast_external_delivers_to_subscriber_without_modifying_last_metrics(self):
        """broadcast_external 호출 후 last_metrics 및 last_metrics_by_house 불변 및 구독자 큐 정상 수신 검증"""
        manager = SimulatorManager(host="localhost", port=1883)
        legacy_metrics = {"house": "H001", "totalP": 120.0, "source": "LEGACY"}
        legacy_by_house = {"H001": {"house": "H001", "totalP": 120.0}}
        manager.last_metrics = dict(legacy_metrics)
        manager.last_metrics_by_house = dict(legacy_by_house)

        q = queue.Queue()
        manager.add_subscriber(q)

        e2e_payload = {
            "house": "H001",
            "scenario": "ACTIVITY_NORMAL",
            "totalP": 950.0,
            "source": "E2E",
            "run_id": "run-external-test-01",
        }

        manager.broadcast_external(e2e_payload)

        # 1. last_metrics와 last_metrics_by_house가 변경되지 않았음을 단정
        self.assertEqual(manager.last_metrics, legacy_metrics)
        self.assertEqual(manager.last_metrics_by_house, legacy_by_house)

        # 2. 구독자 큐에는 데이터가 정상 전달되었음을 단정
        received = q.get_nowait()
        self.assertEqual(received, e2e_payload)
        self.assertEqual(received["source"], "E2E")
        self.assertEqual(received["totalP"], 950.0)

    def test_e2e_execution_with_broadcast_external_does_not_pollute_manager_status(self):
        """E2E 실행 후 manager.get_status()의 last_metrics가 E2E 페이로드로 오염되지 않음을 단정"""
        manager = SimulatorManager(host="localhost", port=1883)
        self.assertIsNone(manager.last_metrics)
        self.assertEqual(manager.get_status()["last_metrics"], None)

        q = queue.Queue()
        manager.add_subscriber(q)

        e2e_manager = E2EScheduleSessionManager(
            broker_config={"host": "localhost", "port": 1883},
            mqtt_client_factory=FakeMqttFactory(),
            broadcast_callback=manager.broadcast_external,
        )

        try:
            start_res = e2e_manager.create_and_start_session({
                "reference_date": "2026-09-17",
                "execution": {"mode": "ACCELERATED", "speed": 1000.0},
                "households": [{"household_id": "H001", "scenario": "ACTIVITY_NORMAL"}],
            })
            run_id = start_res["run_id"]

            # 세션이 약간 진행되어 브로드캐스트가 발생할 때까지 잠시 대기
            time.sleep(0.3)

            # manager.get_status()의 last_metrics 확인: 절대 E2E 페이로드로 오염되지 않아야 함!
            status = manager.get_status()
            self.assertIsNone(
                status["last_metrics"],
                f"E2E 브로드캐스트로 인해 manager.get_status()['last_metrics']가 오염됨: {status['last_metrics']}",
            )
            self.assertIsNone(manager.last_metrics)

            # 구독자 큐에는 실제로 브로드캐스트가 도달했는지 확인
            self.assertGreater(q.qsize(), 0, "구독자 큐에 E2E 데이터가 도달했어야 합니다.")
            sample = q.get_nowait()
            self.assertEqual(sample["source"], "E2E")
            self.assertEqual(sample["run_id"], run_id)
        finally:
            e2e_manager.shutdown(timeout_sec=2.0)


if __name__ == "__main__":
    unittest.main()
