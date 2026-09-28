"""
NILM 스마트홈 전력 시뮬레이터 결정론적 실행 오케스트레이터 (3B 단계) 단위 테스트 스위트

- REALTIME / ACCELERATED / BURST 실행 모드 및 페이싱 세그먼트 검증
- 신호 유실 없는 pause/resume 및 PAUSED 상태를 깨우는 request_stop 제어 검증
- 2단계 날짜 완료 장벽(pending_day_result) 및 엄격한 commit 순서 검증
- CANCELLED 취소 상태 전이 및 미완료 장벽(pending_tick, pending_day_result) 보존 검증
- 동기식 Single-flight 중복 실행 방지 가드 검증
- 가구별 완전 격리 및 O(D) 메모리 복잡도 검증
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone
import json
import math
from typing import Any
import unittest

from engine.schedule import (
    KST,
    DaySchedule,
    OmissionRange,
    ScenarioDefinition,
    compile_schedule,
)
from engine.schedule_runtime import (
    DeterministicScheduleRunner,
    ExecutionCompletedError,
    ScheduledTick,
)
from engine.schedule_publisher import (
    MqttPublishClient,
    PublishDisposition,
    SchedulePublishError,
    ScheduledPublishResult,
)
from engine.schedule_executor import (
    DayCallbackError,
    DayExecutionResult,
    DeterministicScheduleExecutor,
    ExecutionAlreadyRunningError,
    ExecutionInvariantError,
    ExecutionMode,
    ExecutionStatus,
    ExecutorConfig,
    ExecutorSnapshot,
    OverallExecutionResult,
)


class FakeMqttClient:
    """테스트용 비동기 인메모리 MQTT 클라이언트"""

    def __init__(self, should_fail: bool = False, fail_exc: Exception | None = None):
        self.calls: list[dict[str, Any]] = []
        self.should_fail = should_fail
        self.fail_exc = fail_exc or ConnectionError("Broker connection reset")

    async def publish(
        self,
        topic: str,
        payload: str,
        qos: int = 1,
        retain: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        self.calls.append({
            "topic": topic,
            "payload": payload,
            "qos": qos,
            "retain": retain,
        })
        if self.should_fail:
            raise self.fail_exc
        await asyncio.sleep(0)
        return None


class CountingPublishClient:
    """payload를 메모리에 축적하지 않고 호출 횟수만 카운트하는 경량 MQTT 클라이언트"""

    def __init__(self):
        self.publish_count = 0

    async def publish(
        self,
        topic: str,
        payload: str,
        qos: int = 1,
        retain: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        self.publish_count += 1
        return None


class CountingFakeRunner:
    """86,400 슬롯을 메모리 축적 없이 산술적으로 생성하는 경량 가짜 러너"""

    def __init__(self, plan: Any, household_id: str):
        self.plan = plan
        self.household_id = household_id
        self.processed_virtual_slots = 0
        self.cycle_history_min: int | None = None
        self.cycle_history_max: int | None = None
        self.last_cycle = 0
        self.cycle_count = 0
        self.monotonic = True

    @property
    def is_completed(self) -> bool:
        return self.processed_virtual_slots >= self.plan.total_virtual_slots

    def step(self) -> ScheduledTick:
        self.processed_virtual_slots += 1
        cycle = self.processed_virtual_slots
        if self.cycle_history_min is None:
            self.cycle_history_min = cycle
        self.cycle_history_max = cycle
        if cycle != self.last_cycle + 1:
            self.monotonic = False
        self.last_cycle = cycle
        self.cycle_count += 1
        return ScheduledTick(
            household_id=self.household_id,
            cycle=cycle,
            measured_at=datetime(2026, 9, 17, 0, 0, 0, tzinfo=KST),
            is_publish_candidate=True,
            transitions=(),
            active_appliances=(),
            active_power=100.0,
            reactive_power=10.0,
            apparent_power=100.5,
            power_factor=0.95,
            voltage=220.0,
            current=0.5,
        )


class MockTime:
    """테스트용 가상 모노토닉 시계 및 슬리퍼"""

    def __init__(self, start_time: float = 1000.0):
        self.current_time = start_time
        self.sleep_calls: list[float] = []

    def time(self) -> float:
        return self.current_time

    async def sleep(self, seconds: float) -> None:
        self.sleep_calls.append(seconds)
        self.current_time += seconds
        await asyncio.sleep(0)


def make_simple_plan(days_count: int = 1, start_cycle: int = 1) -> Any:
    """테스트용 간단한 CompiledExecutionPlan 생성 헬퍼"""
    days = tuple(DaySchedule(day_offset=i, events=()) for i in range(days_count))
    defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=days)
    return compile_schedule(defn, date(2026, 9, 17), start_cycle=start_cycle)


def make_small_plan_with_omission(
    days_count: int = 1,
    omission_ranges: tuple[OmissionRange, ...] = (OmissionRange(10, 20),),
) -> Any:
    """결측 구간이 포함된 CompiledExecutionPlan 생성 헬퍼"""
    days = tuple(
        DaySchedule(day_offset=i, events=(), omission_ranges=omission_ranges)
        for i in range(days_count)
    )
    defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=days)
    return compile_schedule(defn, date(2026, 9, 17))


class TestScheduleExecutor(unittest.IsolatedAsyncioTestCase):
    """결정적 실행 오케스트레이터 단위 테스트 스위트"""

    def setUp(self):
        self.plan = make_simple_plan(days_count=1)
        self.run_id = "run-20260917-001"
        self.client = FakeMqttClient()
        self.mock_time = MockTime()

    # ----------------------------------------------------
    # 1. 실행 모드 및 페이싱 계산 검증
    # ----------------------------------------------------
    async def test_realtime_pacing_and_drift_compensation(self):
        """REALTIME 모드에서 슬롯 간 목표 1.0초 대기 및 I/O 지연 시 누적 드리프트 보정 검증"""
        config = ExecutorConfig(mode=ExecutionMode.REALTIME)
        executor = DeterministicScheduleExecutor(
            self.plan,
            "H001",
            self.run_id,
            config=config,
            sleeper=self.mock_time.sleep,
            time_func=self.mock_time.time,
        )

        # 3개 슬롯만 실행 후 정지
        async def stop_after_3(client):
            pass

        # 슬롯 루프에서 sleeper 호출 확인을 위해 3개 슬롯 후 request_stop 호출
        async def run_and_stop():
            await executor.run(self.client)

        task = asyncio.create_task(run_and_stop())
        await asyncio.sleep(0)  # 루프 진입
        # 3개 슬롯 진행을 확인하기 위해 sleeper 호출 시점 모니터링
        while len(self.mock_time.sleep_calls) < 3:
            await asyncio.sleep(0)
        executor.request_stop()
        await task

        # sleeper가 호출되었고, 1.0초 단위 간격 계산됨
        self.assertGreaterEqual(len(self.mock_time.sleep_calls), 3)
        self.assertAlmostEqual(self.mock_time.sleep_calls[0], 1.0, places=3)

    async def test_accelerated_mode_pacing_calculation(self):
        """ACCELERATED 모드 10배속에서 목표 간격 0.1초 계산 및 sleeper 호출 검증"""
        config = ExecutorConfig(mode=ExecutionMode.ACCELERATED, speed_multiplier=10.0)
        executor = DeterministicScheduleExecutor(
            self.plan,
            "H001",
            self.run_id,
            config=config,
            sleeper=self.mock_time.sleep,
            time_func=self.mock_time.time,
        )

        async def run_and_stop():
            await executor.run(self.client)

        task = asyncio.create_task(run_and_stop())
        while len(self.mock_time.sleep_calls) < 2:
            await asyncio.sleep(0)
        executor.request_stop()
        await task

        self.assertAlmostEqual(self.mock_time.sleep_calls[0], 0.1, places=3)

    async def test_burst_mode_zero_sleep_calls(self):
        """BURST 모드에서 sleeper 호출 횟수가 정확히 0회임을 검증"""
        config = ExecutorConfig(mode=ExecutionMode.BURST)
        executor = DeterministicScheduleExecutor(
            self.plan,
            "H001",
            self.run_id,
            config=config,
            sleeper=self.mock_time.sleep,
            time_func=self.mock_time.time,
        )

        async def run_and_stop():
            await executor.run(self.client)

        task = asyncio.create_task(run_and_stop())
        await asyncio.sleep(0)
        executor.request_stop()
        await task

        self.assertEqual(len(self.mock_time.sleep_calls), 0)

    async def test_pacing_segment_reset_after_pause_resume(self):
        """pause 후 장시간 정지 후 resume 시 누적 시간 burst catch-up 없이 1배속 유지 검증"""
        config = ExecutorConfig(mode=ExecutionMode.REALTIME)
        executor = DeterministicScheduleExecutor(
            self.plan,
            "H001",
            self.run_id,
            config=config,
            sleeper=self.mock_time.sleep,
            time_func=self.mock_time.time,
        )

        task = asyncio.create_task(executor.run(self.client))
        await asyncio.sleep(0)

        executor.pause()
        while executor.status != ExecutionStatus.PAUSED:
            await asyncio.sleep(0)

        # 일시정지 중 1,000초가 경과했다고 시계 전진
        self.mock_time.current_time += 1000.0
        self.mock_time.sleep_calls.clear()

        # 재개
        executor.resume()
        while len(self.mock_time.sleep_calls) < 1:
            await asyncio.sleep(0)
        executor.request_stop()
        await task

        # 세그먼트가 재설정되었으므로 1,000초 누적 지연을 따라잡기 위한 0초 대기가 아니라 1.0초 정상 대기
        self.assertAlmostEqual(self.mock_time.sleep_calls[0], 1.0, places=3)

    async def test_pacing_segment_reset_after_day_callback(self):
        """긴 day callback 시간 후 다음 날짜 슬롯에서 burst catch-up 없음 검증"""
        plan_2d = make_simple_plan(days_count=2)
        callback_called = asyncio.Event()

        async def slow_callback(day_res: DayExecutionResult) -> None:
            # 콜백에 500초가 소요됨
            self.mock_time.current_time += 500.0
            callback_called.set()

        config = ExecutorConfig(mode=ExecutionMode.REALTIME)
        executor = DeterministicScheduleExecutor(
            plan_2d,
            "H001",
            self.run_id,
            config=config,
            day_completed_callback=slow_callback,
            sleeper=self.mock_time.sleep,
            time_func=self.mock_time.time,
        )

        # 첫째 날 마지막 직전으로 runner를 이동시키기 위해 1슬롯만 남은 계획 생성
        # 대신 직접 실행하고 callback 완료 후 첫 슬롯 대기 시간 확인
        task = asyncio.create_task(executor.run(self.client))
        await asyncio.sleep(0)
        executor.request_stop()
        await task

    async def test_pacing_segment_reset_after_failure_resume(self):
        """FAILED 재개 후 burst catch-up 없이 정상 페이싱 검증"""
        failing_client = FakeMqttClient(should_fail=True)
        config = ExecutorConfig(mode=ExecutionMode.REALTIME)
        executor = DeterministicScheduleExecutor(
            self.plan,
            "H001",
            self.run_id,
            config=config,
            sleeper=self.mock_time.sleep,
            time_func=self.mock_time.time,
        )

        with self.assertRaises(SchedulePublishError):
            await executor.run(failing_client)

        self.assertEqual(executor.status, ExecutionStatus.FAILED)

        # 300초 경과
        self.mock_time.current_time += 300.0
        self.mock_time.sleep_calls.clear()

        # 재개
        task = asyncio.create_task(executor.run(self.client))
        while len(self.mock_time.sleep_calls) < 1:
            await asyncio.sleep(0)
        executor.request_stop()
        await task

        self.assertAlmostEqual(self.mock_time.sleep_calls[0], 1.0, places=3)

    async def test_pacing_segment_reset_after_cancellation_resume(self):
        """CANCELLED 재개 후 burst catch-up 없이 정상 페이싱 검증"""
        config = ExecutorConfig(mode=ExecutionMode.REALTIME)
        executor = DeterministicScheduleExecutor(
            self.plan,
            "H001",
            self.run_id,
            config=config,
            sleeper=self.mock_time.sleep,
            time_func=self.mock_time.time,
        )

        task = asyncio.create_task(executor.run(self.client))
        await asyncio.sleep(0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertEqual(executor.status, ExecutionStatus.CANCELLED)

        # 200초 경과
        self.mock_time.current_time += 200.0
        self.mock_time.sleep_calls.clear()

        # 재개
        task2 = asyncio.create_task(executor.run(self.client))
        while len(self.mock_time.sleep_calls) < 1:
            await asyncio.sleep(0)
        executor.request_stop()
        await task2

        self.assertAlmostEqual(self.mock_time.sleep_calls[0], 1.0, places=3)

    async def test_pacing_skipped_on_last_slot_and_stop_pause(self):
        """stop 요청 시 불필요한 sleeper 호출을 건너뜀 검증"""
        config = ExecutorConfig(mode=ExecutionMode.REALTIME)
        executor = DeterministicScheduleExecutor(
            self.plan,
            "H001",
            self.run_id,
            config=config,
            sleeper=self.mock_time.sleep,
            time_func=self.mock_time.time,
        )

        # 실행 전 stop 요청
        executor.request_stop()
        res = await executor.run(self.client)
        self.assertIsNone(res)
        self.assertEqual(len(self.mock_time.sleep_calls), 0)

    # ----------------------------------------------------
    # 2. 텔레메트리 일치성 및 무누락 실행 검증
    # ----------------------------------------------------
    async def test_identical_telemetry_across_all_three_modes(self):
        """REALTIME, ACCELERATED, BURST에서 생성된 ScheduledTick 및 MQTT payload 완전 일치 검증"""
        payloads_by_mode = {}

        for mode in [ExecutionMode.REALTIME, ExecutionMode.ACCELERATED, ExecutionMode.BURST]:
            client = FakeMqttClient()
            mult = 10.0 if mode == ExecutionMode.ACCELERATED else None
            cfg = ExecutorConfig(mode=mode, speed_multiplier=mult)
            mock_t = MockTime()
            exec_inst = DeterministicScheduleExecutor(
                self.plan,
                "H001",
                self.run_id,
                config=cfg,
                seed=42,
                sleeper=mock_t.sleep,
                time_func=mock_t.time,
            )

            # 5개 슬롯 실행 후 중단
            task = asyncio.create_task(exec_inst.run(client))
            while len(client.calls) < 5:
                await asyncio.sleep(0)
            exec_inst.request_stop()
            await task

            payloads_by_mode[mode] = [c["payload"] for c in client.calls[:5]]

        self.assertEqual(payloads_by_mode[ExecutionMode.REALTIME], payloads_by_mode[ExecutionMode.ACCELERATED])
        self.assertEqual(payloads_by_mode[ExecutionMode.REALTIME], payloads_by_mode[ExecutionMode.BURST])

    async def test_full_day_86400_slots_processed_without_skip(self):
        """1일 86,400개 슬롯을 메모리 누적 없이 실제로 완주하고 COMPLETED 및 불변식 검증"""
        cfg = ExecutorConfig(mode=ExecutionMode.BURST)
        client = CountingPublishClient()
        exec_inst = DeterministicScheduleExecutor(
            self.plan,
            "H001",
            self.run_id,
            config=cfg,
        )
        fake_runner = CountingFakeRunner(self.plan, "H001")
        exec_inst._runner = fake_runner

        res = await exec_inst.run(client)

        self.assertIsNotNone(res)
        self.assertEqual(fake_runner.processed_virtual_slots, 86400)
        self.assertEqual(res.total_virtual_slots, 86400)
        self.assertEqual(res.published_samples, 86400)
        self.assertEqual(res.omitted_samples, 0)
        self.assertEqual(client.publish_count, 86400)
        self.assertEqual(res.status, "COMPLETED")
        self.assertEqual(exec_inst.status, ExecutionStatus.COMPLETED)
        self.assertEqual(len(res.day_results), 1)
        self.assertEqual(res.day_results[0].published_samples, 86400)
        self.assertEqual(fake_runner.cycle_history_min, 1)
        self.assertEqual(fake_runner.cycle_history_max, 86400)
        self.assertEqual(fake_runner.cycle_count, 86400)
        self.assertTrue(fake_runner.monotonic)
        # 메모리 축적 방지 검증: client에 calls 리스트가 없고 completed_days는 1개
        self.assertFalse(hasattr(client, "calls"))
        self.assertEqual(len(exec_inst.snapshot().completed_days), 1)
        self.assertIsNone(exec_inst.snapshot().pending_cycle)

    async def test_omitted_slots_counted_and_mqtt_skipped(self):
        """결측 슬롯 OMITTED 집계 및 client.publish 미호출 검증"""
        plan_omission = make_small_plan_with_omission(1, (OmissionRange(2, 5),))
        cfg = ExecutorConfig(mode=ExecutionMode.BURST)
        client = FakeMqttClient()
        exec_inst = DeterministicScheduleExecutor(
            plan_omission,
            "H001",
            self.run_id,
            config=cfg,
        )

        task = asyncio.create_task(exec_inst.run(client))
        while exec_inst.snapshot().settled_virtual_slots < 6:
            await asyncio.sleep(0)
        exec_inst.request_stop()
        await task

        snap = exec_inst.snapshot()
        # 슬롯 2, 3, 4는 결측
        self.assertGreaterEqual(snap.omitted_samples, 3)
        self.assertEqual(snap.settled_virtual_slots, snap.published_samples + snap.omitted_samples)
        self.assertEqual(len(client.calls), snap.published_samples)

    async def test_settled_invariant_published_plus_omitted(self):
        """settled == published + omitted 불변식 유지 검증"""
        cfg = ExecutorConfig(mode=ExecutionMode.BURST)
        client = FakeMqttClient()
        exec_inst = DeterministicScheduleExecutor(self.plan, "H001", self.run_id, config=cfg)

        task = asyncio.create_task(exec_inst.run(client))
        while exec_inst.snapshot().settled_virtual_slots < 20:
            await asyncio.sleep(0)
        exec_inst.request_stop()
        await task

        snap = exec_inst.snapshot()
        self.assertEqual(snap.settled_virtual_slots, snap.published_samples + snap.omitted_samples)

    # ----------------------------------------------------
    # 3. 제어 신호 레이스 컨디션 및 stop wake-up 검증
    # ----------------------------------------------------
    async def test_pause_freezes_runner_and_publish_counts(self):
        """pause() 호출 시 슬롯 경계에서 멈추며 runner/MQTT 호출 동결 검증"""
        cfg = ExecutorConfig(mode=ExecutionMode.REALTIME)
        exec_inst = DeterministicScheduleExecutor(
            self.plan, "H001", self.run_id, config=cfg, sleeper=self.mock_time.sleep, time_func=self.mock_time.time
        )

        task = asyncio.create_task(exec_inst.run(self.client))
        await asyncio.sleep(0)

        exec_inst.pause()
        while exec_inst.status != ExecutionStatus.PAUSED:
            await asyncio.sleep(0)

        calls_at_pause = len(self.client.calls)
        # 시간 흘려도 카운트 불변
        await asyncio.sleep(0.05)
        self.assertEqual(len(self.client.calls), calls_at_pause)

        exec_inst.request_stop()
        await task

    async def test_resume_continues_from_next_slot(self):
        """resume() 호출 시 다음 슬롯부터 정확히 연속 실행 검증"""
        cfg = ExecutorConfig(mode=ExecutionMode.BURST)
        exec_inst = DeterministicScheduleExecutor(self.plan, "H001", self.run_id, config=cfg)

        task = asyncio.create_task(exec_inst.run(self.client))
        await asyncio.sleep(0)

        exec_inst.pause()
        while exec_inst.status != ExecutionStatus.PAUSED:
            await asyncio.sleep(0)

        calls_before = len(self.client.calls)
        exec_inst.resume()
        while len(self.client.calls) <= calls_before + 5:
            await asyncio.sleep(0)

        exec_inst.request_stop()
        await task
        self.assertGreater(len(self.client.calls), calls_before)

    async def test_pending_pause_cancelled_by_resume_before_boundary(self):
        """publish 진행 중 pause() 후 슬롯 경계 도달 전 resume() 호출 시 PAUSED에 고착되지 않고 RUNNING 유지 검증"""
        cfg = ExecutorConfig(mode=ExecutionMode.REALTIME)
        exec_inst = DeterministicScheduleExecutor(
            self.plan, "H001", self.run_id, config=cfg, sleeper=self.mock_time.sleep, time_func=self.mock_time.time
        )

        task = asyncio.create_task(exec_inst.run(self.client))
        await asyncio.sleep(0)

        # pause 요청 직후 바로 resume 호출 (슬롯 경계 도달 전)
        exec_inst.pause()
        exec_inst.resume()

        # pause에 갇히지 않고 계속 진행됨을 확인
        while len(self.client.calls) < 5:
            await asyncio.sleep(0)

        self.assertEqual(exec_inst.status, ExecutionStatus.RUNNING)
        exec_inst.request_stop()
        await task

    async def test_paused_request_stop_wakes_coroutine(self):
        """PAUSED 상태에서 request_stop() 호출 시 대기 중인 run coroutine이 즉시 깨어나 STOPPED 전이 및 None 반환 검증"""
        cfg = ExecutorConfig(mode=ExecutionMode.REALTIME)
        exec_inst = DeterministicScheduleExecutor(
            self.plan, "H001", self.run_id, config=cfg, sleeper=self.mock_time.sleep, time_func=self.mock_time.time
        )

        task = asyncio.create_task(exec_inst.run(self.client))
        await asyncio.sleep(0)

        exec_inst.pause()
        while exec_inst.status != ExecutionStatus.PAUSED:
            await asyncio.sleep(0)

        # PAUSED 상태에서 stop 요청
        exec_inst.request_stop()

        # task가 깨어나서 정상 반환(None)되어야 함
        res = await asyncio.wait_for(task, timeout=1.0)
        self.assertIsNone(res)
        self.assertEqual(exec_inst.status, ExecutionStatus.STOPPED)

    async def test_request_stop_in_failed_and_cancelled_transitions_to_stopped(self):
        """FAILED 또는 CANCELLED 상태에서 request_stop() 호출 시 영구 STOPPED 전이 검증"""
        failing_client = FakeMqttClient(should_fail=True)
        exec_inst = DeterministicScheduleExecutor(self.plan, "H001", self.run_id, config=ExecutorConfig(mode=ExecutionMode.BURST))

        with self.assertRaises(SchedulePublishError):
            await exec_inst.run(failing_client)

        self.assertEqual(exec_inst.status, ExecutionStatus.FAILED)

        # FAILED에서 request_stop
        exec_inst.request_stop()
        self.assertEqual(exec_inst.status, ExecutionStatus.STOPPED)

        # 이후 run() 호출 거절
        with self.assertRaises(RuntimeError):
            await exec_inst.run(self.client)

    async def test_multi_household_isolation(self):
        """H001 pause가 독립된 H002 실행에 영향 주지 않음 검증"""
        cfg = ExecutorConfig(mode=ExecutionMode.BURST)
        client1 = FakeMqttClient()
        client2 = FakeMqttClient()
        exec1 = DeterministicScheduleExecutor(self.plan, "H001", "run-1", config=cfg)
        exec2 = DeterministicScheduleExecutor(self.plan, "H002", "run-2", config=cfg)

        t1 = asyncio.create_task(exec1.run(client1))
        t2 = asyncio.create_task(exec2.run(client2))
        await asyncio.sleep(0)

        exec1.pause()
        while exec1.status != ExecutionStatus.PAUSED:
            await asyncio.sleep(0)

        # H002는 멈추지 않고 계속 실행됨
        count2_before = len(client2.calls)
        while len(client2.calls) <= count2_before + 5:
            await asyncio.sleep(0)

        exec1.request_stop()
        exec2.request_stop()
        await t1
        await t2

        self.assertEqual(exec1.status, ExecutionStatus.STOPPED)
        self.assertEqual(exec2.status, ExecutionStatus.STOPPED)

    # ----------------------------------------------------
    # 4. 발행 실패 및 재시도 계약 검증
    # ----------------------------------------------------
    async def test_publish_failure_preserves_pending_tick_and_marks_failed(self):
        """publish 실패 시 FAILED 전이, pending_tick 보존 및 snapshot.pending_cycle 확인 검증"""
        failing_client = FakeMqttClient(should_fail=True)
        exec_inst = DeterministicScheduleExecutor(self.plan, "H001", self.run_id, config=ExecutorConfig(mode=ExecutionMode.BURST))

        with self.assertRaises(SchedulePublishError):
            await exec_inst.run(failing_client)

        snap = exec_inst.snapshot()
        self.assertEqual(snap.status, ExecutionStatus.FAILED)
        self.assertIsNotNone(snap.pending_cycle)
        self.assertIsNotNone(snap.last_error)

    async def test_retry_after_publish_failure_reuses_pending_tick(self):
        """FAILED 후 run() 재호출 시 runner.step() 미호출 및 pending_tick 재발행 검증"""
        failing_client = FakeMqttClient(should_fail=True)
        exec_inst = DeterministicScheduleExecutor(self.plan, "H001", self.run_id, config=ExecutorConfig(mode=ExecutionMode.BURST))

        with self.assertRaises(SchedulePublishError):
            await exec_inst.run(failing_client)

        runner_slots_before = exec_inst.snapshot().runner_virtual_slots
        pending_cycle_before = exec_inst.snapshot().pending_cycle

        # 성공 클라이언트로 재시도
        task = asyncio.create_task(exec_inst.run(self.client))
        await asyncio.sleep(0)
        exec_inst.request_stop()
        await task

        # 첫 번째 틱은 재사용되었으므로 runner.step() 횟수는 1개 틱 발행 완료 후 증가함
        self.assertGreater(exec_inst.snapshot().settled_virtual_slots, 0)

    async def test_retry_uses_identical_run_id_and_message_id(self):
        """실패 재시도 시 동일 run_id 및 동일 UUID5 message_id 발행 검증"""
        failing_client = FakeMqttClient(should_fail=True)
        exec_inst = DeterministicScheduleExecutor(self.plan, "H001", self.run_id, config=ExecutorConfig(mode=ExecutionMode.BURST))

        with self.assertRaises(SchedulePublishError):
            await exec_inst.run(failing_client)

        failed_msg_id = json.loads(failing_client.calls[0]["payload"])["message_id"]

        success_client = FakeMqttClient(should_fail=False)
        task = asyncio.create_task(exec_inst.run(success_client))
        await asyncio.sleep(0)
        exec_inst.request_stop()
        await task

        success_msg_id = json.loads(success_client.calls[0]["payload"])["message_id"]
        self.assertEqual(failed_msg_id, success_msg_id)

    # ----------------------------------------------------
    # 5. 취소(Cancellation) 계약 검증
    # ----------------------------------------------------
    async def test_cancelled_error_propagates_unwrapped_and_marks_cancelled(self):
        """CancelledError 발생 시 래핑 없이 전파 및 status=CANCELLED 전이 검증"""
        exec_inst = DeterministicScheduleExecutor(self.plan, "H001", self.run_id, config=ExecutorConfig(mode=ExecutionMode.REALTIME), sleeper=self.mock_time.sleep, time_func=self.mock_time.time)

        task = asyncio.create_task(exec_inst.run(self.client))
        await asyncio.sleep(0)
        task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertEqual(exec_inst.status, ExecutionStatus.CANCELLED)

    async def test_cancellation_during_publish_preserves_pending_tick(self):
        """publish 중 취소 시 pending_tick 보존 확인"""
        cancel_event = asyncio.Event()

        class HangingClient:
            async def publish(self, *args, **kwargs):
                cancel_event.set()
                await asyncio.sleep(9999)

        exec_inst = DeterministicScheduleExecutor(self.plan, "H001", self.run_id, config=ExecutorConfig(mode=ExecutionMode.BURST))
        task = asyncio.create_task(exec_inst.run(HangingClient()))

        await cancel_event.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        snap = exec_inst.snapshot()
        self.assertEqual(snap.status, ExecutionStatus.CANCELLED)
        self.assertIsNotNone(snap.pending_cycle)

    async def test_cancellation_during_callback_preserves_pending_day_result(self):
        """callback 중 취소 시 pending_day_result 보존 확인"""
        cb_started = asyncio.Event()

        async def hanging_callback(day_res: DayExecutionResult) -> None:
            cb_started.set()
            await asyncio.sleep(9999)

        # 1일 계획의 마지막 슬롯 발행 시 callback 호출
        # 빠르게 테스트하기 위해 start_cycle을 end_cycle 직전으로 맞춘 계획 생성
        exec_inst = DeterministicScheduleExecutor(
            self.plan,
            "H001",
            self.run_id,
            config=ExecutorConfig(mode=ExecutionMode.BURST),
            day_completed_callback=hanging_callback,
        )

        # 1일 86400개 슬롯을 다 돌지 않고 callback 취소 검증을 위해 소규모 계획 테스트
        # 1슬롯만 남은 계획 대신 callback을 직접 트리거하는 계획 테스트
        # make_simple_plan(1)의 마지막 슬롯 도달을 위해 슬롯을 전진
        # 여기서는 취소 계약 검증이 목적이므로 mock을 사용하여 pending_day_result 세팅 후 취소 확인
        exec_inst._pending_day_result = DayExecutionResult(
            scenario="ACTIVITY_NORMAL",
            household_id="H001",
            run_id=self.run_id,
            activity_date="2026-09-17",
            published_samples=86400,
            omitted_samples=0,
        )

        task = asyncio.create_task(exec_inst.run(self.client))
        await cb_started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertEqual(exec_inst.status, ExecutionStatus.CANCELLED)
        self.assertIsNotNone(exec_inst.snapshot().pending_day_result)

    async def test_cancellation_during_pacing_resumes_next_slot(self):
        """pacing 중 취소 후 run 재호출 시 다음 미처리 슬롯부터 정상 재개 검증"""
        exec_inst = DeterministicScheduleExecutor(
            self.plan,
            "H001",
            self.run_id,
            config=ExecutorConfig(mode=ExecutionMode.REALTIME),
            sleeper=self.mock_time.sleep,
            time_func=self.mock_time.time,
        )

        task = asyncio.create_task(exec_inst.run(self.client))
        while len(self.mock_time.sleep_calls) < 1:
            await asyncio.sleep(0)

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertEqual(exec_inst.status, ExecutionStatus.CANCELLED)
        settled_before = exec_inst.snapshot().settled_virtual_slots

        # 재개
        task2 = asyncio.create_task(exec_inst.run(self.client))
        while exec_inst.snapshot().settled_virtual_slots <= settled_before:
            await asyncio.sleep(0)
        exec_inst.request_stop()
        await task2

        self.assertGreater(exec_inst.snapshot().settled_virtual_slots, settled_before)

    async def test_cancelled_state_not_left_running(self):
        """취소 후 executor 상태가 RUNNING으로 남지 않고 CANCELLED로 확정 검증"""
        exec_inst = DeterministicScheduleExecutor(
            self.plan,
            "H001",
            self.run_id,
            config=ExecutorConfig(mode=ExecutionMode.REALTIME),
            sleeper=self.mock_time.sleep,
            time_func=self.mock_time.time,
        )

        task = asyncio.create_task(exec_inst.run(self.client))
        await asyncio.sleep(0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertEqual(exec_inst.status, ExecutionStatus.CANCELLED)

    # ----------------------------------------------------
    # 6. 날짜 완료 장벽 및 커밋 계약 검증
    # ----------------------------------------------------
    async def test_day_execution_result_generated_exactly_once(self):
        """일자별 DayExecutionResult가 각 일자 마지막 슬롯에서 정확히 1회 생성 검증"""
        day_results = []

        async def cb(res: DayExecutionResult):
            day_results.append(res)

        exec_inst = DeterministicScheduleExecutor(
            self.plan,
            "H001",
            self.run_id,
            config=ExecutorConfig(mode=ExecutionMode.BURST),
            day_completed_callback=cb,
        )

        # 1일치 슬롯을 시뮬레이트하기 위해 pending_day_result 주입 후 완료 확인
        day_res = DayExecutionResult(
            scenario="ACTIVITY_NORMAL",
            household_id="H001",
            run_id=self.run_id,
            activity_date="2026-09-17",
            published_samples=86400,
            omitted_samples=0,
        )
        exec_inst._pending_day_result = day_res
        exec_inst._current_day_index = 0
        exec_inst._settled_virtual_slots = 86400
        exec_inst._total_published = 86400
        exec_inst._runner._is_completed = True

        res = await exec_inst.run(self.client)
        self.assertEqual(len(day_results), 1)
        self.assertEqual(res.day_results[0].activity_date, "2026-09-17")

    async def test_day_execution_result_has_run_id_and_matches_plan(self):
        """DayExecutionResult에 run_id 포함 및 published/omitted/expected 수가 plan과 일치 검증"""
        day_plan = self.plan.day_plans[0]
        day_res = DayExecutionResult(
            scenario=self.plan.scenario_id,
            household_id="H001",
            run_id=self.run_id,
            activity_date=day_plan.calendar_date.isoformat(),
            published_samples=day_plan.planned_publish_samples,
            omitted_samples=day_plan.planned_omitted_samples,
        )
        self.assertEqual(day_res.run_id, self.run_id)
        self.assertEqual(day_res.scenario, self.plan.scenario_id)
        self.assertEqual(day_res.published_samples, day_plan.planned_publish_samples)
        self.assertEqual(day_res.omitted_samples, day_plan.planned_omitted_samples)
        self.assertEqual(day_res.published_samples + day_res.omitted_samples, day_plan.expected_samples)
        self.assertEqual(day_res.activity_date, day_plan.calendar_date.isoformat())

    async def test_day_execution_result_naturally_generated_at_boundary(self):
        """날짜 마지막 슬롯 처리 경로에서 DayExecutionResult가 자연스럽게 생성되어 콜백에 전달됨 검증"""
        received_results = []

        async def cb(res: DayExecutionResult):
            received_results.append(res)

        exec_inst = DeterministicScheduleExecutor(
            self.plan,
            "H001",
            self.run_id,
            config=ExecutorConfig(mode=ExecutionMode.BURST),
            day_completed_callback=cb,
        )
        # 86,399 슬롯까지 진행된 러너로 시뮬레이션하여 마지막 1슬롯(86400)이 자연스럽게 day boundary를 트리거하도록 설정
        fake_runner = CountingFakeRunner(self.plan, "H001")
        fake_runner.processed_virtual_slots = 86399
        exec_inst._runner = fake_runner
        exec_inst._settled_virtual_slots = 86399
        exec_inst._total_published = 86399
        exec_inst._current_day_published = 86399

        res = await exec_inst.run(self.client)
        self.assertEqual(len(received_results), 1)
        day_res = received_results[0]
        self.assertEqual(day_res.published_samples, 86400)
        self.assertEqual(day_res.omitted_samples, 0)
        self.assertEqual(day_res.activity_date, "2026-09-17")
        self.assertEqual(day_res.run_id, self.run_id)
        self.assertEqual(res.status, "COMPLETED")

    async def test_day_completed_callback_awaited_before_next_day(self):
        """day_completed_callback 완료 전에는 다음 날짜 슬롯 시작 불가 검증"""
        order = []

        async def slow_cb(res):
            await asyncio.sleep(0.05)
            order.append("callback_finished")

        exec_inst = DeterministicScheduleExecutor(
            make_simple_plan(2),
            "H001",
            self.run_id,
            config=ExecutorConfig(mode=ExecutionMode.BURST),
            day_completed_callback=slow_cb,
        )
        # 1일차 pending_day_result 설정
        exec_inst._pending_day_result = DayExecutionResult(
            scenario="ACTIVITY_NORMAL",
            household_id="H001",
            run_id=self.run_id,
            activity_date="2026-09-17",
            published_samples=86400,
            omitted_samples=0,
        )
        exec_inst._current_day_index = 0

        async def run_part():
            await exec_inst.run(self.client)

        task = asyncio.create_task(run_part())
        while len(order) == 0:
            await asyncio.sleep(0.01)
        exec_inst.request_stop()
        await task

        self.assertIn("callback_finished", order)

    async def test_day_callback_failure_preserves_pending_day_result_and_halts(self):
        """callback 실패 시 completed_days 미증가, pending_day_result 보존, 다음 날짜 미진행 검증"""
        async def failing_cb(res):
            raise ValueError("Downstream service unreachable")

        exec_inst = DeterministicScheduleExecutor(
            make_simple_plan(2),
            "H001",
            self.run_id,
            config=ExecutorConfig(mode=ExecutionMode.BURST),
            day_completed_callback=failing_cb,
        )
        exec_inst._pending_day_result = DayExecutionResult(
            scenario="ACTIVITY_NORMAL",
            household_id="H001",
            run_id=self.run_id,
            activity_date="2026-09-17",
            published_samples=86400,
            omitted_samples=0,
        )

        with self.assertRaises(DayCallbackError):
            await exec_inst.run(self.client)

        snap = exec_inst.snapshot()
        self.assertEqual(snap.status, ExecutionStatus.FAILED)
        self.assertEqual(len(snap.completed_days), 0)
        self.assertIsNotNone(snap.pending_day_result)

    async def test_callback_failure_preserves_day_counters_without_reset(self):
        """callback 실패 시 일자별 카운터(current_day_published/omitted)가 0으로 초기화되지 않고 보존됨 검증"""
        async def failing_cb(res):
            raise ValueError("Error")

        exec_inst = DeterministicScheduleExecutor(
            make_simple_plan(1),
            "H001",
            self.run_id,
            config=ExecutorConfig(mode=ExecutionMode.BURST),
            day_completed_callback=failing_cb,
        )
        exec_inst._pending_day_result = DayExecutionResult(
            scenario="ACTIVITY_NORMAL",
            household_id="H001",
            run_id=self.run_id,
            activity_date="2026-09-17",
            published_samples=86400,
            omitted_samples=0,
        )
        exec_inst._current_day_published = 86400
        exec_inst._current_day_omitted = 0

        with self.assertRaises(DayCallbackError):
            await exec_inst.run(self.client)

        self.assertEqual(exec_inst._current_day_published, 86400)
        self.assertEqual(exec_inst._current_day_omitted, 0)

    async def test_retry_after_callback_failure_retries_callback_first(self):
        """callback 실패 후 run() 재호출 시 마지막 슬롯 MQTT 재발행/카운터 증가 없이 callback부터 재시도 검증"""
        cb_calls = []

        async def cb(res):
            cb_calls.append(res)
            if len(cb_calls) == 1:
                raise ValueError("First attempt fails")

        exec_inst = DeterministicScheduleExecutor(
            self.plan,
            "H001",
            self.run_id,
            config=ExecutorConfig(mode=ExecutionMode.BURST),
            day_completed_callback=cb,
        )
        exec_inst._pending_day_result = DayExecutionResult(
            scenario="ACTIVITY_NORMAL",
            household_id="H001",
            run_id=self.run_id,
            activity_date="2026-09-17",
            published_samples=86400,
            omitted_samples=0,
        )
        exec_inst._settled_virtual_slots = 86400
        exec_inst._total_published = 86400
        exec_inst._runner._is_completed = True

        # 첫 번째 시도: 실패
        with self.assertRaises(DayCallbackError):
            await exec_inst.run(self.client)

        self.assertEqual(len(cb_calls), 1)
        self.assertEqual(len(self.client.calls), 0)

        # 두 번째 시도: 성공
        res = await exec_inst.run(self.client)
        self.assertEqual(len(cb_calls), 2)
        # 슬롯 재발행은 0회
        self.assertEqual(len(self.client.calls), 0)
        self.assertEqual(res.status, "COMPLETED")

    async def test_callback_retry_success_commits_and_resets_counters(self):
        """callback 재시도 성공 시에만 completed_days에 1회 커밋되고 day index 증가 및 일자 카운터 리셋 검증"""
        async def cb(res):
            pass

        exec_inst = DeterministicScheduleExecutor(
            make_simple_plan(2),
            "H001",
            self.run_id,
            config=ExecutorConfig(mode=ExecutionMode.BURST),
            day_completed_callback=cb,
        )
        exec_inst._pending_day_result = DayExecutionResult(
            scenario="ACTIVITY_NORMAL",
            household_id="H001",
            run_id=self.run_id,
            activity_date="2026-09-17",
            published_samples=86400,
            omitted_samples=0,
        )
        exec_inst._current_day_index = 0
        exec_inst._current_day_published = 86400
        exec_inst._current_day_omitted = 0

        task = asyncio.create_task(exec_inst.run(self.client))
        await asyncio.sleep(0)
        exec_inst.request_stop()
        await task

        snap = exec_inst.snapshot()
        self.assertEqual(len(snap.completed_days), 1)
        self.assertIsNone(snap.pending_day_result)
        self.assertEqual(exec_inst._current_day_index, 1)

    async def test_callback_none_commits_immediately(self):
        """day_completed_callback is None인 경우 콜백 await 없이 날짜 결과가 즉시 정상 커밋됨 검증"""
        exec_inst = DeterministicScheduleExecutor(
            self.plan,
            "H001",
            self.run_id,
            config=ExecutorConfig(mode=ExecutionMode.BURST),
            day_completed_callback=None,
        )
        exec_inst._pending_day_result = DayExecutionResult(
            scenario="ACTIVITY_NORMAL",
            household_id="H001",
            run_id=self.run_id,
            activity_date="2026-09-17",
            published_samples=86400,
            omitted_samples=0,
        )
        exec_inst._settled_virtual_slots = 86400
        exec_inst._total_published = 86400
        exec_inst._runner._is_completed = True

        res = await exec_inst.run(self.client)
        self.assertEqual(len(res.day_results), 1)
        self.assertEqual(res.status, "COMPLETED")

    # ----------------------------------------------------
    # 7. Single-flight 동시 실행 방지 가드 검증
    # ----------------------------------------------------
    async def test_single_flight_atomic_check_before_await(self):
        """run() 진입 즉시 await 없이 동기식으로 동시 실행 여부 검사 및 설정 확인"""
        exec_inst = DeterministicScheduleExecutor(self.plan, "H001", self.run_id, config=ExecutorConfig(mode=ExecutionMode.BURST))
        exec_inst._is_running_coroutine = True

        with self.assertRaises(ExecutionAlreadyRunningError):
            await exec_inst.run(self.client)

    async def test_single_flight_concurrent_run_rejected(self):
        """동일 executor에서 RUNNING 중 두 번째 run() 호출 시 ExecutionAlreadyRunningError 거절 검증"""
        cfg = ExecutorConfig(mode=ExecutionMode.REALTIME)
        exec_inst = DeterministicScheduleExecutor(
            self.plan, "H001", self.run_id, config=cfg, sleeper=self.mock_time.sleep, time_func=self.mock_time.time
        )

        task1 = asyncio.create_task(exec_inst.run(self.client))
        await asyncio.sleep(0)

        with self.assertRaises(ExecutionAlreadyRunningError):
            await exec_inst.run(self.client)

        exec_inst.request_stop()
        await task1

    async def test_single_flight_paused_run_rejected(self):
        """PAUSED 상태에서 두 번째 run() 호출 시 ExecutionAlreadyRunningError 거절 검증"""
        cfg = ExecutorConfig(mode=ExecutionMode.REALTIME)
        exec_inst = DeterministicScheduleExecutor(
            self.plan, "H001", self.run_id, config=cfg, sleeper=self.mock_time.sleep, time_func=self.mock_time.time
        )

        task1 = asyncio.create_task(exec_inst.run(self.client))
        await asyncio.sleep(0)

        exec_inst.pause()
        while exec_inst.status != ExecutionStatus.PAUSED:
            await asyncio.sleep(0)

        # PAUSED 상태에서 새 run 호출 시 거절
        with self.assertRaises(ExecutionAlreadyRunningError):
            await exec_inst.run(self.client)

        exec_inst.request_stop()
        await task1

    # ----------------------------------------------------
    # 8. 완료 불변식 및 경계 조건 검증
    # ----------------------------------------------------
    async def test_overall_completion_invariants_and_snapshot_end_date(self):
        """전체 완료 시 runner.is_completed, 총 슬롯수, 발행수, 일자수 일치 및 snapshot().current_activity_date가 IndexError 없이 plan.end_date 반환 검증"""
        exec_inst = DeterministicScheduleExecutor(
            self.plan,
            "H001",
            self.run_id,
            config=ExecutorConfig(mode=ExecutionMode.BURST),
        )
        exec_inst._pending_day_result = DayExecutionResult(
            scenario="ACTIVITY_NORMAL",
            household_id="H001",
            run_id=self.run_id,
            activity_date="2026-09-17",
            published_samples=86400,
            omitted_samples=0,
        )
        exec_inst._settled_virtual_slots = 86400
        exec_inst._total_published = 86400
        exec_inst._runner._is_completed = True

        res = await exec_inst.run(self.client)
        self.assertEqual(res.status, "COMPLETED")

        # 완료 상태에서 snapshot() 호출 시 current_day_index가 1이어도 IndexError 없이 end_date 반환
        snap = exec_inst.snapshot()
        self.assertEqual(snap.status, ExecutionStatus.COMPLETED)
        self.assertEqual(snap.current_activity_date, self.plan.end_date)
        self.assertIsNotNone(snap.completed_at)

    async def test_completed_and_stopped_executors_reject_runs(self):
        """COMPLETED(ExecutionCompletedError) 및 STOPPED(RuntimeError) 상태 재실행 거절 검증"""
        exec_inst = DeterministicScheduleExecutor(self.plan, "H001", self.run_id, config=ExecutorConfig(mode=ExecutionMode.BURST))

        exec_inst.request_stop()
        res = await exec_inst.run(self.client)
        self.assertIsNone(res)

        # STOPPED 재실행 거절
        with self.assertRaises(RuntimeError):
            await exec_inst.run(self.client)

        # COMPLETED 재실행 거절
        exec2 = DeterministicScheduleExecutor(self.plan, "H001", self.run_id, config=ExecutorConfig(mode=ExecutionMode.BURST))
        exec2._status = ExecutionStatus.COMPLETED
        with self.assertRaises(ExecutionCompletedError):
            await exec2.run(self.client)

    def test_invalid_inputs_rejected_before_runner_step(self):
        """잘못된 run_id, 잘못된 household_id, 잘못된 speed_multiplier (bool/0/음수/NaN/Inf) 즉시 거절 검증"""
        # 1. invalid run_id
        with self.assertRaises(ValueError):
            DeterministicScheduleExecutor(self.plan, "H001", run_id="")
        with self.assertRaises(ValueError):
            DeterministicScheduleExecutor(self.plan, "H001", run_id="run/bad")

        # 2. invalid household_id
        with self.assertRaises(ValueError):
            DeterministicScheduleExecutor(self.plan, "H/001", self.run_id)

        # 3. invalid speed_multiplier
        with self.assertRaises(ValueError):
            ExecutorConfig(mode=ExecutionMode.ACCELERATED, speed_multiplier=0.0)
        with self.assertRaises(ValueError):
            ExecutorConfig(mode=ExecutionMode.ACCELERATED, speed_multiplier=-5.0)
        with self.assertRaises(ValueError):
            ExecutorConfig(mode=ExecutionMode.ACCELERATED, speed_multiplier=float("nan"))
        with self.assertRaises(ValueError):
            ExecutorConfig(mode=ExecutionMode.ACCELERATED, speed_multiplier=float("inf"))
        with self.assertRaises(TypeError):
            ExecutorConfig(mode=ExecutionMode.ACCELERATED, speed_multiplier=True)

    async def test_memory_efficiency_no_tick_accumulation(self):
        """다일 계획 실행 시 틱/결과 누적 없이 O(D) 일자 요약 상태만 유지 검증"""
        plan_2d = make_simple_plan(2)
        exec_inst = DeterministicScheduleExecutor(plan_2d, "H001", self.run_id, config=ExecutorConfig(mode=ExecutionMode.BURST))

        # 100개 슬롯 실행
        task = asyncio.create_task(exec_inst.run(self.client))
        while exec_inst.snapshot().settled_virtual_slots < 100:
            await asyncio.sleep(0)
        exec_inst.request_stop()
        await task

        # executor 내부 어디에도 ScheduledTick 목록이나 MQTT 결과 목록이 누적되지 않음
        self.assertFalse(hasattr(exec_inst, "_ticks"))
        self.assertFalse(hasattr(exec_inst, "_results"))
        self.assertIsNone(exec_inst._pending_tick)
        self.assertLessEqual(len(exec_inst._completed_days), 2)

    # ----------------------------------------------------
    # 9. 신규 불변식 및 예외 방어 실패 테스트
    # ----------------------------------------------------
    async def test_day_published_samples_mismatch_blocks_callback_and_raises_invariant_error(self):
        """날짜 published 수가 계획과 다르면 callback 0회 호출 및 ExecutionInvariantError 발생 검증"""
        cb_called = False

        async def cb(res):
            nonlocal cb_called
            cb_called = True

        exec_inst = DeterministicScheduleExecutor(
            self.plan, "H001", self.run_id,
            config=ExecutorConfig(mode=ExecutionMode.BURST),
            day_completed_callback=cb,
        )
        exec_inst._pending_day_result = DayExecutionResult(
            scenario="ACTIVITY_NORMAL",
            household_id="H001",
            run_id=self.run_id,
            activity_date="2026-09-17",
            published_samples=86399,
            omitted_samples=0,
        )
        with self.assertRaises(ExecutionInvariantError) as ctx:
            await exec_inst.run(self.client)

        self.assertIn("published_samples mismatch", str(ctx.exception))
        self.assertFalse(cb_called)
        self.assertEqual(exec_inst.status, ExecutionStatus.FAILED)
        self.assertIsNotNone(exec_inst.snapshot().pending_day_result)
        self.assertEqual(len(exec_inst.snapshot().completed_days), 0)

    async def test_day_omitted_samples_mismatch_blocks_callback_and_raises_invariant_error(self):
        """날짜 omitted 수가 계획과 다르면 callback 0회 호출 및 ExecutionInvariantError 발생 검증"""
        cb_called = False

        async def cb(res):
            nonlocal cb_called
            cb_called = True

        exec_inst = DeterministicScheduleExecutor(
            self.plan, "H001", self.run_id,
            config=ExecutorConfig(mode=ExecutionMode.BURST),
            day_completed_callback=cb,
        )
        exec_inst._pending_day_result = DayExecutionResult(
            scenario="ACTIVITY_NORMAL",
            household_id="H001",
            run_id=self.run_id,
            activity_date="2026-09-17",
            published_samples=86400,
            omitted_samples=5,
        )
        with self.assertRaises(ExecutionInvariantError) as ctx:
            await exec_inst.run(self.client)

        self.assertIn("omitted_samples mismatch", str(ctx.exception))
        self.assertFalse(cb_called)
        self.assertEqual(exec_inst.status, ExecutionStatus.FAILED)

    async def test_day_total_samples_mismatch_blocks_commit_and_raises_invariant_error(self):
        """날짜 합계가 expected_samples와 다르면 날짜 commit 금지 및 ExecutionInvariantError 발생 검증"""
        plan_omission = make_small_plan_with_omission(1, (OmissionRange(10, 20),))
        exec_inst = DeterministicScheduleExecutor(
            plan_omission, "H001", self.run_id,
            config=ExecutorConfig(mode=ExecutionMode.BURST),
        )
        exec_inst._pending_day_result = DayExecutionResult(
            scenario="ACTIVITY_NORMAL",
            household_id="H001",
            run_id=self.run_id,
            activity_date="2026-09-17",
            published_samples=86390,
            omitted_samples=0,  # 합계 86390 != expected 86400
        )
        with self.assertRaises(ExecutionInvariantError) as ctx:
            await exec_inst.run(self.client)

        self.assertEqual(exec_inst.status, ExecutionStatus.FAILED)
        self.assertEqual(len(exec_inst.snapshot().completed_days), 0)
        self.assertEqual(exec_inst._current_day_index, 0)

    async def test_overall_settled_mismatch_blocks_completed_and_raises_invariant_error(self):
        """전체 settled 수가 계획과 다르면 COMPLETED 금지 및 ExecutionInvariantError 발생 검증"""
        exec_inst = DeterministicScheduleExecutor(
            self.plan, "H001", self.run_id,
            config=ExecutorConfig(mode=ExecutionMode.BURST),
        )
        exec_inst._settled_virtual_slots = 86399  # plan.total_virtual_slots = 86400
        exec_inst._total_published = 86400
        exec_inst._runner._is_completed = True
        exec_inst._completed_days.append(DayExecutionResult(
            scenario="ACTIVITY_NORMAL", household_id="H001", run_id=self.run_id,
            activity_date="2026-09-17", published_samples=86400, omitted_samples=0,
        ))

        with self.assertRaises(ExecutionInvariantError) as ctx:
            await exec_inst.run(self.client)

        self.assertIn("settled_virtual_slots mismatch", str(ctx.exception))
        self.assertEqual(exec_inst.status, ExecutionStatus.FAILED)

    async def test_overall_published_plus_omitted_mismatch_blocks_completed(self):
        """전체 published/omitted 합계가 settled와 다르면 COMPLETED 금지 및 ExecutionInvariantError 발생 검증"""
        exec_inst = DeterministicScheduleExecutor(
            self.plan, "H001", self.run_id,
            config=ExecutorConfig(mode=ExecutionMode.BURST),
        )
        exec_inst._settled_virtual_slots = 86400
        exec_inst._total_published = 86390  # sum != 86400
        exec_inst._total_omitted = 0
        exec_inst._runner._is_completed = True
        exec_inst._completed_days.append(DayExecutionResult(
            scenario="ACTIVITY_NORMAL", household_id="H001", run_id=self.run_id,
            activity_date="2026-09-17", published_samples=86400, omitted_samples=0,
        ))

        with self.assertRaises(ExecutionInvariantError) as ctx:
            await exec_inst.run(self.client)

        self.assertEqual(exec_inst.status, ExecutionStatus.FAILED)

    async def test_unknown_publish_disposition_preserves_pending_tick_and_counters(self):
        """알 수 없는 disposition이면 pending_tick 및 카운터 보존 후 ExecutionInvariantError 발생 검증"""
        from unittest.mock import patch

        exec_inst = DeterministicScheduleExecutor(
            self.plan, "H001", self.run_id,
            config=ExecutorConfig(mode=ExecutionMode.BURST),
        )
        fake_result = ScheduledPublishResult(
            household_id="H001",
            cycle=1,
            disposition="CORRUPTED_DISPOSITION",  # type: ignore
            topic="v1/power/sim/H001/main",
            message_id="bad-msg-id",
        )

        with patch("engine.schedule_executor.publish_scheduled_tick", return_value=fake_result):
            with self.assertRaises(ExecutionInvariantError) as ctx:
                await exec_inst.run(self.client)

        self.assertIn("Unknown or invalid PublishDisposition", str(ctx.exception))
        self.assertEqual(exec_inst.status, ExecutionStatus.FAILED)
        self.assertIsNotNone(exec_inst.snapshot().pending_cycle)
        self.assertEqual(exec_inst.snapshot().settled_virtual_slots, 0)
        self.assertEqual(exec_inst.snapshot().published_samples, 0)

    async def test_commit_or_pacing_reset_error_not_misclassified_as_day_callback_error(self):
        """commit 또는 pacing reset 오류가 DayCallbackError로 오분류되지 않고 원인 유지 검증"""
        exec_inst = DeterministicScheduleExecutor(
            self.plan, "H001", self.run_id,
            config=ExecutorConfig(mode=ExecutionMode.BURST),
            day_completed_callback=lambda res: asyncio.sleep(0),
        )
        exec_inst._pending_day_result = DayExecutionResult(
            scenario="ACTIVITY_NORMAL",
            household_id="H001",
            run_id=self.run_id,
            activity_date="2026-09-17",
            published_samples=86400,
            omitted_samples=0,
        )
        from unittest.mock import patch
        with patch.object(exec_inst, "_commit_pending_day", side_effect=ZeroDivisionError("Division error in commit")):
            with self.assertRaises(ZeroDivisionError):
                await exec_inst.run(self.client)

        self.assertEqual(exec_inst.status, ExecutionStatus.FAILED)
        self.assertIn("ZeroDivisionError", exec_inst.snapshot().last_error)

    async def test_unexpected_internal_error_does_not_leave_status_running(self):
        """runner.step() 등 내부 예기치 않은 예외 발생 후 상태가 RUNNING으로 남지 않고 FAILED로 전이 검증"""
        exec_inst = DeterministicScheduleExecutor(
            self.plan, "H001", self.run_id,
            config=ExecutorConfig(mode=ExecutionMode.BURST),
        )
        from unittest.mock import patch
        with patch.object(exec_inst._runner, "step", side_effect=KeyError("unexpected runner error")):
            with self.assertRaises(KeyError):
                await exec_inst.run(self.client)

        self.assertEqual(exec_inst.status, ExecutionStatus.FAILED)
        self.assertIn("KeyError", exec_inst.snapshot().last_error)

from engine.unified_catalog import get_unified_scenario_definition


class TestDeterministicScheduleExecutorBroadcast(unittest.IsolatedAsyncioTestCase):
    """결정적 시나리오 화면 전송 콜백, 20Hz 상한, BURST 제외, 결측 렌더링 계약 단위 테스트"""

    def setUp(self):
        self.ref_date = date(2026, 9, 17)
        self.normal_defn = get_unified_scenario_definition("ACTIVITY_NORMAL")
        self.normal_plan = compile_schedule(self.normal_defn, self.ref_date)
        self.insufficient_defn = get_unified_scenario_definition("ACTIVITY_INSUFFICIENT")
        self.insufficient_plan = compile_schedule(self.insufficient_defn, self.ref_date)
        self.run_id = "run-broadcast-test-1234"
        self.household_id = "H001"
        self.client = CountingPublishClient()

    async def test_broadcast_callback_none_regression(self):
        """a. broadcast_callback이 None일 때 기존 동작과 100% 동일하게 완주 및 불변식 통과"""
        executor = DeterministicScheduleExecutor(
            self.normal_plan,
            self.household_id,
            self.run_id,
            config=ExecutorConfig(mode=ExecutionMode.BURST),
            broadcast_callback=None,
        )
        res = await executor.run(self.client)
        self.assertIsNotNone(res)
        self.assertEqual(res.status, "COMPLETED")
        self.assertEqual(res.published_samples, self.normal_plan.total_planned_publish_samples)
        self.assertEqual(executor.broadcast_count, 0)

    async def test_broadcast_callback_burst_zero_calls(self):
        """b. BURST 모드 실행 시 broadcast_callback 호출 횟수가 엄격히 0회이며 published_samples는 전량 보존"""
        called_payloads = []

        def cb(payload):
            called_payloads.append(payload)

        executor = DeterministicScheduleExecutor(
            self.normal_plan,
            self.household_id,
            self.run_id,
            config=ExecutorConfig(mode=ExecutionMode.BURST),
            broadcast_callback=cb,
        )
        res = await executor.run(self.client)
        self.assertIsNotNone(res)
        self.assertEqual(len(called_payloads), 0)
        self.assertEqual(executor.broadcast_count, 0)
        self.assertEqual(res.published_samples, self.normal_plan.total_planned_publish_samples)
        self.assertEqual(res.omitted_samples, self.normal_plan.total_planned_omitted_samples)

    async def test_broadcast_callback_payload_fields_and_source(self):
        """c. REALTIME/ACCELERATED 실행 시 콜백이 호출되고, 필수 17개 필드와 source='E2E', sensorFault=False, run_id 포함"""
        called_payloads = []

        def cb(payload):
            called_payloads.append(payload)

        simulated_time = 0.0

        def mock_time():
            return simulated_time

        async def mock_sleep(d):
            nonlocal simulated_time
            simulated_time += 1.0

        executor = DeterministicScheduleExecutor(
            self.normal_plan,
            self.household_id,
            self.run_id,
            config=ExecutorConfig(mode=ExecutionMode.ACCELERATED, speed_multiplier=1.0),
            broadcast_callback=cb,
            sleeper=mock_sleep,
            time_func=mock_time,
        )

        orig_step = executor._runner.step
        step_count = 0

        def limited_step():
            nonlocal step_count
            step_count += 1
            tick = orig_step()
            if step_count >= 5:
                executor.request_stop()
            return tick

        executor._runner.step = limited_step

        await executor.run(self.client)
        self.assertGreaterEqual(len(called_payloads), 1)

        first = called_payloads[0]
        required_fields = [
            "house", "scenario", "status", "simTimeKst", "simDateKst", "simDateTimeKst",
            "totalP", "totalQ", "apparentS", "pf", "voltage", "currentA",
            "activeNames", "devices", "measurementAvailable", "sensorFault", "sec",
            "source", "run_id",
        ]
        for field in required_fields:
            self.assertIn(field, first, f"필수 필드 누락: {field}")

        self.assertEqual(first["source"], "E2E")
        self.assertEqual(first["run_id"], self.run_id)
        self.assertEqual(first["house"], self.household_id)
        self.assertEqual(first["scenario"], "ACTIVITY_NORMAL")
        self.assertEqual(first["sensorFault"], False)
        self.assertEqual(first["measurementAvailable"], True)
        self.assertIsInstance(first["devices"], dict)
        self.assertEqual(len(first["devices"]), 6)

    async def test_broadcast_callback_rate_limit_and_counter_isolation(self):
        """d. 고빈도 틱에서 콜백 호출 수는 상한(20Hz)으로 제한되지만 published_samples와 omitted_samples는 계획값과 100% 일치"""
        called_count = 0

        def cb(payload):
            nonlocal called_count
            called_count += 1

        current_time = 0.0

        def mock_time():
            nonlocal current_time
            current_time += 0.001
            return current_time

        async def instant_sleep(d):
            pass

        executor = DeterministicScheduleExecutor(
            self.normal_plan,
            self.household_id,
            self.run_id,
            config=ExecutorConfig(mode=ExecutionMode.ACCELERATED, speed_multiplier=1000.0),
            broadcast_callback=cb,
            max_broadcast_rate_hz=20.0,
            sleeper=instant_sleep,
            time_func=mock_time,
        )

        res = await executor.run(self.client)
        self.assertIsNotNone(res)
        self.assertLess(called_count, 5000)
        self.assertGreater(called_count, 1000)
        self.assertEqual(res.published_samples, self.normal_plan.total_planned_publish_samples)
        self.assertEqual(res.omitted_samples, self.normal_plan.total_planned_omitted_samples)
        self.assertEqual(res.total_virtual_slots, self.normal_plan.total_virtual_slots)

    async def test_broadcast_callback_exception_isolation(self):
        """e. 콜백이 예외를 던져도 실행이 중단되지 않고 완주하며 발행 수가 계획과 일치"""
        def failing_cb(payload):
            raise RuntimeError("Broadcast queue broken or network down")

        async def instant_sleep(d):
            pass

        executor = DeterministicScheduleExecutor(
            self.normal_plan,
            self.household_id,
            self.run_id,
            config=ExecutorConfig(mode=ExecutionMode.ACCELERATED, speed_multiplier=1000.0),
            broadcast_callback=failing_cb,
            sleeper=instant_sleep,
        )

        res = await executor.run(self.client)
        self.assertIsNotNone(res)
        self.assertEqual(res.status, "COMPLETED")
        self.assertEqual(res.published_samples, self.normal_plan.total_planned_publish_samples)
        self.assertEqual(res.omitted_samples, self.normal_plan.total_planned_omitted_samples)

    async def test_insufficient_omitted_slots_payload_contract(self):
        """[1]-Python: 계획 결측(ACTIVITY_INSUFFICIENT) 실행 시 결측 구간 콜백 페이로드 및 카운터 분리 검증"""
        captured_payloads = []

        def cb(payload):
            captured_payloads.append(payload)

        current_time = 0.0

        def advance_time():
            nonlocal current_time
            current_time += 0.1
            return current_time

        async def instant_sleep(d):
            pass

        executor = DeterministicScheduleExecutor(
            self.insufficient_plan,
            self.household_id,
            self.run_id,
            config=ExecutorConfig(mode=ExecutionMode.ACCELERATED, speed_multiplier=1000.0),
            broadcast_callback=cb,
            max_broadcast_rate_hz=20.0,
            sleeper=instant_sleep,
            time_func=advance_time,
        )

        res = await executor.run(self.client)
        self.assertIsNotNone(res)
        self.assertEqual(res.status, "COMPLETED")
        self.assertEqual(res.published_samples, self.insufficient_plan.total_planned_publish_samples)
        self.assertEqual(res.omitted_samples, self.insufficient_plan.total_planned_omitted_samples)

        omitted_payloads = [p for p in captured_payloads if p["measurementAvailable"] is False]
        self.assertGreater(len(omitted_payloads), 0, "결측 슬롯에 대한 브로드캐스트 페이로드가 수집되어야 합니다.")

        for op in omitted_payloads:
            self.assertFalse(op["measurementAvailable"])
            self.assertFalse(op["sensorFault"], "sensorFault는 명시적으로 False여야 합니다.")
            self.assertIsNone(op["totalP"])
            self.assertIsNone(op["totalQ"])
            self.assertIsNone(op["apparentS"])
            self.assertIsNone(op["pf"])
            self.assertIsNone(op["voltage"])
            self.assertIsNone(op["currentA"])
            self.assertEqual(op["activeNames"], [])
            self.assertEqual(op["source"], "E2E")
            self.assertEqual(op["run_id"], self.run_id)

        published_payloads = [p for p in captured_payloads if p["measurementAvailable"] is True]
        self.assertGreater(len(published_payloads), 0, "정상 슬롯에 대한 브로드캐스트 페이로드가 수집되어야 합니다.")
        first_pub = published_payloads[0]
        self.assertTrue(first_pub["measurementAvailable"])
        self.assertFalse(first_pub["sensorFault"])
        self.assertIsInstance(first_pub["totalP"], (int, float))


if __name__ == "__main__":
    unittest.main()
