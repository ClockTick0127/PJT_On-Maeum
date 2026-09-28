"""
NILM 스마트홈 전력 시뮬레이터 결정론적 일정 실행기 (2단계) 단위 테스트 스위트
"""

from datetime import date, datetime, timedelta
import json
import math
import random
import unittest

from engine.profiles import DEVICE_PROFILES
from engine.schedule import (
    KST,
    SECONDS_PER_DAY,
    SUPPORTED_APPLIANCES,
    ApplianceEvent,
    DaySchedule,
    OmissionRange,
    ScenarioDefinition,
    TransitionType,
    compile_schedule,
)
from engine.schedule_runtime import (
    DeterministicScheduleRunner,
    ExecutionCompletedError,
    RunnerSnapshot,
    ScheduledTick,
    derive_household_seed,
)


class TestScheduleRuntime(unittest.TestCase):
    """결정적 일정 실행기(2단계) 핵심 계약 및 물리 엔진 검증"""

    def setUp(self):
        self.base_date = date(2026, 9, 17)

    # ----------------------------------------------------
    # 1. 생성자 입력 검증
    # ----------------------------------------------------
    def test_constructor_input_validation(self):
        """plan, household_id의 타입 및 공백/빈값 엄격 검증"""
        day = DaySchedule(day_offset=0, events=())
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day,))
        plan = compile_schedule(defn, self.base_date)

        # 1. plan 타입 검증
        with self.assertRaises(TypeError):
            DeterministicScheduleRunner(plan="not_a_plan", household_id="H001")

        # 2. household_id 타입 검증
        with self.assertRaises(TypeError):
            DeterministicScheduleRunner(plan=plan, household_id=True)
        with self.assertRaises(TypeError):
            DeterministicScheduleRunner(plan=plan, household_id=123)

        # 3. household_id 빈 문자열 거절
        with self.assertRaises(ValueError):
            DeterministicScheduleRunner(plan=plan, household_id="")

        # 4. household_id 앞뒤 공백 거절 (조용히 strip하지 않음)
        with self.assertRaises(ValueError):
            DeterministicScheduleRunner(plan=plan, household_id=" H001")
        with self.assertRaises(ValueError):
            DeterministicScheduleRunner(plan=plan, household_id="H001 ")

        # 정상 생성
        runner = DeterministicScheduleRunner(plan=plan, household_id="H001")
        self.assertEqual(runner.household_id, "H001")

    # ----------------------------------------------------
    # 2. 시드 인자 검증 및 타입 태그 분리 검증
    # ----------------------------------------------------
    def test_seed_validation_and_type_tag_differentiation(self):
        """seed 타입 검증 및 seed=1과 seed='1'의 독립적 파생 검증"""
        day = DaySchedule(day_offset=0, events=())
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day,))
        plan = compile_schedule(defn, self.base_date)

        # bool 거절
        with self.assertRaises(TypeError):
            DeterministicScheduleRunner(plan=plan, household_id="H001", seed=True)
        # float/list/dict 거절
        with self.assertRaises(TypeError):
            DeterministicScheduleRunner(plan=plan, household_id="H001", seed=1.5)
        with self.assertRaises(TypeError):
            DeterministicScheduleRunner(plan=plan, household_id="H001", seed=[1])
        # 빈 문자열 및 앞뒤 공백 거절
        with self.assertRaises(ValueError):
            DeterministicScheduleRunner(plan=plan, household_id="H001", seed="")
        with self.assertRaises(ValueError):
            DeterministicScheduleRunner(plan=plan, household_id="H001", seed=" 42")
        with self.assertRaises(ValueError):
            DeterministicScheduleRunner(plan=plan, household_id="H001", seed="42 ")

        # None, int, str 정상 허용
        self.assertIsNotNone(DeterministicScheduleRunner(plan=plan, household_id="H001", seed=None))
        self.assertIsNotNone(DeterministicScheduleRunner(plan=plan, household_id="H001", seed=1))
        self.assertIsNotNone(DeterministicScheduleRunner(plan=plan, household_id="H001", seed="1"))

        # seed=1 ("INT:1") vs seed="1" ("STR:1") 서로 다른 시드 및 초기 틱 생성 검증
        seed_int = derive_household_seed(plan, "H001", 1)
        seed_str = derive_household_seed(plan, "H001", "1")
        self.assertNotEqual(seed_int, seed_str)

        runner_int = DeterministicScheduleRunner(plan=plan, household_id="H001", seed=1)
        runner_str = DeterministicScheduleRunner(plan=plan, household_id="H001", seed="1")
        tick_int = runner_int.step()
        tick_str = runner_str.step()
        self.assertNotEqual(tick_int.active_power, tick_str.active_power)

    # ----------------------------------------------------
    # 3. 동일 입력 및 동일 시드의 비트 단위 결정성 검증
    # ----------------------------------------------------
    def test_identical_inputs_and_seed_produce_identical_ticks(self):
        """동일한 plan, household_id, seed로 실행한 두 실행기의 모든 틱이 100% 동일함"""
        ev = ApplianceEvent("kettle", "07:00:00", 120)
        day = DaySchedule(day_offset=0, events=(ev,))
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day,))
        plan = compile_schedule(defn, self.base_date)

        runner_a = DeterministicScheduleRunner(plan, "H001", seed=42)
        runner_b = DeterministicScheduleRunner(plan, "H001", seed=42)

        for _ in range(300):
            tick_a = runner_a.step()
            tick_b = runner_b.step()
            self.assertEqual(tick_a, tick_b)

    # ----------------------------------------------------
    # 4. H001 진행 중 H002 교차 실행 시 완전 격리 검증
    # ----------------------------------------------------
    def test_h001_interleaved_with_h002_isolation(self):
        """H001 실행 중간에 H002를 교차 실행해도 H001의 결과가 단 1비트도 변하지 않음"""
        ev = ApplianceEvent("kettle", "07:00:00", 120)
        day = DaySchedule(day_offset=0, events=(ev,))
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day,))
        plan = compile_schedule(defn, self.base_date)

        # 기준: 연속 실행 H001 (100 슬롯)
        runner_ref = DeterministicScheduleRunner(plan, "H001", seed=100)
        ref_ticks = [runner_ref.step() for _ in range(100)]

        # 실험: 교차 실행 H001 (50 슬롯) -> H002 (30 슬롯) -> H001 (50 슬롯)
        runner_h1 = DeterministicScheduleRunner(plan, "H001", seed=100)
        runner_h2 = DeterministicScheduleRunner(plan, "H002", seed=200)

        interleaved_ticks = []
        for _ in range(50):
            interleaved_ticks.append(runner_h1.step())
        for _ in range(30):
            runner_h2.step()
        for _ in range(50):
            interleaved_ticks.append(runner_h1.step())

        self.assertEqual(ref_ticks, interleaved_ticks)

    # ----------------------------------------------------
    # 5. 가구 실행 순서 무관성 검증
    # ----------------------------------------------------
    def test_execution_order_invariance(self):
        """(H001 완주 후 H002 완주) vs (H002 완주 후 H001 완주) 결과 동일성 검증"""
        day = DaySchedule(day_offset=0, events=())
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day,))
        plan = compile_schedule(defn, self.base_date)

        # 순서 1: H001 -> H002
        r1_h1 = DeterministicScheduleRunner(plan, "H001", seed=77)
        r1_h2 = DeterministicScheduleRunner(plan, "H002", seed=88)
        h1_order1 = [r1_h1.step() for _ in range(50)]
        h2_order1 = [r1_h2.step() for _ in range(50)]

        # 순서 2: H002 -> H001
        r2_h2 = DeterministicScheduleRunner(plan, "H002", seed=88)
        r2_h1 = DeterministicScheduleRunner(plan, "H001", seed=77)
        h2_order2 = [r2_h2.step() for _ in range(50)]
        h1_order2 = [r2_h1.step() for _ in range(50)]

        self.assertEqual(h1_order1, h1_order2)
        self.assertEqual(h2_order1, h2_order2)

    # ----------------------------------------------------
    # 6. 전역 random 모듈 미사용 및 상태 불변 검증
    # ----------------------------------------------------
    def test_global_random_not_touched(self):
        """실행기 가동 중 전역 random.getstate()가 100% 불변임을 검증"""
        day = DaySchedule(day_offset=0, events=())
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day,))
        plan = compile_schedule(defn, self.base_date)
        runner = DeterministicScheduleRunner(plan, "H001", seed=12345)

        state_before = random.getstate()
        for _ in range(1000):
            runner.step()
        state_after = random.getstate()

        self.assertEqual(state_before, state_after)

    # ----------------------------------------------------
    # 7. DEVICE_PROFILES 및 CompiledExecutionPlan 불변성 검증
    # ----------------------------------------------------
    def test_device_profiles_and_plan_immutability(self):
        """실행기 가동 전후 DEVICE_PROFILES 및 plan 객체가 일절 수정되지 않음"""
        profiles_keys_before = tuple(DEVICE_PROFILES.keys())
        kettle_nominal_before = DEVICE_PROFILES["kettle"]["nominal_w"]

        ev = ApplianceEvent("kettle", "07:00:00", 120)
        day = DaySchedule(day_offset=0, events=(ev,))
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day,))
        plan = compile_schedule(defn, self.base_date)

        total_virtual_slots_before = plan.total_virtual_slots
        plan_days_count = len(plan.day_plans)

        runner = DeterministicScheduleRunner(plan, "H001", seed=99)
        for _ in range(300):
            runner.step()

        self.assertEqual(tuple(DEVICE_PROFILES.keys()), profiles_keys_before)
        self.assertEqual(DEVICE_PROFILES["kettle"]["nominal_w"], kettle_nominal_before)
        self.assertEqual(plan.total_virtual_slots, total_virtual_slots_before)
        self.assertEqual(len(plan.day_plans), plan_days_count)

    # ----------------------------------------------------
    # 8. active_appliances의 고정 canonical 순서 검증
    # ----------------------------------------------------
    def test_active_appliances_canonical_ordering(self):
        """active_appliances가 등록 순서와 무관하게 항상 SUPPORTED_APPLIANCES canonical 순서로 반환"""
        # vacuum_cleaner(760s), kettle(1657W), hair_dryer(934W) 동시 가동
        ev1 = ApplianceEvent("vacuum_cleaner", "07:00:00", 60)
        ev2 = ApplianceEvent("kettle", "07:00:00", 60)
        ev3 = ApplianceEvent("hair_dryer", "07:00:00", 60)
        day = DaySchedule(day_offset=0, events=(ev1, ev2, ev3))
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day,))
        plan = compile_schedule(defn, self.base_date, start_cycle=1)

        runner = DeterministicScheduleRunner(plan, "H001", seed=55)
        # 07:00:00은 25200초 -> cycle 25201
        for _ in range(25200):
            runner.step()

        tick_on = runner.step()  # cycle 25201
        self.assertEqual(tick_on.cycle, 25201)
        expected_canonical = ("hair_dryer", "kettle", "vacuum_cleaner")
        self.assertEqual(tick_on.active_appliances, expected_canonical)

    # ----------------------------------------------------
    # 9. transitions 순서 보존 검증 (plan.transitions_at 순서 불변)
    # ----------------------------------------------------
    def test_transitions_order_preserved_from_plan(self):
        """동일 초에 OFF, ON 다중 전이 발생 시 plan.transitions_at의 정렬 순서가 tick.transitions에 그대로 보존됨"""
        # kettle OFF at 25320, microwave ON at 25320
        ev1 = ApplianceEvent("kettle", "07:00:00", 120)  # OFF at 25320
        ev2 = ApplianceEvent("microwave", "07:02:00", 60) # ON at 25320
        day = DaySchedule(day_offset=0, events=(ev1, ev2))
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day,))
        plan = compile_schedule(defn, self.base_date, start_cycle=1)

        runner = DeterministicScheduleRunner(plan, "H001", seed=10)
        for _ in range(25320):
            runner.step()

        tick = runner.step()  # cycle 25321 (25320초)
        plan_transitions = plan.transitions_at(25321)
        self.assertEqual(tick.transitions, plan_transitions)
        self.assertEqual(tick.transitions[0].transition_type, TransitionType.OFF)
        self.assertEqual(tick.transitions[0].appliance, "kettle")
        self.assertEqual(tick.transitions[1].transition_type, TransitionType.ON)
        self.assertEqual(tick.transitions[1].appliance, "microwave")

    # ----------------------------------------------------
    # 10. ON 전환 슬롯부터 active_appliances 포함 검증
    # ----------------------------------------------------
    def test_on_transition_slot_includes_in_active_appliances(self):
        """ON 전이가 발생한 바로 그 슬롯부터 active_appliances에 해당 가전이 즉시 포함됨"""
        ev = ApplianceEvent("kettle", "00:00:03", 10)  # cycle 4 ON
        day = DaySchedule(day_offset=0, events=(ev,))
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day,))
        plan = compile_schedule(defn, self.base_date, start_cycle=1)

        runner = DeterministicScheduleRunner(plan, "H001")
        tick1 = runner.step()  # cycle 1 (0초)
        tick2 = runner.step()  # cycle 2 (1초)
        tick3 = runner.step()  # cycle 3 (2초)
        self.assertNotIn("kettle", tick3.active_appliances)

        tick4 = runner.step()  # cycle 4 (3초 - ON 전이)
        self.assertIn("kettle", tick4.active_appliances)
        self.assertEqual(len(tick4.transitions), 1)
        self.assertEqual(tick4.transitions[0].transition_type, TransitionType.ON)

    # ----------------------------------------------------
    # 11. OFF 전환 슬롯부터 active_appliances 제외 검증
    # ----------------------------------------------------
    def test_off_transition_slot_excludes_from_active_appliances(self):
        """OFF 전이가 발생한 바로 그 슬롯부터 active_appliances에서 즉시 제외됨"""
        ev = ApplianceEvent("kettle", "00:00:03", 5)  # cycle 4 ON, cycle 9 OFF (end_sec=8)
        day = DaySchedule(day_offset=0, events=(ev,))
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day,))
        plan = compile_schedule(defn, self.base_date, start_cycle=1)

        runner = DeterministicScheduleRunner(plan, "H001")
        for _ in range(7):  # cycles 1..7
            runner.step()

        tick8 = runner.step()  # cycle 8 (7초: 가동 마지막 슬롯)
        self.assertIn("kettle", tick8.active_appliances)

        tick9 = runner.step()  # cycle 9 (8초: OFF 전이 슬롯)
        self.assertNotIn("kettle", tick9.active_appliances)
        self.assertEqual(len(tick9.transitions), 1)
        self.assertEqual(tick9.transitions[0].transition_type, TransitionType.OFF)

    # ----------------------------------------------------
    # 12. 가전 활성 슬롯 총합이 duration_seconds와 일치 검증
    # ----------------------------------------------------
    def test_event_duration_matches_schedule_definition(self):
        """가전이 active_appliances에 포함된 총 슬롯 수가 duration_seconds와 정확히 일치"""
        duration = 45
        ev = ApplianceEvent("kettle", "01:00:00", duration)
        day = DaySchedule(day_offset=0, events=(ev,))
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day,))
        plan = compile_schedule(defn, self.base_date, start_cycle=1)

        runner = DeterministicScheduleRunner(plan, "H001")
        active_count = 0
        for _ in range(4000):
            tick = runner.step()
            if "kettle" in tick.active_appliances:
                active_count += 1

        self.assertEqual(active_count, duration)

    # ----------------------------------------------------
    # 13. 결측 슬롯에서도 가상 시각과 사이클 정상 전진 검증
    # ----------------------------------------------------
    def test_omitted_slots_advance_virtual_time_and_cycle(self):
        """should_publish가 False인 결측 슬롯에서도 cycle과 virtual_time은 매초 단조 증가"""
        day = DaySchedule(day_offset=0, events=(), omission_ranges=(OmissionRange(10, 20),))
        defn = ScenarioDefinition(scenario_id="ACTIVITY_GAP", days=(day,))
        plan = compile_schedule(defn, self.base_date, start_cycle=1)

        runner = DeterministicScheduleRunner(plan, "H001")
        # 10초까지 정상 진행 (cycles 1..10)
        for _ in range(10):
            runner.step()

        # cycle 11 (10초: 결측 시작)
        tick_omitted = runner.step()
        self.assertEqual(tick_omitted.cycle, 11)
        self.assertFalse(tick_omitted.is_publish_candidate)
        self.assertEqual(tick_omitted.measured_at, datetime(2026, 9, 17, 0, 0, 10, tzinfo=KST))

        # cycle 12 (11초: 결측 중)
        tick_omitted2 = runner.step()
        self.assertEqual(tick_omitted2.cycle, 12)
        self.assertFalse(tick_omitted2.is_publish_candidate)
        self.assertEqual(tick_omitted2.measured_at, datetime(2026, 9, 17, 0, 0, 11, tzinfo=KST))

    # ----------------------------------------------------
    # 14. 결측 슬롯은 generated_publish_candidates에 불포함 검증
    # ----------------------------------------------------
    def test_omitted_slots_not_in_generated_publish_candidates(self):
        """결측 슬롯은 generated_publish_candidates를 증가시키지 않고 omitted_slots만 증가시킴"""
        day = DaySchedule(day_offset=0, events=(), omission_ranges=(OmissionRange(10, 20),))
        defn = ScenarioDefinition(scenario_id="ACTIVITY_GAP", days=(day,))
        plan = compile_schedule(defn, self.base_date, start_cycle=1)

        runner = DeterministicScheduleRunner(plan, "H001")
        for _ in range(10):  # 0..9초 (정상 슬롯 10개)
            runner.step()

        self.assertEqual(runner.generated_publish_candidates, 10)
        self.assertEqual(runner.omitted_slots, 0)

        for _ in range(10):  # 10..19초 (결측 슬롯 10개)
            runner.step()

        self.assertEqual(runner.generated_publish_candidates, 10)
        self.assertEqual(runner.omitted_slots, 10)
        self.assertEqual(runner.processed_virtual_slots, 20)

    # ----------------------------------------------------
    # 15. 카운터 합계 불변식 검증
    # ----------------------------------------------------
    def test_counter_sum_invariant(self):
        """매 슬롯 processed_virtual_slots == generated_publish_candidates + omitted_slots 성립"""
        day = DaySchedule(day_offset=0, events=(), omission_ranges=(OmissionRange(5, 15),))
        defn = ScenarioDefinition(scenario_id="ACTIVITY_GAP", days=(day,))
        plan = compile_schedule(defn, self.base_date, start_cycle=1)

        runner = DeterministicScheduleRunner(plan, "H001")
        for _ in range(50):
            runner.step()
            self.assertEqual(
                runner.processed_virtual_slots,
                runner.generated_publish_candidates + runner.omitted_slots,
            )

    # ----------------------------------------------------
    # 16. 초기 스냅샷 경계값 검증
    # ----------------------------------------------------
    def test_initial_snapshot_boundaries(self):
        """step() 실행 전 초기 스냅샷 상태 검증"""
        day = DaySchedule(day_offset=0, events=())
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day,))
        plan = compile_schedule(defn, self.base_date, start_cycle=100)

        runner = DeterministicScheduleRunner(plan, "H001")
        snap = runner.snapshot()

        self.assertIsNone(snap.last_processed_cycle)
        self.assertEqual(snap.next_cycle, 100)
        self.assertEqual(snap.first_cycle, 100)
        self.assertEqual(snap.last_cycle, 100 + 86400 - 1)
        self.assertEqual(snap.total_virtual_slots, 86400)
        self.assertEqual(snap.processed_virtual_slots, 0)
        self.assertEqual(snap.generated_publish_candidates, 0)
        self.assertEqual(snap.omitted_slots, 0)
        self.assertFalse(snap.is_completed)
        self.assertEqual(snap.active_appliances, ())

    # ----------------------------------------------------
    # 17. 완료 스냅샷 경계값 검증
    # ----------------------------------------------------
    def test_completion_snapshot_boundaries(self):
        """완료 시점의 스냅샷 경계값 검증"""
        day = DaySchedule(day_offset=0, events=(), omission_ranges=(OmissionRange(82079, 86400),))
        defn = ScenarioDefinition(scenario_id="ACTIVITY_INSUFFICIENT", days=(day,))
        plan = compile_schedule(defn, self.base_date, start_cycle=1)

        runner = DeterministicScheduleRunner(plan, "H001")
        for _ in range(86400):
            runner.step()

        snap = runner.snapshot()
        self.assertEqual(snap.last_processed_cycle, 86400)
        self.assertEqual(snap.next_cycle, 86401)
        self.assertEqual(snap.processed_virtual_slots, 86400)
        self.assertEqual(snap.generated_publish_candidates, 82079)
        self.assertEqual(snap.omitted_slots, 4321)
        self.assertTrue(snap.is_completed)

    # ----------------------------------------------------
    # 18. 완료 후 step() 에러 및 iterator StopIteration 구분 검증
    # ----------------------------------------------------
    def test_step_completion_error_vs_iterator_stop_iteration(self):
        """완료 후 step()은 ExecutionCompletedError를, next()는 StopIteration을 발생시킴"""
        day = DaySchedule(day_offset=0, events=())
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day,))
        plan = compile_schedule(defn, self.base_date, start_cycle=1)

        runner = DeterministicScheduleRunner(plan, "H001")
        for _ in range(86400):
            runner.step()

        self.assertTrue(runner.is_completed)

        with self.assertRaises(ExecutionCompletedError):
            runner.step()

        with self.assertRaises(StopIteration):
            next(runner)

    # ----------------------------------------------------
    # 19. for 루프 이터레이터 완주 검증
    # ----------------------------------------------------
    def test_for_loop_iterator_consumes_all_slots_cleanly(self):
        """for 루프가 모든 슬롯을 예외 노출 없이 정상 소진하며 완료 상태로 전환됨"""
        day = DaySchedule(day_offset=0, events=())
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day,))
        plan = compile_schedule(defn, self.base_date, start_cycle=1)

        runner = DeterministicScheduleRunner(plan, "H001")
        count = sum(1 for _ in runner)

        self.assertEqual(count, 86400)
        self.assertTrue(runner.is_completed)
        self.assertTrue(runner.snapshot().is_completed)

        # 완주 후 추가 step()은 여전히 ExecutionCompletedError 발생
        with self.assertRaises(ExecutionCompletedError):
            runner.step()

    # ----------------------------------------------------
    # 20. reset() 복원 및 재실행 결과 동일성 검증
    # ----------------------------------------------------
    def test_reset_restores_initial_state_and_reproduces_identical_ticks(self):
        """reset() 호출 시 스냅샷이 최초 상태와 동일하며, 재실행 시 첫 번째 실행과 비트 단위 일치"""
        ev = ApplianceEvent("kettle", "07:00:00", 120)
        day = DaySchedule(day_offset=0, events=(ev,))
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day,))
        plan = compile_schedule(defn, self.base_date, start_cycle=1)

        runner = DeterministicScheduleRunner(plan, "H001", seed=777)
        initial_snap = runner.snapshot()

        first_run_ticks = [runner.step() for _ in range(25300)]
        self.assertFalse(runner.is_completed)

        runner.reset()
        reset_snap = runner.snapshot()
        self.assertEqual(initial_snap, reset_snap)

        second_run_ticks = [runner.step() for _ in range(25300)]
        self.assertEqual(first_run_ticks, second_run_ticks)

    # ----------------------------------------------------
    # 21. 날짜 경계에서의 measured_at 1초 연속 단조 증가 검증
    # ----------------------------------------------------
    def test_measured_at_advances_one_second_across_day_boundary(self):
        """23:59:59에서 다음 날 00:00:00으로 1초 정확히 증가함"""
        day0 = DaySchedule(day_offset=0, events=())
        day1 = DaySchedule(day_offset=1, events=())
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day0, day1))
        plan = compile_schedule(defn, self.base_date, start_cycle=1)

        runner = DeterministicScheduleRunner(plan, "H001")
        for _ in range(86399):
            runner.step()

        tick_last_d0 = runner.step()  # slot 86400 (23:59:59)
        tick_first_d1 = runner.step() # slot 86401 (다음 날 00:00:00)

        self.assertEqual(tick_last_d0.measured_at, datetime(2026, 9, 17, 23, 59, 59, tzinfo=KST))
        self.assertEqual(tick_first_d1.measured_at, datetime(2026, 9, 18, 0, 0, 0, tzinfo=KST))
        self.assertEqual(tick_first_d1.measured_at - tick_last_d0.measured_at, timedelta(seconds=1))

    # ----------------------------------------------------
    # 22. timezone-aware KST (+09:00) 검증
    # ----------------------------------------------------
    def test_timezone_aware_kst(self):
        """모든 틱의 measured_at이 timezone-aware KST (+09:00) 객체임"""
        day = DaySchedule(day_offset=0, events=())
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day,))
        plan = compile_schedule(defn, self.base_date)

        runner = DeterministicScheduleRunner(plan, "H001")
        for _ in range(10):
            tick = runner.step()
            self.assertIsNotNone(tick.measured_at.tzinfo)
            self.assertEqual(tick.measured_at.utcoffset(), timedelta(hours=9))

    # ----------------------------------------------------
    # 23. 추가 필수 테스트 1: 7,200초 전기포트 세션 조기 종료 차단 검증
    # ----------------------------------------------------
    def test_long_kettle_session_ignores_profile_session_sec(self):
        """kettle 7,200초 가동 시 profile session_sec(210초)에 영향받지 않고 정확히 7,200개 연속 슬롯 활성 유지"""
        # 07:00:00(25200초) ~ 09:00:00(32400초): duration 7,200초
        ev = ApplianceEvent("kettle", "07:00:00", 7200)
        day = DaySchedule(day_offset=0, events=(ev,))
        defn = ScenarioDefinition(scenario_id="ACTIVITY_LONG_KETTLE", days=(day,))
        plan = compile_schedule(defn, self.base_date, start_cycle=1)

        runner = DeterministicScheduleRunner(plan, "H001", seed=42)

        # 07:00:00 직전까지 건너뛰기
        for _ in range(25200):
            runner.step()

        # 7,200초 동안 연속 활성 상태 스트리밍 검증
        consecutive_kettle_active = 0
        for _ in range(7200):
            tick = runner.step()
            if "kettle" in tick.active_appliances:
                consecutive_kettle_active += 1
            else:
                break

        self.assertEqual(consecutive_kettle_active, 7200)

        # OFF 전환 슬롯(32400초 -> cycle 32401)부터 즉시 제외
        tick_off = runner.step()
        self.assertNotIn("kettle", tick_off.active_appliances)
        self.assertEqual(len(tick_off.transitions), 1)
        self.assertEqual(tick_off.transitions[0].transition_type, TransitionType.OFF)

    # ----------------------------------------------------
    # 24. 추가 필수 테스트 2: 부분 omission range 포함 완료 카운트 일치 검증
    # ----------------------------------------------------
    def test_partial_omission_completion_counts_match_plan(self):
        """부분 omission range 포함 1일 계획 스트리밍 완주 후 스냅샷 카운트 불변식 검증"""
        # kettle: 01:00:00(3600초) 60초 가동 (보호 구간 [3597, 3663))
        # omission: [5000, 6000) (1,000초 결측) -> 보호 구간과 충돌 없음
        ev = ApplianceEvent("kettle", "01:00:00", 60)
        om = OmissionRange(5000, 6000)
        day = DaySchedule(day_offset=0, events=(ev,), omission_ranges=(om,), publish_samples=85400)
        defn = ScenarioDefinition(scenario_id="ACTIVITY_PARTIAL_OMISSION", days=(day,))
        plan = compile_schedule(defn, self.base_date, start_cycle=1)

        runner = DeterministicScheduleRunner(plan, "H001", seed=123)

        # 리스트에 모으지 않고 스트리밍 완주
        for _ in runner:
            pass

        snap = runner.snapshot()
        self.assertEqual(snap.processed_virtual_slots, plan.total_virtual_slots)
        self.assertEqual(snap.generated_publish_candidates, plan.total_planned_publish_samples)
        self.assertEqual(snap.omitted_slots, plan.total_planned_omitted_samples)
        self.assertEqual(
            snap.processed_virtual_slots,
            snap.generated_publish_candidates + snap.omitted_slots,
        )
        self.assertEqual(snap.last_processed_cycle, plan.last_cycle)
        self.assertEqual(snap.next_cycle, plan.last_cycle + 1)
        self.assertTrue(snap.is_completed)

    # ----------------------------------------------------
    # 25. 계산 순서 기반 물리 허용 오차 및 부호 검증
    # ----------------------------------------------------
    def test_physical_invariants_and_formulas(self):
        """P, Q, S, PF, V, I의 물리 관계 및 엄격한 허용 오차 검증"""
        ev = ApplianceEvent("kettle", "07:00:00", 120)
        day = DaySchedule(day_offset=0, events=(ev,))
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day,))
        plan = compile_schedule(defn, self.base_date, start_cycle=1)

        runner = DeterministicScheduleRunner(plan, "H001", seed=101)
        for _ in range(500):
            tick = runner.step()
            self.assertGreaterEqual(tick.active_power, 0.0)
            self.assertGreaterEqual(tick.reactive_power, 0.0)
            self.assertGreaterEqual(tick.apparent_power, 0.0)
            self.assertGreaterEqual(tick.current, 0.0)
            self.assertTrue(0.0 <= tick.power_factor <= 1.0)
            self.assertTrue(212.0 <= tick.voltage <= 228.0)

            # S_out = round(sqrt(P_out^2 + Q_out^2), 2) 오차 검증
            expected_s = math.sqrt(tick.active_power**2 + tick.reactive_power**2)
            self.assertAlmostEqual(tick.apparent_power, expected_s, delta=0.01)

            # I_out = round(S_out / V_out, 3) 오차 검증
            expected_i = tick.apparent_power / tick.voltage
            self.assertAlmostEqual(tick.current, expected_i, delta=0.001)

            # PF_out = round(P_out / S_out, 3) 오차 검증
            if tick.apparent_power > 1e-4:
                expected_pf = tick.active_power / tick.apparent_power
                self.assertAlmostEqual(tick.power_factor, expected_pf, delta=0.001)

    # ----------------------------------------------------
    # 26. ScheduledTick.to_dict() 순수 JSON 직렬화 검증
    # ----------------------------------------------------
    def test_scheduled_tick_to_dict_pure_json_serialization(self):
        """to_dict()가 순수 JSON 호환 딕셔너리를 반환하며 json.dumps()가 즉시 성공함"""
        ev = ApplianceEvent("kettle", "07:00:00", 120)
        day = DaySchedule(day_offset=0, events=(ev,))
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day,))
        plan = compile_schedule(defn, self.base_date, start_cycle=1)

        runner = DeterministicScheduleRunner(plan, "H001", seed=202)
        # 07:00:00 (cycle 25201)으로 이동
        for _ in range(25200):
            runner.step()

        tick = runner.step()
        data = tick.to_dict()

        # json.dumps() 성공 검증
        json_str = json.dumps(data)
        self.assertIsInstance(json_str, str)

        # 필드 검증
        self.assertEqual(data["household_id"], "H001")
        self.assertEqual(data["cycle"], 25201)
        self.assertIn("+09:00", data["measured_at"])
        self.assertIsInstance(data["transitions"], list)
        self.assertEqual(data["transitions"][0]["transition_type"], "ON")
        self.assertIn("+09:00", data["transitions"][0]["virtual_time"])
        self.assertEqual(data["active_appliances"], ["kettle"])

    # ----------------------------------------------------
    # 27. ACTIVITY_NONE 및 FULL_DAY_SENSOR_GAP 스트리밍 카운터 검증
    # ----------------------------------------------------
    def test_activity_none_and_full_gap_streaming_counts(self):
        """86,400개 슬롯을 리스트에 모으지 않고 스트리밍 카운터로 완주 검증"""
        # 1. ACTIVITY_NONE: 86,400 후보 발행, 종일 가전 OFF
        day_none = DaySchedule(day_offset=0, events=(), publish_samples=86400)
        defn_none = ScenarioDefinition(scenario_id="ACTIVITY_NONE", days=(day_none,))
        plan_none = compile_schedule(defn_none, self.base_date)

        runner_none = DeterministicScheduleRunner(plan_none, "H001")
        candidates_none = 0
        any_appliance_active = False
        for tick in runner_none:
            if tick.is_publish_candidate:
                candidates_none += 1
            if len(tick.active_appliances) > 0:
                any_appliance_active = True

        self.assertEqual(candidates_none, 86400)
        self.assertFalse(any_appliance_active)
        self.assertEqual(runner_none.omitted_slots, 0)

        # 2. FULL_DAY_SENSOR_GAP: 86,400 슬롯 처리, 후보 발행 0
        day_gap = DaySchedule(day_offset=0, events=(), omission_ranges=(OmissionRange(0, 86400),), publish_samples=0)
        defn_gap = ScenarioDefinition(scenario_id="FULL_DAY_SENSOR_GAP", days=(day_gap,))
        plan_gap = compile_schedule(defn_gap, self.base_date)

        runner_gap = DeterministicScheduleRunner(plan_gap, "H001")
        candidates_gap = 0
        omitted_gap = 0
        for tick in runner_gap:
            if tick.is_publish_candidate:
                candidates_gap += 1
            else:
                omitted_gap += 1

        self.assertEqual(candidates_gap, 0)
        self.assertEqual(omitted_gap, 86400)
        self.assertEqual(runner_gap.processed_virtual_slots, 86400)

    # ----------------------------------------------------
    # 28. 28일 일정 실행기 생성 시 지연 메모리 효율 검증
    # ----------------------------------------------------
    def test_28d_plan_lazy_streaming_memory_efficient(self):
        """28일(242만 슬롯) 실행기 인스턴스 생성 시 대규모 틱 배열이 존재하지 않고 O(1) 지연 실행됨"""
        days = [DaySchedule(day_offset=d, events=()) for d in range(-27, 1)]
        defn = ScenarioDefinition(scenario_id="BASELINE_28D", days=tuple(days))
        plan = compile_schedule(defn, self.base_date)

        runner = DeterministicScheduleRunner(plan, "H001", seed=42)

        self.assertEqual(runner.plan.total_virtual_slots, 2_419_200)
        self.assertFalse(hasattr(runner, "_ticks"))
        self.assertFalse(hasattr(runner, "samples"))

        # 지연 실행이 1슬롯씩 정확히 동작함
        tick1 = runner.step()
        self.assertEqual(tick1.cycle, 1)
        self.assertEqual(tick1.measured_at, datetime(2026, 8, 21, 0, 0, 0, tzinfo=KST))

        tick2 = runner.step()
        self.assertEqual(tick2.cycle, 2)
        self.assertEqual(tick2.measured_at, datetime(2026, 8, 21, 0, 0, 1, tzinfo=KST))


class TestScheduledOnWindowIsContinuous(unittest.TestCase):
    """
    선언된 ON 구간은 듀티 사이클 없이 연속 가동되어야 한다.

    induction/iron은 프로파일상 duty_cycle 가전이지만, 일정 기반 실행에서는
    선언된 구간이 곧 '사람이 사용한 구간'이므로 내부 휴지를 넣으면 안 된다.
    휴지가 들어가면 AI가 한 번의 사용을 수십 개 세션으로 쪼개어 인식하고,
    사용시간 합계가 선언값보다 짧아지며, 시드에 따라 값이 흔들려 결정성도 깨진다.
    """

    ON_THRESHOLD_W = 300.0

    def _high_power_blocks(self, definition, base_date, household_id):
        """active_power가 임계값 이상인 연속 구간 목록 [(start_sec, end_sec_exclusive), ...]"""
        plan = compile_schedule(definition, base_date=base_date)
        runner = DeterministicScheduleRunner(plan, household_id)
        blocks: list[tuple[int, int]] = []
        current_start: int | None = None
        previous_sec = -1
        for tick in runner:
            second = (tick.cycle - plan.first_cycle) % SECONDS_PER_DAY
            if tick.active_power >= self.ON_THRESHOLD_W:
                if current_start is None:
                    current_start = second
                previous_sec = second
            elif current_start is not None:
                blocks.append((current_start, previous_sec + 1))
                current_start = None
        if current_start is not None:
            blocks.append((current_start, previous_sec + 1))
        return blocks

    @staticmethod
    def _session_merge_definition():
        return ScenarioDefinition(
            scenario_id="ACTIVITY_SESSION_MERGE",
            days=(
                DaySchedule(
                    day_offset=0,
                    events=(
                        ApplianceEvent(appliance="induction", start_time="10:00:00", duration_seconds=600),
                        ApplianceEvent(appliance="induction", start_time="10:11:00", duration_seconds=600),
                    ),
                    omission_ranges=(),
                    publish_samples=SECONDS_PER_DAY,
                ),
            ),
        )

    def test_induction_on_window_has_no_duty_cycle_gaps(self):
        """인덕션 600초 ON 구간이 조각나지 않고 정확히 2블록으로 나온다"""
        blocks = self._high_power_blocks(
            self._session_merge_definition(), date(2026, 9, 16), "H001"
        )

        self.assertEqual(
            len(blocks), 2,
            f"선언된 ON 구간이 듀티 사이클로 쪼개졌습니다: {len(blocks)}개 블록",
        )
        self.assertEqual(blocks[0], (10 * 3600, 10 * 3600 + 600))
        self.assertEqual(blocks[1], (10 * 3600 + 660, 10 * 3600 + 1260))

        # 선언된 OFF 간격은 정확히 60초 (INDUCTION 병합 간격 120초 미만)
        self.assertEqual(blocks[1][0] - blocks[0][1], 60)

        # AI가 집계할 총 사용시간은 선언값과 정확히 일치해야 한다
        total_on_seconds = sum(end - start for start, end in blocks)
        self.assertEqual(total_on_seconds, 1200)

    def test_on_window_identical_across_reference_dates_and_households(self):
        """가동 구간이 기준일·가구와 무관하게 동일하다 (결정성)"""
        definition = self._session_merge_definition()
        expected = [(10 * 3600, 10 * 3600 + 600), (10 * 3600 + 660, 10 * 3600 + 1260)]

        for base_date in (date(2026, 9, 16), date(2026, 9, 17), date(2026, 9, 18)):
            for household_id in ("H001", "H002"):
                with self.subTest(base_date=base_date, household_id=household_id):
                    self.assertEqual(
                        self._high_power_blocks(definition, base_date, household_id),
                        expected,
                    )

    def test_runtime_state_machine_has_no_resting_state(self):
        """일정 기반 런타임에는 RESTING(듀티 휴지) 상태가 존재하지 않는다"""
        definition = self._session_merge_definition()
        plan = compile_schedule(definition, base_date=date(2026, 9, 16))
        runner = DeterministicScheduleRunner(plan, "H001")

        # ON 구간 한복판(10:05:00)까지 진행시킨 뒤 상태를 확인한다
        for _ in range(10 * 3600 + 300 + 1):
            runner.step()

        induction_state = runner._app_states["induction"]
        self.assertEqual(induction_state.state, "RUNNING")
        self.assertFalse(
            hasattr(induction_state, "duty_remaining"),
            "일정 기반 런타임에 duty 잔여 카운터가 남아 있습니다.",
        )
        self.assertIn("induction", runner.snapshot().active_appliances)


if __name__ == "__main__":
    unittest.main()
