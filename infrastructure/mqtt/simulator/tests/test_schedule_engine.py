"""
선언적 일정 기반 E2E 시나리오 엔진 (1단계) 단위 테스트 스위트
"""

import copy
from datetime import date, datetime, timedelta
from types import MappingProxyType
import unittest

from engine.schedule import (
    KST,
    SECONDS_PER_DAY,
    SUPPORTED_APPLIANCES,
    TRANSITION_PRIORITY,
    ApplianceEvent,
    ApplianceTransition,
    CompiledDayPlan,
    CompiledExecutionPlan,
    CycleOutOfRangeError,
    DateBoundaryViolationError,
    DayOffsetSequenceError,
    DaySchedule,
    EventEvidenceOmittedError,
    EventOverlapError,
    InvalidApplianceError,
    InvalidDurationError,
    InvalidScenarioIdError,
    InvalidTimeFormatError,
    OmissionRange,
    OmissionRangeError,
    SampleBoundaryViolationError,
    SampleCountViolationError,
    ScenarioDefinition,
    ScheduleError,
    TransitionType,
    UnknownFieldError,
    compile_schedule,
    normalize_appliance_name,
    parse_scenario_definition,
    validate_scenario_definition,
)
from engine.profiles import DEVICE_PROFILES


class TestScheduleEngine(unittest.TestCase):
    """결정적 일정 기반 E2E 시나리오 엔진 1단계 단위 테스트"""

    def setUp(self):
        self.base_date = date(2026, 9, 17)

    # ----------------------------------------------------
    # 1. ACTIVITY_NONE 시나리오 의미 검증
    # ----------------------------------------------------
    def test_activity_none_semantics(self):
        """ACTIVITY_NONE: events=(), omission_ranges=(), planned_publish_samples=86400, 종일 OFF 대기전력 발행"""
        day = DaySchedule(day_offset=0, events=(), omission_ranges=(), publish_samples=86400)
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NONE", days=(day,), description="정상 대기전력 무활동 일상")
        plan = compile_schedule(defn, self.base_date)

        self.assertEqual(plan.total_virtual_slots, 86400)
        self.assertEqual(plan.total_planned_publish_samples, 86400)
        self.assertEqual(plan.total_planned_omitted_samples, 0)
        self.assertEqual(len(plan.transitions_by_cycle), 0)
        self.assertTrue(plan.should_publish(1))
        self.assertTrue(plan.should_publish(86400))
        self.assertEqual(plan.virtual_time_at(1), datetime(2026, 9, 17, 0, 0, 0, tzinfo=KST))
        self.assertEqual(plan.virtual_time_at(86400), datetime(2026, 9, 17, 23, 59, 59, tzinfo=KST))

    # ----------------------------------------------------
    # 2. 완전 결측일 (FULL_DAY_SENSOR_GAP) 및 가전 이벤트 금지 검증
    # ----------------------------------------------------
    def test_full_day_sensor_gap_semantics(self):
        """완전 결측일: omission_ranges=[(0, 86400)], planned_publish_samples=0, events=()이면 허용, events 존재 시 거절"""
        # events=() 정상 허용
        day_valid = DaySchedule(day_offset=0, events=(), omission_ranges=(OmissionRange(0, 86400),), publish_samples=0)
        defn_valid = ScenarioDefinition(scenario_id="FULL_DAY_SENSOR_GAP", days=(day_valid,))
        plan = compile_schedule(defn_valid, self.base_date)
        self.assertEqual(plan.total_planned_publish_samples, 0)
        self.assertEqual(plan.total_planned_omitted_samples, 86400)
        self.assertFalse(plan.should_publish(1))
        self.assertFalse(plan.should_publish(86400))

        # events가 1개라도 존재하면 EventEvidenceOmittedError 거절
        ev = ApplianceEvent(appliance="kettle", start_time="07:00:00", duration_seconds=120)
        with self.assertRaises(EventEvidenceOmittedError) as ctx:
            DaySchedule(day_offset=0, events=(ev,), omission_ranges=(OmissionRange(0, 86400),))
        self.assertIn("완전 결측일", str(ctx.exception))

    # ----------------------------------------------------
    # 3. 28일 일정 컴파일 시 메모리 효율성 검증 (242만 슬롯 마스크 미생성)
    # ----------------------------------------------------
    def test_28d_no_full_publish_mask_memory_efficient(self):
        """28일 일정 컴파일 시 242만 개 마스크/datetime 배열을 생성하지 않고 O(D+E+R) sparse 구조 유지"""
        days = []
        for d in range(-27, 1):  # D-27 ~ D (총 28일)
            events = ()
            if d == 0:
                events = (
                    ApplianceEvent("kettle", "07:00:00", 120),
                    ApplianceEvent("microwave", "09:00:00", 180),
                )
            days.append(DaySchedule(day_offset=d, events=events))

        defn = ScenarioDefinition(scenario_id="BASELINE_MICROWAVE_20D", days=tuple(days))
        plan = compile_schedule(defn, self.base_date)

        # 전체 슬롯 수는 28 * 86,400 = 2,419,200
        self.assertEqual(plan.total_virtual_slots, 2_419_200)
        self.assertEqual(len(plan.day_plans), 28)

        # 슬롯별 publish mask 필드가 plan 객체에 존재하지 않음 확인
        self.assertFalse(hasattr(plan, "_publish_mask_by_cycle"))

        # transitions_by_cycle 매핑 크기는 전체 슬롯이 아닌 전환 이벤트가 있는 사이클(4개)만 보관
        self.assertEqual(len(plan.transitions_by_cycle), 4)

        # 산술 조회가 빠르고 정확하게 수행됨
        self.assertTrue(plan.should_publish(1))
        self.assertTrue(plan.should_publish(2_419_200))
        self.assertEqual(plan.virtual_time_at(1), datetime(2026, 8, 21, 0, 0, 0, tzinfo=KST))
        self.assertEqual(plan.virtual_time_at(2_419_200), datetime(2026, 9, 17, 23, 59, 59, tzinfo=KST))

    # ----------------------------------------------------
    # 4. start_cycle 오프셋 지정 검증
    # ----------------------------------------------------
    def test_start_cycle_offset(self):
        """start_cycle=500 전달 시 first_cycle=500, first virtual_time=start_date 00:00:00 KST, last_cycle 정확 계산"""
        day = DaySchedule(day_offset=0, events=())
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day,))
        plan = compile_schedule(defn, self.base_date, start_cycle=500)

        self.assertEqual(plan.first_cycle, 500)
        self.assertEqual(plan.last_cycle, 500 + 86400 - 1)
        self.assertEqual(plan.virtual_time_at(500), datetime(2026, 9, 17, 0, 0, 0, tzinfo=KST))
        self.assertEqual(plan.virtual_time_at(plan.last_cycle), datetime(2026, 9, 17, 23, 59, 59, tzinfo=KST))

    # ----------------------------------------------------
    # 5. cycle 범위 밖 조회 시 CycleOutOfRangeError 검증
    # ----------------------------------------------------
    def test_cycle_out_of_range_error(self):
        """first_cycle-1, last_cycle+1 및 비정수 cycle에 대해 CycleOutOfRangeError 발생 검증"""
        day = DaySchedule(day_offset=0, events=())
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day,))
        plan = compile_schedule(defn, self.base_date, start_cycle=10)

        out_of_bounds = [9, 86410, 0, -1, True, False, "10"]
        for bad_cycle in out_of_bounds:
            with self.assertRaises(CycleOutOfRangeError):
                plan.should_publish(bad_cycle)
            with self.assertRaises(CycleOutOfRangeError):
                plan.virtual_time_at(bad_cycle)
            with self.assertRaises(CycleOutOfRangeError):
                plan.transitions_at(bad_cycle)

    # ----------------------------------------------------
    # 6. 입력 순서 무관 canonical 결과 일치 검증 (결정성)
    # ----------------------------------------------------
    def test_canonical_sorting_order_invariance(self):
        """days, events, omission_ranges의 입력 순서가 달라도 동일한 canonical CompiledExecutionPlan 생성"""
        # 정의 A: 자연 순서
        ev1 = ApplianceEvent("kettle", "07:00:00", 120)
        ev2 = ApplianceEvent("microwave", "09:00:00", 180)
        om1 = OmissionRange(1000, 2000)
        om2 = OmissionRange(5000, 6000)
        day0 = DaySchedule(day_offset=0, events=(ev1, ev2), omission_ranges=(om1, om2))
        day1 = DaySchedule(day_offset=1, events=())
        defn_a = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day0, day1))
        plan_a = compile_schedule(defn_a, self.base_date)

        # 정의 B: 역순으로 전달 (days 역순, events 역순, omission_ranges 역순)
        ev1_rev = ApplianceEvent("KETTLE", "07:00:00", 120)
        ev2_rev = ApplianceEvent("MICROWAVE", "09:00:00", 180)
        day1_rev = DaySchedule(day_offset=1, events=())
        day0_rev = DaySchedule(day_offset=0, events=(ev2_rev, ev1_rev), omission_ranges=(om2, om1))
        defn_b = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day1_rev, day0_rev))
        plan_b = compile_schedule(defn_b, self.base_date)

        self.assertEqual(plan_a.total_virtual_slots, plan_b.total_virtual_slots)
        self.assertEqual(plan_a.total_planned_publish_samples, plan_b.total_planned_publish_samples)
        self.assertEqual(plan_a.total_planned_omitted_samples, plan_b.total_planned_omitted_samples)
        self.assertEqual(plan_a.day_plans, plan_b.day_plans)
        self.assertEqual(dict(plan_a.transitions_by_cycle), dict(plan_b.transitions_by_cycle))

    # ----------------------------------------------------
    # 7. 중첩 컬렉션 원소 타입 검증
    # ----------------------------------------------------
    def test_nested_collection_element_types(self):
        """days, events, omission_ranges에 잘못된 타입의 원소 전달 시 ScheduleError 발생"""
        with self.assertRaises(ScheduleError):
            ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=("not_a_day",))

        with self.assertRaises(ScheduleError):
            DaySchedule(day_offset=0, events=("not_an_event",))

        with self.assertRaises(ScheduleError):
            DaySchedule(day_offset=0, omission_ranges=("not_a_range",))

    # ----------------------------------------------------
    # 8. 계획 발행 수와 런타임 카운터 개념 분리 필드명 검증
    # ----------------------------------------------------
    def test_planned_vs_runtime_actual_count_distinction(self):
        """컴파일 계획은 planned 수량 명칭을 사용하며 total_virtual_slots = planned_publish + planned_omitted 불변식 만족"""
        day = DaySchedule(day_offset=0, events=(), omission_ranges=(OmissionRange(82079, 86400),))
        defn = ScenarioDefinition(scenario_id="ACTIVITY_INSUFFICIENT", days=(day,))
        plan = compile_schedule(defn, self.base_date)

        self.assertTrue(hasattr(plan, "total_planned_publish_samples"))
        self.assertTrue(hasattr(plan, "total_planned_omitted_samples"))
        self.assertTrue(hasattr(plan, "total_virtual_slots"))
        self.assertEqual(
            plan.total_virtual_slots,
            plan.total_planned_publish_samples + plan.total_planned_omitted_samples
        )
        self.assertEqual(plan.day_plans[0].planned_publish_samples, 82079)
        self.assertEqual(plan.day_plans[0].planned_omitted_samples, 4321)

    # ----------------------------------------------------
    # 9. ACTIVITY_INSUFFICIENT 후행 결측 시각 및 개수 계산
    # ----------------------------------------------------
    def test_partial_omission_trailing_gap_calculation(self):
        """ACTIVITY_INSUFFICIENT: [82079, 86400) 미발행 시 발행 82,079건, 미발행 4,321건 계산"""
        day = DaySchedule(day_offset=0, events=(), omission_ranges=(OmissionRange(82079, 86400),), publish_samples=82079)
        defn = ScenarioDefinition(scenario_id="ACTIVITY_INSUFFICIENT", days=(day,))
        plan = compile_schedule(defn, self.base_date)

        self.assertEqual(plan.total_planned_publish_samples, 82079)
        self.assertEqual(plan.total_planned_omitted_samples, 4321)

    # ----------------------------------------------------
    # 10. 결측 슬롯에서도 가상 시각과 cycle 연속 진행 검증
    # ----------------------------------------------------
    def test_omitted_slots_advance_virtual_time_and_cycle(self):
        """결측 슬롯에서도 should_publish만 False이며 cycle과 virtual_time은 매초 정상 진행"""
        day = DaySchedule(day_offset=0, events=(), omission_ranges=(OmissionRange(82079, 86400),))
        defn = ScenarioDefinition(scenario_id="ACTIVITY_INSUFFICIENT", days=(day,))
        plan = compile_schedule(defn, self.base_date)

        # cycle 82079 (slot 82078)은 발행 대상 (22:47:58)
        self.assertTrue(plan.should_publish(82079))
        self.assertEqual(plan.virtual_time_at(82079), datetime(2026, 9, 17, 22, 47, 58, tzinfo=KST))

        # cycle 82080 (slot 82079)은 미발행 대상 (22:47:59)
        self.assertFalse(plan.should_publish(82080))
        self.assertEqual(plan.virtual_time_at(82080), datetime(2026, 9, 17, 22, 47, 59, tzinfo=KST))

        # cycle 82081 (slot 82080)도 미발행 대상 (22:48:00)
        self.assertFalse(plan.should_publish(82081))
        self.assertEqual(plan.virtual_time_at(82081), datetime(2026, 9, 17, 22, 48, 0, tzinfo=KST))

    # ----------------------------------------------------
    # 11. CompiledExecutionPlan 불변성 검증
    # ----------------------------------------------------
    def test_plan_immutability_mutation_blocked(self):
        """plan.transitions_by_cycle 수정 시 TypeError 발생 및 frozen 필드 갱신 차단"""
        ev = ApplianceEvent("kettle", "07:00:00", 120)
        day = DaySchedule(day_offset=0, events=(ev,))
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day,))
        plan = compile_schedule(defn, self.base_date)

        with self.assertRaises(TypeError):
            plan.transitions_by_cycle[10] = ()  # MappingProxyType 수정 시도 차단

        with self.assertRaises(Exception):  # FrozenInstanceError
            plan.total_virtual_slots = 0

        with self.assertRaises(Exception):  # FrozenInstanceError
            plan.day_plans = ()

    # ----------------------------------------------------
    # 12. dict 입력의 알 수 없는 필드 거절 검증
    # ----------------------------------------------------
    def test_unknown_input_fields_rejected(self):
        """raw dict 입력에 알 수 없는 최상위 및 중첩 필드가 포함되어 있으면 UnknownFieldError 발생"""
        # 최상위 알 수 없는 필드
        raw_bad_top = {
            "scenario": "ACTIVITY_NORMAL",
            "typo_field": "error",
            "days": [{"day_offset": 0, "events": []}]
        }
        with self.assertRaises(UnknownFieldError):
            parse_scenario_definition(raw_bad_top)

        # days 내부 알 수 없는 필드
        raw_bad_day = {
            "scenario": "ACTIVITY_NORMAL",
            "days": [{"day_offset": 0, "unknown_day_key": 123}]
        }
        with self.assertRaises(UnknownFieldError):
            parse_scenario_definition(raw_bad_day)

        # events 내부 알 수 없는 필드
        raw_bad_ev = {
            "scenario": "ACTIVITY_NORMAL",
            "days": [{
                "day_offset": 0,
                "events": [{"appliance": "kettle", "start_time": "07:00:00", "duration_seconds": 120, "extra_ev": True}]
            }]
        }
        with self.assertRaises(UnknownFieldError):
            parse_scenario_definition(raw_bad_ev)

    # ----------------------------------------------------
    # 13. base_date에 datetime.datetime 전달 시 명시적 거절
    # ----------------------------------------------------
    def test_datetime_datetime_rejected_for_base_date(self):
        """base_date에 datetime.datetime 객체 전달 시 암묵적 date 변환 없이 ScheduleError 발생"""
        day = DaySchedule(day_offset=0, events=())
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day,))

        dt_input = datetime(2026, 9, 17, 10, 30, 0, tzinfo=KST)
        with self.assertRaises(ScheduleError) as ctx:
            compile_schedule(defn, dt_input)
        self.assertIn("datetime.datetime 거절", str(ctx.exception))

        # 정상 date 객체 및 YYYY-MM-DD 문자열은 허용
        self.assertIsNotNone(compile_schedule(defn, date(2026, 9, 17)))
        self.assertIsNotNone(compile_schedule(defn, "2026-09-17"))

    # ----------------------------------------------------
    # 14. 비연속 day_offset 거절 검증
    # ----------------------------------------------------
    def test_non_consecutive_day_offset_rejected(self):
        """[-2, 0]처럼 중간 날짜가 누락된 비연속 day_offset 거절"""
        day_m2 = DaySchedule(day_offset=-2, events=())
        day_0 = DaySchedule(day_offset=0, events=())

        with self.assertRaises(DayOffsetSequenceError) as ctx:
            ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day_m2, day_0))
        self.assertIn("연속된 정수", str(ctx.exception))

        # 중복 day_offset 거절
        day_dup1 = DaySchedule(day_offset=0, events=())
        day_dup2 = DaySchedule(day_offset=0, events=())
        with self.assertRaises(DayOffsetSequenceError):
            ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day_dup1, day_dup2))

    # ----------------------------------------------------
    # 15. 단일 이벤트 ON/OFF 사이클 및 시각 정확성 검증
    # ----------------------------------------------------
    def test_single_event_on_off_cycle_accuracy(self):
        """07:00:00(25,200초) 120초 가동 시 ON=25201, OFF=25321 정확성 검증"""
        ev = ApplianceEvent("kettle", "07:00:00", 120)
        day = DaySchedule(day_offset=0, events=(ev,))
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day,))
        plan = compile_schedule(defn, self.base_date, start_cycle=1)

        # cycle 25201 (second 25200)에 ON 전환
        transitions_on = plan.transitions_at(25201)
        self.assertEqual(len(transitions_on), 1)
        self.assertEqual(transitions_on[0].transition_type, TransitionType.ON)
        self.assertEqual(transitions_on[0].appliance, "kettle")
        self.assertEqual(transitions_on[0].second_of_day, 25200)
        self.assertEqual(transitions_on[0].virtual_time, datetime(2026, 9, 17, 7, 0, 0, tzinfo=KST))

        # cycle 25321 (second 25320)에 OFF 전환
        transitions_off = plan.transitions_at(25321)
        self.assertEqual(len(transitions_off), 1)
        self.assertEqual(transitions_off[0].transition_type, TransitionType.OFF)
        self.assertEqual(transitions_off[0].appliance, "kettle")
        self.assertEqual(transitions_off[0].second_of_day, 25320)
        self.assertEqual(transitions_off[0].virtual_time, datetime(2026, 9, 17, 7, 2, 0, tzinfo=KST))

    # ----------------------------------------------------
    # 16. 자정 직후 / 자정 직전 OFF 샘플 부족 거절 검증
    # ----------------------------------------------------
    def test_boundary_off_samples_violations(self):
        """00:00:00~00:00:02 시작 및 23:59:58~23:59:59 종료 이벤트 거절 검증"""
        # start_sec < 3 거절
        for bad_time in ["00:00:00", "00:00:01", "00:00:02"]:
            ev_bad = ApplianceEvent("kettle", bad_time, 10)
            with self.assertRaises(SampleBoundaryViolationError):
                DaySchedule(day_offset=0, events=(ev_bad,))

        # 00:00:03 시작은 정상 허용
        ev_ok_start = ApplianceEvent("kettle", "00:00:03", 10)
        self.assertIsNotNone(DaySchedule(day_offset=0, events=(ev_ok_start,)))

        # end_sec > SECONDS_PER_DAY - 3 거절
        ev_late = ApplianceEvent("kettle", "23:58:00", 118)  # end = SECONDS_PER_DAY - 2 = 86398
        with self.assertRaises(SampleBoundaryViolationError):
            DaySchedule(day_offset=0, events=(ev_late,))

        # end_sec <= SECONDS_PER_DAY - 3 정상 허용
        ev_ok_end = ApplianceEvent("kettle", "23:58:00", 117)  # end = SECONDS_PER_DAY - 3 = 86397
        self.assertIsNotNone(DaySchedule(day_offset=0, events=(ev_ok_end,)))

        # 자정 초과 거절
        ev_overflow = ApplianceEvent("kettle", "23:59:00", 70)  # end = 86410 > SECONDS_PER_DAY
        with self.assertRaises(DateBoundaryViolationError):
            DaySchedule(day_offset=0, events=(ev_overflow,))

    # ----------------------------------------------------
    # 17. 날짜 경계에서의 cycle 및 virtual_time 연속성 검증
    # ----------------------------------------------------
    def test_day_boundary_cycle_and_virtual_time_continuity(self):
        """Day 0 23:59:59(slot 86399)와 Day 1 00:00:00(slot 86400) 간 1초 연속성 검증"""
        day0 = DaySchedule(day_offset=0, events=())
        day1 = DaySchedule(day_offset=1, events=())
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day0, day1))
        plan = compile_schedule(defn, self.base_date, start_cycle=1)

        t_end_day0 = plan.virtual_time_at(86400)
        t_start_day1 = plan.virtual_time_at(86401)

        self.assertEqual(t_end_day0, datetime(2026, 9, 17, 23, 59, 59, tzinfo=KST))
        self.assertEqual(t_start_day1, datetime(2026, 9, 18, 0, 0, 0, tzinfo=KST))
        self.assertEqual(t_start_day1 - t_end_day0, timedelta(seconds=1))

    # ----------------------------------------------------
    # 18. omission range가 이벤트 ON 이전 3개 샘플과 겹치면 거절
    # ----------------------------------------------------
    def test_omission_range_overlaps_prior_off_samples_rejected(self):
        """가전 ON 전 3개 샘플 [start_sec-3, start_sec)이 omission_ranges와 겹치면 EventEvidenceOmittedError 발생"""
        ev = ApplianceEvent("kettle", "01:00:00", 60)  # start=3600, end=3660, prot_start=3597
        om = OmissionRange(3595, 3598)  # 3597과 겹침
        with self.assertRaises(EventEvidenceOmittedError) as ctx:
            DaySchedule(day_offset=0, events=(ev,), omission_ranges=(om,))
        self.assertIn("보호 구간", str(ctx.exception))

    # ----------------------------------------------------
    # 19. omission range가 가전 ON 세션과 겹치면 거절
    # ----------------------------------------------------
    def test_omission_range_overlaps_on_session_rejected(self):
        """가전 ON 가동 구간 [start_sec, end_sec)이 omission_ranges와 겹치면 EventEvidenceOmittedError 발생"""
        ev = ApplianceEvent("kettle", "01:00:00", 60)  # [3600, 3660)
        om = OmissionRange(3620, 3640)
        with self.assertRaises(EventEvidenceOmittedError) as ctx:
            DaySchedule(day_offset=0, events=(ev,), omission_ranges=(om,))
        self.assertIn("보호 구간", str(ctx.exception))

    # ----------------------------------------------------
    # 20. omission range가 이벤트 OFF 이후 3개 샘플과 겹치면 거절
    # ----------------------------------------------------
    def test_omission_range_overlaps_post_off_samples_rejected(self):
        """가전 OFF 후 3개 샘플 [end_sec, end_sec+3)이 omission_ranges와 겹치면 EventEvidenceOmittedError 발생"""
        ev = ApplianceEvent("kettle", "01:00:00", 60)  # start=3600, end=3660, prot_end=3663
        om = OmissionRange(3662, 3680)  # 3662와 겹침
        with self.assertRaises(EventEvidenceOmittedError) as ctx:
            DaySchedule(day_offset=0, events=(ev,), omission_ranges=(om,))
        self.assertIn("보호 구간", str(ctx.exception))

    # ----------------------------------------------------
    # 21. 보호 구간 밖의 결측 구간은 정상 허용
    # ----------------------------------------------------
    def test_omission_range_outside_protected_window_allowed(self):
        """보호 구간 [start_sec-3, end_sec+3) 바깥에 위치한 omission_ranges는 정상 통과"""
        ev = ApplianceEvent("kettle", "01:00:00", 60)  # start=3600, end=3660, 보호=[3597, 3663)
        om_before = OmissionRange(1000, 3597)  # 3597 직전까지
        om_after = OmissionRange(3663, 5000)   # 3663부터
        day = DaySchedule(day_offset=0, events=(ev,), omission_ranges=(om_before, om_after))
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day,))
        plan = compile_schedule(defn, self.base_date)
        self.assertIsNotNone(plan)

    # ----------------------------------------------------
    # 22. ACTIVITY_INSUFFICIENT 정확한 발행/미발행 타임스탬프 검증
    # ----------------------------------------------------
    def test_activity_insufficient_sample_timestamps(self):
        """ACTIVITY_INSUFFICIENT의 마지막 발행 시각 22:47:58, 첫 미발행 시각 22:47:59 검증"""
        day = DaySchedule(day_offset=0, events=(), omission_ranges=(OmissionRange(82079, 86400),))
        defn = ScenarioDefinition(scenario_id="ACTIVITY_INSUFFICIENT", days=(day,))
        plan = compile_schedule(defn, self.base_date, start_cycle=1)

        # cycle 82079 = second 82078 -> 22:47:58 KST (발행 대상)
        self.assertTrue(plan.should_publish(82079))
        self.assertEqual(plan.virtual_time_at(82079).strftime("%H:%M:%S"), "22:47:58")

        # cycle 82080 = second 82079 -> 22:47:59 KST (첫 미발행 대상)
        self.assertFalse(plan.should_publish(82080))
        self.assertEqual(plan.virtual_time_at(82080).strftime("%H:%M:%S"), "22:47:59")

    # ----------------------------------------------------
    # 23. 동일 초 다중 전환의 명시적 TRANSITION_PRIORITY (OFF 우선) 검증
    # ----------------------------------------------------
    def test_transition_sorting_off_priority(self):
        """동일 초에 가전 A OFF, 가전 B ON 전환 시 TRANSITION_PRIORITY에 의해 OFF가 ON보다 먼저 정렬"""
        # ev_a: 07:00:00 ~ 07:02:00 (종료 25320초에 OFF)
        ev_a = ApplianceEvent("kettle", "07:00:00", 120)
        # ev_b: 07:02:00 ~ 07:05:00 (시작 25320초에 ON) - 다른 가전
        ev_b = ApplianceEvent("microwave", "07:02:00", 180)
        day = DaySchedule(day_offset=0, events=(ev_a, ev_b))
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day,))
        plan = compile_schedule(defn, self.base_date, start_cycle=1)

        # cycle 25321 (second 25320)에 2개 전환
        transitions = plan.transitions_at(25321)
        self.assertEqual(len(transitions), 2)
        self.assertEqual(transitions[0].transition_type, TransitionType.OFF)
        self.assertEqual(transitions[0].appliance, "kettle")
        self.assertEqual(transitions[1].transition_type, TransitionType.ON)
        self.assertEqual(transitions[1].appliance, "microwave")

    # ----------------------------------------------------
    # 24. 동일 가전 사용 일정 중복 및 3개 OFF 샘플 간격 검증
    # ----------------------------------------------------
    def test_same_appliance_overlapping_events_rejected(self):
        """동일 가전 중복 및 간격 < 3초 거절, 3초 이상 간격 정상 허용"""
        # 겹침 거절
        ev1 = ApplianceEvent("kettle", "07:00:00", 120)  # 25200 ~ 25320
        ev_overlap = ApplianceEvent("kettle", "07:01:30", 120)  # 25290 시작 (겹침)
        with self.assertRaises(EventOverlapError):
            DaySchedule(day_offset=0, events=(ev1, ev_overlap))

        # 간격 2초 거절 (3개 미만)
        # 25320 종료 후 25322 시작 (간격 2초)
        ev_close = ApplianceEvent("kettle", "07:02:02", 60)
        with self.assertRaises(EventOverlapError):
            DaySchedule(day_offset=0, events=(ev1, ev_close))

        # 간격 3초 이상 정상 허용
        # 25320 종료 후 25323 시작 (간격 3초)
        ev_ok = ApplianceEvent("kettle", "07:02:03", 60)
        day_ok = DaySchedule(day_offset=0, events=(ev1, ev_ok))
        self.assertEqual(len(day_ok.events), 2)


class TestScheduleEngineTypesAndApplianceConsistency(unittest.TestCase):
    """가전 일치성, 파싱 및 타입 방어 테스트"""

    def test_supported_appliances_matches_device_profiles(self):
        """SUPPORTED_APPLIANCES가 engine.profiles.DEVICE_PROFILES와 단일 출처로 일치함을 검증"""
        expected = tuple(sorted(DEVICE_PROFILES.keys()))
        self.assertEqual(SUPPORTED_APPLIANCES, expected)
        self.assertIn("kettle", SUPPORTED_APPLIANCES)
        self.assertIn("microwave", SUPPORTED_APPLIANCES)
        self.assertIn("induction", SUPPORTED_APPLIANCES)
        self.assertIn("iron", SUPPORTED_APPLIANCES)
        self.assertIn("hair_dryer", SUPPORTED_APPLIANCES)
        self.assertIn("vacuum_cleaner", SUPPORTED_APPLIANCES)

    def test_invalid_appliance_and_duration_rejected(self):
        """미지원 가전명 및 duration_seconds bool/부동소수점/1 미만/3 미만 거절 검증"""
        # 미지원 가전
        with self.assertRaises(InvalidApplianceError):
            ApplianceEvent("air_conditioner", "07:00:00", 120)

        # bool duration
        with self.assertRaises(InvalidDurationError):
            ApplianceEvent("kettle", "07:00:00", True)

        # float duration
        with self.assertRaises(InvalidDurationError):
            ApplianceEvent("kettle", "07:00:00", 120.5)

        # duration < 1
        with self.assertRaises(InvalidDurationError):
            ApplianceEvent("kettle", "07:00:00", 0)

        # duration < 3
        with self.assertRaises(SampleBoundaryViolationError):
            ApplianceEvent("kettle", "07:00:00", 2)

    def test_invalid_scenario_id_and_empty_days_rejected(self):
        """scenario_id 소문자/특수문자/공백/개행 거절 및 빈 days 거절 (re.fullmatch)"""
        # 요구사항 거절 대상: "ACTIVITY_NORMAL\n", "ACTIVITY_NORMAL ", " ACTIVITY_NORMAL", "activity_normal"
        rejected_ids = [
            "ACTIVITY_NORMAL\n",
            "ACTIVITY_NORMAL ",
            " ACTIVITY_NORMAL",
            "activity_normal",
            "ACTIVITY NORMAL",
            "",
            "123_NORMAL",
            "ACTIVITY@NORMAL",
        ]
        for bad_id in rejected_ids:
            with self.assertRaises(InvalidScenarioIdError, msg=f"Failed to reject: {bad_id!r}"):
                ScenarioDefinition(scenario_id=bad_id, days=(DaySchedule(0),))

        with self.assertRaises(ScheduleError):
            ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=())

    def test_scenario_and_scenario_id_conflict_rejected(self):
        """scenario_id와 scenario 필드의 존재 여부 및 충돌 규칙 검증"""
        # 1. 둘 다 없음 -> InvalidScenarioIdError
        with self.assertRaises(InvalidScenarioIdError):
            parse_scenario_definition({"days": [{"day_offset": 0}]})

        # 2. 둘 다 존재 (동일 값) -> ScheduleError 거절
        with self.assertRaises(ScheduleError) as ctx:
            parse_scenario_definition({
                "scenario_id": "ACTIVITY_NORMAL",
                "scenario": "ACTIVITY_NORMAL",
                "days": [{"day_offset": 0}],
            })
        self.assertIn("동시에 지정할 수 없습니다", str(ctx.exception))

        # 3. 둘 다 존재 (다른 값) -> ScheduleError 거절
        with self.assertRaises(ScheduleError):
            parse_scenario_definition({
                "scenario_id": "ACTIVITY_NORMAL",
                "scenario": "ACTIVITY_PEAK",
                "days": [{"day_offset": 0}],
            })

        # 4. scenario_id만 존재 -> 정상 허용
        defn1 = parse_scenario_definition({
            "scenario_id": "ACTIVITY_NORMAL",
            "days": [{"day_offset": 0}],
        })
        self.assertEqual(defn1.scenario_id, "ACTIVITY_NORMAL")

        # 5. scenario만 존재 -> 하위 호환 별칭 정상 허용
        defn2 = parse_scenario_definition({
            "scenario": "ACTIVITY_NORMAL",
            "days": [{"day_offset": 0}],
        })
        self.assertEqual(defn2.scenario_id, "ACTIVITY_NORMAL")

    def test_description_type_validation_rejected(self):
        """description에 문자열이 아닌 타입(None, True, 123, {}, []) 전달 시 ScheduleError 발생 검증"""
        invalid_descriptions = [None, True, False, 123, {}, []]

        for bad_desc in invalid_descriptions:
            # 1. 직접 ScenarioDefinition 생성 시 거절
            with self.assertRaises(ScheduleError, msg=f"Direct creation should reject {bad_desc!r}"):
                ScenarioDefinition(
                    scenario_id="ACTIVITY_NORMAL",
                    days=(DaySchedule(0),),
                    description=bad_desc,
                )

            # 2. parse_scenario_definition 호출 시 거절 (강제 str 변환 없이 타입 전달)
            raw = {
                "scenario_id": "ACTIVITY_NORMAL",
                "description": bad_desc,
                "days": [{"day_offset": 0}],
            }
            with self.assertRaises(ScheduleError, msg=f"Parsing should reject {bad_desc!r}"):
                parse_scenario_definition(raw)

    def test_seconds_per_day_constant(self):
        """SECONDS_PER_DAY 상수가 86,400이며 경계값 계산에 올바르게 사용됨을 검증"""
        self.assertEqual(SECONDS_PER_DAY, 86_400)
        self.assertEqual(SECONDS_PER_DAY - 3, 86_397)

    def test_transition_priority_immutability(self):
        """TRANSITION_PRIORITY가 MappingProxyType으로 정의되어 외부 변조 시 TypeError 발생 검증"""
        with self.assertRaises(TypeError):
            TRANSITION_PRIORITY[TransitionType.ON] = 99
        with self.assertRaises(TypeError):
            TRANSITION_PRIORITY[TransitionType.OFF] = -1

    def test_bool_day_offset_and_start_cycle_rejected(self):
        """day_offset 및 start_cycle에 bool 전달 시 명시적 거절"""
        with self.assertRaises(DayOffsetSequenceError):
            DaySchedule(day_offset=True)

        day = DaySchedule(day_offset=0)
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day,))
        with self.assertRaises(ScheduleError):
            compile_schedule(defn, date(2026, 9, 17), start_cycle=True)
        with self.assertRaises(ScheduleError):
            compile_schedule(defn, date(2026, 9, 17), start_cycle=0)

    def test_parse_scenario_definition_dict_roundtrip(self):
        """raw dict 입력 파싱 결과와 dataclass 직접 생성 결과가 100% 동일한 canonical 플랜을 생성함"""
        raw_dict = {
            "scenario_id": "ACTIVITY_NORMAL",
            "description": "라운드트립 테스트",
            "days": [
                {
                    "day_offset": 0,
                    "events": [
                        {"appliance": "KETTLE", "start_time": "07:00:00", "duration_seconds": 120}
                    ],
                    "omission_ranges": [
                        {"start_second": 1000, "end_second": 2000}
                    ],
                    "publish_samples": 85400
                }
            ]
        }
        defn_parsed = parse_scenario_definition(raw_dict)
        plan_parsed = compile_schedule(defn_parsed, "2026-09-17")

        ev_direct = ApplianceEvent("kettle", "07:00:00", 120)
        day_direct = DaySchedule(
            day_offset=0,
            events=(ev_direct,),
            omission_ranges=(OmissionRange(1000, 2000),),
            publish_samples=85400
        )
        defn_direct = ScenarioDefinition(
            scenario_id="ACTIVITY_NORMAL",
            days=(day_direct,),
            description="라운드트립 테스트"
        )
        plan_direct = compile_schedule(defn_direct, date(2026, 9, 17))

        self.assertEqual(plan_parsed.total_virtual_slots, plan_direct.total_virtual_slots)
        self.assertEqual(plan_parsed.total_planned_publish_samples, plan_direct.total_planned_publish_samples)
        self.assertEqual(plan_parsed.day_plans, plan_direct.day_plans)
        self.assertEqual(dict(plan_parsed.transitions_by_cycle), dict(plan_direct.transitions_by_cycle))


if __name__ == "__main__":
    unittest.main()
