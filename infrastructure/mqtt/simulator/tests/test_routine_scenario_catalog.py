"""
28일 루틴 변화 시나리오 카탈로그(routine_scenario_catalog.py) 단위 및 회귀 테스트 스위트

검증 내용:
1. 카탈로그 공개 API 계약: 정확한 3개 canonical ID, 고정 순서, 불변 MappingProxyType, 비문자열/미등록 ID 거절
2. 공통 28일 일정 구조: 28개 DaySchedule, day_offset 0~27 연속, 매일 86400개 발행, 결측 0, 전자레인지 60초 1회
3. ROUTINE_CHANGED_LATER 명세: 이전 21일 08:00, 최근 7일 10:30, median 및 signed circular shift +150분(+9000초)
4. ROUTINE_CHANGED_EARLIER 명세: 이전 21일 10:30, 최근 7일 08:00, median 및 signed circular shift -150분(-9000초)
5. ROUTINE_CHANGED_WITHIN_THRESHOLD 명세: 이전 21일 08:00, 최근 7일 09:30, shift +90분(+5400초), 120분 미만
6. 날짜 구간 및 KST 타임존: D=2026-09-16 기준 D-27(2026-08-20) 컴파일, D-7/D-6/D 매핑, KST aware
7. Sparse 메모리 및 사이클 산술 검증: 56개 transition, 2,419,200개 가상 슬롯, 슬롯 배열 미생성, plan 공개 accessor 활용
8. MQTT 페이로드 정답 라벨 미유출 및 안전한 초기 틱 결정론 검증
"""

from datetime import date, datetime, timedelta, timezone
import math
import statistics
import unittest

from engine.routine_scenario_catalog import (
    ROUTINE_SCENARIO_IDS,
    UnknownRoutineScenarioError,
    list_routine_scenario_ids,
    get_routine_scenario_definition,
    get_routine_scenario_catalog,
)
from engine.schedule import (
    ScenarioDefinition,
    DaySchedule,
    ApplianceEvent,
    TransitionType,
    compile_schedule,
)
from engine.schedule_runtime import DeterministicScheduleRunner
from engine.schedule_publisher import (
    build_scheduled_topic,
    build_scheduled_power_payload,
)

KST = timezone(timedelta(hours=9))


class TestRoutineScenarioCatalogAPI(unittest.TestCase):
    """28일 루틴 카탈로그 공개 API 및 도메인 예외 계약 검증"""

    def test_list_routine_scenario_ids_exact_three(self):
        """정확히 3개 시나리오 ID를 불변 튜플로 반환하는지 확인"""
        ids = list_routine_scenario_ids()
        self.assertIsInstance(ids, tuple)
        self.assertEqual(len(ids), 3)

    def test_list_routine_scenario_ids_order_fixed(self):
        """반환 튜플의 고정 순서 및 ROUTINE_SCENARIO_IDS 상수와의 일치 확인"""
        expected = (
            "ROUTINE_CHANGED_LATER",
            "ROUTINE_CHANGED_EARLIER",
            "ROUTINE_CHANGED_WITHIN_THRESHOLD",
        )
        self.assertEqual(list_routine_scenario_ids(), expected)
        self.assertEqual(ROUTINE_SCENARIO_IDS, expected)

    def test_get_routine_scenario_definition_repeated_calls_identical(self):
        """동일 ID 반복 호출 시 동일 인스턴스 반환 확인"""
        def1 = get_routine_scenario_definition("ROUTINE_CHANGED_LATER")
        def2 = get_routine_scenario_definition("ROUTINE_CHANGED_LATER")
        self.assertIs(def1, def2)
        self.assertEqual(def1, def2)

    def test_catalog_mapping_is_immutable_proxy(self):
        """get_routine_scenario_catalog()이 MappingProxyType으로 반환되어 수정 시도 시 TypeError 발생 확인"""
        cat = get_routine_scenario_catalog()
        with self.assertRaises(TypeError):
            cat["NEW_SCENARIO"] = get_routine_scenario_definition("ROUTINE_CHANGED_LATER")  # type: ignore
        with self.assertRaises(TypeError):
            del cat["ROUTINE_CHANGED_LATER"]  # type: ignore

    def test_unknown_scenario_id_rejected(self):
        """미등록 ID 조회 시 UnknownRoutineScenarioError 발생 확인"""
        with self.assertRaises(UnknownRoutineScenarioError):
            get_routine_scenario_definition("UNKNOWN_ROUTINE")
        with self.assertRaises(UnknownRoutineScenarioError):
            get_routine_scenario_definition("ACTIVITY_NORMAL")

    def test_lowercase_scenario_id_rejected(self):
        """소문자 ID 조회 시 UnknownRoutineScenarioError 발생 확인"""
        with self.assertRaises(UnknownRoutineScenarioError):
            get_routine_scenario_definition("routine_changed_later")
        with self.assertRaises(UnknownRoutineScenarioError):
            get_routine_scenario_definition("routine_changed_earlier")

    def test_scenario_id_with_whitespace_rejected(self):
        """앞뒤 공백 포함 ID 조회 시 UnknownRoutineScenarioError 발생 확인"""
        with self.assertRaises(UnknownRoutineScenarioError):
            get_routine_scenario_definition(" ROUTINE_CHANGED_LATER")
        with self.assertRaises(UnknownRoutineScenarioError):
            get_routine_scenario_definition("ROUTINE_CHANGED_LATER ")
        with self.assertRaises(UnknownRoutineScenarioError):
            get_routine_scenario_definition("ROUTINE_CHANGED_LATER\n")

    def test_non_string_types_rejected(self):
        """None, bool, 숫자, 리스트 등 문자열이 아닌 타입 전달 시 UnknownRoutineScenarioError 발생 확인"""
        invalid_types = [None, True, False, 123, 45.6, [], {}, ()]
        for val in invalid_types:
            with self.subTest(val=val):
                with self.assertRaises(UnknownRoutineScenarioError):
                    get_routine_scenario_definition(val)  # type: ignore

    def test_all_catalog_entries_are_scenario_definitions(self):
        """카탈로그의 모든 항목이 ScenarioDefinition 인스턴스이며 scenario_id가 키와 일치하는지 확인"""
        cat = get_routine_scenario_catalog()
        for scenario_id, defn in cat.items():
            self.assertIsInstance(defn, ScenarioDefinition)
            self.assertEqual(defn.scenario_id, scenario_id)


class TestCommonRoutineStructureAndInvariants(unittest.TestCase):
    """모든 28일 루틴 시나리오의 공통 불변 구조 및 계약 검증"""

    def test_all_scenarios_have_exact_28_days(self):
        """모든 루틴 시나리오가 정확히 28개의 DaySchedule을 갖는지 확인"""
        for sid in ROUTINE_SCENARIO_IDS:
            defn = get_routine_scenario_definition(sid)
            self.assertEqual(len(defn.days), 28, f"{sid} must have 28 days")

    def test_day_offsets_contiguous_sequence(self):
        """모든 시나리오의 day_offset이 0부터 27까지 연속된 정수 튜플인지 확인"""
        expected_offsets = tuple(range(28))
        for sid in ROUTINE_SCENARIO_IDS:
            defn = get_routine_scenario_definition(sid)
            actual_offsets = tuple(d.day_offset for d in defn.days)
            self.assertEqual(actual_offsets, expected_offsets, f"{sid} day_offsets mismatch")

    def test_each_day_exact_single_microwave_event_60s(self):
        """모든 날짜에 전자레인지 60초 이벤트가 정확히 1개 선언되어 있는지 확인"""
        for sid in ROUTINE_SCENARIO_IDS:
            defn = get_routine_scenario_definition(sid)
            for day in defn.days:
                self.assertEqual(len(day.events), 1)
                event = day.events[0]
                self.assertEqual(event.appliance, "microwave")
                self.assertEqual(event.duration_seconds, 60)

    def test_each_day_full_publish_no_omissions(self):
        """모든 날짜가 결측 구간 없이 86,400개 발행 후보를 가지는지 확인"""
        for sid in ROUTINE_SCENARIO_IDS:
            defn = get_routine_scenario_definition(sid)
            for day in defn.days:
                self.assertEqual(day.publish_samples, 86_400)
                self.assertEqual(day.omission_ranges, ())

    def test_no_other_appliances(self):
        """전자레인지 외 6대 가전 이벤트가 일체 없는지 확인"""
        for sid in ROUTINE_SCENARIO_IDS:
            defn = get_routine_scenario_definition(sid)
            for day in defn.days:
                appliances = {e.appliance for e in day.events}
                self.assertEqual(appliances, {"microwave"})

    def test_total_raw_duration_and_event_count(self):
        """28일간 총 이벤트 수 28회, 총 가동 시간 1,680초(28분) 확인"""
        for sid in ROUTINE_SCENARIO_IDS:
            defn = get_routine_scenario_definition(sid)
            total_events = sum(len(d.events) for d in defn.days)
            total_duration = sum(e.duration_seconds for d in defn.days for e in d.events)
            self.assertEqual(total_events, 28)
            self.assertEqual(total_duration, 28 * 60)  # 1680초

    def test_compiled_plan_totals_and_slots(self):
        """컴파일 후 총 가상 슬롯, 발행 샘플, 결측 샘플 수 확인"""
        base = date(2026, 8, 20)
        for sid in ROUTINE_SCENARIO_IDS:
            defn = get_routine_scenario_definition(sid)
            plan = compile_schedule(defn, base)
            self.assertEqual(len(plan.day_plans), 28)
            self.assertEqual(plan.total_virtual_slots, 28 * 86_400)
            self.assertEqual(plan.total_virtual_slots, 2_419_200)
            self.assertEqual(plan.total_planned_publish_samples, 2_419_200)
            self.assertEqual(plan.total_planned_omitted_samples, 0)

    def test_sparse_transitions_count_and_mapping(self):
        """
        Sparse Transition 매핑 계약 검증:
        - 28일 x 2회(ON, OFF) = 총 56개 transition
        - day_plans의 transition 합계 56개
        - transitions_by_cycle 매핑의 transition 합계 56개
        - 242만 개 슬롯 배열이나 boolean 마스크가 메모리에 존재하지 않는 O(D + E + R) 계약 확인
        """
        base = date(2026, 8, 20)
        for sid in ROUTINE_SCENARIO_IDS:
            defn = get_routine_scenario_definition(sid)
            plan = compile_schedule(defn, base)

            # day_plans를 통한 transition 총합 검증
            total_day_transitions = sum(len(dp.transitions) for dp in plan.day_plans)
            self.assertEqual(total_day_transitions, 56)

            # transitions_by_cycle 공개 accessor를 통한 sparse transition 총합 검증
            sparse_map = plan.transitions_by_cycle
            total_sparse_transitions = sum(len(t_list) for t_list in sparse_map.values())
            self.assertEqual(total_sparse_transitions, 56)
            self.assertEqual(len(sparse_map), 56)

            # 슬롯 배열, 틱 리스트, 불리언 마스크가 존재하지 않음을 확인
            self.assertFalse(hasattr(plan, "slots"))
            self.assertFalse(hasattr(plan, "ticks"))
            self.assertFalse(hasattr(plan, "publish_mask"))


class TestRoutineChangedLaterSpec(unittest.TestCase):
    """ROUTINE_CHANGED_LATER 시나리오 명세 및 산술 검증"""

    def setUp(self):
        self.defn = get_routine_scenario_definition("ROUTINE_CHANGED_LATER")

    def test_later_start_times_offsets(self):
        """이전 21일(0~20)은 08:00, 최근 7일(21~27)은 10:30 선언 확인"""
        for offset, day in enumerate(self.defn.days):
            event = day.events[0]
            if offset < 21:
                self.assertEqual(event.start_time, "08:00:00", f"Day {offset} should be 08:00:00")
            else:
                self.assertEqual(event.start_time, "10:30:00", f"Day {offset} should be 10:30:00")

    def test_later_source_schedule_median_and_shift(self):
        """
        원천 일정으로부터 reference(0~20)와 recent(21~27)의 첫 사용 초 median 및
        signed circular difference 산술 검증:
        - reference median: 08:00 (28,800초)
        - recent median: 10:30 (37,800초)
        - signed shift: +9,000초 (+150분 > 0 -> LATER)
        """
        ref_seconds = [day.events[0].start_second for day in self.defn.days[:21]]
        rec_seconds = [day.events[0].start_second for day in self.defn.days[21:]]

        ref_median = round(statistics.median(ref_seconds))
        rec_median = round(statistics.median(rec_seconds))

        self.assertEqual(ref_median, 8 * 3600)  # 28,800초 (08:00)
        self.assertEqual(rec_median, 10 * 3600 + 30 * 60)  # 37,800초 (10:30)

        # signed circular difference: (recent - previous + 43,200) % 86,400 - 43,200
        shift_seconds = (rec_median - ref_median + 43_200) % 86_400 - 43_200
        self.assertEqual(shift_seconds, 9_000)
        shift_minutes = round(shift_seconds / 60)
        self.assertEqual(shift_minutes, 150)
        self.assertGreater(shift_seconds, 0)  # LATER


class TestRoutineChangedEarlierSpec(unittest.TestCase):
    """ROUTINE_CHANGED_EARLIER 시나리오 명세 및 산술 검증"""

    def setUp(self):
        self.defn = get_routine_scenario_definition("ROUTINE_CHANGED_EARLIER")

    def test_earlier_start_times_offsets(self):
        """이전 21일(0~20)은 10:30, 최근 7일(21~27)은 08:00 선언 확인"""
        for offset, day in enumerate(self.defn.days):
            event = day.events[0]
            if offset < 21:
                self.assertEqual(event.start_time, "10:30:00", f"Day {offset} should be 10:30:00")
            else:
                self.assertEqual(event.start_time, "08:00:00", f"Day {offset} should be 08:00:00")

    def test_earlier_source_schedule_median_and_shift(self):
        """
        원천 일정으로부터 reference(0~20)와 recent(21~27)의 첫 사용 초 median 및
        signed circular difference 산술 검증:
        - reference median: 10:30 (37,800초)
        - recent median: 08:00 (28,800초)
        - signed shift: -9,000초 (-150분 < 0 -> EARLIER)
        """
        ref_seconds = [day.events[0].start_second for day in self.defn.days[:21]]
        rec_seconds = [day.events[0].start_second for day in self.defn.days[21:]]

        ref_median = round(statistics.median(ref_seconds))
        rec_median = round(statistics.median(rec_seconds))

        self.assertEqual(ref_median, 10 * 3600 + 30 * 60)  # 37,800초 (10:30)
        self.assertEqual(rec_median, 8 * 3600)  # 28,800초 (08:00)

        shift_seconds = (rec_median - ref_median + 43_200) % 86_400 - 43_200
        self.assertEqual(shift_seconds, -9_000)
        shift_minutes = round(shift_seconds / 60)
        self.assertEqual(shift_minutes, -150)
        self.assertLess(shift_seconds, 0)  # EARLIER


class TestRoutineChangedWithinThresholdSpec(unittest.TestCase):
    """ROUTINE_CHANGED_WITHIN_THRESHOLD 시나리오 명세 및 산술 검증"""

    def setUp(self):
        self.defn = get_routine_scenario_definition("ROUTINE_CHANGED_WITHIN_THRESHOLD")

    def test_within_threshold_start_times_offsets(self):
        """이전 21일(0~20)은 08:00, 최근 7일(21~27)은 09:30 선언 확인"""
        for offset, day in enumerate(self.defn.days):
            event = day.events[0]
            if offset < 21:
                self.assertEqual(event.start_time, "08:00:00", f"Day {offset} should be 08:00:00")
            else:
                self.assertEqual(event.start_time, "09:30:00", f"Day {offset} should be 09:30:00")

    def test_within_threshold_source_schedule_median_and_shift(self):
        """
        원천 일정으로부터 reference(0~20)와 recent(21~27)의 첫 사용 초 median 및
        signed circular difference 산술 검증:
        - reference median: 08:00 (28,800초)
        - recent median: 09:30 (34,200초)
        - signed shift: +5,400초 (+90분)
        - abs(shift) = 5,400 < 7,200초 (120분) 임계값 미만 확인
        """
        ref_seconds = [day.events[0].start_second for day in self.defn.days[:21]]
        rec_seconds = [day.events[0].start_second for day in self.defn.days[21:]]

        ref_median = round(statistics.median(ref_seconds))
        rec_median = round(statistics.median(rec_seconds))

        self.assertEqual(ref_median, 8 * 3600)  # 28,800초 (08:00)
        self.assertEqual(rec_median, 9 * 3600 + 30 * 60)  # 34,200초 (09:30)

        shift_seconds = (rec_median - ref_median + 43_200) % 86_400 - 43_200
        self.assertEqual(shift_seconds, 5_400)
        shift_minutes = round(shift_seconds / 60)
        self.assertEqual(shift_minutes, 90)

        # 120분 (7,200초) 미만 확인
        minimum_shift_seconds = 120 * 60
        self.assertLess(abs(shift_seconds), minimum_shift_seconds)


class TestRoutineDateMappingAndTimezone(unittest.TestCase):
    """기준 날짜 D와 컴파일 시작일 D-27 매핑, KST 타임존 및 사이클 산술 검증"""

    def setUp(self):
        self.D = date(2026, 9, 16)
        self.base_date = self.D - timedelta(days=27)  # date(2026, 8, 20)
        self.defn = get_routine_scenario_definition("ROUTINE_CHANGED_LATER")
        self.plan = compile_schedule(self.defn, self.base_date)

    def test_reference_date_mapping(self):
        """
        D = 2026-09-16 기준:
        - start_date == 2026-08-20 (D-27)
        - end_date == 2026-09-16 (D)
        - day_plans[0].calendar_date == 2026-08-20 (D-27)
        - day_plans[20].calendar_date == 2026-09-09 (D-7)
        - day_plans[21].calendar_date == 2026-09-10 (D-6)
        - day_plans[27].calendar_date == 2026-09-16 (D)
        """
        self.assertEqual(self.plan.start_date, date(2026, 8, 20))
        self.assertEqual(self.plan.end_date, date(2026, 9, 16))
        self.assertEqual(self.plan.day_plans[0].calendar_date, date(2026, 8, 20))
        self.assertEqual(self.plan.day_plans[20].calendar_date, date(2026, 9, 9))
        self.assertEqual(self.plan.day_plans[21].calendar_date, date(2026, 9, 10))
        self.assertEqual(self.plan.day_plans[27].calendar_date, date(2026, 9, 16))

    def test_cycle_accessors_without_seeking_day0(self):
        """
        Day 0 (D-27) 08:00:00 ON/OFF 전환 사이클을 산술 계산하여
        plan.transitions_at(cycle) 및 plan.virtual_time_at(cycle) 검증 (runner seek 없이 검증)
        """
        # Day 0: start_cycle = 1
        # 08:00:00 = 28,800초 -> on_cycle = 1 + 28,800 = 28,801
        on_cycle = 1 + 28_800
        transitions = self.plan.transitions_at(on_cycle)
        self.assertEqual(len(transitions), 1)
        trans = transitions[0]
        self.assertEqual(trans.transition_type, TransitionType.ON)
        self.assertEqual(trans.appliance, "microwave")
        self.assertEqual(trans.virtual_time.tzinfo, KST)
        self.assertEqual(trans.virtual_time.isoformat(), "2026-08-20T08:00:00+09:00")
        self.assertEqual(self.plan.virtual_time_at(on_cycle), trans.virtual_time)
        self.assertTrue(self.plan.should_publish(on_cycle))

        # 08:01:00 = 28,860초 -> off_cycle = 1 + 28,860 = 28,861
        off_cycle = 1 + 28_860
        off_transitions = self.plan.transitions_at(off_cycle)
        self.assertEqual(len(off_transitions), 1)
        self.assertEqual(off_transitions[0].transition_type, TransitionType.OFF)
        self.assertEqual(off_transitions[0].virtual_time.isoformat(), "2026-08-20T08:01:00+09:00")

    def test_cycle_accessors_without_seeking_day20_and_day21_boundary(self):
        """
        Day 20 (D-7, 이전 기준 마지막 날 08:00)과 Day 21 (D-6, 최근 첫 날 10:30) 경계 전환 검증
        """
        # Day 20: start_cycle = 1 + 20 * 86,400 = 1,728,001
        # on_cycle = 1,728,001 + 28,800 = 1,756,801 (08:00:00)
        day20_on_cycle = 1 + 20 * 86_400 + 28_800
        trans20 = self.plan.transitions_at(day20_on_cycle)
        self.assertEqual(len(trans20), 1)
        self.assertEqual(trans20[0].transition_type, TransitionType.ON)
        self.assertEqual(trans20[0].virtual_time.isoformat(), "2026-09-09T08:00:00+09:00")

        # Day 21: start_cycle = 1 + 21 * 86,400 = 1,814,401
        # on_cycle = 1,814,401 + 37,800 = 1,852,201 (10:30:00)
        day21_on_cycle = 1 + 21 * 86_400 + 37_800
        trans21 = self.plan.transitions_at(day21_on_cycle)
        self.assertEqual(len(trans21), 1)
        self.assertEqual(trans21[0].transition_type, TransitionType.ON)
        self.assertEqual(trans21[0].virtual_time.isoformat(), "2026-09-10T10:30:00+09:00")

    def test_cycle_accessors_without_seeking_day27_last_day_and_slot(self):
        """
        Day 27 (D, 2026-09-16) 10:30:00 전환 및 마지막 2,419,200번째 슬롯(23:59:59) 검증
        """
        # Day 27 on_cycle: 1 + 27 * 86,400 + 37,800 = 2,370,601
        day27_on_cycle = 1 + 27 * 86_400 + 37_800
        trans27 = self.plan.transitions_at(day27_on_cycle)
        self.assertEqual(len(trans27), 1)
        self.assertEqual(trans27[0].transition_type, TransitionType.ON)
        self.assertEqual(trans27[0].virtual_time.isoformat(), "2026-09-16T10:30:00+09:00")

        # Day 27 off_cycle: 2,370,601 + 60 = 2,370,661 (10:31:00)
        day27_off_cycle = day27_on_cycle + 60
        trans27_off = self.plan.transitions_at(day27_off_cycle)
        self.assertEqual(len(trans27_off), 1)
        self.assertEqual(trans27_off[0].transition_type, TransitionType.OFF)
        self.assertEqual(trans27_off[0].virtual_time.isoformat(), "2026-09-16T10:31:00+09:00")

        # 마지막 가상 슬롯 (cycle = 2,419,200) 시각 확인
        last_cycle = self.plan.last_cycle
        self.assertEqual(last_cycle, 2_419_200)
        last_time = self.plan.virtual_time_at(last_cycle)
        self.assertEqual(last_time.isoformat(), "2026-09-16T23:59:59+09:00")
        self.assertTrue(self.plan.should_publish(last_cycle))


class TestNoAnswerLabelsAndSafeSampling(unittest.TestCase):
    """정답 라벨 미유출 및 극소수 샘플링을 통한 안전한 결정론 검증"""

    def test_no_answer_labels_in_definition_or_mqtt_payload(self):
        """
        선언 객체(description 포함) 및 실제 MQTT 페이로드에 AI 정답 라벨(scenario_id, event_type,
        direction, shift_minutes, expected_event_type, activity_index, score)이나
        다운스트림 판정 결과 표현("150분", "90분", "임계값", "(LATER)", "(EARLIER)")이
        일체 들어가지 않는 계약 검증.
        (242만 개가 아닌 최초 10개 슬롯만 검증하여 O(1) 메모리 유지)
        """
        forbidden_labels = [
            "expected_event_type",
            "direction",
            "shift_minutes",
            "activity_index",
            "score",
            "data_status",
            "logical_uses",
        ]

        forbidden_description_phrases = [
            "150분",
            "90분",
            "임계값",
            "(LATER)",
            "(EARLIER)",
            "지연",
            "앞당겨짐",
        ]

        # 1. 모든 시나리오 선언 객체 및 description 검사
        for sid in ROUTINE_SCENARIO_IDS:
            defn = get_routine_scenario_definition(sid)
            for phrase in forbidden_description_phrases:
                self.assertNotIn(
                    phrase,
                    defn.description,
                    f"ScenarioDefinition({sid}).description contains downstream judgment phrase: {phrase!r}",
                )
            for label in forbidden_labels:
                self.assertFalse(hasattr(defn, label), f"ScenarioDefinition({sid}) has attribute {label}")
                self.assertFalse(hasattr(defn.days[0], label), f"DaySchedule has attribute {label}")
                self.assertFalse(hasattr(defn.days[0].events[0], label), f"ApplianceEvent has attribute {label}")

        # 2. 컴파일 및 실제 빌드된 MQTT 페이로드 검사 (최초 10개 슬롯)
        later_defn = get_routine_scenario_definition("ROUTINE_CHANGED_LATER")
        plan = compile_schedule(later_defn, date(2026, 8, 20))
        runner = DeterministicScheduleRunner(plan, "H001")

        expected_fields = {
            "message_id", "household_id", "device_id", "measured_at",
            "active_power", "reactive_power", "power_factor", "current",
            "house", "device", "ts", "power_w", "voltage", "apparent_power",
        }

        for _ in range(10):
            tick = runner.step()
            payload = build_scheduled_power_payload(tick, "run_routine_001")
            self.assertEqual(set(payload.keys()), expected_fields)
            self.assertNotIn("scenario_id", payload)
            self.assertNotIn("event_type", payload)
            self.assertNotIn("direction", payload)
            self.assertNotIn("shift_minutes", payload)
            self.assertNotIn("expected_event_type", payload)
            for label in forbidden_labels:
                self.assertNotIn(label, payload)

    def test_initial_ticks_determinism_and_physical_values(self):
        """
        동일한 플랜 및 household_id로 독립 생성된 두 runner가 최초 5개 슬롯에서
        100% 동일한 물리 계측값을 생성하는 결정론 검증
        """
        defn = get_routine_scenario_definition("ROUTINE_CHANGED_EARLIER")
        plan = compile_schedule(defn, date(2026, 8, 20))

        runner1 = DeterministicScheduleRunner(plan, "H001", seed=42)
        runner2 = DeterministicScheduleRunner(plan, "H001", seed=42)

        for i in range(5):
            tick1 = runner1.step()
            tick2 = runner2.step()
            self.assertEqual(tick1.measured_at, tick2.measured_at)
            self.assertEqual(tick1.active_power, tick2.active_power)
            self.assertEqual(tick1.reactive_power, tick2.reactive_power)
            self.assertEqual(tick1.voltage, tick2.voltage)
            self.assertEqual(tick1.current, tick2.current)
            self.assertEqual(tick1.power_factor, tick2.power_factor)
            self.assertTrue(math.isfinite(tick1.active_power))


if __name__ == "__main__":
    unittest.main()
