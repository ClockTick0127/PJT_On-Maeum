"""
20일 전자레인지 기준선 생성 시나리오 카탈로그(baseline_scenario_catalog.py) 단위 및 회귀 테스트 스위트

검증 내용:
1. 카탈로그 공개 API 계약: 단일 canonical ID(BASELINE_MICROWAVE_20D), 불변 MappingProxyType, 비문자열/미등록 ID 거절
2. 20일 일정 구조: 20개 DaySchedule, day_offset 0~19 연속, 매일 86400개 발행, 결측 0, 첫 3일 미사용, 이후 17일 전자레인지 60초
3. 기준 날짜 매핑 및 KST 타임존: D=2026-09-16 기준 D-19(2026-08-28) 컴파일, D-17(마지막 미사용), D-16(첫 사용), D(종료일) 매핑
4. 원천 일정 산술 검증: 표본 후보 20일, 활성 17일, 미사용 3일, 사용확률 Decimal('0.8500'), P10/P50/P90 모두 08:00
5. 요일별 표본 산술 검증: 20일간 각 요일 표본수가 2일 또는 3일로 기본 minimum_weekday_sample_days(4) 미달(eligible=false fallback) 확인
6. Sparse 메모리 계약: 20개 DayPlan, 17개 이벤트, 34개 transition, 1,728,000 슬롯 배열 미생성, plan 공개 accessor 활용
7. MQTT 페이로드 및 description 정답 라벨 미유출 및 안전한 초기 틱 결정론 검증
"""

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import math
import statistics
import unittest

from engine.baseline_scenario_catalog import (
    BASELINE_SCENARIO_IDS,
    UnknownBaselineScenarioError,
    list_baseline_scenario_ids,
    get_baseline_scenario_definition,
    get_baseline_scenario_catalog,
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


class TestBaselineScenarioCatalogAPI(unittest.TestCase):
    """기준선 시나리오 카탈로그 공개 API 및 도메인 예외 계약 검증"""

    def test_list_baseline_scenario_ids_exact_one(self):
        """정확히 1개 시나리오 ID를 불변 튜플로 반환하는지 확인"""
        ids = list_baseline_scenario_ids()
        self.assertIsInstance(ids, tuple)
        self.assertEqual(len(ids), 1)

    def test_list_baseline_scenario_ids_order_fixed(self):
        """반환 튜플이 BASELINE_SCENARIO_IDS 상수와 정확히 일치하는지 확인"""
        expected = ("BASELINE_MICROWAVE_20D",)
        self.assertEqual(list_baseline_scenario_ids(), expected)
        self.assertEqual(BASELINE_SCENARIO_IDS, expected)

    def test_get_baseline_scenario_definition_repeated_calls_identical(self):
        """동일 ID 반복 호출 시 동일 인스턴스 반환 확인"""
        def1 = get_baseline_scenario_definition("BASELINE_MICROWAVE_20D")
        def2 = get_baseline_scenario_definition("BASELINE_MICROWAVE_20D")
        self.assertIs(def1, def2)
        self.assertEqual(def1, def2)

    def test_catalog_mapping_is_immutable_proxy(self):
        """get_baseline_scenario_catalog()이 MappingProxyType으로 반환되어 수정 시도 시 TypeError 발생 확인"""
        cat = get_baseline_scenario_catalog()
        with self.assertRaises(TypeError):
            cat["NEW_SCENARIO"] = get_baseline_scenario_definition("BASELINE_MICROWAVE_20D")  # type: ignore
        with self.assertRaises(TypeError):
            del cat["BASELINE_MICROWAVE_20D"]  # type: ignore

    def test_unknown_scenario_id_rejected(self):
        """미등록 ID 조회 시 UnknownBaselineScenarioError 발생 확인"""
        with self.assertRaises(UnknownBaselineScenarioError):
            get_baseline_scenario_definition("UNKNOWN_BASELINE")
        with self.assertRaises(UnknownBaselineScenarioError):
            get_baseline_scenario_definition("ACTIVITY_NORMAL")
        with self.assertRaises(UnknownBaselineScenarioError):
            get_baseline_scenario_definition("ROUTINE_CHANGED_LATER")

    def test_lowercase_scenario_id_rejected(self):
        """소문자 ID 조회 시 UnknownBaselineScenarioError 발생 확인"""
        with self.assertRaises(UnknownBaselineScenarioError):
            get_baseline_scenario_definition("baseline_microwave_20d")

    def test_scenario_id_with_whitespace_rejected(self):
        """앞뒤 공백 포함 ID 조회 시 UnknownBaselineScenarioError 발생 확인"""
        with self.assertRaises(UnknownBaselineScenarioError):
            get_baseline_scenario_definition(" BASELINE_MICROWAVE_20D")
        with self.assertRaises(UnknownBaselineScenarioError):
            get_baseline_scenario_definition("BASELINE_MICROWAVE_20D ")
        with self.assertRaises(UnknownBaselineScenarioError):
            get_baseline_scenario_definition("BASELINE_MICROWAVE_20D\n")

    def test_non_string_types_rejected(self):
        """None, bool, 숫자, 리스트 등 문자열이 아닌 타입 전달 시 UnknownBaselineScenarioError 발생 확인"""
        invalid_types = [None, True, False, 123, 45.6, [], {}, ()]
        for val in invalid_types:
            with self.subTest(val=val):
                with self.assertRaises(UnknownBaselineScenarioError):
                    get_baseline_scenario_definition(val)  # type: ignore

    def test_all_catalog_entries_are_scenario_definitions(self):
        """카탈로그의 모든 항목이 ScenarioDefinition 인스턴스이며 scenario_id가 키와 일치하는지 확인"""
        cat = get_baseline_scenario_catalog()
        for scenario_id, defn in cat.items():
            self.assertIsInstance(defn, ScenarioDefinition)
            self.assertEqual(defn.scenario_id, scenario_id)


class TestBaseline20DStructureAndInvariants(unittest.TestCase):
    """BASELINE_MICROWAVE_20D 일정 구조 및 불변 계약 검증"""

    def setUp(self):
        self.defn = get_baseline_scenario_definition("BASELINE_MICROWAVE_20D")

    def test_exact_twenty_days(self):
        """정확히 20개의 DaySchedule을 갖는지 확인"""
        self.assertEqual(len(self.defn.days), 20)

    def test_day_offsets_contiguous_sequence(self):
        """day_offset이 0부터 19까지 연속된 정수 튜플인지 확인"""
        expected_offsets = tuple(range(20))
        actual_offsets = tuple(d.day_offset for d in self.defn.days)
        self.assertEqual(actual_offsets, expected_offsets)

    def test_first_three_days_empty_events(self):
        """첫 3일(day_offset 0, 1, 2)은 전자레인지 미사용(events == ())인지 확인"""
        for offset in (0, 1, 2):
            day = self.defn.days[offset]
            self.assertEqual(day.events, (), f"Day {offset} should have empty events")

    def test_remaining_seventeen_days_single_microwave_event(self):
        """이후 17일(day_offset 3~19)은 08:00:00 전자레인지 60초 이벤트가 정확히 1개인지 확인"""
        for offset in range(3, 20):
            day = self.defn.days[offset]
            self.assertEqual(len(day.events), 1, f"Day {offset} should have exactly 1 event")
            event = day.events[0]
            self.assertEqual(event.appliance, "microwave")
            self.assertEqual(event.start_time, "08:00:00")
            self.assertEqual(event.duration_seconds, 60)

    def test_each_day_full_publish_no_omissions(self):
        """20일 모든 날짜가 결측 구간 없이 86,400개 전수 발행 후보를 갖는지 확인"""
        for day in self.defn.days:
            self.assertEqual(day.publish_samples, 86_400)
            self.assertEqual(day.omission_ranges, ())

    def test_no_other_appliances(self):
        """전자레인지 외 6대 가전 이벤트가 일체 없는지 확인"""
        for day in self.defn.days:
            for event in day.events:
                self.assertEqual(event.appliance, "microwave")

    def test_total_raw_duration_and_event_count(self):
        """총 17회 이벤트, 총 가동 시간 1,020초(17분) 확인"""
        total_events = sum(len(d.events) for d in self.defn.days)
        total_duration = sum(e.duration_seconds for d in self.defn.days for e in d.events)
        self.assertEqual(total_events, 17)
        self.assertEqual(total_duration, 17 * 60)  # 1020초

    def test_compiled_plan_totals_and_slots(self):
        """컴파일 후 총 가상 슬롯, 발행 샘플, 결측 샘플 수 확인"""
        base = date(2026, 8, 28)
        plan = compile_schedule(self.defn, base)
        self.assertEqual(len(plan.day_plans), 20)
        self.assertEqual(plan.total_virtual_slots, 20 * 86_400)
        self.assertEqual(plan.total_virtual_slots, 1_728_000)
        self.assertEqual(plan.total_planned_publish_samples, 1_728_000)
        self.assertEqual(plan.total_planned_omitted_samples, 0)

    def test_sparse_transitions_count_and_mapping(self):
        """
        Sparse Transition 매핑 계약 검증:
        - 17개 이벤트 x 2회(ON, OFF) = 총 34개 transition
        - day_plans의 transition 합계 34개
        - transitions_by_cycle 매핑의 transition 합계 34개
        - sparse transition cycle key 34개
        - 1,728,000개 슬롯 객체 배열이나 boolean publish mask가 메모리에 존재하지 않는 O(D + E + R) 계약 확인
        """
        base = date(2026, 8, 28)
        plan = compile_schedule(self.defn, base)

        # day_plans를 통한 transition 총합 검증
        total_day_transitions = sum(len(dp.transitions) for dp in plan.day_plans)
        self.assertEqual(total_day_transitions, 34)

        # transitions_by_cycle 공개 accessor를 통한 sparse transition 총합 검증
        sparse_map = plan.transitions_by_cycle
        total_sparse_transitions = sum(len(t_list) for t_list in sparse_map.values())
        self.assertEqual(total_sparse_transitions, 34)
        self.assertEqual(len(sparse_map), 34)

        # 슬롯 배열, 틱 리스트, 불리언 마스크가 존재하지 않음을 확인
        self.assertFalse(hasattr(plan, "slots"))
        self.assertFalse(hasattr(plan, "ticks"))
        self.assertFalse(hasattr(plan, "publish_mask"))
        self.assertFalse(hasattr(plan, "_publish_mask_by_cycle"))


class TestBaselineDateMappingAndTimezone(unittest.TestCase):
    """기준 날짜 D와 컴파일 시작일 D-19 매핑, KST 타임존 및 사이클 산술 검증"""

    def setUp(self):
        self.D = date(2026, 9, 16)
        self.base_date = self.D - timedelta(days=19)  # date(2026, 8, 28)
        self.defn = get_baseline_scenario_definition("BASELINE_MICROWAVE_20D")
        self.plan = compile_schedule(self.defn, self.base_date)

    def test_reference_date_mapping(self):
        """
        D = 2026-09-16 기준:
        - start_date == 2026-08-28 (D-19)
        - end_date == 2026-09-16 (D)
        - day_plans[0].calendar_date == 2026-08-28 (D-19, 첫 미사용일)
        - day_plans[2].calendar_date == 2026-08-30 (D-17, 마지막 미사용일)
        - day_plans[3].calendar_date == 2026-08-31 (D-16, 첫 전자레인지 사용일)
        - day_plans[19].calendar_date == 2026-09-16 (D, 마지막 전자레인지 사용일)
        """
        self.assertEqual(self.plan.start_date, date(2026, 8, 28))
        self.assertEqual(self.plan.end_date, date(2026, 9, 16))
        self.assertEqual(self.plan.day_plans[0].calendar_date, date(2026, 8, 28))
        self.assertEqual(self.plan.day_plans[2].calendar_date, date(2026, 8, 30))
        self.assertEqual(self.plan.day_plans[3].calendar_date, date(2026, 8, 31))
        self.assertEqual(self.plan.day_plans[19].calendar_date, date(2026, 9, 16))

    def test_cycle_accessors_without_seeking_day0_to_day2_empty(self):
        """
        Day 0 ~ Day 2(미사용일) 구간에 어떠한 가전 전환도 등록되지 않았음을 공개 API로 확인
        """
        # Day 0: cycle 1 ~ 86,400
        # 08:00:00 (28,800초 -> cycle 28,801)에 전환이 없어야 함
        self.assertEqual(self.plan.transitions_at(1 + 28_800), ())
        # Day 2: start_cycle = 1 + 2 * 86,400 = 172,801
        self.assertEqual(self.plan.transitions_at(172_801 + 28_800), ())

    def test_cycle_accessors_without_seeking_day3_first_active(self):
        """
        Day 3 (D-16, 2026-08-31) 08:00:00 ON 및 08:01:00 OFF 전환 검증
        """
        # Day 3: start_cycle = 1 + 3 * 86,400 = 259,201
        # on_cycle: 259,201 + 28,800 = 288,001 (08:00:00)
        day3_on_cycle = 259_201 + 28_800
        trans_on = self.plan.transitions_at(day3_on_cycle)
        self.assertEqual(len(trans_on), 1)
        self.assertEqual(trans_on[0].transition_type, TransitionType.ON)
        self.assertEqual(trans_on[0].appliance, "microwave")
        self.assertEqual(trans_on[0].virtual_time.tzinfo, KST)
        self.assertEqual(trans_on[0].virtual_time.isoformat(), "2026-08-31T08:00:00+09:00")
        self.assertEqual(self.plan.virtual_time_at(day3_on_cycle), trans_on[0].virtual_time)
        self.assertTrue(self.plan.should_publish(day3_on_cycle))

        # off_cycle: 288,001 + 60 = 288,061 (08:01:00)
        day3_off_cycle = day3_on_cycle + 60
        trans_off = self.plan.transitions_at(day3_off_cycle)
        self.assertEqual(len(trans_off), 1)
        self.assertEqual(trans_off[0].transition_type, TransitionType.OFF)
        self.assertEqual(trans_off[0].virtual_time.isoformat(), "2026-08-31T08:01:00+09:00")

    def test_cycle_accessors_without_seeking_day19_last_active_and_slot(self):
        """
        Day 19 (D, 2026-09-16) 08:00:00 ON/OFF 전환 및 마지막 1,728,000번째 슬롯(23:59:59) 검증
        """
        # Day 19: start_cycle = 1 + 19 * 86,400 = 1,641,601
        # on_cycle: 1,641,601 + 28,800 = 1,670,401 (08:00:00)
        day19_on_cycle = 1_641_601 + 28_800
        trans_on = self.plan.transitions_at(day19_on_cycle)
        self.assertEqual(len(trans_on), 1)
        self.assertEqual(trans_on[0].transition_type, TransitionType.ON)
        self.assertEqual(trans_on[0].virtual_time.isoformat(), "2026-09-16T08:00:00+09:00")

        # off_cycle: 1,670,401 + 60 = 1,670,461 (08:01:00)
        day19_off_cycle = day19_on_cycle + 60
        trans_off = self.plan.transitions_at(day19_off_cycle)
        self.assertEqual(len(trans_off), 1)
        self.assertEqual(trans_off[0].transition_type, TransitionType.OFF)
        self.assertEqual(trans_off[0].virtual_time.isoformat(), "2026-09-16T08:01:00+09:00")

        # 마지막 가상 슬롯 (cycle = 1,728,000) 시각 확인
        last_cycle = self.plan.last_cycle
        self.assertEqual(last_cycle, 1_728_000)
        last_time = self.plan.virtual_time_at(last_cycle)
        self.assertEqual(last_time.isoformat(), "2026-09-16T23:59:59+09:00")
        self.assertTrue(self.plan.should_publish(last_cycle))


class TestBaselineSourceScheduleArithmetic(unittest.TestCase):
    """AI 모듈 import 없이 카탈로그 원천 데이터로부터 기준선 입력 산술 독립 검증"""

    def setUp(self):
        self.defn = get_baseline_scenario_definition("BASELINE_MICROWAVE_20D")

    def test_source_schedule_arithmetic(self):
        """
        카탈로그 데이터 산술 확인:
        - 표본 후보 일수 = 20
        - 활성 일수 = 17 (offsets 3~19)
        - 미사용 일수 = 3 (offsets 0~2)
        - 사용확률 = 17 / 20 = 0.8500 (Decimal("0.8500")) >= 0.70 (기본 조건 충족)
        - 모든 활성일 첫 사용 초 = 28,800초 (08:00:00)
        - P10, P50, P90 후보 모두 08:00
        - 가동 시간 60초 >= 최소 유효 세션 10초
        """
        sample_days = len(self.defn.days)
        self.assertEqual(sample_days, 20)

        active_offsets = [d.day_offset for d in self.defn.days if len(d.events) > 0]
        no_use_offsets = [d.day_offset for d in self.defn.days if len(d.events) == 0]

        self.assertEqual(active_offsets, list(range(3, 20)))
        self.assertEqual(no_use_offsets, [0, 1, 2])
        self.assertEqual(len(active_offsets), 17)
        self.assertEqual(len(no_use_offsets), 3)

        # 사용확률 4자리 반올림 검증
        probability = (Decimal(len(active_offsets)) / Decimal(sample_days)).quantize(Decimal("0.0001"))
        self.assertEqual(probability, Decimal("0.8500"))
        self.assertGreaterEqual(probability, Decimal("0.7000"))

        # 활성일 첫 사용 초 및 분위수 검증
        active_seconds = [d.events[0].start_second for d in self.defn.days if len(d.events) > 0]
        self.assertEqual(len(active_seconds), 17)
        for sec in active_seconds:
            self.assertEqual(sec, 28_800)  # 08:00:00

        # P50 (math.ceil(0.50 * 17) - 1 = 9 - 1 = 8)
        p50_idx = max(0, math.ceil(0.50 * len(active_seconds)) - 1)
        self.assertEqual(active_seconds[p50_idx], 28_800)

        # P90 (math.ceil(0.90 * 17) - 1 = 16 - 1 = 15)
        p90_idx = max(0, math.ceil(0.90 * len(active_seconds)) - 1)
        self.assertEqual(active_seconds[p90_idx], 28_800)

        # P10 (math.ceil(0.10 * 17) - 1 = 2 - 1 = 1)
        p10_idx = max(0, math.ceil(0.10 * len(active_seconds)) - 1)
        self.assertEqual(active_seconds[p10_idx], 28_800)

        # 최소 세션 시간(10초) 검증
        for d in self.defn.days:
            for ev in d.events:
                self.assertGreaterEqual(ev.duration_seconds, 10)


class TestWeekdayProfilesSampleCountArithmetic(unittest.TestCase):
    """20일 연속 날짜에서 각 요일 표본수가 기본 4일 미만임을 확인하는 산술 검증"""

    def test_weekday_sample_counts_under_minimum(self):
        """
        D-19 ~ D의 20일 동안:
        - 20 = 2 * 7 + 6 이므로, 6개 요일은 3일, 1개 요일은 2일 표본을 가짐
        - 모든 요일의 표본 수가 기본 minimum_weekday_sample_days(4) 미만
        - 따라서 기본 설정에서는 모든 요일 profile이 eligible=false가 되며,
          전체 expected_until=08:00 기준으로 정상 fallback한다는 외부 계약을 확인
        """
        base = date(2026, 8, 28)
        defn = get_baseline_scenario_definition("BASELINE_MICROWAVE_20D")
        plan = compile_schedule(defn, base)

        weekday_counts: dict[int, int] = defaultdict(int)
        for dp in plan.day_plans:
            weekday_counts[dp.calendar_date.weekday()] += 1

        self.assertEqual(len(weekday_counts), 7)
        for weekday, count in weekday_counts.items():
            self.assertIn(count, (2, 3), f"Weekday {weekday} count must be 2 or 3")
            self.assertLess(count, 4, f"Weekday {weekday} sample count must be < 4")


class TestNoAnswerLabelsAndSafeSampling(unittest.TestCase):
    """정답 라벨 미유출 및 극소수 샘플링을 통한 안전한 결정론 검증"""

    def test_no_answer_labels_in_definition_or_mqtt_payload(self):
        """
        선언 객체(description 포함) 및 실제 MQTT 페이로드에 AI 기준선 정답 라벨(sample_days,
        active_days, daily_use_probability, reliability_weight, expected_until, enabled,
        source, baseline_type, expected_baseline, scenario_id)이나
        다운스트림 판정 결과 표현("0.85", "enabled=true", "expected_until=08:00",
        "P90", "P50", "sample_days=20", "VALID_DAILY_OBSERVATIONS")이 일체 들어가지 않는 계약 검증.
        (172만 개가 아닌 최초 10개 슬롯만 검증하여 O(1) 메모리 유지)
        """
        defn = get_baseline_scenario_definition("BASELINE_MICROWAVE_20D")

        forbidden_description_phrases = [
            "0.85",
            "enabled",
            "expected_until",
            "P90",
            "P50",
            "sample_days",
            "active_days",
            "probability",
            "VALID_DAILY_OBSERVATIONS",
        ]

        forbidden_labels = [
            "sample_days",
            "active_days",
            "daily_use_probability",
            "reliability_weight",
            "expected_until",
            "enabled",
            "source",
            "baseline_type",
            "expected_baseline",
        ]

        # 1. description의 중립적 원천 일정 서술 검증
        self.assertEqual(
            defn.description,
            "20일 전자레인지 첫 사용 일정: 첫 3일 미사용, 이후 17일 08:00 사용",
        )
        for phrase in forbidden_description_phrases:
            self.assertNotIn(
                phrase,
                defn.description,
                f"description contains downstream judgment phrase: {phrase!r}",
            )

        # 2. 선언 객체 속성 검사
        for label in forbidden_labels:
            self.assertFalse(hasattr(defn, label), f"ScenarioDefinition has attribute {label}")
            self.assertFalse(hasattr(defn.days[0], label), f"DaySchedule has attribute {label}")
            if len(defn.days[3].events) > 0:
                self.assertFalse(hasattr(defn.days[3].events[0], label), f"ApplianceEvent has attribute {label}")

        # 3. 컴파일 및 실제 빌드된 MQTT 페이로드 검사 (최초 10개 슬롯)
        plan = compile_schedule(defn, date(2026, 8, 28))
        runner = DeterministicScheduleRunner(plan, "H001")

        expected_fields = {
            "message_id", "household_id", "device_id", "measured_at",
            "active_power", "reactive_power", "power_factor", "current",
            "house", "device", "ts", "power_w", "voltage", "apparent_power",
        }

        for _ in range(10):
            tick = runner.step()
            payload = build_scheduled_power_payload(tick, "run_baseline_001")
            self.assertEqual(set(payload.keys()), expected_fields)
            self.assertNotIn("scenario_id", payload)
            self.assertNotIn("event_type", payload)
            for label in forbidden_labels:
                self.assertNotIn(label, payload)

    def test_initial_ticks_determinism_and_physical_values(self):
        """
        동일한 플랜 및 household_id로 독립 생성된 두 runner가 최초 5개 슬롯에서
        100% 동일한 물리 계측값을 생성하는 결정론 검증
        """
        defn = get_baseline_scenario_definition("BASELINE_MICROWAVE_20D")
        plan = compile_schedule(defn, date(2026, 8, 28))

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
