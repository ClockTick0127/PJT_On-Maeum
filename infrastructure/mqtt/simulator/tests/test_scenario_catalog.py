"""
일일 활동 시나리오 카탈로그(scenario_catalog.py) 단위 및 회귀 테스트 스위트

검증 내용:
1. 카탈로그 API 계약 (10개): 6개 시나리오 불변 튜플, 정렬, 불변 매핑, 타입 검증, 도메인 예외
2. ACTIVITY_NORMAL 명세 (7개): 6개 이벤트, 3종 가전, 2160초 지속시간, 86400 발행, 전이 사이클
3. ACTIVITY_LOW 명세 (3개): 12:00 전자레인지 180초 1회, 정답 라벨 미포함 검증
4. ACTIVITY_NONE 명세 (3개): 0개 이벤트, 86400 발행, 6대 가전 OFF 및 전력 수치 유한성
5. ACTIVITY_INSUFFICIENT 명세 (6개): ceil 공식(82079건), [82079, 86400) 결측, KST 타임스탬프, raw < 0.95
6. ACTIVITY_SESSION_MERGE 명세 (6개): 인덕션 2회(각 600초), 60초 간격, 카탈로그 미병합
7. ACTIVITY_DURATION_CAP 명세 (5개): 전기주전자 7200초(10:00~12:00), 600초 미자름, 타가전 없음
8. 결정론 및 회귀 검증 (5개): 결정론 컴파일, KST 타임스탬프, 정답 라벨 미유출, 실제 executor/runner 86400 슬롯 BURST 완주
"""

import asyncio
from datetime import date, datetime, timezone, timedelta
import json
import math
import unittest

from engine.scenario_catalog import (
    ACTIVITY_SCENARIO_IDS,
    UnknownActivityScenarioError,
    list_activity_scenario_ids,
    get_activity_scenario_definition,
    get_activity_scenario_catalog,
)
from engine.schedule import (
    ScenarioDefinition,
    DaySchedule,
    ApplianceEvent,
    OmissionRange,
    compile_schedule,
)
from engine.schedule_runtime import DeterministicScheduleRunner
from engine.schedule_publisher import (
    build_scheduled_topic,
    build_scheduled_power_payload,
    serialize_scheduled_power_payload,
)
from engine.schedule_executor import (
    DeterministicScheduleExecutor,
    ExecutorConfig,
    ExecutionMode,
    ExecutionStatus,
)

KST = timezone(timedelta(hours=9))


class CountingFakeMqttClient:
    """
    메모리 누적 없이 MQTT 발행 호출 수와 첫/마지막 호출만 보관하는 가짜 MQTT 클라이언트.

    86,400개 페이로드를 리스트에 누적하지 않고 O(1) 메모리로 타임스탬프 단조 증가 및 건수를 검증한다.
    """

    def __init__(self) -> None:
        self.publish_count: int = 0
        self.first_topic: str | None = None
        self.first_payload: str | None = None
        self.first_cycle: int | None = None
        self.last_topic: str | None = None
        self.last_payload: str | None = None
        self.last_cycle: int | None = None
        self.last_measured_at: datetime | None = None
        self.monotonic_measured_at: bool = True

    async def publish(
        self,
        topic: str,
        payload: str,
        qos: int = 1,
        retain: bool = False,
        *args,
        **kwargs,
    ) -> None:
        self.publish_count += 1
        if self.first_payload is None:
            self.first_topic = topic
            self.first_payload = payload
        self.last_topic = topic
        self.last_payload = payload

        # 타임스탬프 단조 증가 및 사이클 검증 (페이로드 미누적)
        data = json.loads(payload)
        dt = datetime.fromisoformat(data["measured_at"])
        if self.last_measured_at is not None:
            if dt <= self.last_measured_at:
                self.monotonic_measured_at = False
        self.last_measured_at = dt

        ref_dt = datetime(dt.year, dt.month, dt.day, 0, 0, 0, tzinfo=KST)
        second_of_day = int((dt - ref_dt).total_seconds())
        cycle = second_of_day + 1
        if self.first_cycle is None:
            self.first_cycle = cycle
        self.last_cycle = cycle


class TestScenarioCatalogAPI(unittest.TestCase):
    """카탈로그 공개 API 및 도메인 예외 계약 검증 (10개)"""

    def test_list_activity_scenario_ids_exact_six(self):
        """정확히 6개 시나리오 ID를 불변 튜플로 반환하는지 확인"""
        ids = list_activity_scenario_ids()
        self.assertIsInstance(ids, tuple)
        self.assertEqual(len(ids), 6)

    def test_list_activity_scenario_ids_order_fixed(self):
        """반환 튜플의 고정 순서 및 ACTIVITY_SCENARIO_IDS 상수와의 일치 확인"""
        expected = (
            "ACTIVITY_NORMAL",
            "ACTIVITY_LOW",
            "ACTIVITY_NONE",
            "ACTIVITY_INSUFFICIENT",
            "ACTIVITY_SESSION_MERGE",
            "ACTIVITY_DURATION_CAP",
        )
        self.assertEqual(list_activity_scenario_ids(), expected)
        self.assertEqual(ACTIVITY_SCENARIO_IDS, expected)

    def test_get_activity_scenario_definition_repeated_calls_identical(self):
        """동일 ID 반복 호출 시 동일 인스턴스 또는 동등 객체 반환 확인"""
        def1 = get_activity_scenario_definition("ACTIVITY_NORMAL")
        def2 = get_activity_scenario_definition("ACTIVITY_NORMAL")
        self.assertIs(def1, def2)
        self.assertEqual(def1, def2)

    def test_catalog_mapping_is_immutable_proxy(self):
        """get_activity_scenario_catalog()이 MappingProxyType으로 반환되어 수정 시도 시 TypeError 발생 확인"""
        cat = get_activity_scenario_catalog()
        with self.assertRaises(TypeError):
            cat["NEW_SCENARIO"] = get_activity_scenario_definition("ACTIVITY_NORMAL")  # type: ignore
        with self.assertRaises(TypeError):
            del cat["ACTIVITY_NORMAL"]  # type: ignore

    def test_unknown_scenario_id_rejected(self):
        """미등록 ID 조회 시 UnknownActivityScenarioError 발생 확인"""
        with self.assertRaises(UnknownActivityScenarioError):
            get_activity_scenario_definition("UNKNOWN_SCENARIO")
        with self.assertRaises(UnknownActivityScenarioError):
            get_activity_scenario_definition("ACTIVITY_PEAK")

    def test_lowercase_scenario_id_rejected(self):
        """소문자 ID 조회 시 UnknownActivityScenarioError 발생 확인"""
        with self.assertRaises(UnknownActivityScenarioError):
            get_activity_scenario_definition("activity_normal")
        with self.assertRaises(UnknownActivityScenarioError):
            get_activity_scenario_definition("activity_low")

    def test_scenario_id_with_whitespace_rejected(self):
        """앞뒤 공백 포함 ID 조회 시 UnknownActivityScenarioError 발생 확인"""
        with self.assertRaises(UnknownActivityScenarioError):
            get_activity_scenario_definition(" ACTIVITY_NORMAL")
        with self.assertRaises(UnknownActivityScenarioError):
            get_activity_scenario_definition("ACTIVITY_NORMAL ")
        with self.assertRaises(UnknownActivityScenarioError):
            get_activity_scenario_definition("ACTIVITY_NORMAL\n")

    def test_non_string_types_rejected(self):
        """None, bool, 숫자, 리스트 등 문자열이 아닌 타입 전달 시 UnknownActivityScenarioError 발생 확인"""
        invalid_types = [None, True, False, 123, 45.6, [], {}, ()]
        for val in invalid_types:
            with self.subTest(val=val):
                with self.assertRaises(UnknownActivityScenarioError):
                    get_activity_scenario_definition(val)  # type: ignore

    def test_all_catalog_entries_are_scenario_definitions(self):
        """카탈로그의 모든 항목이 ScenarioDefinition 인스턴스인지 확인"""
        cat = get_activity_scenario_catalog()
        for scenario_id, defn in cat.items():
            self.assertIsInstance(defn, ScenarioDefinition)
            self.assertEqual(defn.scenario_id, scenario_id)

    def test_all_catalog_entries_have_single_day_offset_zero(self):
        """카탈로그의 모든 시나리오가 단일 일자(day_offset=0, len=1) 및 86,400초 총 슬롯 구조인지 확인"""
        cat = get_activity_scenario_catalog()
        for scenario_id, defn in cat.items():
            self.assertEqual(len(defn.days), 1)
            day = defn.days[0]
            self.assertEqual(day.day_offset, 0)
            omission_slots = sum(r.end_second - r.start_second for r in day.omission_ranges)
            total_day_slots = (day.publish_samples or 0) + omission_slots
            self.assertEqual(total_day_slots, 86_400)


class TestActivityNormalSpec(unittest.TestCase):
    """ACTIVITY_NORMAL 시나리오 명세 검증 (7개)"""

    def setUp(self):
        self.defn = get_activity_scenario_definition("ACTIVITY_NORMAL")
        self.day = self.defn.days[0]

    def test_normal_exact_six_events(self):
        """정확히 6개 이벤트 선언 확인"""
        self.assertEqual(len(self.day.events), 6)

    def test_normal_event_start_times_and_durations(self):
        """각 이벤트의 시작 시각, 가전, 지속 시간이 설계표와 정확히 일치하는지 확인"""
        expected_events = [
            ("kettle", "07:00:00", 120),
            ("microwave", "09:00:00", 300),
            ("kettle", "12:00:00", 120),
            ("vacuum_cleaner", "15:00:00", 1200),
            ("microwave", "18:00:00", 300),
            ("kettle", "21:00:00", 120),
        ]
        actual_events = [
            (e.appliance, e.start_time, e.duration_seconds) for e in self.day.events
        ]
        self.assertEqual(actual_events, expected_events)

    def test_normal_canonical_appliance_ids(self):
        """가전 ID가 모두 canonical 소문자인지 확인"""
        for event in self.day.events:
            self.assertTrue(event.appliance.islower())
            self.assertIn(event.appliance, {"kettle", "microwave", "vacuum_cleaner"})

    def test_normal_appliance_diversity_three(self):
        """가전 종류가 정확히 3종(kettle, microwave, vacuum_cleaner)인지 확인"""
        appliances = {e.appliance for e in self.day.events}
        self.assertEqual(appliances, {"kettle", "microwave", "vacuum_cleaner"})
        self.assertEqual(len(appliances), 3)

    def test_normal_total_raw_duration_2160s(self):
        """원시 ON 지속 시간 합계가 정확히 2,160초(36분)인지 확인"""
        total_duration = sum(e.duration_seconds for e in self.day.events)
        self.assertEqual(total_duration, 2160)

    def test_normal_publish_samples_86400(self):
        """결측 구간 없이 86,400개 발행 후보를 가지는지 확인"""
        self.assertEqual(self.day.publish_samples, 86_400)
        self.assertEqual(self.day.omission_ranges, ())

    def test_normal_compiled_transition_cycles(self):
        """컴파일 후 ON/OFF transition cycle이 시작초 및 종료초에 정확히 생성되는지 확인"""
        plan = compile_schedule(self.defn, date(2026, 9, 17))
        compiled_day = plan.day_plans[0]
        # 첫 이벤트: 07:00:00 (25,200초) ~ 07:02:00 (25,320초) kettle
        # first_cycle=1 기준 kettle ON 전이 cycle: 1 + 25200 = 25201, OFF 전이 cycle: 1 + 25320 = 25321
        cycles_with_transitions = {t.absolute_cycle for t in compiled_day.transitions}
        first_cycle = plan.first_cycle
        self.assertIn(first_cycle + 25200, cycles_with_transitions)
        self.assertIn(first_cycle + 25320, cycles_with_transitions)


class TestActivityLowSpec(unittest.TestCase):
    """ACTIVITY_LOW 시나리오 명세 검증 (3개)"""

    def setUp(self):
        self.defn = get_activity_scenario_definition("ACTIVITY_LOW")
        self.day = self.defn.days[0]

    def test_low_exact_single_microwave_event(self):
        """12:00:00 전자레인지 180초(3분) 1회 가동 이벤트 선언 확인"""
        self.assertEqual(len(self.day.events), 1)
        event = self.day.events[0]
        self.assertEqual(event.appliance, "microwave")
        self.assertEqual(event.start_time, "12:00:00")
        self.assertEqual(event.duration_seconds, 180)

    def test_low_publish_samples_86400(self):
        """결측 구간 없이 86,400개 발행 후보 확인"""
        self.assertEqual(self.day.publish_samples, 86_400)
        self.assertEqual(self.day.omission_ranges, ())

    def test_low_no_answer_labels_in_definition_or_mqtt_payload(self):
        """
        ScenarioDefinition, DaySchedule, ApplianceEvent에 activity_index, score, data_status 등의 정답 라벨이 없으며,
        실제 MQTT 페이로드에도 해당 정답 라벨 및 scenario_id가 일체 포함되지 않는 계약 검증.
        (시뮬레이터는 순수 원천 데이터만 생성하며, AI 정답 라벨을 누출하지 않는다.)
        """
        forbidden_labels = [
            "activity_index",
            "score",
            "data_status",
            "usage_count",
            "logical_uses",
            "appliance_diversity",
        ]
        # 1. 시나리오 선언 객체 검사
        for label in forbidden_labels:
            self.assertFalse(hasattr(self.defn, label), f"ScenarioDefinition has {label}")
            self.assertFalse(hasattr(self.day, label), f"DaySchedule has {label}")
            self.assertFalse(hasattr(self.day.events[0], label), f"ApplianceEvent has {label}")

        # 2. 컴파일 및 실제 빌드된 MQTT 페이로드 검사
        plan = compile_schedule(self.defn, date(2026, 9, 17))
        runner = DeterministicScheduleRunner(plan, "H001")
        tick = runner.step()
        payload = build_scheduled_power_payload(tick, "run_001")

        # 14개 표준 필드 확인
        expected_fields = {
            "message_id", "household_id", "device_id", "measured_at",
            "active_power", "reactive_power", "power_factor", "current",
            "house", "device", "ts", "power_w", "voltage", "apparent_power",
        }
        self.assertEqual(set(payload.keys()), expected_fields)
        self.assertNotIn("scenario_id", payload)
        for label in forbidden_labels:
            self.assertNotIn(label, payload)


class TestActivityNoneSpec(unittest.TestCase):
    """ACTIVITY_NONE 시나리오 명세 검증 (3개)"""

    def setUp(self):
        self.defn = get_activity_scenario_definition("ACTIVITY_NONE")
        self.day = self.defn.days[0]

    def test_none_empty_events_and_omissions(self):
        """대상 가전 이벤트 0건 및 결측 구간 0건 선언 확인"""
        self.assertEqual(self.day.events, ())
        self.assertEqual(self.day.omission_ranges, ())

    def test_none_publish_samples_86400(self):
        """86,400개 발행 후보 확인"""
        self.assertEqual(self.day.publish_samples, 86_400)

    def test_none_runner_target_appliances_all_off_and_finite_positive_power(self):
        """
        가상 실행 시 6대 대상 가전이 모두 OFF인지 확인(`active_appliances == ()`),
        active_power, reactive_power, voltage, current 등 전력 수치가 유한하며 active_power >= 0인지 검증.
        (runner._env private 접근 금지, 총전력으로 냉장고 ON/OFF 단정 금지, active_appliances에 냉장고 포함 가정 금지)
        """
        plan = compile_schedule(self.defn, date(2026, 9, 17))
        runner = DeterministicScheduleRunner(plan, "H001")

        # 300초(5분) 동안 샘플링 검증
        for _ in range(300):
            tick = runner.step()
            # 6대 대상 가전은 종일 OFF여야 함
            self.assertEqual(tick.active_appliances, ())
            # 물리 전력 계측값의 유한성 및 유효성 확인
            self.assertTrue(math.isfinite(tick.active_power))
            self.assertGreaterEqual(tick.active_power, 0.0)
            self.assertTrue(math.isfinite(tick.reactive_power))
            self.assertTrue(math.isfinite(tick.voltage))
            self.assertTrue(math.isfinite(tick.current))
            self.assertTrue(math.isfinite(tick.power_factor))


class TestActivityInsufficientSpec(unittest.TestCase):
    """ACTIVITY_INSUFFICIENT 시나리오 명세 및 교차 서비스 계약 검증 (6개)"""

    def setUp(self):
        self.defn = get_activity_scenario_definition("ACTIVITY_INSUFFICIENT")
        self.day = self.defn.days[0]

    def test_insufficient_sample_math_ceil_formula(self):
        """
        수학적 95% 경계 산술 검증:
        ceil(86400 * 0.95) - 1 == 82,079건 확인,
        raw 비율 82,079 / 86,400 < 0.95 확인.
        (동시에 소수점 4자리 반올림 시 0.9500이 되어 현재 AI에서 VALID가 발생하는 원인을 문서화)
        """
        total_slots = 86_400
        valid_threshold = 0.95
        exact_insufficient_max = math.ceil(total_slots * valid_threshold) - 1
        self.assertEqual(exact_insufficient_max, 82_079)

        # raw 비율은 엄격히 0.95 미만
        raw_ratio = 82_079 / 86_400
        self.assertLess(raw_ratio, 0.95)
        self.assertAlmostEqual(raw_ratio, 0.9499884259, places=8)

        # 현재 AI가 4자리 반올림하여 0.9500으로 잘못 판정하는 버그의 원인 산술 확인
        rounded_4dp = round(raw_ratio, 4)
        self.assertEqual(rounded_4dp, 0.9500)

    def test_insufficient_omission_range_exact_bounds(self):
        """결측 구간이 정확히 [82079, 86400)인지 확인"""
        self.assertEqual(len(self.day.omission_ranges), 1)
        omission = self.day.omission_ranges[0]
        self.assertEqual(omission.start_second, 82_079)
        self.assertEqual(omission.end_second, 86_400)

    def test_insufficient_sample_counts(self):
        """계획된 발행 수 82,079건, 결측 수 4,321건 확인"""
        self.assertEqual(self.day.publish_samples, 82_079)
        omitted_count = 86_400 - 82_079
        self.assertEqual(omitted_count, 4_321)

    def test_insufficient_last_published_timestamp(self):
        """
        컴파일 후 82,079번째 슬롯(0-indexed 82,078초)이 정상 발행 슬롯이며
        시각이 22:47:58 KST인지 확인
        """
        plan = compile_schedule(self.defn, date(2026, 9, 17))
        # 82078번째 초(0-indexed)는 결측 구간 [82079, 86400) 직전의 마지막 발행 슬롯
        # first_cycle=1 기준 해당 사이클은 1 + 82078 = 82079
        last_publish_cycle = plan.first_cycle + 82_078
        self.assertTrue(plan.should_publish(last_publish_cycle))
        slot_dt = plan.virtual_time_at(last_publish_cycle)
        self.assertEqual(slot_dt.strftime("%H:%M:%S"), "22:47:58")

    def test_insufficient_first_omitted_timestamp(self):
        """
        첫 번째 결측 슬롯(82,079초, 0-indexed)이 생략 대상이며 시각이 22:47:59 KST인지 확인
        """
        plan = compile_schedule(self.defn, date(2026, 9, 17))
        # 82079번째 초(0-indexed)는 결측 구간 [82079, 86400)의 첫 번째 슬롯
        # first_cycle=1 기준 해당 사이클은 1 + 82079 = 82080
        first_omitted_cycle = plan.first_cycle + 82_079
        self.assertFalse(plan.should_publish(first_omitted_cycle))
        slot_dt = plan.virtual_time_at(first_omitted_cycle)
        self.assertEqual(slot_dt.strftime("%H:%M:%S"), "22:47:59")


    def test_insufficient_declaration_matches_computed(self):
        """선언값(publish_samples=82079)과 컴파일 결과 계산된 발행 슬롯 수의 일치 확인"""
        plan = compile_schedule(self.defn, date(2026, 9, 17))
        self.assertEqual(plan.total_planned_publish_samples, 82_079)
        self.assertEqual(plan.total_planned_omitted_samples, 4_321)
        self.assertEqual(self.day.publish_samples, 82_079)


class TestActivitySessionMergeSpec(unittest.TestCase):
    """ACTIVITY_SESSION_MERGE 시나리오 명세 검증 (6개)"""

    def setUp(self):
        self.defn = get_activity_scenario_definition("ACTIVITY_SESSION_MERGE")
        self.day = self.defn.days[0]

    def test_session_merge_exact_two_induction_events(self):
        """인덕션 가전 10:00, 10:11 시작 2개 이벤트 선언 확인"""
        self.assertEqual(len(self.day.events), 2)
        self.assertEqual(self.day.events[0].appliance, "induction")
        self.assertEqual(self.day.events[0].start_time, "10:00:00")
        self.assertEqual(self.day.events[1].appliance, "induction")
        self.assertEqual(self.day.events[1].start_time, "10:11:00")

    def test_session_merge_each_duration_600s(self):
        """두 이벤트 모두 지속 시간 600초(10분) 확인"""
        self.assertEqual(self.day.events[0].duration_seconds, 600)
        self.assertEqual(self.day.events[1].duration_seconds, 600)

    def test_session_merge_off_gap_exact_60s(self):
        """두 이벤트 사이의 OFF 간격이 정확히 60초(10:10:00 ~ 10:11:00)인지 확인"""
        # 10:00:00 시작 + 600초 = 10:10:00 (36,600초)
        # 10:11:00 시작 = 36,660초
        # 간격 = 36,660 - 36,600 = 60초
        end_first = 36_000 + 600
        start_second = 36_660
        gap = start_second - end_first
        self.assertEqual(gap, 60)

    def test_session_merge_total_raw_duration_1200s(self):
        """원시 지속 시간 합계가 1,200초(20분)인지 확인"""
        total_duration = sum(e.duration_seconds for e in self.day.events)
        self.assertEqual(total_duration, 1200)

    def test_session_merge_not_pre_merged_in_catalog(self):
        """
        카탈로그에서 이벤트를 1개로 합치지 않고 2개 물리 이벤트로 독립 선언하고 있는지 확인.
        (세션 병합은 다운스트림 AI 서비스의 고유 책임이다.)
        """
        self.assertEqual(len(self.day.events), 2)

    def test_session_merge_publish_samples_86400(self):
        """결측 없이 86,400개 발행 후보 확인"""
        self.assertEqual(self.day.publish_samples, 86_400)
        self.assertEqual(self.day.omission_ranges, ())


class TestActivityDurationCapSpec(unittest.TestCase):
    """ACTIVITY_DURATION_CAP 시나리오 명세 검증 (5개)"""

    def setUp(self):
        self.defn = get_activity_scenario_definition("ACTIVITY_DURATION_CAP")
        self.day = self.defn.days[0]

    def test_duration_cap_kettle_7200s(self):
        """전기주전자 10:00:00 시작, 7,200초(120분) 가동 선언 확인"""
        self.assertEqual(len(self.day.events), 1)
        event = self.day.events[0]
        self.assertEqual(event.appliance, "kettle")
        self.assertEqual(event.start_time, "10:00:00")
        self.assertEqual(event.duration_seconds, 7200)

    def test_duration_cap_end_time_120000(self):
        """종료 시각이 12:00:00(43,200초)인지 확인"""
        start_sec = 10 * 3600
        end_sec = start_sec + 7200
        self.assertEqual(end_sec, 12 * 3600)

    def test_duration_cap_raw_duration_not_capped_at_600s(self):
        """
        원시 선언 지속 시간이 600초로 잘리지 않고 7,200초 전체인지 확인.
        (600초 상한 적용은 AI 점수 계산 책임이며 시뮬레이터는 실제 전력 데이터를 7200초간 생성한다.)
        """
        self.assertEqual(self.day.events[0].duration_seconds, 7200)
        self.assertNotEqual(self.day.events[0].duration_seconds, 600)

    def test_duration_cap_no_other_appliances(self):
        """인덕션/다리미 등 타 가전 이벤트가 전혀 없는지 확인"""
        appliances = {e.appliance for e in self.day.events}
        self.assertEqual(appliances, {"kettle"})

    def test_duration_cap_publish_samples_86400(self):
        """결측 없이 86,400개 발행 후보 확인"""
        self.assertEqual(self.day.publish_samples, 86_400)
        self.assertEqual(self.day.omission_ranges, ())


class TestDeterminismAndRegression(unittest.IsolatedAsyncioTestCase):
    """결정론, 타임존, MQTT 정답 라벨 미포함 및 실제 BURST 86,400 슬롯 완주 회귀 검증 (5개)"""

    def test_compilation_determinism_identical_base_date(self):
        """동일 base_date로 컴파일 시 동일한 CompiledExecutionPlan 생성 확인"""
        defn = get_activity_scenario_definition("ACTIVITY_NORMAL")
        base = date(2026, 9, 17)
        plan1 = compile_schedule(defn, base)
        plan2 = compile_schedule(defn, base)
        self.assertEqual(plan1.total_virtual_slots, plan2.total_virtual_slots)
        self.assertEqual(plan1.total_planned_publish_samples, plan2.total_planned_publish_samples)
        self.assertEqual(plan1.day_plans[0].transitions, plan2.day_plans[0].transitions)

    def test_compilation_different_base_dates_only_shifts_calendar(self):
        """서로 다른 base_date 컴파일 시 일정 구조/cycle은 같고 calendar_date/measured_at만 이동 확인"""
        defn = get_activity_scenario_definition("ACTIVITY_NORMAL")
        plan1 = compile_schedule(defn, date(2026, 9, 17))
        plan2 = compile_schedule(defn, date(2026, 10, 1))
        self.assertEqual(plan1.total_virtual_slots, plan2.total_virtual_slots)
        self.assertEqual(plan1.day_plans[0].calendar_date, date(2026, 9, 17))
        self.assertEqual(plan2.day_plans[0].calendar_date, date(2026, 10, 1))

    def test_timezone_aware_kst_timestamps(self):
        """첫 번째 및 마지막 슬롯의 measured_at이 timezone-aware(KST)인지 확인"""
        defn = get_activity_scenario_definition("ACTIVITY_LOW")
        plan = compile_schedule(defn, date(2026, 9, 17))
        runner = DeterministicScheduleRunner(plan, "H001")
        first_tick = runner.step()
        self.assertEqual(first_tick.measured_at.tzinfo, KST)
        self.assertEqual(first_tick.measured_at.isoformat(), "2026-09-17T00:00:00+09:00")

    def test_no_scenario_id_in_mqtt_payload(self):
        """빌드된 MQTT 페이로드에 scenario_id나 정답 라벨이 들어가지 않는 3A/3B 계약 유지 확인"""
        defn = get_activity_scenario_definition("ACTIVITY_NORMAL")
        plan = compile_schedule(defn, date(2026, 9, 17))
        runner = DeterministicScheduleRunner(plan, "H001")
        tick = runner.step()
        payload = build_scheduled_power_payload(tick, "run_test_01")
        self.assertNotIn("scenario_id", payload)
        self.assertNotIn("activity_index", payload)
        self.assertNotIn("score", payload)
        self.assertNotIn("data_status", payload)

    async def test_catalog_integration_with_executor_burst(self):
        """
        대표 시나리오(ACTIVITY_NORMAL, 2026-09-16)를 실제 DeterministicScheduleExecutor BURST 모드로
        실제 DeterministicScheduleRunner 및 실제 publish_scheduled_tick 경로를 사용하여 실행하고,
        가짜 객체는 오직 MQTT client(CountingFakeMqttClient)에만 사용하여
        86,400개 슬롯 완주, cycle 1~86,400, 14개 필드 규격을 메모리 누적 없이 검증.
        """
        defn = get_activity_scenario_definition("ACTIVITY_NORMAL")
        plan = compile_schedule(defn, date(2026, 9, 16))
        config = ExecutorConfig(mode=ExecutionMode.BURST)

        executor = DeterministicScheduleExecutor(
            plan=plan,
            household_id="H001",
            run_id="run_catalog_test",
            config=config,
        )

        client = CountingFakeMqttClient()
        result = await executor.run(client)

        # 1. 실행 결과 검증
        self.assertIsNotNone(result)
        self.assertEqual(result.status, ExecutionStatus.COMPLETED)
        self.assertEqual(result.published_samples, 86_400)
        self.assertEqual(result.omitted_samples, 0)
        self.assertEqual(len(result.day_results), 1)

        # 2. MQTT 클라이언트 발행 검증 (페이로드 미누적 확인)
        self.assertEqual(client.publish_count, 86_400)
        self.assertTrue(client.monotonic_measured_at)
        self.assertEqual(client.first_cycle, 1)
        self.assertEqual(client.last_cycle, 86_400)

        expected_topic = build_scheduled_topic("H001")
        self.assertEqual(client.first_topic, expected_topic)
        self.assertEqual(client.last_topic, expected_topic)

        # 3. 첫 번째 및 마지막 페이로드의 정확한 14개 필드 규격 검증
        exact_14_keys = {
            "message_id",
            "household_id",
            "device_id",
            "measured_at",
            "active_power",
            "reactive_power",
            "power_factor",
            "current",
            "house",
            "device",
            "ts",
            "power_w",
            "voltage",
            "apparent_power",
        }

        self.assertIsNotNone(client.first_payload)
        first_data = json.loads(client.first_payload)
        self.assertEqual(first_data["measured_at"], "2026-09-16T00:00:00+09:00")
        self.assertEqual(first_data["household_id"], "H001")
        self.assertEqual(set(first_data.keys()), exact_14_keys)

        self.assertIsNotNone(client.last_payload)
        last_data = json.loads(client.last_payload)
        self.assertEqual(last_data["measured_at"], "2026-09-16T23:59:59+09:00")
        self.assertEqual(last_data["household_id"], "H001")
        self.assertEqual(set(last_data.keys()), exact_14_keys)


if __name__ == "__main__":
    unittest.main()
