"""
NILM 스마트홈 전력 시뮬레이터 결정론적 MQTT 발행 어댑터 (3A 단계) 단위 테스트 스위트

- ScheduledTick to MQTT 원천 전력 페이로드 변환 계약 검증
- UUID5 기반 결정론적 message_id 생성 및 재시도 안정성 검증
- 결측 슬롯(is_publish_candidate=False) 무부하 OMITTED 처분 검증
- aiomqtt 2.5.1 QoS 1 및 retain=False 발행 계약 검증
- 비정상 부동소수점(NaN, +Inf, -Inf) 거절 및 예외 체이닝 경계 검증
- 공개 함수별 독립 입력 검증
"""

from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError
from datetime import date, datetime, timezone
import json
import math
import random
from typing import Any
import unittest
import uuid

from engine.schedule import KST, DaySchedule, ScenarioDefinition, compile_schedule
from engine.schedule_runtime import (
    DeterministicScheduleRunner,
    ScheduledTick,
)
from engine.schedule_publisher import (
    SCHEDULE_PUBLISHER_NAMESPACE,
    PublishDisposition,
    ScheduledPublishResult,
    SchedulePublishError,
    MqttPublishClient,
    build_scheduled_topic,
    generate_deterministic_message_id,
    build_scheduled_power_payload,
    serialize_scheduled_power_payload,
    publish_scheduled_tick,
)


class FakeMqttClient:
    """테스트용 비동기 인메모리 MQTT 클라이언트 (aiomqtt.Client 호환)"""

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
            "args": args,
            "kwargs": kwargs,
        })
        if self.should_fail:
            raise self.fail_exc
        return None


def make_sample_tick(
    household_id: str = "H001",
    cycle: int = 0,
    measured_at: datetime | None = None,
    is_publish_candidate: bool = True,
    active_power: float = 150.5,
    reactive_power: float = 30.2,
    apparent_power: float = 153.51,
    power_factor: float = 0.98,
    voltage: float = 220.3,
    current: float = 0.697,
) -> ScheduledTick:
    """테스트용 유효한 ScheduledTick 인스턴스 팩토리"""
    if measured_at is None:
        measured_at = datetime(2026, 9, 17, 0, 0, 0, tzinfo=KST)
    return ScheduledTick(
        household_id=household_id,
        cycle=cycle,
        measured_at=measured_at,
        is_publish_candidate=is_publish_candidate,
        transitions=(),
        active_appliances=(),
        active_power=active_power,
        reactive_power=reactive_power,
        apparent_power=apparent_power,
        power_factor=power_factor,
        voltage=voltage,
        current=current,
    )


class TestSchedulePublisher(unittest.IsolatedAsyncioTestCase):
    """결정적 MQTT 발행 어댑터 단위 테스트 스위트"""

    def setUp(self):
        self.run_id = "run-20260917-001"
        self.sample_tick = make_sample_tick()

    # ----------------------------------------------------
    # 1. 페이로드 필드 매핑 및 키 집합 검증
    # ----------------------------------------------------
    def test_payload_field_mapping(self):
        """ScheduledTick 계측 필드가 payload 14개 필드에 정확히 매핑되는지 검증"""
        tick = make_sample_tick(
            household_id="H002",
            cycle=42,
            measured_at=datetime(2026, 9, 17, 10, 30, 0, tzinfo=KST),
            active_power=345.67,
            reactive_power=56.78,
            apparent_power=350.3,
            power_factor=0.987,
            voltage=221.4,
            current=1.56,
        )
        payload = build_scheduled_power_payload(tick, self.run_id)

        self.assertEqual(payload["household_id"], "H002")
        self.assertEqual(payload["device_id"], "main")
        self.assertEqual(payload["measured_at"], "2026-09-17T10:30:00+09:00")
        self.assertEqual(payload["active_power"], 345.67)
        self.assertEqual(payload["reactive_power"], 56.78)
        self.assertEqual(payload["power_factor"], 0.987)
        self.assertEqual(payload["current"], 1.56)
        self.assertEqual(payload["house"], "H002")
        self.assertEqual(payload["device"], "main")
        self.assertEqual(payload["ts"], "2026-09-17T10:30:00+09:00")
        self.assertEqual(payload["power_w"], 345.67)
        self.assertEqual(payload["voltage"], 221.4)
        self.assertEqual(payload["apparent_power"], 350.3)
        self.assertTrue(isinstance(payload["message_id"], str))

    def test_payload_key_set_exact_match(self):
        """payload의 키가 정확히 14개이며 초과/누락 키가 없는지 검증"""
        payload = build_scheduled_power_payload(self.sample_tick, self.run_id)
        expected_keys = {
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
        self.assertEqual(set(payload.keys()), expected_keys)
        self.assertEqual(len(payload), 14)

    def test_timestamp_consistency(self):
        """measured_at과 ts가 완전히 동일하고 timezone offset을 포함하는지 검증"""
        payload = build_scheduled_power_payload(self.sample_tick, self.run_id)
        self.assertEqual(payload["measured_at"], payload["ts"])
        self.assertTrue("+09:00" in payload["measured_at"] or "Z" in payload["measured_at"])

    def test_power_fields_consistency(self):
        """active_power와 power_w가 완전히 동일한 수치인지 검증"""
        payload = build_scheduled_power_payload(self.sample_tick, self.run_id)
        self.assertEqual(payload["active_power"], payload["power_w"])
        self.assertIs(type(payload["active_power"]), float)

    def test_no_answer_labels_or_scenario_metadata(self):
        """정답 라벨이나 시나리오 내부 메타데이터가 페이로드에 포함되지 않는지 검증"""
        payload = build_scheduled_power_payload(self.sample_tick, self.run_id)
        forbidden_keys = {
            "scenario_id",
            "transitions",
            "active_appliances",
            "is_publish_candidate",
            "expected_event_type",
            "activity_index",
            "run_id",
        }
        for key in forbidden_keys:
            self.assertNotIn(key, payload)

    # ----------------------------------------------------
    # 2. 결정론 및 직렬화 검증
    # ----------------------------------------------------
    def test_payload_determinism_identical_input(self):
        """동일 tick과 동일 run_id에서 payload dict 완전 동일성 검증"""
        p1 = build_scheduled_power_payload(self.sample_tick, self.run_id)
        p2 = build_scheduled_power_payload(self.sample_tick, self.run_id)
        self.assertEqual(p1, p2)

    def test_serialization_bytes_identical(self):
        """동일 tick과 동일 run_id의 JSON 직렬화 바이트 및 문자열 완전 동일성 검증"""
        p1 = build_scheduled_power_payload(self.sample_tick, self.run_id)
        s1 = serialize_scheduled_power_payload(p1)
        s2 = serialize_scheduled_power_payload(p1)
        self.assertEqual(s1, s2)
        # 키 정렬 검증
        parsed = json.loads(s1)
        expected_serialized = json.dumps(
            parsed,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        self.assertEqual(s1, expected_serialized)

    def test_message_id_determinism(self):
        """동일 tick과 동일 run_id에서 항상 동일한 UUID5 message_id 생성 검증"""
        m1 = generate_deterministic_message_id(self.sample_tick, self.run_id)
        m2 = generate_deterministic_message_id(self.sample_tick, self.run_id)
        self.assertEqual(m1, m2)

    def test_message_id_differs_for_different_run_id(self):
        """동일 tick이라도 run_id가 다르면 message_id가 상이함 검증"""
        m1 = generate_deterministic_message_id(self.sample_tick, "run-001")
        m2 = generate_deterministic_message_id(self.sample_tick, "run-002")
        self.assertNotEqual(m1, m2)

    def test_message_id_differs_for_different_cycle(self):
        """동일 run_id라도 cycle이 다르면 message_id가 상이함 검증"""
        tick1 = make_sample_tick(cycle=0)
        tick2 = make_sample_tick(cycle=1)
        m1 = generate_deterministic_message_id(tick1, self.run_id)
        m2 = generate_deterministic_message_id(tick2, self.run_id)
        self.assertNotEqual(m1, m2)

    def test_message_id_is_valid_uuid(self):
        """message_id가 유효한 RFC 4122 v5 UUID 형식인지 검증"""
        m = generate_deterministic_message_id(self.sample_tick, self.run_id)
        u = uuid.UUID(m)
        self.assertEqual(u.version, 5)
        self.assertEqual(str(u), m)

    def test_message_id_matches_versioned_golden_vector(self):
        """
        고정 입력과 하드코딩된 기대 UUID를 직접 비교하는 골든 벡터 테스트.
        동일한 구현식을 테스트 안에서 다시 조립하지 않고 사전 계산된 기대값과 직접 비교한다.
        구분자, 네임스페이스, 버전 접두사('SCHEDULE_PUBLISHER_V1::') 중 하나라도 변경되면 실패해야 한다.
        """
        # namespace: a9e29f27-6f11-4eb7-9c60-84dfa183561a
        # run_id: run-20260917-001
        # household_id: H001
        # cycle: 0
        # measured_at: 2026-09-17T00:00:00+09:00
        # UUID5 name: SCHEDULE_PUBLISHER_V1::run-20260917-001::H001::0::2026-09-17T00:00:00+09:00
        # expected message_id: 87bb97a7-8c89-593c-8520-58c2f7853b39
        tick = make_sample_tick(
            household_id="H001",
            cycle=0,
            measured_at=datetime(2026, 9, 17, 0, 0, 0, tzinfo=KST),
        )
        run_id = "run-20260917-001"
        expected_message_id = "87bb97a7-8c89-593c-8520-58c2f7853b39"

        actual_message_id = generate_deterministic_message_id(tick, run_id)
        self.assertEqual(actual_message_id, expected_message_id)

    # ----------------------------------------------------
    # 3. 토픽 및 비동기 발행 프로토콜 검증
    # ----------------------------------------------------
    def test_topic_format_matches_contract(self):
        """build_scheduled_topic('H001')이 v1/power/sim/H001/main과 정확히 일치하는지 검증"""
        self.assertEqual(build_scheduled_topic("H001"), "v1/power/sim/H001/main")
        self.assertEqual(build_scheduled_topic("H999"), "v1/power/sim/H999/main")

    async def test_publish_uses_qos1_and_retain_false(self):
        """client.publish가 정확히 1회, qos=1, retain=False, 정확한 토픽/페이로드로 호출되는지 검증"""
        client = FakeMqttClient()
        result = await publish_scheduled_tick(client, self.sample_tick, self.run_id)

        self.assertEqual(len(client.calls), 1)
        call = client.calls[0]
        self.assertEqual(call["topic"], "v1/power/sim/H001/main")
        self.assertEqual(call["qos"], 1)
        self.assertEqual(call["retain"], False)
        # 페이로드 내용 검증
        payload = json.loads(call["payload"])
        self.assertEqual(payload["message_id"], result.message_id)

    async def test_published_result_returned_on_success(self):
        """publish 정상 완료 후 PUBLISHED, 유효한 topic, 유효한 message_id가 반환되는지 검증"""
        client = FakeMqttClient()
        result = await publish_scheduled_tick(client, self.sample_tick, self.run_id)

        self.assertEqual(result.household_id, "H001")
        self.assertEqual(result.cycle, 0)
        self.assertEqual(result.disposition, PublishDisposition.PUBLISHED)
        self.assertEqual(result.topic, "v1/power/sim/H001/main")
        self.assertIsNotNone(result.message_id)

    # ----------------------------------------------------
    # 4. 결측 및 검증 순서 계약
    # ----------------------------------------------------
    async def test_validation_occurs_before_omission_decision(self):
        """잘못된 run_id/household_id는 omitted tick이라도 OMITTED 판정 전에 즉시 예외 발생 검증"""
        omitted_tick = make_sample_tick(is_publish_candidate=False)
        client = FakeMqttClient()

        # 잘못된 run_id는 결측 틱이어도 예외 발생
        with self.assertRaises(ValueError):
            await publish_scheduled_tick(client, omitted_tick, run_id="")
        with self.assertRaises(TypeError):
            await publish_scheduled_tick(client, omitted_tick, run_id=1234)

        # 잘못된 tick의 household_id도 결측 틱이어도 예외 발생
        bad_household_tick = ScheduledTick(
            household_id="H/01+",
            cycle=0,
            measured_at=datetime(2026, 9, 17, 0, 0, 0, tzinfo=KST),
            is_publish_candidate=False,
            transitions=(),
            active_appliances=(),
            active_power=0.0,
            reactive_power=0.0,
            apparent_power=0.0,
            power_factor=1.0,
            voltage=220.0,
            current=0.0,
        )
        with self.assertRaises(ValueError):
            await publish_scheduled_tick(client, bad_household_tick, self.run_id)

    def test_naive_measured_at_rejected(self):
        """timezone 정보가 없는 naive datetime tick 전달 시 ValueError 거절 검증"""
        naive_tick = make_sample_tick(
            measured_at=datetime(2026, 9, 17, 0, 0, 0)  # tzinfo=None
        )
        with self.assertRaises(ValueError):
            generate_deterministic_message_id(naive_tick, self.run_id)
        with self.assertRaises(ValueError):
            build_scheduled_power_payload(naive_tick, self.run_id)

    def test_omitted_builder_and_message_id_generation_rejected(self):
        """결측 tick을 build_scheduled_power_payload 및 generate_deterministic_message_id에 전달 시 ValueError 거절 검증"""
        omitted_tick = make_sample_tick(is_publish_candidate=False)
        with self.assertRaises(ValueError):
            generate_deterministic_message_id(omitted_tick, self.run_id)
        with self.assertRaises(ValueError):
            build_scheduled_power_payload(omitted_tick, self.run_id)

    async def test_omitted_publish_skips_uuid_serialization_and_network(self):
        """결측 tick 발행 시 publish 미호출, message_id 미생성, JSON 직렬화 미수행, OMITTED 반환 검증"""
        omitted_tick = make_sample_tick(is_publish_candidate=False)
        client = FakeMqttClient()

        result = await publish_scheduled_tick(client, omitted_tick, self.run_id)
        self.assertEqual(result.disposition, PublishDisposition.OMITTED)
        self.assertIsNone(result.topic)
        self.assertIsNone(result.message_id)
        self.assertEqual(len(client.calls), 0)

    async def test_omitted_tick_with_none_client_succeeds(self):
        """결측 틱은 client를 사용하지 않으므로 client=None이어도 OMITTED 정상 반환 검증"""
        omitted_tick = make_sample_tick(is_publish_candidate=False)
        result = await publish_scheduled_tick(None, omitted_tick, self.run_id)
        self.assertEqual(result.disposition, PublishDisposition.OMITTED)
        self.assertIsNone(result.topic)
        self.assertIsNone(result.message_id)

    async def test_publish_candidate_with_invalid_client_raises_type_error(self):
        """발행 후보 tick에 publish 메서드가 없는 객체 전달 시 TypeError 발생 검증"""
        with self.assertRaises(TypeError):
            await publish_scheduled_tick(None, self.sample_tick, self.run_id)

        class NotAClient:
            pass

        with self.assertRaises(TypeError):
            await publish_scheduled_tick(NotAClient(), self.sample_tick, self.run_id)

    # ----------------------------------------------------
    # 5. 예외 체이닝 및 직렬화 오류 경계 검증
    # ----------------------------------------------------
    async def test_publish_error_wraps_only_client_publish_failure(self):
        """client.publish 실행 중 발생한 네트워크/브로커 예외만 SchedulePublishError로 감싸고 원인 체이닝 검증"""
        client = FakeMqttClient(should_fail=True, fail_exc=TimeoutError("PUBACK timeout"))

        with self.assertRaises(SchedulePublishError) as ctx:
            await publish_scheduled_tick(client, self.sample_tick, self.run_id)

        self.assertIsInstance(ctx.exception.__cause__, TimeoutError)
        self.assertIn("PUBACK timeout", str(ctx.exception))

    async def test_serialization_error_not_wrapped_as_network_failure(self):
        """payload 내 NaN 주입 시 직렬화에서 ValueError가 발생하며 SchedulePublishError로 오인 래핑되지 않음 검증"""
        nan_tick = make_sample_tick(active_power=float("nan"))
        client = FakeMqttClient()

        # serialize_scheduled_power_payload 직접 호출 검증
        payload = build_scheduled_power_payload(nan_tick, self.run_id)
        with self.assertRaises(ValueError):
            serialize_scheduled_power_payload(payload)

        # publish_scheduled_tick 호출 시 ValueError 발생, SchedulePublishError 아님
        with self.assertRaises(ValueError):
            await publish_scheduled_tick(client, nan_tick, self.run_id)

        # client.publish가 절대 호출되지 않음
        self.assertEqual(len(client.calls), 0)

    async def test_nan_and_infinity_rejection_for_all_values(self):
        """
        비정상 부동소수점 값(NaN, +Infinity, -Infinity) 각각에 대해
        serialize_scheduled_power_payload 및 publish_scheduled_tick에서 ValueError 거절 검증.
        client.publish는 절대 호출되지 않아야 하며 SchedulePublishError로 감싸지 않는다.
        """
        invalid_floats = [float("nan"), float("inf"), float("-inf")]

        for bad_val in invalid_floats:
            with self.subTest(bad_float=bad_val):
                bad_tick = make_sample_tick(active_power=bad_val)
                client = FakeMqttClient()

                # 1. serialize_scheduled_power_payload 직접 호출 검증
                payload = build_scheduled_power_payload(bad_tick, self.run_id)
                with self.assertRaises(ValueError):
                    serialize_scheduled_power_payload(payload)

                # 2. publish_scheduled_tick 검증 (ValueError 발생, SchedulePublishError 아님)
                with self.assertRaises(ValueError) as ctx:
                    await publish_scheduled_tick(client, bad_tick, self.run_id)

                self.assertNotIsInstance(ctx.exception, SchedulePublishError)

                # 3. client.publish 미호출 확인
                self.assertEqual(len(client.calls), 0)

    # ----------------------------------------------------
    # 6. 재시도 안정성 및 격리성 검증
    # ----------------------------------------------------
    async def test_retry_same_tick_produces_identical_publish_call(self):
        """실패 후 동일 tick과 동일 run_id로 재시도 시 topic, message_id, payload bytes가 완전 동일함 검증"""
        failing_client = FakeMqttClient(should_fail=True)
        with self.assertRaises(SchedulePublishError):
            await publish_scheduled_tick(failing_client, self.sample_tick, self.run_id)

        # 재시도 (동일 run_id, 동일 tick)
        success_client = FakeMqttClient(should_fail=False)
        result = await publish_scheduled_tick(success_client, self.sample_tick, self.run_id)

        self.assertEqual(len(failing_client.calls), 1)
        self.assertEqual(len(success_client.calls), 1)
        self.assertEqual(failing_client.calls[0]["topic"], success_client.calls[0]["topic"])
        self.assertEqual(failing_client.calls[0]["payload"], success_client.calls[0]["payload"])
        self.assertEqual(failing_client.calls[0]["qos"], success_client.calls[0]["qos"])
        self.assertEqual(failing_client.calls[0]["retain"], success_client.calls[0]["retain"])

    async def test_adapter_never_mutates_runner(self):
        """어댑터 함수 호출이 DeterministicScheduleRunner의 내부 상태나 step을 변경하지 않음을 검증"""
        day = DaySchedule(day_offset=0, events=())
        defn = ScenarioDefinition(scenario_id="ACTIVITY_NORMAL", days=(day,))
        plan = compile_schedule(defn, date(2026, 9, 17))
        runner = DeterministicScheduleRunner(plan, "H001")

        tick = runner.step()
        snapshot_before = runner.snapshot()

        client = FakeMqttClient()
        await publish_scheduled_tick(client, tick, self.run_id)

        snapshot_after = runner.snapshot()
        self.assertEqual(snapshot_before, snapshot_after)
        self.assertEqual(runner.processed_virtual_slots, 1)

    # ----------------------------------------------------
    # 7. 입력 검증 규칙 (run_id, household_id)
    # ----------------------------------------------------
    def test_run_id_validation_rules(self):
        """run_id 빈 문자열, 앞뒤 공백, None, bool, 제어문자, 줄바꿈, 65자 이상 거절 검증"""
        # 타입 오류
        with self.assertRaises(TypeError):
            generate_deterministic_message_id(self.sample_tick, None)
        with self.assertRaises(TypeError):
            generate_deterministic_message_id(self.sample_tick, True)
        with self.assertRaises(TypeError):
            generate_deterministic_message_id(self.sample_tick, 123)

        # 값 오류
        invalid_run_ids = [
            "",
            " run1",
            "run1 ",
            "run\n1",
            "run/1",
            "run.1",
            "run@1",
            "a" * 65,
        ]
        for bad_id in invalid_run_ids:
            with self.subTest(bad_id=bad_id):
                with self.assertRaises(ValueError):
                    generate_deterministic_message_id(self.sample_tick, bad_id)

    def test_household_id_topic_safety_validation(self):
        """household_id /, +, #, \0, 공백, 제어문자 포함 가구 ID 거절 검증"""
        invalid_households = [
            "",
            " H001",
            "H001 ",
            "H001/extra",
            "H001+",
            "H001#",
            "H\001",
            "H001\n",
            "a" * 51,
        ]
        for bad_id in invalid_households:
            with self.subTest(bad_id=bad_id):
                with self.assertRaises(ValueError):
                    build_scheduled_topic(bad_id)

        with self.assertRaises(TypeError):
            build_scheduled_topic(None)
        with self.assertRaises(TypeError):
            build_scheduled_topic(True)

    def test_independent_validation_of_public_functions(self):
        """
        모든 공개 함수가 직접 호출되더라도 자신의 입력 계약을 독립적으로 검증하는지 확인.
        publish_scheduled_tick을 우회한 직접 호출 시에도 검증이 생략되지 않아야 한다.
        """
        # 1. build_scheduled_topic
        with self.assertRaises(TypeError):
            build_scheduled_topic(1234)
        with self.assertRaises(ValueError):
            build_scheduled_topic("H/bad/topic")

        # 2. generate_deterministic_message_id
        with self.assertRaises(TypeError):
            generate_deterministic_message_id("not_a_tick", self.run_id)
        with self.assertRaises(TypeError):
            generate_deterministic_message_id(self.sample_tick, 999)

        # 3. build_scheduled_power_payload
        with self.assertRaises(TypeError):
            build_scheduled_power_payload("not_a_tick", self.run_id)
        with self.assertRaises(TypeError):
            build_scheduled_power_payload(self.sample_tick, 999)

        # 4. serialize_scheduled_power_payload
        with self.assertRaises(TypeError):
            serialize_scheduled_power_payload("not_a_mapping")
        with self.assertRaises(ValueError):
            serialize_scheduled_power_payload({"active_power": float("nan")})

    # ----------------------------------------------------
    # 8. 기타 계약 검증
    # ----------------------------------------------------
    def test_no_global_random_or_uuid4_influence(self):
        """uuid.uuid4 모킹 또는 전역 난수 상태 변경과 무관하게 결정론적 결과 검증"""
        original_random_state = random.getstate()
        try:
            random.seed(99999)
            m1 = generate_deterministic_message_id(self.sample_tick, self.run_id)
            random.seed(11111)
            m2 = generate_deterministic_message_id(self.sample_tick, self.run_id)
            self.assertEqual(m1, m2)
        finally:
            random.setstate(original_random_state)

    def test_execution_order_independence(self):
        """슬롯 A와 슬롯 B의 발행 순서를 바꾸어 호출해도 개별 결과가 불변임을 검증"""
        tick_a = make_sample_tick(cycle=10)
        tick_b = make_sample_tick(cycle=20)

        # 순서 1: A -> B
        p_a1 = build_scheduled_power_payload(tick_a, self.run_id)
        p_b1 = build_scheduled_power_payload(tick_b, self.run_id)

        # 순서 2: B -> A
        p_b2 = build_scheduled_power_payload(tick_b, self.run_id)
        p_a2 = build_scheduled_power_payload(tick_a, self.run_id)

        self.assertEqual(p_a1, p_a2)
        self.assertEqual(p_b1, p_b2)

    def test_tick_to_dict_separation_from_mqtt_payload(self):
        """ScheduledTick.to_dict()(내부 상태 덤프)와 build_scheduled_power_payload()(원천 MQTT)의 분리 검증"""
        state_dump = self.sample_tick.to_dict()
        mqtt_payload = build_scheduled_power_payload(self.sample_tick, self.run_id)

        # to_dict에는 transitions, active_appliances 등 내부 상태가 포함됨
        self.assertIn("transitions", state_dump)
        self.assertIn("active_appliances", state_dump)

        # mqtt_payload에는 message_id, ts, power_w 등 브로커 규격 필드가 포함되고 내부 상태는 배제됨
        self.assertIn("message_id", mqtt_payload)
        self.assertIn("ts", mqtt_payload)
        self.assertNotIn("transitions", mqtt_payload)
        self.assertNotIn("active_appliances", mqtt_payload)

    def test_result_dataclass_immutability(self):
        """ScheduledPublishResult가 frozen dataclass로서 필드 변조가 차단됨을 검증"""
        result = ScheduledPublishResult(
            household_id="H001",
            cycle=0,
            disposition=PublishDisposition.PUBLISHED,
            topic="v1/power/sim/H001/main",
            message_id="8f3b2a19-4d6e-5c72-9b12-a1b2c3d4e5f6",
        )
        with self.assertRaises(FrozenInstanceError):
            result.cycle = 99
        with self.assertRaises(FrozenInstanceError):
            result.disposition = PublishDisposition.OMITTED


if __name__ == "__main__":
    unittest.main()
