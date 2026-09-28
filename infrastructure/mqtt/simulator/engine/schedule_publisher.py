"""
NILM 스마트홈 전력 시뮬레이터 결정론적 MQTT 발행 어댑터 (Deterministic MQTT Publishing Adapter)

- ScheduledTick을 원천 전력 페이로드 규격(14개 필드)으로 변환하고 MQTT 브로커에 QoS 1로 발행
- 가상 시간 상태 전진(runner.step)을 일절 수행하지 않으며 단일 슬롯에 대한 발행 변환 및 I/O만 수행
- aiomqtt 2.5.1의 Client.publish(..., qos=1) await 및 PUBACK 수신 경계 명확화
- 결측 슬롯(is_publish_candidate == False)의 무부하 OMITTED 반환
- UUID5 기반 결정론적 message_id 생성 및 재시도 안정성 확보
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import json
import re
from typing import Any, Protocol, runtime_checkable
import uuid

from .schedule_runtime import ScheduledTick


# 결정적 message_id 생성을 위한 고정 UUID 네임스페이스 (RFC 4122 v5)
SCHEDULE_PUBLISHER_NAMESPACE = uuid.UUID("a9e29f27-6f11-4eb7-9c60-84dfa183561a")

# MQTT 단일 토픽 세그먼트용 가구 ID 정규식 (영숫자, _, -, 1~50자)
_HOUSEHOLD_TOPIC_REGEX = re.compile(r"^[A-Za-z0-9_-]{1,50}$")

# 실행 식별자 run_id 정규식 (영숫자, _, -, 1~64자)
_RUN_ID_REGEX = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


class SchedulePublishError(RuntimeError):
    """
    MQTT publish 호출 또는 PUBACK 대기 중 발생한 네트워크/브로커 I/O 실행 오류.

    입력 검증(TypeError, ValueError)이나 직렬화 오류는 이 예외로 감싸지 않고
    원래의 예외를 그대로 전파한다.
    """
    pass


class PublishDisposition(str, Enum):
    """
    단일 가상 슬롯의 MQTT 발행 결과 처분 상태.

    - PUBLISHED: aiomqtt 2.5.1에서 qos=1, retain=False로 client.publish(...)를 호출하여
      브로커로부터 PUBACK 콜백 수신이 정상 완료되었음을 의미한다.
      (주의: MQTT-Kafka Bridge 처리, Kafka 저장, AI 서비스 소비, DB 반영, 이벤트 생성을
       보장하지 않는다. QoS 1은 네트워크 특성상 중복 전달이 가능하며, UUID5는 중복 식별 키일 뿐
       시스템 전체의 분산 멱등성을 자동 보장하지 않는다.)
    - OMITTED: 시나리오 일정상 의도적으로 생략된 결측 슬롯.
      (MQTT I/O 0회, UUID5 미생성, JSON 직렬화 미수행, client 미사용)
    """
    PUBLISHED = "PUBLISHED"
    OMITTED = "OMITTED"


@dataclass(frozen=True)
class ScheduledPublishResult:
    """단일 가상 슬롯의 발행 실행 결과 (불변)"""
    household_id: str
    cycle: int
    disposition: PublishDisposition
    topic: str | None
    message_id: str | None


@runtime_checkable
class MqttPublishClient(Protocol):
    """aiomqtt.Client와 호환되는 비동기 MQTT 발행 프로토콜"""
    async def publish(
        self,
        topic: str,
        payload: str,
        qos: int = 1,
        retain: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        ...


def _validate_run_id(run_id: str) -> str:
    """run_id 타입, 길이(1~64), 공백, 제어문자 및 허용 문자셋 내부 검증"""
    if not isinstance(run_id, str) or isinstance(run_id, bool):
        raise TypeError(f"run_id는 str이어야 합니다: {type(run_id).__name__}")
    if len(run_id) == 0:
        raise ValueError("run_id는 비어 있을 수 없습니다.")
    if run_id != run_id.strip():
        raise ValueError(f"run_id는 앞뒤 공백을 포함할 수 없습니다: {run_id!r}")
    if not _RUN_ID_REGEX.fullmatch(run_id):
        raise ValueError(f"run_id는 1~64자의 영숫자, '_', '-'만 허용됩니다: {run_id!r}")
    return run_id


def _validate_topic_household_id(household_id: str) -> str:
    """household_id가 MQTT 단일 세그먼트로 안전한지 내부 검증"""
    if not isinstance(household_id, str) or isinstance(household_id, bool):
        raise TypeError(f"household_id는 str이어야 합니다: {type(household_id).__name__}")
    if len(household_id) == 0:
        raise ValueError("household_id는 비어 있을 수 없습니다.")
    if household_id != household_id.strip():
        raise ValueError(f"household_id는 앞뒤 공백을 포함할 수 없습니다: {household_id!r}")
    if not _HOUSEHOLD_TOPIC_REGEX.fullmatch(household_id):
        raise ValueError(
            f"household_id는 1~50자의 영숫자, '_', '-'만 허용되며 토픽 구분자(/)나 "
            f"와일드카드(+, #)를 포함할 수 없습니다: {household_id!r}"
        )
    return household_id


def _validate_measured_at(measured_at: datetime) -> datetime:
    """measured_at 타입 및 timezone-aware 여부 내부 검증"""
    if not isinstance(measured_at, datetime):
        raise TypeError(f"measured_at은 datetime이어야 합니다: {type(measured_at).__name__}")
    if measured_at.tzinfo is None or measured_at.utcoffset() is None:
        raise ValueError(f"measured_at은 timezone-aware datetime이어야 합니다: {measured_at!r}")
    return measured_at


def _validate_tick(tick: ScheduledTick) -> ScheduledTick:
    """ScheduledTick 인스턴스 타입, household_id 및 measured_at 내부 검증"""
    if not isinstance(tick, ScheduledTick):
        raise TypeError(f"tick은 ScheduledTick 인스턴스여야 합니다: {type(tick).__name__}")
    _validate_topic_household_id(tick.household_id)
    _validate_measured_at(tick.measured_at)
    return tick


def _validate_publish_candidate(tick: ScheduledTick) -> None:
    """발행 후보(is_publish_candidate == True) 여부 검증"""
    if not tick.is_publish_candidate:
        raise ValueError(
            f"결측 슬롯(is_publish_candidate=False)에 대해서는 MQTT 페이로드 또는 "
            f"message_id를 생성할 수 없습니다: household_id='{tick.household_id}', "
            f"cycle={tick.cycle}"
        )


def build_scheduled_topic(household_id: str) -> str:
    """
    스마트홈 전력 계측 원천 토픽 생성.

    형식: v1/power/sim/{household_id}/main
    직접 호출 시에도 household_id의 MQTT 토픽 안전성을 검증한다.
    """
    validated_id = _validate_topic_household_id(household_id)
    return f"v1/power/sim/{validated_id}/main"


def generate_deterministic_message_id(tick: ScheduledTick, run_id: str) -> str:
    """
    동일 E2E 실행 세션(run_id) 및 틱 입력에 대해 결정론적 UUIDv5 message_id 생성.

    UUID5 Name 형식:
        "SCHEDULE_PUBLISHER_V1::{run_id}::{tick.household_id}::{tick.cycle}::{tick.measured_at.isoformat()}"
    고정 네임스페이스:
        SCHEDULE_PUBLISHER_NAMESPACE (a9e29f27-6f11-4eb7-9c60-84dfa183561a)

    직접 호출 시에도 ScheduledTick 타입, run_id, household_id, timezone-aware
    measured_at, is_publish_candidate=True 계약을 독립 검증한다.
    결측 슬롯(is_publish_candidate=False)인 경우 ValueError로 거절한다.
    """
    _validate_tick(tick)
    _validate_run_id(run_id)
    _validate_publish_candidate(tick)
    name = (
        "SCHEDULE_PUBLISHER_V1::"
        f"{run_id}::{tick.household_id}::{tick.cycle}::"
        f"{tick.measured_at.isoformat()}"
    )
    return str(uuid.uuid5(SCHEDULE_PUBLISHER_NAMESPACE, name))


def build_scheduled_power_payload(tick: ScheduledTick, run_id: str) -> dict[str, object]:
    """
    ScheduledTick으로부터 정확히 14개 필드로 구성된 MQTT 원천 전력 페이로드 딕셔너리 생성.

    직접 호출 시에도 ScheduledTick 타입, run_id, household_id, timezone-aware
    measured_at, is_publish_candidate=True 계약을 독립 검증한다.
    결측 슬롯(is_publish_candidate=False)인 경우 ValueError로 거절한다.
    """
    _validate_tick(tick)
    _validate_run_id(run_id)
    _validate_publish_candidate(tick)

    message_id = generate_deterministic_message_id(tick, run_id)
    measured_at_str = tick.measured_at.isoformat()

    return {
        "message_id": message_id,
        "household_id": tick.household_id,
        "device_id": "main",
        "measured_at": measured_at_str,
        "active_power": tick.active_power,
        "reactive_power": tick.reactive_power,
        "power_factor": tick.power_factor,
        "current": tick.current,
        "house": tick.household_id,
        "device": "main",
        "ts": measured_at_str,
        "power_w": tick.active_power,
        "voltage": tick.voltage,
        "apparent_power": tick.apparent_power,
    }


def serialize_scheduled_power_payload(payload: Mapping[str, object] | dict[str, object]) -> str:
    """
    페이로드 딕셔너리를 결정론적이고 압축된 JSON 문자열로 직렬화.

    - ensure_ascii=False: UTF-8 인코딩 보장
    - sort_keys=True: 딕셔너리 키 순서 독립적 바이트 일치성 보장
    - separators=(',', ':'): 공백 제거 압축 직렬화
    - allow_nan=False: NaN, +Infinity, -Infinity 값 주입 시 브로커 전송 전 즉시 ValueError 차단
    직접 호출 시에도 payload 타입(Mapping)과 NaN/Infinity 거절을 독립 검증한다.
    """
    if not isinstance(payload, Mapping):
        raise TypeError(f"payload는 Mapping(또는 dict)이어야 합니다: {type(payload).__name__}")
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


async def publish_scheduled_tick(
    client: MqttPublishClient,
    tick: ScheduledTick,
    run_id: str,
) -> ScheduledPublishResult:
    """
    단일 가상 슬롯(ScheduledTick)을 MQTT 브로커에 비동기 발행하는 어댑터 함수.

    엄격한 7단계 실행 순서:
    1. tick 타입 검증 (ScheduledTick 여부, household_id 안전성, measured_at timezone)
    2. run_id 유효성 검증 (1~64자 영숫자/언더스코어/하이픈)
    3. is_publish_candidate 판정:
       - False인 경우: 즉시 OMITTED 결과 반환 (I/O 0회, UUID/JSON 미수행, client 미사용)
    4. client 유효성 검증 (callable publish 메서드 소유 확인)
    5. 토픽 및 페이로드 생성 (UUID5 message_id 발급, 14개 필드)
    6. 엄격한 JSON 직렬화 (allow_nan=False)
    7. await client.publish(topic, serialized_payload, qos=1, retain=False)
       - 정상 완료 시 PUBLISHED 결과 반환
       - I/O 예외 발생 시 SchedulePublishError로 래핑하여 발생 (from err)
    """
    # 1~2단계: 결측 여부와 무관하게 필수 입력 형식 먼저 엄격 검증
    _validate_tick(tick)
    _validate_run_id(run_id)

    # 3단계: 결측 슬롯 판정
    if not tick.is_publish_candidate:
        return ScheduledPublishResult(
            household_id=tick.household_id,
            cycle=tick.cycle,
            disposition=PublishDisposition.OMITTED,
            topic=None,
            message_id=None,
        )

    # 4단계: 발행 후보 경로 진입 시 client 검증
    if client is None or not hasattr(client, "publish") or not callable(getattr(client, "publish")):
        raise TypeError("client는 callable publish 메서드를 가진 MqttPublishClient여야 합니다.")

    # 5~6단계: 토픽, 페이로드 및 직렬화
    topic = build_scheduled_topic(tick.household_id)
    payload_dict = build_scheduled_power_payload(tick, run_id)
    message_id = str(payload_dict["message_id"])
    serialized_payload = serialize_scheduled_power_payload(payload_dict)

    # 7단계: 비동기 발행 (QoS 1, retain=False)
    try:
        await client.publish(
            topic,
            serialized_payload,
            qos=1,
            retain=False,
        )
    except Exception as err:
        raise SchedulePublishError(
            f"Failed to publish tick for household '{tick.household_id}' "
            f"at cycle {tick.cycle}: {err}"
        ) from err

    return ScheduledPublishResult(
        household_id=tick.household_id,
        cycle=tick.cycle,
        disposition=PublishDisposition.PUBLISHED,
        topic=topic,
        message_id=message_id,
    )
