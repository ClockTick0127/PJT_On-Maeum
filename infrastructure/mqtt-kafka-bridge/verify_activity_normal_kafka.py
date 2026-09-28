"""
ACTIVITY_NORMAL MQTT -> mqtt-kafka-bridge -> Kafka raw topic 원천 데이터 검증 독립 CLI 도구

외부 AI 서비스, PostgreSQL, 모니터링 서비스에 일절 의존하지 않고,
Kafka power.raw.v1 토픽과 시뮬레이터 HTTP API를 직접 연동하여
86,400개 원천 전력 데이터의 정확한 전달, 결측 0, 14개 필드 무결성,
오프셋 격리 및 브리지 경유 전송을 검증합니다.

메모리 복잡도: O(SECONDS_PER_DAY), 86,400비트맵(약 84KB) 기반 스트리밍 검증.
전체 페이로드 리스트는 메모리에 누적하지 않습니다.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
import json
import math
import os
from pathlib import Path
import re
import signal
import sys
import time
from typing import Any, Optional
import urllib.error
import urllib.request
import uuid

from confluent_kafka import Consumer, KafkaError, TopicPartition

# ==============================================================================
# 프로세스 Exit Codes (표준화)
# ==============================================================================
EXIT_SUCCESS: int = 0
EXIT_INTERNAL_ERROR: int = 1
EXIT_CLI_CONFIG_ERROR: int = 2
EXIT_KAFKA_ERROR: int = 3
EXIT_API_ERROR: int = 4
EXIT_VERIFICATION_FAILURE: int = 5
EXIT_TIMEOUT: int = 6
EXIT_KEYBOARD_INTERRUPT: int = 130

# ==============================================================================
# 도메인 상수 계약
# ==============================================================================
TARGET_HOUSEHOLD: str = "H001"
TARGET_DEVICE: str = "main"
TARGET_SCENARIO: str = "ACTIVITY_NORMAL"
TARGET_REFERENCE_DATE: str = "2026-09-16"
TARGET_KAFKA_TOPIC: str = "power.raw.v1"
EXPECTED_PARTITION_COUNT: int = 24
SECONDS_PER_DAY: int = 86400
KST: timezone = timezone(timedelta(hours=9))
BASE_DATE: date = date(2026, 9, 16)
SCHEDULE_PUBLISHER_NAMESPACE: uuid.UUID = uuid.UUID("a9e29f27-6f11-4eb7-9c60-84dfa183561a")

EXACT_FIELDS: frozenset[str] = frozenset([
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
])

FORBIDDEN_AI_FIELDS: frozenset[str] = frozenset([
    "scenario_id",
    "active_appliances",
    "appliance_type",
    "usage_count",
    "activity_index",
    "score",
    "data_status",
    "expected_event_type",
])

NUMERIC_FIELDS: frozenset[str] = frozenset([
    "active_power",
    "reactive_power",
    "power_factor",
    "current",
    "voltage",
    "apparent_power",
    "power_w",
])

CANONICAL_MEASURED_AT_PATTERN: re.Pattern = re.compile(
    r"^2026-09-16T\d{2}:\d{2}:\d{2}\+09:00$"
)


# ==============================================================================
# 비밀번호 중앙 마스킹 및 보안 유틸리티
# ==============================================================================
def redact_secrets(text: Any, secret: str | None = None) -> str:
    """
    텍스트 내에 포함된 민감정보(MQTT_PASS 및 인자로 전달된 secret)를 '***'로 치환합니다.
    계약:
    - text가 None이면 "" 반환
    - secret 또는 MQTT_PASS가 None이거나 정확한 빈 문자열("")이면 제외
    - 공백 전용 비밀번호도 마스킹 대상에 포함
    - 여러 비밀번호 후보는 길이 내림차순(longest-first)으로 정렬하여 치환 (포함 관계 안전 처리)
    """
    if text is None:
        return ""
    val_str = str(text)

    secrets_to_mask: set[str] = set()
    env_pass = os.getenv("MQTT_PASS")
    if env_pass is not None and env_pass != "":
        secrets_to_mask.add(str(env_pass))
    if secret is not None:
        if isinstance(secret, (list, tuple, set)):
            for s in secret:
                if s is not None and s != "":
                    secrets_to_mask.add(str(s))
        elif secret != "":
            secrets_to_mask.add(str(secret))

    sorted_secrets = sorted(secrets_to_mask, key=len, reverse=True)
    for s in sorted_secrets:
        if s != "":
            val_str = val_str.replace(s, "***")
    return val_str


def _safe_exc_msg(exc: BaseException, secret: str | None = None) -> str:
    """예외의 타입명과 마스킹된 메시지만 안전하게 추출"""
    exc_type = type(exc).__name__
    raw_msg = str(exc)
    if not raw_msg:
        return exc_type
    return f"{exc_type}: {redact_secrets(raw_msg, secret)}"


# ==============================================================================
# 예외 클래스 정의
# ==============================================================================
class VerificationToolError(Exception):
    """검증 도구 기본 예외"""
    exit_code: int = EXIT_INTERNAL_ERROR


class CliConfigError(VerificationToolError):
    exit_code = EXIT_CLI_CONFIG_ERROR


class KafkaTopicError(VerificationToolError):
    exit_code = EXIT_KAFKA_ERROR


class KafkaConsumerError(VerificationToolError):
    exit_code = EXIT_KAFKA_ERROR


class SimulatorApiError(VerificationToolError):
    exit_code = EXIT_API_ERROR


class SimulatorTerminalFailureError(VerificationToolError):
    exit_code = EXIT_VERIFICATION_FAILURE


class TimeoutVerificationError(VerificationToolError):
    exit_code = EXIT_TIMEOUT


# ==============================================================================
# Canonical measured_at 엄격 검증 함수
# ==============================================================================
def validate_canonical_measured_at(val: Any) -> tuple[bool, str | None, int | None]:
    """
    canonical measured_at 형식 및 의미 검증 계약:
    - 정확한 형식: ^2026-09-16T\\d{2}:\\d{2}:\\d{2}\\+09:00$
    - 길이 25자
    - datetime.fromisoformat() 파싱 성공
    - 실제 날짜가 2026-09-16
    - UTC offset이 정확히 +09:00
    - microsecond == 0
    - 파싱 결과의 isoformat()이 원본 문자열과 정확히 동일
    - 시간, 분, 초가 실제 유효 범위에 포함 (0<=H<24, 0<=M<60, 0<=S<60)
    반환: (is_valid, reason, second_of_day)
    """
    if not isinstance(val, str):
        return False, f"measured_at is not str: {type(val).__name__}", None

    if len(val) != 25:
        return False, f"measured_at length != 25: {len(val)} ({val})", None

    if not CANONICAL_MEASURED_AT_PATTERN.match(val):
        return False, f"measured_at does not match canonical pattern: {val}", None

    try:
        dt = datetime.fromisoformat(val)
    except Exception as err:
        return False, f"datetime.fromisoformat failed: {err}", None

    if dt.date() != BASE_DATE:
        return False, f"measured_at date != {BASE_DATE}: {dt.date()}", None

    if dt.utcoffset() != timedelta(hours=9):
        return False, f"measured_at utcoffset != +09:00: {dt.utcoffset()}", None

    if dt.microsecond != 0:
        return False, f"measured_at microsecond != 0: {dt.microsecond}", None

    if dt.isoformat() != val:
        return False, f"dt.isoformat() != original: {dt.isoformat()} != {val}", None

    if not (0 <= dt.hour < 24 and 0 <= dt.minute < 60 and 0 <= dt.second < 60):
        return False, f"hour/min/sec out of bounds: {dt.hour}:{dt.minute}:{dt.second}", None

    second_of_day = dt.hour * 3600 + dt.minute * 60 + dt.second
    if not (0 <= second_of_day < SECONDS_PER_DAY):
        return False, f"second_of_day out of bounds: {second_of_day}", None

    return True, None, second_of_day


# ==============================================================================
# Kafka 레코드 스트리밍 검증기
# ==============================================================================
class KafkaRecordVerifier:
    """
    5종 카운터 기반 Kafka 레코드 스트리밍 검증기:
    1. target_unique_records: 현재 런의 고유 유효 H001 레코드 (목표 86,400)
    2. duplicate_target_records: 현재 런의 H001 중복 레코드 (목표 0)
    3. invalid_target_records: TARGET_CANDIDATE로 식별되었으나 엄격 계약 위반 (목표 0)
    4. foreign_records: 타 런, 타 가구, smoke 등 식별되지 않은 정상 JSON 레코드
    5. malformed_unattributed_records: 비정상 JSON, tombstone, measured_at 누락/비canonical
    """

    def __init__(self, run_id: str, topic: str, start_offsets: dict[int, int]):
        self.run_id = run_id
        self.topic = topic
        self.start_offsets = dict(start_offsets)
        self.timeline_bitmap = bytearray(SECONDS_PER_DAY)
        self.target_unique_records: int = 0
        self.duplicate_target_records: int = 0
        self.invalid_target_records: int = 0
        self.foreign_records: int = 0
        self.malformed_unattributed_records: int = 0
        self.warning_messages: list[str] = []
        self.first_measured_at: str | None = None
        self.last_measured_at: str | None = None
        self.end_offsets: dict[int, int] = dict(start_offsets)

    def _add_warning(self, msg: str) -> None:
        if len(self.warning_messages) < 100:
            self.warning_messages.append(msg)

    def process_record(self, record: Any) -> str:
        """
        단일 Kafka 레코드를 5종 분류 체계에 따라 처리합니다:
        반환값: "TARGET", "DUPLICATE", "INVALID_TARGET", "FOREIGN", "MALFORMED_UNATTRIBUTED"
        """
        partition = record.partition()
        offset = record.offset()

        # 시작 전 오프셋보다 이전 레코드는 외래 레코드로 취급
        if offset < self.start_offsets.get(partition, 0):
            self.foreign_records += 1
            return "FOREIGN"

        # 오프셋 추적 갱신
        self.end_offsets[partition] = max(self.end_offsets.get(partition, 0), offset + 1)

        # 1. Tombstone (value is None)
        raw_val = record.value()
        if raw_val is None:
            self.malformed_unattributed_records += 1
            self._add_warning(f"Tombstone record on partition {partition} offset {offset}; 대상 런 귀속 불가")
            return "MALFORMED_UNATTRIBUTED"

        # 2. JSON 파싱
        try:
            payload = json.loads(raw_val.decode("utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("JSON payload is not an object")
        except Exception as err:
            self.malformed_unattributed_records += 1
            self._add_warning(
                f"Malformed non-JSON/non-dict record on partition {partition} offset {offset} ({type(err).__name__}); 대상 런 귀속 불가"
            )
            return "MALFORMED_UNATTRIBUTED"

        # 3. measured_at 최소 파싱 및 canonical 형식 검증
        raw_measured_at = payload.get("measured_at")
        is_canonical, time_reason, second_of_day = validate_canonical_measured_at(raw_measured_at)
        if not is_canonical or second_of_day is None:
            # measured_at이 비정상이면 cycle 및 UUID5를 역산할 수 없으므로 대상 런 귀속 불가
            self.malformed_unattributed_records += 1
            self._add_warning(
                f"Record with non-canonical/missing measured_at on partition {partition} offset {offset} ({raw_measured_at!r}: {time_reason}); 대상 런 귀속 불가"
            )
            return "MALFORMED_UNATTRIBUTED"

        # 4. 기대 UUID5 계산 및 대조
        cycle = second_of_day + 1
        uuid5_name = f"SCHEDULE_PUBLISHER_V1::{self.run_id}::{TARGET_HOUSEHOLD}::{cycle}::{raw_measured_at}"
        expected_uuid5 = str(uuid.uuid5(SCHEDULE_PUBLISHER_NAMESPACE, uuid5_name))
        raw_message_id = payload.get("message_id")

        if raw_message_id != expected_uuid5:
            self.foreign_records += 1
            return "FOREIGN"

        # 5. 기대 UUID5 일치 -> TARGET_CANDIDATE 확정!
        # 이제부터 엄격한 14개 필드, key, 타입, 값 검증 수행 (실패 시 invalid_target_records)
        raw_key = record.key()
        if raw_key != TARGET_HOUSEHOLD.encode("utf-8"):
            self.invalid_target_records += 1
            self._add_warning(f"TARGET_CANDIDATE Kafka key mismatch: expected {TARGET_HOUSEHOLD.encode('utf-8')!r}, got {raw_key!r}")
            return "INVALID_TARGET"

        # 14개 필드 스키마 일치 여부
        if set(payload.keys()) != EXACT_FIELDS:
            self.invalid_target_records += 1
            missing_fields = EXACT_FIELDS - set(payload.keys())
            extra_fields = set(payload.keys()) - EXACT_FIELDS
            self._add_warning(f"TARGET_CANDIDATE field mismatch: missing={missing_fields}, extra={extra_fields}")
            return "INVALID_TARGET"

        # household_id / house 일치
        if payload.get("household_id") != TARGET_HOUSEHOLD or payload.get("house") != TARGET_HOUSEHOLD:
            self.invalid_target_records += 1
            return "INVALID_TARGET"

        # device_id / device 일치
        if payload.get("device_id") != TARGET_DEVICE or payload.get("device") != TARGET_DEVICE:
            self.invalid_target_records += 1
            return "INVALID_TARGET"

        # ts == measured_at
        if payload.get("ts") != raw_measured_at:
            self.invalid_target_records += 1
            return "INVALID_TARGET"

        # power_w == active_power
        if payload.get("power_w") != payload.get("active_power"):
            self.invalid_target_records += 1
            return "INVALID_TARGET"

        # 수치 필드 타입 및 범위 검증
        for num_field in NUMERIC_FIELDS:
            num_val = payload.get(num_field)
            if isinstance(num_val, bool) or not isinstance(num_val, (int, float)) or not math.isfinite(num_val):
                self.invalid_target_records += 1
                self._add_warning(f"TARGET_CANDIDATE numeric field {num_field} invalid: {num_val!r}")
                return "INVALID_TARGET"

        # 금지 AI 필드 부재
        for forbidden in FORBIDDEN_AI_FIELDS:
            if forbidden in payload:
                self.invalid_target_records += 1
                self._add_warning(f"TARGET_CANDIDATE contains forbidden AI field {forbidden}")
                return "INVALID_TARGET"

        # 6. 모든 엄격한 검증 통과 -> 비트맵 중복 여부 확인
        if self.timeline_bitmap[second_of_day] == 1:
            self.duplicate_target_records += 1
            return "DUPLICATE"

        self.timeline_bitmap[second_of_day] = 1
        self.target_unique_records += 1

        if second_of_day == 0 or (self.first_measured_at is None and self.target_unique_records == 1):
            self.first_measured_at = raw_measured_at
        if second_of_day == SECONDS_PER_DAY - 1 or self.target_unique_records == SECONDS_PER_DAY:
            self.last_measured_at = raw_measured_at

        return "TARGET"


# ==============================================================================
# 시뮬레이터 HTTP API 유틸리티
# ==============================================================================
def http_post_start_run(api_url: str, timeout_sec: float) -> str:
    """시뮬레이터에 E2E 실행을 시작하고 run_id를 반환합니다."""
    url = f"{api_url.rstrip('/')}/api/e2e/runs"
    body = {
        "reference_date": TARGET_REFERENCE_DATE,
        "execution": {"mode": "BURST"},
        "households": [
            {
                "household_id": TARGET_HOUSEHOLD,
                "scenario": TARGET_SCENARIO,
            }
        ],
    }
    payload_bytes = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=payload_bytes,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            if resp.status not in (200, 202):
                raise SimulatorApiError(f"E2E 실행 시작 실패 (HTTP {resp.status})")
            resp_data = json.loads(resp.read().decode("utf-8"))
            run_id = resp_data.get("run_id")
            if not run_id:
                raise SimulatorApiError(f"시작 응답에 run_id가 없습니다: {resp_data}")
            return run_id
    except urllib.error.HTTPError as err:
        err_msg = err.read().decode("utf-8", errors="replace")
        raise SimulatorApiError(f"시뮬레이터 HTTP 오류 ({err.code}): {err_msg}")
    except urllib.error.URLError as err:
        raise SimulatorApiError(f"시뮬레이터 연결 실패: {err.reason}")


def http_get_run_snapshot(api_url: str, run_id: str, timeout_sec: float) -> dict[str, Any]:
    """시뮬레이터 실행 세션 상태 스냅샷을 조회합니다."""
    url = f"{api_url.rstrip('/')}/api/e2e/runs/{run_id}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"}, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            if resp.status != 200:
                raise SimulatorApiError(f"스냅샷 조회 실패 (HTTP {resp.status})")
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as err:
        err_msg = err.read().decode("utf-8", errors="replace")
        raise SimulatorApiError(f"스냅샷 조회 HTTP 오류 ({err.code}): {err_msg}")
    except urllib.error.URLError as err:
        raise SimulatorApiError(f"스냅샷 조회 연결 실패: {err.reason}")


def extract_household_snapshot(
    api_snap: Any,
    target_household: str = TARGET_HOUSEHOLD,
) -> tuple[dict[str, Any] | None, str | None]:
    """
    E2E API 스냅샷에서 대상 가구의 메트릭 딕셔너리를 안전하게 추출합니다.
    계약:
    - api_snap이 dict가 아니면 (None, "API snapshot is not an object")
    - api_snap["households"]가 list가 아니면 (None, f"households field is not a list: {type(households).__name__}")
    - list 내부 항목 중 household_id == target_household 인 dict 항목 검색
    - 해당 항목이 없으면 (None, f"household {target_household} not found in households list")
    - 해당 항목이 dict가 아니면 (None, f"household entry for {target_household} is not an object")
    - 성공 시 (h_dict, None) 반환
    """
    if not isinstance(api_snap, dict):
        return None, "API snapshot is not an object"
    households = api_snap.get("households")
    if not isinstance(households, list):
        return None, f"households field is not a list: {type(households).__name__}"

    for item in households:
        if not isinstance(item, dict):
            return None, f"household entry is not an object: {type(item).__name__}"
        if item.get("household_id") == target_household:
            return item, None

    return None, f"household {target_household} not found in households list"



def http_post_stop_run_best_effort(api_url: str, run_id: str, timeout_sec: float = 3.0) -> None:
    """실패 시 시뮬레이터 실행을 정지하는 best-effort 함수"""
    url = f"{api_url.rstrip('/')}/api/e2e/runs/{run_id}/stop"
    req = urllib.request.Request(
        url,
        data=b"{}",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            _ = resp.read()
    except Exception:
        pass


# ==============================================================================
# 검증 결과 데이터클래스
# ==============================================================================
@dataclass
class VerificationSummary:
    status: str
    exit_code: int
    scenario: str
    run_id: str | None
    household_id: str
    reference_date: str
    kafka_topic: str
    target_unique_records: int
    duplicate_target_records: int
    invalid_target_records: int
    foreign_records: int
    malformed_unattributed_records: int
    missing_virtual_seconds: int
    first_measured_at: str | None
    last_measured_at: str | None
    simulator_status: str | None
    start_offsets: dict[str, int]
    end_offsets: dict[str, int]
    warning_messages: list[str]
    elapsed_seconds: float
    error_message: str | None

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, ensure_ascii=False)


# ==============================================================================
# 검증 실행 핵심 엔진
# ==============================================================================
def run_kafka_verification(
    kafka_bootstrap: str,
    kafka_topic: str,
    simulator_url: str,
    drain_timeout: float = 30.0,
    overall_timeout: float = 180.0,
    api_poll_interval: float = 0.5,
    poll_timeout: float = 0.2,
    secret_to_mask: str | None = None,
) -> tuple[VerificationSummary, int]:
    """
    Kafka raw topic 원천 데이터 검증 메인 로직
    """
    start_mono = time.monotonic()
    run_id: str | None = None
    consumer: Consumer | None = None
    consumer_closed: bool = False
    start_offsets: dict[int, int] = {}
    end_offsets: dict[int, int] = {}
    verifier: KafkaRecordVerifier | None = None
    latest_api_snap: dict[str, Any] | None = None

    def _close_consumer() -> None:
        nonlocal consumer_closed
        if consumer is not None and not consumer_closed:
            try:
                consumer.close()
            except Exception:
                pass
            consumer_closed = True

    try:
        # 1. Kafka Consumer 초기화 (Offset 격리 계약 준수)
        consumer_conf = {
            "bootstrap.servers": kafka_bootstrap,
            "group.id": f"verify-activity-kafka-{uuid.uuid4().hex[:12]}",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "error",
        }
        try:
            consumer = Consumer(consumer_conf)
        except Exception as err:
            raise KafkaConsumerError(f"Kafka Consumer 생성 실패: {err}")

        # 2. 토픽 메타데이터 확인 및 24개 파티션 High Watermark 측정
        try:
            metadata = consumer.list_topics(topic=kafka_topic, timeout=10.0)
        except Exception as err:
            raise KafkaTopicError(f"Kafka 메타데이터 조회 실패: {err}")

        topic_info = metadata.topics.get(kafka_topic)
        if not topic_info or topic_info.error is not None:
            err_code = topic_info.error if topic_info else "TOPIC_NOT_FOUND"
            raise KafkaTopicError(f"Kafka 토픽 {kafka_topic} 접근 불가: {err_code}")

        partition_keys = sorted(topic_info.partitions.keys())
        if len(partition_keys) != EXPECTED_PARTITION_COUNT:
            raise KafkaTopicError(
                f"토픽 {kafka_topic}의 파티션 수가 기대값({EXPECTED_PARTITION_COUNT})과 일치하지 않습니다: {len(partition_keys)}개 ({partition_keys})"
            )

        assigned_tps: list[TopicPartition] = []
        for p in partition_keys:
            tp = TopicPartition(kafka_topic, p)
            try:
                _low, high = consumer.get_watermark_offsets(tp, timeout=10.0)
            except Exception as err:
                raise KafkaTopicError(f"파티션 {p} watermark 조회 실패: {err}")
            start_offsets[p] = high
            assigned_tps.append(TopicPartition(kafka_topic, p, high))

        # 3. 명시적 assign 바인딩 (subscribe() 금지)
        consumer.assign(assigned_tps)
        verifier = KafkaRecordVerifier(run_id="", topic=kafka_topic, start_offsets=start_offsets)

        # 4. 시뮬레이터 실행 시작 요청 (POST /api/e2e/runs)
        run_id = http_post_start_run(simulator_url, timeout_sec=10.0)
        verifier.run_id = run_id

        # 5. 실시간 소비 및 API 상태 폴링 루프
        api_completed = False
        drain_deadline: float | None = None
        last_api_poll: float = 0.0

        while True:
            now = time.monotonic()
            if now - start_mono > overall_timeout:
                raise TimeoutVerificationError(f"전체 제한시간({overall_timeout}초) 초과")

            if api_completed and drain_deadline is not None and now >= drain_deadline:
                # Post-completion Drain 제한시간 도달
                break

            # 5.1 Kafka Poll
            try:
                record = consumer.poll(timeout=poll_timeout)
            except Exception as err:
                raise KafkaConsumerError(f"Kafka poll 도중 치명적 예외: {err}")

            if record is not None:
                if record.error():
                    err_code = record.error().code()
                    if err_code == KafkaError._PARTITION_EOF:
                        pass  # 정상 파티션 경계 신호 무시
                    else:
                        raise KafkaConsumerError(f"Kafka record 오류: {record.error()}")
                else:
                    verifier.process_record(record)
                    # 86,400개에 도달하더라도 조기 break하지 않고 drain_deadline까지 지속 poll

            # 5.2 시뮬레이터 API 상태 폴링
            if not api_completed and (now - last_api_poll >= api_poll_interval):
                last_api_poll = now
                snap = http_get_run_snapshot(simulator_url, run_id, timeout_sec=5.0)
                latest_api_snap = snap
                overall_status = snap.get("overall_status") if isinstance(snap, dict) else None

                if overall_status in ("FAILED", "PARTIAL_FAILED", "STOPPED"):
                    raise SimulatorTerminalFailureError(f"시뮬레이터 상태 비정상 종료: {overall_status}")
                elif overall_status == "COMPLETED":
                    api_completed = True
                    completion_observed_at = time.monotonic()
                    drain_deadline = completion_observed_at + drain_timeout
                    # API가 COMPLETED되어도 86,400개 도달 여부와 무관하게 drain_deadline까지 poll 지속

        # 6. 최종 24개 파티션 end_offsets 측정
        for p in partition_keys:
            tp = TopicPartition(kafka_topic, p)
            try:
                _low, high = consumer.get_watermark_offsets(tp, timeout=10.0)
                end_offsets[p] = high
            except Exception:
                end_offsets[p] = verifier.end_offsets.get(p, start_offsets.get(p, 0))

        # 7. 최종 완료 판정 계약 검증
        missing_virtual_seconds = SECONDS_PER_DAY - verifier.target_unique_records
        h001_snap, h001_err = extract_household_snapshot(latest_api_snap, TARGET_HOUSEHOLD)

        failure_reasons: list[str] = []
        if not api_completed:
            failure_reasons.append("시뮬레이터 API 미완료")
        if latest_api_snap and latest_api_snap.get("overall_status") != "COMPLETED":
            failure_reasons.append(f"overall_status != COMPLETED ({latest_api_snap.get('overall_status')})")
        if h001_err is not None:
            failure_reasons.append(f"H001 스냅샷 오류: {h001_err}")
        elif h001_snap is not None:
            if h001_snap.get("state") != "COMPLETED":
                failure_reasons.append(f"H001 state != COMPLETED ({h001_snap.get('state')})")
            if h001_snap.get("published_samples") != SECONDS_PER_DAY:
                failure_reasons.append(f"published_samples != {SECONDS_PER_DAY} ({h001_snap.get('published_samples')})")
            if h001_snap.get("planned_publish_samples") != SECONDS_PER_DAY:
                failure_reasons.append(f"planned_publish_samples != {SECONDS_PER_DAY} ({h001_snap.get('planned_publish_samples')})")
            if h001_snap.get("omitted_samples") != 0:
                failure_reasons.append(f"omitted_samples != 0 ({h001_snap.get('omitted_samples')})")
        else:
            failure_reasons.append("H001 스냅샷 없음")

        if verifier.target_unique_records != SECONDS_PER_DAY:
            failure_reasons.append(f"target_unique_records != {SECONDS_PER_DAY} ({verifier.target_unique_records})")
        if verifier.duplicate_target_records != 0:
            failure_reasons.append(f"duplicate_target_records != 0 ({verifier.duplicate_target_records})")
        if verifier.invalid_target_records != 0:
            failure_reasons.append(f"invalid_target_records != 0 ({verifier.invalid_target_records})")
        if missing_virtual_seconds != 0:
            failure_reasons.append(f"missing_virtual_seconds != 0 ({missing_virtual_seconds})")
        if verifier.first_measured_at != "2026-09-16T00:00:00+09:00":
            failure_reasons.append(f"first_measured_at mismatch ({verifier.first_measured_at})")
        if verifier.last_measured_at != "2026-09-16T23:59:59+09:00":
            failure_reasons.append(f"last_measured_at mismatch ({verifier.last_measured_at})")
        if len(start_offsets) != EXPECTED_PARTITION_COUNT:
            failure_reasons.append(f"start_offsets 파티션 수 불일치 ({len(start_offsets)})")
        if len(end_offsets) != EXPECTED_PARTITION_COUNT:
            failure_reasons.append(f"end_offsets 파티션 수 불일치 ({len(end_offsets)})")

        if not failure_reasons:
            summary = VerificationSummary(
                status="SUCCESS",
                exit_code=EXIT_SUCCESS,
                scenario=TARGET_SCENARIO,
                run_id=run_id,
                household_id=TARGET_HOUSEHOLD,
                reference_date=TARGET_REFERENCE_DATE,
                kafka_topic=kafka_topic,
                target_unique_records=verifier.target_unique_records,
                duplicate_target_records=verifier.duplicate_target_records,
                invalid_target_records=verifier.invalid_target_records,
                foreign_records=verifier.foreign_records,
                malformed_unattributed_records=verifier.malformed_unattributed_records,
                missing_virtual_seconds=missing_virtual_seconds,
                first_measured_at=verifier.first_measured_at,
                last_measured_at=verifier.last_measured_at,
                simulator_status="COMPLETED",
                start_offsets={str(k): v for k, v in sorted(start_offsets.items())},
                end_offsets={str(k): v for k, v in sorted(end_offsets.items())},
                warning_messages=verifier.warning_messages,
                elapsed_seconds=round(time.monotonic() - start_mono, 3),
                error_message=None,
            )
            return summary, EXIT_SUCCESS
        else:
            summary = VerificationSummary(
                status="FAILURE",
                exit_code=EXIT_VERIFICATION_FAILURE,
                scenario=TARGET_SCENARIO,
                run_id=run_id,
                household_id=TARGET_HOUSEHOLD,
                reference_date=TARGET_REFERENCE_DATE,
                kafka_topic=kafka_topic,
                target_unique_records=verifier.target_unique_records,
                duplicate_target_records=verifier.duplicate_target_records,
                invalid_target_records=verifier.invalid_target_records,
                foreign_records=verifier.foreign_records,
                malformed_unattributed_records=verifier.malformed_unattributed_records,
                missing_virtual_seconds=missing_virtual_seconds,
                first_measured_at=verifier.first_measured_at,
                last_measured_at=verifier.last_measured_at,
                simulator_status=latest_api_snap.get("overall_status") if isinstance(latest_api_snap, dict) else None,
                start_offsets={str(k): v for k, v in sorted(start_offsets.items())},
                end_offsets={str(k): v for k, v in sorted(end_offsets.items())},
                warning_messages=verifier.warning_messages,
                elapsed_seconds=round(time.monotonic() - start_mono, 3),
                error_message="; ".join(failure_reasons),
            )
            return summary, EXIT_VERIFICATION_FAILURE

    except KeyboardInterrupt:
        _close_consumer()
        if run_id:
            http_post_stop_run_best_effort(simulator_url, run_id)
        sys.stderr.write("Verification interrupted by user (Ctrl+C)\n")
        sys.exit(EXIT_KEYBOARD_INTERRUPT)

    except VerificationToolError as err:
        _close_consumer()
        if run_id:
            http_post_stop_run_best_effort(simulator_url, run_id)
        summary = VerificationSummary(
            status="FAILURE",
            exit_code=err.exit_code,
            scenario=TARGET_SCENARIO,
            run_id=run_id,
            household_id=TARGET_HOUSEHOLD,
            reference_date=TARGET_REFERENCE_DATE,
            kafka_topic=kafka_topic,
            target_unique_records=verifier.target_unique_records if verifier else 0,
            duplicate_target_records=verifier.duplicate_target_records if verifier else 0,
            invalid_target_records=verifier.invalid_target_records if verifier else 0,
            foreign_records=verifier.foreign_records if verifier else 0,
            malformed_unattributed_records=verifier.malformed_unattributed_records if verifier else 0,
            missing_virtual_seconds=SECONDS_PER_DAY - (verifier.target_unique_records if verifier else 0),
            first_measured_at=verifier.first_measured_at if verifier else None,
            last_measured_at=verifier.last_measured_at if verifier else None,
            simulator_status=latest_api_snap.get("overall_status") if latest_api_snap else None,
            start_offsets={str(k): v for k, v in sorted(start_offsets.items())},
            end_offsets={str(k): v for k, v in sorted(end_offsets.items())},
            warning_messages=verifier.warning_messages if verifier else [],
            elapsed_seconds=round(time.monotonic() - start_mono, 3),
            error_message=_safe_exc_msg(err, secret_to_mask),
        )
        return summary, err.exit_code

    except Exception as err:
        _close_consumer()
        if run_id:
            http_post_stop_run_best_effort(simulator_url, run_id)
        summary = VerificationSummary(
            status="FAILURE",
            exit_code=EXIT_INTERNAL_ERROR,
            scenario=TARGET_SCENARIO,
            run_id=run_id,
            household_id=TARGET_HOUSEHOLD,
            reference_date=TARGET_REFERENCE_DATE,
            kafka_topic=kafka_topic,
            target_unique_records=verifier.target_unique_records if verifier else 0,
            duplicate_target_records=verifier.duplicate_target_records if verifier else 0,
            invalid_target_records=verifier.invalid_target_records if verifier else 0,
            foreign_records=verifier.foreign_records if verifier else 0,
            malformed_unattributed_records=verifier.malformed_unattributed_records if verifier else 0,
            missing_virtual_seconds=SECONDS_PER_DAY - (verifier.target_unique_records if verifier else 0),
            first_measured_at=verifier.first_measured_at if verifier else None,
            last_measured_at=verifier.last_measured_at if verifier else None,
            simulator_status=latest_api_snap.get("overall_status") if latest_api_snap else None,
            start_offsets={str(k): v for k, v in sorted(start_offsets.items())},
            end_offsets={str(k): v for k, v in sorted(end_offsets.items())},
            warning_messages=verifier.warning_messages if verifier else [],
            elapsed_seconds=round(time.monotonic() - start_mono, 3),
            error_message=_safe_exc_msg(err, secret_to_mask),
        )
        return summary, EXIT_INTERNAL_ERROR

    finally:
        _close_consumer()


# ==============================================================================
# CLI 진입점
# ==============================================================================
def parse_cli_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ACTIVITY_NORMAL 시뮬레이터 원천 데이터 Kafka raw topic 도달 검증 CLI 도구"
    )
    parser.add_argument(
        "--kafka-bootstrap",
        default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
        help="Kafka bootstrap server (기본값: localhost:9092 또는 KAFKA_BOOTSTRAP)",
    )
    parser.add_argument(
        "--kafka-topic",
        default=os.getenv("KAFKA_TOPIC", TARGET_KAFKA_TOPIC),
        help="Kafka raw topic (기본값: power.raw.v1 또는 KAFKA_TOPIC)",
    )
    parser.add_argument(
        "--simulator-url",
        default=os.getenv("SIMULATOR_API_URL", "http://127.0.0.1:8085"),
        help="시뮬레이터 HTTP API 베이스 URL (기본값: http://127.0.0.1:8085)",
    )
    parser.add_argument(
        "--drain-timeout",
        type=float,
        default=30.0,
        help="시뮬레이터 API 완료 후 Kafka 잔류 메시지 흡수 제한시간(초) (기본값: 30.0)",
    )
    parser.add_argument(
        "--overall-timeout",
        type=float,
        default=180.0,
        help="전체 검증 제한시간(초) (기본값: 180.0)",
    )
    parser.add_argument(
        "--api-poll-interval",
        type=float,
        default=0.5,
        help="시뮬레이터 API 상태 폴링 간격(초) (기본값: 0.5)",
    )
    parser.add_argument(
        "--poll-timeout",
        type=float,
        default=0.2,
        help="Kafka Consumer poll 타임아웃(초) (기본값: 0.2)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_cli_args(argv)
    except SystemExit as exc:
        return exc.code

    if isinstance(args.drain_timeout, bool) or args.drain_timeout <= 0 or math.isnan(args.drain_timeout) or math.isinf(args.drain_timeout):
        sys.stderr.write("CliConfigError: --drain-timeout must be a positive finite number\n")
        return EXIT_CLI_CONFIG_ERROR
    if isinstance(args.overall_timeout, bool) or args.overall_timeout <= 0 or math.isnan(args.overall_timeout) or math.isinf(args.overall_timeout):
        sys.stderr.write("CliConfigError: --overall-timeout must be a positive finite number\n")
        return EXIT_CLI_CONFIG_ERROR
    if isinstance(args.api_poll_interval, bool) or args.api_poll_interval <= 0 or math.isnan(args.api_poll_interval) or math.isinf(args.api_poll_interval):
        sys.stderr.write("CliConfigError: --api-poll-interval must be a positive finite number\n")
        return EXIT_CLI_CONFIG_ERROR
    if isinstance(args.poll_timeout, bool) or args.poll_timeout < 0 or math.isnan(args.poll_timeout) or math.isinf(args.poll_timeout):
        sys.stderr.write("CliConfigError: --poll-timeout must be a non-negative finite number\n")
        return EXIT_CLI_CONFIG_ERROR

    summary, exit_code = run_kafka_verification(
        kafka_bootstrap=args.kafka_bootstrap,
        kafka_topic=args.kafka_topic,
        simulator_url=args.simulator_url,
        drain_timeout=args.drain_timeout,
        overall_timeout=args.overall_timeout,
        api_poll_interval=args.api_poll_interval,
        poll_timeout=args.poll_timeout,
    )
    print(summary.to_json())
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
