"""
ACTIVITY_NORMAL 시뮬레이터 원천 MQTT 발행 독립 검증 CLI 도구

외부 서비스(Kafka, PostgreSQL, AI)에 일절 연결하지 않고,
Mosquitto 브로커와 시뮬레이터 HTTP API만을 활용하여 86,400개 원천 전력 데이터의
QoS 1 결측 없는 발행 및 무결성을 스트리밍 방식으로 검증합니다.

메모리 복잡도: O(SECONDS_PER_DAY), 현재 1일 시나리오 기준 약 86KB
payload 전체 목록은 메모리에 저장하지 않습니다.
"""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
import json
import math
import os
import socket
import ssl
import sys
import time
from typing import Any, Optional
import urllib.error
import urllib.request
import uuid

import aiomqtt

from engine.scenario_catalog import (
    ACTIVITY_SCENARIO_IDS,
    UnknownActivityScenarioError,
    get_activity_scenario_definition,
)
from engine.unified_catalog import (
    UNIFIED_SCENARIO_IDS,
    UnknownScenarioError,
    get_unified_scenario_definition,
    list_unified_scenario_ids,
)
from engine.schedule import (
    CompiledExecutionPlan,
    ScheduleError,
    compile_schedule,
    resolve_base_date,
)
from engine.tls import (
    create_mqtt_tls_context,
    mask_password,
    parse_tls_enabled,
    resolve_mqtt_config,
    resolve_mqtt_port,
)

# ==============================================================================
# 프로세스 Exit Codes (표준화)
# ==============================================================================
EXIT_SUCCESS: int = 0
EXIT_INTERNAL_ERROR: int = 1
EXIT_CLI_CONFIG_ERROR: int = 2
EXIT_MQTT_ERROR: int = 3
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
TARGET_TOPIC: str = "v1/power/sim/H001/main"
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

TERMINAL_AND_STOPPING_STATUSES: frozenset[str] = frozenset([
    "COMPLETED",
    "FAILED",
    "PARTIAL_FAILED",
    "STOPPED",
    "STOPPING",
])


# ==============================================================================
# 비밀번호 중앙 마스킹 및 보안 유틸리티
# ==============================================================================
def redact_secrets(text: Any, secret: str | None = None) -> str:
    """
    텍스트 내에 포함된 민감정보(MQTT_PASS 및 인자로 전달된 secret)를 '***'로 치환합니다.
    계약:
    - text가 None이면 "" 반환
    - secret 또는 MQTT_PASS가 None이거나 정확한 빈 문자열("")이면 제외
    - " ", "   " 등 공백 전용 비밀번호는 반드시 마스킹 대상에 포함 (strip() 제외 금지)
    - 여러 비밀번호 후보는 길이 내림차순(longest-first)으로 정렬하여 치환 (포함 관계 안전 처리)
    - 환경변수 MQTT_PASS와 broker_config password 모두 적용
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

    # 여러 비밀번호 후보는 길이 내림차순(longest-first)으로 치환
    sorted_secrets = sorted(secrets_to_mask, key=len, reverse=True)
    for s in sorted_secrets:
        if s != "":
            val_str = val_str.replace(s, "***")
    return val_str


def _safe_exc_msg(exc: BaseException, secret: str | None = None) -> str:
    """예외의 타입명과 마스킹된 메시지만 안전하게 추출 (repr 전체 노출 방지)"""
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


class MqttConnectError(VerificationToolError):
    exit_code = EXIT_MQTT_ERROR


class MqttSubscribeError(VerificationToolError):
    exit_code = EXIT_MQTT_ERROR


class SimulatorApiError(VerificationToolError):
    exit_code = EXIT_API_ERROR


class QueueOverflowError(VerificationToolError):
    exit_code = EXIT_VERIFICATION_FAILURE


class PayloadContractError(VerificationToolError):
    exit_code = EXIT_VERIFICATION_FAILURE


class SimulatorTerminalFailureError(VerificationToolError):
    exit_code = EXIT_VERIFICATION_FAILURE


class TimeoutVerificationError(VerificationToolError):
    exit_code = EXIT_TIMEOUT


# ==============================================================================
# 유효성 검사 및 타임아웃 감지 헬퍼
# ==============================================================================
def validate_port(port: Any) -> int | None:
    """MQTT 브로커 포트 유효성 검증 (1~65535 허용)"""
    if port is None:
        return None
    try:
        if isinstance(port, bool):
            raise ValueError()
        port_int = int(port)
        if str(port_int) != str(port).strip():
            raise ValueError()
    except (ValueError, TypeError):
        raise CliConfigError(f"유효하지 않은 포트 번호입니다 (1~65535 허용): {port}")

    if not (1 <= port_int <= 65535):
        raise CliConfigError(f"포트 번호 범위를 벗어났습니다 (1~65535 허용): {port}")
    return port_int


def validate_positive_timeout(val: Any, name: str) -> float:
    """타임아웃 및 폴링 주기 유효성 검증 (0보다 큰 유한한 숫자 허용)"""
    if isinstance(val, bool) or type(val) not in (int, float):
        raise CliConfigError(f"유효하지 않은 {name} 값입니다 (0보다 큰 유한한 숫자만 허용): {val}")
    if not math.isfinite(val) or val <= 0:
        raise CliConfigError(f"유효하지 않은 {name} 값입니다 (0보다 큰 유한한 숫자만 허용): {val}")
    return float(val)


def _is_timeout_exception(exc: BaseException) -> bool:
    """소켓 또는 비동기 타임아웃 예외 여부 감지"""
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError, socket.timeout)):
        return True
    if isinstance(exc, urllib.error.URLError) and isinstance(exc.reason, (socket.timeout, TimeoutError)):
        return True
    reason_str = str(getattr(exc, "reason", "")).lower()
    msg_str = str(exc).lower()
    if "timed out" in reason_str or "time out" in reason_str or "timed out" in msg_str or "time out" in msg_str:
        return True
    return False


# ==============================================================================
# 시나리오 계획 데이터 모델 및 동적 산출 유틸리티
# ==============================================================================
@dataclass(frozen=True)
class ExpectedSchedulePlan:
    """
    단일 또는 다일 시나리오의 컴파일된 실행 계획 및 기대 수치 (불변)
    - 리터럴 하드코딩 배제: total_planned_publish_samples, total_planned_omitted_samples, should_publish 활용
    - schedule_bitmap: O(total_virtual_slots) 바이트맵 (1: 발행 대상, 0: 결측 대상)
    """
    scenario_id: str
    reference_date: date
    reference_date_str: str
    compiled_plan: CompiledExecutionPlan
    expected_publish_samples: int
    expected_omitted_samples: int
    first_publish_second: int | None
    last_publish_second: int | None
    first_expected_measured_at: str | None
    last_expected_measured_at: str | None
    schedule_bitmap: bytes

    @property
    def total_days(self) -> int:
        return len(self.compiled_plan.day_plans)

    @property
    def total_virtual_slots(self) -> int:
        return self.compiled_plan.total_virtual_slots

    @property
    def start_date(self) -> date:
        return self.compiled_plan.start_date

    @property
    def end_date(self) -> date:
        return self.compiled_plan.end_date


def build_expected_schedule_plan(
    scenario_id: str = TARGET_SCENARIO,
    reference_date_str: str = TARGET_REFERENCE_DATE,
) -> ExpectedSchedulePlan:
    """
    카탈로그 및 컴파일러를 통해 대상 시나리오의 기대 발행/결측 계획을 동적으로 산출합니다.
    - 리터럴 하드코딩 배제: total_planned_publish_samples, total_planned_omitted_samples 활용
    - 유효하지 않은 시나리오, 컴파일 오류, 날짜 파싱 오류는 모두 CliConfigError(exit 2)로 감쌉니다.
    """
    try:
        ref_date = datetime.strptime(reference_date_str, "%Y-%m-%d").date()
    except (ValueError, TypeError) as err:
        raise CliConfigError(f"유효하지 않은 reference_date 형식입니다 (YYYY-MM-DD 권장): {reference_date_str}") from err

    try:
        defn = get_unified_scenario_definition(scenario_id)
        base_date = resolve_base_date(ref_date, len(defn.days))
        compiled = compile_schedule(defn, base_date=base_date)
    except (UnknownScenarioError, UnknownActivityScenarioError, ScheduleError, ValueError) as err:
        raise CliConfigError(f"시나리오 계획 생성 실패 ({scenario_id}): {err}") from err

    # [2] 실질적인 가드만 유지 (항진명제 제거)
    if not compiled.day_plans:
        raise CliConfigError(f"시나리오 계획에 실행 일자가 없습니다: {scenario_id}")

    first_cycle = compiled.first_cycle
    total_slots = compiled.total_virtual_slots
    bitmap_arr = bytearray(total_slots)

    # 비트맵 슬라이싱 일괄 초기화 (< 1ms 고속 처리)
    for d, dp in enumerate(compiled.day_plans):
        offset = d * SECONDS_PER_DAY
        if dp.planned_omitted_samples == 0:
            bitmap_arr[offset : offset + SECONDS_PER_DAY] = b"\x01" * SECONDS_PER_DAY
        else:
            bitmap_arr[offset : offset + SECONDS_PER_DAY] = b"\x01" * SECONDS_PER_DAY
            for r in dp.omission_ranges:
                bitmap_arr[offset + r.start_second : offset + r.end_second] = b"\x00" * (r.end_second - r.start_second)

    first_sec: int | None = None
    last_sec: int | None = None

    if compiled.total_planned_publish_samples > 0:
        idx1 = bitmap_arr.find(b"\x01")
        if idx1 != -1:
            first_sec = idx1
        idx2 = bitmap_arr.rfind(b"\x01")
        if idx2 != -1:
            last_sec = idx2

    # [1] 전체 타임라인 기준 인덱스로 virtual_time_at 계산
    first_ts = (
        compiled.virtual_time_at(first_cycle + first_sec).isoformat()
        if first_sec is not None
        else None
    )
    last_ts = (
        compiled.virtual_time_at(first_cycle + last_sec).isoformat()
        if last_sec is not None
        else None
    )

    return ExpectedSchedulePlan(
        scenario_id=scenario_id,
        reference_date=ref_date,
        reference_date_str=reference_date_str,
        compiled_plan=compiled,
        expected_publish_samples=compiled.total_planned_publish_samples,
        expected_omitted_samples=compiled.total_planned_omitted_samples,
        first_publish_second=first_sec,
        last_publish_second=last_sec,
        first_expected_measured_at=first_ts,
        last_expected_measured_at=last_ts,
        schedule_bitmap=bytes(bitmap_arr),
    )


# ==============================================================================
# 페이로드 및 스트림 검증기 (O(SECONDS_PER_DAY) Bounded Memory)
# ==============================================================================
class StreamMessageValidator:
    """
    86,400개 페이로드를 메모리에 누적하지 않고, bytearray 비트맵(약 86KB)과
    실시간 계산된 UUID5를 대조하여 중복·외래·누락·위반을 선형 분리하는 검증기.
    """

    def __init__(self, run_id: str, plan: ExpectedSchedulePlan | None = None) -> None:
        self.run_id: str = run_id
        self.plan: ExpectedSchedulePlan = (
            plan
            if plan is not None
            else build_expected_schedule_plan(TARGET_SCENARIO, TARGET_REFERENCE_DATE)
        )
        self.timeline_bitmap: bytearray = bytearray(self.plan.total_virtual_slots)
        self.target_unique_messages: int = 0
        self.duplicate_deliveries: int = 0
        self.foreign_run_messages: int = 0
        self.invalid_payloads: int = 0
        self.unexpected_in_omission: int = 0
        self.invalid_reasons: list[str] = []
        self.omission_violation_reasons: list[str] = []
        self.first_measured_at: str | None = None
        self.last_measured_at: str | None = None

    def _record_invalid(self, reason: str) -> None:
        self.invalid_payloads += 1
        if len(self.invalid_reasons) < 10:
            self.invalid_reasons.append(reason)

    @property
    def missing_virtual_seconds(self) -> int:
        return self.plan.expected_publish_samples - self.target_unique_messages

    @property
    def planned_omitted_seconds(self) -> int:
        return self.plan.expected_omitted_samples

    def validate_payload_contract(self, raw_data: bytes | str) -> tuple[bool, str | None, dict | None, int | None]:
        """
        JSON 디코딩, 정확한 14개 필드, 호환 필드, 수치 유한성, 타임스탬프 규격을 엄격히 검증합니다.
        반환: (is_valid, error_reason, parsed_dict, second_of_day)
        """
        # 1. JSON 파싱
        try:
            if isinstance(raw_data, bytes):
                text = raw_data.decode("utf-8")
            else:
                text = raw_data
            payload = json.loads(text)
        except Exception as exc:
            return False, f"JSON_PARSE_ERROR: {exc}", None, None

        if not isinstance(payload, dict):
            return False, "PAYLOAD_NOT_A_DICT", None, None

        # 2. 정확한 14개 필드 일치 확인 (추가/누락 필드 거절)
        payload_keys = set(payload.keys())
        if payload_keys != EXACT_FIELDS:
            missing = sorted(EXACT_FIELDS - payload_keys)
            extra = sorted(payload_keys - EXACT_FIELDS)
            return False, f"KEY_MISMATCH: missing={missing}, extra={extra}", payload, None

        # 3. 금지된 AI 정답 라벨 명시적 거절
        found_forbidden = payload_keys & FORBIDDEN_AI_FIELDS
        if found_forbidden:
            return False, f"FORBIDDEN_AI_FIELDS_FOUND: {sorted(found_forbidden)}", payload, None

        # 4. 호환 필드 관계 및 대상 가구/장치 일치 확인
        if payload["household_id"] != TARGET_HOUSEHOLD or payload["house"] != TARGET_HOUSEHOLD:
            return False, f"HOUSEHOLD_MISMATCH: household_id={payload.get('household_id')}, house={payload.get('house')}", payload, None

        if payload["device_id"] != TARGET_DEVICE or payload["device"] != TARGET_DEVICE:
            return False, f"DEVICE_MISMATCH: device_id={payload.get('device_id')}, device={payload.get('device')}", payload, None

        if payload["ts"] != payload["measured_at"]:
            return False, f"TS_MEASURED_AT_MISMATCH: ts={payload.get('ts')}, measured_at={payload.get('measured_at')}", payload, None

        if payload["power_w"] != payload["active_power"]:
            return False, f"POWER_W_ACTIVE_POWER_MISMATCH: power_w={payload.get('power_w')}, active_power={payload.get('active_power')}", payload, None

        # 5. UUID 형식 유효성 확인
        msg_id_raw = payload["message_id"]
        if not isinstance(msg_id_raw, str):
            return False, "MESSAGE_ID_NOT_STRING", payload, None
        try:
            uuid.UUID(msg_id_raw)
        except Exception as exc:
            return False, f"INVALID_UUID_FORMAT: {exc}", payload, None

        # 6. 숫자 필드 유한성 및 범위 확인 (bool 거절)
        def _check_num(name: str, val: Any, min_val: float, max_val: float | None = None, strictly_positive: bool = False) -> str | None:
            if isinstance(val, bool):
                return f"{name}_IS_BOOLEAN"
            if type(val) not in (int, float):
                return f"{name}_NOT_NUMERIC ({type(val).__name__})"
            if not math.isfinite(val):
                return f"{name}_NOT_FINITE ({val})"
            if strictly_positive:
                if val <= 0:
                    return f"{name}_NOT_STRICTLY_POSITIVE ({val})"
            else:
                if val < min_val:
                    return f"{name}_BELOW_MIN ({val} < {min_val})"
            if max_val is not None and val > max_val:
                return f"{name}_ABOVE_MAX ({val} > {max_val})"
            return None

        for field, min_v, max_v, strict_pos in [
            ("active_power", 0.0, None, False),
            ("reactive_power", 0.0, None, False),
            ("apparent_power", 0.0, None, False),
            ("current", 0.0, None, False),
            ("voltage", 0.0, None, True),
            ("power_factor", 0.0, 1.0, False),
            ("power_w", 0.0, None, False),
        ]:
            err = _check_num(field, payload[field], min_v, max_v, strict_pos)
            if err is not None:
                return False, err, payload, None

        # 7. 타임스탬프 형식, 타임존(+09:00), 마이크로초 0, 날짜 검증(plan.reference_date), 범위 0~86399 확인
        measured_at_str = payload["measured_at"]
        if not isinstance(measured_at_str, str):
            return False, "MEASURED_AT_NOT_STRING", payload, None

        try:
            dt = datetime.fromisoformat(measured_at_str)
        except Exception as exc:
            return False, f"MEASURED_AT_ISO_PARSE_ERROR: {exc}", payload, None

        if dt.tzinfo is None:
            return False, "MEASURED_AT_TIMEZONE_MISSING", payload, None

        if dt.utcoffset() != timedelta(hours=9):
            return False, f"MEASURED_AT_NOT_KST (+09:00): {dt.utcoffset()}", payload, None

        if dt.microsecond != 0:
            return False, f"MEASURED_AT_HAS_MICROSECONDS: {dt.microsecond}", payload, None

        if not (self.plan.start_date <= dt.date() <= self.plan.end_date):
            return False, f"MEASURED_AT_DATE_MISMATCH: {dt.date()} not in [{self.plan.start_date}, {self.plan.end_date}]", payload, None

        start_dt = datetime(self.plan.start_date.year, self.plan.start_date.month, self.plan.start_date.day, 0, 0, 0, tzinfo=KST)
        diff_sec = (dt - start_dt).total_seconds()
        if not diff_sec.is_integer():
            return False, f"MEASURED_AT_NOT_INTEGER_SECONDS: {diff_sec}", payload, None

        relative_second = int(diff_sec)
        if not (0 <= relative_second < self.plan.total_virtual_slots):
            return False, f"RELATIVE_SECOND_OUT_OF_RANGE: {relative_second}", payload, None

        return True, None, payload, relative_second

    def process_message(self, raw_data: bytes | str) -> str:
        """
        메시지를 검증하고 다음 분류 문자열 중 하나를 반환합니다:
        - "TARGET": 현재 대상 실행의 새로운 고유 메시지 (idle timer 갱신 대상)
        - "DUPLICATE": 현재 대상 실행의 QoS 1 중복 메시지 (idle timer 미갱신)
        - "FOREIGN": 타 실행 또는 이전 세션의 메시지 (idle timer 미갱신)
        - "INVALID": 계약 위반 메시지 (invalid_payloads 증가)
        - "UNEXPECTED_OMISSION": 계획 결측 구간 침범 메시지 (unexpected_in_omission 증가)
        """
        is_valid, reason, payload, relative_second = self.validate_payload_contract(raw_data)
        if not is_valid:
            self._record_invalid(reason or "UNKNOWN_VALIDATION_ERROR")
            return "INVALID"

        assert payload is not None
        assert relative_second is not None

        # 8. 대상 실행 run_id 기반 실시간 기대 UUID5 계산 (절대 사이클 유도)
        absolute_cycle = self.plan.compiled_plan.first_cycle + relative_second
        measured_at_str = payload["measured_at"]
        uuid5_name = f"SCHEDULE_PUBLISHER_V1::{self.run_id}::{TARGET_HOUSEHOLD}::{absolute_cycle}::{measured_at_str}"
        expected_uuid5 = str(uuid.uuid5(SCHEDULE_PUBLISHER_NAMESPACE, uuid5_name))

        if payload["message_id"] != expected_uuid5:
            self.foreign_run_messages += 1
            return "FOREIGN"

        # 계획 결측 구간 침범 검사 (이중 집계 금지: unexpected_in_omission만 증가, invalid_payloads는 증가하지 않음)
        if self.plan.schedule_bitmap[relative_second] == 0:
            self.unexpected_in_omission += 1
            if len(self.omission_violation_reasons) < 10:
                self.omission_violation_reasons.append(
                    f"UNEXPECTED_IN_OMISSION: relative_second={relative_second}, second_of_day={relative_second % SECONDS_PER_DAY}, measured_at={measured_at_str}"
                )
            return "UNEXPECTED_OMISSION"

        # 9. 비트맵을 통한 중복/최초 수신 판정
        if self.timeline_bitmap[relative_second] == 1:
            self.duplicate_deliveries += 1
            return "DUPLICATE"

        self.timeline_bitmap[relative_second] = 1
        self.target_unique_messages += 1

        # [1] 첫/마지막 발행 시각 기록을 상대 초(relative_second) 기준으로 판정
        if relative_second == self.plan.first_publish_second:
            self.first_measured_at = measured_at_str
        elif self.first_measured_at is None and self.target_unique_messages == 1:
            self.first_measured_at = measured_at_str

        if relative_second == self.plan.last_publish_second:
            self.last_measured_at = measured_at_str
        elif self.target_unique_messages == self.plan.expected_publish_samples:
            self.last_measured_at = measured_at_str

        return "TARGET"


# ==============================================================================
# 시뮬레이터 HTTP 클라이언트 유틸리티 (비동기 to_thread 호출용)
# ==============================================================================
def http_post_start_run(
    api_url: str,
    timeout_sec: float,
    scenario: str = TARGET_SCENARIO,
    reference_date: str = TARGET_REFERENCE_DATE,
    household_id: str = TARGET_HOUSEHOLD,
) -> str:
    """시뮬레이터에 E2E 실행을 시작하고 run_id를 반환합니다."""
    url = f"{api_url.rstrip('/')}/api/e2e/runs"
    body = {
        "reference_date": reference_date,
        "execution": {"mode": "BURST"},
        "households": [
            {
                "household_id": household_id,
                "scenario": scenario,
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
        raise SimulatorApiError(f"시작 HTTP 오류 ({err.code}): {err_msg}") from err
    except (socket.timeout, TimeoutError) as err:
        raise TimeoutVerificationError(f"시뮬레이터 시작 API 호출 시간 초과 ({timeout_sec}s): {err}") from err
    except urllib.error.URLError as err:
        if isinstance(err.reason, (socket.timeout, TimeoutError)) or "timed out" in str(err.reason).lower():
            raise TimeoutVerificationError(f"시뮬레이터 시작 API 요청 시간 초과: {err.reason}") from err
        raise SimulatorApiError(f"시뮬레이터 시작 API 호출 실패: {err.reason}") from err
    except Exception as exc:
        if _is_timeout_exception(exc):
            raise TimeoutVerificationError(f"시뮬레이터 시작 API 타임아웃: {exc}") from exc
        raise SimulatorApiError(f"시뮬레이터 시작 API 호출 실패: {exc}") from exc


def http_get_run_snapshot(api_url: str, run_id: str, timeout_sec: float) -> dict:
    """세션 스냅샷을 조회합니다."""
    url = f"{api_url.rstrip('/')}/api/e2e/runs/{run_id}"
    req = urllib.request.Request(
        url,
        headers={"Accept": "application/json"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            if resp.status != 200:
                raise SimulatorApiError(f"스냅샷 조회 실패 (HTTP {resp.status})")
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as err:
        err_msg = err.read().decode("utf-8", errors="replace")
        raise SimulatorApiError(f"스냅샷 HTTP 오류 ({err.code}): {err_msg}") from err
    except (socket.timeout, TimeoutError) as err:
        raise TimeoutVerificationError(f"스냅샷 조회 시간 초과 ({timeout_sec}s): {err}") from err
    except urllib.error.URLError as err:
        if isinstance(err.reason, (socket.timeout, TimeoutError)) or "timed out" in str(err.reason).lower():
            raise TimeoutVerificationError(f"스냅샷 조회 요청 시간 초과: {err.reason}") from err
        raise SimulatorApiError(f"스냅샷 조회 실패: {err.reason}") from err
    except Exception as exc:
        if _is_timeout_exception(exc):
            raise TimeoutVerificationError(f"스냅샷 조회 타임아웃: {exc}") from exc
        raise SimulatorApiError(f"스냅샷 조회 실패: {exc}") from exc


def http_post_stop_run(api_url: str, run_id: str, timeout_sec: float = 3.0) -> None:
    """세션을 안전하게 정지합니다 (Best-effort 정리, socket timeout 적용)."""
    url = f"{api_url.rstrip('/')}/api/e2e/runs/{run_id}/stop"
    req = urllib.request.Request(
        url,
        data=b"{}",
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec):
            pass
    except Exception:
        pass


async def _cleanup_active_run(
    api_url: str,
    run_id: str | None,
    latest_api_snap: dict | None,
    timeout_sec: float = 3.0,
) -> None:
    """
    최신 스냅샷의 overall_status에 따라 세션 정지를 조건부로 수행합니다.
    - COMPLETED, FAILED, PARTIAL_FAILED, STOPPED, STOPPING: stop 미호출
    - STARTING, RUNNING, PAUSED, 스냅샷 미획득, 알 수 없는 상태: stop 호출
    """
    if not run_id:
        return

    overall_status = None
    if latest_api_snap and isinstance(latest_api_snap, dict):
        overall_status = latest_api_snap.get("overall_status")

    if overall_status in TERMINAL_AND_STOPPING_STATUSES:
        return

    stop_task = asyncio.create_task(
        asyncio.to_thread(http_post_stop_run, api_url, run_id, timeout_sec),
        name=f"cleanup_stop_{run_id}",
    )
    try:
        await asyncio.wait_for(stop_task, timeout=timeout_sec)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        pass
    except Exception:
        pass
    finally:
        if not stop_task.done():
            stop_task.cancel()
            try:
                await asyncio.wait_for(stop_task, timeout=0.5)
            except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                pass


# ==============================================================================
# 최종 결과 데이터 구조
# ==============================================================================
@dataclass
class VerificationSummary:
    status: str
    exit_code: int
    scenario: str
    reference_date: str
    expected_publish_samples: int
    expected_omitted_samples: int
    target_unique_messages: int
    duplicate_deliveries: int
    foreign_run_messages: int
    invalid_payloads: int
    missing_virtual_seconds: int
    planned_omitted_seconds: int
    unexpected_in_omission: int
    first_measured_at: str | None
    last_measured_at: str | None
    simulator_status: str | None
    simulator_state: str | None
    api_summary: dict | None
    elapsed_seconds: float
    error_message: str | None
    invalid_reasons: list[str]
    omission_violation_reasons: list[str]


# ==============================================================================
# 메인 비동기 오케스트레이션 함수
# ==============================================================================
async def run_verification(
    broker_config: dict,
    api_url: str,
    connect_timeout: float = 10.0,
    subscribe_timeout: float = 10.0,
    http_timeout: float = 15.0,
    idle_timeout: float = 30.0,
    overall_timeout: float = 600.0,
    poll_interval: float = 0.5,
    expected_plan: ExpectedSchedulePlan | None = None,
) -> VerificationSummary:
    """
    Mosquitto 브로커와 시뮬레이터 API를 연동하여 활동 시나리오 원천 발행을 검증합니다.
    - expected_plan이 None이면 기본값(ACTIVITY_NORMAL / 2026-09-16) 계획을 동적 산출하여 사용합니다.
    """
    if expected_plan is None:
        expected_plan = build_expected_schedule_plan(TARGET_SCENARIO, TARGET_REFERENCE_DATE)

    start_monotonic = time.monotonic()
    overall_deadline = start_monotonic + overall_timeout
    run_id: str | None = None
    latest_api_snap: dict | None = None
    validator: StreamMessageValidator | None = None
    client_entered: bool = False
    subscriber_task: asyncio.Task | None = None
    start_task: asyncio.Task | None = None
    primary_exc: BaseException | None = None
    success_summary: VerificationSummary | None = None
    password = broker_config.get("password")
    cleaned_up_run_id: str | None = None

    # Bounded Queue (용량: 2000)
    msg_queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=2000)
    client: aiomqtt.Client | None = None

    try:
        # 1. TLS 컨텍스트 구성 (생명주기 단계별 에러 분류: TLS 설정 오류 -> code 2)
        tls_context = None
        if broker_config.get("tls_enabled"):
            try:
                tls_context = create_mqtt_tls_context(broker_config.get("ca_file"))
            except (FileNotFoundError, ssl.SSLError, ValueError, OSError, Exception) as exc:
                raise CliConfigError(f"TLS CA 인증서 설정 오류: {_safe_exc_msg(exc, password)}") from exc

        # 2. MQTT 클라이언트 생성 (생명주기 단계별 에러 분류: 클라이언트 생성 오류 -> code 3)
        try:
            client = aiomqtt.Client(
                hostname=broker_config["host"],
                port=broker_config["port"],
                username=broker_config.get("username"),
                password=broker_config.get("password"),
                tls_context=tls_context,
                timeout=connect_timeout,
            )
        except Exception as exc:
            raise MqttConnectError(f"MQTT 클라이언트 인스턴스 생성 실패: {_safe_exc_msg(exc, password)}") from exc

        # 3. MQTT 브로커 연결 (connect timeout / handshake 실패 분류)
        try:
            await asyncio.wait_for(client.__aenter__(), timeout=connect_timeout)
            client_entered = True
        except (asyncio.TimeoutError, TimeoutError) as exc:
            raise TimeoutVerificationError("MQTT 브로커 연결 시간 초과 (connect timeout)") from exc
        except Exception as exc:
            if _is_timeout_exception(exc):
                raise TimeoutVerificationError(f"MQTT 브로커 연결 타임아웃: {_safe_exc_msg(exc, password)}") from exc
            raise MqttConnectError(f"MQTT 브로커 연결 실패 ({broker_config['host']}:{broker_config['port']}): {_safe_exc_msg(exc, password)}") from exc

        # 4. 토픽 구독 및 SUBACK 확인
        try:
            await asyncio.wait_for(client.subscribe(TARGET_TOPIC, qos=1), timeout=subscribe_timeout)
        except (asyncio.TimeoutError, TimeoutError) as exc:
            raise TimeoutVerificationError("MQTT 토픽 구독 시간 초과 (subscribe timeout)") from exc
        except Exception as exc:
            if _is_timeout_exception(exc):
                raise TimeoutVerificationError(f"MQTT 토픽 구독 타임아웃: {_safe_exc_msg(exc, password)}") from exc
            raise MqttSubscribeError(f"MQTT 토픽 구독 실패 ({TARGET_TOPIC}): {_safe_exc_msg(exc, password)}") from exc

        # 5. 비동기 수신 태스크 시작
        async def _subscriber_loop():
            async for message in client.messages:
                if message is None:
                    continue
                topic_obj = getattr(message, "topic", None)
                if topic_obj is None:
                    continue
                topic_str = str(topic_obj)
                matches = False
                if hasattr(topic_obj, "matches"):
                    try:
                        matches = topic_obj.matches(TARGET_TOPIC)
                    except Exception:
                        matches = False
                if matches or topic_str == TARGET_TOPIC:
                    try:
                        msg_queue.put_nowait(message.payload)
                    except asyncio.QueueFull:
                        raise QueueOverflowError("MQTT 수신 큐 용량(2000) 초과: HTTP 응답 지연 또는 소비 지연")

        subscriber_task = asyncio.create_task(_subscriber_loop(), name="mqtt_subscriber")

        # 6. HTTP POST 비동기 호출 (명시적 Task 생성 및 취소 복구 계약)
        start_task = asyncio.create_task(
            asyncio.to_thread(
                http_post_start_run,
                api_url,
                http_timeout,
                scenario=expected_plan.scenario_id,
                reference_date=expected_plan.reference_date_str,
                household_id=TARGET_HOUSEHOLD,
            ),
            name="http_start_run_task",
        )
        try:
            run_id = await asyncio.wait_for(asyncio.shield(start_task), timeout=http_timeout)
        except asyncio.CancelledError as cancel_err:
            # 시작 요청 진행 중 취소 발생 시:
            # 즉시 버리지 않고 bounded wait(최대 1.5초)로 start_task 결과 회수 시도
            try:
                run_id = await asyncio.wait_for(asyncio.shield(start_task), timeout=1.5)
            except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                pass

            if run_id:
                cleaned_up_run_id = run_id
                try:
                    await _cleanup_active_run(api_url, run_id, None, timeout_sec=3.0)
                except (asyncio.CancelledError, Exception):
                    pass

            if not start_task.done():
                start_task.cancel()
                try:
                    await asyncio.wait_for(start_task, timeout=0.5)
                except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                    pass

            raise cancel_err
        except (asyncio.TimeoutError, TimeoutError) as exc:
            if not start_task.done():
                start_task.cancel()
                try:
                    await asyncio.wait_for(start_task, timeout=0.5)
                except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                    pass
            raise TimeoutVerificationError("시뮬레이터 시작 API 호출 시간 초과 (HTTP timeout)") from exc
        except Exception as exc:
            if not start_task.done():
                start_task.cancel()
                try:
                    await asyncio.wait_for(start_task, timeout=0.5)
                except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                    pass
            if _is_timeout_exception(exc):
                raise TimeoutVerificationError(f"시뮬레이터 시작 API 타임아웃: {_safe_exc_msg(exc, password)}") from exc
            raise

        # 7. 검증기 초기화
        validator = StreamMessageValidator(run_id, plan=expected_plan)
        idle_deadline = time.monotonic() + idle_timeout
        last_poll_time = 0.0

        # 8. 실시간 스트리밍 검증 루프
        while True:
            now = time.monotonic()

            if now > overall_deadline:
                raise TimeoutVerificationError(f"전체 실행 제한시간 초과 ({overall_timeout}s)")

            if now > idle_deadline:
                raise TimeoutVerificationError(f"타겟 신규 메시지 수신 대기시간 초과 ({idle_timeout}s idle timeout)")

            if subscriber_task.done():
                sub_exc = subscriber_task.exception()
                if sub_exc:
                    raise sub_exc

            drained_any = False
            while not msg_queue.empty():
                try:
                    raw_msg = msg_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                drained_any = True
                classification = validator.process_message(raw_msg)
                if classification == "TARGET":
                    idle_deadline = time.monotonic() + idle_timeout

            if now - last_poll_time >= poll_interval:
                last_poll_time = now
                try:
                    latest_api_snap = await asyncio.wait_for(
                        asyncio.to_thread(http_get_run_snapshot, api_url, run_id, http_timeout),
                        timeout=http_timeout,
                    )
                except (asyncio.TimeoutError, TimeoutError) as exc:
                    raise TimeoutVerificationError("상태 조회 API 시간 초과 (HTTP timeout)") from exc
                except Exception as exc:
                    if _is_timeout_exception(exc):
                        raise TimeoutVerificationError(f"상태 조회 API 타임아웃: {_safe_exc_msg(exc, password)}") from exc
                    raise

                ov_status = latest_api_snap.get("overall_status")
                if ov_status in ("FAILED", "STOPPED"):
                    last_err = latest_api_snap.get("last_error")
                    raise SimulatorTerminalFailureError(f"시뮬레이터 세션이 비정상 종료되었습니다: {ov_status} (error: {last_err})")

            mqtt_done = (
                validator.target_unique_messages == expected_plan.expected_publish_samples
                and validator.missing_virtual_seconds == 0
                and validator.invalid_payloads == 0
                and validator.unexpected_in_omission == 0
            )

            api_done = False
            if latest_api_snap is not None:
                h_list = latest_api_snap.get("households", [])
                h_map = {h.get("household_id"): h for h in h_list}
                h001 = h_map.get(TARGET_HOUSEHOLD, {})

                api_done = (
                    latest_api_snap.get("overall_status") == "COMPLETED"
                    and latest_api_snap.get("overall_state") == "COMPLETED"
                    and h001.get("state") == "COMPLETED"
                    and h001.get("runner_virtual_slots") == expected_plan.total_virtual_slots
                    and h001.get("settled_virtual_slots") == expected_plan.total_virtual_slots
                    and h001.get("published_samples") == expected_plan.expected_publish_samples
                    and h001.get("planned_publish_samples") == expected_plan.expected_publish_samples
                    and h001.get("omitted_samples") == expected_plan.expected_omitted_samples
                    and h001.get("completed_days") == expected_plan.total_days
                    and h001.get("total_days") == expected_plan.total_days
                    and h001.get("last_error") is None
                )

            if mqtt_done and api_done:
                elapsed = time.monotonic() - start_monotonic
                success_summary = VerificationSummary(
                    status="SUCCESS",
                    exit_code=EXIT_SUCCESS,
                    scenario=expected_plan.scenario_id,
                    reference_date=expected_plan.reference_date_str,
                    expected_publish_samples=expected_plan.expected_publish_samples,
                    expected_omitted_samples=expected_plan.expected_omitted_samples,
                    target_unique_messages=validator.target_unique_messages,
                    duplicate_deliveries=validator.duplicate_deliveries,
                    foreign_run_messages=validator.foreign_run_messages,
                    invalid_payloads=validator.invalid_payloads,
                    missing_virtual_seconds=validator.missing_virtual_seconds,
                    planned_omitted_seconds=validator.planned_omitted_seconds,
                    unexpected_in_omission=validator.unexpected_in_omission,
                    first_measured_at=validator.first_measured_at,
                    last_measured_at=validator.last_measured_at,
                    simulator_status=latest_api_snap.get("overall_status") if latest_api_snap else None,
                    simulator_state=latest_api_snap.get("overall_state") if latest_api_snap else None,
                    api_summary=latest_api_snap,
                    elapsed_seconds=round(elapsed, 3),
                    error_message=None,
                    invalid_reasons=validator.invalid_reasons,
                    omission_violation_reasons=validator.omission_violation_reasons,
                )
                break

            if not drained_any:
                await asyncio.sleep(0.01)

    except asyncio.CancelledError as err:
        primary_exc = err
    except VerificationToolError as err:
        primary_exc = err
    except Exception as exc:
        primary_exc = exc
    finally:
        # 정리 우선순위:
        # 1. start_task가 남아있으면 bounded cancel & drain
        # 2. run_id가 있으면 overall_status 기준으로 best-effort stop
        # 3. subscriber_task 취소 및 bounded drain
        # 4. client_entered가 True인 경우에만 client.__aexit__
        # 모든 정리 단계의 예외는 흡수되어 primary_exc를 절대 덮어쓰지 않음
        if start_task is not None and not start_task.done():
            start_task.cancel()
            try:
                await asyncio.wait_for(start_task, timeout=0.5)
            except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                pass

        if run_id and run_id != cleaned_up_run_id:
            try:
                await _cleanup_active_run(api_url, run_id, latest_api_snap, timeout_sec=3.0)
            except (asyncio.CancelledError, Exception):
                pass

        if subscriber_task is not None and not subscriber_task.done():
            subscriber_task.cancel()
            try:
                await asyncio.wait_for(subscriber_task, timeout=1.0)
            except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                pass

        if client_entered and client is not None:
            exit_task = asyncio.create_task(
                client.__aexit__(None, None, None),
                name="cleanup_mqtt_exit",
            )
            try:
                await asyncio.wait_for(exit_task, timeout=2.0)
            except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                pass
            finally:
                if not exit_task.done():
                    exit_task.cancel()
                    try:
                        await asyncio.wait_for(exit_task, timeout=0.5)
                    except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                        pass

    # 정리 후 결과 반환 또는 CancelledError 재전파
    if isinstance(primary_exc, asyncio.CancelledError):
        raise primary_exc

    if primary_exc is not None:
        elapsed = time.monotonic() - start_monotonic
        if isinstance(primary_exc, VerificationToolError):
            code = primary_exc.exit_code
            msg = redact_secrets(str(primary_exc), password)
        else:
            code = EXIT_INTERNAL_ERROR
            msg = f"예기치 않은 내부 오류: {_safe_exc_msg(primary_exc, password)}"

        return VerificationSummary(
            status="FAILURE",
            exit_code=code,
            scenario=expected_plan.scenario_id,
            reference_date=expected_plan.reference_date_str,
            expected_publish_samples=expected_plan.expected_publish_samples,
            expected_omitted_samples=expected_plan.expected_omitted_samples,
            target_unique_messages=validator.target_unique_messages if validator else 0,
            duplicate_deliveries=validator.duplicate_deliveries if validator else 0,
            foreign_run_messages=validator.foreign_run_messages if validator else 0,
            invalid_payloads=validator.invalid_payloads if validator else 0,
            missing_virtual_seconds=validator.missing_virtual_seconds if validator else expected_plan.expected_publish_samples,
            planned_omitted_seconds=validator.planned_omitted_seconds if validator else expected_plan.expected_omitted_samples,
            unexpected_in_omission=validator.unexpected_in_omission if validator else 0,
            first_measured_at=validator.first_measured_at if validator else None,
            last_measured_at=validator.last_measured_at if validator else None,
            simulator_status=latest_api_snap.get("overall_status") if latest_api_snap else None,
            simulator_state=latest_api_snap.get("overall_state") if latest_api_snap else None,
            api_summary=latest_api_snap,
            elapsed_seconds=round(elapsed, 3),
            error_message=msg,
            invalid_reasons=validator.invalid_reasons if validator else [],
            omission_violation_reasons=validator.omission_violation_reasons if validator else [],
        )

    assert success_summary is not None
    return success_summary


# ==============================================================================
# CLI 인자 파싱 및 메인 진입점
# ==============================================================================
def parse_arguments(args: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m tools.verify_activity_normal_mqtt",
        description="일일 활동 시나리오 시뮬레이터 원천 MQTT 발행 스트리밍 검증 도구",
    )
    parser.add_argument(
        "--scenario",
        default=TARGET_SCENARIO,
        help="검증 대상 시나리오 ID (허용값: 통합 카탈로그 10종 - ACTIVITY_*, ROUTINE_CHANGED_*, BASELINE_MICROWAVE_20D / 기본값: ACTIVITY_NORMAL)",
    )
    parser.add_argument(
        "--reference-date",
        default=TARGET_REFERENCE_DATE,
        help="시뮬레이션 기준 일자 (단일 일자는 해당일, 다일 시나리오는 마지막 날을 의미 / 기본값: 2026-09-16, YYYY-MM-DD)",
    )
    parser.add_argument("--broker-host", default=None, help="MQTT 브로커 주소 (기본값: MQTT_HOST 또는 localhost)")
    parser.add_argument("--broker-port", type=int, default=None, help="MQTT 브로커 포트 (기본값: resolve_mqtt_port)")
    parser.add_argument("--broker-user", default=None, help="MQTT 계정명 (기본값: MQTT_USER)")
    parser.add_argument("--tls-enabled", default=None, help="TLS 활성화 여부 (true/false, 1/0, on/off)")
    parser.add_argument("--ca-file", default=None, help="TLS CA 인증서 경로 (기본값: MQTT_CA_FILE)")
    parser.add_argument("--api-url", default="http://127.0.0.1:8085", help="시뮬레이터 API URL (기본값: http://127.0.0.1:8085)")
    parser.add_argument("--connect-timeout", type=float, default=10.0, help="MQTT 연결 타임아웃 (초, 기본값: 10.0)")
    parser.add_argument("--subscribe-timeout", type=float, default=10.0, help="MQTT 구독 타임아웃 (초, 기본값: 10.0)")
    parser.add_argument("--http-timeout", type=float, default=15.0, help="HTTP API 요청 타임아웃 (초, 기본값: 15.0)")
    parser.add_argument("--idle-timeout", type=float, default=30.0, help="신규 타겟 메시지 유휴 타임아웃 (초, 기본값: 30.0)")
    parser.add_argument("--overall-timeout", type=float, default=600.0, help="전체 검증 제한시간 (초, 기본값: 600.0)")
    parser.add_argument("--poll-interval", type=float, default=0.5, help="API 상태 폴링 간격 (초, 기본값: 0.5)")
    return parser.parse_args(args)


def main(argv: list[str] | None = None) -> int:
    try:
        parsed_args = parse_arguments(argv)
    except SystemExit as exc:
        return exc.code

    password = os.getenv("MQTT_PASS")
    expected_plan: ExpectedSchedulePlan | None = None

    try:
        # 1. CLI 명시 포트 번호 범위 검증 (1~65535)
        validate_port(parsed_args.broker_port)

        # 2. 타임아웃 및 폴링 주기 유효성 검증
        validate_positive_timeout(parsed_args.connect_timeout, "connect-timeout")
        validate_positive_timeout(parsed_args.subscribe_timeout, "subscribe-timeout")
        validate_positive_timeout(parsed_args.http_timeout, "http-timeout")
        validate_positive_timeout(parsed_args.idle_timeout, "idle-timeout")
        validate_positive_timeout(parsed_args.overall_timeout, "overall-timeout")
        validate_positive_timeout(parsed_args.poll_interval, "poll-interval")

        # 3. 시나리오 및 기준일자 계획 동적 산출 (MQTT 연결 및 HTTP 시작 호출 전 조기 검증)
        if parsed_args.scenario not in UNIFIED_SCENARIO_IDS:
            raise CliConfigError(
                f"지원하지 않는 시나리오 ID입니다: {parsed_args.scenario!r}. "
                f"허용 목록: {list(UNIFIED_SCENARIO_IDS)}"
            )
        expected_plan = build_expected_schedule_plan(parsed_args.scenario, parsed_args.reference_date)

        # 다일 시나리오 타임아웃 자동 상향 산출
        plan_based_timeout = max(600.0, (expected_plan.total_virtual_slots / 500.0) + 300.0)
        effective_overall_timeout = max(parsed_args.overall_timeout, plan_based_timeout)
        if effective_overall_timeout > parsed_args.overall_timeout:
            sys.stdout.write(
                f"[안내] {expected_plan.scenario_id} ({expected_plan.total_days}일, {expected_plan.total_virtual_slots:,} 슬롯) "
                f"시나리오에 맞추어 overall-timeout이 {parsed_args.overall_timeout}초에서 {effective_overall_timeout:.1f}초로 자동 상향되었습니다.\n"
            )

        # 4. TLS 활성화 설정 해석
        tls_bool = None
        if parsed_args.tls_enabled is not None:
            try:
                tls_bool = parse_tls_enabled(parsed_args.tls_enabled)
            except ValueError as err:
                raise CliConfigError(f"TLS 설정 해석 실패: {_safe_exc_msg(err, password)}") from err

        # 5. MQTT 설정 통합 해석 (비밀번호는 환경변수 MQTT_PASS 사용, CLI 인자 미지원)
        try:
            broker_cfg = resolve_mqtt_config(
                host=parsed_args.broker_host,
                port=parsed_args.broker_port,
                username=parsed_args.broker_user,
                password=password,
                tls_enabled=tls_bool,
                ca_file=parsed_args.ca_file,
            )
        except Exception as exc:
            raise CliConfigError(f"MQTT 설정 해석 실패: {_safe_exc_msg(exc, password)}") from exc

        # 6. 환경변수 MQTT_PORT를 포함하여 resolve_mqtt_config()가 최종 결정한 포트 유효성 재검증
        resolved_port = validate_port(broker_cfg.get("port"))
        broker_cfg["port"] = resolved_port

    except VerificationToolError as err:
        scenario_val = expected_plan.scenario_id if expected_plan is not None else parsed_args.scenario
        ref_date_val = expected_plan.reference_date_str if expected_plan is not None else parsed_args.reference_date
        exp_pub = expected_plan.expected_publish_samples if expected_plan else SECONDS_PER_DAY
        exp_omit = expected_plan.expected_omitted_samples if expected_plan else 0

        fail_summary = VerificationSummary(
            status="FAILURE",
            exit_code=err.exit_code,
            scenario=scenario_val,
            reference_date=ref_date_val,
            expected_publish_samples=exp_pub,
            expected_omitted_samples=exp_omit,
            target_unique_messages=0,
            duplicate_deliveries=0,
            foreign_run_messages=0,
            invalid_payloads=0,
            missing_virtual_seconds=exp_pub,
            planned_omitted_seconds=exp_omit,
            unexpected_in_omission=0,
            first_measured_at=None,
            last_measured_at=None,
            simulator_status=None,
            simulator_state=None,
            api_summary=None,
            elapsed_seconds=0.0,
            error_message=redact_secrets(str(err), password),
            invalid_reasons=[],
            omission_violation_reasons=[],
        )
        json_out = json.dumps(asdict(fail_summary), indent=2, ensure_ascii=False)
        sys.stdout.write(redact_secrets(json_out, password) + "\n")
        return err.exit_code
    except Exception as exc:
        scenario_val = expected_plan.scenario_id if expected_plan is not None else parsed_args.scenario
        ref_date_val = expected_plan.reference_date_str if expected_plan is not None else parsed_args.reference_date
        exp_pub = expected_plan.expected_publish_samples if expected_plan else SECONDS_PER_DAY
        exp_omit = expected_plan.expected_omitted_samples if expected_plan else 0

        fail_summary = VerificationSummary(
            status="FAILURE",
            exit_code=EXIT_INTERNAL_ERROR,
            scenario=scenario_val,
            reference_date=ref_date_val,
            expected_publish_samples=exp_pub,
            expected_omitted_samples=exp_omit,
            target_unique_messages=0,
            duplicate_deliveries=0,
            foreign_run_messages=0,
            invalid_payloads=0,
            missing_virtual_seconds=exp_pub,
            planned_omitted_seconds=exp_omit,
            unexpected_in_omission=0,
            first_measured_at=None,
            last_measured_at=None,
            simulator_status=None,
            simulator_state=None,
            api_summary=None,
            elapsed_seconds=0.0,
            error_message=f"설정 초기화 오류: {_safe_exc_msg(exc, password)}",
            invalid_reasons=[],
            omission_violation_reasons=[],
        )
        json_out = json.dumps(asdict(fail_summary), indent=2, ensure_ascii=False)
        sys.stdout.write(redact_secrets(json_out, password) + "\n")
        return EXIT_INTERNAL_ERROR

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        summary = asyncio.run(
            run_verification(
                broker_config=broker_cfg,
                api_url=parsed_args.api_url,
                connect_timeout=parsed_args.connect_timeout,
                subscribe_timeout=parsed_args.subscribe_timeout,
                http_timeout=parsed_args.http_timeout,
                idle_timeout=parsed_args.idle_timeout,
                overall_timeout=effective_overall_timeout,
                poll_interval=parsed_args.poll_interval,
                expected_plan=expected_plan,
            )
        )
    except KeyboardInterrupt:
        sys.stderr.write("[중단] 사용자에 의해 실행이 중단되었습니다 (SIGINT).\n")
        return EXIT_KEYBOARD_INTERRUPT
    except Exception as exc:
        fail_summary = VerificationSummary(
            status="FAILURE",
            exit_code=EXIT_INTERNAL_ERROR,
            scenario=expected_plan.scenario_id,
            reference_date=expected_plan.reference_date_str,
            expected_publish_samples=expected_plan.expected_publish_samples,
            expected_omitted_samples=expected_plan.expected_omitted_samples,
            target_unique_messages=0,
            duplicate_deliveries=0,
            foreign_run_messages=0,
            invalid_payloads=0,
            missing_virtual_seconds=expected_plan.expected_publish_samples,
            planned_omitted_seconds=expected_plan.expected_omitted_samples,
            unexpected_in_omission=0,
            first_measured_at=None,
            last_measured_at=None,
            simulator_status=None,
            simulator_state=None,
            api_summary=None,
            elapsed_seconds=0.0,
            error_message=f"비동기 실행 오류: {_safe_exc_msg(exc, password)}",
            invalid_reasons=[],
            omission_violation_reasons=[],
        )
        json_out = json.dumps(asdict(fail_summary), indent=2, ensure_ascii=False)
        sys.stdout.write(redact_secrets(json_out, password) + "\n")
        return EXIT_INTERNAL_ERROR

    output_dict = asdict(summary)
    json_out = json.dumps(output_dict, indent=2, ensure_ascii=False)
    sys.stdout.write(redact_secrets(json_out, password) + "\n")
    return summary.exit_code


if __name__ == "__main__":
    sys.exit(main())
