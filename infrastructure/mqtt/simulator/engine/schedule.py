"""
NILM 스마트홈 전력 시뮬레이터 선언적 일정 기반 E2E 시나리오 엔진 (Deterministic Schedule Engine)

- 일자별 1초 간격 86,400개 가상 시간 슬롯 기반 결정론적 타임라인 모델
- 가전 가동 이벤트 및 전후 보호 구간 [start_sec - 3, end_sec + 3) 결측 충돌 검증
- 부분/전일 미발행(OmissionRange) 지원 및 242만 개 마스크 없는 O(D + E + R) 산술 조회
- O(1) Sparse Transition 매핑 및 MappingProxyType 기반 읽기 전용 불변 실행 계획 보장
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping

# 한국 표준시 (KST = UTC+9)
KST = timezone(timedelta(hours=9))

# 하루 총 가상 시간 슬롯(초)
SECONDS_PER_DAY: int = 86_400

# 6대 지원 가전 프로파일 (단일 진실 공급원)
try:
    from .profiles import DEVICE_PROFILES
except ImportError:
    from profiles import DEVICE_PROFILES

SUPPORTED_APPLIANCES: tuple[str, ...] = tuple(sorted(DEVICE_PROFILES.keys()))


# ==========================================
# 1. 전환 타입 Enum 및 정렬 우선순위
# ==========================================
class TransitionType(str, Enum):
    """가전 상태 전환 유형 (ON / OFF)"""
    ON = "ON"
    OFF = "OFF"


# 동일 초 동시 전환 시 명시적 우선순위: OFF를 ON보다 먼저 처리 (불변 매핑 프록시)
TRANSITION_PRIORITY: Mapping[TransitionType, int] = MappingProxyType({
    TransitionType.OFF: 0,
    TransitionType.ON: 1,
})


# ==========================================
# 2. 도메인 예외 클래스 계층
# ==========================================
class ScheduleError(ValueError):
    """일정 엔진 기본 도메인 예외"""
    pass


class CycleOutOfRangeError(ScheduleError, IndexError):
    """cycle이 컴파일된 실행 계획의 유효 범위 [first_cycle, last_cycle]를 벗어났을 때 발생"""
    pass


class EventEvidenceOmittedError(ScheduleError):
    """가전 이벤트 보호 구간 [start_sec - 3, end_sec + 3)이 omission_ranges와 겹치거나 완전 결측일에 이벤트가 등록되었을 때 발생"""
    pass


class InvalidScenarioIdError(ScheduleError):
    """scenario_id가 비어 있거나 canonical 형식이 아닐 때 발생"""
    pass


class InvalidApplianceError(ScheduleError):
    """지원하지 않는 가전 지정 시 발생"""
    pass


class InvalidTimeFormatError(ScheduleError):
    """HH:MM:SS 시간 형식이 유효하지 않을 때 발생"""
    pass


class InvalidDurationError(ScheduleError):
    """duration_seconds가 정수가 아니거나 1 미만일 때 발생"""
    pass


class EventOverlapError(ScheduleError):
    """동일 가전의 사용 일정이 겹치거나 OFF 간격이 부족할 때 발생"""
    pass


class SampleBoundaryViolationError(ScheduleError):
    """ON 전 3개 OFF, ON 3개 이상, OFF 후 3개 OFF 샘플 규칙 위반 시 발생"""
    pass


class DateBoundaryViolationError(ScheduleError):
    """이벤트가 하루 경계(24:00:00)를 초과할 때 발생"""
    pass


class OmissionRangeError(ScheduleError):
    """미발행 범위 정의가 잘못되었거나 중복/초과 시 발생"""
    pass


class SampleCountViolationError(ScheduleError):
    """선언된 publish_samples가 계산된 실제 발행 예정 수와 불일치할 때 발생"""
    pass


class DayOffsetSequenceError(ScheduleError):
    """day_offset 중복, 비연속 또는 정렬 오류 시 발생"""
    pass


class UnknownFieldError(ScheduleError):
    """입력 딕셔너리에 알 수 없는 필드가 포함되어 있을 때 발생"""
    pass


# ==========================================
# 3. 헬퍼 함수
# ==========================================
def normalize_appliance_name(name: object) -> str:
    """
    가전 이름을 대소문자 무관 소문자 canonical ID로 정규화 (예: 'KETTLE' -> 'kettle', 'MICROWAVE' -> 'microwave').
    문자열이 아니거나 6대 지원 가전이 아니면 InvalidApplianceError 발생.
    """
    if not isinstance(name, str) or isinstance(name, bool):
        raise InvalidApplianceError(f"가전 이름은 문자열이어야 합니다: {name!r}")
    canonical = name.strip().lower()
    if canonical not in SUPPORTED_APPLIANCES:
        raise InvalidApplianceError(
            f"지원하지 않는 가전 ID입니다: '{name}'. 허용 가전 목록: {list(SUPPORTED_APPLIANCES)}"
        )
    return canonical


def resolve_base_date(reference_date: date, total_days: int) -> date:
    """
    reference_date를 전체 시나리오의 마지막 날(종료일)로 간주하고 시작일(base_date)을 역산하여 반환합니다.
    total_days가 1 미만이면 ScheduleError가 발생합니다.
    """
    if type(total_days) is not int or isinstance(total_days, bool) or total_days < 1:
        raise ScheduleError(f"total_days는 1 이상의 정수여야 합니다: {total_days!r}")
    if not isinstance(reference_date, date):
        raise ScheduleError(f"reference_date는 date 인스턴스여야 합니다: {reference_date!r}")
    return reference_date - timedelta(days=total_days - 1)


# ==========================================
# 4. 선언적 입력 데이터 모델
# ==========================================
@dataclass(frozen=True)
class OmissionRange:
    """
    하루(86,400초) 중 MQTT 메시지 발행을 의도적으로 생략(결측)하는 반열린 구간 [start_second, end_second).
    - start_second: inclusive, 0 <= start_second < end_second <= 86400
    - end_second: exclusive
    """
    start_second: int
    end_second: int

    def __post_init__(self):
        if type(self.start_second) is not int or isinstance(self.start_second, bool):
            raise OmissionRangeError(f"start_second는 정수여야 합니다: {self.start_second!r}")
        if type(self.end_second) is not int or isinstance(self.end_second, bool):
            raise OmissionRangeError(f"end_second는 정수여야 합니다: {self.end_second!r}")
        if not (0 <= self.start_second < self.end_second <= SECONDS_PER_DAY):
            raise OmissionRangeError(
                f"유효하지 않은 OmissionRange 범위입니다: [{self.start_second}, {self.end_second}). "
                f"0 <= start_second < end_second <= {SECONDS_PER_DAY} 이어야 합니다."
            )

    @property
    def omitted_samples(self) -> int:
        return self.end_second - self.start_second


@dataclass(frozen=True)
class ApplianceEvent:
    """단일 가전 가동 이벤트 선언"""
    appliance: str
    start_time: str
    duration_seconds: int

    def __post_init__(self):
        # 1. appliance 검증 및 canonical 소문자 정규화
        canonical_app = normalize_appliance_name(self.appliance)
        object.__setattr__(self, "appliance", canonical_app)

        # 2. start_time 형식 검증
        if not isinstance(self.start_time, str) or isinstance(self.start_time, bool):
            raise InvalidTimeFormatError(f"start_time은 문자열이어야 합니다: {self.start_time!r}")
        clean_time = self.start_time.strip()
        if not re.match(r"^([01]\d|2[0-3]):([0-5]\d):([0-5]\d)$", clean_time):
            raise InvalidTimeFormatError(
                f"올바르지 않은 start_time 형식입니다: '{self.start_time}' (HH:MM:SS 형식이어야 합니다)"
            )
        object.__setattr__(self, "start_time", clean_time)

        # 3. duration_seconds 검증
        if type(self.duration_seconds) is not int or isinstance(self.duration_seconds, bool):
            raise InvalidDurationError(f"duration_seconds는 정수여야 합니다: {self.duration_seconds!r}")
        if self.duration_seconds < 1:
            raise InvalidDurationError(f"duration_seconds는 1 이상이어야 합니다: {self.duration_seconds}")
        if self.duration_seconds < 3:
            raise SampleBoundaryViolationError(
                f"ON 상태는 최소 3개 샘플(3초) 이상이어야 합니다: {self.duration_seconds}초 지정됨"
            )

    @property
    def start_second(self) -> int:
        parts = self.start_time.split(":")
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])

    @property
    def end_second(self) -> int:
        return self.start_second + self.duration_seconds


@dataclass(frozen=True)
class DaySchedule:
    """단일 일자(Day) 단위 일정 선언"""
    day_offset: int
    events: tuple[ApplianceEvent, ...] = ()
    omission_ranges: tuple[OmissionRange, ...] = ()
    publish_samples: int | None = None

    def __post_init__(self):
        if type(self.day_offset) is not int or isinstance(self.day_offset, bool):
            raise DayOffsetSequenceError(f"day_offset은 정수여야 합니다: {self.day_offset!r}")

        # 중첩 원소 타입 개별 엄격 검증
        if not isinstance(self.events, (list, tuple)):
            raise ScheduleError(f"events는 시퀀스(list 또는 tuple)여야 합니다: {type(self.events).__name__}")
        for idx, ev in enumerate(self.events):
            if not isinstance(ev, ApplianceEvent):
                raise ScheduleError(f"events[{idx}]는 ApplianceEvent 인스턴스여야 합니다: {type(ev).__name__}")

        if not isinstance(self.omission_ranges, (list, tuple)):
            raise ScheduleError(f"omission_ranges는 시퀀스(list 또는 tuple)여야 합니다: {type(self.omission_ranges).__name__}")
        for idx, om in enumerate(self.omission_ranges):
            if not isinstance(om, OmissionRange):
                raise ScheduleError(f"omission_ranges[{idx}]는 OmissionRange 인스턴스여야 합니다: {type(om).__name__}")

        # canonical 자동 정렬
        sorted_events = tuple(sorted(self.events, key=lambda e: (e.start_time, e.appliance)))
        object.__setattr__(self, "events", sorted_events)

        sorted_omissions = tuple(sorted(self.omission_ranges, key=lambda r: (r.start_second, r.end_second)))
        object.__setattr__(self, "omission_ranges", sorted_omissions)

        # 1. 이벤트 경계 검증 (ON 전 3개 OFF, OFF 후 3개 OFF, 자정 초과)
        for ev in sorted_events:
            if ev.start_second < 3:
                raise SampleBoundaryViolationError(
                    f"하루 시작(00:00:00) 후 최소 3개의 OFF 샘플이 확보되어야 합니다: '{ev.start_time}' 시작 불가"
                )
            if ev.end_second > SECONDS_PER_DAY:
                raise DateBoundaryViolationError(
                    f"이벤트 종료 시각이 하루 경계(24:00:00)를 초과할 수 없습니다: {ev.end_second}초"
                )
            if ev.end_second > SECONDS_PER_DAY - 3:
                raise SampleBoundaryViolationError(
                    f"하루 종료(23:59:59) 전 최소 3개의 OFF 샘플이 확보되어야 합니다: 종료 {ev.end_second}초"
                )

        # 2. 동일 가전 중복 및 최소 3개 OFF 샘플 간격 검증
        events_by_app: dict[str, list[ApplianceEvent]] = {}
        for ev in sorted_events:
            events_by_app.setdefault(ev.appliance, []).append(ev)

        for app, app_events in events_by_app.items():
            for i in range(len(app_events) - 1):
                prev_ev = app_events[i]
                next_ev = app_events[i + 1]
                if next_ev.start_second < prev_ev.end_second:
                    raise EventOverlapError(
                        f"동일 가전('{app}')의 사용 일정이 서로 겹칩니다: "
                        f"[{prev_ev.start_second}, {prev_ev.end_second}) vs [{next_ev.start_second}, {next_ev.end_second})"
                    )
                if next_ev.start_second < prev_ev.end_second + 3:
                    raise EventOverlapError(
                        f"동일 가전('{app}')의 이벤트 사이에 최소 3개의 OFF 샘플 간격이 확보되어야 합니다: "
                        f"이전 종료 {prev_ev.end_second}초, 다음 시작 {next_ev.start_second}초"
                    )

        # 3. omission_ranges 구간 중복/겹침 검증
        for i in range(len(sorted_omissions) - 1):
            if sorted_omissions[i].end_second > sorted_omissions[i + 1].start_second:
                raise OmissionRangeError(
                    f"omission_ranges 구간이 서로 겹칠 수 없습니다: "
                    f"[{sorted_omissions[i].start_second}, {sorted_omissions[i].end_second}) vs "
                    f"[{sorted_omissions[i + 1].start_second}, {sorted_omissions[i + 1].end_second})"
                )

        # 4. 결측 총량 및 예정 발행량 계산
        total_omitted = sum(r.omitted_samples for r in sorted_omissions)
        computed_publish = SECONDS_PER_DAY - total_omitted

        # 5. 완전 결측일(planned_publish_samples == 0) 가전 이벤트 등록 금지
        if computed_publish == 0 and len(sorted_events) > 0:
            raise EventEvidenceOmittedError(
                "완전 결측일(planned_publish_samples=0)에는 가전 이벤트(events)를 등록할 수 없습니다."
            )

        # 6. 가전 이벤트 보호 구간 [start_sec - 3, end_sec + 3)과 omission_ranges 충돌 검증
        for ev in sorted_events:
            prot_start = ev.start_second - 3
            prot_end = ev.end_second + 3
            for om in sorted_omissions:
                if max(prot_start, om.start_second) < min(prot_end, om.end_second):
                    raise EventEvidenceOmittedError(
                        f"가전 이벤트('{ev.appliance}') 보호 구간 [{prot_start}, {prot_end})이 "
                        f"결측 구간 [{om.start_second}, {om.end_second})과 겹쳐 필수 샘플 발행을 보장할 수 없습니다."
                    )

        # 7. 선언된 publish_samples 검증 (지정된 경우에 한함)
        if self.publish_samples is not None:
            if type(self.publish_samples) is not int or isinstance(self.publish_samples, bool):
                raise SampleCountViolationError(f"publish_samples는 정수여야 합니다: {self.publish_samples!r}")
            if self.publish_samples != computed_publish:
                raise SampleCountViolationError(
                    f"선언된 publish_samples({self.publish_samples})가 omission_ranges 기반 "
                    f"계산 결과({computed_publish})와 일치하지 않습니다."
                )


@dataclass(frozen=True)
class ScenarioDefinition:
    """외부에 노출되는 시나리오 선언 객체"""
    scenario_id: str
    days: tuple[DaySchedule, ...]
    description: str = ""

    def __post_init__(self):
        if not isinstance(self.scenario_id, str) or isinstance(self.scenario_id, bool):
            raise InvalidScenarioIdError(f"scenario_id는 문자열이어야 합니다: {type(self.scenario_id).__name__}")
        if not re.fullmatch(r"[A-Z][A-Z0-9_]*", self.scenario_id):
            raise InvalidScenarioIdError(
                f"scenario_id는 대문자 스네이크 케이스(^[A-Z][A-Z0-9_]*$)여야 합니다: '{self.scenario_id}'"
            )
        if not isinstance(self.description, str) or isinstance(self.description, bool):
            raise ScheduleError(f"description은 문자열이어야 합니다: {type(self.description).__name__}")

        if not isinstance(self.days, (list, tuple)) or len(self.days) == 0:
            raise ScheduleError("days는 최소 1개 이상의 DaySchedule을 포함해야 합니다.")
        for idx, d in enumerate(self.days):
            if not isinstance(d, DaySchedule):
                raise ScheduleError(f"days[{idx}]는 DaySchedule 인스턴스여야 합니다: {type(d).__name__}")

        # day_offset 기준 canonical 자동 정렬
        sorted_days = tuple(sorted(self.days, key=lambda d: d.day_offset))
        object.__setattr__(self, "days", sorted_days)

        # day_offset 중복 및 비연속성 검증
        for i in range(len(sorted_days) - 1):
            curr_off = sorted_days[i].day_offset
            next_off = sorted_days[i + 1].day_offset
            if curr_off == next_off:
                raise DayOffsetSequenceError(f"중복된 day_offset이 존재합니다: {curr_off}")
            if next_off != curr_off + 1:
                raise DayOffsetSequenceError(
                    f"day_offset은 빈 날짜 없이 연속된 정수여야 합니다: {curr_off} 다음 {next_off} 발견"
                )


# ==========================================
# 5. 컴파일된 실행 계획 데이터 모델
# ==========================================
@dataclass(frozen=True)
class ApplianceTransition:
    """단일 시점의 가전 상태 전환 지시 (불변)"""
    transition_type: TransitionType
    appliance: str
    second_of_day: int
    absolute_cycle: int
    virtual_time: datetime


@dataclass(frozen=True)
class CompiledDayPlan:
    """단일 일자의 컴파일 완료된 실행 계획 (불변)"""
    day_offset: int
    calendar_date: date
    expected_samples: int
    planned_publish_samples: int
    planned_omitted_samples: int
    is_fully_omitted: bool
    start_cycle: int
    end_cycle: int
    events: tuple[ApplianceEvent, ...]
    omission_ranges: tuple[OmissionRange, ...]
    transitions: tuple[ApplianceTransition, ...]


@dataclass(frozen=True)
class CompiledExecutionPlan:
    """
    시뮬레이터 런타임이 즉시 실행할 수 있는 불변 최종 계획.
    242만 개 슬롯에 비례하는 메모리를 생성하지 않고, 산술 계산 및 Sparse Transition 매핑 사용.
    """
    scenario_id: str
    base_date: date
    start_date: date
    end_date: date
    first_cycle: int
    last_cycle: int
    total_virtual_slots: int
    total_planned_publish_samples: int
    total_planned_omitted_samples: int
    day_plans: tuple[CompiledDayPlan, ...]
    _transitions_by_cycle: MappingProxyType[int, tuple[ApplianceTransition, ...]]

    def should_publish(self, absolute_cycle: int) -> bool:
        """
        해당 가상 사이클이 실제 MQTT 발행 대상인지 산술 검사.
        시간 복잡도: O(R) (R은 해당 날짜의 omission_ranges 수, 통상 R <= 2).
        사이클 유효 범위 [first_cycle, last_cycle] 밖이면 CycleOutOfRangeError 발생.
        """
        if type(absolute_cycle) is not int or isinstance(absolute_cycle, bool):
            raise CycleOutOfRangeError(f"absolute_cycle은 정수여야 합니다: {absolute_cycle!r}")
        if not (self.first_cycle <= absolute_cycle <= self.last_cycle):
            raise CycleOutOfRangeError(
                f"absolute_cycle({absolute_cycle})이 유효 범위 [{self.first_cycle}, {self.last_cycle}]를 벗어났습니다."
            )
        relative_index = absolute_cycle - self.first_cycle
        day_index, second_of_day = divmod(relative_index, SECONDS_PER_DAY)
        day_plan = self.day_plans[day_index]
        for r in day_plan.omission_ranges:
            if r.start_second <= second_of_day < r.end_second:
                return False
        return True

    def virtual_time_at(self, absolute_cycle: int) -> datetime:
        """
        해당 가상 사이클의 정확한 KST datetime 산술 계산 (O(1)).
        사이클 유효 범위 [first_cycle, last_cycle] 밖이면 CycleOutOfRangeError 발생.
        """
        if type(absolute_cycle) is not int or isinstance(absolute_cycle, bool):
            raise CycleOutOfRangeError(f"absolute_cycle은 정수여야 합니다: {absolute_cycle!r}")
        if not (self.first_cycle <= absolute_cycle <= self.last_cycle):
            raise CycleOutOfRangeError(
                f"absolute_cycle({absolute_cycle})이 유효 범위 [{self.first_cycle}, {self.last_cycle}]를 벗어났습니다."
            )
        relative_index = absolute_cycle - self.first_cycle
        base_dt = datetime(self.start_date.year, self.start_date.month, self.start_date.day, 0, 0, 0, tzinfo=KST)
        return base_dt + timedelta(seconds=relative_index)

    def transitions_at(self, absolute_cycle: int) -> tuple[ApplianceTransition, ...]:
        """
        해당 가상 사이클에 발생할 상태 전환 이벤트 튜플 반환 (희소 매핑 O(1) 조회).
        사이클 유효 범위 [first_cycle, last_cycle] 밖이면 CycleOutOfRangeError 발생.
        """
        if type(absolute_cycle) is not int or isinstance(absolute_cycle, bool):
            raise CycleOutOfRangeError(f"absolute_cycle은 정수여야 합니다: {absolute_cycle!r}")
        if not (self.first_cycle <= absolute_cycle <= self.last_cycle):
            raise CycleOutOfRangeError(
                f"absolute_cycle({absolute_cycle})이 유효 범위 [{self.first_cycle}, {self.last_cycle}]를 벗어났습니다."
            )
        return self._transitions_by_cycle.get(absolute_cycle, ())

    @property
    def transitions_by_cycle(self) -> MappingProxyType[int, tuple[ApplianceTransition, ...]]:
        """외부 노출용 읽기 전용 매핑 프록시 (수정 시도 시 TypeError 발생)"""
        return self._transitions_by_cycle


# ==========================================
# 6. 공개 검증 및 컴파일 함수
# ==========================================
def parse_scenario_definition(raw_data: Mapping[str, Any]) -> ScenarioDefinition:
    """
    dict / YAML 파싱 딕셔너리로부터 알 수 없는 필드를 엄격 거절하고 ScenarioDefinition 객체 생성.
    """
    if not isinstance(raw_data, Mapping):
        raise ScheduleError(f"시나리오 정의는 매핑(dict) 구조여야 합니다: {type(raw_data).__name__}")

    # 최상위 필드 검사
    allowed_top_keys = {"scenario", "scenario_id", "days", "description"}
    extra_top = set(raw_data.keys()) - allowed_top_keys
    if extra_top:
        raise UnknownFieldError(f"알 수 없는 최상위 필드가 포함되어 있습니다: {sorted(extra_top)}")

    has_scenario_id = "scenario_id" in raw_data
    has_scenario = "scenario" in raw_data

    if has_scenario_id and has_scenario:
        raise ScheduleError("scenario_id와 scenario 필드는 동시에 지정할 수 없습니다.")
    elif has_scenario_id:
        scenario_id_raw = raw_data["scenario_id"]
    elif has_scenario:
        scenario_id_raw = raw_data["scenario"]
    else:
        raise InvalidScenarioIdError("scenario_id(또는 scenario) 필드는 필수입니다.")

    if not isinstance(scenario_id_raw, str) or isinstance(scenario_id_raw, bool):
        raise InvalidScenarioIdError(f"scenario_id는 문자열이어야 합니다: {type(scenario_id_raw).__name__}")

    if "description" in raw_data:
        description = raw_data["description"]
    else:
        description = ""

    days_raw = raw_data.get("days")
    if days_raw is None:
        raise ScheduleError("days 필드는 필수입니다.")
    if not isinstance(days_raw, (list, tuple)):
        raise ScheduleError(f"days는 리스트 형식이어야 합니다: {type(days_raw).__name__}")

    parsed_days = []
    allowed_day_keys = {"day_offset", "events", "omission_ranges", "publish_samples"}
    allowed_event_keys = {"appliance", "start_time", "duration_seconds"}
    allowed_omission_keys = {"start_second", "end_second"}

    for d_idx, day_dict in enumerate(days_raw):
        if not isinstance(day_dict, Mapping):
            raise ScheduleError(f"days[{d_idx}]는 매핑(dict) 구조여야 합니다: {type(day_dict).__name__}")
        extra_day = set(day_dict.keys()) - allowed_day_keys
        if extra_day:
            raise UnknownFieldError(f"days[{d_idx}]에 알 수 없는 필드가 포함되어 있습니다: {sorted(extra_day)}")

        if "day_offset" not in day_dict:
            raise DayOffsetSequenceError(f"days[{d_idx}]에 day_offset 필드가 누락되었습니다.")
        day_offset = day_dict["day_offset"]

        # events 파싱
        events_raw = day_dict.get("events", [])
        if not isinstance(events_raw, (list, tuple)):
            raise ScheduleError(f"days[{d_idx}].events는 리스트여야 합니다: {type(events_raw).__name__}")
        parsed_events = []
        for e_idx, ev_dict in enumerate(events_raw):
            if isinstance(ev_dict, ApplianceEvent):
                parsed_events.append(ev_dict)
            elif isinstance(ev_dict, Mapping):
                extra_ev = set(ev_dict.keys()) - allowed_event_keys
                if extra_ev:
                    raise UnknownFieldError(f"days[{d_idx}].events[{e_idx}]에 알 수 없는 필드가 포함되어 있습니다: {sorted(extra_ev)}")
                if "appliance" not in ev_dict or "start_time" not in ev_dict or "duration_seconds" not in ev_dict:
                    raise ScheduleError(f"days[{d_idx}].events[{e_idx}]에 필수 필드가 누락되었습니다.")
                parsed_events.append(
                    ApplianceEvent(
                        appliance=ev_dict["appliance"],
                        start_time=ev_dict["start_time"],
                        duration_seconds=ev_dict["duration_seconds"],
                    )
                )
            else:
                raise ScheduleError(f"days[{d_idx}].events[{e_idx}]는 dict 또는 ApplianceEvent여야 합니다: {type(ev_dict).__name__}")

        # omission_ranges 파싱
        omissions_raw = day_dict.get("omission_ranges", [])
        if not isinstance(omissions_raw, (list, tuple)):
            raise ScheduleError(f"days[{d_idx}].omission_ranges는 리스트여야 합니다: {type(omissions_raw).__name__}")
        parsed_omissions = []
        for o_idx, om_dict in enumerate(omissions_raw):
            if isinstance(om_dict, OmissionRange):
                parsed_omissions.append(om_dict)
            elif isinstance(om_dict, Mapping):
                extra_om = set(om_dict.keys()) - allowed_omission_keys
                if extra_om:
                    raise UnknownFieldError(f"days[{d_idx}].omission_ranges[{o_idx}]에 알 수 없는 필드가 포함되어 있습니다: {sorted(extra_om)}")
                if "start_second" not in om_dict or "end_second" not in om_dict:
                    raise ScheduleError(f"days[{d_idx}].omission_ranges[{o_idx}]에 필수 필드가 누락되었습니다.")
                parsed_omissions.append(
                    OmissionRange(
                        start_second=om_dict["start_second"],
                        end_second=om_dict["end_second"],
                    )
                )
            else:
                raise ScheduleError(f"days[{d_idx}].omission_ranges[{o_idx}]는 dict 또는 OmissionRange여야 합니다: {type(om_dict).__name__}")

        publish_samples = day_dict.get("publish_samples")

        parsed_days.append(
            DaySchedule(
                day_offset=day_offset,
                events=tuple(parsed_events),
                omission_ranges=tuple(parsed_omissions),
                publish_samples=publish_samples,
            )
        )

    return ScenarioDefinition(
        scenario_id=scenario_id_raw,
        days=tuple(parsed_days),
        description=description,
    )


def validate_scenario_definition(definition: ScenarioDefinition) -> None:
    """
    선언된 시나리오 정의의 모든 도메인 규칙을 사전 검증.
    (ScenarioDefinition.__post_init__ 및 DaySchedule.__post_init__의 모든 검증을 포괄)
    """
    if not isinstance(definition, ScenarioDefinition):
        raise ScheduleError(f"definition은 ScenarioDefinition 인스턴스여야 합니다: {type(definition).__name__}")
    # 재검증 트리거
    definition.__post_init__()
    for day in definition.days:
        day.__post_init__()


def compile_schedule(
    definition: ScenarioDefinition,
    base_date: date | str,
    start_cycle: int = 1,
) -> CompiledExecutionPlan:
    """
    선언적 시나리오 정의와 기준 날짜를 기반으로 결정론적(Deterministic) 불변 실행 계획 컴파일.
    - validate_scenario_definition을 호출하여 자체 재검증 수행
    - base_date 엄격 파싱 (datetime.datetime 명시적 거절)
    - start_cycle >= 1 정수 검증 (bool 거절)
    - O(D + E + R) 메모리 복잡도 유지
    """
    # 1. 정의 자체 재검증
    validate_scenario_definition(definition)

    # 2. base_date 엄격 검증
    # (Python에서 datetime은 date의 서브클래스이므로 type() 또는 isinstance() 순서 주의)
    if isinstance(base_date, datetime):
        raise ScheduleError(
            f"base_date는 datetime.date 객체이거나 'YYYY-MM-DD' 문자열이어야 합니다. (datetime.datetime 거절: {base_date!r})"
        )
    elif isinstance(base_date, date):
        validated_base_date = base_date
    elif isinstance(base_date, str):
        clean_date = base_date.strip()
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", clean_date):
            raise ScheduleError(f"올바르지 않은 base_date 형식입니다: '{base_date}'. YYYY-MM-DD 형식이어야 합니다.")
        try:
            validated_base_date = datetime.strptime(clean_date, "%Y-%m-%d").date()
        except ValueError as err:
            raise ScheduleError(f"존재하지 않는 base_date입니다: '{base_date}' ({err})")
    else:
        raise ScheduleError(f"base_date는 datetime.date 객체이거나 'YYYY-MM-DD' 문자열이어야 합니다: {type(base_date).__name__}")

    # 3. start_cycle 엄격 검증
    if type(start_cycle) is not int or isinstance(start_cycle, bool) or start_cycle < 1:
        raise ScheduleError(f"start_cycle은 1 이상의 정수여야 합니다: {start_cycle!r}")

    # 4. 일자 및 사이클 계산
    total_days = len(definition.days)
    total_virtual_slots = total_days * SECONDS_PER_DAY
    first_cycle = start_cycle
    last_cycle = start_cycle + total_virtual_slots - 1

    start_date = validated_base_date + timedelta(days=definition.days[0].day_offset)
    end_date = validated_base_date + timedelta(days=definition.days[-1].day_offset)

    compiled_day_plans: list[CompiledDayPlan] = []
    sparse_transitions: dict[int, list[ApplianceTransition]] = {}

    total_planned_publish = 0
    total_planned_omitted = 0
    current_day_start_cycle = start_cycle

    for day in definition.days:
        calendar_date = validated_base_date + timedelta(days=day.day_offset)
        current_day_end_cycle = current_day_start_cycle + SECONDS_PER_DAY - 1
        day_base_dt = datetime(calendar_date.year, calendar_date.month, calendar_date.day, 0, 0, 0, tzinfo=KST)

        day_transitions: list[ApplianceTransition] = []
        for ev in day.events:
            # ON 전환
            on_sec = ev.start_second
            on_cycle = current_day_start_cycle + on_sec
            on_time = day_base_dt + timedelta(seconds=on_sec)
            day_transitions.append(
                ApplianceTransition(
                    transition_type=TransitionType.ON,
                    appliance=ev.appliance,
                    second_of_day=on_sec,
                    absolute_cycle=on_cycle,
                    virtual_time=on_time,
                )
            )

            # OFF 전환
            off_sec = ev.end_second
            off_cycle = current_day_start_cycle + off_sec
            off_time = day_base_dt + timedelta(seconds=off_sec)
            day_transitions.append(
                ApplianceTransition(
                    transition_type=TransitionType.OFF,
                    appliance=ev.appliance,
                    second_of_day=off_sec,
                    absolute_cycle=off_cycle,
                    virtual_time=off_time,
                )
            )

        # 동일 초 전환 시 명시적 TRANSITION_PRIORITY 적용 (OFF=0, ON=1)
        day_transitions.sort(
            key=lambda t: (
                t.second_of_day,
                TRANSITION_PRIORITY[t.transition_type],
                t.appliance,
            )
        )

        # sparse 매핑에 등록
        for t in day_transitions:
            sparse_transitions.setdefault(t.absolute_cycle, []).append(t)

        omitted_in_day = sum(r.omitted_samples for r in day.omission_ranges)
        publish_in_day = SECONDS_PER_DAY - omitted_in_day
        total_planned_publish += publish_in_day
        total_planned_omitted += omitted_in_day

        compiled_day_plans.append(
            CompiledDayPlan(
                day_offset=day.day_offset,
                calendar_date=calendar_date,
                expected_samples=SECONDS_PER_DAY,
                planned_publish_samples=publish_in_day,
                planned_omitted_samples=omitted_in_day,
                is_fully_omitted=(publish_in_day == 0),
                start_cycle=current_day_start_cycle,
                end_cycle=current_day_end_cycle,
                events=day.events,
                omission_ranges=day.omission_ranges,
                transitions=tuple(day_transitions),
            )
        )

        current_day_start_cycle += SECONDS_PER_DAY

    # 희소 매핑을 불변 튜플 및 MappingProxyType으로 포장
    frozen_sparse_transitions = MappingProxyType(
        {c: tuple(trans_list) for c, trans_list in sparse_transitions.items()}
    )

    return CompiledExecutionPlan(
        scenario_id=definition.scenario_id,
        base_date=validated_base_date,
        start_date=start_date,
        end_date=end_date,
        first_cycle=first_cycle,
        last_cycle=last_cycle,
        total_virtual_slots=total_virtual_slots,
        total_planned_publish_samples=total_planned_publish,
        total_planned_omitted_samples=total_planned_omitted,
        day_plans=tuple(compiled_day_plans),
        _transitions_by_cycle=frozen_sparse_transitions,
    )
