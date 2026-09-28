"""
NILM 스마트홈 시뮬레이터 28일 루틴 변화 시나리오 카탈로그

기준 날짜 D를 기준으로 이전 21일(D-27 ~ D-7)과 최근 7일(D-6 ~ D)의
전자레인지 첫 사용 시각을 결정적으로 생성하는 3개 시나리오를 불변 선언 데이터로 제공합니다.

- ROUTINE_CHANGED_LATER:
    이전 21일 08:00 (60초) -> 최근 7일 10:30 (60초), shift = +150분 (LATER)
- ROUTINE_CHANGED_EARLIER:
    이전 21일 10:30 (60초) -> 최근 7일 08:00 (60초), shift = -150분 (EARLIER)
- ROUTINE_CHANGED_WITHIN_THRESHOLD:
    이전 21일 08:00 (60초) -> 최근 7일 09:30 (60초), shift = +90분 (임계값 120분 미만)

모든 날짜는 86,400개 전수 발행(결측 없음)되며, 매일 60초 전자레인지 이벤트 1회로 구성됩니다.
본 모듈은 순수 선언 계층으로 AI 정답 라벨(event_type, direction, shift_minutes 등)을 포함하지 않습니다.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Mapping

from .schedule import (
    ApplianceEvent,
    DaySchedule,
    ScenarioDefinition,
)


class UnknownRoutineScenarioError(ValueError):
    """카탈로그에 등록되지 않았거나 유효하지 않은 루틴 시나리오 ID 조회 시 발생하는 도메인 예외"""
    pass


ROUTINE_SCENARIO_IDS: tuple[str, ...] = (
    "ROUTINE_CHANGED_LATER",
    "ROUTINE_CHANGED_EARLIER",
    "ROUTINE_CHANGED_WITHIN_THRESHOLD",
)


def _build_routine_scenario(
    scenario_id: str,
    reference_time: str,
    recent_time: str,
    description: str,
) -> ScenarioDefinition:
    """
    28일(day_offset 0~27) 동안 이전 21일(0~20)과 최근 7일(21~27)의
    전자레인지 사용 시각을 결정적으로 배치한 ScenarioDefinition을 생성합니다.
    """
    days: list[DaySchedule] = []
    for offset in range(28):
        start_time = reference_time if offset < 21 else recent_time
        event = ApplianceEvent(
            appliance="microwave",
            start_time=start_time,
            duration_seconds=60,
        )
        day = DaySchedule(
            day_offset=offset,
            events=(event,),
            omission_ranges=(),
            publish_samples=86_400,
        )
        days.append(day)
    return ScenarioDefinition(
        scenario_id=scenario_id,
        days=tuple(days),
        description=description,
    )


_ROUTINE_SCENARIOS_INTERNAL: dict[str, ScenarioDefinition] = {
    "ROUTINE_CHANGED_LATER": _build_routine_scenario(
        scenario_id="ROUTINE_CHANGED_LATER",
        reference_time="08:00:00",
        recent_time="10:30:00",
        description="28일 전자레인지 첫 사용 일정: 이전 21일 08:00, 최근 7일 10:30",
    ),
    "ROUTINE_CHANGED_EARLIER": _build_routine_scenario(
        scenario_id="ROUTINE_CHANGED_EARLIER",
        reference_time="10:30:00",
        recent_time="08:00:00",
        description="28일 전자레인지 첫 사용 일정: 이전 21일 10:30, 최근 7일 08:00",
    ),
    "ROUTINE_CHANGED_WITHIN_THRESHOLD": _build_routine_scenario(
        scenario_id="ROUTINE_CHANGED_WITHIN_THRESHOLD",
        reference_time="08:00:00",
        recent_time="09:30:00",
        description="28일 전자레인지 첫 사용 일정: 이전 21일 08:00, 최근 7일 09:30",
    ),
}

_ROUTINE_SCENARIOS: Mapping[str, ScenarioDefinition] = MappingProxyType(_ROUTINE_SCENARIOS_INTERNAL)


def list_routine_scenario_ids() -> tuple[str, ...]:
    """등록된 3개 루틴 시나리오 ID의 불변 튜플 반환 (순서 보장)"""
    return ROUTINE_SCENARIO_IDS


def get_routine_scenario_definition(scenario_id: str) -> ScenarioDefinition:
    """
    지정된 scenario_id에 해당하는 불변 ScenarioDefinition 반환.

    - scenario_id가 정확한 대문자 문자열이 아니거나 카탈로그에 없으면 UnknownRoutineScenarioError 발생
    - 공백 포함, 소문자, None, bool, 숫자 등은 일절 허용하지 않음
    """
    if type(scenario_id) is not str or isinstance(scenario_id, bool):
        raise UnknownRoutineScenarioError(
            f"scenario_id는 문자열이어야 합니다: {scenario_id!r} (type={type(scenario_id).__name__})"
        )
    if scenario_id not in _ROUTINE_SCENARIOS:
        raise UnknownRoutineScenarioError(
            f"알 수 없는 루틴 시나리오 ID입니다: {scenario_id!r}. "
            f"허용 목록: {list(ROUTINE_SCENARIO_IDS)}"
        )
    return _ROUTINE_SCENARIOS[scenario_id]


def get_routine_scenario_catalog() -> Mapping[str, ScenarioDefinition]:
    """카탈로그 전체의 불변 읽기 전용 매핑(MappingProxyType) 반환"""
    return _ROUTINE_SCENARIOS
