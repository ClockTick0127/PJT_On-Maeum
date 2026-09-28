"""
NILM 스마트홈 시뮬레이터 20일 전자레인지 기준선 생성 시나리오 카탈로그

최근 28일 계산창(D-27 ~ D) 안에 20개의 VALID 날짜(D-19 ~ D)를 생성하고,
그중 17일에 전자레인지를 08:00부터 60초간 가동하는 BASELINE_MICROWAVE_20D 시나리오를 제공합니다.

- BASELINE_MICROWAVE_20D:
    day_offset 0~2 (3일간): 미사용 (events = ())
    day_offset 3~19 (17일간): microwave 08:00:00, 60초 가동

모든 20개 날짜는 86,400개 전수 발행(결측 없음)되며, 본 모듈은 순수 선언 계층으로
AI 기준선 정답 라벨(sample_days, active_days, probability, expected_until 등)을 포함하지 않습니다.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Mapping

from .schedule import (
    ApplianceEvent,
    DaySchedule,
    ScenarioDefinition,
)


class UnknownBaselineScenarioError(ValueError):
    """카탈로그에 등록되지 않았거나 유효하지 않은 기준선 시나리오 ID 조회 시 발생하는 도메인 예외"""
    pass


BASELINE_SCENARIO_IDS: tuple[str, ...] = (
    "BASELINE_MICROWAVE_20D",
)


def _build_baseline_microwave_20d() -> ScenarioDefinition:
    """
    20일(day_offset 0~19) 동안 첫 3일은 미사용, 이후 17일은 08:00에
    전자레인지 60초 이벤트를 배치한 ScenarioDefinition을 생성합니다.
    """
    days: list[DaySchedule] = []
    for offset in range(20):
        if offset < 3:
            events: tuple[ApplianceEvent, ...] = ()
        else:
            events = (
                ApplianceEvent(
                    appliance="microwave",
                    start_time="08:00:00",
                    duration_seconds=60,
                ),
            )
        day = DaySchedule(
            day_offset=offset,
            events=events,
            omission_ranges=(),
            publish_samples=86_400,
        )
        days.append(day)
    return ScenarioDefinition(
        scenario_id="BASELINE_MICROWAVE_20D",
        days=tuple(days),
        description="20일 전자레인지 첫 사용 일정: 첫 3일 미사용, 이후 17일 08:00 사용",
    )


_BASELINE_SCENARIOS_INTERNAL: dict[str, ScenarioDefinition] = {
    "BASELINE_MICROWAVE_20D": _build_baseline_microwave_20d(),
}

_BASELINE_SCENARIOS: Mapping[str, ScenarioDefinition] = MappingProxyType(_BASELINE_SCENARIOS_INTERNAL)


def list_baseline_scenario_ids() -> tuple[str, ...]:
    """등록된 기준선 시나리오 ID의 불변 튜플 반환 (순서 보장)"""
    return BASELINE_SCENARIO_IDS


def get_baseline_scenario_definition(scenario_id: str) -> ScenarioDefinition:
    """
    지정된 scenario_id에 해당하는 불변 ScenarioDefinition 반환.

    - scenario_id가 정확한 대문자 문자열이 아니거나 카탈로그에 없으면 UnknownBaselineScenarioError 발생
    - 공백 포함, 소문자, None, bool, 숫자 등은 일절 허용하지 않음
    """
    if type(scenario_id) is not str or isinstance(scenario_id, bool):
        raise UnknownBaselineScenarioError(
            f"scenario_id는 문자열이어야 합니다: {scenario_id!r} (type={type(scenario_id).__name__})"
        )
    if scenario_id not in _BASELINE_SCENARIOS:
        raise UnknownBaselineScenarioError(
            f"알 수 없는 기준선 시나리오 ID입니다: {scenario_id!r}. "
            f"허용 목록: {list(BASELINE_SCENARIO_IDS)}"
        )
    return _BASELINE_SCENARIOS[scenario_id]


def get_baseline_scenario_catalog() -> Mapping[str, ScenarioDefinition]:
    """카탈로그 전체의 불변 읽기 전용 매핑(MappingProxyType) 반환"""
    return _BASELINE_SCENARIOS
