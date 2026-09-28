"""
NILM 스마트홈 시뮬레이터 일일 활동 시나리오 카탈로그

실제 요구사항에 정의된 6개 단일 일자 활동 시나리오를 불변 선언 데이터로 제공합니다.
- ACTIVITY_NORMAL: 정상 일상 활동 (6회 가전 사용, 3종 가전, 2160초)
- ACTIVITY_LOW: 활동 저하 (12:00 전자레인지 1회 180초)
- ACTIVITY_NONE: 무활동 일상 (대상 가전 0회, 86400개 전수 발행)
- ACTIVITY_INSUFFICIENT: 유효 관측률 95% 미만 결측 (82079건 발행, 4321건 결측)
- ACTIVITY_SESSION_MERGE: 세션 병합 대상 인덕션 (10:00 600초, 10:11 600초, 60초 간격)
- ACTIVITY_DURATION_CAP: 활동 점수 상한 적용 대상 전기주전자 (10:00 7200초)

본 모듈은 순수 선언 계층으로 물리 연산이나 AI 정답 라벨(activity_index, score 등)을 포함하지 않습니다.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Mapping

from .schedule import (
    ApplianceEvent,
    DaySchedule,
    OmissionRange,
    ScenarioDefinition,
)

class UnknownActivityScenarioError(ValueError):
    """카탈로그에 등록되지 않았거나 유효하지 않은 활동 시나리오 ID 조회 시 발생하는 도메인 예외"""
    pass

ACTIVITY_SCENARIO_IDS: tuple[str, ...] = (
    "ACTIVITY_NORMAL",
    "ACTIVITY_LOW",
    "ACTIVITY_NONE",
    "ACTIVITY_INSUFFICIENT",
    "ACTIVITY_SESSION_MERGE",
    "ACTIVITY_DURATION_CAP",
)

_ACTIVITY_SCENARIOS_INTERNAL: dict[str, ScenarioDefinition] = {
    "ACTIVITY_NORMAL": ScenarioDefinition(
        scenario_id="ACTIVITY_NORMAL",
        days=(
            DaySchedule(
                day_offset=0,
                events=(
                    ApplianceEvent(appliance="kettle", start_time="07:00:00", duration_seconds=120),
                    ApplianceEvent(appliance="microwave", start_time="09:00:00", duration_seconds=300),
                    ApplianceEvent(appliance="kettle", start_time="12:00:00", duration_seconds=120),
                    ApplianceEvent(appliance="vacuum_cleaner", start_time="15:00:00", duration_seconds=1200),
                    ApplianceEvent(appliance="microwave", start_time="18:00:00", duration_seconds=300),
                    ApplianceEvent(appliance="kettle", start_time="21:00:00", duration_seconds=120),
                ),
                omission_ranges=(),
                publish_samples=86_400,
            ),
        ),
        description="정상 일상 활동 시나리오 (6회 가전 사용, 3종 가전, 총 2160초 가동)",
    ),
    "ACTIVITY_LOW": ScenarioDefinition(
        scenario_id="ACTIVITY_LOW",
        days=(
            DaySchedule(
                day_offset=0,
                events=(
                    ApplianceEvent(appliance="microwave", start_time="12:00:00", duration_seconds=180),
                ),
                omission_ranges=(),
                publish_samples=86_400,
            ),
        ),
        description="활동 저하 시나리오 (12:00 전자레인지 1회 180초 가동)",
    ),
    "ACTIVITY_NONE": ScenarioDefinition(
        scenario_id="ACTIVITY_NONE",
        days=(
            DaySchedule(
                day_offset=0,
                events=(),
                omission_ranges=(),
                publish_samples=86_400,
            ),
        ),
        description="무활동 일상 시나리오 (대상 가전 0회, 기저부하 및 냉장고 정상 가동)",
    ),
    "ACTIVITY_INSUFFICIENT": ScenarioDefinition(
        scenario_id="ACTIVITY_INSUFFICIENT",
        days=(
            DaySchedule(
                day_offset=0,
                events=(),
                omission_ranges=(
                    OmissionRange(start_second=82_079, end_second=86_400),
                ),
                publish_samples=82_079,
            ),
        ),
        description="측정 샘플 부족 시나리오 (82079건 발행, 22:47:59부터 4321건 결측)",
    ),
    "ACTIVITY_SESSION_MERGE": ScenarioDefinition(
        scenario_id="ACTIVITY_SESSION_MERGE",
        days=(
            DaySchedule(
                day_offset=0,
                events=(
                    ApplianceEvent(appliance="induction", start_time="10:00:00", duration_seconds=600),
                    ApplianceEvent(appliance="induction", start_time="10:11:00", duration_seconds=600),
                ),
                omission_ranges=(),
                publish_samples=86_400,
            ),
        ),
        description="세션 병합 대상 인덕션 시나리오 (10:00 600초, 10:11 600초, 60초 간격)",
    ),
    "ACTIVITY_DURATION_CAP": ScenarioDefinition(
        scenario_id="ACTIVITY_DURATION_CAP",
        days=(
            DaySchedule(
                day_offset=0,
                events=(
                    ApplianceEvent(appliance="kettle", start_time="10:00:00", duration_seconds=7200),
                ),
                omission_ranges=(),
                publish_samples=86_400,
            ),
        ),
        description="점수 상한 적용 대상 전기주전자 장시간 가동 시나리오 (10:00부터 7200초)",
    ),
}

_ACTIVITY_SCENARIOS: Mapping[str, ScenarioDefinition] = MappingProxyType(_ACTIVITY_SCENARIOS_INTERNAL)

def list_activity_scenario_ids() -> tuple[str, ...]:
    """등록된 6개 활동 시나리오 ID의 불변 튜플 반환 (순서 보장)"""
    return ACTIVITY_SCENARIO_IDS

def get_activity_scenario_definition(scenario_id: str) -> ScenarioDefinition:
    """
    지정된 scenario_id에 해당하는 불변 ScenarioDefinition 반환.

    - scenario_id가 정확한 대문자 문자열이 아니거나 카탈로그에 없으면 UnknownActivityScenarioError 발생
    - 공백 포함, 소문자, None, bool, 숫자 등은 일절 허용하지 않음
    """
    if type(scenario_id) is not str or isinstance(scenario_id, bool):
        raise UnknownActivityScenarioError(
            f"scenario_id는 문자열이어야 합니다: {scenario_id!r} (type={type(scenario_id).__name__})"
        )
    if scenario_id not in _ACTIVITY_SCENARIOS:
        raise UnknownActivityScenarioError(
            f"알 수 없는 활동 시나리오 ID입니다: {scenario_id!r}. "
            f"허용 목록: {list(ACTIVITY_SCENARIO_IDS)}"
        )
    return _ACTIVITY_SCENARIOS[scenario_id]

def get_activity_scenario_catalog() -> Mapping[str, ScenarioDefinition]:
    """카탈로그 전체의 불변 읽기 전용 매핑(MappingProxyType) 반환"""
    return _ACTIVITY_SCENARIOS
