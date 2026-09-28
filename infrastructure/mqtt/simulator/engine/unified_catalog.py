"""
NILM 스마트홈 시뮬레이터 통합 E2E 시나리오 카탈로그

기존 3개 카탈로그(scenario_catalog, routine_scenario_catalog, baseline_scenario_catalog)의
10개 시나리오를 단일 불변 매핑으로 통합 조회합니다.
"""
from __future__ import annotations

from types import MappingProxyType
from typing import Any, Mapping

from .scenario_catalog import (
    ACTIVITY_SCENARIO_IDS,
    get_activity_scenario_catalog,
)
from .routine_scenario_catalog import (
    ROUTINE_SCENARIO_IDS,
    get_routine_scenario_catalog,
)
from .baseline_scenario_catalog import (
    BASELINE_SCENARIO_IDS,
    get_baseline_scenario_catalog,
)
from .schedule import ScenarioDefinition, SECONDS_PER_DAY


class UnknownScenarioError(ValueError):
    """카탈로그에 등록되지 않았거나 유효하지 않은 E2E 시나리오 ID 조회 시 발생하는 도메인 예외"""
    pass


# 1. 3개 카탈로그에서 정의 수집 및 중복 검증
_UNIFIED_SCENARIOS_INTERNAL: dict[str, ScenarioDefinition] = {}
_SCENARIO_CATEGORIES: dict[str, str] = {}

# 활동 시나리오 6종
for sc_id, defn in get_activity_scenario_catalog().items():
    if sc_id in _UNIFIED_SCENARIOS_INTERNAL:
        raise ValueError(f"시나리오 ID 중복 발견: {sc_id}")
    _UNIFIED_SCENARIOS_INTERNAL[sc_id] = defn
    _SCENARIO_CATEGORIES[sc_id] = "ACTIVITY"

# 루틴 변화 시나리오 3종
for sc_id, defn in get_routine_scenario_catalog().items():
    if sc_id in _UNIFIED_SCENARIOS_INTERNAL:
        raise ValueError(f"시나리오 ID 중복 발견: {sc_id}")
    _UNIFIED_SCENARIOS_INTERNAL[sc_id] = defn
    _SCENARIO_CATEGORIES[sc_id] = "ROUTINE"

# 기준선 시나리오 1종
for sc_id, defn in get_baseline_scenario_catalog().items():
    if sc_id in _UNIFIED_SCENARIOS_INTERNAL:
        raise ValueError(f"시나리오 ID 중복 발견: {sc_id}")
    _UNIFIED_SCENARIOS_INTERNAL[sc_id] = defn
    _SCENARIO_CATEGORIES[sc_id] = "BASELINE"

UNIFIED_SCENARIO_IDS: tuple[str, ...] = tuple(_UNIFIED_SCENARIOS_INTERNAL.keys())
_UNIFIED_SCENARIOS: Mapping[str, ScenarioDefinition] = MappingProxyType(_UNIFIED_SCENARIOS_INTERNAL)


def list_unified_scenario_ids() -> tuple[str, ...]:
    """등록된 10개 통합 시나리오 ID의 불변 튜플 반환 (순서 보장)"""
    return UNIFIED_SCENARIO_IDS


def get_unified_scenario_definition(scenario_id: str) -> ScenarioDefinition:
    """
    지정된 scenario_id에 해당하는 불변 ScenarioDefinition 반환.
    - scenario_id가 문자열이 아니거나 카탈로그에 없으면 UnknownScenarioError 발생
    """
    if type(scenario_id) is not str or isinstance(scenario_id, bool):
        raise UnknownScenarioError(
            f"scenario_id는 문자열이어야 합니다: {scenario_id!r} (type={type(scenario_id).__name__})"
        )
    if scenario_id not in _UNIFIED_SCENARIOS:
        raise UnknownScenarioError(
            f"알 수 없는 E2E 시나리오 ID입니다: {scenario_id!r}. "
            f"허용 목록: {list(UNIFIED_SCENARIO_IDS)}"
        )
    return _UNIFIED_SCENARIOS[scenario_id]


def get_unified_scenario_catalog() -> Mapping[str, ScenarioDefinition]:
    """통합 카탈로그 전체의 불변 읽기 전용 매핑(MappingProxyType) 반환"""
    return _UNIFIED_SCENARIOS


def get_scenario_category(scenario_id: str) -> str:
    """시나리오 카테고리 반환 ('ACTIVITY' | 'ROUTINE' | 'BASELINE')"""
    if scenario_id not in _SCENARIO_CATEGORIES:
        raise UnknownScenarioError(f"알 수 없는 E2E 시나리오 ID입니다: {scenario_id!r}")
    return _SCENARIO_CATEGORIES[scenario_id]


def get_scenario_api_summary(scenario_id: str) -> dict[str, Any]:
    """
    REST API 응답용 요약 메타데이터 생성 (description 및 AI 정답 라벨 미노출).
    """
    defn = get_unified_scenario_definition(scenario_id)
    category = get_scenario_category(scenario_id)
    total_days = len(defn.days)
    planned_virtual_slots = total_days * SECONDS_PER_DAY
    planned_publish_samples = sum(
        (d.publish_samples if d.publish_samples is not None else SECONDS_PER_DAY)
        for d in defn.days
    )
    return {
        "scenario_id": defn.scenario_id,
        "category": category,
        "total_days": total_days,
        "planned_virtual_slots": planned_virtual_slots,
        "planned_publish_samples": planned_publish_samples,
    }


def list_scenario_api_summaries() -> list[dict[str, Any]]:
    """모든 10개 시나리오의 API 요약 메타데이터 목록 반환"""
    return [get_scenario_api_summary(sc_id) for sc_id in UNIFIED_SCENARIO_IDS]
