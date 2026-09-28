"""
NILM 스마트홈 시뮬레이터 통합 E2E 시나리오 카탈로그(unified_catalog.py) 단위 테스트

검증 내용:
1. 10개 시나리오 정확히 등록 및 ID 고유성
2. MappingProxyType 불변 매핑 보장
3. 카테고리 매핑 (ACTIVITY 6종, ROUTINE 3종, BASELINE 1종)
4. 일수 및 planned count 산술 검증
5. API 요약 메타데이터에서 description 및 AI 정답 라벨 미노출
6. 유효하지 않은 시나리오 ID 조회 시 UnknownScenarioError 발생
"""
from __future__ import annotations

import unittest
from types import MappingProxyType

from engine.unified_catalog import (
    UNIFIED_SCENARIO_IDS,
    UnknownScenarioError,
    list_unified_scenario_ids,
    get_unified_scenario_definition,
    get_unified_scenario_catalog,
    get_scenario_category,
    get_scenario_api_summary,
    list_scenario_api_summaries,
)
from engine.schedule import ScenarioDefinition, SECONDS_PER_DAY


class TestUnifiedCatalog(unittest.TestCase):
    """통합 E2E 시나리오 카탈로그 단위 테스트"""

    def test_unified_catalog_contains_exact_10_scenarios(self):
        """카탈로그에 정확히 10개 시나리오가 등록되어 있는지 확인"""
        ids = list_unified_scenario_ids()
        self.assertIsInstance(ids, tuple)
        self.assertEqual(len(ids), 10)
        self.assertEqual(len(UNIFIED_SCENARIO_IDS), 10)

    def test_unified_catalog_no_duplicate_scenario_ids(self):
        """시나리오 ID에 중복이 없는지 확인"""
        ids = list_unified_scenario_ids()
        self.assertEqual(len(ids), len(set(ids)))

    def test_unified_catalog_is_immutable_mapping(self):
        """카탈로그가 불변 MappingProxyType으로 보호되는지 확인"""
        cat = get_unified_scenario_catalog()
        self.assertIsInstance(cat, MappingProxyType)
        with self.assertRaises(TypeError):
            cat["NEW_SCENARIO"] = None  # type: ignore[index]

    def test_category_mapping(self):
        """시나리오별 카테고리가 정확히 매핑되는지 확인"""
        activity_ids = [
            "ACTIVITY_NORMAL", "ACTIVITY_LOW", "ACTIVITY_NONE",
            "ACTIVITY_INSUFFICIENT", "ACTIVITY_SESSION_MERGE", "ACTIVITY_DURATION_CAP"
        ]
        routine_ids = [
            "ROUTINE_CHANGED_LATER", "ROUTINE_CHANGED_EARLIER", "ROUTINE_CHANGED_WITHIN_THRESHOLD"
        ]
        baseline_ids = [
            "BASELINE_MICROWAVE_20D"
        ]

        for sc_id in activity_ids:
            self.assertEqual(get_scenario_category(sc_id), "ACTIVITY")
        for sc_id in routine_ids:
            self.assertEqual(get_scenario_category(sc_id), "ROUTINE")
        for sc_id in baseline_ids:
            self.assertEqual(get_scenario_category(sc_id), "BASELINE")

    def test_planned_counts_arithmetic(self):
        """시나리오별 total_days, planned_virtual_slots, planned_publish_samples 산술 검증"""
        # 1. 1일 활동 정상
        summary_normal = get_scenario_api_summary("ACTIVITY_NORMAL")
        self.assertEqual(summary_normal["total_days"], 1)
        self.assertEqual(summary_normal["planned_virtual_slots"], 86400)
        self.assertEqual(summary_normal["planned_publish_samples"], 86400)

        # 2. 1일 활동 결측 (4321건 결측)
        summary_insufficient = get_scenario_api_summary("ACTIVITY_INSUFFICIENT")
        self.assertEqual(summary_insufficient["total_days"], 1)
        self.assertEqual(summary_insufficient["planned_virtual_slots"], 86400)
        self.assertEqual(summary_insufficient["planned_publish_samples"], 82079)

        # 3. 28일 루틴
        summary_routine = get_scenario_api_summary("ROUTINE_CHANGED_LATER")
        self.assertEqual(summary_routine["total_days"], 28)
        self.assertEqual(summary_routine["planned_virtual_slots"], 28 * 86400)
        self.assertEqual(summary_routine["planned_publish_samples"], 28 * 86400)

        # 4. 20일 기준선
        summary_baseline = get_scenario_api_summary("BASELINE_MICROWAVE_20D")
        self.assertEqual(summary_baseline["total_days"], 20)
        self.assertEqual(summary_baseline["planned_virtual_slots"], 20 * 86400)
        self.assertEqual(summary_baseline["planned_publish_samples"], 20 * 86400)

    def test_api_summary_excludes_description_and_ai_labels(self):
        """API 요약 응답에 description 및 AI 정답 라벨이 포함되지 않는지 검증"""
        summaries = list_scenario_api_summaries()
        self.assertEqual(len(summaries), 10)
        allowed_keys = {
            "scenario_id",
            "category",
            "total_days",
            "planned_virtual_slots",
            "planned_publish_samples",
        }
        for item in summaries:
            self.assertEqual(set(item.keys()), allowed_keys)
            self.assertNotIn("description", item)
            self.assertNotIn("shift_minutes", item)
            self.assertNotIn("probability", item)
            self.assertNotIn("expected_until", item)

    def test_unknown_scenario_error(self):
        """유효하지 않은 시나리오 ID 조회 시 UnknownScenarioError 발생 검증"""
        with self.assertRaises(UnknownScenarioError):
            get_unified_scenario_definition("NON_EXISTENT_SCENARIO")
        with self.assertRaises(UnknownScenarioError):
            get_unified_scenario_definition(123)  # type: ignore[arg-type]
        with self.assertRaises(UnknownScenarioError):
            get_unified_scenario_definition(None)  # type: ignore[arg-type]
        with self.assertRaises(UnknownScenarioError):
            get_scenario_category("INVALID_SCENARIO")


if __name__ == "__main__":
    unittest.main()
