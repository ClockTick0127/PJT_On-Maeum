from datetime import date, datetime, timezone
from decimal import Decimal

from sqlalchemy import select

from realtime_analysis.models import AnalysisDailyCompletion
from power_silver.completion import AnalysisCompletionRepository, DailyCompletionRecord


def _record(**updates) -> DailyCompletionRecord:
    values = {
        "household_id": "H001",
        "appliance_type": "KETTLE",
        "target_date": date(2026, 9, 19),
        "input_snapshot_id": "input-1",
        "analysis_run_id": "realtime-v1",
        "analysis_evidence_snapshot_id": "evidence-1",
        "session_manifest_set_id": "sessions-1",
        "analysis_status": "COMPLETE",
        "delivery_status": "COMPLETE",
        "coverage_ratio": Decimal("1.000000"),
        "max_unanalyzed_seconds": 0,
        "quality_policy_version": "policy-v1",
        "baseline_eligible": True,
        "completed_at": datetime.now(timezone.utc),
    }
    values.update(updates)
    return DailyCompletionRecord(**values)


def test_completion_reuses_equal_fact_and_revises_changed_input(session_factory) -> None:
    repository = AnalysisCompletionRepository(session_factory)
    first = repository.publish([_record()])[0]
    repeated = repository.publish([_record()])[0]
    revised = repository.publish([_record(input_snapshot_id="input-2")])[0]

    assert first == repeated
    assert revised != first
    with session_factory() as session:
        rows = session.scalars(
            select(AnalysisDailyCompletion).order_by(AnalysisDailyCompletion.revision)
        ).all()
        assert [(row.revision, row.input_snapshot_id) for row in rows] == [
            (1, "input-1"),
            (2, "input-2"),
        ]
