"""Revisioned persistence for daily analysis completion declarations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from realtime_analysis.models import AnalysisDailyCompletion


@dataclass(frozen=True)
class DailyCompletionRecord:
    household_id: str
    appliance_type: str
    target_date: date
    input_snapshot_id: str
    analysis_run_id: str
    analysis_evidence_snapshot_id: str
    session_manifest_set_id: str
    analysis_status: str
    delivery_status: str
    coverage_ratio: Decimal
    max_unanalyzed_seconds: int
    quality_policy_version: str
    baseline_eligible: bool
    completed_at: datetime


class AnalysisCompletionRepository:
    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def publish(self, records: list[DailyCompletionRecord]) -> list[UUID]:
        """Append revisions, reusing the latest row when every declared fact is equal."""

        completion_ids: list[UUID] = []
        with self._session_factory.begin() as session:
            for record in records:
                latest = session.scalar(
                    select(AnalysisDailyCompletion)
                    .where(
                        AnalysisDailyCompletion.household_id == record.household_id,
                        AnalysisDailyCompletion.appliance_type == record.appliance_type,
                        AnalysisDailyCompletion.target_date == record.target_date,
                    )
                    .order_by(AnalysisDailyCompletion.revision.desc())
                    .limit(1)
                    .with_for_update()
                )
                if latest is not None and self._same(latest, record):
                    completion_ids.append(latest.completion_id)
                    continue
                row = AnalysisDailyCompletion(
                    household_id=record.household_id,
                    appliance_type=record.appliance_type,
                    target_date=record.target_date,
                    revision=1 if latest is None else latest.revision + 1,
                    input_snapshot_id=record.input_snapshot_id,
                    analysis_run_id=record.analysis_run_id,
                    analysis_evidence_snapshot_id=record.analysis_evidence_snapshot_id,
                    session_manifest_set_id=record.session_manifest_set_id,
                    analysis_status=record.analysis_status,
                    delivery_status=record.delivery_status,
                    coverage_ratio=record.coverage_ratio,
                    max_unanalyzed_seconds=record.max_unanalyzed_seconds,
                    quality_policy_version=record.quality_policy_version,
                    baseline_eligible=record.baseline_eligible,
                    completed_at=record.completed_at,
                )
                session.add(row)
                session.flush()
                completion_ids.append(row.completion_id)
        return completion_ids

    @staticmethod
    def _same(row: AnalysisDailyCompletion, record: DailyCompletionRecord) -> bool:
        return (
            row.input_snapshot_id == record.input_snapshot_id
            and row.analysis_run_id == record.analysis_run_id
            and row.analysis_evidence_snapshot_id == record.analysis_evidence_snapshot_id
            and row.session_manifest_set_id == record.session_manifest_set_id
            and row.analysis_status == record.analysis_status
            and row.delivery_status == record.delivery_status
            and row.coverage_ratio == record.coverage_ratio
            and row.max_unanalyzed_seconds == record.max_unanalyzed_seconds
            and row.quality_policy_version == record.quality_policy_version
            and row.baseline_eligible == record.baseline_eligible
        )
