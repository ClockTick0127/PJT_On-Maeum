"""Contracts shared by the handler and transactional repository."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from uuid import UUID, uuid4

from realtime_analysis.schemas import AnalysisProcessingOutcome, PowerMeasurement, ProcessingSource


@dataclass(frozen=True)
class ProcessingReceipt:
    message_id: UUID
    household_id: str
    device_id: str
    measured_at: datetime
    analysis_run_id: str
    model_version: str
    pipeline_version: str
    state_epoch: UUID
    outcome: AnalysisProcessingOutcome
    source: ProcessingSource = field(default_factory=ProcessingSource)
    appliance_types: tuple[str, ...] = ()
    error_type: str | None = None
    receipt_id: UUID = field(default_factory=uuid4)
    processed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @classmethod
    def for_measurement(
        cls,
        measurement: PowerMeasurement,
        *,
        analysis_run_id: str,
        model_version: str,
        pipeline_version: str,
        state_epoch: UUID,
        outcome: AnalysisProcessingOutcome,
        source: ProcessingSource | None = None,
        appliance_types: tuple[str, ...] = (),
        error_type: str | None = None,
    ) -> "ProcessingReceipt":
        return cls(
            message_id=measurement.message_id,
            household_id=measurement.household_id,
            device_id=measurement.device_id,
            measured_at=measurement.measured_at,
            analysis_run_id=analysis_run_id,
            model_version=model_version,
            pipeline_version=pipeline_version,
            state_epoch=state_epoch,
            outcome=outcome,
            source=source or ProcessingSource(),
            appliance_types=appliance_types,
            error_type=error_type,
        )
