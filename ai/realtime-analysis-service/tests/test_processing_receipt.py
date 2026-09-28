from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from realtime_analysis.activity_repository import SqlAlchemyApplianceActivityRepository
from realtime_analysis.database import Base
from realtime_analysis.models import AnalysisProcessingReceipt, AnalysisReceiptLakeOutbox
from realtime_analysis.processing_receipt import ProcessingReceipt
from realtime_analysis.schemas import (
    AnalysisProcessingOutcome,
    ApplianceState,
    PowerMeasurement,
    ProcessingSource,
)


def _factory() -> sessionmaker[Session]:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    for name in (
        "household_observation_daily",
        "household_activity_daily",
        "appliance_usage_session",
        "analysis_processing_receipt",
        "analysis_receipt_lake_batch",
        "analysis_receipt_lake_outbox",
    ):
        Base.metadata.tables[name].create(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


def _measurement() -> PowerMeasurement:
    return PowerMeasurement(
        message_id=UUID("8f3b2a19-4d6e-4c72-9b12-a1b2c3d4e5f6"),
        household_id="H001",
        device_id="main",
        measured_at=datetime.fromisoformat("2026-09-20T01:02:03+09:00"),
        active_power=100,
        reactive_power=10,
        power_factor=0.98,
        current=0.5,
    )


def _receipt(outcome: AnalysisProcessingOutcome) -> ProcessingReceipt:
    return ProcessingReceipt.for_measurement(
        _measurement(),
        analysis_run_id="realtime-v1",
        model_version="m1",
        pipeline_version="1",
        state_epoch=uuid4(),
        outcome=outcome,
        source=ProcessingSource(topic="power.raw.v1", partition=2, offset=31),
        appliance_types=("MICROWAVE",),
    )


def test_success_without_session_change_is_durable_and_deliverable() -> None:
    factory = _factory()
    repository = SqlAlchemyApplianceActivityRepository(factory, "Asia/Seoul")
    repository.record(
        "H001",
        _measurement().measured_at,
        [
            ApplianceState(
                appliance_type="MICROWAVE", probability=0.1, threshold=0.5, is_on=False
            )
        ],
        [],
        set(),
        processing_receipt=_receipt(AnalysisProcessingOutcome.SUCCEEDED),
    )

    with factory() as session:
        receipt = session.scalar(select(AnalysisProcessingReceipt))
        outbox = session.scalar(select(AnalysisReceiptLakeOutbox))
        assert receipt is not None
        assert receipt.outcome == "SUCCEEDED"
        assert receipt.session_change_refs == []
        assert outbox is not None
        assert outbox.delivery_status == "PENDING"
        assert outbox.payload["session_change_refs"] == []


def test_failed_attempt_is_preserved_before_later_success() -> None:
    factory = _factory()
    repository = SqlAlchemyApplianceActivityRepository(factory, "Asia/Seoul")
    repository.record_processing_outcome(_receipt(AnalysisProcessingOutcome.FAILED_INFERENCE))
    repository.record(
        "H001",
        _measurement().measured_at,
        [],
        [],
        set(),
        processing_receipt=_receipt(AnalysisProcessingOutcome.SUCCEEDED),
    )

    with factory() as session:
        rows = session.scalars(
            select(AnalysisProcessingReceipt).order_by(AnalysisProcessingReceipt.attempt)
        ).all()
        assert [(row.attempt, row.outcome) for row in rows] == [
            (1, "FAILED_INFERENCE"),
            (2, "SUCCEEDED"),
        ]
