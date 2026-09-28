from datetime import datetime, timezone
from uuid import uuid4

import pyarrow.parquet as pq
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from realtime_analysis.database import Base
from realtime_analysis.models import (
    AnalysisProcessingReceipt,
    AnalysisReceiptLakeBatch,
    AnalysisReceiptLakeOutbox,
)
from session_lake_loader.receipt_lake import AnalysisReceiptLakeLoader
from session_lake_loader.storage import LocalLakeStorage


def test_receipt_batch_writes_manifest_before_delivery_ack(tmp_path) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    for name in (
        "analysis_processing_receipt",
        "analysis_receipt_lake_batch",
        "analysis_receipt_lake_outbox",
    ):
        Base.metadata.tables[name].create(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    receipt_id = uuid4()
    message_id = uuid4()
    now = datetime.now(timezone.utc)
    payload = {
        "receipt_id": str(receipt_id),
        "message_id": str(message_id),
        "household_id": "H001",
        "device_id": "main",
        "source_topic": "power.raw.v1",
        "source_partition": 0,
        "source_offset": 10,
        "measured_at": now.isoformat(),
        "processed_at": now.isoformat(),
        "analysis_run_id": "realtime-v1",
        "attempt": 1,
        "model_version": "m1",
        "pipeline_version": "1",
        "state_epoch": str(uuid4()),
        "outcome": "SUCCEEDED",
        "appliance_types": ["MICROWAVE"],
        "session_change_refs": [],
        "error_type": None,
    }
    with factory.begin() as session:
        session.add(
            AnalysisProcessingReceipt(
                receipt_id=receipt_id,
                message_id=message_id,
                household_id="H001",
                device_id="main",
                measured_at=now,
                processed_at=now,
                analysis_run_id="realtime-v1",
                attempt=1,
                model_version="m1",
                pipeline_version="1",
                state_epoch=uuid4(),
                outcome="SUCCEEDED",
                appliance_types=["MICROWAVE"],
                session_change_refs=[],
            )
        )
        session.add(
            AnalysisReceiptLakeOutbox(
                receipt_id=receipt_id,
                payload=payload,
                changed_at=now,
                delivery_status="PENDING",
            )
        )

    storage = LocalLakeStorage(tmp_path)
    result = AnalysisReceiptLakeLoader(factory, storage).run_once()
    assert result is not None
    assert result.status == "COMPLETED"
    assert storage.exists(result.manifest_path)

    with factory() as session:
        outbox = session.scalar(select(AnalysisReceiptLakeOutbox))
        batch = session.scalar(select(AnalysisReceiptLakeBatch))
        assert outbox is not None and outbox.delivery_status == "DELIVERED"
        assert batch is not None and batch.status == "COMPLETED"
        part = (
            f"/nilm/bronze/analysis-processing-receipt/ingest_date={batch.ingest_date}"
            f"/batch_id={batch.batch_id}/part-00000.parquet"
        )
    table = pq.read_table(storage.resolve(part))
    assert table.num_rows == 1
    assert table.column("outcome").to_pylist() == ["SUCCEEDED"]
