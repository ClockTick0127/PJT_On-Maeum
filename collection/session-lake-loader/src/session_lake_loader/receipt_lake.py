"""Incremental Bronze delivery for per-input analysis receipts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import io
import json
from uuid import UUID, uuid4

import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from realtime_analysis.models import AnalysisReceiptLakeBatch, AnalysisReceiptLakeOutbox
from session_lake_loader.lake_schema import normalize_timestamp, sha256_hex
from session_lake_loader.storage import LakeStorage, parent_of


RECEIPT_SCHEMA_VERSION = 1
RECEIPT_SCHEMA = pa.schema(
    [
        ("event_id", pa.int64()),
        ("receipt_id", pa.string()),
        ("message_id", pa.string()),
        ("household_id", pa.string()),
        ("device_id", pa.string()),
        ("source_topic", pa.string()),
        ("source_partition", pa.int32()),
        ("source_offset", pa.int64()),
        ("measured_at", pa.timestamp("us", tz="UTC")),
        ("processed_at", pa.timestamp("us", tz="UTC")),
        ("analysis_run_id", pa.string()),
        ("attempt", pa.int32()),
        ("model_version", pa.string()),
        ("pipeline_version", pa.string()),
        ("state_epoch", pa.string()),
        ("outcome", pa.string()),
        ("appliance_types", pa.list_(pa.string())),
        ("session_change_refs_json", pa.string()),
        ("error_type", pa.string()),
        ("batch_id", pa.string()),
        ("schema_version", pa.int32()),
    ]
)


@dataclass(frozen=True)
class ReceiptLoadResult:
    batch_id: str
    status: str
    row_count: int
    manifest_path: str


class AnalysisReceiptLakeLoader:
    """Assign, persist, and acknowledge receipt outbox rows as one manifest batch."""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        storage: LakeStorage,
        *,
        bronze_base: str = "/nilm/bronze/analysis-processing-receipt",
        manifest_base: str = "/nilm/manifests/job=analysis-receipt-lake-loader",
    ) -> None:
        self._session_factory = session_factory
        self._storage = storage
        self._bronze_base = bronze_base.rstrip("/")
        self._manifest_base = manifest_base.rstrip("/")

    def run_once(self, limit: int = 5000) -> ReceiptLoadResult | None:
        batch = self._resume_batch() or self._assign_batch(limit)
        if batch is None:
            return None
        try:
            rows = self._batch_rows(batch.batch_id)
            data = self._parquet(rows, batch.batch_id)
            part_path = self._part_path(batch)
            manifest = self._manifest(batch, part_path, data, len(rows))
            if not self._storage.exists(batch.manifest_path):
                self._write_once(part_path, data)
                self._write_once(
                    batch.manifest_path,
                    json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
                )
            else:
                self._verify_manifest(batch.manifest_path)
            self._complete(batch.batch_id)
            return ReceiptLoadResult(
                str(batch.batch_id), "COMPLETED", len(rows), batch.manifest_path
            )
        except Exception as error:
            self._fail(batch.batch_id, error)
            raise

    def _assign_batch(self, limit: int) -> AnalysisReceiptLakeBatch | None:
        if limit < 1:
            raise ValueError("limit must be at least 1")
        now = datetime.now(timezone.utc)
        batch_id = uuid4()
        manifest = (
            f"{self._manifest_base}/ingest_date={now.date().isoformat()}"
            f"/batch_id={batch_id}/manifest.json"
        )
        with self._session_factory.begin() as session:
            events = session.scalars(
                select(AnalysisReceiptLakeOutbox)
                .where(AnalysisReceiptLakeOutbox.delivery_status == "PENDING")
                .order_by(AnalysisReceiptLakeOutbox.event_id)
                .limit(limit)
                .with_for_update(skip_locked=True)
            ).all()
            if not events:
                return None
            batch = AnalysisReceiptLakeBatch(
                batch_id=batch_id,
                status="ASSIGNED",
                ingest_date=now.date(),
                event_count=len(events),
                manifest_path=manifest,
                created_at=now,
            )
            session.add(batch)
            for event in events:
                event.batch_id = batch_id
                event.delivery_status = "ASSIGNED"
            return batch

    def _resume_batch(self) -> AnalysisReceiptLakeBatch | None:
        with self._session_factory() as session:
            return session.scalar(
                select(AnalysisReceiptLakeBatch)
                .where(AnalysisReceiptLakeBatch.status.in_(("ASSIGNED", "FAILED")))
                .order_by(AnalysisReceiptLakeBatch.created_at)
            )

    def _batch_rows(self, batch_id: UUID) -> list[AnalysisReceiptLakeOutbox]:
        with self._session_factory() as session:
            return list(
                session.scalars(
                    select(AnalysisReceiptLakeOutbox)
                    .where(AnalysisReceiptLakeOutbox.batch_id == batch_id)
                    .order_by(AnalysisReceiptLakeOutbox.event_id)
                ).all()
            )

    @staticmethod
    def _parquet(events: list[AnalysisReceiptLakeOutbox], batch_id: UUID) -> bytes:
        rows = []
        for event in events:
            payload = event.payload
            rows.append(
                {
                    "event_id": event.event_id,
                    "receipt_id": payload["receipt_id"],
                    "message_id": payload["message_id"],
                    "household_id": payload["household_id"],
                    "device_id": payload["device_id"],
                    "source_topic": payload.get("source_topic"),
                    "source_partition": payload.get("source_partition"),
                    "source_offset": payload.get("source_offset"),
                    "measured_at": normalize_timestamp(payload["measured_at"]),
                    "processed_at": normalize_timestamp(payload["processed_at"]),
                    "analysis_run_id": payload["analysis_run_id"],
                    "attempt": payload["attempt"],
                    "model_version": payload["model_version"],
                    "pipeline_version": payload["pipeline_version"],
                    "state_epoch": payload["state_epoch"],
                    "outcome": payload["outcome"],
                    "appliance_types": payload.get("appliance_types", []),
                    "session_change_refs_json": json.dumps(
                        payload.get("session_change_refs", []), sort_keys=True
                    ),
                    "error_type": payload.get("error_type"),
                    "batch_id": str(batch_id),
                    "schema_version": RECEIPT_SCHEMA_VERSION,
                }
            )
        table = pa.Table.from_pylist(rows, schema=RECEIPT_SCHEMA)
        output = io.BytesIO()
        pq.write_table(table, output, compression="snappy")
        return output.getvalue()

    def _part_path(self, batch: AnalysisReceiptLakeBatch) -> str:
        return (
            f"{self._bronze_base}/ingest_date={batch.ingest_date.isoformat()}"
            f"/batch_id={batch.batch_id}/part-00000.parquet"
        )

    @staticmethod
    def _manifest(batch, part_path: str, data: bytes, row_count: int) -> dict:
        return {
            "job": "analysis-receipt-lake-loader",
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "batch_id": str(batch.batch_id),
            "ingest_date": batch.ingest_date.isoformat(),
            "row_count": row_count,
            "files": [
                {"path": part_path, "rows": row_count, "bytes": len(data), "sha256": sha256_hex(data)}
            ],
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }

    def _write_once(self, path: str, data: bytes) -> None:
        if self._storage.exists(path):
            if sha256_hex(self._storage.read_bytes(path)) != sha256_hex(data):
                raise RuntimeError(f"confirmed lake file differs: {path}")
            return
        temporary = f"{path}.tmp"
        self._storage.makedirs(parent_of(path))
        self._storage.write_bytes(temporary, data)
        self._storage.rename(temporary, path)

    def _verify_manifest(self, path: str) -> None:
        manifest = json.loads(self._storage.read_bytes(path))
        for item in manifest.get("files", []):
            file_path = item["path"]
            if not self._storage.exists(file_path):
                raise RuntimeError(f"receipt manifest file is missing: {file_path}")
            data = self._storage.read_bytes(file_path)
            if len(data) != int(item["bytes"]) or sha256_hex(data) != item["sha256"]:
                raise RuntimeError(f"receipt manifest file verification failed: {file_path}")

    def _complete(self, batch_id: UUID) -> None:
        now = datetime.now(timezone.utc)
        with self._session_factory.begin() as session:
            batch = session.get(AnalysisReceiptLakeBatch, batch_id)
            if batch is None:
                raise RuntimeError(f"receipt lake batch not found: {batch_id}")
            events = session.scalars(
                select(AnalysisReceiptLakeOutbox).where(
                    AnalysisReceiptLakeOutbox.batch_id == batch_id
                )
            ).all()
            if len(events) != batch.event_count:
                raise RuntimeError("receipt batch event count changed")
            for event in events:
                event.delivery_status = "DELIVERED"
                event.delivered_at = now
            batch.status = "COMPLETED"
            batch.completed_at = now

    def _fail(self, batch_id: UUID, error: Exception) -> None:
        with self._session_factory.begin() as session:
            batch = session.get(AnalysisReceiptLakeBatch, batch_id)
            if batch is not None:
                batch.status = "FAILED"
                batch.last_error = f"{type(error).__name__}: {error}"
