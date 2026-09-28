"""Guarded HDFS Bronze retention with dry-run as the default."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
import json
import logging
from pathlib import PurePosixPath
from uuid import uuid4

from hdfs import InsecureClient

from aggregation_service.batch_runs import BatchAlreadyRunning, BatchRunRepository


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RetentionConfig:
    bronze_base: str
    bronze_manifest_base: str
    manifest_base: str
    retention_days: int
    grace_days: int
    max_delete_bytes: int
    max_delete_dates: int
    apply: bool = False
    require_compaction: bool = True
    compaction_manifest_base: str = "/nilm/manifests/job=compaction"
    staging_base: str = "/nilm/.retention-staging"


@dataclass(frozen=True)
class DeletePlan:
    run_id: str
    policy_version: str
    ingest_date: str
    target_path: str
    planned_files: int
    planned_bytes: int
    related_business_dates: tuple[str, ...]
    apply: bool
    created_at: str


class BronzeRetentionService:
    def __init__(
        self,
        client: InsecureClient,
        runs: BatchRunRepository,
        config: RetentionConfig,
    ) -> None:
        self._client = client
        self._runs = runs
        self._config = config

    def run(self, now: datetime | None = None) -> int:
        now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        try:
            with self._runs.lock("batch:retention-scan"):
                return self._run_locked(now)
        except BatchAlreadyRunning:
            logger.info("Another retention scan already owns the database lock")
            return 0

    def _run_locked(self, now: datetime) -> int:
        processed = 0
        for ingest_date, path in self._expired_candidates(now):
            if processed >= self._config.max_delete_dates:
                break
            pipeline = "retention" if self._config.apply else "retention-dry-run"
            if self._runs.has_succeeded(pipeline, ingest_date):
                continue
            summary = self._client.content(path)
            planned_bytes = int(summary.get("spaceConsumed", summary.get("length", 0)))
            planned_files = int(summary.get("fileCount", 0))
            if planned_bytes > self._config.max_delete_bytes:
                self._runs.record_skipped(
                    pipeline,
                    ingest_date,
                    {
                        "reason": "MAX_DELETE_BYTES_EXCEEDED",
                        "planned_bytes": planned_bytes,
                        "limit": self._config.max_delete_bytes,
                    },
                    dry_run=not self._config.apply,
                )
                continue

            business_dates = self._business_dates(ingest_date)
            failed = self._failed_preconditions(ingest_date, business_dates)
            if failed:
                self._runs.record_skipped(
                    pipeline,
                    ingest_date,
                    {"reason": "PRECONDITION_FAILED", "failed": failed},
                    dry_run=not self._config.apply,
                )
                continue

            plan = DeletePlan(
                run_id=str(uuid4()),
                policy_version="bronze-retention-v1",
                ingest_date=ingest_date.isoformat(),
                target_path=path,
                planned_files=planned_files,
                planned_bytes=planned_bytes,
                related_business_dates=tuple(sorted(business_dates)),
                apply=self._config.apply,
                created_at=now.isoformat(),
            )
            self._runs.run(
                pipeline,
                ingest_date,
                lambda plan=plan: self._execute(plan),
                dry_run=not self._config.apply,
            )
            processed += 1
        return processed

    def _expired_candidates(self, now: datetime) -> list[tuple[date, str]]:
        cutoff = now.date() - timedelta(
            days=self._config.retention_days + self._config.grace_days
        )
        try:
            names = self._client.list(self._config.bronze_base)
        except Exception:
            logger.exception("Unable to list Bronze retention candidates")
            raise
        candidates: list[tuple[date, str]] = []
        for name in names:
            if not name.startswith("ingest_date="):
                continue
            try:
                ingest_date = date.fromisoformat(name.removeprefix("ingest_date="))
            except ValueError:
                logger.warning("Ignoring malformed Bronze directory: %s", name)
                continue
            # A day becomes eligible only after its UTC end plus both windows.
            if ingest_date < cutoff:
                candidates.append(
                    (ingest_date, f"{self._config.bronze_base.rstrip('/')}/{name}")
                )
        return sorted(candidates)

    def _business_dates(self, ingest_date: date) -> set[str]:
        manifest_dir = (
            f"{self._config.bronze_manifest_base.rstrip('/')}"
            f"/date={ingest_date.isoformat()}"
        )
        if self._client.status(manifest_dir, strict=False) is None:
            return set()
        business_dates: set[str] = set()
        for name in self._client.list(manifest_dir):
            if not name.endswith(".json"):
                continue
            with self._client.read(f"{manifest_dir}/{name}", encoding="utf-8") as reader:
                manifest = json.load(reader)
            business_dates.update(manifest.get("business_dates", []))
        return business_dates

    def _failed_preconditions(
        self,
        ingest_date: date,
        business_dates: set[str],
    ) -> list[str]:
        failed: list[str] = []
        if not business_dates:
            failed.append("MISSING_BUSINESS_DATE_MANIFEST")
        for value in sorted(business_dates):
            if not self._runs.has_succeeded("daily-aggregation", date.fromisoformat(value)):
                failed.append(f"DAILY_AGGREGATION_NOT_SUCCEEDED:{value}")
        if self._config.require_compaction:
            marker = (
                f"{self._config.compaction_manifest_base.rstrip('/')}"
                f"/date={ingest_date.isoformat()}/_SUCCESS"
            )
            if self._client.status(marker, strict=False) is None:
                failed.append("COMPACTION_NOT_SUCCEEDED")
        return failed

    def _execute(self, plan: DeletePlan) -> DeletePlan:
        plan_path = f"{self._config.manifest_base}/run_id={plan.run_id}/plan.json"
        self._atomic_json(plan_path, asdict(plan))
        if not plan.apply:
            logger.info(
                "Retention dry-run: path=%s files=%s bytes=%s",
                plan.target_path,
                plan.planned_files,
                plan.planned_bytes,
            )
            return plan

        current = self._client.content(plan.target_path)
        current_bytes = int(current.get("spaceConsumed", current.get("length", 0)))
        current_files = int(current.get("fileCount", 0))
        if (current_bytes, current_files) != (plan.planned_bytes, plan.planned_files):
            raise RuntimeError("Bronze directory changed after retention planning")

        staging = (
            f"{self._config.staging_base.rstrip('/')}/run_id={plan.run_id}/"
            f"{PurePosixPath(plan.target_path).name}"
        )
        self._client.makedirs(str(PurePosixPath(staging).parent))
        self._client.rename(plan.target_path, staging)
        if self._client.status(staging, strict=False) is None:
            raise RuntimeError(f"Failed to stage retention target: {plan.target_path}")
        if not self._client.delete(staging, recursive=True):
            raise RuntimeError(f"Failed to delete staged retention target: {staging}")
        if self._client.status(plan.target_path, strict=False) is not None:
            raise RuntimeError(f"Retention target still exists: {plan.target_path}")
        if self._client.status(staging, strict=False) is not None:
            raise RuntimeError(f"Retention staging path still exists: {staging}")

        self._atomic_json(
            f"{self._config.manifest_base}/run_id={plan.run_id}/result.json",
            {**asdict(plan), "status": "SUCCEEDED", "finished_at": datetime.now(timezone.utc).isoformat()},
        )
        return plan

    def _atomic_json(self, final_path: str, value: dict) -> None:
        data = json.dumps(value, ensure_ascii=False, indent=2).encode()
        temporary = final_path + ".tmp"
        self._client.makedirs(str(PurePosixPath(final_path).parent))
        self._client.write(temporary, data=data, overwrite=True)
        if self._client.status(temporary)["length"] != len(data):
            raise IOError(f"Retention manifest length mismatch: {temporary}")
        self._client.delete(final_path)
        self._client.rename(temporary, final_path)
        if self._client.status(final_path, strict=False) is None:
            raise IOError(f"Retention manifest rename failed: {final_path}")
