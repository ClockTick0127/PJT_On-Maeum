"""Persistent, single-writer execution history for daily batch work."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from datetime import date, datetime, timezone
import json
from typing import TypeVar
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker


T = TypeVar("T")


class BatchAlreadyRunning(RuntimeError):
    pass


class BatchRunRepository:
    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def has_succeeded(self, pipeline_name: str, target_date: date) -> bool:
        with self._session_factory() as session:
            return bool(
                session.execute(
                    text(
                        "SELECT EXISTS ("
                        "SELECT 1 FROM batch_run "
                        "WHERE pipeline_name=:pipeline_name "
                        "AND target_date=:target_date AND status='SUCCEEDED')"
                    ),
                    {"pipeline_name": pipeline_name, "target_date": target_date},
                ).scalar_one()
            )

    @contextmanager
    def lock(self, lock_name: str):
        session = self._session_factory()
        acquired = bool(
            session.execute(
                text("SELECT pg_try_advisory_lock(hashtext(:name))"),
                {"name": lock_name},
            ).scalar_one()
        )
        if not acquired:
            session.close()
            raise BatchAlreadyRunning(lock_name)
        try:
            yield
        finally:
            session.execute(
                text("SELECT pg_advisory_unlock(hashtext(:name))"),
                {"name": lock_name},
            )
            session.close()

    def run(
        self,
        pipeline_name: str,
        target_date: date,
        operation: Callable[[], T],
        *,
        dry_run: bool = False,
    ) -> T:
        lock_name = f"batch:{pipeline_name}:{target_date.isoformat()}"
        with self.lock(lock_name):
            if self.has_succeeded(pipeline_name, target_date):
                raise BatchAlreadyRunning(f"{lock_name}: already succeeded")
            run_id, version = self._start(pipeline_name, target_date, dry_run)
            try:
                result = operation()
            except Exception as error:
                self._finish(run_id, "FAILED", error_message=str(error))
                raise
            self._finish(run_id, "SUCCEEDED")
            return result

    def record_skipped(
        self,
        pipeline_name: str,
        target_date: date,
        details: dict,
        *,
        dry_run: bool,
    ) -> None:
        run_id, _ = self._start(pipeline_name, target_date, dry_run)
        self._finish(run_id, "SKIPPED", details=details)

    def _start(self, pipeline_name: str, target_date: date, dry_run: bool) -> tuple[UUID, int]:
        run_id = uuid4()
        with self._session_factory.begin() as session:
            version = int(
                session.execute(
                    text(
                        "SELECT COALESCE(MAX(run_version), 0) + 1 FROM batch_run "
                        "WHERE pipeline_name=:pipeline_name AND target_date=:target_date"
                    ),
                    {"pipeline_name": pipeline_name, "target_date": target_date},
                ).scalar_one()
            )
            session.execute(
                text(
                    "INSERT INTO batch_run "
                    "(run_id,pipeline_name,target_date,run_version,status,dry_run,started_at) "
                    "VALUES (:run_id,:pipeline_name,:target_date,:version,'RUNNING',:dry_run,:now)"
                ),
                {
                    "run_id": run_id,
                    "pipeline_name": pipeline_name,
                    "target_date": target_date,
                    "version": version,
                    "dry_run": dry_run,
                    "now": datetime.now(timezone.utc),
                },
            )
        return run_id, version

    def _finish(
        self,
        run_id: UUID,
        status: str,
        *,
        details: dict | None = None,
        error_message: str | None = None,
    ) -> None:
        with self._session_factory.begin() as session:
            session.execute(
                text(
                    "UPDATE batch_run SET status=:status, finished_at=:now, "
                    "details=CAST(:details AS jsonb), error_message=:error "
                    "WHERE run_id=:run_id"
                ),
                {
                    "run_id": run_id,
                    "status": status,
                    "now": datetime.now(timezone.utc),
                    "details": json.dumps(details or {}, ensure_ascii=False),
                    "error": error_message,
                },
            )
