"""Analysis policy bootstrap and cached DB access."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock

from pydantic import TypeAdapter
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from realtime_analysis.models import AnalysisPolicy
from realtime_analysis.schemas import AnalysisPolicyDefinition


class PolicyRepository:
    def __init__(self, policies: Sequence[AnalysisPolicyDefinition]) -> None:
        self._policies = tuple(policies)

    @classmethod
    def from_json_file(cls, path: str | Path) -> "PolicyRepository":
        with Path(path).open(encoding="utf-8") as file:
            payload = json.load(file)
        policies = TypeAdapter(list[AnalysisPolicyDefinition]).validate_python(
            payload
        )
        return cls(policies)

    @property
    def policies(self) -> tuple[AnalysisPolicyDefinition, ...]:
        return self._policies


class SqlAlchemyPolicyRepository:
    """Seeds missing policies and serves an immutable enabled-policy cache."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory
        self._lock = Lock()
        self._by_event_type: dict[str, AnalysisPolicyDefinition] = {}
        self.refresh()

    def seed_missing(self, policies: Sequence[AnalysisPolicyDefinition]) -> int:
        inserted = 0
        with self._session_factory.begin() as session:
            for policy in policies:
                existing = session.scalar(
                    select(AnalysisPolicy.id).where(
                        AnalysisPolicy.policy_code == policy.policy_code
                    )
                )
                if existing is not None:
                    continue
                session.add(
                    AnalysisPolicy(
                        policy_code=policy.policy_code,
                        algorithm_type=policy.algorithm_type,
                        parameters=policy.parameters,
                        event_type=policy.event_type,
                        cooldown_hours=policy.cooldown_hours,
                        enabled=policy.enabled,
                        created_at=datetime.now(timezone.utc),
                    )
                )
                inserted += 1
        if inserted:
            self.refresh()
        return inserted

    def refresh(self) -> None:
        with self._session_factory() as session:
            rows = session.scalars(
                select(AnalysisPolicy)
                .where(AnalysisPolicy.enabled.is_(True))
                .order_by(AnalysisPolicy.policy_code)
            ).all()
            snapshot = {
                row.event_type: AnalysisPolicyDefinition(
                    policy_code=row.policy_code,
                    algorithm_type=row.algorithm_type,
                    parameters=row.parameters,
                    event_type=row.event_type,
                    cooldown_hours=row.cooldown_hours,
                    enabled=row.enabled,
                )
                for row in rows
            }
        with self._lock:
            self._by_event_type = snapshot

    def get(self, event_type: str) -> AnalysisPolicyDefinition | None:
        with self._lock:
            return self._by_event_type.get(event_type)
