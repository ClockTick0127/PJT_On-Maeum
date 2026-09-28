"""Readiness checks for Kafka, PostgreSQL, and the loaded model."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker


logger = logging.getLogger(__name__)
ReadinessCheck = Callable[[], bool]


@dataclass(frozen=True)
class ReadinessReport:
    ready: bool
    checks: dict[str, str]


class ReadinessProbe:
    """Evaluates dependency checks without exposing exception details to callers."""

    def __init__(self, checks: Mapping[str, ReadinessCheck]) -> None:
        self._checks = dict(checks)

    def evaluate(self) -> ReadinessReport:
        statuses: dict[str, str] = {}
        for name, check in self._checks.items():
            try:
                is_ready = bool(check())
            except Exception:
                logger.warning("Readiness check failed: dependency=%s", name, exc_info=True)
                is_ready = False
            statuses[name] = "UP" if is_ready else "DOWN"
        return ReadinessReport(
            ready=all(status == "UP" for status in statuses.values()),
            checks=statuses,
        )


def database_readiness_check(
    session_factory: sessionmaker[Session],
) -> ReadinessCheck:
    def check() -> bool:
        with session_factory() as session:
            return session.execute(text("SELECT 1")).scalar_one() == 1

    return check


def kafka_readiness_check(
    admin_client: object,
    topic: str,
    timeout_seconds: float,
) -> ReadinessCheck:
    def check() -> bool:
        metadata = admin_client.list_topics(timeout=timeout_seconds)
        topic_metadata = metadata.topics.get(topic)
        return bool(
            metadata.brokers
            and topic_metadata is not None
            and topic_metadata.error is None
        )

    return check
