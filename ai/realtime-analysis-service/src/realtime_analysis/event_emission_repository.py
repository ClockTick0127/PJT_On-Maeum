"""Persistence for successfully published analysis events."""

from datetime import datetime
from typing import Protocol
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from realtime_analysis.models import AnalysisEventEmission


class EventEmissionRepository(Protocol):
    """Storage contract used by anomaly cooldown handling."""

    def was_emitted(self, event_id: UUID) -> bool: ...

    def last_emitted_at(
        self,
        household_id: str,
        event_type: str,
        appliance_type: str | None,
    ) -> datetime | None: ...

    def record_emission(
        self,
        event_id: UUID,
        household_id: str,
        event_type: str,
        appliance_type: str | None,
        emitted_at: datetime,
    ) -> bool: ...


class SqlAlchemyEventEmissionRepository:
    """SQLAlchemy-backed event emission history repository."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def was_emitted(self, event_id: UUID) -> bool:
        with self._session_factory() as session:
            return session.get(AnalysisEventEmission, event_id) is not None

    def last_emitted_at(
        self,
        household_id: str,
        event_type: str,
        appliance_type: str | None,
    ) -> datetime | None:
        with self._session_factory() as session:
            appliance_filter = (
                AnalysisEventEmission.appliance_type.is_(None)
                if appliance_type is None
                else AnalysisEventEmission.appliance_type == appliance_type
            )
            return session.scalar(
                select(AnalysisEventEmission.emitted_at)
                .where(
                    AnalysisEventEmission.household_id == household_id,
                    AnalysisEventEmission.event_type == event_type,
                    appliance_filter,
                )
                .order_by(AnalysisEventEmission.emitted_at.desc())
                .limit(1)
            )

    def record_emission(
        self,
        event_id: UUID,
        household_id: str,
        event_type: str,
        appliance_type: str | None,
        emitted_at: datetime,
    ) -> bool:
        """Store an event once and report whether a new row was inserted."""

        with self._session_factory.begin() as session:
            if session.get(AnalysisEventEmission, event_id) is not None:
                return False
            session.add(
                AnalysisEventEmission(
                    event_id=event_id,
                    household_id=household_id,
                    event_type=event_type,
                    appliance_type=appliance_type,
                    emitted_at=emitted_at,
                )
            )
        return True
