"""Persistence for each household's latest outing state."""

from datetime import datetime, timezone
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from realtime_analysis.models import HouseholdOutingState
from realtime_analysis.schemas import OutingEvent


class OutingStateProvider(Protocol):
    """Read contract used by outing-aware pattern detection."""

    def get_state(self, household_id: str) -> HouseholdOutingState | None: ...

    def has_outing_overlap(
        self,
        household_id: str,
        started_at: datetime,
        ended_at: datetime,
    ) -> bool: ...


class OutingStateRepository(OutingStateProvider, Protocol):
    """Read/write contract used by the outing event consumer."""

    def apply_event(
        self,
        event: OutingEvent,
        updated_at: datetime | None = None,
    ) -> bool: ...

class SqlAlchemyOutingStateRepository:
    """Applies only the latest event to one state row per household."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def apply_event(
        self,
        event: OutingEvent,
        updated_at: datetime | None = None,
    ) -> bool:
        """Apply a new event and report whether the current state changed."""

        applied_at = updated_at or datetime.now(timezone.utc)
        if applied_at.tzinfo is None or applied_at.utcoffset() is None:
            raise ValueError("updated_at must include a timezone")
        applied_at = _as_utc(applied_at)

        with self._session_factory.begin() as session:
            state = session.scalar(
                select(HouseholdOutingState)
                .where(HouseholdOutingState.household_id == event.household_id)
                .with_for_update()
            )
            if state is None:
                session.add(self._initial_state(event, applied_at))
            elif (
                event.event_id == state.last_event_id
                or _as_utc(event.occurred_at) <= _as_utc(state.last_event_at)
            ):
                return False
            else:
                self._apply_event(state, event, applied_at)

        return True

    def get_state(self, household_id: str) -> HouseholdOutingState | None:
        with self._session_factory() as session:
            return session.get(HouseholdOutingState, household_id)

    def has_outing_overlap(
        self,
        household_id: str,
        started_at: datetime,
        ended_at: datetime,
    ) -> bool:
        if started_at.tzinfo is None or started_at.utcoffset() is None:
            raise ValueError("started_at must include a timezone")
        if ended_at.tzinfo is None or ended_at.utcoffset() is None:
            raise ValueError("ended_at must include a timezone")
        window_start = _as_utc(started_at)
        window_end = _as_utc(ended_at)
        if window_end < window_start:
            raise ValueError("ended_at must not be earlier than started_at")

        state = self.get_state(household_id)
        if state is None or state.outing_started_at is None:
            return False
        outing_start = _as_utc(state.outing_started_at)
        outing_end = (
            None
            if state.is_outing
            else (
                _as_utc(state.last_returned_at)
                if state.last_returned_at is not None
                else None
            )
        )
        if outing_end is None and not state.is_outing:
            return False
        return outing_start <= window_end and (
            outing_end is None or outing_end > window_start
        )

    @staticmethod
    def _initial_state(
        event: OutingEvent,
        updated_at: datetime,
    ) -> HouseholdOutingState:
        is_outing = event.event_type == "OUTING_STARTED"
        occurred_at = _as_utc(event.occurred_at)
        return HouseholdOutingState(
            household_id=event.household_id,
            is_outing=is_outing,
            outing_started_at=occurred_at if is_outing else None,
            last_returned_at=(
                occurred_at if event.event_type == "OUTING_ENDED" else None
            ),
            last_event_id=event.event_id,
            last_event_at=occurred_at,
            updated_at=updated_at,
        )

    @staticmethod
    def _apply_event(
        state: HouseholdOutingState,
        event: OutingEvent,
        updated_at: datetime,
    ) -> None:
        occurred_at = _as_utc(event.occurred_at)
        if event.event_type == "OUTING_STARTED":
            if not state.is_outing or state.outing_started_at is None:
                state.outing_started_at = occurred_at
            state.is_outing = True
        else:
            state.is_outing = False
            state.last_returned_at = occurred_at

        state.last_event_id = event.event_id
        state.last_event_at = occurred_at
        state.updated_at = updated_at


def _as_utc(value: datetime) -> datetime:
    """Normalize timestamps returned by drivers that omit the stored UTC tzinfo."""

    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
