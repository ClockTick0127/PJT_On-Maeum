"""Routine baseline providers backed by bootstrap JSON or analysis_db."""

import json
import logging
from collections.abc import Sequence
from datetime import datetime, time, timezone
from decimal import Decimal
from pathlib import Path
from threading import Lock
from typing import Protocol
from zoneinfo import ZoneInfo

from pydantic import TypeAdapter
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from realtime_analysis.models import RoutineBaselineModel
from realtime_analysis.schemas import RoutineBaseline


logger = logging.getLogger(__name__)
WEEKDAY_NAMES = ("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN")


class RoutineBaselineProvider(Protocol):
    def find_by_household(
        self,
        household_id: str,
        observed_at: datetime | None = None,
    ) -> list[RoutineBaseline]: ...

    @property
    def household_ids(self) -> tuple[str, ...]: ...


class BaselineRepository:
    """In-memory bootstrap repository used by tests and local seed data."""

    def __init__(self, baselines: list[RoutineBaseline]) -> None:
        self._by_household: dict[str, list[RoutineBaseline]] = {}
        for baseline in baselines:
            self._by_household.setdefault(baseline.household_id, []).append(baseline)

    @classmethod
    def from_json_file(cls, path: str | Path) -> "BaselineRepository":
        with Path(path).open(encoding="utf-8") as file:
            payload = json.load(file)
        baselines = TypeAdapter(list[RoutineBaseline]).validate_python(payload)
        return cls(baselines)

    def find_by_household(
        self,
        household_id: str,
        observed_at: datetime | None = None,
    ) -> list[RoutineBaseline]:
        # Kept for compatibility with RoutineBaselineProvider. Bootstrap
        # baselines are static, so their lookup does not depend on time.
        _ = observed_at
        return [
            baseline
            for baseline in self._by_household.get(household_id, [])
            if baseline.enabled
        ]

    @property
    def household_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._by_household))

    @property
    def baselines(self) -> tuple[RoutineBaseline, ...]:
        return tuple(
            baseline
            for household_baselines in self._by_household.values()
            for baseline in household_baselines
        )


class SqlAlchemyBaselineRepository:
    """Caches active DB baselines and refreshes atomically after daily updates."""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        timezone_name: str,
    ) -> None:
        self._session_factory = session_factory
        self._timezone = ZoneInfo(timezone_name)
        self._lock = Lock()
        self._rows_by_household: dict[str, tuple[RoutineBaselineModel, ...]] = {}
        self.refresh()

    def seed_missing(self, baselines: Sequence[RoutineBaseline]) -> int:
        """Insert bootstrap rows only when a generated/configured key is absent."""

        inserted = 0
        calculated_at = datetime.now(timezone.utc)
        with self._session_factory.begin() as session:
            for baseline in baselines:
                exists = session.scalar(
                    select(RoutineBaselineModel.id).where(
                        RoutineBaselineModel.household_id == baseline.household_id,
                        RoutineBaselineModel.appliance_type == baseline.appliance_type,
                        RoutineBaselineModel.baseline_type == baseline.baseline_type,
                    )
                )
                if exists is not None:
                    continue
                session.add(
                    RoutineBaselineModel(
                        id=baseline.id,
                        household_id=baseline.household_id,
                        appliance_type=baseline.appliance_type,
                        baseline_type=baseline.baseline_type,
                        sample_days=baseline.window_days,
                        active_days=baseline.normal_days,
                        daily_use_probability=(
                            Decimal(baseline.normal_days)
                            / Decimal(baseline.window_days)
                        ),
                        reliability_weight=Decimal(
                            str(baseline.reliability_weight)
                        ),
                        baseline_data={
                            "source": "BOOTSTRAP_CONFIG",
                            "expected_until": baseline.expected_until.strftime(
                                "%H:%M"
                            ),
                        },
                        enabled=baseline.enabled,
                        calculated_at=calculated_at,
                    )
                )
                inserted += 1
        if inserted:
            self.refresh()
        return inserted

    def refresh(self) -> None:
        with self._session_factory() as session:
            rows = session.scalars(
                select(RoutineBaselineModel)
                .where(RoutineBaselineModel.enabled.is_(True))
                .order_by(
                    RoutineBaselineModel.household_id,
                    RoutineBaselineModel.appliance_type,
                )
            ).all()
            grouped: dict[str, list[RoutineBaselineModel]] = {}
            for row in rows:
                # Detach values from the Session before publishing the new cache.
                session.expunge(row)
                grouped.setdefault(row.household_id, []).append(row)
        snapshot = {
            household_id: tuple(household_rows)
            for household_id, household_rows in grouped.items()
        }
        with self._lock:
            self._rows_by_household = snapshot

    def find_by_household(
        self,
        household_id: str,
        observed_at: datetime | None = None,
    ) -> list[RoutineBaseline]:
        with self._lock:
            rows = self._rows_by_household.get(household_id, ())
        return [
            baseline
            for row in rows
            if (baseline := self._to_schema(row, observed_at)) is not None
        ]

    @property
    def household_ids(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._rows_by_household))

    def _to_schema(
        self,
        row: RoutineBaselineModel,
        observed_at: datetime | None,
    ) -> RoutineBaseline | None:
        data = row.baseline_data or {}
        expected_until = data.get("expected_until")
        normal_days = row.active_days
        window_days = row.sample_days

        if observed_at is not None:
            local_date = observed_at.astimezone(self._timezone).date()
            weekday = WEEKDAY_NAMES[local_date.weekday()]
            weekday_profile = data.get("weekday_profiles", {}).get(weekday)
            if weekday_profile and weekday_profile.get("eligible", False):
                expected_until = weekday_profile.get("expected_until")
                normal_days = int(weekday_profile["active_days"])
                window_days = int(weekday_profile["sample_days"])

        parsed_expected_until = self._parse_time(expected_until)
        if parsed_expected_until is None or window_days <= 0:
            logger.warning(
                "Ignoring unusable routine baseline: household=%s appliance=%s",
                row.household_id,
                row.appliance_type,
            )
            return None
        return RoutineBaseline(
            id=row.id,
            household_id=row.household_id,
            appliance_type=row.appliance_type,
            baseline_type=row.baseline_type,
            expected_until=parsed_expected_until,
            normal_days=normal_days,
            window_days=window_days,
            reliability_weight=float(row.reliability_weight),
            enabled=row.enabled,
        )

    @staticmethod
    def _parse_time(value: object) -> time | None:
        if not isinstance(value, str):
            return None
        try:
            return time.fromisoformat(value)
        except ValueError:
            return None
