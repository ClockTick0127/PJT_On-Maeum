"""Daily generation of appliance routine baselines from VALID observations."""

from __future__ import annotations

import logging
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from uuid import uuid4
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from realtime_analysis.baseline import SqlAlchemyBaselineRepository
from aggregation_service.daily_activity_index import (
    MINIMUM_SESSION_SECONDS,
    SESSION_MERGE_GAP_SECONDS,
)
from realtime_analysis.models import (
    APPLIANCE_TYPES,
    ApplianceUsageSession,
    HouseholdActivityDaily,
    HouseholdObservationDaily,
    RoutineBaselineModel,
)


logger = logging.getLogger(__name__)

BASELINE_TYPE = "ROUTINE_MISSED"
WEEKDAY_NAMES = ("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN")


def first_valid_logical_use(
    appliance_type: str,
    sessions: list[tuple[datetime, datetime]],
) -> datetime | None:
    merge_gap = SESSION_MERGE_GAP_SECONDS.get(appliance_type, 60)
    current_start: datetime | None = None
    current_end: datetime | None = None
    active_seconds = 0.0

    for started_at, ended_at in sorted(sessions):
        if current_start is None or current_end is None:
            current_start = started_at
            current_end = ended_at
            active_seconds = (ended_at - started_at).total_seconds()
            continue

        gap_seconds = (started_at - current_end).total_seconds()
        if gap_seconds <= merge_gap:
            current_end = max(current_end, ended_at)
            active_seconds += (ended_at - started_at).total_seconds()
            continue

        if active_seconds >= MINIMUM_SESSION_SECONDS:
            return current_start
        current_start = started_at
        current_end = ended_at
        active_seconds = (ended_at - started_at).total_seconds()

    if current_start is not None and active_seconds >= MINIMUM_SESSION_SECONDS:
        return current_start
    return None


@dataclass(frozen=True)
class BaselineCandidate:
    appliance_type: str
    sample_days: int
    active_days: int
    daily_use_probability: Decimal
    reliability_weight: Decimal
    baseline_data: dict[str, object]
    enabled: bool


class RoutineBaselineCalculator:
    """Calculates daily-use frequency and first-use time profiles."""

    def __init__(
        self,
        window_days: int,
        minimum_sample_days: int,
        minimum_weekday_sample_days: int,
        minimum_daily_use_probability: float,
    ) -> None:
        self._window_days = window_days
        self._minimum_sample_days = minimum_sample_days
        self._minimum_weekday_sample_days = minimum_weekday_sample_days
        self._minimum_daily_use_probability = Decimal(
            str(minimum_daily_use_probability)
        )

    def calculate(
        self,
        as_of_date: date,
        valid_dates: list[date],
        first_use_seconds: dict[str, dict[date, int]],
    ) -> list[BaselineCandidate]:
        sample_days = len(valid_dates)
        if sample_days < self._minimum_sample_days:
            return []

        window_start = as_of_date - timedelta(days=self._window_days - 1)
        reliability = self._ratio(sample_days, self._minimum_sample_days)
        candidates: list[BaselineCandidate] = []
        for appliance_type in APPLIANCE_TYPES:
            uses_by_date = first_use_seconds.get(appliance_type, {})
            active_times = [
                uses_by_date[valid_date]
                for valid_date in valid_dates
                if valid_date in uses_by_date
            ]
            active_days = len(active_times)
            probability = self._ratio(active_days, sample_days)
            expected_until = self._percentile(active_times, 0.90)
            baseline_data: dict[str, object] = {
                "source": "VALID_DAILY_OBSERVATIONS",
                "as_of_date": as_of_date.isoformat(),
                "window_start_date": window_start.isoformat(),
                "window_end_date": as_of_date.isoformat(),
                "expected_until": self._format_time(expected_until),
                "first_use_time_p50": self._format_time(
                    self._percentile(active_times, 0.50)
                ),
                "preferred_window": self._preferred_window(active_times),
                "weekday_profiles": self._weekday_profiles(
                    valid_dates,
                    uses_by_date,
                ),
            }
            candidates.append(
                BaselineCandidate(
                    appliance_type=appliance_type,
                    sample_days=sample_days,
                    active_days=active_days,
                    daily_use_probability=probability,
                    reliability_weight=reliability,
                    baseline_data=baseline_data,
                    enabled=(
                        expected_until is not None
                        and probability >= self._minimum_daily_use_probability
                    ),
                )
            )
        return candidates

    def _weekday_profiles(
        self,
        valid_dates: list[date],
        uses_by_date: dict[date, int],
    ) -> dict[str, dict[str, object]]:
        profiles: dict[str, dict[str, object]] = {}
        for weekday, weekday_name in enumerate(WEEKDAY_NAMES):
            weekday_dates = [item for item in valid_dates if item.weekday() == weekday]
            active_times = [
                uses_by_date[item] for item in weekday_dates if item in uses_by_date
            ]
            sample_days = len(weekday_dates)
            active_days = len(active_times)
            expected_until = self._percentile(active_times, 0.90)
            profiles[weekday_name] = {
                "sample_days": sample_days,
                "active_days": active_days,
                "daily_use_probability": float(
                    self._ratio(active_days, sample_days)
                ),
                "expected_until": self._format_time(expected_until),
                "eligible": (
                    sample_days >= self._minimum_weekday_sample_days
                    and expected_until is not None
                ),
            }
        return profiles

    @classmethod
    def _preferred_window(cls, values: list[int]) -> dict[str, str] | None:
        start = cls._percentile(values, 0.10)
        end = cls._percentile(values, 0.90)
        if start is None or end is None:
            return None
        return {
            "start": cls._format_time(start),
            "end": cls._format_time(end),
        }

    @staticmethod
    def _ratio(numerator: int, denominator: int) -> Decimal:
        if denominator <= 0:
            return Decimal("0.0000")
        return min(
            Decimal("1.0000"),
            (Decimal(numerator) / Decimal(denominator)).quantize(
                Decimal("0.0001")
            ),
        )

    @staticmethod
    def _percentile(values: list[int], percentile: float) -> int | None:
        if not values:
            return None
        ordered = sorted(values)
        index = max(0, math.ceil(percentile * len(ordered)) - 1)
        return ordered[index]

    @staticmethod
    def _format_time(seconds: int | None) -> str | None:
        if seconds is None:
            return None
        seconds = max(0, min(seconds, 24 * 60 * 60 - 1))
        hour, remainder = divmod(seconds, 3600)
        minute = remainder // 60
        return f"{hour:02d}:{minute:02d}"


class RoutineBaselineUpdateService:
    """Recalculates and upserts current baselines after a local day is finalized."""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        baseline_repository: SqlAlchemyBaselineRepository,
        timezone_name: str,
        window_days: int = 28,
        minimum_sample_days: int = 14,
        minimum_weekday_sample_days: int = 4,
        minimum_daily_use_probability: float = 0.70,
    ) -> None:
        self._session_factory = session_factory
        self._baseline_repository = baseline_repository
        self._timezone = ZoneInfo(timezone_name)
        self._window_days = window_days
        self._calculator = RoutineBaselineCalculator(
            window_days=window_days,
            minimum_sample_days=minimum_sample_days,
            minimum_weekday_sample_days=minimum_weekday_sample_days,
            minimum_daily_use_probability=minimum_daily_use_probability,
        )

    def update(self, as_of_date: date) -> int:
        window_start = as_of_date - timedelta(days=self._window_days - 1)
        calculated_at = datetime.now(timezone.utc)
        updated = 0
        updated_households = 0

        with self._session_factory.begin() as session:
            observations = session.scalars(
                select(HouseholdObservationDaily)
                .where(
                    HouseholdObservationDaily.observation_date >= window_start,
                    HouseholdObservationDaily.observation_date <= as_of_date,
                    HouseholdObservationDaily.observation_status == "VALID",
                )
                .order_by(
                    HouseholdObservationDaily.household_id,
                    HouseholdObservationDaily.observation_date,
                )
            ).all()
            by_household: dict[str, list[HouseholdObservationDaily]] = defaultdict(
                list
            )
            for observation in observations:
                by_household[observation.household_id].append(observation)

            for household_id, household_observations in by_household.items():
                valid_dates = [
                    observation.observation_date
                    for observation in household_observations
                ]
                first_use_seconds = self._first_use_seconds(
                    session,
                    household_observations,
                )
                candidates = self._calculator.calculate(
                    as_of_date,
                    valid_dates,
                    first_use_seconds,
                )
                if not candidates:
                    continue
                updated_households += 1
                for candidate in candidates:
                    self._upsert(
                        session,
                        household_id,
                        candidate,
                        calculated_at,
                    )
                    updated += 1

        self._baseline_repository.refresh()
        logger.info(
            "Routine baselines updated: as_of_date=%s households=%s rows=%s",
            as_of_date,
            updated_households,
            updated,
        )
        return updated

    def _first_use_seconds(
        self,
        session: Session,
        observations: list[HouseholdObservationDaily],
    ) -> dict[str, dict[date, int]]:
        observation_ids = [observation.id for observation in observations]
        rows = session.execute(
            select(
                HouseholdObservationDaily.observation_date,
                HouseholdActivityDaily.appliance_type,
                ApplianceUsageSession.started_at,
                ApplianceUsageSession.ended_at,
            )
            .join(
                HouseholdActivityDaily,
                HouseholdActivityDaily.observation_daily_id
                == HouseholdObservationDaily.id,
            )
            .join(
                ApplianceUsageSession,
                ApplianceUsageSession.activity_daily_id
                == HouseholdActivityDaily.id,
            )
            .where(HouseholdObservationDaily.id.in_(observation_ids))
        ).all()

        grouped_sessions: dict[
            tuple[str, date],
            list[tuple[datetime, datetime]],
        ] = defaultdict(list)
        for observation_date, appliance_type, started_at, ended_at in rows:
            started_utc = self._as_utc(started_at)
            local_day_start = datetime.combine(
                observation_date,
                time.min,
                self._timezone,
            )
            day_start_utc = local_day_start.astimezone(timezone.utc)
            day_end_utc = (local_day_start + timedelta(days=1)).astimezone(
                timezone.utc
            )
            if ended_at is None:
                ended_utc = day_end_utc
            else:
                ended_utc = self._as_utc(ended_at)
            clipped_start = max(started_utc, day_start_utc)
            clipped_end = min(ended_utc, day_end_utc)
            if clipped_end <= clipped_start:
                continue
            grouped_sessions[(appliance_type, observation_date)].append(
                (clipped_start, clipped_end)
            )

        result: dict[str, dict[date, int]] = defaultdict(dict)
        for (appliance_type, observation_date), sessions in grouped_sessions.items():
            first_start = first_valid_logical_use(appliance_type, sessions)
            if first_start is None:
                continue
            local_start = first_start.astimezone(self._timezone)
            seconds = (
                local_start.hour * 3600
                + local_start.minute * 60
                + local_start.second
            )
            result[appliance_type][observation_date] = seconds
        return result

    @staticmethod
    def _as_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @staticmethod
    def _upsert(
        session: Session,
        household_id: str,
        candidate: BaselineCandidate,
        calculated_at: datetime,
    ) -> None:
        row = session.scalar(
            select(RoutineBaselineModel)
            .where(
                RoutineBaselineModel.household_id == household_id,
                RoutineBaselineModel.appliance_type == candidate.appliance_type,
                RoutineBaselineModel.baseline_type == BASELINE_TYPE,
            )
            .with_for_update()
        )
        if row is None:
            row = RoutineBaselineModel(
                id=uuid4(),
                household_id=household_id,
                appliance_type=candidate.appliance_type,
                baseline_type=BASELINE_TYPE,
                sample_days=candidate.sample_days,
                active_days=candidate.active_days,
                daily_use_probability=candidate.daily_use_probability,
                reliability_weight=candidate.reliability_weight,
                baseline_data=candidate.baseline_data,
                enabled=candidate.enabled,
                calculated_at=calculated_at,
            )
            session.add(row)
            return

        row.sample_days = candidate.sample_days
        row.active_days = candidate.active_days
        row.daily_use_probability = candidate.daily_use_probability
        row.reliability_weight = candidate.reliability_weight
        row.baseline_data = candidate.baseline_data
        row.enabled = candidate.enabled
        row.calculated_at = calculated_at
