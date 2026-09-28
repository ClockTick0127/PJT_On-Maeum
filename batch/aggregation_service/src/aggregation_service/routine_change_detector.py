"""Daily informational ROUTINE_CHANGED detection."""

from __future__ import annotations

import statistics
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from time import perf_counter
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from realtime_analysis.anomaly_detector import event_id_for
from aggregation_service.baseline_updater import first_valid_logical_use
from realtime_analysis.metrics import AnalysisMetrics, METRICS
from realtime_analysis.models import (
    APPLIANCE_TYPES,
    ApplianceUsageSession,
    HouseholdActivityDaily,
    HouseholdObservationDaily,
)
from realtime_analysis.policy import SqlAlchemyPolicyRepository
from realtime_analysis.schemas import AnalysisEvent


class EventPublisher(Protocol):
    def publish(self, event: AnalysisEvent) -> None: ...


@dataclass(frozen=True)
class _DetectionCriteria:
    recent_start: date
    reference_start: date
    reference_end: date
    recent_minimum: int
    reference_minimum: int
    minimum_probability: float
    minimum_shift_seconds: int

    @classmethod
    def from_parameters(
        cls,
        as_of_date: date,
        parameters: Mapping[str, Any],
    ) -> "_DetectionCriteria":
        recent_days = int(parameters.get("recent_window_days", 7))
        reference_days = int(parameters.get("reference_window_days", 21))
        recent_start = as_of_date - timedelta(days=recent_days - 1)
        reference_end = recent_start - timedelta(days=1)
        return cls(
            recent_start=recent_start,
            reference_start=reference_end
            - timedelta(days=reference_days - 1),
            reference_end=reference_end,
            recent_minimum=int(
                parameters.get("recent_minimum_valid_days", 5)
            ),
            reference_minimum=int(
                parameters.get("reference_minimum_valid_days", 14)
            ),
            minimum_probability=float(
                parameters.get("minimum_daily_use_probability", 0.7)
            ),
            minimum_shift_seconds=int(
                parameters.get("minimum_shift_minutes", 120)
            )
            * 60,
        )


class RoutineChangeDetectionService:
    """Compares recent and reference first-use times once per completed day."""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        policy_repository: SqlAlchemyPolicyRepository,
        publisher: EventPublisher,
        timezone_name: str,
        metrics: AnalysisMetrics = METRICS,
    ) -> None:
        self._session_factory = session_factory
        self._policies = policy_repository
        self._publisher = publisher
        self._timezone = ZoneInfo(timezone_name)
        self._metrics = metrics
        self._active_signatures: dict[tuple[str, str], str] = {}

    def detect_and_publish(self, as_of_date: date) -> int:
        started_at = perf_counter()
        result = "not_detected"
        try:
            policy = self._policies.get("ROUTINE_CHANGED")
            if policy is None:
                result = "skipped"
                return 0
            criteria = _DetectionCriteria.from_parameters(
                as_of_date,
                policy.parameters,
            )

            with self._session_factory() as session:
                by_household = self._observations_by_household(
                    session,
                    criteria.reference_start,
                    as_of_date,
                )
                detected = sum(
                    self._detect_for_household(
                        session,
                        household_id,
                        household_observations,
                        as_of_date,
                        criteria,
                    )
                    for household_id, household_observations in by_household.items()
                )
            result = "detected" if detected else "not_detected"
            return detected
        except Exception:
            result = "error"
            raise
        finally:
            self._metrics.observe_pattern_detection(
                "ROUTINE_CHANGED",
                perf_counter() - started_at,
            )
            self._metrics.record_pattern_detection("ROUTINE_CHANGED", result)

    @staticmethod
    def _observations_by_household(
        session: Session,
        start_date: date,
        end_date: date,
    ) -> dict[str, list[HouseholdObservationDaily]]:
        observations = session.scalars(
            select(HouseholdObservationDaily)
            .where(
                HouseholdObservationDaily.observation_date >= start_date,
                HouseholdObservationDaily.observation_date <= end_date,
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
        return by_household

    def _detect_for_household(
        self,
        session: Session,
        household_id: str,
        observations: list[HouseholdObservationDaily],
        as_of_date: date,
        criteria: _DetectionCriteria,
    ) -> int:
        recent_observations = self._observations_between(
            observations,
            criteria.recent_start,
            as_of_date,
        )
        reference_observations = self._observations_between(
            observations,
            criteria.reference_start,
            criteria.reference_end,
        )
        if (
            len(recent_observations) < criteria.recent_minimum
            or len(reference_observations) < criteria.reference_minimum
        ):
            return 0

        first_uses = self._first_use_seconds(session, observations)
        return sum(
            self._detect_for_appliance(
                household_id,
                appliance_type,
                first_uses.get(appliance_type, {}),
                recent_observations,
                reference_observations,
                criteria,
            )
            for appliance_type in APPLIANCE_TYPES
        )

    def _detect_for_appliance(
        self,
        household_id: str,
        appliance_type: str,
        uses: dict[date, int],
        recent_observations: list[HouseholdObservationDaily],
        reference_observations: list[HouseholdObservationDaily],
        criteria: _DetectionCriteria,
    ) -> int:
        key = (household_id, appliance_type)
        recent_values = self._use_values(uses, recent_observations)
        reference_values = self._use_values(uses, reference_observations)
        if (
            len(recent_values) / len(recent_observations)
            < criteria.minimum_probability
            or len(reference_values) / len(reference_observations)
            < criteria.minimum_probability
        ):
            self._active_signatures.pop(key, None)
            return 0

        recent_median = round(statistics.median(recent_values))
        reference_median = round(statistics.median(reference_values))
        shift_seconds = self._signed_circular_difference(
            reference_median,
            recent_median,
        )
        if abs(shift_seconds) < criteria.minimum_shift_seconds:
            self._active_signatures.pop(key, None)
            return 0

        direction = "LATER" if shift_seconds > 0 else "EARLIER"
        recent_bucket = round(recent_median / 1800) * 1800
        signature = f"{direction}:{recent_bucket}"
        if self._active_signatures.get(key) == signature:
            return 0

        event = self._change_event(
            household_id,
            appliance_type,
            signature,
            direction,
            reference_median,
            recent_median,
            shift_seconds,
            len(recent_observations),
            len(reference_observations),
        )
        self._publisher.publish(event)
        self._metrics.record_pattern_event(event.event_type)
        self._active_signatures[key] = signature
        return 1

    @staticmethod
    def _observations_between(
        observations: list[HouseholdObservationDaily],
        start_date: date,
        end_date: date,
    ) -> list[HouseholdObservationDaily]:
        return [
            observation
            for observation in observations
            if start_date <= observation.observation_date <= end_date
        ]

    @staticmethod
    def _use_values(
        uses: dict[date, int],
        observations: list[HouseholdObservationDaily],
    ) -> list[int]:
        return [
            uses[observation.observation_date]
            for observation in observations
            if observation.observation_date in uses
        ]

    def _change_event(
        self,
        household_id: str,
        appliance_type: str,
        signature: str,
        direction: str,
        reference_median: int,
        recent_median: int,
        shift_seconds: int,
        recent_valid_days: int,
        reference_valid_days: int,
    ) -> AnalysisEvent:
        return AnalysisEvent(
            event_id=event_id_for(
                household_id,
                "ROUTINE_CHANGED",
                appliance_type,
                signature,
            ),
            household_id=household_id,
            event_type="ROUTINE_CHANGED",
            occurred_at=datetime.now(timezone.utc),
            reason={
                "appliance_type": appliance_type,
                "change_type": "FIRST_USE_TIME_SHIFT",
                "direction": direction,
                "previous_time": self._format_seconds(reference_median),
                "recent_time": self._format_seconds(recent_median),
                "shift_minutes": round(shift_seconds / 60),
                "recent_valid_days": recent_valid_days,
                "reference_valid_days": reference_valid_days,
            },
        )

    def _first_use_seconds(
        self,
        session: Session,
        observations: list[HouseholdObservationDaily],
    ) -> dict[str, dict[date, int]]:
        observation_by_id = {item.id: item for item in observations}
        rows = session.execute(
            select(
                HouseholdActivityDaily.observation_daily_id,
                HouseholdActivityDaily.appliance_type,
                ApplianceUsageSession.started_at,
                ApplianceUsageSession.ended_at,
            )
            .join(
                ApplianceUsageSession,
                ApplianceUsageSession.activity_daily_id
                == HouseholdActivityDaily.id,
            )
            .where(
                HouseholdActivityDaily.observation_daily_id.in_(
                    list(observation_by_id)
                )
            )
        ).all()

        grouped: dict[
            tuple[str, date],
            list[tuple[datetime, datetime]],
        ] = defaultdict(list)
        for observation_id, appliance_type, started_at, ended_at in rows:
            observation_date = observation_by_id[observation_id].observation_date
            local_day_start = datetime.combine(
                observation_date,
                time.min,
                self._timezone,
            )
            day_start = local_day_start.astimezone(timezone.utc)
            day_end = (local_day_start + timedelta(days=1)).astimezone(
                timezone.utc
            )
            start = max(self._as_utc(started_at), day_start)
            end = min(
                self._as_utc(ended_at) if ended_at is not None else day_end,
                day_end,
            )
            if end > start:
                grouped[(appliance_type, observation_date)].append((start, end))

        result: dict[str, dict[date, int]] = defaultdict(dict)
        for (appliance_type, observation_date), sessions in grouped.items():
            first_use = first_valid_logical_use(appliance_type, sessions)
            if first_use is None:
                continue
            local_start = first_use.astimezone(self._timezone)
            result[appliance_type][observation_date] = (
                local_start.hour * 3600
                + local_start.minute * 60
                + local_start.second
            )
        return result

    @staticmethod
    def _signed_circular_difference(previous: int, recent: int) -> int:
        return (recent - previous + 43_200) % 86_400 - 43_200

    @staticmethod
    def _format_seconds(seconds: int) -> str:
        seconds %= 86_400
        hour, remainder = divmod(seconds, 3600)
        minute = remainder // 60
        return f"{hour:02d}:{minute:02d}"

    @staticmethod
    def _as_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
