"""Daily absolute activity-index calculation and scheduled publication."""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Callable, Collection, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from threading import Event, Thread
from time import perf_counter
from typing import Literal, Protocol
from uuid import UUID, uuid5
from zoneinfo import ZoneInfo

from sqlalchemy import distinct, or_, select
from sqlalchemy.orm import Session, sessionmaker

from realtime_analysis.metrics import AnalysisMetrics, METRICS
from realtime_analysis.models import (
    ApplianceUsageSession,
    HouseholdActivityDaily,
    HouseholdObservationDaily,
)
from realtime_analysis.schemas import ActivityIndexComponents, ActivityIndexMessage


logger = logging.getLogger(__name__)

ACTIVITY_MESSAGE_NAMESPACE = UUID("75b0266d-d7ac-4f0d-b09c-79b41dd28af5")
MINIMUM_SESSION_SECONDS = 10
COUNT_TARGET = 6
DIVERSITY_TARGET = 3
DURATION_TARGET_SECONDS = 3_600
COUNT_CAP_PER_APPLIANCE = 3

# Long operation must not keep raising the activity score indefinitely. These
# caps affect only the score; components.usage_duration_seconds keeps the raw
# measured duration for monitoring and audits.
DURATION_SCORE_CAP_SECONDS = {
    "KETTLE": 10 * 60,
    "MICROWAVE": 30 * 60,
    "HAIR_DRYER": 30 * 60,
    "IRON": 60 * 60,
    "VACUUM_CLEANER": 90 * 60,
    "INDUCTION": 180 * 60,
}

# Thermostat/inverter duty cycles can create several nearby model sessions for
# one human action. Hysteresis filters sample noise; these gaps merge the
# remaining device-specific cycling into one logical use.
SESSION_MERGE_GAP_SECONDS = {
    "KETTLE": 60,
    "MICROWAVE": 60,
    "HAIR_DRYER": 60,
    "VACUUM_CLEANER": 60,
    "INDUCTION": 120,
    "IRON": 300,
}


class ActivityMessagePublisher(Protocol):
    def publish(self, message: ActivityIndexMessage) -> None: ...


class DailyBaselineUpdater(Protocol):
    def update(self, as_of_date: date) -> int: ...


class DailyEventDetector(Protocol):
    def detect_and_publish(self, as_of_date: date) -> int: ...


@dataclass(frozen=True)
class SessionSlice:
    appliance_type: str
    started_at: datetime
    ended_at: datetime
    started_in_day: bool

    @property
    def duration_seconds(self) -> float:
        return (self.ended_at - self.started_at).total_seconds()


@dataclass
class LogicalUse:
    appliance_type: str
    started_at: datetime
    ended_at: datetime
    active_seconds: float
    started_in_day: bool


class DailyActivityIndexCalculator:
    """Turns validated appliance sessions into a 0-100 absolute daily index."""

    def calculate(self, sessions: Sequence[SessionSlice]) -> ActivityIndexComponents:
        uses = self._logical_uses(sessions)
        valid_uses = [
            use for use in uses if use.active_seconds >= MINIMUM_SESSION_SECONDS
        ]

        counts_by_appliance: dict[str, int] = defaultdict(int)
        duration_by_appliance: dict[str, float] = defaultdict(float)
        used_appliances: set[str] = set()

        for use in valid_uses:
            used_appliances.add(use.appliance_type)
            duration_by_appliance[use.appliance_type] += use.active_seconds
            if use.started_in_day:
                counts_by_appliance[use.appliance_type] += 1

        usage_count = sum(counts_by_appliance.values())
        usage_duration_seconds = round(sum(duration_by_appliance.values()))

        effective_count = sum(
            min(count, COUNT_CAP_PER_APPLIANCE)
            for count in counts_by_appliance.values()
        )
        effective_duration = sum(
            min(
                seconds,
                DURATION_SCORE_CAP_SECONDS.get(
                    appliance_type,
                    DURATION_TARGET_SECONDS,
                ),
            )
            for appliance_type, seconds in duration_by_appliance.items()
        )

        return ActivityIndexComponents(
            usage_count=usage_count,
            appliance_type_count=len(used_appliances),
            usage_duration_seconds=usage_duration_seconds,
            usage_count_score=self._score(effective_count, COUNT_TARGET),
            appliance_diversity_score=self._score(
                len(used_appliances),
                DIVERSITY_TARGET,
            ),
            usage_duration_score=self._score(
                effective_duration,
                DURATION_TARGET_SECONDS,
            ),
        )

    @staticmethod
    def activity_index(components: ActivityIndexComponents) -> int:
        return round(
            components.usage_count_score * 0.50
            + components.appliance_diversity_score * 0.30
            + components.usage_duration_score * 0.20
        )

    @staticmethod
    def _score(value: float, target: float) -> int:
        return round(min(value / target, 1.0) * 100)

    @staticmethod
    def _logical_uses(sessions: Sequence[SessionSlice]) -> list[LogicalUse]:
        by_appliance: dict[str, list[SessionSlice]] = defaultdict(list)
        for session in sessions:
            if session.duration_seconds > 0:
                by_appliance[session.appliance_type].append(session)

        result: list[LogicalUse] = []
        for appliance_type, appliance_sessions in by_appliance.items():
            merge_gap = SESSION_MERGE_GAP_SECONDS.get(appliance_type, 60)
            current: LogicalUse | None = None
            for session in sorted(appliance_sessions, key=lambda item: item.started_at):
                if current is None:
                    current = LogicalUse(
                        appliance_type=appliance_type,
                        started_at=session.started_at,
                        ended_at=session.ended_at,
                        active_seconds=session.duration_seconds,
                        started_in_day=session.started_in_day,
                    )
                    continue

                gap_seconds = (session.started_at - current.ended_at).total_seconds()
                if gap_seconds <= merge_gap:
                    current.ended_at = max(current.ended_at, session.ended_at)
                    current.active_seconds += session.duration_seconds
                    continue

                result.append(current)
                current = LogicalUse(
                    appliance_type=appliance_type,
                    started_at=session.started_at,
                    ended_at=session.ended_at,
                    active_seconds=session.duration_seconds,
                    started_in_day=session.started_in_day,
                )
            if current is not None:
                result.append(current)
        return result


class DailyActivityIndexRepository:
    """Finalizes one local date and builds one message per known household."""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        timezone_name: str,
        expected_samples_per_day: int,
        valid_coverage_ratio: float,
        calculator: DailyActivityIndexCalculator | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._timezone = ZoneInfo(timezone_name)
        self._expected_samples_per_day = expected_samples_per_day
        self._valid_coverage_ratio = Decimal(str(valid_coverage_ratio))
        self._calculator = calculator or DailyActivityIndexCalculator()

    def build_messages(
        self,
        activity_date: date,
        configured_household_ids: Collection[str] = (),
        finalized_at: datetime | None = None,
    ) -> list[ActivityIndexMessage]:
        finalized_at = finalized_at or datetime.now(timezone.utc)
        with self._session_factory.begin() as session:
            known_households = set(configured_household_ids)
            known_households.update(
                session.scalars(
                    select(distinct(HouseholdObservationDaily.household_id))
                ).all()
            )

            messages: list[ActivityIndexMessage] = []
            for household_id in sorted(known_households):
                observation = self._finalize_observation(
                    session,
                    household_id,
                    activity_date,
                    finalized_at,
                )
                if observation.observation_status != "VALID":
                    messages.append(
                        self._message(
                            household_id,
                            activity_date,
                            activity_index=None,
                            data_status="INSUFFICIENT_DATA",
                        )
                    )
                    continue

                sessions = self._session_slices(
                    session,
                    household_id,
                    activity_date,
                )
                components = self._calculator.calculate(sessions)
                messages.append(
                    self._message(
                        household_id,
                        activity_date,
                        activity_index=self._calculator.activity_index(components),
                        data_status="VALID",
                        components=components,
                    )
                )
            return messages

    def _finalize_observation(
        self,
        session: Session,
        household_id: str,
        activity_date: date,
        finalized_at: datetime,
    ) -> HouseholdObservationDaily:
        observation = session.scalar(
            select(HouseholdObservationDaily)
            .where(
                HouseholdObservationDaily.household_id == household_id,
                HouseholdObservationDaily.observation_date == activity_date,
            )
            .with_for_update()
        )
        if observation is None:
            observation = HouseholdObservationDaily(
                household_id=household_id,
                observation_date=activity_date,
                sample_count=0,
                expected_sample_count=self._expected_samples_per_day,
                coverage_ratio=Decimal("0.0000"),
                observation_status="SENSOR_GAP",
                updated_at=finalized_at,
            )
            session.add(observation)
            session.flush()
            return observation

        expected = observation.expected_sample_count or self._expected_samples_per_day
        raw_ratio = (
            Decimal(observation.sample_count)
            / Decimal(expected)
        )
        display_ratio = min(
            Decimal("1.0000"),
            raw_ratio.quantize(Decimal("0.0001")),
        )
        observation.coverage_ratio = display_ratio
        if observation.sample_count == 0:
            observation.observation_status = "SENSOR_GAP"
        elif raw_ratio >= self._valid_coverage_ratio:
            observation.observation_status = "VALID"
        else:
            observation.observation_status = "INSUFFICIENT_DATA"
        observation.updated_at = finalized_at
        return observation

    def _session_slices(
        self,
        session: Session,
        household_id: str,
        activity_date: date,
    ) -> list[SessionSlice]:
        day_start = datetime.combine(activity_date, time.min, self._timezone)
        day_end = day_start + timedelta(days=1)
        start_utc = day_start.astimezone(timezone.utc)
        end_utc = day_end.astimezone(timezone.utc)

        rows = session.execute(
            select(ApplianceUsageSession, HouseholdActivityDaily.appliance_type)
            .join(
                HouseholdActivityDaily,
                HouseholdActivityDaily.id == ApplianceUsageSession.activity_daily_id,
            )
            .join(
                HouseholdObservationDaily,
                HouseholdObservationDaily.id
                == HouseholdActivityDaily.observation_daily_id,
            )
            .where(
                HouseholdObservationDaily.household_id == household_id,
                ApplianceUsageSession.started_at < end_utc,
                or_(
                    ApplianceUsageSession.ended_at.is_(None),
                    ApplianceUsageSession.ended_at > start_utc,
                ),
            )
        ).all()

        slices: list[SessionSlice] = []
        for usage_session, appliance_type in rows:
            original_start = self._as_utc(usage_session.started_at)
            original_end = (
                self._as_utc(usage_session.ended_at)
                if usage_session.ended_at is not None
                else end_utc
            )
            clipped_start = max(original_start, start_utc)
            clipped_end = min(original_end, end_utc)
            if clipped_end <= clipped_start:
                continue
            slices.append(
                SessionSlice(
                    appliance_type=appliance_type,
                    started_at=clipped_start,
                    ended_at=clipped_end,
                    started_in_day=start_utc <= original_start < end_utc,
                )
            )
        return slices

    @staticmethod
    def _as_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @staticmethod
    def _message(
        household_id: str,
        activity_date: date,
        activity_index: int | None,
        data_status: Literal["VALID", "INSUFFICIENT_DATA"],
        components: ActivityIndexComponents | None = None,
    ) -> ActivityIndexMessage:
        message_id = uuid5(
            ACTIVITY_MESSAGE_NAMESPACE,
            f"{household_id}:{activity_date.isoformat()}",
        )
        return ActivityIndexMessage(
            message_id=message_id,
            household_id=household_id,
            activity_date=activity_date,
            activity_index=activity_index,
            data_status=data_status,
            components=components,
        )


class DailyActivityIndexService:
    def __init__(
        self,
        repository: DailyActivityIndexRepository,
        publisher: ActivityMessagePublisher,
        configured_household_ids: Collection[str],
        baseline_updater: DailyBaselineUpdater | None = None,
        daily_event_detector: DailyEventDetector | None = None,
        metrics: AnalysisMetrics = METRICS,
    ) -> None:
        self._repository = repository
        self._publisher = publisher
        self._configured_household_ids = tuple(configured_household_ids)
        self._baseline_updater = baseline_updater
        self._daily_event_detector = daily_event_detector
        self._metrics = metrics

    def publish_date(self, activity_date: date) -> int:
        published = self._run_daily_job(
            "activity_index",
            lambda: self._publish_activity_index(activity_date),
        )
        if self._daily_event_detector is not None:
            self._run_daily_job(
                "routine_changed",
                lambda: self._daily_event_detector.detect_and_publish(activity_date),
            )
        if self._baseline_updater is not None:
            self._run_daily_job(
                "baseline_update",
                lambda: self._baseline_updater.update(activity_date),
            )
        return published

    def _publish_activity_index(self, activity_date: date) -> int:
        messages = self._repository.build_messages(
            activity_date,
            self._configured_household_ids,
        )
        for message in messages:
            self._publisher.publish(message)
        logger.info(
            "Daily activity index published: date=%s households=%s",
            activity_date,
            len(messages),
        )
        return len(messages)

    def _run_daily_job(
        self,
        job: str,
        operation: Callable[[], int],
    ) -> int:
        started_at = perf_counter()
        status = "success"
        try:
            return operation()
        except Exception:
            status = "error"
            raise
        finally:
            self._metrics.observe_daily_job(job, perf_counter() - started_at)
            self._metrics.record_daily_job(job, status)


class DailyActivityIndexScheduler:
    """Publishes the latest completed local date once per running process."""

    def __init__(
        self,
        service: DailyActivityIndexService,
        timezone_name: str,
        publish_hour: int,
        publish_minute: int,
        poll_seconds: float,
        run_repository: object | None = None,
        completion_hook: Callable[[date], object] | None = None,
    ) -> None:
        self._service = service
        self._timezone = ZoneInfo(timezone_name)
        self._publish_time = time(publish_hour, publish_minute)
        self._poll_seconds = poll_seconds
        self._run_repository = run_repository
        self._completion_hook = completion_hook
        self._thread: Thread | None = None
        self._stop_event: Event | None = None
        self._last_published_date: date | None = None

    def start(self, stop_event: Event) -> None:
        if self._thread is not None:
            raise RuntimeError("Daily activity scheduler is already running")
        self._stop_event = stop_event
        self._thread = Thread(
            target=self._run,
            name="daily-activity-index",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._thread = None

    def due_activity_date(self, now: datetime) -> date:
        local_now = now.astimezone(self._timezone)
        latest = local_now.date() - timedelta(days=1)
        if local_now.timetz().replace(tzinfo=None) < self._publish_time:
            latest -= timedelta(days=1)
        return latest

    def run_due(self, now: datetime | None = None) -> bool:
        now = now or datetime.now(timezone.utc)
        activity_date = self.due_activity_date(now)
        if self._last_published_date == activity_date:
            return False
        ran_daily = True
        if self._run_repository is None:
            self._service.publish_date(activity_date)
        elif self._run_repository.has_succeeded("daily-aggregation", activity_date):
            ran_daily = False
        else:
            self._run_repository.run(
                "daily-aggregation",
                activity_date,
                lambda: self._service.publish_date(activity_date),
            )
        if self._completion_hook is not None:
            self._completion_hook(activity_date)
        self._last_published_date = activity_date
        return ran_daily

    def _run(self) -> None:
        assert self._stop_event is not None
        while not self._stop_event.is_set():
            try:
                self.run_due()
            except Exception:
                logger.exception("Daily activity index publication failed")
            self._stop_event.wait(self._poll_seconds)
