"""MVP routine-missed anomaly detection."""

from dataclasses import dataclass
from datetime import date, datetime, timezone
from uuid import uuid4
from zoneinfo import ZoneInfo

from realtime_analysis.schemas import AnalysisEvent, RoutineBaseline
from realtime_analysis.state_tracker import DailyActivityTracker


# 이상 점수 계산
@dataclass(frozen=True)
class PendingAnomaly:
    event: AnalysisEvent
    activity_date: date
    appliance_type: str
    baseline_type: str


class RoutineMissedDetector:
    def __init__(
        self,
        tracker: DailyActivityTracker,
        score_threshold: int,
        timezone_name: str,
    ) -> None:
        self._tracker = tracker
        self._score_threshold = score_threshold
        self._timezone = ZoneInfo(timezone_name)

    def detect(
        self,
        household_id: str,
        measured_at: datetime,
        baselines: list[RoutineBaseline],
    ) -> list[PendingAnomaly]:
        local_datetime = measured_at.astimezone(self._timezone)  # Kafka 메시지 시각을 한국으로 바꿈 
        activity_date = local_datetime.date()
        pending: list[PendingAnomaly] = []

        for baseline in baselines:
            if local_datetime.time().replace(tzinfo=None) < baseline.expected_until:  # 마감 시각 전이면 검사X
                continue
            if self._tracker.was_used(
                household_id, activity_date, baseline.appliance_type
            ):
                continue   
            if self._tracker.was_emitted(
                household_id,
                activity_date,
                baseline.appliance_type,
                baseline.baseline_type,
            ):
                continue

            # 점수 계산 
            score = min(
                100,
                round(
                    (baseline.normal_days / baseline.window_days)
                    * 100
                    * baseline.reliability_weight
                ),
            )
            if score < self._score_threshold:
                continue

            pending.append(
                PendingAnomaly(
                    event=AnalysisEvent(
                        event_id=uuid4(),
                        household_id=household_id,
                        score=score,
                        occurred_at=measured_at.astimezone(timezone.utc),
                        reason={
                            "expected_until": baseline.expected_until.strftime("%H:%M"),
                            "normal_days": baseline.normal_days,
                            "window_days": baseline.window_days,
                        },
                    ),
                    activity_date=activity_date,
                    appliance_type=baseline.appliance_type,
                    baseline_type=baseline.baseline_type,
                )
            )

        return pending

    def mark_emitted(self, anomaly: PendingAnomaly) -> None:
        self._tracker.mark_emitted(
            anomaly.event.household_id,
            anomaly.activity_date,
            anomaly.appliance_type,
            anomaly.baseline_type,
        )
