from datetime import datetime
from uuid import UUID

from realtime_analysis.anomaly_detector import RoutineMissedDetector
from realtime_analysis.schemas import ApplianceState, RoutineBaseline
from realtime_analysis.state_tracker import DailyActivityTracker


def make_baseline() -> RoutineBaseline:
    return RoutineBaseline(
        id=UUID("226dfc19-3222-49a4-8fde-a314573aa23e"),
        household_id="H001",
        appliance_type="MICROWAVE",
        expected_until="08:10",
        normal_days=12,
        window_days=14,
        reliability_weight=1.0,
    )


def test_detects_routine_missed_after_deadline() -> None:
    tracker = DailyActivityTracker()
    detector = RoutineMissedDetector(tracker, 80, "Asia/Seoul")
    measured_at = datetime.fromisoformat("2026-09-08T09:00:00+09:00")

    anomalies = detector.detect("H001", measured_at, [make_baseline()])

    assert len(anomalies) == 1
    assert anomalies[0].event.score == 86
    assert anomalies[0].event.reason == {
        "expected_until": "08:10",
        "normal_days": 12,
        "window_days": 14,
    }


def test_skips_routine_missed_when_appliance_was_used() -> None:
    tracker = DailyActivityTracker()
    detector = RoutineMissedDetector(tracker, 80, "Asia/Seoul")
    measured_at = datetime.fromisoformat("2026-09-08T09:00:00+09:00")
    tracker.record_states(
        "H001",
        measured_at.date(),
        [ApplianceState(appliance_type="MICROWAVE", is_on=True)],
    )

    anomalies = detector.detect("H001", measured_at, [make_baseline()])

    assert anomalies == []


def test_emits_same_daily_anomaly_only_once_after_marking() -> None:
    tracker = DailyActivityTracker()
    detector = RoutineMissedDetector(tracker, 80, "Asia/Seoul")
    measured_at = datetime.fromisoformat("2026-09-08T09:00:00+09:00")

    first = detector.detect("H001", measured_at, [make_baseline()])
    detector.mark_emitted(first[0])
    second = detector.detect("H001", measured_at, [make_baseline()])

    assert second == []
