from datetime import datetime, timedelta, timezone

from realtime_analysis.data_quality_monitor import DataQualityMonitor
from realtime_analysis.schemas import DataQualityEvent


BASE = datetime.fromisoformat("2026-09-17T10:00:00+09:00")


class RecordingPublisher:
    def __init__(self) -> None:
        self.events: list[DataQualityEvent] = []

    def publish(self, event: DataQualityEvent) -> None:
        self.events.append(event)


def test_gap_is_emitted_once_and_recovery_requires_confirmed_samples() -> None:
    publisher = RecordingPublisher()
    closed_sessions: list[tuple[str, datetime]] = []
    monitor = DataQualityMonitor(
        publisher,
        gap_threshold_seconds=120,
        recovery_confirmation_samples=3,
        on_gap=lambda household_id, measured_at: closed_sessions.append(
            (household_id, measured_at)
        ),
    )

    first = monitor.observe("H001", BASE, received_at=BASE)
    assert first.is_healthy is True
    assert monitor.detect_gaps(checked_at=BASE + timedelta(seconds=119)) == 0

    assert monitor.detect_gaps(checked_at=BASE + timedelta(seconds=120)) == 1
    assert monitor.detect_gaps(checked_at=BASE + timedelta(seconds=180)) == 0
    assert monitor.is_healthy("H001") is False
    assert closed_sessions == [("H001", BASE.astimezone(timezone.utc))]

    gap = publisher.events[0]
    assert gap.event_type == "DATA_GAP"
    assert gap.reason == {
        "last_valid_received_at": BASE.astimezone(timezone.utc).isoformat(),
        "gap_seconds": 120,
    }

    first_recovery = monitor.observe(
        "H001",
        BASE + timedelta(seconds=181),
        received_at=BASE + timedelta(seconds=181),
    )
    assert first_recovery.is_healthy is False
    assert first_recovery.reset_required is True
    second_recovery = monitor.observe(
        "H001",
        BASE + timedelta(seconds=182),
        received_at=BASE + timedelta(seconds=182),
    )
    assert second_recovery.is_healthy is False
    assert second_recovery.reset_required is False
    third_recovery = monitor.observe(
        "H001",
        BASE + timedelta(seconds=183),
        received_at=BASE + timedelta(seconds=183),
    )
    assert third_recovery.is_healthy is True
    assert monitor.is_healthy("H001") is True

    recovered = publisher.events[1]
    assert recovered.event_type == "DATA_RECOVERED"
    assert recovered.reason["gap_seconds"] == 183
    assert recovered.reason["gap_detected_at"] == (
        BASE + timedelta(seconds=120)
    ).astimezone(timezone.utc).isoformat()


def test_replayed_measurement_does_not_recover_a_gap() -> None:
    publisher = RecordingPublisher()
    monitor = DataQualityMonitor(
        publisher,
        gap_threshold_seconds=10,
        recovery_confirmation_samples=1,
    )
    monitor.observe("H001", BASE, received_at=BASE)
    monitor.detect_gaps(checked_at=BASE + timedelta(seconds=10))

    replay = monitor.observe(
        "H001",
        BASE,
        received_at=BASE + timedelta(seconds=11),
    )

    assert replay.is_healthy is False
    assert [event.event_type for event in publisher.events] == ["DATA_GAP"]


def test_gap_event_id_is_stable_for_the_same_episode() -> None:
    ids = []
    for _ in range(2):
        publisher = RecordingPublisher()
        monitor = DataQualityMonitor(
            publisher,
            gap_threshold_seconds=10,
            recovery_confirmation_samples=1,
        )
        monitor.observe("H001", BASE, received_at=BASE)
        monitor.detect_gaps(checked_at=BASE + timedelta(seconds=10))
        ids.append(publisher.events[0].event_id)

    assert ids[0] == ids[1]


def test_reset_forgets_only_one_household() -> None:
    monitor = DataQualityMonitor(
        RecordingPublisher(),
        gap_threshold_seconds=10,
        recovery_confirmation_samples=1,
    )
    monitor.observe("H001", BASE, received_at=BASE)
    monitor.observe("H002", BASE, received_at=BASE)
    assert monitor.detect_gaps(checked_at=BASE + timedelta(seconds=10)) == 2

    monitor.reset("H001")

    assert monitor.is_healthy("H001") is True
    assert monitor.is_healthy("H002") is False
