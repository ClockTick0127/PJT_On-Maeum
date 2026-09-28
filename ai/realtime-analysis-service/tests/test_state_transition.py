from datetime import datetime, timedelta

from realtime_analysis.schemas import (
    ApplianceState,
    ApplianceStateTransition,
    ApplianceTransitionType,
)
from realtime_analysis.state_transition import ApplianceStateTransitionDetector


START = datetime.fromisoformat("2026-09-09T09:00:00+09:00")


def state(probability: float) -> ApplianceState:
    return ApplianceState(
        appliance_type="MICROWAVE",
        probability=probability,
        threshold=0.5,
        is_on=probability >= 0.5,
    )


def detect(
    detector: ApplianceStateTransitionDetector,
    second: int,
    probability: float,
) -> list[ApplianceStateTransition]:
    return detector.detect(
        "H001",
        START + timedelta(seconds=second),
        [state(probability)],
    )


def test_on_requires_three_consecutive_predictions() -> None:
    detector = ApplianceStateTransitionDetector(3, 3, 0.05)

    assert detect(detector, 0, 0.8) == []
    assert detect(detector, 1, 0.9) == []
    transitions = detect(detector, 2, 0.7)

    assert len(transitions) == 1
    transition = transitions[0]
    assert transition.transition_type == ApplianceTransitionType.TURNED_ON
    assert transition.occurred_at == START
    assert transition.confirmed_at == START + timedelta(seconds=2)
    assert detector.is_on("H001", "MICROWAVE") is True


def test_hysteresis_holds_on_until_probability_is_below_off_threshold() -> None:
    detector = ApplianceStateTransitionDetector(1, 2, 0.05)
    assert len(detect(detector, 0, 0.8)) == 1

    assert detect(detector, 1, 0.47) == []
    assert detector.is_on("H001", "MICROWAVE") is True
    assert detect(detector, 2, 0.44) == []
    transitions = detect(detector, 3, 0.40)

    assert len(transitions) == 1
    assert transitions[0].transition_type == ApplianceTransitionType.TURNED_OFF
    assert transitions[0].occurred_at == START + timedelta(seconds=2)
    assert detector.is_on("H001", "MICROWAVE") is False


def test_explicit_model_off_threshold_overrides_global_margin() -> None:
    detector = ApplianceStateTransitionDetector(
        1,
        1,
        0.05,
        off_thresholds={"MICROWAVE": 0.2},
    )
    assert len(detect(detector, 0, 0.8)) == 1

    assert detect(detector, 1, 0.3) == []
    assert detector.is_on("H001", "MICROWAVE") is True
    transitions = detect(detector, 2, 0.2)

    assert len(transitions) == 1
    assert transitions[0].transition_type == ApplianceTransitionType.TURNED_OFF


def test_equal_model_threshold_keeps_on_at_boundary() -> None:
    detector = ApplianceStateTransitionDetector(
        1,
        1,
        0,
        off_thresholds={"MICROWAVE": 0.5},
    )
    assert len(detect(detector, 0, 0.5)) == 1

    assert detect(detector, 1, 0.5) == []
    assert detector.is_on("H001", "MICROWAVE") is True
    transitions = detect(detector, 2, 0.49)

    assert len(transitions) == 1
    assert transitions[0].transition_type == ApplianceTransitionType.TURNED_OFF


def test_probability_bounce_resets_pending_transition() -> None:
    detector = ApplianceStateTransitionDetector(3, 3, 0.05)

    assert detect(detector, 0, 0.8) == []
    assert detect(detector, 1, 0.2) == []
    assert detect(detector, 2, 0.8) == []
    assert detect(detector, 3, 0.8) == []
    assert len(detect(detector, 4, 0.8)) == 1


def test_duplicate_or_older_observation_is_ignored() -> None:
    detector = ApplianceStateTransitionDetector(1, 1, 0.05)

    assert len(detect(detector, 1, 0.8)) == 1
    assert detect(detector, 1, 0.1) == []
    assert detect(detector, 0, 0.1) == []
    assert detector.is_on("H001", "MICROWAVE") is True


def test_reset_preserves_other_household_state() -> None:
    detector = ApplianceStateTransitionDetector(1, 1, 0.05)
    detector.detect("H001", START, [state(0.8)])
    detector.detect("H002", START, [state(0.8)])

    detector.reset("H001")

    assert detector.is_on("H001", "MICROWAVE") is False
    assert detector.is_on("H002", "MICROWAVE") is True
