"""In-memory daily appliance usage and emitted-event state."""

from datetime import date

from realtime_analysis.schemas import (
    ApplianceState,
    ApplianceStateTransition,
    ApplianceTransitionType,
)

# AI가 가전을 ON으로 판단하면 오늘 사용한 가전으로 기록 
class DailyActivityTracker:
    def __init__(self) -> None:
        self._used: set[tuple[str, date, str]] = set()
        self._emitted: set[tuple[str, date, str, str]] = set()

    def record_states(
        self,
        household_id: str,
        activity_date: date,
        states: list[ApplianceState],
    ) -> None:
        for state in states:
            if state.is_on:
                self._used.add(
                    (household_id, activity_date, state.appliance_type)
                )

    def record_transitions(
        self,
        activity_date: date,
        transitions: list[ApplianceStateTransition],
    ) -> None:
        for transition in transitions:
            if transition.transition_type == ApplianceTransitionType.TURNED_ON:
                self._used.add(
                    (
                        transition.household_id,
                        activity_date,
                        transition.appliance_type,
                    )
                )

    def was_used(
        self,
        household_id: str,
        activity_date: date,
        appliance_type: str,
    ) -> bool:
        return (household_id, activity_date, appliance_type) in self._used

    def was_emitted(
        self,
        household_id: str,
        activity_date: date,
        appliance_type: str,
        baseline_type: str,
    ) -> bool:
        return (
            household_id,
            activity_date,
            appliance_type,
            baseline_type,
        ) in self._emitted

    def mark_emitted(
        self,
        household_id: str,
        activity_date: date,
        appliance_type: str,
        baseline_type: str,
    ) -> None:
        self._emitted.add(
            (household_id, activity_date, appliance_type, baseline_type)
        )
