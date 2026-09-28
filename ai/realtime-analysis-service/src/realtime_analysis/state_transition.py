"""Debounced and hysteretic appliance ON/OFF transition detection."""

from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime

from realtime_analysis.schemas import (
    ApplianceState,
    ApplianceStateTransition,
    ApplianceTransitionType,
)


@dataclass
class _ApplianceMemory:
    stable_is_on: bool = False
    candidate_is_on: bool | None = None
    candidate_count: int = 0
    candidate_started_at: datetime | None = None
    last_observed_at: datetime | None = None


class ApplianceStateTransitionDetector:
    """Turn raw model states into stable transitions per household and appliance."""

    def __init__(
        self,
        on_confirmation_samples: int,
        off_confirmation_samples: int,
        off_threshold_margin: float,
        *,
        off_thresholds: Mapping[str, float] | None = None,
    ) -> None:
        if on_confirmation_samples < 1 or off_confirmation_samples < 1:
            raise ValueError("confirmation samples must be at least one")
        if not 0 <= off_threshold_margin <= 1:
            raise ValueError("off_threshold_margin must be between zero and one")
        if off_thresholds is not None and any(
            not 0 <= threshold <= 1 for threshold in off_thresholds.values()
        ):
            raise ValueError("off thresholds must be between zero and one")

        self._on_confirmation_samples = on_confirmation_samples
        self._off_confirmation_samples = off_confirmation_samples
        self._off_threshold_margin = off_threshold_margin
        self._off_thresholds = dict(off_thresholds or {})
        self._memory: dict[tuple[str, str], _ApplianceMemory] = {}

    def detect(
        self,
        household_id: str,
        observed_at: datetime,
        states: Sequence[ApplianceState],
    ) -> list[ApplianceStateTransition]:
        transitions: list[ApplianceStateTransition] = []
        for state in states:
            transition = self._detect_one(household_id, observed_at, state)
            if transition is not None:
                transitions.append(transition)
        return transitions

    def is_on(self, household_id: str, appliance_type: str) -> bool:
        memory = self._memory.get((household_id, appliance_type))
        return memory.stable_is_on if memory is not None else False

    def reset(self, household_id: str) -> None:
        """Discard hysteresis state whose continuity was broken by a data gap."""

        stale_keys = [
            key for key in self._memory if key[0] == household_id
        ]
        for key in stale_keys:
            self._memory.pop(key, None)

    def checkpoint(self, household_id: str) -> dict[str, _ApplianceMemory]:
        """Copy household state so a failed DB transaction can be retried safely."""

        return {
            appliance_type: deepcopy(memory)
            for (saved_household, appliance_type), memory in self._memory.items()
            if saved_household == household_id
        }

    def restore(
        self,
        household_id: str,
        checkpoint: dict[str, _ApplianceMemory],
    ) -> None:
        self.reset(household_id)
        for appliance_type, memory in checkpoint.items():
            self._memory[(household_id, appliance_type)] = deepcopy(memory)

    def _detect_one(
        self,
        household_id: str,
        observed_at: datetime,
        state: ApplianceState,
    ) -> ApplianceStateTransition | None:
        key = (household_id, state.appliance_type)
        memory = self._memory.setdefault(key, _ApplianceMemory())
        if (
            memory.last_observed_at is not None
            and observed_at <= memory.last_observed_at
        ):
            return None
        memory.last_observed_at = observed_at

        target_is_on = self._target_state(memory.stable_is_on, state)
        if target_is_on is None or target_is_on == memory.stable_is_on:
            self._reset_candidate(memory)
            return None

        if memory.candidate_is_on != target_is_on:
            memory.candidate_is_on = target_is_on
            memory.candidate_count = 1
            memory.candidate_started_at = observed_at
        else:
            memory.candidate_count += 1

        required = (
            self._on_confirmation_samples
            if target_is_on
            else self._off_confirmation_samples
        )
        if memory.candidate_count < required:
            return None

        previous_is_on = memory.stable_is_on
        occurred_at = memory.candidate_started_at or observed_at
        memory.stable_is_on = target_is_on
        self._reset_candidate(memory)
        return ApplianceStateTransition(
            household_id=household_id,
            appliance_type=state.appliance_type,
            transition_type=(
                ApplianceTransitionType.TURNED_ON
                if target_is_on
                else ApplianceTransitionType.TURNED_OFF
            ),
            previous_is_on=previous_is_on,
            current_is_on=target_is_on,
            occurred_at=occurred_at,
            confirmed_at=observed_at,
            probability=state.probability,
            threshold=state.threshold,
        )

    def _target_state(
        self,
        stable_is_on: bool,
        state: ApplianceState,
    ) -> bool | None:
        if not stable_is_on:
            return True if state.probability >= state.threshold else False

        explicit_off_threshold = self._off_thresholds.get(state.appliance_type)
        if explicit_off_threshold is not None:
            if state.probability >= state.threshold:
                return True
            if explicit_off_threshold == state.threshold:
                return False if state.probability < explicit_off_threshold else None
            if state.probability <= explicit_off_threshold:
                return False
            return None

        off_threshold = max(0.0, state.threshold - self._off_threshold_margin)
        if state.probability <= off_threshold:
            return False
        if state.probability >= state.threshold:
            return True
        return None

    @staticmethod
    def _reset_candidate(memory: _ApplianceMemory) -> None:
        memory.candidate_is_on = None
        memory.candidate_count = 0
        memory.candidate_started_at = None
