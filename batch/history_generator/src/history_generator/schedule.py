"""Deterministic per-day usage plans: logical uses, their sessions, missing windows.

The plan is the ground truth the aggregation must reproduce, so it is printed by
``plan`` and consumed unchanged by the lake writer, the waveform generator and the
snapshot synthesizer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
import random

from history_generator.scenario import (
    MINIMUM_USE_SECONDS,
    SECONDS_PER_DAY,
    Household,
    Period,
    UseRule,
)


@dataclass(frozen=True)
class Session:
    """One appliance ON interval in local-day seconds, ``[start, end)``."""

    appliance: str
    start_second: int
    end_second: int

    @property
    def duration(self) -> int:
        return self.end_second - self.start_second


@dataclass(frozen=True)
class LogicalUse:
    appliance: str
    start_second: int
    end_second: int
    sessions: tuple[Session, ...]
    rule_label: str = ""


@dataclass(frozen=True)
class DayPlan:
    household_id: str
    day: date
    uses: tuple[LogicalUse, ...]
    missing_windows: tuple[tuple[int, int], ...] = ()
    applied_periods: tuple[str, ...] = ()

    @property
    def sessions(self) -> tuple[Session, ...]:
        return tuple(session for use in self.uses for session in use.sessions)

    def is_missing(self, second: int) -> bool:
        return any(a <= second < b for a, b in self.missing_windows)

    def first_use_second(self, appliance: str) -> int | None:
        starts = [use.start_second for use in self.uses if use.appliance == appliance]
        return min(starts) if starts else None

    def use_count(self, appliance: str) -> int:
        return sum(1 for use in self.uses if use.appliance == appliance)

    def is_on(self, appliance: str, second: int) -> bool:
        return any(s.start_second <= second < s.end_second
                   for use in self.uses if use.appliance == appliance for s in use.sessions)


def _effective_probability(rule: UseRule, periods: list[Period]) -> float:
    probability = rule.probability
    for period in periods:
        if "*" in period.probability_override:
            probability = period.probability_override["*"]
        if rule.appliance in period.probability_override:
            probability = period.probability_override[rule.appliance]
    return probability


def _shift_seconds(appliance: str, periods: list[Period]) -> int:
    return sum(period.shift_minutes.get(appliance, 0) for period in periods) * 60


def _draw_segments(rng: random.Random, rule: UseRule) -> tuple[int, list[int]]:
    """Always consume the same number of random values, used or not."""

    if rule.segments is None:
        return 1, []
    count = rng.randint(*rule.segments)
    gaps = [rng.randint(*rule.segment_gap_seconds) for _ in range(rule.segments[1] - 1)]
    return count, gaps


def _split(appliance: str, start: int, duration: int, count: int, gaps: list[int]) -> tuple[Session, ...]:
    if count <= 1:
        return (Session(appliance, start, start + duration),)
    base, extra = divmod(duration, count)
    sessions: list[Session] = []
    cursor = start
    for index in range(count):
        length = base + (1 if index < extra else 0)
        sessions.append(Session(appliance, cursor, cursor + length))
        cursor += length + (gaps[index] if index < count - 1 else 0)
    return tuple(sessions)


def _clamp_use(use: LogicalUse) -> LogicalUse | None:
    """Keep the use inside the day; drop sessions that fall out entirely."""

    sessions = tuple(Session(s.appliance, max(0, s.start_second), min(SECONDS_PER_DAY, s.end_second))
                     for s in use.sessions)
    sessions = tuple(s for s in sessions if s.duration >= MINIMUM_USE_SECONDS)
    if not sessions:
        return None
    return LogicalUse(use.appliance, sessions[0].start_second, sessions[-1].end_second, sessions, use.rule_label)


def plan_day(household: Household, day: date) -> DayPlan:
    """Build the day's plan. Same household, seed and day always give the same plan."""

    periods = [period for period in household.periods if period.covers(day)]
    rng = random.Random(f"{household.seed}:{household.household_id}:{day.isoformat()}")
    uses: list[LogicalUse] = []
    for rule in household.rules:
        # Draw every random value even when the rule is skipped so that period
        # edits never reshuffle the other rules of the day.
        roll = rng.random()
        offset = rng.uniform(-rule.jitter_seconds, rule.jitter_seconds) if rule.jitter_seconds else 0.0
        duration = rng.randint(*rule.duration_seconds)
        count, gaps = _draw_segments(rng, rule)
        if not rule.applies_on(day) or roll >= _effective_probability(rule, periods):
            continue
        start = rule.median_second + int(round(offset)) + _shift_seconds(rule.appliance, periods)
        sessions = _split(rule.appliance, start, duration, count, gaps)
        use = _clamp_use(LogicalUse(rule.appliance, start, sessions[-1].end_second, sessions, rule.label))
        if use is not None:
            uses.append(use)

    if any(period.keep_first_use_only for period in periods):
        first: dict[str, LogicalUse] = {}
        for use in uses:
            if use.appliance not in first or use.start_second < first[use.appliance].start_second:
                first[use.appliance] = use
        uses = list(first.values())

    extensions: dict[str, int] = {}
    for period in periods:
        extensions.update(period.extend_first_use_seconds)
    if extensions:
        rewritten: list[LogicalUse] = []
        seen: set[str] = set()
        for use in sorted(uses, key=lambda item: (item.start_second, item.appliance)):
            if use.appliance in extensions and use.appliance not in seen:
                seen.add(use.appliance)
                end = min(SECONDS_PER_DAY, use.start_second + extensions[use.appliance])
                use = LogicalUse(use.appliance, use.start_second, end,
                                 (Session(use.appliance, use.start_second, end),), use.rule_label)
            rewritten.append(use)
        uses = rewritten

    # One appliance cannot be on twice at once: keep the earlier use on overlap.
    resolved: list[LogicalUse] = []
    last_end: dict[str, int] = {}
    for use in sorted(uses, key=lambda item: (item.start_second, item.appliance)):
        if use.start_second < last_end.get(use.appliance, -1):
            continue
        resolved.append(use)
        last_end[use.appliance] = use.end_second

    missing: list[tuple[int, int]] = []
    for period in periods:
        missing.extend(period.missing_windows)
    return DayPlan(
        household_id=household.household_id,
        day=day,
        uses=tuple(resolved),
        missing_windows=tuple(sorted(missing)),
        applied_periods=tuple(period.name for period in periods),
    )


def plan_summary(plan: DayPlan) -> dict:
    """Small JSON-friendly summary used by ``plan`` and the manifest."""

    return {
        "household_id": plan.household_id,
        "date": plan.day.isoformat(),
        "periods": list(plan.applied_periods),
        "missing_seconds": sum(b - a for a, b in plan.missing_windows),
        "uses": [
            {
                "appliance": use.appliance,
                "start": _clock(use.start_second),
                "end": _clock(use.end_second),
                "sessions": len(use.sessions),
                "duration_seconds": sum(s.duration for s in use.sessions),
            }
            for use in plan.uses
        ],
    }


def _clock(second: int) -> str:
    hours, rest = divmod(second, 3600)
    minutes, seconds = divmod(rest, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
