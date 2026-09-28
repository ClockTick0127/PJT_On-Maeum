"""Scenario JSON: households, usage rules, period edits, load-household expansion.

Everything that varies per household lives inside ``households[]``; only the date
range, the business time zone and the lake/topic identifiers are global. The file
is validated eagerly so a typo fails before anything is written.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import random
from typing import Any


APPLIANCES = ("kettle", "induction", "iron", "microwave", "hair_dryer", "vacuum_cleaner")
WEEKDAY_NAMES = ("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN")
SECONDS_PER_DAY = 86_400

# usage-daily merges sessions closer than this into one logical use (0920 design 7.2).
MERGE_GAP_SECONDS = {
    "kettle": 60,
    "microwave": 60,
    "hair_dryer": 60,
    "vacuum_cleaner": 60,
    "induction": 120,
    "iron": 300,
}
MINIMUM_USE_SECONDS = 10


class ScenarioError(ValueError):
    """The scenario file is malformed or inconsistent."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ScenarioError(message)


def parse_clock(text: str) -> int:
    """``HH:MM`` or ``HH:MM:SS`` -> seconds since local midnight (``24:00`` allowed)."""

    parts = text.split(":")
    _require(2 <= len(parts) <= 3, f"invalid clock {text!r}")
    try:
        numbers = [int(part) for part in parts]
    except ValueError as error:
        raise ScenarioError(f"invalid clock {text!r}") from error
    hours, minutes = numbers[0], numbers[1]
    seconds = numbers[2] if len(numbers) == 3 else 0
    _require(0 <= hours <= 24 and 0 <= minutes < 60 and 0 <= seconds < 60, f"invalid clock {text!r}")
    total = hours * 3600 + minutes * 60 + seconds
    _require(total <= SECONDS_PER_DAY, f"clock past midnight {text!r}")
    return total


def _parse_date(value: Any, label: str) -> date:
    try:
        return date.fromisoformat(str(value))
    except ValueError as error:
        raise ScenarioError(f"{label}: invalid date {value!r}") from error


def _parse_instant(value: Any, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as error:
        raise ScenarioError(f"{label}: invalid timestamp {value!r}") from error
    _require(parsed.tzinfo is not None, f"{label}: timestamp needs a time zone: {value!r}")
    return parsed.astimezone(timezone.utc)


def _pair(value: Any, label: str, *, minimum: int = 0) -> tuple[int, int]:
    _require(isinstance(value, (list, tuple)) and len(value) == 2, f"{label}: expected [min, max]")
    low, high = int(value[0]), int(value[1])
    _require(minimum <= low <= high, f"{label}: expected {minimum} <= min <= max, got {value!r}")
    return low, high


@dataclass(frozen=True)
class UseRule:
    """One habitual use of one appliance per day."""

    appliance: str
    median_second: int
    jitter_seconds: int
    probability: float
    duration_seconds: tuple[int, int]
    weekdays: frozenset[int] | None = None  # 0 = Monday; None = every day
    segments: tuple[int, int] | None = None  # split the use into n sessions
    segment_gap_seconds: tuple[int, int] = (20, 40)
    label: str = ""

    def applies_on(self, day: date) -> bool:
        return self.weekdays is None or day.weekday() in self.weekdays


@dataclass(frozen=True)
class Period:
    """A date range in which the household's rules are edited."""

    name: str
    start: date
    end: date
    shift_minutes: dict[str, int] = field(default_factory=dict)
    probability_override: dict[str, float] = field(default_factory=dict)  # "*" = all
    keep_first_use_only: bool = False
    extend_first_use_seconds: dict[str, int] = field(default_factory=dict)
    missing_windows: tuple[tuple[int, int], ...] = ()  # [start, end) local seconds

    def covers(self, day: date) -> bool:
        return self.start <= day <= self.end


@dataclass(frozen=True)
class AwayPeriod:
    start: datetime
    end: datetime


@dataclass(frozen=True)
class Household:
    household_id: str
    seed: int
    sampling_interval_seconds: int
    device_id: str
    rules: tuple[UseRule, ...]
    periods: tuple[Period, ...] = ()
    away: tuple[AwayPeriod, ...] = ()
    is_load: bool = False
    template_id: str | None = None

    def appliances(self) -> tuple[str, ...]:
        seen: list[str] = []
        for rule in self.rules:
            if rule.appliance not in seen:
                seen.append(rule.appliance)
        return tuple(seen)


@dataclass(frozen=True)
class LoadSpec:
    count: int
    prefix: str
    sampling_interval_seconds: int
    templates: tuple[str, ...]
    weights: tuple[int, ...]
    seed: int
    median_jitter_minutes: int = 60
    probability_jitter: float = 0.1
    inactive_fraction: float = 0.0
    inactive_days: int = 4
    waveform_pool_size: int = 50


@dataclass(frozen=True)
class Scenario:
    start: date
    end: date
    utc_offset_seconds: int
    analysis_run_id: str
    topic: str
    households: tuple[Household, ...]
    load: LoadSpec | None = None
    source_path: str = ""

    @property
    def tzinfo(self) -> timezone:
        return timezone(timedelta(seconds=self.utc_offset_seconds))

    def dates(self) -> list[date]:
        count = (self.end - self.start).days + 1
        return [self.start + timedelta(days=index) for index in range(count)]

    def day_start_utc(self, day: date) -> datetime:
        return datetime(day.year, day.month, day.day, tzinfo=self.tzinfo).astimezone(timezone.utc)

    def household(self, household_id: str) -> Household:
        for item in self.households:
            if item.household_id == household_id:
                return item
        raise KeyError(household_id)

    def demo_households(self) -> tuple[Household, ...]:
        return tuple(item for item in self.households if not item.is_load)

    def load_households(self) -> tuple[Household, ...]:
        return tuple(item for item in self.households if item.is_load)


# --- parsing ----------------------------------------------------------------------


def _parse_rule(raw: dict, label: str) -> UseRule:
    _require(isinstance(raw, dict), f"{label}: rule must be an object")
    appliance = str(raw.get("appliance", "")).lower()
    _require(appliance in APPLIANCES, f"{label}: unknown appliance {raw.get('appliance')!r}")
    probability = float(raw.get("probability", 1.0))
    _require(0.0 <= probability <= 1.0, f"{label}: probability out of range")
    duration = _pair(raw.get("duration_seconds"), f"{label}.duration_seconds", minimum=MINIMUM_USE_SECONDS)
    weekdays = None
    if raw.get("weekdays") is not None:
        names = raw["weekdays"]
        _require(isinstance(names, list) and names, f"{label}.weekdays must be a non-empty list")
        weekdays = frozenset(WEEKDAY_NAMES.index(str(name).upper()) for name in names
                             if str(name).upper() in WEEKDAY_NAMES)
        _require(len(weekdays) == len(names), f"{label}.weekdays has unknown names {names!r}")
    segments = None
    gap = (20, 40)
    if raw.get("segments") is not None:
        segments = _pair(raw["segments"], f"{label}.segments", minimum=1)
        gap = _pair(raw.get("segment_gap_seconds", [20, 40]), f"{label}.segment_gap_seconds", minimum=1)
        _require(gap[1] <= MERGE_GAP_SECONDS[appliance],
                 f"{label}: segment gap {gap[1]}s exceeds merge gap {MERGE_GAP_SECONDS[appliance]}s;"
                 " segments would not merge into one logical use")
        _require(duration[0] >= MINIMUM_USE_SECONDS * segments[1],
                 f"{label}: duration too short for {segments[1]} segments of >= {MINIMUM_USE_SECONDS}s")
    return UseRule(
        appliance=appliance,
        median_second=parse_clock(str(raw.get("median", "12:00"))),
        jitter_seconds=int(raw.get("jitter_minutes", 0)) * 60,
        probability=probability,
        duration_seconds=duration,
        weekdays=weekdays,
        segments=segments,
        segment_gap_seconds=gap,
        label=str(raw.get("label", "")),
    )


def _appliance_map(raw: Any, label: str, cast) -> dict:
    if raw is None:
        return {}
    _require(isinstance(raw, dict), f"{label} must be an object keyed by appliance")
    result = {}
    for key, value in raw.items():
        name = str(key).lower()
        _require(name == "*" or name in APPLIANCES, f"{label}: unknown appliance {key!r}")
        result[name] = cast(value)
    return result


def _parse_period(raw: dict, label: str, start: date, end: date) -> Period:
    _require(isinstance(raw, dict), f"{label}: period must be an object")
    p_start = _parse_date(raw.get("start"), f"{label}.start")
    p_end = _parse_date(raw.get("end", raw.get("start")), f"{label}.end")
    _require(p_start <= p_end, f"{label}: start after end")
    _require(start <= p_start and p_end <= end, f"{label}: outside the scenario range")
    windows = []
    for item in raw.get("missing_windows", []) or []:
        _require(isinstance(item, (list, tuple)) and len(item) == 2, f"{label}.missing_windows: expected [from, to]")
        a, b = parse_clock(str(item[0])), parse_clock(str(item[1]))
        _require(a < b, f"{label}.missing_windows: empty window {item!r}")
        windows.append((a, b))
    probability = _appliance_map(raw.get("probability_override"), f"{label}.probability_override", float)
    for value in probability.values():
        _require(0.0 <= value <= 1.0, f"{label}.probability_override out of range")
    return Period(
        name=str(raw.get("name", f"{p_start}~{p_end}")),
        start=p_start,
        end=p_end,
        shift_minutes=_appliance_map(raw.get("shift_minutes"), f"{label}.shift_minutes", int),
        probability_override=probability,
        keep_first_use_only=bool(raw.get("keep_first_use_only", False)),
        extend_first_use_seconds=_appliance_map(
            raw.get("extend_first_use_seconds"), f"{label}.extend_first_use_seconds", int
        ),
        missing_windows=tuple(windows),
    )


def _parse_household(raw: dict, label: str, start: date, end: date) -> Household:
    _require(isinstance(raw, dict), f"{label}: household must be an object")
    household_id = str(raw.get("household_id", "")).strip()
    _require(1 <= len(household_id) <= 50, f"{label}: household_id must be 1..50 chars")
    interval = int(raw.get("sampling_interval_seconds", 1))
    _require(1 <= interval <= 3600 and SECONDS_PER_DAY % interval == 0,
             f"{label}: sampling_interval_seconds must divide a day")
    rules = tuple(_parse_rule(item, f"{label}.schedule[{index}]")
                  for index, item in enumerate(raw.get("schedule", []) or []))
    _require(rules, f"{label}: schedule must contain at least one rule")
    periods = tuple(_parse_period(item, f"{label}.periods[{index}]", start, end)
                    for index, item in enumerate(raw.get("periods", []) or []))
    away = []
    for index, item in enumerate(raw.get("away", []) or []):
        _require(isinstance(item, dict), f"{label}.away[{index}] must be an object")
        a = _parse_instant(item.get("start"), f"{label}.away[{index}].start")
        b = _parse_instant(item.get("end"), f"{label}.away[{index}].end")
        _require(a < b, f"{label}.away[{index}]: start after end")
        away.append(AwayPeriod(a, b))
    return Household(
        household_id=household_id,
        seed=int(raw.get("seed", 0)),
        sampling_interval_seconds=interval,
        device_id=str(raw.get("device_id", "main")),
        rules=rules,
        periods=periods,
        away=tuple(away),
    )


def _parse_load(raw: dict | None, households: tuple[Household, ...]) -> LoadSpec | None:
    if not raw:
        return None
    _require(isinstance(raw, dict), "load_households must be an object")
    count = int(raw.get("count", 0))
    if count <= 0:
        return None
    templates = tuple(str(item) for item in raw.get("templates", []) or [])
    known = {item.household_id for item in households}
    _require(templates and all(item in known for item in templates),
             "load_households.templates must name demo households")
    weights = tuple(int(item) for item in raw.get("weights", [1] * len(templates)))
    _require(len(weights) == len(templates) and all(item > 0 for item in weights),
             "load_households.weights must be positive and match templates")
    interval = int(raw.get("sampling_interval_seconds", 10))
    _require(1 <= interval <= 3600 and SECONDS_PER_DAY % interval == 0,
             "load_households.sampling_interval_seconds must divide a day")
    prefix = str(raw.get("prefix", "L"))
    _require(prefix.isalnum(), "load_households.prefix must be alphanumeric")
    fraction = float(raw.get("inactive_fraction", 0.0))
    _require(0.0 <= fraction <= 1.0, "load_households.inactive_fraction out of range")
    return LoadSpec(
        count=count,
        prefix=prefix,
        sampling_interval_seconds=interval,
        templates=templates,
        weights=weights,
        seed=int(raw.get("seed", 1)),
        median_jitter_minutes=int(raw.get("median_jitter_minutes", 60)),
        probability_jitter=float(raw.get("probability_jitter", 0.1)),
        inactive_fraction=fraction,
        inactive_days=int(raw.get("inactive_days", 4)),
        waveform_pool_size=max(1, int(raw.get("waveform_pool_size", 50))),
    )


def stable_int(*parts: object, bits: int = 31) -> int:
    """Deterministic non-negative integer from text parts (for RNG seeds)."""

    digest = hashlib.sha256(":".join(str(part) for part in parts).encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % (2**bits - 1)


def expand_load_households(spec: LoadSpec, templates: dict[str, Household],
                           start: date, end: date) -> tuple[Household, ...]:
    """Derive ``spec.count`` households from demo templates with seeded jitter.

    Waveforms of load households are only coverage input, so no attempt is made
    to keep them consistent with the jittered sessions (see README).
    """

    rng = random.Random(f"load:{spec.seed}")
    population = list(spec.templates)
    generated: list[Household] = []
    width = max(4, len(str(spec.count)))
    total_days = (end - start).days + 1
    for index in range(1, spec.count + 1):
        template = templates[rng.choices(population, weights=spec.weights, k=1)[0]]
        household_id = f"{spec.prefix}{index:0{width}d}"
        rules = []
        for rule in template.rules:
            shift = rng.randint(-spec.median_jitter_minutes, spec.median_jitter_minutes) * 60
            median = min(SECONDS_PER_DAY - rule.duration_seconds[1] - 1, max(0, rule.median_second + shift))
            probability = rule.probability + rng.uniform(-spec.probability_jitter, spec.probability_jitter)
            rules.append(replace(rule, median_second=median, probability=min(1.0, max(0.0, probability))))
        periods: list[Period] = []
        if spec.inactive_fraction > 0 and rng.random() < spec.inactive_fraction and total_days > spec.inactive_days:
            offset = rng.randint(0, total_days - spec.inactive_days)
            first = start + timedelta(days=offset)
            periods.append(Period(
                name="load-inactive",
                start=first,
                end=first + timedelta(days=spec.inactive_days - 1),
                probability_override={"*": 0.0},
            ))
        generated.append(Household(
            household_id=household_id,
            seed=stable_int("load", spec.seed, household_id),
            sampling_interval_seconds=spec.sampling_interval_seconds,
            device_id=template.device_id,
            rules=tuple(rules),
            periods=tuple(periods),
            is_load=True,
            template_id=template.household_id,
        ))
    return tuple(generated)


def load_scenario(path: str | Path) -> Scenario:
    source = Path(path)
    try:
        raw = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise ScenarioError(f"cannot read scenario {source}: {error}") from error
    _require(isinstance(raw, dict), "scenario must be a JSON object")
    _require(int(raw.get("scenario_version", 1)) == 1, "unsupported scenario_version")
    range_raw = raw.get("range") or {}
    start = _parse_date(range_raw.get("start"), "range.start")
    end = _parse_date(range_raw.get("end"), "range.end")
    _require(start <= end, "range.start after range.end")
    offset = int(raw.get("utc_offset_seconds", 9 * 3600))
    _require(-SECONDS_PER_DAY < offset < SECONDS_PER_DAY, "utc_offset_seconds out of range")
    households = tuple(_parse_household(item, f"households[{index}]", start, end)
                       for index, item in enumerate(raw.get("households", []) or []))
    _require(households, "households must not be empty")
    ids = [item.household_id for item in households]
    _require(len(ids) == len(set(ids)), "duplicate household_id")
    load = _parse_load(raw.get("load_households"), households)
    if load is not None:
        templates = {item.household_id: item for item in households}
        expanded = expand_load_households(load, templates, start, end)
        collision = {item.household_id for item in expanded} & set(ids)
        _require(not collision, f"load household ids collide with demo ids: {sorted(collision)[:3]}")
        households = households + expanded
    return Scenario(
        start=start,
        end=end,
        utc_offset_seconds=offset,
        analysis_run_id=str(raw.get("analysis_run_id", "realtime-v1")),
        topic=str(raw.get("topic", "power.raw.v1")),
        households=households,
        load=load,
        source_path=str(source),
    )
