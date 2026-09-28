"""Per-second main-panel waveforms for a day plan.

Two generators share one interface: the existing MQTT simulator's physics engine
(imported unchanged from ``infrastructure/mqtt/simulator``) and a small synthetic
fallback used by unit tests and when the simulator directory is not mounted.
Waveforms only feed Silver coverage, so both are acceptable for aggregation.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import importlib
import math
from pathlib import Path
import sys
from typing import Protocol

import numpy as np

from history_generator.scenario import APPLIANCES, SECONDS_PER_DAY
from history_generator.schedule import DayPlan


COLUMNS = ("active_power", "reactive_power", "power_factor", "current")

# Standby/appliance powers for the synthetic fallback (roughly the simulator medians).
SYNTHETIC_WATTS = {
    "kettle": 1657.0,
    "induction": 1463.0,
    "iron": 1389.0,
    "microwave": 941.0,
    "hair_dryer": 934.0,
    "vacuum_cleaner": 819.0,
}
SYNTHETIC_PF = {
    "kettle": 0.99,
    "induction": 0.93,
    "iron": 0.99,
    "microwave": 0.91,
    "hair_dryer": 0.96,
    "vacuum_cleaner": 0.80,
}


def seed_from(*parts: object) -> int:
    digest = hashlib.sha256(":".join(str(part) for part in parts).encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "big") % (2**31 - 1)


class WaveformGenerator(Protocol):
    name: str

    def generate(self, plan: DayPlan, seed: int) -> np.ndarray:
        """Return ``(SECONDS_PER_DAY, 4)`` float64 array in COLUMNS order."""


@dataclass
class SyntheticWaveform:
    """Standby random walk plus rectangular appliance blocks. No simulator needed."""

    name: str = "synthetic"

    def generate(self, plan: DayPlan, seed: int) -> np.ndarray:
        rng = np.random.default_rng(seed)
        standby = 60.0 + 25.0 * rng.random()
        active = standby + np.cumsum(rng.normal(0.0, 0.3, SECONDS_PER_DAY))
        active = np.clip(active, 20.0, None)
        reactive = active * math.tan(math.acos(0.90))
        for session in plan.sessions:
            watts = SYNTHETIC_WATTS[session.appliance]
            pf = SYNTHETIC_PF[session.appliance]
            span = slice(session.start_second, session.end_second)
            block = watts + rng.normal(0.0, 1.5, session.duration)
            active[span] += block
            reactive[span] += block * math.tan(math.acos(pf))
        active += rng.normal(0.0, 0.8, SECONDS_PER_DAY)
        active = np.clip(active, 0.0, None)
        reactive = np.clip(reactive, 0.0, None)
        apparent = np.sqrt(active * active + reactive * reactive)
        power_factor = np.where(apparent > 1e-3, active / np.maximum(apparent, 1e-9), 1.0)
        voltage = 220.0 + rng.normal(0.0, 0.5, SECONDS_PER_DAY)
        current = apparent / voltage
        return np.column_stack([
            np.round(active, 2), np.round(reactive, 2),
            np.round(np.clip(power_factor, 0.0, 1.0), 3), np.round(current, 3),
        ])


def _stub_aiomqtt() -> None:
    """``engine/__init__`` imports the MQTT publisher, which imports ``aiomqtt``.

    Nothing here ever publishes, so when the client library is absent a stub that
    refuses to connect is registered (same approach as the R2 replay adapter).
    """

    try:
        importlib.import_module("aiomqtt")
        return
    except ImportError:
        pass
    import types

    class DisabledClient:
        def __init__(self, *args, **kwargs):
            raise RuntimeError("history-generator never opens an MQTT connection")

    stub = types.ModuleType("aiomqtt")
    stub.Client = DisabledClient
    stub.MqttError = RuntimeError
    sys.modules["aiomqtt"] = stub


class SimulatorWaveform:
    """Drive ``infrastructure/mqtt/simulator/engine`` second by second.

    The engine keeps module-level state, so one household-day is generated at a
    time and the state is re-initialised with a seed derived from the plan.
    """

    name = "simulator"

    def __init__(self, simulator_dir: str | Path) -> None:
        directory = Path(simulator_dir).resolve()
        if not (directory / "engine" / "power_model.py").exists():
            raise FileNotFoundError(f"simulator engine not found under {directory}")
        if str(directory) not in sys.path:
            sys.path.insert(0, str(directory))
        _stub_aiomqtt()
        state = importlib.import_module("engine.state")
        power_model = importlib.import_module("engine.power_model")
        self._init = state.init_simulation_states
        self._manual = state.set_manual_device_state
        self._metrics = power_model.calculate_main_panel_metrics
        profiles = importlib.import_module("engine.profiles").DEVICE_PROFILES
        missing = [name for name in APPLIANCES if name not in profiles]
        if missing:
            raise RuntimeError(f"simulator profiles lack appliances: {missing}")

    def generate(self, plan: DayPlan, seed: int) -> np.ndarray:
        house = plan.household_id
        self._init([house], seed=seed)
        events: dict[int, list[tuple[str, bool]]] = {}
        for session in plan.sessions:
            events.setdefault(session.start_second, []).append((session.appliance, True))
            if session.end_second < SECONDS_PER_DAY:
                events.setdefault(session.end_second, []).append((session.appliance, False))
        out = np.empty((SECONDS_PER_DAY, 4), dtype=np.float64)
        metrics = self._metrics
        for second in range(SECONDS_PER_DAY):
            pending = events.get(second)
            if pending:
                for appliance, enabled in pending:
                    self._manual(house, appliance, enabled)
            m = metrics(house, allow_random=False)
            out[second, 0] = m["active_power"]
            out[second, 1] = m["reactive_power"]
            out[second, 2] = m["power_factor"]
            out[second, 3] = m["current"]
        return out


def make_generator(kind: str, simulator_dir: str | Path | None) -> WaveformGenerator:
    if kind == "synthetic":
        return SyntheticWaveform()
    if kind == "simulator":
        if simulator_dir is None:
            raise ValueError("--simulator-dir is required for the simulator waveform")
        return SimulatorWaveform(simulator_dir)
    raise ValueError(f"unknown waveform generator {kind!r}")


class WaveformPool:
    """Reuse a bounded set of generated days for load households.

    Load households only need plausible coverage. Generating a physics waveform
    for 1,000 households x 91 days would take ~30 hours; a pool of ``size`` days
    keyed by (template, slot) keeps it to minutes. Values are copied per use.
    """

    def __init__(self, generator: WaveformGenerator, size: int) -> None:
        self._generator = generator
        self._size = max(1, size)
        self._cache: dict[tuple[str, int], np.ndarray] = {}

    def waveform(self, plan: DayPlan, template_id: str, ordinal: int) -> np.ndarray:
        slot = ordinal % self._size
        key = (template_id, slot)
        cached = self._cache.get(key)
        if cached is None:
            cached = self._generator.generate(plan, seed_from("pool", template_id, slot))
            self._cache[key] = cached
        return cached
