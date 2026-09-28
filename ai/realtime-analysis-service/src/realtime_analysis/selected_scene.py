"""R3 confirm=1 decoding; UNKNOWN and initial/recovery sync are explicit."""

from dataclasses import dataclass
import math
import struct


def float32(value: float) -> float:
    return struct.unpack("f", struct.pack("f", value))[0]


@dataclass(frozen=True)
class SceneDecision:
    state: str
    transition: str | None


class SelectedSceneDecoder:
    def __init__(self, on: float, off: float, confirm: int = 1):
        if not 0 <= off <= on <= 1 or confirm != 1:
            raise ValueError("Expected frozen R3 confirm=1 thresholds")
        self.on, self.off = float32(on), float32(off)
        self.state = "UNKNOWN"

    def reset(self) -> None:
        self.state = "UNKNOWN"

    def step(self, score: float | None) -> SceneDecision:
        previous = self.state
        if score is None or not math.isfinite(score):
            self.reset()
            return SceneDecision(self.state, None)
        if not 0 <= score <= 1:
            raise ValueError("Probability outside [0,1]")
        if score >= self.on:
            self.state = "ON"
        elif score < self.off or (self.on != self.off and score <= self.off):
            self.state = "OFF"
        transition = None
        if self.state != previous:
            transition = "SYNC" if previous == "UNKNOWN" else f"TURNED_{self.state}"
        return SceneDecision(self.state, transition)
