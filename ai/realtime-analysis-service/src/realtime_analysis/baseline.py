"""MVP JSON-backed routine baseline repository."""

import json
from pathlib import Path

from pydantic import TypeAdapter

from realtime_analysis.schemas import RoutineBaseline


class BaselineRepository:
    def __init__(self, baselines: list[RoutineBaseline]) -> None:
        self._by_household: dict[str, list[RoutineBaseline]] = {}
        for baseline in baselines:
            self._by_household.setdefault(baseline.household_id, []).append(baseline)

    @classmethod
    def from_json_file(cls, path: str | Path) -> "BaselineRepository":
        with Path(path).open(encoding="utf-8") as file:
            payload = json.load(file)
        baselines = TypeAdapter(list[RoutineBaseline]).validate_python(payload)
        return cls(baselines)

    def find_by_household(self, household_id: str) -> list[RoutineBaseline]:
        return [
            baseline
            for baseline in self._by_household.get(household_id, [])
            if baseline.enabled
        ]
