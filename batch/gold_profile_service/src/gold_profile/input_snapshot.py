from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import hashlib
import json


@dataclass(frozen=True)
class ProfileInputSnapshot:
    snapshot_id: str
    as_of_date: date
    window_start_date: date
    expected_dates: tuple[date, ...]
    missing_usage_dates: tuple[date, ...]
    missing_slice_dates: tuple[date, ...]
    usage_refs: tuple
    slice_refs: tuple

    @property
    def incomplete(self) -> bool:
        return bool(self.missing_usage_dates or self.missing_slice_dates)

    def as_dict(self) -> dict:
        def refs(items):
            return [
                {
                    "dataset_name": item.dataset_name,
                    "target_date": item.target_date.isoformat(),
                    "run_id": str(item.run_id),
                    "version_id": str(item.version_id),
                    "output_path": item.output_path,
                    "manifest_path": item.manifest_path,
                    "input_snapshot_id": item.input_snapshot_id,
                    "rule_version": item.rule_version,
                    "config_version": item.config_version,
                }
                for item in items
            ]

        return {
            "snapshot_id": self.snapshot_id,
            "as_of_date": self.as_of_date.isoformat(),
            "window_start_date": self.window_start_date.isoformat(),
            "expected_dates": [item.isoformat() for item in self.expected_dates],
            "missing_usage_dates": [item.isoformat() for item in self.missing_usage_dates],
            "missing_slice_dates": [item.isoformat() for item in self.missing_slice_dates],
            "usage_versions": refs(self.usage_refs),
            "slice_versions": refs(self.slice_refs),
        }


def window_dates(as_of_date: date, days: int) -> tuple[date, ...]:
    start = as_of_date - timedelta(days=days - 1)
    return tuple(start + timedelta(days=offset) for offset in range(days))


def build_profile_snapshot(
    as_of_date: date,
    days: int,
    usage_versions: dict,
    slice_versions: dict,
    *,
    rule_version: str,
    statistic_rule_version: str,
    analysis_run_id: str,
    timezone_name: str,
) -> ProfileInputSnapshot:
    expected = window_dates(as_of_date, days)
    usage_refs = tuple(usage_versions[item] for item in expected if item in usage_versions)
    slice_refs = tuple(slice_versions[item] for item in expected if item in slice_versions)
    payload = {
        "as_of_date": as_of_date.isoformat(),
        "expected_dates": [item.isoformat() for item in expected],
        "usage": [(item.target_date.isoformat(), str(item.version_id), str(item.run_id)) for item in usage_refs],
        "slices": [(item.target_date.isoformat(), str(item.version_id), str(item.run_id)) for item in slice_refs],
        "rule_version": rule_version,
        "statistic_rule_version": statistic_rule_version,
        "analysis_run_id": analysis_run_id,
        "timezone_name": timezone_name,
    }
    snapshot_id = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return ProfileInputSnapshot(
        snapshot_id=snapshot_id,
        as_of_date=as_of_date,
        window_start_date=expected[0],
        expected_dates=expected,
        missing_usage_dates=tuple(item for item in expected if item not in usage_versions),
        missing_slice_dates=tuple(item for item in expected if item not in slice_versions),
        usage_refs=usage_refs,
        slice_refs=slice_refs,
    )
