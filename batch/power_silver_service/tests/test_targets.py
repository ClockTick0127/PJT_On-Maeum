from __future__ import annotations

from datetime import date
import json

import pytest

from power_silver.targets import (
    InvalidObservationTargets,
    SECONDS_PER_DAY,
    day_start_utc,
    load_targets,
)


KST = 9 * 3600


def write(path, targets, config_version="v1"):
    path.write_text(
        json.dumps({"config_version": config_version, "targets": targets}),
        encoding="utf-8",
    )
    return path


def test_full_day_segment_expects_one_slot_per_second(tmp_path):
    targets = load_targets(
        write(
            tmp_path / "t.json",
            [{"household_id": "H001", "device_id": "main"}],
        )
    )
    segments = targets.segments_for_date(date(2026, 9, 19), KST)
    assert len(segments) == 1
    assert (segments[0].start_second, segments[0].end_second) == (0, SECONDS_PER_DAY)
    assert segments[0].expected_sample_count == 86_400


def test_install_day_covers_only_part_of_the_day(tmp_path):
    targets = load_targets(
        write(
            tmp_path / "t.json",
            [
                {
                    "household_id": "H001",
                    "device_id": "main",
                    "effective_from": "2026-09-19T06:00:00+09:00",
                }
            ],
        )
    )
    segments = targets.segments_for_date(date(2026, 9, 19), KST)
    assert (segments[0].start_second, segments[0].end_second) == (21_600, 86_400)
    assert segments[0].expected_sample_count == 64_800


def test_device_replacement_splits_the_day_into_two_segments(tmp_path):
    targets = load_targets(
        write(
            tmp_path / "t.json",
            [
                {
                    "household_id": "H001",
                    "device_id": "old",
                    "effective_to": "2026-09-19T12:00:00+09:00",
                },
                {
                    "household_id": "H001",
                    "device_id": "new",
                    "effective_from": "2026-09-19T12:00:00+09:00",
                },
            ],
        )
    )
    segments = targets.segments_for_date(date(2026, 9, 19), KST)
    assert [(item.device_id, item.start_second, item.end_second) for item in segments] == [
        ("old", 0, 43_200),
        ("new", 43_200, 86_400),
    ]
    assert sum(item.expected_sample_count for item in segments) == 86_400


def test_overlapping_effective_ranges_are_a_configuration_error(tmp_path):
    with pytest.raises(InvalidObservationTargets):
        load_targets(
            write(
                tmp_path / "t.json",
                [
                    {"household_id": "H001", "device_id": "a"},
                    {"household_id": "H001", "device_id": "b"},
                ],
            )
        )


def test_disabled_household_is_listed_but_has_no_segment(tmp_path):
    targets = load_targets(
        write(
            tmp_path / "t.json",
            [
                {
                    "household_id": "H001",
                    "device_id": "main",
                    "observation_enabled": False,
                }
            ],
        )
    )
    day = date(2026, 9, 19)
    assert targets.household_ids_for_date(day, KST) == ("H001",)
    assert targets.segments_for_date(day, KST) == ()


def test_household_effective_after_the_day_is_not_listed(tmp_path):
    targets = load_targets(
        write(
            tmp_path / "t.json",
            [
                {
                    "household_id": "H001",
                    "device_id": "main",
                    "effective_from": "2026-10-01T00:00:00+09:00",
                }
            ],
        )
    )
    assert targets.household_ids_for_date(date(2026, 9, 19), KST) == ()


def test_fingerprint_changes_when_content_changes_under_the_same_name(tmp_path):
    first = load_targets(
        write(tmp_path / "a.json", [{"household_id": "H001", "device_id": "main"}])
    )
    second = load_targets(
        write(tmp_path / "b.json", [{"household_id": "H001", "device_id": "other"}])
    )
    assert first.config_version == second.config_version
    assert first.fingerprint != second.fingerprint


def test_day_start_is_the_local_midnight_in_utc():
    assert day_start_utc(date(2026, 9, 19), KST).isoformat() == "2026-09-18T15:00:00+00:00"


def test_sampling_interval_must_divide_the_day(tmp_path):
    with pytest.raises(InvalidObservationTargets):
        load_targets(
            write(
                tmp_path / "t.json",
                [
                    {
                        "household_id": "H001",
                        "device_id": "main",
                        "sampling_interval_seconds": 7,
                    }
                ],
            )
        )
