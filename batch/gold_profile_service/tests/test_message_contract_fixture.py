"""The producer envelope must match the fixture monitoring-service deserializes.

monitoring-service owns the contract. Its test resources keep a real payload
captured from this batch (``contracts/gold-household-profile-v2.json``) and a
Spring test feeds it through the consumer. This test keeps the producer side
honest: the keys this module emits are exactly the keys of that fixture, at
every level. It is skipped when the monitoring module is not checked out next
to this service (for example inside the Docker test stage).
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
import json
from pathlib import Path

import pytest
from pyspark.sql import Row

from gold_profile.message_contract import (
    BASELINE_FIELDS, STATISTIC_FIELDS, build_household_messages,
)


def _fixture_path() -> Path | None:
    parents = Path(__file__).resolve().parents
    if len(parents) < 4:  # copied into an image without the repository around it
        return None
    return parents[3] / (
        "backend/monitoring-service/src/test/resources/contracts/gold-household-profile-v2.json"
    )


FIXTURE = _fixture_path()


class Frame:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return self._rows


def _fixture() -> dict:
    if FIXTURE is None or not FIXTURE.exists():
        pytest.skip(f"monitoring contract fixture is not available: {FIXTURE}")
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _message() -> dict:
    baseline = Row(
        household_id="H001", profile_version="run-1", appliance_type="KETTLE",
        baseline_scope="OVERALL", weekday=None, sample_days=3, active_days=2,
        daily_use_probability=Decimal("0.6667"), reliability_weight=Decimal("1.0000"),
        first_use_time_p50_second=28800, expected_until_second=28800,
        preferred_window_start_second=27000, preferred_window_end_second=28800,
        quality_status="READY", enabled=False,
    )
    statistic = Row(
        household_id="H001", profile_version="run-1",
        metric_name="INACTIVITY_ELAPSED", appliance_type=None, weekday_group="ALL",
        time_bucket="09:30", sample_count=3, eligible_day_count=3,
        p50=1800.0, p90=3600.0, mad=0.0, unit="seconds", quality_status="READY",
    )
    return build_household_messages(
        Frame([baseline]), Frame([statistic]),
        profile_version="run-1", profile_revision=1, delivery_mode="SHADOW",
        as_of_date=date(2026, 9, 17), window_start_date=date(2026, 9, 15),
        effective_from=datetime(2026, 9, 18, 0, 0, tzinfo=timezone.utc),
        published_at=datetime(2026, 9, 18, 0, 0, tzinfo=timezone.utc),
        input_snapshot_id="snapshot", rule_version="gold-profile-v1",
        statistic_rule_version="household-statistics-v1-nearest-rank",
        input_incomplete=False,
    )[0]


def test_envelope_keys_match_the_monitoring_fixture():
    fixture = _fixture()
    message = _message()

    assert set(message) == set(fixture)
    assert set(message["routine_baselines"][0]) == set(fixture["routine_baselines"][0])
    assert set(message["statistics"][0]) == set(fixture["statistics"][0])


def test_fixture_field_lists_are_the_projection_used_by_the_producer():
    fixture = _fixture()

    assert set(fixture["routine_baselines"][0]) == set(BASELINE_FIELDS)
    assert set(fixture["statistics"][0]) == set(STATISTIC_FIELDS)
    # Values the consumer coerces: decimals travel as strings, timestamps as ISO 8601.
    assert isinstance(fixture["routine_baselines"][0]["daily_use_probability"], str)
    Decimal(fixture["routine_baselines"][0]["daily_use_probability"])
    datetime.fromisoformat(fixture["effective_from"])
    date.fromisoformat(fixture["as_of_date"])
    assert fixture["schema_version"] == 2
    assert fixture["delivery_mode"] in {"ACTIVE", "SHADOW"}
    assert fixture["profile_revision"] >= 1
