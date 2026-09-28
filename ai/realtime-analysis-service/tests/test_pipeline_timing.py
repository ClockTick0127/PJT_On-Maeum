import json
import logging
from datetime import datetime
from uuid import UUID

import pytest

from realtime_analysis.pipeline_timing import pipeline_timing, stage
from realtime_analysis.schemas import PowerMeasurement


def measurement() -> PowerMeasurement:
    return PowerMeasurement(
        message_id=UUID("8f3b2a19-4d6e-4c72-9b12-a1b2c3d4e5f6"),
        household_id="H001",
        device_id="main",
        measured_at=datetime.fromisoformat("2026-09-08T03:41:05.120+00:00"),
        active_power=1789.47,
        reactive_power=340.01,
        power_factor=0.982,
        current=8.279,
    )


def timing_records(caplog) -> list[dict[str, object]]:
    return [
        json.loads(record.message)
        for record in caplog.records
        if record.name == "realtime_analysis.pipeline_timing"
    ]


def test_pipeline_timing_emits_one_parseable_json_record(caplog) -> None:
    with caplog.at_level(
        logging.INFO,
        logger="realtime_analysis.pipeline_timing",
    ):
        with pipeline_timing(
            kafka_topic="power.raw.v1",
            kafka_partition=3,
            kafka_offset=42,
        ) as timer:
            timer.bind_measurement(measurement())
            with stage("preprocess"):
                pass
            with stage("inference"):
                pass
            timer.mark("processed")

    records = timing_records(caplog)
    assert len(records) == 1
    assert records[0]["event"] == "pipeline_timing"
    assert records[0]["status"] == "processed"
    assert records[0]["message_id"] == str(measurement().message_id)
    assert records[0]["kafka"] == {
        "topic": "power.raw.v1",
        "partition": 3,
        "offset": 42,
    }
    assert records[0]["stage_counts"] == {"inference": 1, "preprocess": 1}
    assert records[0]["stage_durations_ns"]["inference"] >= 0
    assert records[0]["processing_total_ns"] >= 0


def test_repeated_stage_durations_are_accumulated(caplog) -> None:
    with caplog.at_level(
        logging.INFO,
        logger="realtime_analysis.pipeline_timing",
    ):
        with pipeline_timing() as timer:
            with stage("event_publish_ack"):
                pass
            with stage("event_publish_ack"):
                pass
            timer.mark("processed")

    record = timing_records(caplog)[0]
    assert record["stage_counts"]["event_publish_ack"] == 2
    assert record["stage_durations_ns"]["event_publish_ack"] >= 0


def test_pipeline_failure_is_logged_and_reraised(caplog) -> None:
    with caplog.at_level(
        logging.INFO,
        logger="realtime_analysis.pipeline_timing",
    ):
        with pytest.raises(RuntimeError, match="temporary failure"):
            with pipeline_timing():
                with stage("activity_db"):
                    raise RuntimeError("temporary failure")

    record = timing_records(caplog)[0]
    assert record["status"] == "failed"
    assert record["error_type"] == "RuntimeError"
    assert record["stage_counts"]["activity_db"] == 1
