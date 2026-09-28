import importlib.util
import json
import sys
import unittest
from unittest.mock import MagicMock, Mock


# 로컬 테스트 환경에 컨테이너 런타임 의존성이 없을 때도 순수 계약 로직을 검증한다.
if importlib.util.find_spec("pyarrow") is None:
    sys.modules["pyarrow"] = MagicMock()
    sys.modules["pyarrow.parquet"] = MagicMock()
for module_name in ("confluent_kafka", "hdfs"):
    if importlib.util.find_spec(module_name) is None:
        sys.modules[module_name] = MagicMock()

from loader import BronzeLoader, measurement_manifest_summary, parse_measurement


def valid_payload(**overrides):
    payload = {
        "message_id": "123e4567-e89b-12d3-a456-426614174000",
        "household_id": "H001",
        "device_id": "main",
        "measured_at": "2026-09-18T01:02:03.456Z",
        "active_power": 1823.5,
        "reactive_power": 217.4,
        "power_factor": 0.91,
        "current": 8.2,
    }
    payload.update(overrides)
    return json.dumps(payload).encode()


class MeasurementContractTest(unittest.TestCase):
    def test_reads_new_contract_without_legacy_fields(self):
        record = parse_measurement(valid_payload())

        self.assertEqual(record["household_id"], "H001")
        self.assertEqual(record["active_power"], 1823.5)
        self.assertNotIn("house", record)
        self.assertNotIn("power_w", record)

    def test_rejects_missing_new_field_even_when_legacy_fields_exist(self):
        payload = json.loads(valid_payload())
        payload["power_w"] = payload.pop("active_power")

        with self.assertRaises(KeyError):
            parse_measurement(json.dumps(payload).encode())

    def test_rejects_timestamp_without_timezone(self):
        with self.assertRaises(ValueError):
            parse_measurement(valid_payload(measured_at="2026-09-18T01:02:03"))

    def test_rejects_out_of_range_power_factor(self):
        with self.assertRaises(ValueError):
            parse_measurement(valid_payload(power_factor=1.1))

    def test_manifest_summary_tracks_utc_range_and_kst_dates(self):
        rows = [
            parse_measurement(valid_payload(measured_at="2026-09-17T14:59:59Z")),
            parse_measurement(valid_payload(measured_at="2026-09-17T15:00:01Z")),
        ]

        summary = measurement_manifest_summary(rows)

        self.assertEqual(
            summary["business_dates"],
            ["2026-09-17", "2026-09-18"],
        )
        self.assertEqual(
            summary["min_measured_at"],
            "2026-09-17T14:59:59+00:00",
        )
        self.assertEqual(
            summary["max_measured_at"],
            "2026-09-17T15:00:01+00:00",
        )


class IngestRoutingTest(unittest.TestCase):
    def test_invalid_new_contract_is_quarantined_with_original_payload(self):
        loader = BronzeLoader.__new__(BronzeLoader)
        loader.buffers = {}
        loader.flush_partition = Mock()
        message = Mock()
        message.partition.return_value = 0
        message.offset.return_value = 12
        message.value.return_value = valid_payload(current=-1)
        message.topic.return_value = "power.raw.v1"
        message.timestamp.return_value = (0, 0)

        loader.ingest(message)

        self.assertEqual(len(loader.buffers[0].ok_rows), 0)
        self.assertEqual(len(loader.buffers[0].quarantine_rows), 1)
        self.assertEqual(
            loader.buffers[0].quarantine_rows[0]["raw_payload"],
            message.value.return_value,
        )


if __name__ == "__main__":
    unittest.main()
