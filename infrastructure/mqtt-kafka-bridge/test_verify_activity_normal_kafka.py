"""
test_verify_activity_normal_kafka.py
ACTIVITY_NORMAL Kafka 원천 데이터 검증 도구 단위 및 회귀 테스트 스위트
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
import math
import os
import sys
import unittest
from unittest.mock import MagicMock, Mock, patch
import uuid

# bridge.py 및 verify_activity_normal_kafka 임포트 보장
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from confluent_kafka import KafkaError, TopicPartition

from bridge import forward_message
from verify_activity_normal_kafka import (
    CANONICAL_MEASURED_AT_PATTERN,
    EXACT_FIELDS,
    EXIT_CLI_CONFIG_ERROR,
    EXIT_INTERNAL_ERROR,
    EXIT_KAFKA_ERROR,
    EXIT_SUCCESS,
    EXIT_TIMEOUT,
    EXIT_VERIFICATION_FAILURE,
    EXPECTED_PARTITION_COUNT,
    FORBIDDEN_AI_FIELDS,
    KafkaConsumerError,
    KafkaRecordVerifier,
    KafkaTopicError,
    NUMERIC_FIELDS,
    SCHEDULE_PUBLISHER_NAMESPACE,
    SECONDS_PER_DAY,
    TARGET_DEVICE,
    TARGET_HOUSEHOLD,
    TARGET_KAFKA_TOPIC,
    TARGET_REFERENCE_DATE,
    TARGET_SCENARIO,
    TimeoutVerificationError,
    extract_household_snapshot,
    parse_cli_args,
    redact_secrets,
    run_kafka_verification,
    validate_canonical_measured_at,
)


def make_valid_payload(run_id: str, second_of_day: int) -> dict:
    """second_of_day (0~86399)에 대응하는 유효한 14개 필드 MQTT/Kafka 페이로드 생성"""
    h = second_of_day // 3600
    m = (second_of_day % 3600) // 60
    s = second_of_day % 60
    measured_at = f"2026-09-16T{h:02d}:{m:02d}:{s:02d}+09:00"
    cycle = second_of_day + 1
    uuid5_name = f"SCHEDULE_PUBLISHER_V1::{run_id}::{TARGET_HOUSEHOLD}::{cycle}::{measured_at}"
    msg_id = str(uuid.uuid5(SCHEDULE_PUBLISHER_NAMESPACE, uuid5_name))

    return {
        "message_id": msg_id,
        "household_id": TARGET_HOUSEHOLD,
        "device_id": TARGET_DEVICE,
        "measured_at": measured_at,
        "active_power": 120.5,
        "reactive_power": 15.2,
        "power_factor": 0.95,
        "current": 0.55,
        "house": TARGET_HOUSEHOLD,
        "device": TARGET_DEVICE,
        "ts": measured_at,
        "power_w": 120.5,
        "voltage": 220.0,
        "apparent_power": 121.5,
    }


class CanonicalMeasuredAtTests(unittest.TestCase):
    """1. canonical measured_at 엄격 형식 및 의미 검증 테스트"""

    def test_valid_start_and_end_of_day(self):
        # 00:00:00 -> second_of_day = 0
        valid, reason, sec = validate_canonical_measured_at("2026-09-16T00:00:00+09:00")
        self.assertTrue(valid)
        self.assertIsNone(reason)
        self.assertEqual(sec, 0)

        # 23:59:59 -> second_of_day = 86399
        valid, reason, sec = validate_canonical_measured_at("2026-09-16T23:59:59+09:00")
        self.assertTrue(valid)
        self.assertIsNone(reason)
        self.assertEqual(sec, 86399)

    def test_reject_out_of_bounds_time(self):
        # 24:00:00
        valid, reason, sec = validate_canonical_measured_at("2026-09-16T24:00:00+09:00")
        self.assertFalse(valid)
        self.assertIsNone(sec)

        # 23:60:00
        valid, reason, sec = validate_canonical_measured_at("2026-09-16T23:60:00+09:00")
        self.assertFalse(valid)
        self.assertIsNone(sec)

        # 23:59:60
        valid, reason, sec = validate_canonical_measured_at("2026-09-16T23:59:60+09:00")
        self.assertFalse(valid)
        self.assertIsNone(sec)

    def test_reject_milliseconds_or_microseconds(self):
        valid, reason, sec = validate_canonical_measured_at("2026-09-16T00:00:00.000+09:00")
        self.assertFalse(valid)
        self.assertIsNone(sec)

    def test_reject_space_separator(self):
        valid, reason, sec = validate_canonical_measured_at("2026-09-16 00:00:00+09:00")
        self.assertFalse(valid)
        self.assertIsNone(sec)

    def test_reject_utc_z_representation(self):
        valid, reason, sec = validate_canonical_measured_at("2026-09-16T00:00:00Z")
        self.assertFalse(valid)
        self.assertIsNone(sec)

    def test_reject_missing_timezone(self):
        valid, reason, sec = validate_canonical_measured_at("2026-09-16T00:00:00")
        self.assertFalse(valid)
        self.assertIsNone(sec)

    def test_reject_non_string_types(self):
        for invalid_val in (None, 12345, True, [], {}):
            with self.subTest(val=invalid_val):
                valid, reason, sec = validate_canonical_measured_at(invalid_val)
                self.assertFalse(valid)
                self.assertIsNone(sec)


class BridgeJsonSemanticPreservationTests(unittest.TestCase):
    """3. 브리지의 JSON 의미 보존 단위 테스트 (bridge.py 연동)"""

    def test_forward_message_preserves_json_semantics_and_key(self):
        client, producer = Mock(), Mock()
        failed = Mock()
        run_id = "test_run_123"
        payload_dict = make_valid_payload(run_id, 3600)  # 01:00:00

        raw_payload = json.dumps(payload_dict).encode("utf-8")
        message = Mock(payload=raw_payload, mid=42, qos=1, topic="v1/power/sim/H001/main")

        forward_message(client, producer, message, "power.raw.v1", failed)

        producer.produce.assert_called_once()
        call_kwargs = producer.produce.call_args.kwargs
        self.assertEqual(call_kwargs["key"], b"H001")

        # JSON 역직렬화 후 의미상 완전히 동일한지 확인
        produced_val = json.loads(call_kwargs["value"].decode("utf-8"))
        self.assertEqual(produced_val, payload_dict)

        # delivery 성공 전에는 MQTT ACK가 호출되지 않음
        client.ack.assert_not_called()

        # delivery 성공 콜백 호출
        on_delivery = call_kwargs["on_delivery"]
        on_delivery(None, Mock())

        # delivery 성공 후에만 정확히 1회 ACK 호출
        client.ack.assert_called_once_with(42, 1)


class RecordAttributionAnd5TierClassificationTests(unittest.TestCase):
    """2. 레코드 귀속 한계 및 5종 카운터 분류 계약 테스트"""

    def setUp(self):
        self.run_id = "run_test_attribution_001"
        self.start_offsets = {p: 0 for p in range(24)}
        self.verifier = KafkaRecordVerifier(self.run_id, TARGET_KAFKA_TOPIC, self.start_offsets)

    def _make_mock_record(self, key: bytes, value: bytes | None, partition: int = 14, offset: int = 0):
        rec = Mock()
        rec.key.return_value = key
        rec.value.return_value = value
        rec.partition.return_value = partition
        rec.offset.return_value = offset
        rec.error.return_value = None
        return rec

    def test_valid_target_record_increments_target_unique(self):
        payload = make_valid_payload(self.run_id, 100)
        rec = self._make_mock_record(b"H001", json.dumps(payload).encode("utf-8"), offset=0)
        res = self.verifier.process_record(rec)

        self.assertEqual(res, "TARGET")
        self.assertEqual(self.verifier.target_unique_records, 1)
        self.assertEqual(self.verifier.invalid_target_records, 0)
        self.assertEqual(self.verifier.duplicate_target_records, 0)
        self.assertEqual(self.verifier.foreign_records, 0)
        self.assertEqual(self.verifier.malformed_unattributed_records, 0)

    def test_duplicate_target_record_increments_duplicate_target(self):
        payload = make_valid_payload(self.run_id, 100)
        rec1 = self._make_mock_record(b"H001", json.dumps(payload).encode("utf-8"), offset=0)
        rec2 = self._make_mock_record(b"H001", json.dumps(payload).encode("utf-8"), offset=1)

        self.verifier.process_record(rec1)
        res = self.verifier.process_record(rec2)

        self.assertEqual(res, "DUPLICATE")
        self.assertEqual(self.verifier.target_unique_records, 1)
        self.assertEqual(self.verifier.duplicate_target_records, 1)

    def test_target_candidate_key_mismatch_is_invalid_target(self):
        payload = make_valid_payload(self.run_id, 200)
        # expected UUID5를 가졌으나 Kafka key가 H002
        rec = self._make_mock_record(b"H002", json.dumps(payload).encode("utf-8"), offset=2)
        res = self.verifier.process_record(rec)

        self.assertEqual(res, "INVALID_TARGET")
        self.assertEqual(self.verifier.invalid_target_records, 1)
        self.assertEqual(self.verifier.target_unique_records, 0)

    def test_target_candidate_field_contract_violation_is_invalid_target(self):
        payload = make_valid_payload(self.run_id, 300)
        del payload["voltage"]  # 14개 필드 중 하나 누락
        rec = self._make_mock_record(b"H001", json.dumps(payload).encode("utf-8"), offset=3)
        res = self.verifier.process_record(rec)

        self.assertEqual(res, "INVALID_TARGET")
        self.assertEqual(self.verifier.invalid_target_records, 1)

    def test_target_candidate_numeric_type_bool_is_invalid_target(self):
        payload = make_valid_payload(self.run_id, 400)
        payload["active_power"] = True  # bool은 수치 필드로 거절되어야 함
        payload["power_w"] = True
        rec = self._make_mock_record(b"H001", json.dumps(payload).encode("utf-8"), offset=4)
        res = self.verifier.process_record(rec)

        self.assertEqual(res, "INVALID_TARGET")
        self.assertEqual(self.verifier.invalid_target_records, 1)

    def test_target_candidate_forbidden_ai_fields_is_invalid_target(self):
        payload = make_valid_payload(self.run_id, 500)
        payload["scenario_id"] = "ACTIVITY_NORMAL"  # 금지 AI 필드
        rec = self._make_mock_record(b"H001", json.dumps(payload).encode("utf-8"), offset=5)
        res = self.verifier.process_record(rec)

        self.assertEqual(res, "INVALID_TARGET")
        self.assertEqual(self.verifier.invalid_target_records, 1)

    def test_other_household_normal_record_is_foreign_not_invalid_target(self):
        # 다른 가구(H002) 정상 레코드 -> 기대 UUID5가 일치하지 않으므로 foreign
        other_payload = {
            "message_id": str(uuid.uuid4()),
            "household_id": "H002",
            "measured_at": "2026-09-16T10:00:00+09:00",
            "power_w": 50.0,
        }
        rec = self._make_mock_record(b"H002", json.dumps(other_payload).encode("utf-8"), offset=6)
        res = self.verifier.process_record(rec)

        self.assertEqual(res, "FOREIGN")
        self.assertEqual(self.verifier.foreign_records, 1)
        self.assertEqual(self.verifier.invalid_target_records, 0)

    def test_smoke_record_is_foreign_not_invalid_target(self):
        smoke_payload = {
            "house": "smoke-12345",
            "power_w": 0.0,
            "ts": "2026-09-16T10:00:00+09:00",
            "measured_at": "2026-09-16T10:00:00+09:00",
            "smoke_test": True,
        }
        rec = self._make_mock_record(b"smoke-12345", json.dumps(smoke_payload).encode("utf-8"), offset=7)
        res = self.verifier.process_record(rec)

        self.assertEqual(res, "FOREIGN")
        self.assertEqual(self.verifier.foreign_records, 1)
        self.assertEqual(self.verifier.invalid_target_records, 0)

    def test_tombstone_record_is_malformed_unattributed(self):
        rec = self._make_mock_record(b"H001", None, offset=8)
        res = self.verifier.process_record(rec)

        self.assertEqual(res, "MALFORMED_UNATTRIBUTED")
        self.assertEqual(self.verifier.malformed_unattributed_records, 1)
        self.assertEqual(self.verifier.invalid_target_records, 0)
        self.assertTrue(any("Tombstone" in w for w in self.verifier.warning_messages))

    def test_malformed_non_json_is_malformed_unattributed(self):
        rec = self._make_mock_record(b"H001", b"not-a-json", offset=9)
        res = self.verifier.process_record(rec)

        self.assertEqual(res, "MALFORMED_UNATTRIBUTED")
        self.assertEqual(self.verifier.malformed_unattributed_records, 1)
        self.assertEqual(self.verifier.invalid_target_records, 0)
        self.assertTrue(any("Malformed non-JSON" in w for w in self.verifier.warning_messages))

    def test_non_canonical_measured_at_is_malformed_unattributed_not_invalid_target(self):
        # measured_at이 비정상이면 cycle을 역산할 수 없으므로 대상 런 귀속 불가 -> malformed_unattributed
        bad_payload = {
            "message_id": str(uuid.uuid4()),
            "measured_at": "2026-09-16T00:00:00.000+09:00",  # 비canonical
        }
        rec = self._make_mock_record(b"H001", json.dumps(bad_payload).encode("utf-8"), offset=10)
        res = self.verifier.process_record(rec)

        self.assertEqual(res, "MALFORMED_UNATTRIBUTED")
        self.assertEqual(self.verifier.malformed_unattributed_records, 1)
        self.assertEqual(self.verifier.invalid_target_records, 0)
        self.assertTrue(any("non-canonical/missing measured_at" in w for w in self.verifier.warning_messages))

    def test_bitmap_86400_completion(self):
        # 86,400개 모든 비트가 1로 채워졌을 때 missing_virtual_seconds 계산
        self.verifier.timeline_bitmap = bytearray([1] * SECONDS_PER_DAY)
        self.verifier.target_unique_records = SECONDS_PER_DAY
        missing = SECONDS_PER_DAY - self.verifier.target_unique_records
        self.assertEqual(missing, 0)


class KafkaOffsetAndConsumerIsolationTests(unittest.TestCase):
    """4. Kafka consumer 오프셋 불변 및 메소드 호출 계약 테스트"""

    @patch("verify_activity_normal_kafka.Consumer")
    @patch("verify_activity_normal_kafka.http_post_start_run")
    @patch("verify_activity_normal_kafka.http_get_run_snapshot")
    def test_consumer_isolation_settings_and_methods(
        self, mock_get_snap, mock_start_run, mock_consumer_cls
    ):
        mock_consumer = Mock()
        mock_consumer_cls.return_value = mock_consumer

        # 메타데이터 24개 파티션 모킹
        mock_topic_meta = Mock(error=None)
        mock_topic_meta.partitions = {p: Mock() for p in range(24)}
        mock_meta = Mock()
        mock_meta.topics = {TARGET_KAFKA_TOPIC: mock_topic_meta}
        mock_consumer.list_topics.return_value = mock_meta
        mock_consumer.get_watermark_offsets.return_value = (0, 100)

        # 즉시 종료되도록 모킹
        mock_start_run.return_value = "run_iso_test"
        mock_get_snap.return_value = {
            "overall_status": "COMPLETED",
            "households": [
                {
                    "household_id": TARGET_HOUSEHOLD,
                    "state": "COMPLETED",
                    "published_samples": SECONDS_PER_DAY,
                    "planned_publish_samples": SECONDS_PER_DAY,
                    "omitted_samples": 0,
                }
            ],
        }
        mock_consumer.poll.return_value = None

        summary, code = run_kafka_verification(
            kafka_bootstrap="localhost:9092",
            kafka_topic=TARGET_KAFKA_TOPIC,
            simulator_url="http://127.0.0.1:8085",
            drain_timeout=0.01,
            overall_timeout=0.5,
        )

        # Consumer 생성 시 옵션 검증
        init_conf = mock_consumer_cls.call_args[0][0]
        self.assertFalse(init_conf["enable.auto.commit"])
        self.assertFalse(init_conf["enable.auto.offset.store"])
        self.assertEqual(init_conf["auto.offset.reset"], "error")

        # assign()만 호출되고 subscribe(), commit(), store_offsets()는 절대 호출되지 않음
        mock_consumer.assign.assert_called_once()
        self.assertEqual(len(mock_consumer.assign.call_args[0][0]), 24)
        mock_consumer.subscribe.assert_not_called()
        mock_consumer.commit.assert_not_called()
        mock_consumer.store_offsets.assert_not_called()

        # consumer.close()가 정확히 1회 호출됨
        mock_consumer.close.assert_called_once()


class WatermarkAndPollErrorHandlingTests(unittest.TestCase):
    """5. Watermark 24개 파티션 및 Poll 오류 처리 테스트"""

    @patch("verify_activity_normal_kafka.Consumer")
    def test_mismatched_partition_count_fails_precheck(self, mock_consumer_cls):
        mock_consumer = Mock()
        mock_consumer_cls.return_value = mock_consumer
        mock_topic_meta = Mock(error=None)
        mock_topic_meta.partitions = {0: Mock(), 1: Mock()}  # 2개뿐 (기대 24개)
        mock_meta = Mock()
        mock_meta.topics = {TARGET_KAFKA_TOPIC: mock_topic_meta}
        mock_consumer.list_topics.return_value = mock_meta

        summary, code = run_kafka_verification(
            kafka_bootstrap="localhost:9092",
            kafka_topic=TARGET_KAFKA_TOPIC,
            simulator_url="http://127.0.0.1:8085",
        )
        self.assertEqual(code, EXIT_KAFKA_ERROR)
        self.assertIn("파티션 수가 기대값(24)과 일치하지 않습니다", summary.error_message)

    @patch("verify_activity_normal_kafka.Consumer")
    @patch("verify_activity_normal_kafka.http_post_start_run")
    @patch("verify_activity_normal_kafka.http_get_run_snapshot")
    def test_poll_partition_eof_is_ignored(
        self, mock_get_snap, mock_start_run, mock_consumer_cls
    ):
        mock_consumer = Mock()
        mock_consumer_cls.return_value = mock_consumer
        mock_topic_meta = Mock(error=None)
        mock_topic_meta.partitions = {p: Mock() for p in range(24)}
        mock_meta = Mock()
        mock_meta.topics = {TARGET_KAFKA_TOPIC: mock_topic_meta}
        mock_consumer.list_topics.return_value = mock_meta
        mock_consumer.get_watermark_offsets.return_value = (0, 0)
        mock_start_run.return_value = "run_eof_test"
        mock_get_snap.return_value = {"overall_status": "RUNNING"}

        # _PARTITION_EOF 에러 레코드 1회 반환 후 None을 지속 반환하여 overall_timeout 초과 유도
        mock_eof_err = Mock()
        mock_eof_err.code.return_value = KafkaError._PARTITION_EOF
        mock_eof_rec = Mock()
        mock_eof_rec.error.return_value = mock_eof_err

        has_yielded_eof = [False]

        def _mock_poll(timeout=0.2):
            if not has_yielded_eof[0]:
                has_yielded_eof[0] = True
                return mock_eof_rec
            return None

        mock_consumer.poll.side_effect = _mock_poll

        summary, code = run_kafka_verification(
            kafka_bootstrap="localhost:9092",
            kafka_topic=TARGET_KAFKA_TOPIC,
            simulator_url="http://127.0.0.1:8085",
            overall_timeout=0.05,
            poll_timeout=0.01,
        )
        # PARTITION_EOF로 인해 크래시되지 않고 계속 진행 후 타임아웃 종료됨
        self.assertEqual(code, EXIT_TIMEOUT)

    @patch("verify_activity_normal_kafka.Consumer")
    @patch("verify_activity_normal_kafka.http_post_start_run")
    @patch("verify_activity_normal_kafka.http_get_run_snapshot")
    def test_poll_fatal_kafka_error_causes_failure(
        self, mock_get_snap, mock_start_run, mock_consumer_cls
    ):
        mock_consumer = Mock()
        mock_consumer_cls.return_value = mock_consumer
        mock_topic_meta = Mock(error=None)
        mock_topic_meta.partitions = {p: Mock() for p in range(24)}
        mock_meta = Mock()
        mock_meta.topics = {TARGET_KAFKA_TOPIC: mock_topic_meta}
        mock_consumer.list_topics.return_value = mock_meta
        mock_consumer.get_watermark_offsets.return_value = (0, 0)
        mock_start_run.return_value = "run_fatal_err_test"
        mock_get_snap.return_value = {"overall_status": "RUNNING"}

        # 치명적 에러 레코드 (BROKER_NOT_AVAILABLE 등)
        mock_fatal_err = Mock()
        mock_fatal_err.code.return_value = KafkaError._FAIL
        mock_fatal_rec = Mock()
        mock_fatal_rec.error.return_value = mock_fatal_err
        mock_consumer.poll.return_value = mock_fatal_rec

        summary, code = run_kafka_verification(
            kafka_bootstrap="localhost:9092",
            kafka_topic=TARGET_KAFKA_TOPIC,
            simulator_url="http://127.0.0.1:8085",
            overall_timeout=1.0,
        )
        self.assertEqual(code, EXIT_KAFKA_ERROR)
        self.assertIn("Kafka record 오류", summary.error_message)


class SecretRedactionAndCliTests(unittest.TestCase):
    """비밀번호 마스킹 및 CLI 설정 검증 테스트"""

    def test_redact_secrets_longest_first(self):
        text = "Connecting with pass: secret_long and secret"
        res = redact_secrets(text, secret=["secret", "secret_long"])
        self.assertEqual(res, "Connecting with pass: *** and ***")

    def test_redact_secrets_with_whitespace_password(self):
        text = "Password was:    in system"
        res = redact_secrets(text, secret="   ")
        self.assertNotIn("   ", res)
        self.assertIn("***", res)

    def test_cli_help(self):
        with patch("sys.stdout"):
            with self.assertRaises(SystemExit) as cm:
                parse_cli_args(["--help"])
            self.assertEqual(cm.exception.code, 0)

    def test_cli_invalid_drain_timeout(self):
        from verify_activity_normal_kafka import main
        with patch("sys.stderr"):
            code = main(["--drain-timeout", "-5"])
            self.assertEqual(code, EXIT_CLI_CONFIG_ERROR)


class ExtendedLifecycleAndPartitionTests(unittest.TestCase):
    """전체 24개 파티션 보존, foreign/malformed 공존 성공, 실패 시 stop 및 인터럽트 테스트"""

    def test_success_with_foreign_and_malformed_records_present(self):
        run_id = "run_coexist_001"
        start_offsets = {p: 0 for p in range(24)}
        verifier = KafkaRecordVerifier(run_id, TARGET_KAFKA_TOPIC, start_offsets)

        # 1. 외래 레코드 3건 투입
        for i in range(3):
            foreign_rec = Mock()
            foreign_rec.key.return_value = b"H002"
            foreign_rec.value.return_value = json.dumps({"household_id": "H002", "message_id": f"foreign_{i}", "measured_at": "2026-09-16T10:00:00+09:00"}).encode()
            foreign_rec.partition.return_value = i
            foreign_rec.offset.return_value = i
            verifier.process_record(foreign_rec)

        # 2. malformed 레코드 2건 투입
        malformed_rec1 = Mock()
        malformed_rec1.key.return_value = b"H001"
        malformed_rec1.value.return_value = None  # Tombstone
        malformed_rec1.partition.return_value = 5
        malformed_rec1.offset.return_value = 0
        verifier.process_record(malformed_rec1)

        malformed_rec2 = Mock()
        malformed_rec2.key.return_value = b"H001"
        malformed_rec2.value.return_value = b"broken-json"
        malformed_rec2.partition.return_value = 6
        malformed_rec2.offset.return_value = 0
        verifier.process_record(malformed_rec2)

        # 3. 대상 런의 86,400개 완전 레코드 투입 (시뮬레이션: 비트맵 전체 채움)
        verifier.timeline_bitmap = bytearray([1] * SECONDS_PER_DAY)
        verifier.target_unique_records = SECONDS_PER_DAY

        # 검증: foreign과 malformed가 존재하더라도 target_unique는 86,400이며 missing은 0
        self.assertEqual(verifier.target_unique_records, SECONDS_PER_DAY)
        self.assertEqual(verifier.foreign_records, 3)
        self.assertEqual(verifier.malformed_unattributed_records, 2)
        self.assertEqual(verifier.invalid_target_records, 0)
        self.assertEqual(verifier.duplicate_target_records, 0)
        self.assertEqual(SECONDS_PER_DAY - verifier.target_unique_records, 0)
        self.assertTrue(len(verifier.warning_messages) >= 2)

    @patch("verify_activity_normal_kafka.Consumer")
    @patch("verify_activity_normal_kafka.http_post_start_run")
    @patch("verify_activity_normal_kafka.http_get_run_snapshot")
    def test_start_and_end_offsets_contain_all_24_partitions(
        self, mock_get_snap, mock_start_run, mock_consumer_cls
    ):
        mock_consumer = Mock()
        mock_consumer_cls.return_value = mock_consumer
        mock_topic_meta = Mock(error=None)
        mock_topic_meta.partitions = {p: Mock() for p in range(24)}
        mock_meta = Mock()
        mock_meta.topics = {TARGET_KAFKA_TOPIC: mock_topic_meta}
        mock_consumer.list_topics.return_value = mock_meta
        mock_consumer.get_watermark_offsets.return_value = (0, 100)
        mock_start_run.return_value = "run_all_parts"
        mock_get_snap.return_value = {
            "overall_status": "COMPLETED",
            "households": [
                {
                    "household_id": TARGET_HOUSEHOLD,
                    "state": "COMPLETED",
                    "published_samples": SECONDS_PER_DAY,
                    "planned_publish_samples": SECONDS_PER_DAY,
                    "omitted_samples": 0,
                }
            ],
        }
        mock_consumer.poll.return_value = None

        summary, code = run_kafka_verification(
            kafka_bootstrap="localhost:9092",
            kafka_topic=TARGET_KAFKA_TOPIC,
            simulator_url="http://127.0.0.1:8085",
            drain_timeout=0.01,
            overall_timeout=0.5,
        )

        # 24개 파티션이 start_offsets와 end_offsets에 모두 존재하는지 확인
        self.assertEqual(len(summary.start_offsets), 24)
        self.assertEqual(len(summary.end_offsets), 24)
        for p in range(24):
            self.assertIn(str(p), summary.start_offsets)
            self.assertIn(str(p), summary.end_offsets)

    @patch("verify_activity_normal_kafka.http_post_stop_run_best_effort")
    @patch("verify_activity_normal_kafka.Consumer")
    @patch("verify_activity_normal_kafka.http_post_start_run")
    @patch("verify_activity_normal_kafka.http_get_run_snapshot")
    def test_best_effort_stop_called_on_failure(
        self, mock_get_snap, mock_start_run, mock_consumer_cls, mock_stop_run
    ):
        mock_consumer = Mock()
        mock_consumer_cls.return_value = mock_consumer
        mock_topic_meta = Mock(error=None)
        mock_topic_meta.partitions = {p: Mock() for p in range(24)}
        mock_meta = Mock()
        mock_meta.topics = {TARGET_KAFKA_TOPIC: mock_topic_meta}
        mock_consumer.list_topics.return_value = mock_meta
        mock_consumer.get_watermark_offsets.return_value = (0, 0)
        mock_consumer.poll.return_value = None
        mock_start_run.return_value = "run_stop_test"
        mock_get_snap.return_value = {"overall_status": "FAILED"}

        summary, code = run_kafka_verification(
            kafka_bootstrap="localhost:9092",
            kafka_topic=TARGET_KAFKA_TOPIC,
            simulator_url="http://127.0.0.1:8085",
            overall_timeout=1.0,
        )
        self.assertEqual(code, EXIT_VERIFICATION_FAILURE)
        mock_stop_run.assert_called_once_with("http://127.0.0.1:8085", "run_stop_test")

    @patch("verify_activity_normal_kafka.http_post_stop_run_best_effort")
    @patch("verify_activity_normal_kafka.Consumer")
    @patch("verify_activity_normal_kafka.http_post_start_run")
    def test_keyboard_interrupt_handles_cleanly(
        self, mock_start_run, mock_consumer_cls, mock_stop_run
    ):
        mock_consumer = Mock()
        mock_consumer_cls.return_value = mock_consumer
        mock_topic_meta = Mock(error=None)
        mock_topic_meta.partitions = {p: Mock() for p in range(24)}
        mock_meta = Mock()
        mock_meta.topics = {TARGET_KAFKA_TOPIC: mock_topic_meta}
        mock_consumer.list_topics.return_value = mock_meta
        mock_consumer.get_watermark_offsets.return_value = (0, 0)
        mock_start_run.return_value = "run_sigint_test"
        mock_consumer.poll.side_effect = KeyboardInterrupt()

        with patch("sys.stderr"):
            with self.assertRaises(SystemExit) as cm:
                run_kafka_verification(
                    kafka_bootstrap="localhost:9092",
                    kafka_topic=TARGET_KAFKA_TOPIC,
                    simulator_url="http://127.0.0.1:8085",
                )
            self.assertEqual(cm.exception.code, 130)
        mock_consumer.close.assert_called_once()
        mock_stop_run.assert_called_once_with("http://127.0.0.1:8085", "run_sigint_test")


class ExtractHouseholdSnapshotTests(unittest.TestCase):
    """A. 실제 API 응답 형식 (households list) 검증 테스트"""

    def test_extract_valid_list_finds_h001(self):
        snap = {
            "overall_status": "COMPLETED",
            "households": [
                {"household_id": "H002", "state": "RUNNING"},
                {
                    "household_id": TARGET_HOUSEHOLD,
                    "state": "COMPLETED",
                    "published_samples": SECONDS_PER_DAY,
                    "planned_publish_samples": SECONDS_PER_DAY,
                    "omitted_samples": 0,
                },
            ],
        }
        h_dict, err = extract_household_snapshot(snap, TARGET_HOUSEHOLD)
        self.assertIsNone(err)
        self.assertIsNotNone(h_dict)
        self.assertEqual(h_dict["household_id"], TARGET_HOUSEHOLD)
        self.assertEqual(h_dict["state"], "COMPLETED")

    def test_extract_not_dict_returns_clean_failure(self):
        for bad_snap in (None, "not a dict", 123, []):
            with self.subTest(bad_snap=bad_snap):
                h_dict, err = extract_household_snapshot(bad_snap, TARGET_HOUSEHOLD)
                self.assertIsNone(h_dict)
                self.assertIn("API snapshot is not an object", err)

    def test_extract_households_not_list_returns_clean_failure(self):
        bad_snap = {
            "overall_status": "COMPLETED",
            "households": {TARGET_HOUSEHOLD: {"state": "COMPLETED"}},
        }
        h_dict, err = extract_household_snapshot(bad_snap, TARGET_HOUSEHOLD)
        self.assertIsNone(h_dict)
        self.assertIn("households field is not a list", err)

    def test_extract_h001_not_found_returns_clean_failure(self):
        snap = {
            "overall_status": "COMPLETED",
            "households": [{"household_id": "H002", "state": "COMPLETED"}],
        }
        h_dict, err = extract_household_snapshot(snap, TARGET_HOUSEHOLD)
        self.assertIsNone(h_dict)
        self.assertIn(f"household {TARGET_HOUSEHOLD} not found in households list", err)

    def test_extract_item_not_dict_returns_clean_failure(self):
        snap = {
            "overall_status": "COMPLETED",
            "households": ["not-a-dict", {"household_id": "H002"}],
        }
        h_dict, err = extract_household_snapshot(snap, TARGET_HOUSEHOLD)
        self.assertIsNone(h_dict)
        self.assertIn("household entry is not an object", err)

    def test_extract_none_item_returns_clean_failure(self):
        snap = {
            "overall_status": "COMPLETED",
            "households": [
                None,
                {"household_id": TARGET_HOUSEHOLD},
            ],
        }

        h_dict, err = extract_household_snapshot(
            snap,
            TARGET_HOUSEHOLD,
        )

        self.assertIsNone(h_dict)
        self.assertIn(
            "household entry is not an object: NoneType",
            err,
        )



class FullSuccessPathAndDrainTests(unittest.TestCase):
    """B & C. 전체 성공 결과 검증 및 Post-completion Drain 동작 검증 테스트"""

    def _setup_mock_consumer_and_metadata(self, mock_consumer_cls):
        mock_consumer = Mock()
        mock_consumer_cls.return_value = mock_consumer
        mock_topic_meta = Mock(error=None)
        mock_topic_meta.partitions = {p: Mock() for p in range(24)}
        mock_meta = Mock()
        mock_meta.topics = {TARGET_KAFKA_TOPIC: mock_topic_meta}
        mock_consumer.list_topics.return_value = mock_meta
        mock_consumer.get_watermark_offsets.return_value = (0, 100)
        return mock_consumer

    @patch("verify_activity_normal_kafka.Consumer")
    @patch("verify_activity_normal_kafka.http_post_start_run")
    @patch("verify_activity_normal_kafka.http_get_run_snapshot")
    def test_full_success_path_verification(
        self, mock_get_snap, mock_start_run, mock_consumer_cls
    ):
        """B. 전체 성공 결과 검증: code == 0, status == SUCCESS, error_message is None"""
        mock_consumer = self._setup_mock_consumer_and_metadata(mock_consumer_cls)
        mock_start_run.return_value = "run_full_success_001"
        mock_get_snap.return_value = {
            "overall_status": "COMPLETED",
            "households": [
                {
                    "household_id": TARGET_HOUSEHOLD,
                    "state": "COMPLETED",
                    "published_samples": SECONDS_PER_DAY,
                    "planned_publish_samples": SECONDS_PER_DAY,
                    "omitted_samples": 0,
                }
            ],
        }
        mock_consumer.poll.return_value = None

        # 86,400개 완전 레코드 비트맵 사전 준비
        verifier = KafkaRecordVerifier(
            run_id="run_full_success_001",
            topic=TARGET_KAFKA_TOPIC,
            start_offsets={p: 100 for p in range(24)},
        )
        verifier.timeline_bitmap = bytearray([1] * SECONDS_PER_DAY)
        verifier.target_unique_records = SECONDS_PER_DAY
        verifier.first_measured_at = "2026-09-16T00:00:00+09:00"
        verifier.last_measured_at = "2026-09-16T23:59:59+09:00"

        with patch(
            "verify_activity_normal_kafka.KafkaRecordVerifier",
            return_value=verifier,
        ):
            summary, code = run_kafka_verification(
                kafka_bootstrap="localhost:9092",
                kafka_topic=TARGET_KAFKA_TOPIC,
                simulator_url="http://127.0.0.1:8085",
                drain_timeout=0.01,
                overall_timeout=0.5,
            )

        self.assertEqual(code, EXIT_SUCCESS)
        self.assertEqual(summary.status, "SUCCESS")
        self.assertEqual(summary.exit_code, EXIT_SUCCESS)
        self.assertIsNone(summary.error_message)
        self.assertEqual(summary.target_unique_records, SECONDS_PER_DAY)
        self.assertEqual(summary.duplicate_target_records, 0)
        self.assertEqual(summary.invalid_target_records, 0)
        self.assertEqual(summary.missing_virtual_seconds, 0)
        self.assertEqual(summary.first_measured_at, "2026-09-16T00:00:00+09:00")
        self.assertEqual(summary.last_measured_at, "2026-09-16T23:59:59+09:00")

    @patch("verify_activity_normal_kafka.Consumer")
    @patch("verify_activity_normal_kafka.http_post_start_run")
    @patch("verify_activity_normal_kafka.http_get_run_snapshot")
    def test_drain_window_consumes_duplicate_record_and_fails(
        self, mock_get_snap, mock_start_run, mock_consumer_cls
    ):
        """C. Drain 동작 검증: API COMPLETED 및 86,400개 달성 후 중복 레코드가 poll되면 drain에서 소비되어 실패"""
        mock_consumer = self._setup_mock_consumer_and_metadata(mock_consumer_cls)
        run_id = "run_drain_dup_001"
        mock_start_run.return_value = run_id
        mock_get_snap.return_value = {
            "overall_status": "COMPLETED",
            "households": [
                {
                    "household_id": TARGET_HOUSEHOLD,
                    "state": "COMPLETED",
                    "published_samples": SECONDS_PER_DAY,
                    "planned_publish_samples": SECONDS_PER_DAY,
                    "omitted_samples": 0,
                }
            ],
        }

        # 86,400개 완전 레코드 비트맵 사전 준비
        verifier = KafkaRecordVerifier(
            run_id=run_id,
            topic=TARGET_KAFKA_TOPIC,
            start_offsets={p: 100 for p in range(24)},
        )
        verifier.timeline_bitmap = bytearray([1] * SECONDS_PER_DAY)
        verifier.target_unique_records = SECONDS_PER_DAY
        verifier.first_measured_at = "2026-09-16T00:00:00+09:00"
        verifier.last_measured_at = "2026-09-16T23:59:59+09:00"

        # drain 윈도우 중 중복 레코드(가상초 50) 1건 도착 후 None 반환
        dup_payload = make_valid_payload(run_id, 50)
        dup_rec = Mock()
        dup_rec.key.return_value = b"H001"
        dup_rec.value.return_value = json.dumps(dup_payload).encode("utf-8")
        dup_rec.partition.return_value = 14
        dup_rec.offset.return_value = 101
        dup_rec.error.return_value = None

        # 1차 poll은 None 반환 (첫 루프에서 API COMPLETED 관측 및 drain deadline 설정 유도)
        # 2차 poll에서 중복 레코드(가상초 50) 반환 (실제 drain 윈도우에서 소비됨을 보장)
        poll_calls = [None, dup_rec, None, None]

        def _mock_poll(timeout=0.2):
            if poll_calls:
                return poll_calls.pop(0)
            return None

        mock_consumer.poll.side_effect = _mock_poll

        with patch(
            "verify_activity_normal_kafka.KafkaRecordVerifier",
            return_value=verifier,
        ):
            summary, code = run_kafka_verification(
                kafka_bootstrap="localhost:9092",
                kafka_topic=TARGET_KAFKA_TOPIC,
                simulator_url="http://127.0.0.1:8085",
                drain_timeout=0.05,
                overall_timeout=0.5,
            )

        # 1차(None), 2차(dup_rec) 이상 최소 2회 이상 poll되었음을 검증
        self.assertGreaterEqual(mock_consumer.poll.call_count, 2)
        # 즉시 종료되지 않고 API 완료 후 drain 윈도우에서 중복을 소비하여 실패해야 함
        self.assertEqual(code, EXIT_VERIFICATION_FAILURE)
        self.assertEqual(summary.status, "FAILURE")
        self.assertEqual(summary.duplicate_target_records, 1)
        self.assertIn("duplicate_target_records != 0 (1)", summary.error_message)

    @patch("verify_activity_normal_kafka.Consumer")
    @patch("verify_activity_normal_kafka.http_post_start_run")
    @patch("verify_activity_normal_kafka.http_get_run_snapshot")
    def test_drain_window_consumes_invalid_target_and_fails(
        self, mock_get_snap, mock_start_run, mock_consumer_cls
    ):
        """C. Drain 동작 검증: API COMPLETED 후 invalid target 도착 시 실패"""
        mock_consumer = self._setup_mock_consumer_and_metadata(mock_consumer_cls)
        run_id = "run_drain_inv_001"
        mock_start_run.return_value = run_id
        mock_get_snap.return_value = {
            "overall_status": "COMPLETED",
            "households": [
                {
                    "household_id": TARGET_HOUSEHOLD,
                    "state": "COMPLETED",
                    "published_samples": SECONDS_PER_DAY,
                    "planned_publish_samples": SECONDS_PER_DAY,
                    "omitted_samples": 0,
                }
            ],
        }

        verifier = KafkaRecordVerifier(
            run_id=run_id,
            topic=TARGET_KAFKA_TOPIC,
            start_offsets={p: 100 for p in range(24)},
        )
        verifier.timeline_bitmap = bytearray([1] * SECONDS_PER_DAY)
        verifier.target_unique_records = SECONDS_PER_DAY
        verifier.first_measured_at = "2026-09-16T00:00:00+09:00"
        verifier.last_measured_at = "2026-09-16T23:59:59+09:00"

        # UUID5는 일치하나 key가 b"H002"인 invalid target 1건 도착
        inv_payload = make_valid_payload(run_id, 75)
        inv_rec = Mock()
        inv_rec.key.return_value = b"H002"  # key mismatch
        inv_rec.value.return_value = json.dumps(inv_payload).encode("utf-8")
        inv_rec.partition.return_value = 14
        inv_rec.offset.return_value = 102
        inv_rec.error.return_value = None

        # 1차 poll은 None 반환 (첫 루프에서 API COMPLETED 관측 및 drain deadline 설정 유도)
        # 2차 poll에서 invalid target(key mismatch) 반환 (실제 drain 윈도우에서 소비됨을 보장)
        poll_calls = [None, inv_rec, None, None]

        def _mock_poll(timeout=0.2):
            if poll_calls:
                return poll_calls.pop(0)
            return None

        mock_consumer.poll.side_effect = _mock_poll

        with patch(
            "verify_activity_normal_kafka.KafkaRecordVerifier",
            return_value=verifier,
        ):
            summary, code = run_kafka_verification(
                kafka_bootstrap="localhost:9092",
                kafka_topic=TARGET_KAFKA_TOPIC,
                simulator_url="http://127.0.0.1:8085",
                drain_timeout=0.05,
                overall_timeout=0.5,
            )

        self.assertGreaterEqual(mock_consumer.poll.call_count, 2)
        self.assertEqual(code, EXIT_VERIFICATION_FAILURE)
        self.assertEqual(summary.status, "FAILURE")
        self.assertEqual(summary.invalid_target_records, 1)
        self.assertIn("invalid_target_records != 0 (1)", summary.error_message)

    @patch("verify_activity_normal_kafka.Consumer")
    @patch("verify_activity_normal_kafka.http_post_start_run")
    @patch("verify_activity_normal_kafka.http_get_run_snapshot")
    def test_overall_timeout_before_drain_deadline_retains_timeout_exit(
        self, mock_get_snap, mock_start_run, mock_consumer_cls
    ):
        """C. Drain 동작 검증: drain 도중 overall_timeout 초과 시 EXIT_TIMEOUT 유지"""
        mock_consumer = self._setup_mock_consumer_and_metadata(mock_consumer_cls)
        mock_start_run.return_value = "run_timeout_drain"
        mock_get_snap.return_value = {
            "overall_status": "COMPLETED",
            "households": [
                {
                    "household_id": TARGET_HOUSEHOLD,
                    "state": "COMPLETED",
                    "published_samples": SECONDS_PER_DAY,
                    "planned_publish_samples": SECONDS_PER_DAY,
                    "omitted_samples": 0,
                }
            ],
        }
        mock_consumer.poll.return_value = None

        summary, code = run_kafka_verification(
            kafka_bootstrap="localhost:9092",
            kafka_topic=TARGET_KAFKA_TOPIC,
            simulator_url="http://127.0.0.1:8085",
            drain_timeout=10.0,
            overall_timeout=0.05,
            poll_timeout=0.01,
        )

        self.assertEqual(code, EXIT_TIMEOUT)
        self.assertEqual(summary.status, "FAILURE")
        self.assertEqual(summary.exit_code, EXIT_TIMEOUT)
        self.assertIn("전체 제한시간", summary.error_message)

    @patch("verify_activity_normal_kafka.Consumer")
    @patch("verify_activity_normal_kafka.http_post_start_run")
    @patch("verify_activity_normal_kafka.http_get_run_snapshot")
    def test_clean_failure_when_households_is_dict(
        self, mock_get_snap, mock_start_run, mock_consumer_cls
    ):
        """A. 실제 API 응답 형태 테스트: households가 dict 형태일 때 clean failure"""
        mock_consumer = self._setup_mock_consumer_and_metadata(mock_consumer_cls)
        mock_start_run.return_value = "run_bad_snap_001"
        mock_get_snap.return_value = {
            "overall_status": "COMPLETED",
            "households": {"H001": {"state": "COMPLETED"}},  # 잘못된 dict 형태
        }
        mock_consumer.poll.return_value = None

        summary, code = run_kafka_verification(
            kafka_bootstrap="localhost:9092",
            kafka_topic=TARGET_KAFKA_TOPIC,
            simulator_url="http://127.0.0.1:8085",
            drain_timeout=0.01,
            overall_timeout=0.5,
        )
        self.assertEqual(code, EXIT_VERIFICATION_FAILURE)
        self.assertEqual(summary.status, "FAILURE")
        self.assertIn("households field is not a list", summary.error_message)

    @patch("verify_activity_normal_kafka.Consumer")
    @patch("verify_activity_normal_kafka.http_post_start_run")
    @patch("verify_activity_normal_kafka.http_get_run_snapshot")
    def test_clean_failure_when_h001_is_missing(
        self, mock_get_snap, mock_start_run, mock_consumer_cls
    ):
        """A. 실제 API 응답 형태 테스트: households 리스트에 H001이 없을 때 clean failure"""
        mock_consumer = self._setup_mock_consumer_and_metadata(mock_consumer_cls)
        mock_start_run.return_value = "run_missing_h001_001"
        mock_get_snap.return_value = {
            "overall_status": "COMPLETED",
            "households": [
                {
                    "household_id": "H002",
                    "state": "COMPLETED",
                    "published_samples": 86400,
                    "planned_publish_samples": 86400,
                    "omitted_samples": 0,
                }
            ],
        }
        mock_consumer.poll.return_value = None

        summary, code = run_kafka_verification(
            kafka_bootstrap="localhost:9092",
            kafka_topic=TARGET_KAFKA_TOPIC,
            simulator_url="http://127.0.0.1:8085",
            drain_timeout=0.01,
            overall_timeout=0.5,
        )
        self.assertEqual(code, EXIT_VERIFICATION_FAILURE)
        self.assertEqual(summary.status, "FAILURE")
        self.assertIn(
            "household H001 not found in households list",
            summary.error_message,
        )


class CliValidationExtendedTests(unittest.TestCase):
    """5. 추가 점검: CLI 인자 유효성 검증 단위 테스트"""

    def test_cli_invalid_api_poll_interval(self):
        from verify_activity_normal_kafka import main

        for bad_val in ("-1", "0", "nan", "inf"):
            with self.subTest(bad_val=bad_val):
                with patch("sys.stderr"):
                    code = main(["--api-poll-interval", bad_val])
                    self.assertEqual(code, EXIT_CLI_CONFIG_ERROR)

    def test_cli_invalid_poll_timeout_negative(self):
        from verify_activity_normal_kafka import main

        for bad_val in ("-0.1", "-1", "nan", "inf"):
            with self.subTest(bad_val=bad_val):
                with patch("sys.stderr"):
                    code = main(["--poll-timeout", bad_val])
                    self.assertEqual(code, EXIT_CLI_CONFIG_ERROR)

    def test_cli_poll_timeout_zero_allowed(self):
        args = parse_cli_args(["--poll-timeout", "0"])
        self.assertEqual(args.poll_timeout, 0.0)

    def test_cli_invalid_overall_timeout(self):
        from verify_activity_normal_kafka import main

        with patch("sys.stderr"):
            code = main(["--overall-timeout", "0"])
            self.assertEqual(code, EXIT_CLI_CONFIG_ERROR)


if __name__ == "__main__":
    unittest.main()

