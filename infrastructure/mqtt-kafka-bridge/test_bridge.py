import json
import os
import ssl
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch
from bridge import configure_tls, forward_message


class ForwardingTests(unittest.TestCase):
    def setUp(self):
        self.client, self.producer = Mock(), Mock()
        self.failed = threading.Event()
        self.message = SimpleNamespace(
            payload=json.dumps({"house": "H001", "power_w": 10, "ts": "2026-09-06T00:00:00Z"}).encode(),
            mid=12, qos=1, topic="v1/power/sim/H001/main")

    def forward(self):
        forward_message(self.client, self.producer, self.message, "power.raw.v1", self.failed)

    def test_ack_only_after_kafka_delivery(self):
        self.forward()
        self.client.ack.assert_not_called()
        kwargs = self.producer.produce.call_args.kwargs
        self.assertEqual(kwargs["key"], b"H001")
        kwargs["on_delivery"](None, Mock())
        self.client.ack.assert_called_once_with(12, 1)

    def test_failed_delivery_is_not_acknowledged(self):
        self.forward()
        self.producer.produce.call_args.kwargs["on_delivery"]("offline", Mock())
        self.client.ack.assert_not_called()
        self.assertTrue(self.failed.is_set())

    def test_full_queue_is_not_acknowledged(self):
        self.producer.produce.side_effect = BufferError("full")
        self.forward()
        self.client.ack.assert_not_called()
        self.assertTrue(self.failed.is_set())

    def test_malformed_payload_is_skipped(self):
        for payload in (b"not json", b"[]", b"{}", b'{"house":42,"power_w":1,"ts":"x"}'):
            with self.subTest(payload=payload):
                self.client.reset_mock()
                self.message.payload = payload
                self.forward()
                self.client.ack.assert_called_once_with(12, 1)
        self.producer.produce.assert_not_called()

    def test_tls_verifies_certificate(self):
        with patch.dict(os.environ, {"MQTT_TLS_ENABLED": "true", "MQTT_CA_FILE": "/ca.crt"}):
            configure_tls(self.client)
        self.client.tls_set.assert_called_once_with(ca_certs="/ca.crt", cert_reqs=ssl.CERT_REQUIRED)
        self.client.tls_insecure_set.assert_not_called()


if __name__ == "__main__":
    unittest.main()
