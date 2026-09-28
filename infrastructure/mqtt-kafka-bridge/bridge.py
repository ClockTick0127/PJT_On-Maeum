"""MQTT QoS 1 -> Kafka. Acknowledge only after Kafka confirms delivery."""
import json
import logging
import os
from pathlib import Path
import signal
import ssl
import threading
import time

import paho.mqtt.client as mqtt
from confluent_kafka import Producer

LOG = logging.getLogger("bridge")
HEALTH_FILE = Path("/tmp/bridge-ready")


def forward_message(client, producer, message, topic, failed):
    try:
        payload = json.loads(message.payload)
        if not isinstance(payload, dict):
            raise ValueError("payload must be an object")
        house = payload.get("household_id") or payload.get("house")
        has_required = ({"power_w", "ts"} <= payload.keys()) or ({"active_power", "measured_at"} <= payload.keys())
        if not isinstance(house, str) or not house or not has_required:
            raise ValueError("house/household_id, power_w/active_power and ts/measured_at required")
    except (ValueError, UnicodeError):
        LOG.warning("Skipping invalid message on %s", message.topic)
        client.ack(message.mid, message.qos)
        return

    def delivered(error, _record):
        if error:
            LOG.error("Kafka delivery failed: %s", error)
            failed.set()
        else:
            client.ack(message.mid, message.qos)

    try:
        producer.produce(topic, key=house.encode(), value=json.dumps(payload).encode(),
                         on_delivery=delivered)
    except Exception:
        LOG.exception("Kafka enqueue failed")
        failed.set()


def configure_tls(client):
    if os.getenv("MQTT_TLS_ENABLED", "false").lower() == "true":
        client.tls_set(ca_certs=os.getenv("MQTT_CA_FILE") or None,
                       cert_reqs=ssl.CERT_REQUIRED)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stop, failed, subscribed = threading.Event(), threading.Event(), threading.Event()
    HEALTH_FILE.unlink(missing_ok=True)
    topic = os.getenv("KAFKA_TOPIC", "power.raw.v1")
    producer = Producer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
        "enable.idempotence": True, "acks": "all", "linger.ms": 20,
        "delivery.timeout.ms": 30000,
    })
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                         client_id=os.getenv("MQTT_CLIENT_ID", "nilm-kafka-bridge"),
                         clean_session=False, manual_ack=True)
    username = os.getenv("MQTT_USER", "kafka_bridge_user")
    if username:
        client.username_pw_set(username, os.getenv("MQTT_PASS", "test1234"))
    configure_tls(client)
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    def on_connect(connection, _userdata, _flags, reason, _properties):
        subscribed.clear()
        if reason == 0:
            connection.subscribe(os.getenv("MQTT_TOPIC", "v1/power/sim/+/main"), qos=1)
        else:
            LOG.error("MQTT authentication/connection failed: %s", reason)

    def on_subscribe(_client, _userdata, _mid, reasons, _properties):
        if any(reason.is_failure for reason in reasons):
            failed.set()
        else:
            subscribed.set()
            LOG.info("MQTT subscription ready")

    def on_disconnect(_client, _userdata, _flags, _reason, _properties):
        subscribed.clear()
        HEALTH_FILE.unlink(missing_ok=True)

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_disconnect = on_disconnect
    client.on_message = lambda c, u, m: forward_message(c, producer, m, topic, failed)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_args: stop.set())

    client.connect_async(os.getenv("MQTT_HOST", "localhost"),
                         int(os.getenv("MQTT_PORT", "1883")), keepalive=30)
    client.loop_start()
    kafka_ready, next_check = False, 0.0
    try:
        while not stop.is_set() and not failed.is_set():
            served = producer.poll(0.05)
            while served > 0 and not stop.is_set() and not failed.is_set():
                served = producer.poll(0)
            now = time.monotonic()
            if now >= next_check:
                try:
                    metadata = producer.list_topics(topic=topic, timeout=5)
                    info = metadata.topics.get(topic)
                    kafka_ready = bool(info and info.error is None and info.partitions)
                except Exception:
                    kafka_ready = False
                    LOG.warning("Kafka not ready; retrying")
                next_check = now + 10
            if subscribed.is_set() and kafka_ready:
                HEALTH_FILE.touch()
            else:
                HEALTH_FILE.unlink(missing_ok=True)
    finally:
        HEALTH_FILE.unlink(missing_ok=True)
        # Keep unacknowledged messages in the broker's persistent session.
        client.on_message = lambda *_args: None
        remaining = producer.flush(20)
        client.disconnect()
        client.loop_stop()
    return 1 if failed.is_set() or remaining else 0


if __name__ == "__main__":
    raise SystemExit(main())
