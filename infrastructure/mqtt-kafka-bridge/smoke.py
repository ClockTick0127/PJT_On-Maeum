"""Publish a unique MQTT sample and verify that the exact sample reaches Kafka."""
import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
from confluent_kafka import Consumer, TopicPartition
from bridge import configure_tls


def main():
    marker = "smoke-" + uuid.uuid4().hex
    topic = os.getenv("KAFKA_TOPIC", "power.raw.v1")
    consumer = Consumer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
        "group.id": marker, "enable.auto.commit": False, "auto.offset.reset": "latest"})
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=marker)
    client.username_pw_set(os.environ["MQTT_USER"], os.environ["MQTT_PASS"])
    configure_tls(client)
    connected = threading.Event()
    client.on_connect = lambda c, u, f, rc, p: connected.set() if rc == 0 else None
    try:
        metadata = consumer.list_topics(topic=topic, timeout=15)
        info = metadata.topics.get(topic)
        if not info or info.error:
            raise RuntimeError("Kafka topic is unavailable")
        partitions = []
        for number in info.partitions:
            tp = TopicPartition(topic, number)
            _, high = consumer.get_watermark_offsets(tp, timeout=10)
            partitions.append(TopicPartition(topic, number, high))
        consumer.assign(partitions)
        client.connect(os.environ["MQTT_HOST"], int(os.getenv("MQTT_PORT", "1883")), 30)
        client.loop_start()
        if not connected.wait(15):
            raise RuntimeError("MQTT connection/authentication failed")
        payload = {"house": marker, "power_w": 0.0,
                   "ts": datetime.now(timezone.utc).isoformat(), "smoke_test": True}
        publication = client.publish("v1/power/sim/" + marker + "/main", json.dumps(payload), qos=1)
        publication.wait_for_publish(timeout=15)
        if not publication.is_published():
            raise RuntimeError("MQTT publish timed out")
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            record = consumer.poll(1)
            if record and not record.error() and record.key() == marker.encode():
                if json.loads(record.value()) == payload:
                    print("PASS: MQTT -> Bridge -> Kafka (unique sample)")
                    return
        raise RuntimeError("Sample did not reach Kafka within 60 seconds")
    finally:
        client.disconnect()
        client.loop_stop()
        consumer.close()


if __name__ == "__main__":
    main()
