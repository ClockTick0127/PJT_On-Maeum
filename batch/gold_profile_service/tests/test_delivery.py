from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from realtime_analysis.database import Base

from gold_profile.config import GoldProfileSettings
from gold_profile.delivery import (
    GoldProfileDeliveryOutbox, GoldProfilePublisher, enqueue_payloads,
)


class FakeProducer:
    def __init__(self, fail=False):
        self.fail = fail
        self.messages = []
        self.callback = None

    def produce(self, **kwargs):
        self.messages.append(kwargs)
        self.callback = kwargs["callback"]

    def flush(self, _timeout):
        self.callback(RuntimeError("broker unavailable") if self.fail else None, None)
        return 0


def payload(version="run-1"):
    return {
        "schema_version": 2,
        "household_id": "H001",
        "profile_version": version,
        "profile_revision": 3,
        "delivery_mode": "ACTIVE",
    }


def test_failed_publish_is_retried_and_duplicate_enqueue_is_harmless(tmp_path):
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'outbox.db'}")
    Base.metadata.create_all(engine)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    now = datetime(2026, 9, 20, tzinfo=timezone.utc)
    with sessions.begin() as session:
        assert enqueue_payloads(session, [payload()], now=now) == 1
        assert enqueue_payloads(session, [payload()], now=now) == 0

    settings = GoldProfileSettings(
        profile_publisher_retry_seconds=10,
        kafka_bootstrap_servers="unused:9092",
    )
    producer = FakeProducer(fail=True)
    publisher = GoldProfilePublisher(settings, sessions, producer=producer)
    assert publisher.publish_pending(now=now) == (0, 1)
    with sessions() as session:
        row = session.query(GoldProfileDeliveryOutbox).one()
        assert row.status == "PENDING"
        assert row.attempt_count == 1
        assert "broker unavailable" in row.last_error

    producer.fail = False
    assert publisher.publish_pending(now=now + timedelta(seconds=10)) == (1, 0)
    assert publisher.publish_pending(now=now + timedelta(seconds=20)) == (0, 0)
    with sessions() as session:
        row = session.query(GoldProfileDeliveryOutbox).one()
        assert row.status == "PUBLISHED"
        assert row.attempt_count == 2
    assert len(producer.messages) == 2


def test_published_shadow_does_not_consume_new_active_run(tmp_path):
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'outbox.db'}")
    Base.metadata.create_all(engine)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    now = datetime(2026, 9, 20, tzinfo=timezone.utc)
    shadow = {
        **payload("shadow-run"),
        "profile_revision": 1,
        "delivery_mode": "SHADOW",
    }
    active = {
        **payload("active-run"),
        "profile_revision": 2,
        "delivery_mode": "ACTIVE",
    }
    with sessions.begin() as session:
        assert enqueue_payloads(session, [shadow], now=now) == 1
        row = session.query(GoldProfileDeliveryOutbox).one()
        row.status = "PUBLISHED"
        row.published_at = now

    with sessions.begin() as session:
        assert enqueue_payloads(session, [active], now=now) == 1
        assert enqueue_payloads(session, [active], now=now) == 0

    with sessions() as session:
        rows = session.query(GoldProfileDeliveryOutbox).order_by(
            GoldProfileDeliveryOutbox.profile_revision
        ).all()
        assert [(row.delivery_mode, row.status) for row in rows] == [
            ("SHADOW", "PUBLISHED"),
            ("ACTIVE", "PENDING"),
        ]


def test_publisher_reads_its_broker_from_settings(monkeypatch):
    """ACTIVE 전달은 설정에서 브로커 주소를 읽는다.

    producer를 주입하지 않는 운영 경로만 이 값을 읽으므로, 설정에 필드가 없으면
    테스트는 모두 통과한 채 실제 발행에서만 AttributeError로 터진다.
    """

    import confluent_kafka

    captured = {}

    def fake_producer(config):
        captured.update(config)
        return FakeProducer()

    monkeypatch.setattr(confluent_kafka, "Producer", fake_producer)

    settings = GoldProfileSettings(kafka_bootstrap_servers="broker:9092")
    GoldProfilePublisher(settings, sessionmaker())

    assert captured["bootstrap.servers"] == "broker:9092"
    # 중복 발행은 아웃박스가 막지 못한다. 브로커 쪽 멱등 전송으로 막는다.
    assert captured["enable.idempotence"] is True


def test_broker_comes_from_the_deployment_environment_variable(monkeypatch):
    # compose가 주는 이름. 바뀌면 배포는 기본값(localhost)으로 조용히 되돌아간다.
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")

    assert GoldProfileSettings().kafka_bootstrap_servers == "kafka:19092"
