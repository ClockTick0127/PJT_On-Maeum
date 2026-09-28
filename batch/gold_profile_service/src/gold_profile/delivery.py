"""Durable household-profile outbox and retrying Kafka publisher."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import logging
import threading
from uuid import UUID, uuid5

from sqlalchemy import (
    BigInteger, CheckConstraint, DateTime, Index, Integer, JSON, String, Text,
    UniqueConstraint, select,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, Session, mapped_column

from realtime_analysis.database import Base


logger = logging.getLogger(__name__)

OUTBOX_NAMESPACE = UUID("db9ce0b6-276e-44db-b5e1-1df643d775a4")


class GoldProfileDeliveryOutbox(Base):
    __tablename__ = "gold_profile_delivery_outbox"
    __table_args__ = (
        UniqueConstraint(
            "household_id", "profile_version", "delivery_mode",
            name="uq_gold_profile_delivery_identity",
        ),
        CheckConstraint("profile_revision >= 1", name="profile_revision_positive"),
        CheckConstraint(
            "delivery_mode IN ('ACTIVE', 'SHADOW')", name="delivery_mode"
        ),
        CheckConstraint(
            "status IN ('PENDING', 'PUBLISHED')", name="status"
        ),
        Index("ix_gold_profile_delivery_pending", "status", "next_attempt_at"),
    )

    event_id: Mapped[UUID] = mapped_column(primary_key=True)
    household_id: Mapped[str] = mapped_column(String(50), nullable=False)
    profile_version: Mapped[str] = mapped_column(String(100), nullable=False)
    profile_revision: Mapped[int] = mapped_column(BigInteger, nullable=False)
    delivery_mode: Mapped[str] = mapped_column(String(20), nullable=False)
    payload: Mapped[dict] = mapped_column(
        JSON().with_variant(JSONB, "postgresql"), nullable=False
    )
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="PENDING"
    )
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


def event_id_for(payload: dict) -> UUID:
    # A delivery-mode change normally has a different profile_version because
    # it is a new Gold run.  Keeping the mode in the event identity also makes
    # retries idempotent without confusing an old PUBLISHED SHADOW row with the
    # new ACTIVE request.
    return uuid5(
        OUTBOX_NAMESPACE,
        f"{payload['household_id']}:{payload['profile_version']}:{payload['delivery_mode']}",
    )


def enqueue_payloads(session: Session, payloads: list[dict], *, now=None) -> int:
    """Insert missing events in the caller's activation transaction."""

    now = now or datetime.now(timezone.utc)
    inserted = 0
    for payload in payloads:
        event_id = event_id_for(payload)
        if session.get(GoldProfileDeliveryOutbox, event_id) is not None:
            continue
        session.add(GoldProfileDeliveryOutbox(
            event_id=event_id,
            household_id=payload["household_id"],
            profile_version=payload["profile_version"],
            profile_revision=int(payload["profile_revision"]),
            delivery_mode=payload["delivery_mode"],
            payload=payload,
            status="PENDING",
            attempt_count=0,
            next_attempt_at=now,
            created_at=now,
        ))
        inserted += 1
    return inserted


class GoldProfilePublisher:
    """Publish pending rows; a crash may duplicate a send, never lose one."""

    def __init__(self, settings, session_factory, producer=None):
        if producer is None:
            from confluent_kafka import Producer
            producer = Producer({
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
            })
        self._producer = producer
        self._sessions = session_factory
        self._topic = settings.profile_kafka_topic
        self._batch_size = settings.profile_publisher_batch_size
        self._retry = timedelta(seconds=settings.profile_publisher_retry_seconds)

    def publish_pending(self, *, now=None) -> tuple[int, int]:
        now = now or datetime.now(timezone.utc)
        with self._sessions() as session:
            ids = list(session.scalars(
                select(GoldProfileDeliveryOutbox.event_id)
                .where(
                    GoldProfileDeliveryOutbox.status == "PENDING",
                    GoldProfileDeliveryOutbox.next_attempt_at <= now,
                )
                .order_by(GoldProfileDeliveryOutbox.created_at)
                .limit(self._batch_size)
            ))
        published = failed = 0
        for event_id in ids:
            try:
                self._publish_one(event_id, now)
                published += 1
            except Exception as error:  # next invocation retries the durable row
                failed += 1
                with self._sessions.begin() as session:
                    row = session.get(GoldProfileDeliveryOutbox, event_id)
                    if row is not None and row.status == "PENDING":
                        row.attempt_count += 1
                        row.next_attempt_at = now + self._retry
                        row.last_error = f"{type(error).__name__}: {error}"[:2000]
        return published, failed

    def _publish_one(self, event_id: UUID, now: datetime) -> None:
        with self._sessions() as session:
            row = session.get(GoldProfileDeliveryOutbox, event_id)
            if row is None or row.status != "PENDING":
                return
            household_id = row.household_id
            payload = row.payload
        errors = []
        self._producer.produce(
            topic=self._topic,
            key=household_id.encode(),
            value=json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(),
            callback=lambda error, _message: errors.append(error) if error else None,
        )
        remaining = self._producer.flush(30)
        if remaining or errors:
            raise RuntimeError(errors[0] if errors else f"{remaining} message(s) undelivered")
        with self._sessions.begin() as session:
            row = session.get(GoldProfileDeliveryOutbox, event_id)
            if row is not None and row.status == "PENDING":
                row.status = "PUBLISHED"
                row.attempt_count += 1
                row.published_at = now
                row.last_error = None



def drain_outbox(publisher, interval_seconds: float, stop: threading.Event) -> int:
    """아웃박스가 빌 때까지 계속 쓸어 담는다. 상주 발행자의 본체다.

    한 번 실행으로 끝나는 명령만 있으면 브로커가 잠깐 죽었을 때 생긴 실패 행은
    누가 다시 부르기 전까지 그대로 남는다. 행에 적힌 ``next_attempt_at``은
    다시 부르는 사람이 있어야 뜻이 있다.

    쓸어 담기 자체가 실패해도 루프는 죽지 않는다. DB가 잠깐 끊긴 것과 프로세스가
    끝나는 것은 다른 일이고, 아웃박스 행은 어차피 남아 있다.

    :param stop: 세워진 순간 다음 주기를 기다리지 않고 빠져나온다
    :return: 프로세스 종료 코드
    """

    while not stop.is_set():
        try:
            published, failed = publisher.publish_pending()
            if published or failed:
                logger.info(
                    "gold profile outbox published=%s failed=%s", published, failed
                )
        except Exception:
            logger.exception("아웃박스를 쓸어 담지 못했다. 다음 주기에 다시 한다")
        stop.wait(interval_seconds)
    return 0
