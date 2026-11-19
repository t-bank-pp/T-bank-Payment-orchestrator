from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from payment.db.base import Base


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("amount_minor > 0", name="positive_amount"),
        CheckConstraint("version >= 0", name="nonnegative_version"),
        CheckConstraint(
            "status IN ('routing', 'in_progress', 'pending', "
            "'succeeded', 'failed', 'needs_review')",
            name="valid_status",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        primary_key=True, default=uuid4, server_default=text("gen_random_uuid()")
    )
    idempotency_key: Mapped[str] = mapped_column(String(100), unique=True)
    request_hash: Mapped[str] = mapped_column(String(64))
    amount_minor: Mapped[int] = mapped_column(BigInteger)
    currency: Mapped[str] = mapped_column(String(3))
    recipient: Mapped[str] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(32), server_default="in_progress")
    version: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class PaymentAttempt(Base):
    __tablename__ = "payment_attempts"
    __table_args__ = (
        UniqueConstraint("payment_id", "number"),
        CheckConstraint("number > 0", name="positive_number"),
        CheckConstraint("result_version >= 0", name="nonnegative_result_version"),
        CheckConstraint(
            "outcome IS NULL OR outcome IN ('succeeded', 'failed', 'unknown')", name="valid_outcome"
        ),
    )

    attempt_id: Mapped[UUID] = mapped_column(
        primary_key=True, default=uuid4, server_default=text("gen_random_uuid()")
    )
    payment_id: Mapped[UUID] = mapped_column(ForeignKey("payments.id", ondelete="RESTRICT"))
    number: Mapped[int] = mapped_column(Integer)
    channel_id: Mapped[str] = mapped_column(String(32))
    outcome: Mapped[str | None] = mapped_column(String(16))
    result_version: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    provider_operation_id: Mapped[str | None] = mapped_column(String(255))
    reason_code: Mapped[str | None] = mapped_column(String(100))
    decision: Mapped[dict[str, Any]] = mapped_column(JSONB, server_default=text("'{}'::jsonb"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class OutboxMessage(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        Index(
            "ix_outbox_unpublished_created_at",
            "created_at",
            postgresql_where=text("published_at IS NULL"),
        ),
    )

    # id is also the envelope's event_id; it is reused when publishing is retried.
    id: Mapped[UUID] = mapped_column(
        primary_key=True, default=uuid4, server_default=text("gen_random_uuid()")
    )
    topic: Mapped[str] = mapped_column(String(255))
    message_key: Mapped[str] = mapped_column(Text)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class InboxEvent(Base):
    __tablename__ = "inbox"
    __table_args__ = (UniqueConstraint("attempt_id", "state_version"),)

    event_id: Mapped[UUID] = mapped_column(primary_key=True)
    attempt_id: Mapped[UUID] = mapped_column(
        ForeignKey("payment_attempts.attempt_id", ondelete="RESTRICT")
    )
    state_version: Mapped[int] = mapped_column(Integer, server_default=text("1"))
    payload_hash: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class RejectedResult(Base):
    __tablename__ = "payment_rejections"
    __table_args__ = (
        CheckConstraint("partition >= 0 AND kafka_offset >= 0", name="valid_position"),
    )

    topic: Mapped[str] = mapped_column(String(255), primary_key=True)
    partition: Mapped[int] = mapped_column(primary_key=True)
    kafka_offset: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    reason: Mapped[str] = mapped_column(String(32))
    raw_key: Mapped[bytes | None] = mapped_column(LargeBinary)
    raw_value: Mapped[bytes | None] = mapped_column(LargeBinary)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
