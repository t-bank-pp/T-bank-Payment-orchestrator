from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import BigInteger, CheckConstraint, DateTime, ForeignKey, LargeBinary, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class ExecutorAttempt(Base):
    __tablename__ = "executor_attempts"
    __table_args__ = (
        CheckConstraint(
            "state IN ('queued','in_progress','succeeded','failed','unknown')",
            name="ck_executor_attempts_state",
        ),
        CheckConstraint(
            "(state = 'in_progress' AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL) "
            "OR (state <> 'in_progress' AND lease_token IS NULL AND lease_expires_at IS NULL)",
            name="ck_executor_attempts_lease",
        ),
    )

    attempt_id: Mapped[UUID] = mapped_column(primary_key=True)
    # Payment belongs to another service: deliberately no cross-database foreign key.
    payment_id: Mapped[UUID] = mapped_column(index=True)
    command_hash: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    state: Mapped[str] = mapped_column(String(32), server_default="queued")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    lease_token: Mapped[UUID | None] = mapped_column()
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    reason_code: Mapped[str | None] = mapped_column(String(100))
    provider_operation_id: Mapped[str | None] = mapped_column(String(255))
    result_payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB)


class InboxEvent(Base):
    __tablename__ = "inbox"

    event_id: Mapped[UUID] = mapped_column(primary_key=True)
    attempt_id: Mapped[UUID] = mapped_column(
        ForeignKey("executor_attempts.attempt_id", ondelete="RESTRICT"), index=True
    )
    payload_hash: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class RejectedCommand(Base):
    __tablename__ = "executor_rejections"
    __table_args__ = (
        CheckConstraint("partition >= 0 AND kafka_offset >= 0", name="ck_rejection_position"),
        CheckConstraint(
            "reason IN ('INVALID_COMMAND', 'KEY_MISMATCH', 'COMMAND_CONFLICT')",
            name="ck_rejection_reason",
        ),
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
