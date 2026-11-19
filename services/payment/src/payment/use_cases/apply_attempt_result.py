"""Apply versioned attempt outcomes in the caller's transaction."""

import hashlib
import json

from contracts import ATTEMPT_COMMANDS, AttemptResultEvent, AttemptResultUpdate
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from payment.db.models import InboxEvent, OutboxMessage, Payment, PaymentAttempt


class ResultConflict(Exception):
    """A valid envelope cannot be applied to the stored payment/attempt."""


async def apply_attempt_result(session: AsyncSession, result: AttemptResultEvent) -> bool:
    """Return True when applied, False for an identical event; roll back on errors.

    Lock payment before attempt so concurrent results cannot overwrite each other.
    Inbox insertion and state changes must commit before acknowledging Kafka.
    """
    payment = await session.scalar(
        select(Payment).where(Payment.id == result.payment_id).with_for_update()
    )
    if payment is None:
        raise ResultConflict("PAYMENT_NOT_FOUND")
    attempt = await session.scalar(
        select(PaymentAttempt)
        .where(
            PaymentAttempt.attempt_id == result.attempt_id,
            PaymentAttempt.payment_id == result.payment_id,
        )
        .with_for_update()
    )
    if attempt is None:
        raise ResultConflict("ATTEMPT_MISMATCH")
    if attempt.channel_id != result.data.channel_id:
        raise ResultConflict("CHANNEL_MISMATCH")

    command = await session.scalar(
        select(OutboxMessage.payload).where(
            OutboxMessage.topic == ATTEMPT_COMMANDS,
            OutboxMessage.message_key == str(result.payment_id),
            OutboxMessage.payload["attempt_id"].astext == str(result.attempt_id),
        )
    )
    if command is None or command.get("correlation_id") != str(result.correlation_id):
        raise ResultConflict("CORRELATION_MISMATCH")
    version = result.state_version if isinstance(result, AttemptResultUpdate) else 1
    if version > 1 and (
        result.data.outcome not in {"succeeded", "failed"} or not result.data.provider_operation_id
    ):
        raise ResultConflict("INVALID_REFINEMENT")

    payload = result.model_dump(mode="json")
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    payload_hash = hashlib.sha256(canonical.encode()).hexdigest()
    inserted = await session.scalar(
        insert(InboxEvent)
        .values(
            event_id=result.event_id,
            attempt_id=result.attempt_id,
            state_version=version,
            payload=payload,
            payload_hash=payload_hash,
        )
        .on_conflict_do_nothing()
        .returning(InboxEvent.event_id)
    )
    if inserted is None:
        previous = await session.get(InboxEvent, result.event_id)
        if previous is not None and previous.payload_hash == payload_hash:
            return False
        raise ResultConflict("RESULT_CONFLICT")

    if version < attempt.result_version:
        return False
    if version == attempt.result_version:
        if (
            attempt.outcome != result.data.outcome
            or attempt.reason_code != result.data.reason_code
            or attempt.finished_at != result.occurred_at
            or attempt.provider_operation_id not in {None, result.data.provider_operation_id}
        ):
            raise ResultConflict("RESULT_CONFLICT")
        return False

    latest_number = await session.scalar(
        select(func.max(PaymentAttempt.number)).where(
            PaymentAttempt.payment_id == result.payment_id
        )
    )
    initial = attempt.outcome is None and payment.status in {"in_progress", "pending"}
    refinement = attempt.outcome == "unknown" and payment.status == "needs_review"
    if attempt.number != latest_number or not (initial or refinement):
        raise ResultConflict("STATE_CONFLICT")
    if attempt.provider_operation_id not in {None, result.data.provider_operation_id}:
        raise ResultConflict("PROVIDER_OPERATION_MISMATCH")

    attempt.outcome = result.data.outcome
    attempt.reason_code = result.data.reason_code
    attempt.result_version = version
    attempt.provider_operation_id = result.data.provider_operation_id
    attempt.finished_at = result.occurred_at
    payment.status = "needs_review" if result.data.outcome == "unknown" else result.data.outcome
    payment.version += 1
    # provider_operation_id and the full envelope remain available in the inbox.
    await session.flush()
    return True
