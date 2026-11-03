"""Durable acceptance only; channel execution is a separate stage."""

import hashlib
import json
from typing import Any

from contracts import ExecuteAttempt
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from executor.db.models import ExecutorAttempt, InboxEvent


class CommandConflict(Exception):
    """An existing event or attempt ID was reused with incompatible content."""


def fingerprint(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


async def register_attempt(session: AsyncSession, command: ExecuteAttempt) -> bool:
    """Return True for a new attempt. Caller must roll back on any exception.

    Commit this transaction before acknowledging a future Kafka delivery. An accepted
    inbox entry means durable receipt, not a completed channel operation.
    """
    payload = command.model_dump(mode="json")
    # A new event may deliver the same business command, but may not change it.
    command_hash = fingerprint(
        {key: value for key, value in payload.items() if key not in {"event_id", "occurred_at"}}
    )
    created = await session.scalar(
        insert(ExecutorAttempt)
        .values(
            attempt_id=command.attempt_id,
            payment_id=command.payment_id,
            command_hash=command_hash,
            payload=payload,
        )
        .on_conflict_do_nothing(index_elements=[ExecutorAttempt.attempt_id])
        .returning(ExecutorAttempt.attempt_id)
    )
    existing = await session.get(ExecutorAttempt, command.attempt_id)
    if existing is None or existing.command_hash != command_hash:
        raise CommandConflict("Attempt ID reused with a different command")

    payload_hash = fingerprint(payload)
    await session.execute(
        insert(InboxEvent)
        .values(
            event_id=command.event_id,
            attempt_id=command.attempt_id,
            payload_hash=payload_hash,
            payload=payload,
        )
        .on_conflict_do_nothing(index_elements=[InboxEvent.event_id])
    )
    event = await session.get(InboxEvent, command.event_id)
    if (
        event is None
        or event.attempt_id != command.attempt_id
        or event.payload_hash != payload_hash
    ):
        raise CommandConflict("Event ID reused with a different envelope")
    return created is not None
