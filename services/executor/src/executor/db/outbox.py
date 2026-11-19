import json
from typing import cast

from contracts import (
    ATTEMPT_EVENTS,
    AttemptOutcome,
    AttemptResult,
    AttemptResultEvent,
    AttemptResultUpdate,
    decode_execute_attempt,
)
from sqlalchemy.ext.asyncio import AsyncSession

from executor.db.models import ExecutorAttempt, OutboxMessage


def add_attempt_result(session: AsyncSession, attempt: ExecutorAttempt) -> None:
    """Called with the attempt locked, in the transaction that records its outcome."""
    if attempt.finished_at is None or attempt.reason_code is None:
        raise ValueError("A completed attempt must have a finish time and reason")
    command = decode_execute_attempt(json.dumps(attempt.payload).encode())
    attempt.state_version = max(1, attempt.state_version)
    initial = AttemptResult.new(
        command,
        occurred_at=attempt.finished_at,
        outcome=cast(AttemptOutcome, attempt.state),
        reason_code=attempt.reason_code,
        provider_operation_id=attempt.provider_operation_id,
    )
    result: AttemptResultEvent = initial
    if attempt.state_version > 1:
        result = AttemptResultUpdate.from_result(initial, attempt.state_version)
    session.add(
        OutboxMessage(
            id=result.event_id,
            attempt_id=attempt.attempt_id,
            state_version=attempt.state_version,
            topic=ATTEMPT_EVENTS,
            message_key=str(result.payment_id),
            payload=result.model_dump(mode="json"),
        )
    )
