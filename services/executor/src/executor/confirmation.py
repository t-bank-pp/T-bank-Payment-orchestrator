from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from executor.channel_a import ExecutionResult
from executor.db.models import ExecutorAttempt
from executor.db.outbox import add_attempt_result


class ConfirmationConflict(Exception):
    """The confirmation does not match the recorded operation or state."""


async def apply_confirmation(
    session: AsyncSession,
    *,
    attempt_id: UUID,
    payment_id: UUID,
    channel_id: str,
    result: ExecutionResult,
) -> bool:
    """Apply a trusted channel confirmation and outbox in the caller's transaction."""
    if result.outcome not in {"succeeded", "failed"}:
        return False
    if not result.provider_operation_id:
        raise ConfirmationConflict("PROVIDER_OPERATION_REQUIRED")
    attempt = await session.scalar(
        select(ExecutorAttempt).where(ExecutorAttempt.attempt_id == attempt_id).with_for_update()
    )
    if attempt is None:
        raise ConfirmationConflict("ATTEMPT_NOT_FOUND")
    if attempt.payment_id != payment_id or attempt.payload["data"]["channel_id"] != channel_id:
        raise ConfirmationConflict("ATTEMPT_MISMATCH")
    if attempt.provider_operation_id not in {None, result.provider_operation_id}:
        raise ConfirmationConflict("PROVIDER_OPERATION_MISMATCH")
    if attempt.state in {"succeeded", "failed"}:
        if attempt.state != result.outcome:
            raise ConfirmationConflict("CONFIRMED_OUTCOME_CONFLICT")
        return False
    if attempt.state not in {"unknown", "in_progress"}:
        raise ConfirmationConflict("ATTEMPT_NOT_STARTED")
    attempt.state_version = max(1, attempt.state_version) + 1 if attempt.state == "unknown" else 1
    attempt.state = result.outcome
    attempt.reason_code = result.reason_code
    attempt.provider_operation_id = result.provider_operation_id
    attempt.result_payload = result.payload
    attempt.finished_at = await session.scalar(select(func.clock_timestamp()))
    attempt.lease_token = None
    attempt.lease_expires_at = None
    add_attempt_result(session, attempt)
    await session.flush()
    return True
