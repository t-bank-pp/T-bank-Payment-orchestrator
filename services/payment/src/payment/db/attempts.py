from typing import Any
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from payment.db.models import PaymentAttempt


async def add_attempt(
    session: AsyncSession,
    *,
    payment_id: UUID,
    number: int,
    channel_id: str,
    decision: dict[str, Any],
    attempt_id: UUID | None = None,
) -> PaymentAttempt:
    # Flush only: the caller owns the transaction and commits it together with other writes.
    attempt = PaymentAttempt(
        attempt_id=uuid4() if attempt_id is None else attempt_id,
        payment_id=payment_id,
        number=number,
        channel_id=channel_id,
        decision=decision,
    )
    session.add(attempt)
    await session.flush()
    return attempt


async def get_attempt(session: AsyncSession, attempt_id: UUID) -> PaymentAttempt | None:
    return await session.get(PaymentAttempt, attempt_id)
