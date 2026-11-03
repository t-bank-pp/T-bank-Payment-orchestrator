from typing import Final
from uuid import uuid4

from contracts import ExecuteAttempt, ExecuteAttemptData
from sqlalchemy.ext.asyncio import AsyncSession

from payment.db.attempts import add_attempt
from payment.db.models import Payment, PaymentAttempt
from payment.db.outbox import add_execute_attempt

FIXED_CHANNEL_ID: Final = "A"
FIRST_ATTEMPT_NUMBER: Final = 1


async def create_first_attempt(session: AsyncSession, payment: Payment) -> PaymentAttempt:
    if payment.id is None:
        await session.flush()

    attempt = await add_attempt(
        session,
        payment_id=payment.id,
        number=FIRST_ATTEMPT_NUMBER,
        channel_id=FIXED_CHANNEL_ID,
        decision={"strategy": "fixed", "selected_channel": FIXED_CHANNEL_ID},
        attempt_id=uuid4(),
    )
    command = ExecuteAttempt.new(
        payment_id=payment.id,
        attempt_id=attempt.attempt_id,
        data=ExecuteAttemptData.model_validate(
            {
                "channel_id": FIXED_CHANNEL_ID,
                "amount_minor": payment.amount_minor,
                "currency": payment.currency,
                "recipient": payment.recipient,
            }
        ),
    )
    await add_execute_attempt(session, command)
    return attempt
