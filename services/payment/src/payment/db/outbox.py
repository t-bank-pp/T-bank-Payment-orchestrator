from uuid import UUID

from contracts import ATTEMPT_COMMANDS, ExecuteAttempt
from sqlalchemy.ext.asyncio import AsyncSession

from payment.db.models import OutboxMessage


async def add_execute_attempt(session: AsyncSession, command: ExecuteAttempt) -> OutboxMessage:
    # Flush only: the command must be committed atomically with the payment and the attempt.
    message = OutboxMessage(
        id=command.event_id,
        topic=ATTEMPT_COMMANDS,
        message_key=str(command.payment_id),
        payload=command.model_dump(mode="json"),
        published_at=None,
    )
    session.add(message)
    await session.flush()
    return message


async def get_outbox_message(session: AsyncSession, message_id: UUID) -> OutboxMessage | None:
    return await session.get(OutboxMessage, message_id)
