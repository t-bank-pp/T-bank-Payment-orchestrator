import asyncio
import json
import logging
from datetime import UTC, datetime
from typing import Protocol

from sqlalchemy import ScalarResult, select

from payment.db.models import OutboxMessage
from payment.db.session import Database

logger = logging.getLogger(__name__)


class MessageSender(Protocol):
    async def send_and_wait(self, topic: str, value: bytes, *, key: bytes) -> object: ...


async def publish_next(database: Database, sender: MessageSender, timeout: float) -> bool:
    """Publish one locked row; a failed send or commit leaves it eligible for retry."""
    async with database.sessions() as session, session.begin():
        result: ScalarResult[OutboxMessage] = await session.scalars(
            select(OutboxMessage)
            .where(OutboxMessage.published_at.is_(None))
            .order_by(OutboxMessage.created_at, OutboxMessage.id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        message = result.one_or_none()
        if message is None:
            return False

        # Serialize the stored envelope, never create a new ExecuteAttempt or IDs here.
        value = json.dumps(message.payload, ensure_ascii=False, separators=(",", ":")).encode()
        try:
            async with asyncio.timeout(timeout):
                await sender.send_and_wait(
                    message.topic, value=value, key=message.message_key.encode()
                )
        except Exception:
            logger.warning("Outbox delivery not confirmed: event_id=%s", message.id)
            raise
        message.published_at = datetime.now(UTC)
        message_id = message.id

    logger.info("Outbox delivery committed: event_id=%s", message_id)
    return True
