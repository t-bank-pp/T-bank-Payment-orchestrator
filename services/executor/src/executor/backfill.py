import asyncio
import logging

from sqlalchemy import select

from executor.config import get_settings
from executor.db.models import ExecutorAttempt, OutboxMessage
from executor.db.outbox import add_attempt_result
from executor.db.session import Database


async def backfill_next_result(database: Database) -> bool:
    """Queue an old completed attempt's result without executing the channel again."""
    async with database.sessions() as session, session.begin():
        attempt = await session.scalar(
            select(ExecutorAttempt)
            .where(
                ExecutorAttempt.state.in_(("succeeded", "failed", "unknown")),
                ~select(OutboxMessage.id)
                .where(OutboxMessage.attempt_id == ExecutorAttempt.attempt_id)
                .exists(),
            )
            .order_by(ExecutorAttempt.created_at, ExecutorAttempt.attempt_id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if attempt is None:
            return False
        add_attempt_result(session, attempt)
    return True


async def main() -> None:
    settings = get_settings()
    database = Database(settings)
    count = 0
    try:
        while True:
            async with asyncio.timeout(settings.execution_database_timeout):
                saved = await backfill_next_result(database)
            if not saved:
                break
            count += 1
        logging.info("Backfilled attempt results: %s", count)
    finally:
        await database.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
