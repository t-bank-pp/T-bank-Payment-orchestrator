import asyncio
import json
import logging
import signal
from collections.abc import Mapping
from datetime import timedelta
from typing import Protocol

from contracts import ExecuteAttempt, decode_execute_attempt
from sqlalchemy import func, or_, select

from executor.channel_a import ExecutionResult
from executor.channels import channel_clients, select_channel
from executor.config import Settings, get_settings
from executor.confirmation import apply_confirmation
from executor.db.models import ExecutorAttempt
from executor.db.session import Database

logger = logging.getLogger(__name__)


class StatusReader(Protocol):
    async def status(self, command: ExecuteAttempt) -> ExecutionResult: ...


async def claim_reconciliation(database: Database, interval: float) -> ExecuteAttempt | None:
    async with database.sessions() as session, session.begin():
        attempt = await session.scalar(
            select(ExecutorAttempt)
            .where(
                ExecutorAttempt.state == "unknown",
                or_(
                    ExecutorAttempt.next_reconcile_at.is_(None),
                    ExecutorAttempt.next_reconcile_at <= func.clock_timestamp(),
                ),
            )
            .order_by(
                ExecutorAttempt.next_reconcile_at.asc().nullsfirst(), ExecutorAttempt.attempt_id
            )
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if attempt is None:
            return None
        now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
        attempt.next_reconcile_at = now + timedelta(seconds=interval)
        command = decode_execute_attempt(json.dumps(attempt.payload).encode())
    return command


async def reconcile_next(
    database: Database, channel: StatusReader | Mapping[str, StatusReader], settings: Settings
) -> bool:
    async with asyncio.timeout(settings.execution_database_timeout):
        command = await claim_reconciliation(database, settings.reconciliation_interval)
    if command is None:
        return False
    selected = select_channel(channel, command.data.channel_id)
    if selected is None:
        return True
    async with asyncio.timeout(settings.reconciliation_request_timeout):
        result = await selected.status(command)
    async with asyncio.timeout(settings.execution_database_timeout):
        async with database.sessions() as session, session.begin():
            await apply_confirmation(
                session,
                attempt_id=command.attempt_id,
                payment_id=command.payment_id,
                channel_id=command.data.channel_id,
                result=result,
            )
    return True


async def run_reconciliation(
    database: Database,
    channel: StatusReader | Mapping[str, StatusReader],
    settings: Settings,
    stop: asyncio.Event,
) -> None:
    while not stop.is_set():
        try:
            worked = await reconcile_next(database, channel, settings)
        except Exception as exc:
            logger.warning("Reconciliation cycle failed: %s", type(exc).__name__)
            worked = False
        if not worked:
            try:
                async with asyncio.timeout(settings.reconciliation_poll_interval):
                    await stop.wait()
            except TimeoutError:
                pass


async def main() -> None:
    settings = get_settings()
    database = Database(settings)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        async with channel_clients(settings, settings.reconciliation_request_timeout) as channels:
            await run_reconciliation(database, channels, settings, stop)
    finally:
        await database.close()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
