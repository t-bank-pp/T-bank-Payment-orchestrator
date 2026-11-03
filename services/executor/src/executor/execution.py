import asyncio
import json
import logging
import signal
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol
from uuid import UUID, uuid4

import httpx
from contracts import ExecuteAttempt, decode_execute_attempt
from sqlalchemy import func, select, update

from executor.channel_a import ChannelA, ExecutionResult
from executor.config import Settings, get_settings
from executor.db.models import ExecutorAttempt
from executor.db.session import Database

logger = logging.getLogger(__name__)


class Channel(Protocol):
    async def execute(self, command: ExecuteAttempt) -> ExecutionResult: ...


@dataclass(frozen=True)
class Claim:
    attempt_id: UUID
    token: UUID
    payload: dict[str, Any]


async def claim_next(database: Database, lease_seconds: float) -> Claim | None:
    async with database.sessions() as session, session.begin():
        attempt = await session.scalar(
            select(ExecutorAttempt)
            .where(ExecutorAttempt.state == "queued")
            .order_by(ExecutorAttempt.created_at, ExecutorAttempt.attempt_id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if attempt is None:
            return None
        now: datetime = (await session.execute(select(func.clock_timestamp()))).scalar_one()
        token = uuid4()
        attempt.state = "in_progress"
        attempt.lease_token = token
        attempt.lease_expires_at = now + timedelta(seconds=lease_seconds)
        claim = Claim(attempt.attempt_id, token, attempt.payload)
    return claim


async def expire_leases(database: Database) -> None:
    async with database.sessions() as session, session.begin():
        await session.execute(
            update(ExecutorAttempt)
            .where(
                ExecutorAttempt.state == "in_progress",
                ExecutorAttempt.lease_expires_at <= func.clock_timestamp(),
            )
            .values(
                state="unknown",
                reason_code="EXECUTION_LEASE_EXPIRED",
                finished_at=func.clock_timestamp(),
                lease_token=None,
                lease_expires_at=None,
            )
        )


async def finish_claim(database: Database, claim: Claim, result: ExecutionResult) -> bool:
    async with database.sessions() as session, session.begin():
        saved = await session.scalar(
            update(ExecutorAttempt)
            .where(
                ExecutorAttempt.attempt_id == claim.attempt_id,
                ExecutorAttempt.state == "in_progress",
                ExecutorAttempt.lease_token == claim.token,
                ExecutorAttempt.lease_expires_at > func.clock_timestamp(),
            )
            .values(
                state=result.outcome,
                reason_code=result.reason_code,
                provider_operation_id=result.provider_operation_id,
                result_payload=result.payload,
                finished_at=func.clock_timestamp(),
                lease_token=None,
                lease_expires_at=None,
            )
            .returning(ExecutorAttempt.attempt_id)
        )
    return saved is not None


async def execute_next(database: Database, channel: Channel, settings: Settings) -> bool:
    async with asyncio.timeout(settings.execution_database_timeout):
        await expire_leases(database)
        claim = await claim_next(database, settings.execution_lease_seconds)
    if claim is None:
        return False
    try:
        command = decode_execute_attempt(json.dumps(claim.payload).encode())
        if command.data.channel_id != "A":
            result = ExecutionResult("unknown", "UNSUPPORTED_CHANNEL")
        else:
            async with asyncio.timeout(settings.execution_request_timeout):
                result = await channel.execute(command)
    except TimeoutError:
        result = ExecutionResult("unknown", "CHANNEL_TIMEOUT")
    except Exception as exc:
        logger.warning("Channel operation uncertain: %s", type(exc).__name__)
        result = ExecutionResult("unknown", "EXECUTION_ERROR")
    async with asyncio.timeout(settings.execution_database_timeout):
        saved = await finish_claim(database, claim, result)
        if not saved:
            await expire_leases(database)
    logger.info("Execution result recorded=%s: attempt_id=%s", saved, claim.attempt_id)
    return True


async def run_worker(
    database: Database, channel: Channel, settings: Settings, stop: asyncio.Event
) -> None:
    while not stop.is_set():
        try:
            worked = await execute_next(database, channel, settings)
        except Exception as exc:
            # A failed save leaves the durable lease. Never put it back in queued.
            logger.warning("Execution cycle failed: %s", type(exc).__name__)
            worked = False
        if not worked and not stop.is_set():
            try:
                async with asyncio.timeout(settings.execution_poll_interval):
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
        async with httpx.AsyncClient(
            base_url=str(settings.channel_a_base_url), follow_redirects=False, trust_env=False
        ) as client:
            await run_worker(
                database, ChannelA(client, settings.execution_request_timeout), settings, stop
            )
    finally:
        await database.close()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
