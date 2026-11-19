import asyncio
import json
import logging
import signal
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Protocol

from aiokafka import AIOKafkaProducer
from sqlalchemy import select

from executor.config import Settings, get_settings
from executor.db.models import OutboxMessage
from executor.db.session import Database

logger = logging.getLogger(__name__)


class MessageSender(Protocol):
    async def send_and_wait(self, topic: str, value: bytes, *, key: bytes) -> object: ...


class Producer(MessageSender, Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...


async def publish_next(database: Database, sender: MessageSender, timeout: float) -> bool:
    """Keep the stored event eligible for retry until Kafka ack and DB commit."""
    async with asyncio.timeout(timeout):
        async with database.sessions() as session, session.begin():
            message = await session.scalar(
                select(OutboxMessage)
                .where(OutboxMessage.published_at.is_(None))
                .order_by(OutboxMessage.created_at, OutboxMessage.id)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            if message is None:
                return False
            await sender.send_and_wait(
                message.topic,
                value=json.dumps(
                    message.payload, ensure_ascii=False, separators=(",", ":")
                ).encode(),
                key=message.message_key.encode(),
            )
            message.published_at = datetime.now(UTC)
            event_id = message.id
    logger.info("Result delivery committed: event_id=%s", event_id)
    return True


async def pause(stop: asyncio.Event, delay: float) -> None:
    try:
        async with asyncio.timeout(delay):
            await stop.wait()
    except TimeoutError:
        pass


def make_producer(settings: Settings) -> Producer:
    producer: Producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        enable_idempotence=True,
        acks="all",
        request_timeout_ms=int(settings.publisher_send_timeout * 1000),
    )
    return producer


async def run_publisher(
    database: Database,
    settings: Settings,
    stop: asyncio.Event,
    *,
    producer_factory: Callable[[], Producer] | None = None,
) -> None:
    while not stop.is_set():
        producer = producer_factory() if producer_factory else make_producer(settings)
        try:
            async with asyncio.timeout(settings.publisher_send_timeout):
                await producer.start()
            while not stop.is_set():
                sent = await publish_next(database, producer, settings.publisher_send_timeout)
                if not sent:
                    await pause(stop, settings.publisher_poll_interval)
        except Exception as exc:
            logger.warning("Result publisher cycle failed: %s", type(exc).__name__)
        finally:
            # Fail fast on a stuck close so the supervisor can reclaim the old producer.
            async with asyncio.timeout(settings.publisher_shutdown_timeout):
                await producer.stop()
        if not stop.is_set():
            await pause(stop, settings.publisher_retry_interval)


async def main() -> None:
    settings = get_settings()
    database = Database(settings)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        await run_publisher(database, settings, stop)
    finally:
        await database.close()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
