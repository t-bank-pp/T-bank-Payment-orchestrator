import asyncio
import logging
import signal
from collections.abc import Callable
from typing import Protocol

from aiokafka import AIOKafkaProducer

from payment.config import Settings, get_settings
from payment.db.session import Database
from payment.publisher import MessageSender, publish_next

logger = logging.getLogger(__name__)


class Producer(MessageSender, Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...


async def pause_until_retry(stop: asyncio.Event, delay: float) -> None:
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
                    await pause_until_retry(stop, settings.publisher_poll_interval)
        except Exception as exc:
            logger.warning("Publisher cycle failed: %s", type(exc).__name__)
        finally:
            # A timed-out stop is fatal: do not leak an old producer and create new ones.
            # The supervisor restarts the process; uncommitted rows remain in outbox.
            async with asyncio.timeout(settings.publisher_shutdown_timeout):
                await producer.stop()
        if not stop.is_set():
            await pause_until_retry(stop, settings.publisher_retry_interval)


async def main() -> None:
    settings = get_settings()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    database = Database(settings)
    try:
        await run_publisher(database, settings, stop)
    finally:
        await database.close()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
