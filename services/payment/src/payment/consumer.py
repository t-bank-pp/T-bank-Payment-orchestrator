import asyncio
import logging
import signal
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from aiokafka import AIOKafkaConsumer, TopicPartition
from contracts import ATTEMPT_EVENTS, decode_attempt_result_event
from pydantic import ValidationError
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import DBAPIError

from payment.config import Settings, get_settings
from payment.db.models import RejectedResult
from payment.db.session import Database
from payment.use_cases.apply_attempt_result import ResultConflict, apply_attempt_result

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Record:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None


class Consumer(Protocol):
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def getone(self) -> Record: ...
    async def commit(self, offsets: dict[TopicPartition, int]) -> None: ...


class SourceRecordConflict(Exception):
    """A reused Kafka position no longer contains the previously rejected bytes."""


async def reject(database: Database, record: Record, reason: str) -> None:
    async with database.sessions() as session, session.begin():
        await session.execute(
            insert(RejectedResult)
            .values(
                topic=record.topic,
                partition=record.partition,
                kafka_offset=record.offset,
                reason=reason,
                raw_key=record.key,
                raw_value=record.value,
            )
            .on_conflict_do_nothing()
        )
        saved = await session.get(RejectedResult, (record.topic, record.partition, record.offset))
        if saved is None or saved.raw_key != record.key or saved.raw_value != record.value:
            raise SourceRecordConflict("Kafka position reused with different bytes")


async def store_record(database: Database, record: Record) -> str:
    if record.value is None:
        await reject(database, record, "INVALID_RESULT")
        return "rejected"
    try:
        result = decode_attempt_result_event(record.value)
    except ValidationError:
        await reject(database, record, "INVALID_RESULT")
        return "rejected"
    if record.key != str(result.payment_id).encode():
        await reject(database, record, "KEY_MISMATCH")
        return "rejected"
    try:
        async with database.sessions() as session, session.begin():
            created = await apply_attempt_result(session, result)
    except ResultConflict as exc:
        # The attempted application has been fully rolled back before saving rejection.
        await reject(database, record, str(exc))
        return "rejected"
    except DBAPIError as exc:
        # Valid JSON can still be unrepresentable in JSONB (NUL or numeric overflow).
        # Only permanent data errors are quarantined; connection/transaction errors retry.
        if getattr(exc.orig, "sqlstate", None) not in {"22P05", "22003", "22P02", "22021"}:
            raise
        await reject(database, record, "INVALID_RESULT")
        return "rejected"
    return "accepted" if created else "duplicate"


async def process_record(
    database: Database, consumer: Consumer, record: Record, settings: Settings
) -> None:
    async with asyncio.timeout(settings.consumer_processing_timeout):
        result = await store_record(database, record)
    # Commit exactly this record's next offset, never positions of other fetched records.
    async with asyncio.timeout(settings.consumer_request_timeout):
        await consumer.commit({TopicPartition(record.topic, record.partition): record.offset + 1})
    logger.info(
        "Result %s and offset committed: topic=%s partition=%s offset=%s",
        result,
        record.topic,
        record.partition,
        record.offset,
    )


def make_consumer(settings: Settings) -> Consumer:
    consumer: Consumer = AIOKafkaConsumer(
        ATTEMPT_EVENTS,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        isolation_level="read_committed",
        request_timeout_ms=int(settings.consumer_request_timeout * 1000),
    )
    return consumer


async def run_consumer(
    database: Database,
    settings: Settings,
    stop: asyncio.Event,
    *,
    consumer_factory: Callable[[], Consumer] | None = None,
) -> None:
    while not stop.is_set():
        consumer = consumer_factory() if consumer_factory else make_consumer(settings)
        try:
            async with asyncio.timeout(settings.consumer_request_timeout):
                await consumer.start()
            while not stop.is_set():
                try:
                    async with asyncio.timeout(settings.consumer_poll_interval):
                        record = await consumer.getone()
                except TimeoutError:
                    continue
                await process_record(database, consumer, record, settings)
        except Exception as exc:
            logger.warning("Consumer cycle failed: %s", type(exc).__name__)
        finally:
            # A stuck close is fatal; the supervisor restarts instead of leaking clients.
            async with asyncio.timeout(settings.consumer_shutdown_timeout):
                await consumer.stop()
        # Reconnect from committed offsets after every processing/commit failure.
        if not stop.is_set():
            try:
                async with asyncio.timeout(settings.consumer_retry_interval):
                    await stop.wait()
            except TimeoutError:
                pass


async def main() -> None:
    settings = get_settings()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    database = Database(settings)
    try:
        await run_consumer(database, settings, stop)
    finally:
        await database.close()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
