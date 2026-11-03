import asyncio
import os
from uuid import uuid4

import httpx
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from contracts import ATTEMPT_COMMANDS, encode
from executor.consumer import process_record
from executor.db.models import ExecutorAttempt, InboxEvent, RejectedCommand
from payment.config import Settings as PaymentSettings
from payment.db.models import OutboxMessage
from payment.main import create_app
from payment.publisher import publish_next
from sqlalchemy import select

from .test_consumer import settings
from .test_registration import command, snapshot

pytestmark = [pytest.mark.integration, pytest.mark.kafka]


def bootstrap():
    value = os.getenv("TEST_KAFKA_BOOTSTRAP_SERVERS")
    if not value:
        pytest.skip("Set TEST_KAFKA_BOOTSTRAP_SERVERS")
    return value


async def client(group):
    consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap(),
        group_id=group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        isolation_level="read_committed",
    )
    consumer.assign([TopicPartition(ATTEMPT_COMMANDS, p) for p in range(3)])
    await consumer.start()
    return consumer


async def begin_at_end(consumer):
    await consumer.seek_to_end()
    await consumer.commit({p: await consumer.position(p) for p in consumer.assignment()})


async def next_for_key(consumer, key):
    async with asyncio.timeout(15):
        while True:
            record = await consumer.getone()
            if record.key == key:
                return record


@pytest.mark.parametrize("lost_commit", [False, True])
async def test_post_outbox_kafka_to_executor_with_restart(
    executor_database, consumer_payment_database, lost_commit
):
    group = "tbpo16-" + uuid4().hex
    consumer = await client(group)
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap(), enable_idempotence=True)
    db, payment = executor_database, consumer_payment_database
    try:
        await begin_at_end(consumer)
        await producer.start()
        app = create_app(
            PaymentSettings(
                database_url=payment.engine.url.render_as_string(hide_password=False),
                _env_file=None,
            )
        )
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://payment"
            ) as api:
                response = await api.post(
                    "/payments",
                    headers={"Idempotency-Key": uuid4().hex},
                    json={"amount_minor": 125000, "currency": "RUB", "recipient": "consumer-demo"},
                )
        assert response.status_code == 202
        payment_id = response.json()["payment_id"]
        async with payment.sessions() as session:
            outbox = await session.scalar(select(OutboxMessage))
            expected = outbox.payload
        assert await publish_next(payment, producer, 5)
        record = await next_for_key(consumer, payment_id.encode())
        tp = TopicPartition(record.topic, record.partition)
        if lost_commit:
            previous = await consumer.committed(tp)

            class LostAck:
                async def commit(self, offsets):
                    raise ConnectionError("simulated crash before offset commit")

            with pytest.raises(ConnectionError):
                await process_record(db, LostAck(), record, settings())
            assert await consumer.committed(tp) == previous
            before = await snapshot(db)
            await consumer.stop()
            consumer = await client(group)
            replay = await next_for_key(consumer, payment_id.encode())
            assert replay.offset == record.offset
            await process_record(db, consumer, replay, settings())
            assert await snapshot(db) == before
        else:
            await process_record(db, consumer, record, settings())
        assert await consumer.committed(tp) == record.offset + 1
        rows = await snapshot(db)
        assert list(map(len, rows)) == [1, 1]
        assert rows[0][0]["payload"] == rows[1][0]["payload"] == expected
        assert rows[0][0]["state"] == "queued"
    finally:
        await consumer.stop()
        await producer.stop()


async def test_poison_record_does_not_block_next_valid_record(executor_database):
    consumer = await client("tbpo16-" + uuid4().hex)
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap())
    db, cmd = executor_database, command()
    key = str(cmd.payment_id).encode()
    try:
        await begin_at_end(consumer)
        await producer.start()
        bad = await producer.send_and_wait(ATTEMPT_COMMANDS, b"bad-json", key=key, partition=0)
        good = await producer.send_and_wait(ATTEMPT_COMMANDS, encode(cmd), key=key, partition=0)
        for _ in range(2):
            await process_record(db, consumer, await next_for_key(consumer, key), settings())
        assert await consumer.committed(TopicPartition(ATTEMPT_COMMANDS, 0)) == good.offset + 1
        async with db.sessions() as session:
            rejection = await session.get(RejectedCommand, (ATTEMPT_COMMANDS, 0, bad.offset))
            assert rejection.reason == "INVALID_COMMAND" and rejection.raw_value == b"bad-json"
            assert await session.get(InboxEvent, cmd.event_id)
            assert await session.get(ExecutorAttempt, cmd.attempt_id)
    finally:
        await consumer.stop()
        await producer.stop()
