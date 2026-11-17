import asyncio
import json
import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from contracts import ATTEMPT_EVENTS, AttemptResult, decode_execute_attempt, encode
from payment.config import Settings
from payment.consumer import process_record, store_record
from payment.db.models import InboxEvent, OutboxMessage, Payment
from payment.use_cases.create_payment import create_payment
from pydantic import SecretStr
from sqlalchemy import select

pytestmark = [pytest.mark.integration, pytest.mark.kafka]


async def test_real_kafka_restart_between_database_commit_and_offset_commit(migrated_database):
    bootstrap = os.getenv("TEST_KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap:
        pytest.skip("Set TEST_KAFKA_BOOTSTRAP_SERVERS for real Kafka checks")
    db = migrated_database
    async with db.sessions() as session:
        payment_id = await create_payment(
            session,
            idempotency_key=uuid4().hex,
            amount_minor=100,
            currency="RUB",
            recipient="demo",
        )
    async with db.sessions() as session:
        payload = await session.scalar(select(OutboxMessage.payload))
    result = AttemptResult.new(
        decode_execute_attempt(json.dumps(payload).encode()),
        occurred_at=datetime.now(UTC),
        outcome="unknown",
        reason_code="CHANNEL_TIMEOUT",
    )
    settings = Settings(
        database_url=SecretStr(db.engine.url.render_as_string(hide_password=False)), _env_file=None
    )
    group = "tbpo23-" + uuid4().hex
    partition = TopicPartition(ATTEMPT_EVENTS, 0)

    def new_consumer():
        client = AIOKafkaConsumer(
            bootstrap_servers=bootstrap,
            group_id=group,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            isolation_level="read_committed",
        )
        client.assign([partition])
        return client

    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    first, second = new_consumer(), new_consumer()
    try:
        async with asyncio.timeout(30):
            await producer.start()
            sent = await producer.send_and_wait(
                ATTEMPT_EVENTS, encode(result), key=str(payment_id).encode(), partition=0
            )
            await first.start()
            # Establish a durable starting offset before simulating process death.
            await first.commit({partition: sent.offset})
            first.seek(partition, sent.offset)
            record = await first.getone()
            assert await store_record(db, record) == "accepted"
            assert await first.committed(partition) == sent.offset
            await first.stop()  # No offset commit for the stored result.

            await second.start()
            replay = await second.getone()
            assert replay.offset == sent.offset and replay.value == record.value
            await process_record(db, second, replay, settings)
            assert await second.committed(partition) == sent.offset + 1
        async with db.sessions() as session:
            payment = await session.get(Payment, payment_id)
            assert payment.status == "needs_review" and payment.version == 1
            assert (await session.get(InboxEvent, result.event_id)).payload == result.model_dump(
                mode="json"
            )
    finally:
        await first.stop()
        await second.stop()
        await producer.stop()
