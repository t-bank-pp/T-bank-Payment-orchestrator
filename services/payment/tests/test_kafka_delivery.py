import asyncio
import json
import os
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from contracts import ATTEMPT_COMMANDS, decode_execute_attempt
from payment.config import Settings
from payment.db.models import OutboxMessage
from payment.publisher import publish_next
from payment_test_support import api_client
from pydantic import SecretStr
from sqlalchemy import event, select

pytestmark = [pytest.mark.integration, pytest.mark.kafka]


@pytest.mark.parametrize("lost_commit", [False, True])
async def test_post_to_real_kafka_preserves_key_and_ids(
    migrated_database, monkeypatch, lost_commit
):
    bootstrap = os.getenv("TEST_KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap:
        pytest.skip("Set TEST_KAFKA_BOOTSTRAP_SERVERS for real Kafka checks")
    db = migrated_database
    consumer = AIOKafkaConsumer(
        ATTEMPT_COMMANDS,
        bootstrap_servers=bootstrap,
        group_id=None,
        enable_auto_commit=False,
        auto_offset_reset="latest",
    )
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap, enable_idempotence=True)
    try:
        await consumer.start()
        await consumer.seek_to_end()
        await producer.start()
        settings = Settings(
            database_url=SecretStr(db.engine.url.render_as_string(hide_password=False)),
            _env_file=None,
        )
        async with api_client(settings) as client:
            response = await client.post(
                "/payments",
                headers={"Idempotency-Key": "kafka-test-" + uuid4().hex},
                json={"amount_minor": 125000, "currency": "RUB", "recipient": "demo-kafka"},
            )
        assert response.status_code == 202
        payment_id = response.json()["payment_id"]
        async with db.sessions() as session:
            original = await session.scalar(select(OutboxMessage))
            payload = original.payload
            event_id = original.id
            assert original.published_at is None

        if lost_commit:
            session = db.sessions()

            def fail_commit(_):
                raise RuntimeError("simulated crash after ack")

            event.listen(session.sync_session, "before_commit", fail_commit)
            with monkeypatch.context() as patch:
                patch.setattr(db, "sessions", lambda: session)
                with pytest.raises(RuntimeError, match="after ack"):
                    await publish_next(db, producer, 10)
            async with db.sessions() as session:
                assert (await session.get(OutboxMessage, event_id)).published_at is None

        assert await publish_next(db, producer, 10) is True
        deliveries = []
        async with asyncio.timeout(15):
            while len(deliveries) < (2 if lost_commit else 1):
                record = await consumer.getone()
                if record.key == payment_id.encode():
                    deliveries.append(record)
        for record in deliveries:
            assert json.loads(record.value) == payload
            command = decode_execute_attempt(record.value)
            assert str(command.payment_id) == payment_id
            assert command.event_id == event_id
            assert str(command.attempt_id) == payload["attempt_id"]
        async with db.sessions() as session:
            assert (await session.get(OutboxMessage, event_id)).published_at is not None
        assert await publish_next(db, producer, 1) is False
    finally:
        await producer.stop()
        await consumer.stop()
