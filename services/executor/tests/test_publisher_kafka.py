import asyncio
import os
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from contracts import ATTEMPT_EVENTS, decode_attempt_result
from executor.channel_a import ExecutionResult
from executor.execution import claim_next, finish_claim
from executor.publisher import publish_next
from sqlalchemy import event

from .test_registration import accept, command

pytestmark = [pytest.mark.integration, pytest.mark.kafka]


@pytest.mark.parametrize("outcome", ["succeeded", "failed", "unknown"])
async def test_saved_result_reaches_kafka_with_stable_identity(
    executor_database, monkeypatch, outcome
):
    bootstrap = os.getenv("TEST_KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap:
        pytest.skip("Set TEST_KAFKA_BOOTSTRAP_SERVERS")
    db, cmd = executor_database, command()
    await accept(db, cmd)
    claim = await claim_next(db, 60)
    assert await finish_claim(db, claim, ExecutionResult(outcome, "TEST_RESULT"))
    consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap,
        group_id="tbpo22-" + uuid4().hex,
        enable_auto_commit=False,
        isolation_level="read_committed",
    )
    consumer.assign([TopicPartition(ATTEMPT_EVENTS, i) for i in range(3)])
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap, enable_idempotence=True, acks="all")
    try:
        await consumer.start()
        await consumer.seek_to_end()
        await producer.start()
        session = db.sessions()

        def fail_commit(_):
            raise RuntimeError("Crash after Kafka ack")

        event.listen(session.sync_session, "before_commit", fail_commit)
        with monkeypatch.context() as patch:
            patch.setattr(db, "sessions", lambda: session)
            with pytest.raises(RuntimeError, match="after Kafka ack"):
                await publish_next(db, producer, 5)
        assert await publish_next(db, producer, 5)
        received = []
        async with asyncio.timeout(15):
            while len(received) < 2:
                record = await consumer.getone()
                if record.key == str(cmd.payment_id).encode():
                    received.append(decode_attempt_result(record.value))
        assert received[0] == received[1]
        result = received[0]
        assert result.payment_id == cmd.payment_id
        assert result.attempt_id == cmd.attempt_id
        assert result.correlation_id == cmd.correlation_id
        assert result.data.outcome == outcome
        assert not await publish_next(db, producer, 5)
    finally:
        await consumer.stop()
        await producer.stop()
