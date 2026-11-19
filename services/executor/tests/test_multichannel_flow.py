import asyncio
import os
from uuid import uuid4

import httpx
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from contracts import ATTEMPT_COMMANDS, ATTEMPT_EVENTS
from executor.channel_b import ChannelB
from executor.channel_c import ChannelC
from executor.config import Settings as ExecutorSettings
from executor.consumer import process_record as receive_command
from executor.db.models import CallbackReceipt, ExecutorAttempt
from executor.demo_callback_delivery import CallbackDeliverySettings, deliver_next
from executor.demo_channel import DemoStore
from executor.demo_extra_channels import (
    ChannelBSettings,
    ChannelCSettings,
    create_b_app,
    create_c_app,
)
from executor.execution import execute_next
from executor.main import create_app as executor_app
from executor.publisher import publish_next as publish_result
from executor.reconciliation import reconcile_next
from payment.config import Settings as PaymentSettings
from payment.consumer import process_record as receive_result
from payment.db.models import PaymentAttempt
from payment.main import create_app as payment_app
from payment.publisher import publish_next as publish_command
from sqlalchemy import func, select

from .test_demo_channel import ledger

pytestmark = [pytest.mark.integration, pytest.mark.kafka]
SECRET = "local-channel-c-test-secret-" + "a" * 32


async def matching(consumer, key):
    async with asyncio.timeout(15):
        while True:
            record = await consumer.getone()
            if record.key == key:
                return record


@pytest.mark.parametrize(
    "channel,recipient,via,final,charge",
    [
        ("B", "demo-success", "sync", "succeeded", 125000),
        ("B", "demo-decline", "sync", "failed", 0),
        ("C", "demo-success", "callback", "succeeded", 125000),
        ("C", "demo-decline", "callback", "failed", 0),
        ("C", "demo-success", "status", "succeeded", 125000),
    ],
)
async def test_post_kafka_channel_refinement_and_get(
    executor_database,
    consumer_payment_database,
    tmp_path,
    monkeypatch,
    channel,
    recipient,
    via,
    final,
    charge,
):
    bootstrap = os.getenv("TEST_KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap:
        pytest.skip("Set TEST_KAFKA_BOOTSTRAP_SERVERS")
    db, payment = executor_database, consumer_payment_database
    config = ExecutorSettings(
        database_url=db.engine.url.render_as_string(hide_password=False),
        callback_secrets={"C": SECRET},
        _env_file=None,
    )
    payment_config = PaymentSettings(
        database_url=payment.engine.url.render_as_string(hide_password=False),
        fixed_channel_id=channel,
        _env_file=None,
    )
    app, callbacks = payment_app(payment_config), executor_app(config)
    path = tmp_path / "channel.sqlite3"
    demo = (
        create_b_app(ChannelBSettings(database=path))
        if channel == "B"
        else create_c_app(ChannelCSettings(database=path, callback_delay=0))
    )
    consumers = []
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap, enable_idempotence=True, acks="all")
    try:
        for topic in (ATTEMPT_COMMANDS, ATTEMPT_EVENTS):
            consumer = AIOKafkaConsumer(
                bootstrap_servers=bootstrap,
                group_id="tbpo31-" + uuid4().hex,
                enable_auto_commit=False,
                isolation_level="read_committed",
            )
            consumers.append(consumer)
            consumer.assign([TopicPartition(topic, i) for i in range(3)])
            await consumer.start()
            await consumer.seek_to_end()
        await producer.start()
        async with app.router.lifespan_context(app), callbacks.router.lifespan_context(callbacks):
            async with (
                httpx.AsyncClient(
                    base_url="http://payment", transport=httpx.ASGITransport(app=app)
                ) as api,
                httpx.AsyncClient(
                    base_url="http://demo", transport=httpx.ASGITransport(app=demo)
                ) as provider,
                httpx.AsyncClient(
                    base_url="http://executor", transport=httpx.ASGITransport(app=callbacks)
                ) as callback_client,
            ):
                created = await api.post(
                    "/payments",
                    headers={"Idempotency-Key": uuid4().hex},
                    json={"amount_minor": 125000, "currency": "RUB", "recipient": recipient},
                )
                assert created.status_code == 202
                payment_id = created.json()["payment_id"]
                key = payment_id.encode()
                assert await publish_command(payment, producer, 5)
                command_record = await matching(consumers[0], key)
                await receive_command(db, consumers[0], command_record, config)
                adapters = {
                    channel: ChannelB(provider, 1) if channel == "B" else ChannelC(provider, 1)
                }
                assert await execute_next(db, adapters, config)
                assert await publish_result(db, producer, 5)
                initial = await matching(consumers[1], key)
                await receive_result(payment, consumers[1], initial, payment_config)
                first = (await api.get("/payments/" + payment_id)).json()
                assert first["attempts"][0]["channel_id"] == channel
                if channel == "C":
                    assert first["status"] == "needs_review"
                    if via == "callback":
                        store = DemoStore(path)
                        delivery = CallbackDeliverySettings(
                            database=path,
                            secret=SECRET,
                            url="http://executor/callbacks/C",
                        )

                        # Lose the callback acknowledgement locally after the API committed.
                        def fail_ack(attempt_id):
                            raise RuntimeError("lost callback ack")

                        with monkeypatch.context() as patch:
                            patch.setattr(store, "mark_callback_delivered", fail_ack)
                            with pytest.raises(RuntimeError, match="lost callback ack"):
                                await deliver_next(store, callback_client, delivery)
                        assert await deliver_next(DemoStore(path), callback_client, delivery)
                        async with db.sessions() as session:
                            assert (
                                await session.scalar(
                                    select(func.count()).select_from(CallbackReceipt)
                                )
                                == 1
                            )
                    else:
                        assert await reconcile_next(db, adapters, config)
                    assert await publish_result(db, producer, 5)
                    initial = await matching(consumers[1], key)
                    await receive_result(payment, consumers[1], initial, payment_config)
                result = (await api.get("/payments/" + payment_id)).json()
                assert result["status"] == final
                assert len(result["attempts"]) == 1
                assert result["attempts"][0]["outcome"] == final
                # Real duplicate deliveries at new Kafka offsets.
                await producer.send_and_wait(ATTEMPT_COMMANDS, command_record.value, key=key)
                await receive_command(db, consumers[0], await matching(consumers[0], key), config)
                assert not await execute_next(db, adapters, config)
                await producer.send_and_wait(ATTEMPT_EVENTS, initial.value, key=key)
                await receive_result(
                    payment, consumers[1], await matching(consumers[1], key), payment_config
                )
                assert (await api.get("/payments/" + payment_id)).json() == result
                assert ledger(path) == (1, charge)
                for database, model in ((db, ExecutorAttempt), (payment, PaymentAttempt)):
                    async with database.sessions() as session:
                        assert await session.scalar(select(func.count()).select_from(model)) == 1
    finally:
        for consumer in consumers:
            await consumer.stop()
        await producer.stop()
