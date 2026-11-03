import asyncio
import sqlite3
from contextlib import closing
from uuid import uuid4

import httpx
import pytest
from executor.channel_a import ChannelA
from executor.demo_channel import DemoSettings, create_app
from executor.execution import execute_next

from .test_consumer import settings
from .test_execution import state
from .test_registration import accept, command


def body(cmd):
    return {
        **cmd.data.model_dump(mode="json"),
        "attempt_id": str(cmd.attempt_id),
        "payment_id": str(cmd.payment_id),
    }


def ledger(path):
    with closing(sqlite3.connect(path)) as conn:
        return conn.execute(
            "SELECT count(*),coalesce(sum(charged_amount_minor),0) FROM operations"
        ).fetchone()


async def test_provider_deduplicates_concurrent_requests_and_survives_restart(tmp_path):
    config = DemoSettings(database=tmp_path / "channel.sqlite3")
    cmd = command()
    headers = {"Idempotency-Key": str(cmd.attempt_id)}
    async with httpx.AsyncClient(
        base_url="http://demo", transport=httpx.ASGITransport(app=create_app(config))
    ) as client:
        responses = await asyncio.gather(
            *[client.post("/attempts", json=body(cmd), headers=headers) for _ in range(10)]
        )
        assert all(response.status_code == 200 for response in responses)
        assert all(response.json() == responses[0].json() for response in responses)
        assert ledger(config.database) == (1, cmd.data.amount_minor)
        changed = {**body(cmd), "amount_minor": cmd.data.amount_minor + 1}
        assert (await client.post("/attempts", json=changed, headers=headers)).status_code == 409
    async with httpx.AsyncClient(
        base_url="http://demo", transport=httpx.ASGITransport(app=create_app(config))
    ) as client:
        replay = await client.post("/attempts", json=body(cmd), headers=headers)
        assert replay.json() == responses[0].json()
        assert (await client.get(f"/attempts/{cmd.attempt_id}")).json() == replay.json()
        assert (await client.get(f"/attempts/{uuid4()}")).status_code == 404
    assert ledger(config.database) == (1, cmd.data.amount_minor)


async def test_demo_decline_does_not_charge_and_invalid_key_does_not_write(tmp_path):
    config = DemoSettings(database=tmp_path / "channel.sqlite3")
    cmd = command()
    payload = {**body(cmd), "recipient": "demo-decline"}
    async with httpx.AsyncClient(
        base_url="http://demo", transport=httpx.ASGITransport(app=create_app(config))
    ) as client:
        assert (await client.post("/attempts", json=payload)).status_code == 422
        assert (
            await client.post("/attempts", json=payload, headers={"Idempotency-Key": "wrong"})
        ).status_code == 422
        assert ledger(config.database) == (0, 0)
        response = await client.post(
            "/attempts", json=payload, headers={"Idempotency-Key": str(cmd.attempt_id)}
        )
        assert response.json()["outcome"] == "failed"
    assert ledger(config.database) == (1, 0)


@pytest.mark.integration
@pytest.mark.parametrize(
    "recipient,outcome,amount",
    [
        ("demo", "succeeded", 125000),
        ("demo-decline", "failed", 0),
        ("demo-timeout", "unknown", 125000),
    ],
)
async def test_executor_with_durable_demo_channel(
    executor_database, tmp_path, recipient, outcome, amount
):
    db, cmd = executor_database, command()
    cmd = cmd.model_copy(update={"data": cmd.data.model_copy(update={"recipient": recipient})})
    await accept(db, cmd)
    config = DemoSettings(database=tmp_path / "channel.sqlite3", delay_seconds=1)
    async with httpx.AsyncClient(
        base_url="http://demo", transport=httpx.ASGITransport(app=create_app(config))
    ) as client:
        adapter = ChannelA(client, 0.5 if recipient == "demo-timeout" else 2)
        assert await execute_next(db, adapter, settings())
        assert (await state(db, cmd.attempt_id)).state == outcome
        assert not await execute_next(db, adapter, settings())
    assert ledger(config.database) == (1, amount)
