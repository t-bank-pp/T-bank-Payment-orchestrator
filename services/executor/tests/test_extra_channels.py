import json
import time

import httpx
import pytest
from executor.channel_b import ChannelB
from executor.channel_c import ChannelC
from executor.demo_callback_delivery import CallbackDeliverySettings, deliver_next
from executor.demo_channel import DemoStore
from executor.demo_extra_channels import (
    ChannelBSettings,
    ChannelCSettings,
    create_b_app,
    create_c_app,
)

from .test_demo_channel import ledger
from .test_registration import command


def extra_command(channel, recipient="demo-success"):
    cmd = command()
    return cmd.model_copy(
        update={"data": cmd.data.model_copy(update={"channel_id": channel, "recipient": recipient})}
    )


@pytest.mark.parametrize("channel", ["B", "C"])
@pytest.mark.parametrize(
    "recipient,final,charge",
    [
        ("demo-success", "succeeded", 125000),
        ("demo-decline", "failed", 0),
    ],
)
async def test_extra_channel_apis_are_idempotent_and_survive_restart(
    tmp_path, channel, recipient, final, charge
):
    path = tmp_path / "ledger.sqlite3"
    cmd = extra_command(channel, recipient)
    settings = (
        ChannelBSettings(database=path)
        if channel == "B"
        else ChannelCSettings(database=path, callback_delay=0)
    )
    app_factory, adapter = (create_b_app, ChannelB) if channel == "B" else (create_c_app, ChannelC)
    async with httpx.AsyncClient(
        base_url="http://demo", transport=httpx.ASGITransport(app=app_factory(settings))
    ) as client:
        provider = adapter(client, 1)
        first = await provider.execute(cmd)
        second = await provider.execute(cmd)
        assert first.provider_operation_id == second.provider_operation_id
        assert first.outcome == (final if channel == "B" else "unknown")
        assert (await provider.status(cmd)).outcome == final
        changed = cmd.model_copy(update={"data": cmd.data.model_copy(update={"amount_minor": 1})})
        assert (await provider.execute(changed)).outcome == "unknown"
    assert ledger(path) == (1, charge)
    before = DemoStore(path).pending_callback()
    async with httpx.AsyncClient(
        base_url="http://demo", transport=httpx.ASGITransport(app=app_factory(settings))
    ) as client:
        replay = await adapter(client, 1).execute(cmd)
    assert replay.provider_operation_id == first.provider_operation_id
    assert ledger(path) == (1, charge)
    assert DemoStore(path).pending_callback() == before
    assert (before is not None) == (channel == "C")


async def test_callback_queue_retries_with_the_same_event(tmp_path, monkeypatch):
    path = tmp_path / "ledger.sqlite3"
    cmd = extra_command("C")
    async with httpx.AsyncClient(
        base_url="http://demo",
        transport=httpx.ASGITransport(
            app=create_c_app(ChannelCSettings(database=path, callback_delay=0))
        ),
    ) as api:
        assert (await ChannelC(api, 1).execute(cmd)).outcome == "unknown"
    store, received = DemoStore(path), []
    config = CallbackDeliverySettings(database=path, secret="x" * 32, retry_interval=30)

    def handler(request):
        received.append(request.content)
        return httpx.Response(503 if len(received) == 1 else 200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        assert await deliver_next(store, client, config)
        pending = store.pending_callback()
        # Retry scheduling is durable, independent of a worker's memory.
        assert pending is None
        monkeypatch.setattr("executor.demo_channel.time.time", lambda: time.time_ns() / 1e9 + 31)
        assert await deliver_next(DemoStore(path), client, config)
        assert DemoStore(path).pending_callback() is None
    assert len(received) == 2 and received[0] == received[1]
    assert json.loads(received[0])["attempt_id"] == str(cmd.attempt_id)
    assert ledger(path) == (1, cmd.data.amount_minor)
