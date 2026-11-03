import asyncio

import pytest
from executor.consumer import run_consumer

from .test_consumer import settings


class Client:
    def __init__(self, *, fail_start=False, hang_stop=False):
        self.fail_start, self.hang_stop, self.closed = fail_start, hang_stop, False

    async def start(self):
        if self.fail_start:
            raise ConnectionError("Kafka offline")

    async def stop(self):
        self.closed = True
        if self.hang_stop:
            await asyncio.Event().wait()

    async def getone(self):
        return object()


@pytest.mark.parametrize("failed_stage", ["start", "processing"])
async def test_failure_closes_client_and_reconnects_before_next_record(monkeypatch, failed_stage):
    stop = asyncio.Event()
    clients = []
    calls = 0
    config = settings().model_copy(update={"consumer_retry_interval": 0.01})

    def factory():
        client = Client(fail_start=failed_stage == "start" and not clients)
        clients.append(client)
        return client

    async def process(*args):
        nonlocal calls
        calls += 1
        if failed_stage == "processing" and calls == 1:
            raise ConnectionError("Lost database/offset commit")
        stop.set()

    monkeypatch.setattr("executor.consumer.process_record", process)
    await run_consumer(None, config, stop, consumer_factory=factory)
    assert len(clients) == 2 and all(client.closed for client in clients)


async def test_stop_interrupts_empty_poll_and_closes_consumer():
    stop = asyncio.Event()

    class Empty(Client):
        async def getone(self):
            stop.set()
            await asyncio.Event().wait()

    client = Empty()
    config = settings().model_copy(update={"consumer_poll_interval": 0.01})
    async with asyncio.timeout(1):
        await run_consumer(None, config, stop, consumer_factory=lambda: client)
    assert client.closed


async def test_stuck_shutdown_is_fatal_instead_of_leaking_consumers():
    client = Client(fail_start=True, hang_stop=True)
    config = settings().model_copy(update={"consumer_shutdown_timeout": 0.02})
    with pytest.raises(TimeoutError):
        await run_consumer(None, config, asyncio.Event(), consumer_factory=lambda: client)
    assert client.closed
