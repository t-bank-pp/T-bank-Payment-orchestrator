import asyncio

import pytest
from payment.config import Settings
from payment.worker import pause_until_retry, run_publisher


def settings():
    return Settings(
        database_url="postgresql+asyncpg://demo:demo@127.0.0.1:1/unreachable",
        publisher_poll_interval=0.01,
        publisher_retry_interval=0.01,
        publisher_shutdown_timeout=0.03,
        _env_file=None,
    )


class FakeProducer:
    def __init__(self, *, fail_start=False, hang_stop=False):
        self.fail_start, self.hang_stop = fail_start, hang_stop
        self.started = self.stopped = False

    async def start(self):
        self.started = True
        if self.fail_start:
            raise ConnectionError("Kafka offline")

    async def stop(self):
        self.stopped = True
        if self.hang_stop:
            await asyncio.Event().wait()


async def test_startup_failure_closes_producer_and_reconnects(monkeypatch):
    stop = asyncio.Event()
    producers = [FakeProducer(fail_start=True), FakeProducer()]
    created = []

    def factory():
        producer = producers[len(created)]
        created.append(producer)
        return producer

    async def publish(*args):
        stop.set()
        return True

    monkeypatch.setattr("payment.worker.publish_next", publish)
    await run_publisher(None, settings(), stop, producer_factory=factory)
    assert len(created) == 2
    assert all(p.started and p.stopped for p in producers)


async def test_delivery_failure_reconnects_before_retry(monkeypatch):
    stop = asyncio.Event()
    created = []
    calls = []

    def factory():
        producer = FakeProducer()
        created.append(producer)
        return producer

    async def publish(db, producer, timeout):
        calls.append(producer)
        if len(calls) == 1:
            raise ConnectionError("send failed")
        stop.set()
        return True

    monkeypatch.setattr("payment.worker.publish_next", publish)
    await run_publisher(None, settings(), stop, producer_factory=factory)
    assert len(created) == 2 and calls[0] is not calls[1]
    assert all(p.stopped for p in created)


async def test_empty_queue_wait_can_be_interrupted_by_stop(monkeypatch):
    stop = asyncio.Event()
    producer = FakeProducer()

    async def publish(*args):
        stop.set()
        return False

    monkeypatch.setattr("payment.worker.publish_next", publish)
    async with asyncio.timeout(1):
        await run_publisher(None, settings(), stop, producer_factory=lambda: producer)
        await pause_until_retry(stop, 60)
    assert producer.stopped


async def test_hanging_producer_shutdown_is_fatal_not_a_resource_leaking_retry():
    producer = FakeProducer(fail_start=True, hang_stop=True)
    with pytest.raises(TimeoutError):
        await run_publisher(None, settings(), asyncio.Event(), producer_factory=lambda: producer)
    assert producer.started and producer.stopped
