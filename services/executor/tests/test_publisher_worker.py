import asyncio

import pytest
from executor.publisher import run_publisher

from .test_consumer import settings


class Producer:
    def __init__(self, *, fail_start=False, hang_stop=False):
        self.fail_start, self.hang_stop = fail_start, hang_stop
        self.started = self.stopped = False

    async def start(self):
        self.started = True
        if self.fail_start:
            raise ConnectionError("Kafka unavailable")

    async def stop(self):
        self.stopped = True
        if self.hang_stop:
            await asyncio.Event().wait()


@pytest.mark.parametrize("startup", [False, True])
async def test_failure_closes_producer_before_retry(monkeypatch, startup):
    stop = asyncio.Event()
    created, called = [], []

    def factory():
        assert not created or created[-1].stopped
        producer = Producer(fail_start=startup and not created)
        created.append(producer)
        return producer

    async def publish(db, sender, timeout):
        called.append(sender)
        if not startup and len(called) == 1:
            raise ConnectionError("Delivery failed")
        stop.set()
        return True

    monkeypatch.setattr("executor.publisher.publish_next", publish)
    config = settings().model_copy(update={"publisher_retry_interval": 0.01})
    await run_publisher(None, config, stop, producer_factory=factory)
    assert len(created) == 2 and all(p.started and p.stopped for p in created)


async def test_shutdown_timeout_ends_process():
    producer = Producer(fail_start=True, hang_stop=True)
    config = settings().model_copy(update={"publisher_shutdown_timeout": 0.05})
    with pytest.raises(TimeoutError):
        await run_publisher(None, config, asyncio.Event(), producer_factory=lambda: producer)
    assert producer.stopped


async def test_idle_publisher_stops_promptly(monkeypatch):
    entered, stop = asyncio.Event(), asyncio.Event()
    producer = Producer()

    async def publish(*args):
        entered.set()
        return False

    monkeypatch.setattr("executor.publisher.publish_next", publish)
    task = asyncio.create_task(
        run_publisher(None, settings(), stop, producer_factory=lambda: producer)
    )
    await asyncio.wait_for(entered.wait(), 1)
    stop.set()
    await asyncio.wait_for(task, 1)
    assert producer.stopped
