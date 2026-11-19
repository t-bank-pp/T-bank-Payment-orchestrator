import asyncio
import json

import pytest
from executor.channel_a import ExecutionResult
from executor.db.models import OutboxMessage
from executor.execution import claim_next, finish_claim
from executor.publisher import publish_next
from sqlalchemy import event, select

from .test_registration import accept, command

pytestmark = pytest.mark.integration


class Sender:
    def __init__(self):
        self.messages = []

    async def send_and_wait(self, topic, value, *, key):
        self.messages.append((topic, value, key))


async def seed_result(db):
    cmd = command()
    await accept(db, cmd)
    claim = await claim_next(db, 60)
    assert await finish_claim(db, claim, ExecutionResult("succeeded", "TEST_APPROVED"))
    async with db.sessions() as session:
        return await session.scalar(
            select(OutboxMessage).where(OutboxMessage.attempt_id == cmd.attempt_id)
        )


async def get_message(db, event_id):
    async with db.sessions() as session:
        return await session.get(OutboxMessage, event_id)


async def test_ack_precedes_published_commit(executor_database):
    db = executor_database
    message = await seed_result(db)

    class InspectingSender(Sender):
        async def send_and_wait(self, topic, value, *, key):
            assert (await get_message(db, message.id)).published_at is None
            await super().send_and_wait(topic, value, key=key)

    sender = InspectingSender()
    assert await publish_next(db, sender, 2)
    topic, value, key = sender.messages[0]
    assert topic == "attempt.events" and key == message.message_key.encode()
    assert json.loads(value) == message.payload
    assert (await get_message(db, message.id)).published_at is not None
    assert not await publish_next(db, sender, 2)
    assert len(sender.messages) == 1


@pytest.mark.parametrize("failure", ["error", "timeout", "cancel"])
async def test_failed_send_is_retried_with_identical_event(executor_database, failure):
    db = executor_database
    message = await seed_result(db)

    class FailingSender(Sender):
        async def send_and_wait(self, topic, value, *, key):
            await super().send_and_wait(topic, value, key=key)
            if failure == "error":
                raise ConnectionError("Kafka unavailable")
            if failure == "cancel":
                raise asyncio.CancelledError
            await asyncio.Event().wait()

    failed, retry = FailingSender(), Sender()
    expected = {"error": ConnectionError, "timeout": TimeoutError, "cancel": asyncio.CancelledError}
    with pytest.raises(expected[failure]):
        await publish_next(db, failed, 0.1)
    assert (await get_message(db, message.id)).published_at is None
    assert await publish_next(db, retry, 2)
    assert retry.messages == failed.messages


async def test_commit_failure_after_ack_keeps_result_for_replay(executor_database, monkeypatch):
    db = executor_database
    message, sender = await seed_result(db), Sender()
    session = db.sessions()

    def fail_commit(_):
        raise RuntimeError("Failure after ack")

    event.listen(session.sync_session, "before_commit", fail_commit)
    with monkeypatch.context() as patch:
        patch.setattr(db, "sessions", lambda: session)
        with pytest.raises(RuntimeError, match="after ack"):
            await publish_next(db, sender, 2)
    assert (await get_message(db, message.id)).published_at is None
    assert await publish_next(db, sender, 2)
    assert len(sender.messages) == 2 and sender.messages[0] == sender.messages[1]


async def test_publishers_skip_locked_results(executor_database):
    db = executor_database
    await seed_result(db)
    await seed_result(db)
    entered, release = asyncio.Event(), asyncio.Event()

    class BlockingSender(Sender):
        async def send_and_wait(self, topic, value, *, key):
            await super().send_and_wait(topic, value, key=key)
            entered.set()
            await release.wait()

    first, second = BlockingSender(), Sender()
    task = asyncio.create_task(publish_next(db, first, 3))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        assert await publish_next(db, second, 2)
        assert not await publish_next(db, second, 2)
    finally:
        release.set()
        assert await task
    assert first.messages[0] != second.messages[0]
