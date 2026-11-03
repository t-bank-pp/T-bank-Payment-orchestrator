import asyncio
import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from payment.db.models import OutboxMessage
from payment.publisher import publish_next
from sqlalchemy import event, insert, select

pytestmark = pytest.mark.integration


class RecordingSender:
    def __init__(self):
        self.messages = []

    async def send_and_wait(self, topic, value, *, key):
        self.messages.append((topic, value, key))


async def seed_message(db, *, published=False):
    event_id, payment_id, attempt_id = uuid4(), uuid4(), uuid4()
    payload = {
        "event_id": str(event_id),
        "payment_id": str(payment_id),
        "attempt_id": str(attempt_id),
        "type": "ExecuteAttempt",
        "version": 1,
        "occurred_at": "2026-11-02T12:00:00Z",
        "correlation_id": str(payment_id),
        "data": {"channel_id": "A", "amount_minor": 125000, "currency": "RUB", "recipient": "demo"},
    }
    async with db.engine.begin() as conn:
        await conn.execute(
            insert(OutboxMessage).values(
                id=event_id,
                topic="attempt.commands",
                message_key=str(payment_id),
                payload=payload,
                published_at=datetime.now(UTC) if published else None,
            )
        )
    return event_id, payload


async def read_message(db, event_id):
    async with db.sessions() as session:
        return await session.get(OutboxMessage, event_id)


async def test_publishes_saved_envelope_and_marks_only_after_ack(migrated_database):
    db = migrated_database
    event_id, payload = await seed_message(db)
    sender = RecordingSender()
    assert await publish_next(db, sender, 1) is True
    topic, value, key = sender.messages[0]
    assert topic == "attempt.commands" and key == payload["payment_id"].encode()
    assert json.loads(value) == payload
    message = await read_message(db, event_id)
    assert message.payload == payload and message.published_at.tzinfo is not None
    assert await publish_next(db, sender, 1) is False
    assert len(sender.messages) == 1


async def test_published_messages_and_empty_queue_are_not_sent(migrated_database):
    sender = RecordingSender()
    assert await publish_next(migrated_database, sender, 1) is False
    event_id, _ = await seed_message(migrated_database, published=True)
    before = (await read_message(migrated_database, event_id)).published_at
    assert await publish_next(migrated_database, sender, 1) is False
    assert sender.messages == []
    assert (await read_message(migrated_database, event_id)).published_at == before


@pytest.mark.parametrize("failure", ["error", "timeout", "cancel"])
async def test_failed_delivery_releases_lock_and_reuses_ids(migrated_database, failure):
    db = migrated_database
    event_id, payload = await seed_message(db)

    class FailingSender(RecordingSender):
        async def send_and_wait(self, topic, value, *, key):
            await super().send_and_wait(topic, value, key=key)
            if failure == "error":
                raise ConnectionError("broker unavailable")
            if failure == "cancel":
                raise asyncio.CancelledError
            await asyncio.Event().wait()

    sender = FailingSender()
    expected = {"error": ConnectionError, "timeout": TimeoutError, "cancel": asyncio.CancelledError}
    with pytest.raises(expected[failure]):
        await publish_next(db, sender, 0.03)
    message = await read_message(db, event_id)
    assert message.published_at is None and message.payload == payload
    retry = RecordingSender()
    assert await publish_next(db, retry, 1) is True
    assert retry.messages == sender.messages


async def test_ack_followed_by_commit_failure_is_retried_with_same_ids(
    migrated_database, monkeypatch
):
    db = migrated_database
    event_id, payload = await seed_message(db)
    sender = RecordingSender()
    session = db.sessions()

    def fail_commit(_):
        raise RuntimeError("failure after Kafka ack, before DB commit")

    event.listen(session.sync_session, "before_commit", fail_commit)
    with monkeypatch.context() as patch:
        patch.setattr(db, "sessions", lambda: session)
        with pytest.raises(RuntimeError, match="after Kafka ack"):
            await publish_next(db, sender, 1)
    assert (await read_message(db, event_id)).published_at is None
    assert json.loads(sender.messages[0][1]) == payload
    assert await publish_next(db, sender, 1) is True
    assert len(sender.messages) == 2 and sender.messages[0] == sender.messages[1]


async def test_multiple_workers_skip_locked_rows_without_duplicate_send(migrated_database):
    db = migrated_database
    await seed_message(db)
    await seed_message(db)
    started, release = asyncio.Event(), asyncio.Event()

    class BlockingSender(RecordingSender):
        async def send_and_wait(self, topic, value, *, key):
            await super().send_and_wait(topic, value, key=key)
            started.set()
            await release.wait()

    first, second = BlockingSender(), RecordingSender()
    task = asyncio.create_task(publish_next(db, first, 3))
    try:
        async with asyncio.timeout(2):
            await started.wait()
        assert await publish_next(db, second, 1) is True
        assert await publish_next(db, second, 1) is False
    finally:
        release.set()
        await task
    assert len(first.messages) == len(second.messages) == 1
    ids = {json.loads(messages[0][1])["event_id"] for messages in (first.messages, second.messages)}
    assert len(ids) == 2
    async with db.sessions() as session:
        rows = (await session.scalars(select(OutboxMessage))).all()
        assert all(row.published_at is not None for row in rows)
