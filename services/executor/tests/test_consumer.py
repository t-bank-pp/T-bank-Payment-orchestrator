import asyncio
from uuid import uuid4

import pytest
from aiokafka import TopicPartition
from contracts import encode
from executor.config import Settings
from executor.consumer import Record, SourceRecordConflict, process_record
from executor.db.models import RejectedCommand
from executor.registration import register_attempt
from sqlalchemy import select

from .test_registration import command, snapshot

pytestmark = pytest.mark.integration


def settings():
    return Settings(database_url="postgresql+asyncpg://demo:demo@localhost/test", _env_file=None)


def record(cmd, offset=42, *, key=None):
    return Record(
        "attempt.commands",
        1,
        offset,
        str(cmd.payment_id).encode() if key is None else key,
        encode(cmd),
    )


class Acknowledger:
    def __init__(self, db, *, fail=False):
        self.db, self.fail, self.committed = db, fail, []

    async def commit(self, offsets):
        # Independent committed-state read while the Kafka commit is being requested.
        async with self.db.sessions() as session:
            rejected = (await session.scalars(select(RejectedCommand))).all()
        rows = await snapshot(self.db)
        assert rows[0] or rejected
        if self.fail:
            raise ConnectionError("Kafka commit failed")
        self.committed.append(offsets)


async def test_confirm_only_after_durable_registration_and_only_this_offset(executor_database):
    db, cmd = executor_database, command()
    consumer = Acknowledger(db)
    await process_record(db, consumer, record(cmd), settings())
    assert consumer.committed == [{TopicPartition("attempt.commands", 1): 43}]
    assert list(map(len, await snapshot(db))) == [1, 1]


async def test_lost_offset_commit_replays_without_second_attempt(executor_database):
    db, cmd = executor_database, command()
    consumer = Acknowledger(db, fail=True)
    with pytest.raises(ConnectionError):
        await process_record(db, consumer, record(cmd), settings())
    before = await snapshot(db)
    consumer.fail = False
    await process_record(db, consumer, record(cmd), settings())
    assert await snapshot(db) == before
    assert consumer.committed == [{TopicPartition("attempt.commands", 1): 43}]


@pytest.mark.parametrize("bad", [b'{"wrong":1}', b"not-json", b"\xff", None])
async def test_invalid_command_is_saved_before_ack_and_rejection_is_idempotent(
    executor_database, bad
):
    db = executor_database
    consumer = Acknowledger(db)
    incoming = Record("attempt.commands", 0, 7, b"bad-key", bad)
    await process_record(db, consumer, incoming, settings())
    await process_record(db, consumer, incoming, settings())
    async with db.sessions() as session:
        rows = (await session.scalars(select(RejectedCommand))).all()
    assert len(rows) == 1 and rows[0].reason == "INVALID_COMMAND"
    assert rows[0].raw_value == bad and rows[0].raw_key == b"bad-key"
    assert await snapshot(db) == [[], []]
    assert consumer.committed == [{TopicPartition("attempt.commands", 0): 8}] * 2


@pytest.mark.parametrize("key", [b"wrong", b"", b"\xff", None])
async def test_wrong_or_missing_payment_key_is_rejected(executor_database, key):
    db, cmd = executor_database, command()
    consumer = Acknowledger(db)
    incoming = Record("attempt.commands", 0, 3, key, encode(cmd))
    await process_record(db, consumer, incoming, settings())
    async with db.sessions() as session:
        saved = await session.get(
            RejectedCommand, (incoming.topic, incoming.partition, incoming.offset)
        )
        assert saved.reason == "KEY_MISMATCH"
    assert await snapshot(db) == [[], []]


async def test_command_conflict_rolls_back_new_attempt_before_persisting_rejection(
    executor_database,
):
    db, cmd = executor_database, command()
    async with db.sessions() as session, session.begin():
        await register_attempt(session, cmd)
    before = await snapshot(db)
    conflicting = cmd.model_copy(update={"attempt_id": uuid4()})
    consumer = Acknowledger(db)
    await process_record(db, consumer, record(conflicting), settings())
    assert await snapshot(db) == before
    async with db.sessions() as session:
        rejected = await session.scalar(select(RejectedCommand))
        assert rejected.reason == "COMMAND_CONFLICT"


async def test_transient_db_error_rolls_back_without_ack_or_rejection(
    executor_database, monkeypatch
):
    db, cmd = executor_database, command()
    consumer = Acknowledger(db)

    async def fail_after_writes(session, value):
        await register_attempt(session, value)
        raise RuntimeError("Database transaction failed")

    with monkeypatch.context() as patch:
        patch.setattr("executor.consumer.register_attempt", fail_after_writes)
        with pytest.raises(RuntimeError):
            await process_record(db, consumer, record(cmd), settings())
    assert consumer.committed == []
    assert await snapshot(db) == [[], []]
    async with db.sessions() as session:
        assert await session.scalar(select(RejectedCommand)) is None
    await process_record(db, consumer, record(cmd), settings())


async def test_failed_rejection_commit_is_not_acknowledged(executor_database, monkeypatch):
    from sqlalchemy import event

    db = executor_database
    consumer = Acknowledger(db)
    session = db.sessions()

    def fail_commit(_):
        raise RuntimeError("Rejected message commit failed")

    event.listen(session.sync_session, "before_commit", fail_commit)
    with monkeypatch.context() as patch:
        patch.setattr(db, "sessions", lambda: session)
        with pytest.raises(RuntimeError):
            await process_record(
                db, consumer, Record("attempt.commands", 0, 8, None, None), settings()
            )
    assert consumer.committed == []
    async with db.sessions() as session:
        assert await session.scalar(select(RejectedCommand)) is None


async def test_changed_bytes_at_rejected_position_are_not_silently_acknowledged(executor_database):
    consumer = Acknowledger(executor_database)
    await process_record(
        executor_database, consumer, Record("attempt.commands", 0, 8, None, b"x"), settings()
    )
    with pytest.raises(SourceRecordConflict):
        await process_record(
            executor_database, consumer, Record("attempt.commands", 0, 8, None, b"y"), settings()
        )
    assert len(consumer.committed) == 1


async def test_processing_timeout_rolls_back_and_does_not_ack(executor_database, monkeypatch):
    db, cmd = executor_database, command()
    consumer = Acknowledger(db)

    async def hang_after_writes(session, value):
        await register_attempt(session, value)
        await asyncio.Event().wait()

    monkeypatch.setattr("executor.consumer.register_attempt", hang_after_writes)
    config = settings().model_copy(update={"consumer_processing_timeout": 0.1})
    with pytest.raises(TimeoutError):
        await process_record(db, consumer, record(cmd), config)
    assert consumer.committed == []
    assert await snapshot(db) == [[], []]


async def test_unrepresentable_jsonb_data_is_rejected_without_blocking(executor_database):
    db, cmd = executor_database, command()
    invalid = cmd.model_copy(
        update={"data": cmd.data.model_copy(update={"recipient": "bad\x00value"})}
    )
    consumer = Acknowledger(db)
    await process_record(db, consumer, record(invalid), settings())
    assert await snapshot(db) == [[], []]
    async with db.sessions() as session:
        rejected = await session.scalar(select(RejectedCommand))
        assert rejected.reason == "INVALID_COMMAND" and rejected.raw_value == encode(invalid)
    await process_record(db, consumer, record(cmd, offset=43), settings())
    assert list(map(len, await snapshot(db))) == [1, 1]
