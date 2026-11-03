import asyncio
from uuid import uuid4

import pytest
from contracts import ExecuteAttempt, ExecuteAttemptData
from executor.db.models import ExecutorAttempt, InboxEvent
from executor.registration import CommandConflict, register_attempt
from sqlalchemy import func, select

pytestmark = pytest.mark.integration


def command():
    return ExecuteAttempt.new(
        payment_id=uuid4(),
        attempt_id=uuid4(),
        data=ExecuteAttemptData(
            channel_id="A", amount_minor=125000, currency="RUB", recipient="demo"
        ),
    )


async def snapshot(db):
    async with db.engine.connect() as conn:
        return [
            [dict(row) for row in (await conn.execute(select(model.__table__))).mappings()]
            for model in (ExecutorAttempt, InboxEvent)
        ]


async def accept(db, cmd):
    async with db.sessions() as session, session.begin():
        return await register_attempt(session, cmd)


async def test_persists_full_command_and_replay_changes_nothing(executor_database):
    db, cmd = executor_database, command()
    assert await accept(db, cmd) is True
    before = await snapshot(db)
    attempt, event = before[0][0], before[1][0]
    assert attempt["state"] == "queued"
    assert attempt["attempt_id"] == event["attempt_id"] == cmd.attempt_id
    assert attempt["payment_id"] == cmd.payment_id
    assert event["event_id"] == cmd.event_id
    assert attempt["payload"] == event["payload"] == cmd.model_dump(mode="json")
    assert attempt["created_at"].tzinfo and event["received_at"].tzinfo
    assert await accept(db, cmd) is False
    assert await snapshot(db) == before


async def test_new_event_same_attempt_does_not_create_second_attempt(executor_database):
    cmd = command()
    await accept(executor_database, cmd)
    second = cmd.model_copy(update={"event_id": uuid4()})
    assert await accept(executor_database, second) is False
    rows = await snapshot(executor_database)
    assert list(map(len, rows)) == [1, 2]
    assert rows[0][0]["payload"] == cmd.model_dump(mode="json")


@pytest.mark.parametrize("conflict", ["amount", "payment", "event", "time"])
async def test_conflict_rolls_back_entire_registration(executor_database, conflict):
    db, cmd = executor_database, command()
    await accept(db, cmd)
    before = await snapshot(db)
    if conflict == "amount":
        changed = cmd.model_copy(
            update={"event_id": uuid4(), "data": cmd.data.model_copy(update={"amount_minor": 1})}
        )
    elif conflict == "payment":
        changed = cmd.model_copy(update={"event_id": uuid4(), "payment_id": uuid4()})
    elif conflict == "event":
        changed = cmd.model_copy(update={"attempt_id": uuid4()})
    else:
        from datetime import timedelta

        changed = cmd.model_copy(update={"occurred_at": cmd.occurred_at + timedelta(seconds=1)})
    with pytest.raises(CommandConflict):
        await accept(db, changed)
    assert await snapshot(db) == before


async def test_does_not_commit_and_allows_retry_after_rollback(executor_database):
    db, cmd = executor_database, command()
    async with db.sessions() as session:
        assert await register_attempt(session, cmd)
        # Verify the records actually reached PostgreSQL before rollback.
        assert await session.scalar(select(func.count()).select_from(InboxEvent)) == 1
        await session.rollback()
    assert await snapshot(db) == [[], []]
    assert await accept(db, cmd)


@pytest.mark.parametrize("different_events", [False, True])
async def test_concurrent_deliveries_create_one_attempt(executor_database, different_events):
    db, cmd = executor_database, command()
    start = asyncio.Event()

    async def delivery():
        await start.wait()
        current = cmd.model_copy(update={"event_id": uuid4()}) if different_events else cmd
        return await accept(db, current)

    tasks = [asyncio.create_task(delivery()) for _ in range(20)]
    start.set()
    results = await asyncio.gather(*tasks)
    assert sum(results) == 1
    assert list(map(len, await snapshot(db))) == [1, 20 if different_events else 1]


async def test_concurrent_conflicting_commands_have_one_winner(executor_database):
    cmd = command()
    changed = cmd.model_copy(
        update={"event_id": uuid4(), "data": cmd.data.model_copy(update={"recipient": "other"})}
    )
    results = await asyncio.gather(
        accept(executor_database, cmd), accept(executor_database, changed), return_exceptions=True
    )
    assert sum(result is True for result in results) == 1
    assert sum(isinstance(result, CommandConflict) for result in results) == 1
    assert list(map(len, await snapshot(executor_database))) == [1, 1]


async def test_concurrent_event_reuse_rolls_back_losing_attempt(executor_database):
    cmd = command()
    changed = cmd.model_copy(update={"attempt_id": uuid4()})
    results = await asyncio.gather(
        accept(executor_database, cmd), accept(executor_database, changed), return_exceptions=True
    )
    assert sum(result is True for result in results) == 1
    assert sum(isinstance(result, CommandConflict) for result in results) == 1
    assert list(map(len, await snapshot(executor_database))) == [1, 1]
