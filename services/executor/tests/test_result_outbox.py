import asyncio
import json

import pytest
from contracts import ATTEMPT_EVENTS, decode_attempt_result
from executor.backfill import backfill_next_result
from executor.channel_a import ExecutionResult
from executor.db.models import OutboxMessage
from executor.execution import claim_next, expire_leases, finish_claim
from sqlalchemy import event, select, text

from .test_execution import force_expiry, state
from .test_registration import accept, command

pytestmark = pytest.mark.integration


async def messages(db):
    async with db.sessions() as session:
        return (await session.scalars(select(OutboxMessage))).all()


@pytest.mark.parametrize("outcome", ["succeeded", "failed", "unknown"])
async def test_finish_persists_matching_envelope_once(executor_database, outcome):
    db, cmd = executor_database, command()
    await accept(db, cmd)
    claim = await claim_next(db, 60)
    assert await finish_claim(db, claim, ExecutionResult(outcome, "TEST_RESULT", "operation-1"))
    assert not await finish_claim(db, claim, ExecutionResult("failed", "LATE"))
    attempt = await state(db, cmd.attempt_id)
    rows = await messages(db)
    assert len(rows) == 1
    row = rows[0]
    result = decode_attempt_result(json.dumps(row.payload).encode())
    assert row.id == result.event_id != cmd.event_id
    assert row.attempt_id == result.attempt_id == cmd.attempt_id
    assert result.payment_id == cmd.payment_id
    assert result.correlation_id == cmd.correlation_id
    assert row.message_key == str(cmd.payment_id) and row.topic == ATTEMPT_EVENTS
    assert result.data.channel_id == cmd.data.channel_id
    assert result.data.outcome == attempt.state == outcome
    assert result.data.provider_operation_id == "operation-1"
    assert result.occurred_at == attempt.finished_at
    assert row.published_at is None


@pytest.mark.parametrize("expired", [False, True])
async def test_outbox_insert_failure_rolls_back_attempt(executor_database, expired):
    db, cmd = executor_database, command()
    await accept(db, cmd)
    claim = await claim_next(db, 60)
    if expired:
        await force_expiry(db, cmd.attempt_id)

    def fail_outbox(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("INSERT INTO outbox"):
            raise RuntimeError("Injected outbox failure")

    event.listen(db.engine.sync_engine, "before_cursor_execute", fail_outbox)
    try:
        with pytest.raises(RuntimeError, match="Injected outbox"):
            if expired:
                await expire_leases(db)
            else:
                await finish_claim(db, claim, ExecutionResult("succeeded", "OK"))
    finally:
        event.remove(db.engine.sync_engine, "before_cursor_execute", fail_outbox)
    assert (await state(db, cmd.attempt_id)).state == "in_progress"
    assert await messages(db) == []
    if expired:
        await expire_leases(db)
        await expire_leases(db)
        assert not await finish_claim(db, claim, ExecutionResult("succeeded", "LATE"))
        expected = "unknown"
    else:
        assert await finish_claim(db, claim, ExecutionResult("succeeded", "OK"))
        expected = "succeeded"
    rows = await messages(db)
    assert len(rows) == 1 and rows[0].payload["data"]["outcome"] == expected


async def test_upgrade_and_repeat_backfill_preserve_legacy_results(
    executor_database, executor_migrate
):
    db = executor_database
    url = db.engine.url.render_as_string(hide_password=False)
    executor_migrate(url, "downgrade", "c61b0e6a17a0")
    commands = [command() for _ in range(3)]
    async with db.engine.begin() as conn:
        for cmd, outcome in zip(commands, ("succeeded", "failed", "unknown"), strict=True):
            await conn.execute(
                text(
                    "INSERT INTO executor_attempts "
                    "(attempt_id,payment_id,command_hash,payload,state,finished_at,reason_code) "
                    "VALUES (:a,:p,:h,CAST(:body AS jsonb),:s,clock_timestamp(),'OLD_RESULT')"
                ),
                {
                    "a": cmd.attempt_id,
                    "p": cmd.payment_id,
                    "h": "0" * 64,
                    "body": cmd.model_dump_json(),
                    "s": outcome,
                },
            )
    executor_migrate(url, "upgrade", "head")
    old_messages = await messages(db)
    assert all(str(row.id) == row.payload["event_id"] for row in old_messages)
    original = {row.attempt_id: row.payload for row in old_messages}
    assert len(original) == 3
    for cmd in commands:
        result = decode_attempt_result(json.dumps(original[cmd.attempt_id]).encode())
        attempt = await state(db, cmd.attempt_id)
        assert result.occurred_at == attempt.finished_at
        assert result.data.outcome == attempt.state
        assert result.correlation_id == cmd.correlation_id
    assert not await backfill_next_result(db)
    executor_migrate(url, "upgrade", "head")
    assert {row.attempt_id: row.payload for row in await messages(db)} == original
    with pytest.raises(AssertionError, match="Cannot downgrade result outbox"):
        executor_migrate(url, "downgrade", "c61b0e6a17a0")
    executor_migrate(url, "check")


async def test_concurrent_backfill_handles_old_worker_completion(executor_database):
    db, cmd = executor_database, command()
    await accept(db, cmd)
    # An old worker can commit after the outbox migration during a rolling upgrade.
    async with db.engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE executor_attempts SET state='unknown', finished_at=clock_timestamp(), "
                "reason_code='OLD_WORKER' WHERE attempt_id=:a"
            ),
            {"a": cmd.attempt_id},
        )
    saved = await asyncio.gather(backfill_next_result(db), backfill_next_result(db))
    assert saved.count(True) == 1
    assert not await backfill_next_result(db)
    rows = await messages(db)
    assert len(rows) == 1 and rows[0].payload["data"]["reason_code"] == "OLD_WORKER"
