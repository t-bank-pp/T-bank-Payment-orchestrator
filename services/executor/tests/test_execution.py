import asyncio
from dataclasses import replace
from uuid import uuid4

import pytest
from executor.channel_a import ExecutionResult
from executor.db.models import ExecutorAttempt
from executor.execution import claim_next, execute_next, finish_claim, run_worker
from sqlalchemy import func, update

from .test_consumer import settings
from .test_registration import accept, command

pytestmark = pytest.mark.integration


class Channel:
    def __init__(self, db, outcome="succeeded"):
        self.db, self.outcome, self.calls = db, outcome, []

    async def execute(self, cmd):
        async with self.db.sessions() as session:
            saved = await session.get(ExecutorAttempt, cmd.attempt_id)
            assert saved.state == "in_progress" and saved.lease_token
            assert saved.lease_expires_at.tzinfo
        self.calls.append(cmd)
        return ExecutionResult(self.outcome, "DEMO_RESULT", "operation-1")


async def state(db, attempt_id):
    async with db.sessions() as session:
        return await session.get(ExecutorAttempt, attempt_id)


async def force_expiry(db, attempt_id):
    async with db.sessions() as session, session.begin():
        await session.execute(
            update(ExecutorAttempt)
            .where(ExecutorAttempt.attempt_id == attempt_id)
            .values(lease_expires_at=func.clock_timestamp())
        )


@pytest.mark.parametrize("outcome", ["succeeded", "failed", "unknown"])
async def test_execution_commits_claim_then_result_and_does_not_replay(executor_database, outcome):
    db, cmd = executor_database, command()
    await accept(db, cmd)
    channel = Channel(db, outcome)
    assert await execute_next(db, channel, settings())
    result = await state(db, cmd.attempt_id)
    assert result.state == outcome and result.reason_code == "DEMO_RESULT"
    assert result.finished_at.tzinfo and result.lease_token is None
    assert result.provider_operation_id == "operation-1"
    assert await accept(db, cmd) is False
    assert await accept(db, cmd.model_copy(update={"event_id": uuid4()})) is False
    assert not await execute_next(db, channel, settings())
    assert len(channel.calls) == 1


async def test_two_workers_cannot_call_same_attempt_concurrently(executor_database):
    db, cmd = executor_database, command()
    await accept(db, cmd)
    entered, release = asyncio.Event(), asyncio.Event()

    class Slow(Channel):
        async def execute(self, value):
            result = await super().execute(value)
            entered.set()
            await release.wait()
            return result

    channel = Slow(db)
    first = asyncio.create_task(execute_next(db, channel, settings()))
    await asyncio.wait_for(entered.wait(), 2)
    try:
        assert not await execute_next(db, channel, settings())
    finally:
        release.set()
        assert await first
    assert len(channel.calls) == 1


async def test_expired_claim_becomes_unknown_without_retry_or_late_overwrite(executor_database):
    db, cmd = executor_database, command()
    await accept(db, cmd)
    claim = await claim_next(db, 60)
    await force_expiry(db, cmd.attempt_id)
    assert not await finish_claim(db, claim, ExecutionResult("succeeded", "LATE"))
    channel = Channel(db)
    assert not await execute_next(db, channel, settings())
    result = await state(db, cmd.attempt_id)
    assert result.state == "unknown" and result.reason_code == "EXECUTION_LEASE_EXPIRED"
    assert not await finish_claim(db, claim, ExecutionResult("succeeded", "LATE"))
    assert channel.calls == []


async def test_wrong_lease_token_cannot_finish_attempt(executor_database):
    db, cmd = executor_database, command()
    await accept(db, cmd)
    claim = await claim_next(db, 60)
    assert not await finish_claim(
        db, replace(claim, token=uuid4()), ExecutionResult("failed", "BAD")
    )
    assert await finish_claim(db, claim, ExecutionResult("succeeded", "OK"))
    assert not await finish_claim(db, claim, ExecutionResult("failed", "LATE"))
    assert (await state(db, cmd.attempt_id)).state == "succeeded"


async def test_crash_after_channel_before_result_save_is_not_reexecuted(
    executor_database, monkeypatch
):
    db, cmd = executor_database, command()
    await accept(db, cmd)
    channel = Channel(db)

    async def fail_save(*args):
        raise ConnectionError("Lost DB after channel call")

    with monkeypatch.context() as patch:
        patch.setattr("executor.execution.finish_claim", fail_save)
        with pytest.raises(ConnectionError):
            await execute_next(db, channel, settings())
    assert len(channel.calls) == 1
    assert (await state(db, cmd.attempt_id)).state == "in_progress"
    await force_expiry(db, cmd.attempt_id)
    assert not await execute_next(db, channel, settings())
    assert len(channel.calls) == 1
    assert (await state(db, cmd.attempt_id)).state == "unknown"


async def test_execution_deadline_marks_unknown_without_retry(executor_database):
    db, cmd = executor_database, command()
    await accept(db, cmd)

    class Hanging(Channel):
        async def execute(self, value):
            await super().execute(value)
            await asyncio.Event().wait()

    channel = Hanging(db)
    config = settings().model_copy(update={"execution_request_timeout": 0.1})
    assert await execute_next(db, channel, config)
    result = await state(db, cmd.attempt_id)
    assert result.state == "unknown" and result.reason_code == "CHANNEL_TIMEOUT"
    assert not await execute_next(db, channel, config)
    assert len(channel.calls) == 1


async def test_unsupported_channel_is_not_sent_to_a(executor_database):
    db, cmd = executor_database, command()
    cmd = cmd.model_copy(update={"data": cmd.data.model_copy(update={"channel_id": "B"})})
    await accept(db, cmd)
    channel = Channel(db)
    await execute_next(db, channel, settings())
    assert channel.calls == []
    assert (await state(db, cmd.attempt_id)).reason_code == "UNSUPPORTED_CHANNEL"


async def test_downgrade_refuses_to_erase_completed_result(executor_database, executor_migrate):
    db, cmd = executor_database, command()
    await accept(db, cmd)
    await execute_next(db, Channel(db), settings())
    url = db.engine.url.render_as_string(hide_password=False)
    with pytest.raises(AssertionError, match="Cannot downgrade"):
        executor_migrate(url, "downgrade", "5913233c2aee")
    assert (await state(db, cmd.attempt_id)).state == "succeeded"
    executor_migrate(url, "check")


async def test_worker_idle_wait_is_interruptible(executor_database):
    stop = asyncio.Event()
    task = asyncio.create_task(
        run_worker(executor_database, Channel(executor_database), settings(), stop)
    )
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, 1)


async def test_upgrade_preserves_queued_payload(executor_database, executor_migrate):
    db, cmd = executor_database, command()
    url = db.engine.url.render_as_string(hide_password=False)
    executor_migrate(url, "downgrade", "5913233c2aee")
    # registration inserts no execution columns and works against the prior schema.
    import json

    from sqlalchemy import text

    async with db.engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO executor_attempts (attempt_id,payment_id,command_hash,payload) "
                "VALUES (:a,:p,:h,CAST(:body AS jsonb))"
            ),
            {
                "a": cmd.attempt_id,
                "p": cmd.payment_id,
                "h": "0" * 64,
                "body": cmd.model_dump_json(),
            },
        )
    executor_migrate(url, "upgrade", "head")
    saved = await state(db, cmd.attempt_id)
    assert saved.state == "queued" and saved.lease_token is None
    assert saved.payload == json.loads(cmd.model_dump_json())
