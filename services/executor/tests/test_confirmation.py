import asyncio
import json

import pytest
from contracts import decode_attempt_result_event
from executor.channel_a import ExecutionResult
from executor.confirmation import ConfirmationConflict, apply_confirmation
from executor.execution import claim_next, finish_claim
from sqlalchemy import event

from .test_execution import state
from .test_registration import accept, command
from .test_result_outbox import messages

pytestmark = pytest.mark.integration


async def confirm(db, cmd, outcome="succeeded", provider="operation-1"):
    async with db.sessions() as session, session.begin():
        return await apply_confirmation(
            session,
            attempt_id=cmd.attempt_id,
            payment_id=cmd.payment_id,
            channel_id=cmd.data.channel_id,
            result=ExecutionResult(outcome, "CONFIRMED", provider),
        )


async def unknown(db):
    cmd = command()
    await accept(db, cmd)
    claim = await claim_next(db, 60)
    await finish_claim(db, claim, ExecutionResult("unknown", "TIMEOUT"))
    return cmd


async def test_concurrent_confirmations_make_one_refinement(executor_database):
    db = executor_database
    cmd = await unknown(db)
    assert sorted(await asyncio.gather(confirm(db, cmd), confirm(db, cmd))) == [False, True]
    assert (await state(db, cmd.attempt_id)).state_version == 2
    rows = sorted(await messages(db), key=lambda r: r.state_version)
    assert len(rows) == 2 and rows[0].id != rows[1].id
    first, second = [decode_attempt_result_event(json.dumps(r.payload).encode()) for r in rows]
    assert first.version == 1 and first.data.outcome == "unknown"
    assert second.version == second.state_version == 2
    assert second.data.outcome == "succeeded"


async def test_conflicting_confirmation_does_not_regress_terminal(executor_database):
    db = executor_database
    cmd = await unknown(db)
    assert await confirm(db, cmd)
    with pytest.raises(ConfirmationConflict, match="OUTCOME_CONFLICT"):
        await confirm(db, cmd, "failed")
    with pytest.raises(ConfirmationConflict, match="OPERATION_MISMATCH"):
        await confirm(db, cmd, provider="other-operation")
    assert (await state(db, cmd.attempt_id)).state == "succeeded"
    assert len(await messages(db)) == 2


async def test_callback_before_worker_result_wins_without_second_initial_result(executor_database):
    db, cmd = executor_database, command()
    await accept(db, cmd)
    claim = await claim_next(db, 60)
    assert await confirm(db, cmd)
    assert not await finish_claim(db, claim, ExecutionResult("unknown", "LATE"))
    rows = await messages(db)
    assert len(rows) == 1 and rows[0].state_version == 1


async def test_refinement_and_outbox_roll_back_together(executor_database):
    db = executor_database
    cmd = await unknown(db)

    def fail_outbox(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("INSERT INTO outbox"):
            raise RuntimeError("outbox unavailable")

    event.listen(db.engine.sync_engine, "before_cursor_execute", fail_outbox)
    try:
        with pytest.raises(RuntimeError, match="outbox unavailable"):
            await confirm(db, cmd)
    finally:
        event.remove(db.engine.sync_engine, "before_cursor_execute", fail_outbox)
    assert (await state(db, cmd.attempt_id)).state_version == 1
    assert len(await messages(db)) == 1
    assert await confirm(db, cmd)
