import asyncio
from uuid import uuid4

import httpx
import pytest
from executor.channel_a import ChannelA
from executor.demo_channel import DemoSettings, create_app
from executor.db.models import ExecutorAttempt
from executor.execution import execute_next
from executor.reconciliation import claim_reconciliation, reconcile_next, run_reconciliation
from sqlalchemy import func, update

from .test_confirmation import unknown
from .test_consumer import settings
from .test_demo_channel import ledger
from .test_execution import state
from .test_registration import accept, command
from .test_result_outbox import messages

pytestmark = pytest.mark.integration


async def test_status_resolves_timeout_after_debit_without_new_post(executor_database, tmp_path):
    db, cmd = executor_database, command()
    cmd = cmd.model_copy(update={"data": cmd.data.model_copy(update={"recipient": "demo-timeout"})})
    await accept(db, cmd)
    demo = DemoSettings(database=tmp_path / "ledger.sqlite3", delay_seconds=0.5)
    config = settings().model_copy(update={"execution_request_timeout": 0.1})
    async with httpx.AsyncClient(
        base_url="http://demo", transport=httpx.ASGITransport(app=create_app(demo))
    ) as client:
        channel = ChannelA(client, 0.1)
        assert await execute_next(db, channel, config)
        assert (await state(db, cmd.attempt_id)).state == "unknown"
        assert ledger(demo.database) == (1, cmd.data.amount_minor)
        assert await reconcile_next(db, channel, config)
        assert not await reconcile_next(db, channel, config)
    attempt = await state(db, cmd.attempt_id)
    assert attempt.state == "succeeded" and attempt.state_version == 2
    assert ledger(demo.database) == (1, cmd.data.amount_minor)
    assert len(await messages(db)) == 2


@pytest.mark.parametrize("kind", ["404", "500", "pending", "wrong_id", "network", "timeout"])
async def test_uncertain_status_preserves_unknown_and_reschedules(executor_database, kind):
    db = executor_database
    cmd = await unknown(db)
    methods = []

    async def reply(request):
        methods.append(request.method)
        if kind == "network":
            raise httpx.ConnectError("offline", request=request)
        if kind == "timeout":
            raise httpx.ReadTimeout("late", request=request)
        return httpx.Response(
            int(kind) if kind in {"404", "500"} else 200,
            json={
                "attempt_id": str(uuid4() if kind == "wrong_id" else cmd.attempt_id),
                "outcome": "pending" if kind == "pending" else "succeeded",
                "reason_code": "TEST",
                "provider_operation_id": "operation-1",
            },
        )

    async with httpx.AsyncClient(
        base_url="http://demo", transport=httpx.MockTransport(reply)
    ) as client:
        assert await reconcile_next(db, ChannelA(client, 0.1), settings())
        assert not await reconcile_next(db, ChannelA(client, 0.1), settings())
    attempt = await state(db, cmd.attempt_id)
    assert attempt.state == "unknown" and attempt.state_version == 1
    assert attempt.next_reconcile_at is not None
    assert methods == ["GET"] and len(await messages(db)) == 1


async def test_crashed_check_becomes_available_again(executor_database):
    db = executor_database
    cmd = await unknown(db)
    assert await claim_reconciliation(db, 30)
    assert await claim_reconciliation(db, 30) is None
    async with db.sessions() as session, session.begin():
        await session.execute(
            update(ExecutorAttempt)
            .where(ExecutorAttempt.attempt_id == cmd.attempt_id)
            .values(next_reconcile_at=func.clock_timestamp())
        )
    assert await claim_reconciliation(db, 30)
    assert (await state(db, cmd.attempt_id)).state == "unknown"


async def test_idle_reconciliation_stops_promptly(executor_database):
    class Channel:
        async def status(self, command):
            raise AssertionError("No attempts to check")

    stop = asyncio.Event()
    task = asyncio.create_task(run_reconciliation(executor_database, Channel(), settings(), stop))
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, 1)
