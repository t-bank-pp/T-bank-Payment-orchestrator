import asyncio
import hashlib
import hmac
import json
import time
from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest
from executor.callbacks import sign_callback
from executor.channel_a import ExecutionResult
from executor.config import Settings
from executor.confirmation import apply_confirmation
from executor.db.models import CallbackReceipt
from executor.main import create_app
from sqlalchemy import event, func, select

from .test_confirmation import unknown
from .test_execution import state
from .test_result_outbox import messages

pytestmark = pytest.mark.integration
SECRET = "test-secret-for-callbacks-" + "a" * 32


@pytest.fixture
async def callback_client(executor_database):
    app = create_app(
        Settings(
            database_url=executor_database.engine.url.render_as_string(hide_password=False),
            callback_secrets={"A": SECRET},
            _env_file=None,
        )
    )
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            base_url="http://executor", transport=httpx.ASGITransport(app=app)
        ) as client,
    ):
        yield client


def payload(cmd, **updates):
    return {
        "event_id": str(uuid4()),
        "occurred_at": datetime.now(UTC).isoformat(),
        "attempt_id": str(cmd.attempt_id),
        "payment_id": str(cmd.payment_id),
        "channel_id": "A",
        "outcome": "succeeded",
        "reason_code": "CALLBACK_CONFIRMED",
        "provider_operation_id": "operation-1",
        **updates,
    }


def signed(value, timestamp=None):
    raw = json.dumps(value).encode()
    ts = str(int(time.time())) if timestamp is None else timestamp
    signature = hmac.new(SECRET.encode(), ts.encode() + b"." + raw, hashlib.sha256).hexdigest()
    assert signature == sign_callback(SECRET, ts, raw)
    return {
        "content": raw,
        "headers": {
            "X-Callback-Timestamp": ts,
            "X-Callback-Signature": signature,
            "Content-Type": "application/json",
        },
    }


async def receipt_count(db):
    async with db.sessions() as session:
        return await session.scalar(select(func.count()).select_from(CallbackReceipt))


async def test_signed_callback_is_atomic_and_deduplicated(executor_database, callback_client):
    db = executor_database
    cmd = await unknown(db)
    value = payload(cmd)
    response = await callback_client.post("/callbacks/A", **signed(value))
    assert response.status_code == 200 and response.json()["applied"]
    duplicate = await callback_client.post("/callbacks/A", **signed(value))
    assert duplicate.status_code == 200 and not duplicate.json()["applied"]
    assert (await state(db, cmd.attempt_id)).state_version == 2
    assert await receipt_count(db) == 1 and len(await messages(db)) == 2
    changed = {**value, "reason_code": "DIFFERENT_EVENT_BODY"}
    assert (await callback_client.post("/callbacks/A", **signed(changed))).status_code == 409
    assert await receipt_count(db) == 1 and len(await messages(db)) == 2


@pytest.mark.parametrize("bad", ["signature", "old", "future", "payment", "attempt", "channel"])
async def test_untrusted_or_mismatched_callback_cannot_change_attempt(
    executor_database, callback_client, bad
):
    db = executor_database
    cmd = await unknown(db)
    value = payload(cmd)
    if bad in {"payment", "attempt"}:
        value[bad + "_id"] = str(uuid4())
    if bad == "channel":
        value["channel_id"] = "B"
    ts = str(int(time.time()) + (1000 if bad == "future" else -1000))
    request = signed(value, ts if bad in {"old", "future"} else None)
    if bad == "signature":
        request["headers"]["X-Callback-Signature"] = "invalid"
    response = await callback_client.post("/callbacks/A", **request)
    assert response.status_code in {401, 404, 409}
    assert (await state(db, cmd.attempt_id)).state == "unknown"
    assert await receipt_count(db) == 0 and len(await messages(db)) == 1


async def test_callback_and_status_confirmation_race_saves_one_update(
    executor_database, callback_client
):
    db = executor_database
    cmd = await unknown(db)

    async def confirm():
        async with db.sessions() as session, session.begin():
            return await apply_confirmation(
                session,
                attempt_id=cmd.attempt_id,
                payment_id=cmd.payment_id,
                channel_id="A",
                result=ExecutionResult("succeeded", "STATUS_CONFIRMED", "operation-1"),
            )

    callback, _ = await asyncio.gather(
        callback_client.post("/callbacks/A", **signed(payload(cmd))), confirm()
    )
    assert callback.status_code == 200
    assert (await state(db, cmd.attempt_id)).state_version == 2
    assert len(await messages(db)) == 2 and await receipt_count(db) == 1


async def test_receipt_failure_rolls_back_state_and_outbox(executor_database, callback_client):
    db = executor_database
    cmd = await unknown(db)

    def fail_receipt(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("INSERT INTO executor_callbacks"):
            raise RuntimeError("callback receipt failed")

    # The API owns another engine connected to the same isolated DB.
    api_engine = callback_client._transport.app.state.database.engine.sync_engine
    event.listen(api_engine, "before_cursor_execute", fail_receipt)
    try:
        with pytest.raises(RuntimeError, match="receipt failed"):
            await callback_client.post("/callbacks/A", **signed(payload(cmd)))
    finally:
        event.remove(api_engine, "before_cursor_execute", fail_receipt)
    assert (await state(db, cmd.attempt_id)).state == "unknown"
    assert await receipt_count(db) == 0 and len(await messages(db)) == 1
