import asyncio
import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from conftest import run_alembic
from contracts import (
    ATTEMPT_EVENTS,
    AttemptResult,
    AttemptResultUpdate,
    decode_execute_attempt,
    encode,
)
from payment.config import Settings
from payment.consumer import Record, process_record, store_record
from payment.db.models import InboxEvent, OutboxMessage, Payment, PaymentAttempt
from payment.use_cases.create_payment import create_payment
from payment_test_support import api_client
from sqlalchemy import event, func, select

pytestmark = pytest.mark.integration


@pytest.fixture
async def result_pair(migrated_database):
    db = migrated_database
    async with db.sessions() as session:
        payment_id = await create_payment(
            session,
            idempotency_key=uuid4().hex,
            amount_minor=125000,
            currency="RUB",
            recipient="demo-timeout",
        )
    async with db.sessions() as session:
        payload = await session.scalar(
            select(OutboxMessage.payload).where(OutboxMessage.message_key == str(payment_id))
        )
    command = decode_execute_attempt(json.dumps(payload).encode())
    initial = AttemptResult.new(
        command,
        occurred_at=datetime.now(UTC),
        outcome="unknown",
        reason_code="CHANNEL_TIMEOUT",
    )
    confirmed = AttemptResult.new(
        command,
        occurred_at=datetime.now(UTC),
        outcome="succeeded",
        reason_code="CONFIRMED",
        provider_operation_id="operation-1",
    )
    return initial, AttemptResultUpdate.from_result(confirmed, 2)


def record(result, offset=0):
    return Record(ATTEMPT_EVENTS, 0, offset, str(result.payment_id).encode(), encode(result))


async def snapshot(db, attempt_id):
    async with db.sessions() as session:
        attempt = await session.get(PaymentAttempt, attempt_id)
        payment = await session.get(Payment, attempt.payment_id)
        return payment.status, payment.version, attempt.outcome, attempt.result_version


@pytest.mark.parametrize("reverse", [False, True])
async def test_out_of_order_events_and_replays_preserve_final_get(
    migrated_database, result_pair, reverse
):
    db = migrated_database
    initial, confirmed = result_pair
    ordered = [confirmed, initial] if reverse else [initial, confirmed]
    for i, item in enumerate(ordered):
        assert await store_record(db, record(item, i)) in {"accepted", "duplicate"}
    expected = ("succeeded", 1 if reverse else 2, "succeeded", 2)
    assert await snapshot(db, initial.attempt_id) == expected
    for item in (initial, confirmed):
        assert await store_record(db, record(item, 2)) == "duplicate"
    assert await snapshot(db, initial.attempt_id) == expected
    async with db.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(InboxEvent)) == 2
        assert await session.scalar(select(func.count()).select_from(PaymentAttempt)) == 1
        assert await session.scalar(select(func.count()).select_from(OutboxMessage)) == 1
    config = Settings(
        database_url=db.engine.url.render_as_string(hide_password=False), _env_file=None
    )
    async with api_client(config) as client:
        response = await client.get(f"/payments/{initial.payment_id}")
        assert response.status_code == 200 and response.json()["status"] == "succeeded"
        assert response.json()["attempts"][0]["outcome"] == "succeeded"


async def test_concurrent_initial_and_refined_result_converge(migrated_database, result_pair):
    db = migrated_database
    initial, confirmed = result_pair
    await asyncio.gather(store_record(db, record(initial)), store_record(db, record(confirmed, 1)))
    assert (await snapshot(db, initial.attempt_id))[2:] == ("succeeded", 2)


@pytest.mark.parametrize("kind", ["same_version", "terminal", "unknown_v2", "correlation"])
async def test_conflicts_cannot_overwrite_final_result(migrated_database, result_pair, kind):
    db = migrated_database
    initial, confirmed = result_pair
    await store_record(db, record(confirmed))
    before = await snapshot(db, initial.attempt_id)
    changes = {"event_id": uuid4()}
    if kind == "terminal":
        changes.update(
            state_version=3, data=confirmed.data.model_copy(update={"outcome": "failed"})
        )
    elif kind == "unknown_v2":
        changes.update(state_version=3, data=initial.data)
    elif kind == "correlation":
        changes.update(state_version=3, correlation_id=uuid4())
    else:
        changes["data"] = confirmed.data.model_copy(update={"reason_code": "DIFFERENT"})
    assert await store_record(db, record(confirmed.model_copy(update=changes), 1)) == "rejected"
    assert await snapshot(db, initial.attempt_id) == before


async def test_failed_refinement_commit_does_not_ack_or_change_unknown(
    migrated_database, result_pair, app_settings, monkeypatch
):
    db = migrated_database
    initial, confirmed = result_pair
    await store_record(db, record(initial))
    session = db.sessions()

    def fail_commit(_):
        raise RuntimeError("refinement commit failed")

    class NoAck:
        async def commit(self, offsets):
            pytest.fail("Failed commit must not acknowledge Kafka")

    event.listen(session.sync_session, "before_commit", fail_commit)
    with monkeypatch.context() as patch:
        patch.setattr(db, "sessions", lambda: session)
        with pytest.raises(RuntimeError, match="refinement commit failed"):
            await process_record(db, NoAck(), record(confirmed, 1), app_settings)
    assert await snapshot(db, initial.attempt_id) == ("needs_review", 1, "unknown", 1)
    assert await store_record(db, record(confirmed, 1)) == "accepted"


async def test_upgrade_keeps_v1_hash_and_allows_refinement(migrated_database, result_pair):
    db = migrated_database
    initial, confirmed = result_pair
    await store_record(db, record(initial))
    async with db.sessions() as session:
        old_hash = (await session.get(InboxEvent, initial.event_id)).payload_hash
    url = db.engine.url.render_as_string(hide_password=False)
    await db.engine.dispose()
    run_alembic(url, "downgrade", "0002_payment_results")
    run_alembic(url, "upgrade", "head")
    run_alembic(url, "check")
    assert await store_record(db, record(initial, 1)) == "duplicate"
    async with db.sessions() as session:
        assert (await session.get(InboxEvent, initial.event_id)).payload_hash == old_hash
    assert await store_record(db, record(confirmed, 2)) == "accepted"
    with pytest.raises(AssertionError, match="Cannot downgrade"):
        run_alembic(url, "downgrade", "0002_payment_results")
