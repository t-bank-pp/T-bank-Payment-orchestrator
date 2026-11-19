import asyncio
import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from aiokafka import TopicPartition
from contracts import ATTEMPT_EVENTS, AttemptResult, decode_execute_attempt, encode
from payment.consumer import Record, SourceRecordConflict, process_record, store_record
from payment.db.models import InboxEvent, OutboxMessage, Payment, PaymentAttempt, RejectedResult
from payment.use_cases.create_payment import create_payment
from sqlalchemy import event, func, select

pytestmark = pytest.mark.integration


@pytest.fixture
async def result_event(migrated_database):
    db = migrated_database
    async with db.sessions() as session:
        payment_id = await create_payment(
            session,
            idempotency_key=uuid4().hex,
            amount_minor=100,
            currency="RUB",
            recipient="demo",
        )
    async with db.sessions() as session:
        payload = await session.scalar(
            select(OutboxMessage.payload).where(OutboxMessage.message_key == str(payment_id))
        )
    command = decode_execute_attempt(json.dumps(payload).encode())
    return AttemptResult.new(
        command,
        occurred_at=datetime.now(UTC),
        outcome="succeeded",
        reason_code="DEMO_APPROVED",
        provider_operation_id="operation-1",
    )


def record_for(result, *, offset=0, key=None):
    return Record(ATTEMPT_EVENTS, 0, offset, key or str(result.payment_id).encode(), encode(result))


async def state(db, result):
    async with db.sessions() as session:
        payment = await session.get(Payment, result.payment_id)
        attempt = await session.get(PaymentAttempt, result.attempt_id)
        return payment.status, payment.version, attempt.outcome, attempt.finished_at


@pytest.mark.parametrize(
    "outcome,status",
    [
        ("succeeded", "succeeded"),
        ("failed", "failed"),
        ("unknown", "needs_review"),
    ],
)
async def test_applies_each_outcome_and_replays_without_changes(
    migrated_database, result_event, outcome, status
):
    db = migrated_database
    result = result_event.model_copy(
        update={"data": result_event.data.model_copy(update={"outcome": outcome})}
    )
    record = record_for(result)
    assert await store_record(db, record) == "accepted"
    assert await state(db, result) == (status, 1, outcome, result.occurred_at)
    assert await store_record(db, record) == "duplicate"
    assert await store_record(db, record_for(result, offset=1)) == "duplicate"
    assert await state(db, result) == (status, 1, outcome, result.occurred_at)
    async with db.sessions() as session:
        inbox = await session.get(InboxEvent, result.event_id)
        assert inbox.payload == result.model_dump(mode="json")
        attempt = await session.get(PaymentAttempt, result.attempt_id)
        assert attempt.reason_code == result.data.reason_code
        for model in (PaymentAttempt, OutboxMessage, InboxEvent):
            assert await session.scalar(select(func.count()).select_from(model)) == 1


@pytest.mark.parametrize(
    "mutation,reason",
    [
        ("payment", "PAYMENT_NOT_FOUND"),
        ("attempt", "ATTEMPT_MISMATCH"),
        ("channel", "CHANNEL_MISMATCH"),
        ("correlation", "CORRELATION_MISMATCH"),
        ("key", "KEY_MISMATCH"),
        ("malformed", "INVALID_RESULT"),
        ("tombstone", "INVALID_RESULT"),
        ("nul", "INVALID_RESULT"),
    ],
)
async def test_invalid_records_are_durable_without_applying(
    migrated_database, result_event, mutation, reason
):
    db, result = migrated_database, result_event
    if mutation in {"payment", "attempt", "correlation"}:
        result = result.model_copy(update={mutation + "_id": uuid4()})
    if mutation in {"channel", "nul"}:
        change = {"channel_id": "B"} if mutation == "channel" else {"reason_code": "bad\x00code"}
        result = result.model_copy(update={"data": result.data.model_copy(update=change)})
    record = record_for(result, key=b"wrong" if mutation == "key" else None)
    if mutation in {"malformed", "tombstone"}:
        record = Record(record.topic, 0, 0, record.key, b"{" if mutation == "malformed" else None)
    assert await store_record(db, record) == "rejected"
    assert await store_record(db, record) == "rejected"
    assert await state(db, result_event) == ("in_progress", 0, None, None)
    async with db.sessions() as session:
        rejected = await session.get(RejectedResult, (ATTEMPT_EVENTS, 0, 0))
        assert rejected.reason == reason
        assert rejected.raw_value == record.value and rejected.raw_key == record.key
        assert await session.scalar(select(func.count()).select_from(InboxEvent)) == 0


@pytest.mark.parametrize("same_id", [True, False])
async def test_conflicting_outcome_preserves_original(migrated_database, result_event, same_id):
    db, result = migrated_database, result_event
    assert await store_record(db, record_for(result)) == "accepted"
    changed = result.model_copy(
        update={
            "event_id": result.event_id if same_id else uuid4(),
            "data": result.data.model_copy(update={"outcome": "failed"}),
        }
    )
    assert await store_record(db, record_for(changed, offset=1)) == "rejected"
    assert await state(db, result) == ("succeeded", 1, "succeeded", result.occurred_at)
    async with db.sessions() as session:
        assert (await session.get(InboxEvent, result.event_id)).payload == result.model_dump(
            mode="json"
        )
        assert (
            await session.get(RejectedResult, (ATTEMPT_EVENTS, 0, 1))
        ).reason == "RESULT_CONFLICT"


@pytest.mark.parametrize("conflict", [False, True])
async def test_concurrent_deliveries_apply_once(migrated_database, result_event, conflict):
    second = result_event
    if conflict:
        second = result_event.model_copy(
            update={
                "event_id": uuid4(),
                "data": result_event.data.model_copy(update={"outcome": "unknown"}),
            }
        )
    async with asyncio.timeout(10):
        outcomes = await asyncio.gather(
            store_record(migrated_database, record_for(result_event)),
            store_record(migrated_database, record_for(second, offset=1)),
        )
    assert sorted(outcomes) == ["accepted", "rejected" if conflict else "duplicate"]
    assert (await state(migrated_database, result_event))[1] == 1


@pytest.mark.parametrize("status", ["succeeded", "failed", "needs_review", "routing"])
async def test_terminal_or_unstarted_payment_is_not_overwritten(
    migrated_database, result_event, status
):
    async with migrated_database.sessions() as session, session.begin():
        payment = await session.get(Payment, result_event.payment_id)
        payment.status = status
    assert await store_record(migrated_database, record_for(result_event)) == "rejected"
    assert await state(migrated_database, result_event) == (status, 0, None, None)


async def test_result_for_an_older_attempt_is_rejected(migrated_database, result_event):
    async with migrated_database.sessions() as session, session.begin():
        session.add(PaymentAttempt(payment_id=result_event.payment_id, number=2, channel_id="A"))
    assert await store_record(migrated_database, record_for(result_event)) == "rejected"
    assert await state(migrated_database, result_event) == ("in_progress", 0, None, None)


async def test_attempt_cannot_be_attached_to_another_payment(migrated_database, result_event):
    async with migrated_database.sessions() as session:
        other = await create_payment(
            session,
            idempotency_key=uuid4().hex,
            amount_minor=100,
            currency="RUB",
            recipient="other",
        )
    changed = result_event.model_copy(update={"payment_id": other})
    assert await store_record(migrated_database, record_for(changed)) == "rejected"
    assert await state(migrated_database, result_event) == ("in_progress", 0, None, None)


async def test_database_commit_failure_rolls_back_everything_and_does_not_ack(
    migrated_database, result_event, app_settings, monkeypatch
):
    db = migrated_database
    session = db.sessions()

    def fail_commit(_):
        raise RuntimeError("lost database commit")

    class NoAck:
        async def commit(self, offsets):
            pytest.fail("Offset must not be committed after failed database commit")

    event.listen(session.sync_session, "before_commit", fail_commit)
    with monkeypatch.context() as patch:
        patch.setattr(db, "sessions", lambda: session)
        with pytest.raises(RuntimeError, match="lost database commit"):
            await process_record(db, NoAck(), record_for(result_event), app_settings)
    assert await state(db, result_event) == ("in_progress", 0, None, None)
    async with db.sessions() as session:
        assert await session.get(InboxEvent, result_event.event_id) is None
    assert await store_record(db, record_for(result_event)) == "accepted"


async def test_kafka_commit_failure_redelivers_already_committed_event(
    migrated_database, result_event, app_settings
):
    db = migrated_database
    commits = []

    class Ack:
        async def commit(self, offsets):
            assert (await state(db, result_event))[:3] == ("succeeded", 1, "succeeded")
            commits.append(offsets)
            if len(commits) == 1:
                raise ConnectionError("lost offset commit")

    record = record_for(result_event, offset=42)
    with pytest.raises(ConnectionError):
        await process_record(db, Ack(), record, app_settings)
    await process_record(db, Ack(), record, app_settings)
    assert commits == [{TopicPartition(ATTEMPT_EVENTS, 0): 43}] * 2


async def test_rejection_is_committed_before_ack(migrated_database, result_event, app_settings):
    class Ack:
        async def commit(self, offsets):
            async with migrated_database.sessions() as session:
                saved = await session.get(RejectedResult, (ATTEMPT_EVENTS, 0, 5))
                assert saved.reason == "KEY_MISMATCH"
            assert offsets == {TopicPartition(ATTEMPT_EVENTS, 0): 6}

    await process_record(
        migrated_database, Ack(), record_for(result_event, key=b"bad", offset=5), app_settings
    )


async def test_rejected_position_cannot_silently_change_bytes(migrated_database, result_event):
    await store_record(migrated_database, record_for(result_event, key=b"bad"))
    with pytest.raises(SourceRecordConflict):
        await store_record(migrated_database, record_for(result_event, key=b"different"))


async def test_v1_does_not_refine_unknown(migrated_database, result_event):
    unknown = result_event.model_copy(
        update={
            "data": result_event.data.model_copy(update={"outcome": "unknown"}),
        }
    )
    assert await store_record(migrated_database, record_for(unknown)) == "accepted"
    confirmation = result_event.model_copy(update={"event_id": uuid4()})
    assert await store_record(migrated_database, record_for(confirmation, offset=1)) == "rejected"
    assert await state(migrated_database, unknown) == (
        "needs_review",
        1,
        "unknown",
        unknown.occurred_at,
    )


async def test_event_id_reused_for_another_payment_rolls_back_second_payment(
    migrated_database, result_event
):
    db = migrated_database
    assert await store_record(db, record_for(result_event)) == "accepted"
    async with db.sessions() as session:
        payment_id = await create_payment(
            session,
            idempotency_key=uuid4().hex,
            amount_minor=100,
            currency="RUB",
            recipient="another",
        )
    async with db.sessions() as session:
        payload = await session.scalar(
            select(OutboxMessage.payload).where(OutboxMessage.message_key == str(payment_id))
        )
    other = AttemptResult.new(
        decode_execute_attempt(json.dumps(payload).encode()),
        occurred_at=datetime.now(UTC),
        outcome="failed",
        reason_code="DECLINED",
    ).model_copy(update={"event_id": result_event.event_id})
    assert await store_record(db, record_for(other, offset=1)) == "rejected"
    assert await state(db, other) == ("in_progress", 0, None, None)
    assert await state(db, result_event) == (
        "succeeded",
        1,
        "succeeded",
        result_event.occurred_at,
    )
