import json
from uuid import UUID, uuid4

import pytest
from contracts import ExecuteAttempt, ExecuteAttemptData, decode_execute_attempt
from payment.db.attempts import add_attempt, get_attempt
from payment.db.models import Payment
from payment.db.outbox import add_execute_attempt, get_outbox_message
from payment_test_support import payment_values
from sqlalchemy import text

pytestmark = pytest.mark.integration


def execute_attempt(payment_id: UUID, attempt_id: UUID) -> ExecuteAttempt:
    data = ExecuteAttemptData(
        channel_id="A", amount_minor=125000, currency="RUB", recipient="merchant-demo-001"
    )
    return ExecuteAttempt.new(payment_id=payment_id, attempt_id=attempt_id, data=data)


async def write_payment_attempt_and_command(session) -> ExecuteAttempt:
    values = payment_values()
    session.add(Payment(**values))
    await session.flush()
    attempt = await add_attempt(
        session,
        payment_id=values["id"],
        number=1,
        channel_id="A",
        decision={"strategy": "fixed", "selected_channel": "A"},
    )
    command = execute_attempt(values["id"], attempt.attempt_id)
    await add_execute_attempt(session, command)
    return command


async def count_rows(db) -> dict[str, int]:
    async with db.engine.connect() as conn:
        return {
            name: await conn.scalar(text(f"SELECT count(*) FROM {name}"))
            for name in ("payments", "payment_attempts", "outbox")
        }


async def test_attempt_and_outbox_message_are_saved_and_read(migrated_database):
    db = migrated_database
    async with db.sessions() as session, session.begin():
        command = await write_payment_attempt_and_command(session)

    async with db.sessions() as session:
        attempt = await get_attempt(session, command.attempt_id)
        message = await get_outbox_message(session, command.event_id)

    assert attempt is not None
    assert attempt.payment_id == command.payment_id
    assert attempt.number == 1 and attempt.channel_id == "A"
    assert attempt.decision == {"strategy": "fixed", "selected_channel": "A"}
    assert attempt.outcome is None and attempt.finished_at is None

    assert message is not None
    assert message.id == command.event_id
    assert message.topic == "attempt.commands"
    assert message.message_key == str(command.payment_id)
    assert message.published_at is None
    assert message.payload == command.model_dump(mode="json")
    assert decode_execute_attempt(json.dumps(message.payload).encode()) == command


async def test_missing_records_are_not_found(migrated_database):
    async with migrated_database.sessions() as session:
        assert await get_attempt(session, uuid4()) is None
        assert await get_outbox_message(session, uuid4()) is None


async def test_functions_do_not_commit(migrated_database):
    db = migrated_database
    async with db.sessions() as session:
        await write_payment_attempt_and_command(session)
        assert session.in_transaction()
        assert await count_rows(db) == {"payments": 0, "payment_attempts": 0, "outbox": 0}
    assert await count_rows(db) == {"payments": 0, "payment_attempts": 0, "outbox": 0}


async def test_rollback_discards_attempt_and_outbox_message(migrated_database):
    db = migrated_database
    with pytest.raises(RuntimeError, match="simulated failure"):
        async with db.sessions() as session, session.begin():
            await write_payment_attempt_and_command(session)
            raise RuntimeError("simulated failure before commit")
    assert await count_rows(db) == {"payments": 0, "payment_attempts": 0, "outbox": 0}
