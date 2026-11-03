import json
from uuid import UUID

import pytest
from contracts import ATTEMPT_COMMANDS, decode_execute_attempt
from payment.db.models import OutboxMessage, Payment, PaymentAttempt
from payment.use_cases.create_first_attempt import FIXED_CHANNEL_ID, create_first_attempt
from payment_test_support import payment_values
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

pytestmark = pytest.mark.integration

FIXED_DECISION = {"strategy": "fixed", "selected_channel": "A"}


def new_payment() -> Payment:
    values = payment_values()
    del values["id"]
    return Payment(**values)


async def outbox_for(session, payment_id: UUID) -> list[OutboxMessage]:
    statement = select(OutboxMessage).where(OutboxMessage.message_key == str(payment_id))
    return list((await session.scalars(statement)).all())


async def test_creates_single_first_attempt_on_fixed_channel(migrated_database):
    async with migrated_database.sessions() as session, session.begin():
        payment = new_payment()
        session.add(payment)
        created = await create_first_attempt(session, payment)
        payment_id = payment.id

    assert FIXED_CHANNEL_ID == "A"
    async with migrated_database.sessions() as session:
        attempts = (
            await session.scalars(
                select(PaymentAttempt).where(PaymentAttempt.payment_id == payment_id)
            )
        ).all()

    assert len(attempts) == 1
    attempt = attempts[0]
    assert attempt.attempt_id == created.attempt_id
    assert attempt.number == 1
    assert attempt.channel_id == "A"
    assert attempt.outcome is None
    assert attempt.decision == FIXED_DECISION


async def test_attempt_ids_are_unique_uuids(migrated_database):
    async with migrated_database.sessions() as session, session.begin():
        first_payment, second_payment = new_payment(), new_payment()
        session.add_all([first_payment, second_payment])
        first = await create_first_attempt(session, first_payment)
        second = await create_first_attempt(session, second_payment)

    assert isinstance(first.attempt_id, UUID)
    assert isinstance(second.attempt_id, UUID)
    assert first.attempt_id != second.attempt_id


async def test_writes_execute_attempt_command_to_outbox(migrated_database):
    async with migrated_database.sessions() as session, session.begin():
        payment = new_payment()
        session.add(payment)
        await create_first_attempt(session, payment)
        payment_id = payment.id

    async with migrated_database.sessions() as session:
        stored_payment = await session.get(Payment, payment_id)
        attempt = await session.scalar(
            select(PaymentAttempt).where(PaymentAttempt.payment_id == payment_id)
        )
        messages = await outbox_for(session, payment_id)

    assert stored_payment is not None and attempt is not None
    assert len(messages) == 1
    message = messages[0]
    command = decode_execute_attempt(json.dumps(message.payload).encode())

    assert message.topic == ATTEMPT_COMMANDS
    assert message.message_key == str(stored_payment.id)
    assert message.id == command.event_id
    assert message.published_at is None

    assert command.payment_id == stored_payment.id
    assert command.attempt_id == attempt.attempt_id
    assert command.correlation_id == stored_payment.id
    assert command.data.channel_id == attempt.channel_id
    assert command.data.amount_minor == stored_payment.amount_minor
    assert command.data.currency == stored_payment.currency
    assert command.data.recipient == stored_payment.recipient


async def test_does_not_commit_transaction(migrated_database):
    async with migrated_database.sessions() as session:
        payment = new_payment()
        session.add(payment)
        attempt = await create_first_attempt(session, payment)
        payment_id, attempt_id = payment.id, attempt.attempt_id
        assert session.in_transaction()
        await session.rollback()

    async with migrated_database.sessions() as session:
        assert await session.get(Payment, payment_id) is None
        assert await session.get(PaymentAttempt, attempt_id) is None
        assert await outbox_for(session, payment_id) == []
        assert await session.scalar(select(func.count()).select_from(OutboxMessage)) == 0


async def test_second_call_for_same_payment_violates_unique_number(migrated_database):
    async with migrated_database.sessions() as session, session.begin():
        payment = new_payment()
        session.add(payment)
        await create_first_attempt(session, payment)
        with pytest.raises(IntegrityError, match="uq_payment_attempts_payment_id_number"):
            await create_first_attempt(session, payment)
        await session.rollback()
