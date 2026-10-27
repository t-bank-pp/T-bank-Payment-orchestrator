from uuid import UUID, uuid4

import pytest
from payment.config import Settings
from payment.db.models import Payment, PaymentAttempt
from payment.db.session import Database
from payment_test_support import payment_values
from pydantic import SecretStr
from sqlalchemy import insert, select, update

pytestmark = pytest.mark.integration


@pytest.fixture
def app_settings(migrated_database: Database) -> Settings:
    return Settings(
        database_url=SecretStr(migrated_database.engine.url.render_as_string(hide_password=False)),
        _env_file=None,
    )


async def seed_payment(database: Database, status: str = "in_progress") -> UUID:
    values = payment_values(amount_minor=2**33, status=status)
    async with database.engine.begin() as conn:
        await conn.execute(insert(Payment).values(**values))
    return values["id"]


async def test_get_payment_returns_public_fields_and_sorted_attempts(client, migrated_database):
    payment_id = await seed_payment(migrated_database, "pending")
    another_payment_id = await seed_payment(migrated_database)
    attempt_ids = {number: uuid4() for number in (1, 2, 3)}
    outcomes = {1: "failed", 2: "failed", 3: "unknown"}
    async with migrated_database.engine.begin() as conn:
        # Insert out of order and include an unrelated payment's attempt.
        for number in (3, 1, 2):
            await conn.execute(
                insert(PaymentAttempt).values(
                    attempt_id=attempt_ids[number],
                    payment_id=payment_id,
                    number=number,
                    channel_id=f"channel-{number}",
                    outcome=outcomes[number],
                    reason_code="private-reason",
                    decision={"internal": "private-decision"},
                )
            )
        await conn.execute(
            insert(PaymentAttempt).values(
                payment_id=another_payment_id,
                number=1,
                channel_id="another-channel",
            )
        )

    response = await client.get(f"/payments/{payment_id}")

    assert response.status_code == 200
    assert response.json() == {
        "payment_id": str(payment_id),
        "amount_minor": 2**33,
        "currency": "RUB",
        "recipient": "merchant-demo-001",
        "status": "pending",
        "attempts": [
            {
                "attempt_id": str(attempt_ids[n]),
                "number": n,
                "channel_id": f"channel-{n}",
                "outcome": outcomes[n],
            }
            for n in (1, 2, 3)
        ],
    }


async def test_unresolved_outcome_is_explicit_null_and_read_does_not_change_payment(
    client,
    migrated_database,
):
    payment_id = await seed_payment(migrated_database)
    attempt_id = uuid4()
    async with migrated_database.engine.begin() as conn:
        await conn.execute(
            insert(PaymentAttempt).values(
                attempt_id=attempt_id,
                payment_id=payment_id,
                number=1,
                channel_id="A",
            )
        )
        before = (
            await conn.execute(
                select(Payment.status, Payment.version, Payment.updated_at).where(
                    Payment.id == payment_id
                )
            )
        ).one()

    response = await client.get(f"/payments/{payment_id}")

    assert response.status_code == 200
    assert response.json()["attempts"] == [
        {"attempt_id": str(attempt_id), "number": 1, "channel_id": "A", "outcome": None}
    ]
    async with migrated_database.engine.connect() as conn:
        after = (
            await conn.execute(
                select(Payment.status, Payment.version, Payment.updated_at).where(
                    Payment.id == payment_id
                )
            )
        ).one()
        assert after == before


async def test_payment_without_attempts_returns_empty_list(client, migrated_database):
    payment_id = await seed_payment(migrated_database, "routing")

    response = await client.get(f"/payments/{payment_id}")

    assert response.status_code == 200
    assert response.json()["payment_id"] == str(payment_id)
    assert response.json()["attempts"] == []


async def test_missing_payment_returns_404_instead_of_another_payment(client, migrated_database):
    await seed_payment(migrated_database)

    response = await client.get(f"/payments/{uuid4()}")

    assert response.status_code == 404
    assert response.json() == {
        "error": {"code": "PAYMENT_NOT_FOUND", "message": "Платёж не найден"}
    }


async def test_each_read_returns_current_payment_and_attempt_state(client, migrated_database):
    payment_id = await seed_payment(migrated_database, "pending")
    attempt_id = uuid4()
    async with migrated_database.engine.begin() as conn:
        await conn.execute(
            insert(PaymentAttempt).values(
                attempt_id=attempt_id,
                payment_id=payment_id,
                number=1,
                channel_id="A",
                outcome="unknown",
            )
        )
    initial = await client.get(f"/payments/{payment_id}")
    assert initial.status_code == 200
    assert initial.json()["status"] == "pending"
    assert initial.json()["attempts"][0]["outcome"] == "unknown"

    async with migrated_database.engine.begin() as conn:
        await conn.execute(
            update(Payment).where(Payment.id == payment_id).values(status="succeeded")
        )
        await conn.execute(
            update(PaymentAttempt)
            .where(PaymentAttempt.attempt_id == attempt_id)
            .values(outcome="succeeded")
        )

    current = await client.get(f"/payments/{payment_id}")
    assert current.status_code == 200
    assert current.json()["status"] == "succeeded"
    assert current.json()["attempts"][0]["outcome"] == "succeeded"
