from uuid import uuid4

import pytest
from conftest import run_alembic
from payment.config import Settings
from payment.db.models import OutboxMessage, Payment, PaymentAttempt
from payment.db.session import Database
from payment_test_support import api_client, payment_values
from pydantic import SecretStr
from sqlalchemy import BigInteger, DateTime, insert, inspect, select, text
from sqlalchemy.exc import IntegrityError

pytestmark = pytest.mark.integration


async def test_migration_roundtrip_and_model_consistency(empty_database):
    run_alembic(empty_database, "upgrade", "head")
    run_alembic(empty_database, "check")
    db = Database(Settings(database_url=SecretStr(empty_database), _env_file=None))
    try:
        async with db.engine.connect() as conn:
            tables = await conn.run_sync(lambda c: inspect(c).get_table_names())
            assert set(tables) == {"payments", "payment_attempts", "outbox", "alembic_version"}
            columns = await conn.run_sync(lambda c: inspect(c).get_columns("payments"))
            by_name = {c["name"]: c for c in columns}
            assert isinstance(by_name["amount_minor"]["type"], BigInteger)
            for name in ("created_at", "updated_at"):
                assert isinstance(by_name[name]["type"], DateTime)
                assert by_name[name]["type"].timezone
            constraints = await conn.run_sync(
                lambda c: inspect(c).get_unique_constraints("payment_attempts")
            )
            assert any(c["column_names"] == ["payment_id", "number"] for c in constraints)
        await db.close()
        run_alembic(empty_database, "downgrade", "base")
        async with db.engine.connect() as conn:
            tables = await conn.run_sync(lambda c: inspect(c).get_table_names())
            assert tables == ["alembic_version"]
        await db.close()
        run_alembic(empty_database, "upgrade", "head")
        run_alembic(empty_database, "check")
    finally:
        await db.close()


@pytest.mark.parametrize("amount", [0, -1])
async def test_nonpositive_amount_is_rejected(migrated_database, amount):
    with pytest.raises(IntegrityError) as error:
        async with migrated_database.engine.begin() as conn:
            await conn.execute(insert(Payment).values(**payment_values(amount_minor=amount)))
    assert error.value.orig.sqlstate == "23514"


async def test_duplicate_idempotency_key_is_rejected(migrated_database):
    with pytest.raises(IntegrityError) as error:
        async with migrated_database.engine.begin() as conn:
            await conn.execute(insert(Payment).values(**payment_values(idempotency_key="same")))
            await conn.execute(insert(Payment).values(**payment_values(idempotency_key="same")))
    assert error.value.orig.sqlstate == "23505"


async def test_duplicate_attempt_number_is_rejected(migrated_database):
    values = payment_values()
    with pytest.raises(IntegrityError) as error:
        async with migrated_database.engine.begin() as conn:
            await conn.execute(insert(Payment).values(**values))
            for _ in range(2):
                await conn.execute(
                    insert(PaymentAttempt).values(
                        payment_id=values["id"],
                        number=1,
                        channel_id="A",
                    )
                )
    assert error.value.orig.sqlstate == "23505"


async def test_orphan_attempt_is_rejected(migrated_database):
    with pytest.raises(IntegrityError) as error:
        async with migrated_database.engine.begin() as conn:
            await conn.execute(
                insert(PaymentAttempt).values(
                    payment_id=uuid4(),
                    number=1,
                    channel_id="A",
                )
            )
    assert error.value.orig.sqlstate == "23503"


async def test_models_share_one_transaction_and_rollback_together(migrated_database):
    db = migrated_database
    values = payment_values(amount_minor=2**33)
    attempt_id, event_id = uuid4(), uuid4()

    async def write_all(session):
        session.add(Payment(**values))
        await session.flush()
        session.add(
            PaymentAttempt(
                attempt_id=attempt_id,
                payment_id=values["id"],
                number=1,
                channel_id="A",
                decision={"strategy": "fixed", "selected_channel": "A"},
            )
        )
        session.add(
            OutboxMessage(
                id=event_id,
                topic="attempt.commands",
                message_key=str(values["id"]),
                payload={"event_id": str(event_id), "attempt_id": str(attempt_id)},
            )
        )
        await session.flush()

    with pytest.raises(RuntimeError, match="simulated failure"):
        async with db.sessions() as session, session.begin():
            await write_all(session)
            raise RuntimeError("simulated failure before commit")
    async with db.engine.connect() as conn:
        for name in ("payments", "payment_attempts", "outbox"):
            assert await conn.scalar(text(f"SELECT count(*) FROM {name}")) == 0

    async with db.sessions() as session, session.begin():
        await write_all(session)
    async with db.sessions() as session:
        payment = await session.get(Payment, values["id"])
        attempt = await session.get(PaymentAttempt, attempt_id)
        outbox = await session.get(OutboxMessage, event_id)
        assert payment.amount_minor == 2**33
        assert payment.status == "in_progress" and payment.version == 0
        assert payment.created_at.tzinfo is not None and payment.updated_at.tzinfo is not None
        assert attempt.outcome is None and attempt.finished_at is None
        assert attempt.decision["strategy"] == "fixed"
        assert outbox.published_at is None
        assert outbox.payload["event_id"] == str(outbox.id)
        assert len((await session.scalars(select(PaymentAttempt))).all()) == 1


async def test_readiness_checks_real_postgres(empty_database):
    settings = Settings(database_url=SecretStr(empty_database), _env_file=None)
    async with api_client(settings) as client:
        response = await client.get("/health/ready")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}
