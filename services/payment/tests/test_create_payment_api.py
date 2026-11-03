import asyncio
import importlib
import json
from uuid import UUID

import pytest
from contracts import decode_execute_attempt
from payment.config import Settings
from payment.db.models import OutboxMessage, Payment, PaymentAttempt
from payment.db.session import Database
from payment_test_support import api_client
from pydantic import SecretStr
from sqlalchemy import func, select, update

pytestmark = pytest.mark.integration
BODY = {"amount_minor": 125000, "currency": "RUB", "recipient": "merchant-demo-001"}
MODELS = (Payment, PaymentAttempt, OutboxMessage)


@pytest.fixture
def app_settings(migrated_database: Database) -> Settings:
    return Settings(
        database_url=SecretStr(migrated_database.engine.url.render_as_string(hide_password=False)),
        _env_file=None,
    )


async def snapshot(db):
    async with db.engine.connect() as conn:
        return {
            model.__tablename__: [
                dict(row) for row in (await conn.execute(select(model))).mappings()
            ]
            for model in MODELS
        }


async def post(client, key="create-payment", body=None):
    return await client.post(
        "/payments", headers={"Idempotency-Key": key}, json=BODY if body is None else body
    )


async def test_post_commits_payment_attempt_and_full_command_before_reply(
    client, migrated_database
):
    response = await post(client, key=" normalized-key ", body={**BODY, "recipient": " demo "})
    assert response.status_code == 202
    payment_id = UUID(response.json()["payment_id"])
    assert response.json() == {"payment_id": str(payment_id)}
    rows = await snapshot(migrated_database)
    assert [len(rows[model.__tablename__]) for model in MODELS] == [1, 1, 1]
    payment, attempt, outbox = [rows[model.__tablename__][0] for model in MODELS]
    assert payment["id"] == payment_id
    assert payment["idempotency_key"] == "normalized-key"
    assert payment["recipient"] == "demo"
    assert payment["amount_minor"] == 125000 and payment["currency"] == "RUB"
    assert payment["status"] == "in_progress" and payment["version"] == 0
    assert len(payment["request_hash"]) == 64
    assert payment["created_at"].tzinfo is not None and payment["updated_at"].tzinfo is not None
    assert attempt["payment_id"] == payment_id
    assert attempt["number"] == 1 and attempt["channel_id"] == "A"
    assert attempt["outcome"] is None and attempt["finished_at"] is None
    assert attempt["reason_code"] is None
    assert attempt["decision"] == {"strategy": "fixed", "selected_channel": "A"}
    assert outbox["topic"] == "attempt.commands"
    assert outbox["message_key"] == str(payment_id) and outbox["published_at"] is None
    command = decode_execute_attempt(json.dumps(outbox["payload"]).encode())
    assert command.event_id == outbox["id"]
    assert command.payment_id == command.correlation_id == payment_id
    assert command.attempt_id == attempt["attempt_id"]
    assert command.data.model_dump() == {**BODY, "recipient": "demo", "channel_id": "A"}
    read = await client.get(f"/payments/{payment_id}")
    assert read.status_code == 200
    assert read.json()["attempts"] == [
        {"attempt_id": str(attempt["attempt_id"]), "number": 1, "channel_id": "A", "outcome": None}
    ]


async def test_replay_normalizes_input_and_preserves_all_saved_fields(client, migrated_database):
    first = await post(client)
    assert first.status_code == 202
    payment_id = UUID(first.json()["payment_id"])
    async with migrated_database.engine.begin() as conn:
        await conn.execute(
            update(Payment).where(Payment.id == payment_id).values(status="succeeded", version=7)
        )
    before = await snapshot(migrated_database)
    for _ in range(2):
        repeated = await post(
            client,
            key=" create-payment ",
            body={"recipient": " merchant-demo-001 ", "currency": "RUB", "amount_minor": 125000},
        )
        assert repeated.status_code == 202 and repeated.json() == first.json()
    assert await snapshot(migrated_database) == before


@pytest.mark.parametrize("changes", [{"amount_minor": 125001}, {"recipient": "merchant-demo-002"}])
async def test_different_valid_data_is_409_without_changes(client, migrated_database, changes):
    first = await post(client)
    assert first.status_code == 202
    before = await snapshot(migrated_database)
    response = await post(client, body={**BODY, **changes})
    assert response.status_code == 409
    assert response.json() == {
        "error": {
            "code": "IDEMPOTENCY_CONFLICT",
            "message": "Ключ уже использован с другими данными",
        }
    }
    assert await snapshot(migrated_database) == before


async def test_invalid_data_with_existing_key_is_422_before_conflict(client, migrated_database):
    assert (await post(client)).status_code == 202
    before = await snapshot(migrated_database)
    for changes in ({"currency": "USD"}, {"amount_minor": 0}):
        response = await post(client, body={**BODY, **changes})
        assert response.status_code == 422
        assert "detail" in response.json()
    assert await snapshot(migrated_database) == before


async def test_different_keys_create_different_payments(client, migrated_database):
    first, second = await post(client, key="one"), await post(client, key="two")
    assert first.status_code == second.status_code == 202
    assert first.json()["payment_id"] != second.json()["payment_id"]
    rows = await snapshot(migrated_database)
    assert [len(rows[model.__tablename__]) for model in MODELS] == [2, 2, 2]
    assert rows["payments"][0]["request_hash"] == rows["payments"][1]["request_hash"]


async def test_integer_and_string_boundaries_are_accepted(client, migrated_database):
    for i, amount in enumerate((1, 9223372036854775807)):
        response = await post(
            client,
            key=" " + str(i) * 100 + " ",
            body={**BODY, "amount_minor": amount, "recipient": " я" + "я" * 254 + " "},
        )
        assert response.status_code == 202
        read = await client.get("/payments/" + response.json()["payment_id"])
        assert read.json()["amount_minor"] == amount
        assert read.json()["recipient"] == "я" * 255


async def test_one_hundred_simultaneous_identical_requests_create_one_set(
    client, migrated_database
):
    start = asyncio.Barrier(100)

    async def request():
        await start.wait()
        return await post(client)

    tasks = [asyncio.create_task(request()) for _ in range(100)]
    responses = await asyncio.gather(*tasks)
    assert all(response.status_code == 202 for response in responses)
    assert len({response.json()["payment_id"] for response in responses}) == 1
    rows = await snapshot(migrated_database)
    assert [len(rows[model.__tablename__]) for model in MODELS] == [1, 1, 1]


async def test_concurrent_different_bodies_return_one_202_and_one_409(client, migrated_database):
    responses = await asyncio.gather(
        post(client), post(client, body={**BODY, "amount_minor": 125001})
    )
    assert sorted(response.status_code for response in responses) == [202, 409]
    rows = await snapshot(migrated_database)
    assert [len(rows[model.__tablename__]) for model in MODELS] == [1, 1, 1]


@pytest.mark.parametrize("point", ["payment", "attempt", "outbox"])
async def test_failure_rolls_back_every_write_and_allows_retry(
    migrated_database, app_settings, monkeypatch, point
):
    creator = importlib.import_module("payment.use_cases.create_payment")
    first_attempt = importlib.import_module("payment.use_cases.create_first_attempt")
    module, name, expected_counts = {
        "payment": (creator, "insert_payment_if_new", [1, 0, 0]),
        "attempt": (first_attempt, "add_attempt", [1, 1, 0]),
        "outbox": (first_attempt, "add_execute_attempt", [1, 1, 1]),
    }[point]
    original = getattr(module, name)
    observed = []

    async def failing(session, *args, **kwargs):
        await original(session, *args, **kwargs)
        await session.flush()
        observed.extend(
            [await session.scalar(select(func.count()).select_from(model)) for model in MODELS]
        )
        raise RuntimeError("injected failure before commit")

    async with api_client(app_settings, raise_app_exceptions=False) as client:
        with monkeypatch.context() as patch:
            patch.setattr(module, name, failing)
            response = await post(client)
        assert response.status_code == 500
        assert response.json() == {
            "error": {"code": "INTERNAL_ERROR", "message": "Внутренняя ошибка сервиса"}
        }
        assert observed == expected_counts
        assert all(not rows for rows in (await snapshot(migrated_database)).values())
        retry = await post(client)
        assert retry.status_code == 202
        before = await snapshot(migrated_database)
        assert [len(before[model.__tablename__]) for model in MODELS] == [1, 1, 1]
        repeated = await post(client)
        assert repeated.status_code == 202 and repeated.json() == retry.json()
        assert await snapshot(migrated_database) == before
