"""TBPO-12 acceptance checks for the stage-one payment API."""

from uuid import UUID, uuid4

import pytest

from .tbpo12_support import snapshot

pytestmark = pytest.mark.integration
BODY = {"amount_minor": 125000, "currency": "RUB", "recipient": "merchant-demo-001"}
MAX_AMOUNT = 9223372036854775807


async def post(client, key, body=None):
    headers = {} if key is None else {"Idempotency-Key": key}
    return await client.post("/payments", headers=headers, json=BODY if body is None else body)


async def assert_created(client, database, key, body):
    response = await post(client, key, body)
    assert response.status_code == 202, response.text
    result = response.json()
    assert set(result) == {"payment_id"}
    payment_id = UUID(result["payment_id"])
    before = await snapshot(database)
    payments, attempts, outbox = before
    assert tuple(map(len, before)) == (1, 1, 1)
    p, a, o = payments[0], attempts[0], outbox[0]
    assert p["id"] == payment_id
    assert p["idempotency_key"] == key.strip()
    assert p["status"] == "in_progress"
    assert p["version"] == 0
    assert p["request_hash"]
    expected = {**body, "recipient": body["recipient"].strip()}
    for field, value in expected.items():
        assert p[field] == value
    assert a["payment_id"] == payment_id
    assert (a["number"], a["channel_id"], a["outcome"]) == (1, "A", None)
    assert a["reason_code"] is None and a["finished_at"] is None
    assert a["decision"]["strategy"] == "fixed"
    assert a["decision"]["selected_channel"] == "A"
    assert o["topic"] == "attempt.commands"
    assert o["message_key"] == str(payment_id)
    assert o["published_at"] is None
    envelope = o["payload"]
    assert envelope["type"] == "ExecuteAttempt" and envelope["version"] == 1
    assert UUID(envelope["event_id"]) == o["id"]
    assert UUID(envelope["payment_id"]) == payment_id
    assert UUID(envelope["correlation_id"]) == payment_id
    assert UUID(envelope["attempt_id"]) == a["attempt_id"]
    assert envelope["data"] == {**expected, "channel_id": "A"}
    read = await client.get(f"/payments/{payment_id}")
    assert read.status_code == 200, read.text
    assert read.json() == {
        "payment_id": str(payment_id),
        **expected,
        "status": "in_progress",
        "attempts": [
            {"attempt_id": str(a["attempt_id"]), "number": 1, "channel_id": "A", "outcome": None}
        ],
    }
    assert await snapshot(database) == before
    return result, before


async def test_new_payment_and_repeated_request(client, migrated_database):
    key = uuid4().hex
    result, before = await assert_created(client, migrated_database, key, BODY)
    for _ in range(2):
        response = await post(client, key)
        assert response.status_code == 202
        assert response.json() == result
        assert await snapshot(migrated_database) == before


async def test_normalization_and_json_field_order(client, migrated_database):
    key = uuid4().hex
    result, before = await assert_created(
        client, migrated_database, f" {key} ", {**BODY, "recipient": " merchant-demo-001 "}
    )
    for changed in (BODY, dict(reversed(list(BODY.items())))):
        response = await post(client, key, changed)
        assert response.status_code == 202
        assert response.json() == result
        assert await snapshot(migrated_database) == before


@pytest.mark.parametrize(
    "change",
    [
        {"amount_minor": 125001},
        {"recipient": "merchant-demo-002"},
        {"recipient": "Merchant-demo-001"},
        {"recipient": "merchant- demo-001"},
    ],
)
async def test_conflict_preserves_all_data(client, migrated_database, change):
    key = uuid4().hex
    _, before = await assert_created(client, migrated_database, key, BODY)
    response = await post(client, key, {**BODY, **change})
    assert response.status_code == 409, response.text
    assert response.json() == {
        "error": {
            "code": "IDEMPOTENCY_CONFLICT",
            "message": "Ключ уже использован с другими данными",
        }
    }
    assert await snapshot(migrated_database) == before


@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize(
    "field,value",
    [
        ("amount_minor", 0),
        ("amount_minor", -1),
        ("amount_minor", 1.5),
        ("amount_minor", 1.0),
        ("amount_minor", "125000"),
        ("amount_minor", True),
        ("amount_minor", False),
        ("amount_minor", None),
        ("amount_minor", MAX_AMOUNT + 1),
        ("amount_minor", -MAX_AMOUNT - 2),
        ("currency", "USD"),
        ("currency", "rub"),
        ("recipient", ""),
        ("recipient", "   "),
        ("recipient", "x" * 256),
        ("recipient", "я" * 256),
        ("recipient", None),
    ],
)
async def test_invalid_body_never_writes(client, migrated_database, existing, field, value):
    key = uuid4().hex
    if existing:
        await assert_created(client, migrated_database, key, BODY)
    before = await snapshot(migrated_database)
    response = await post(client, key, {**BODY, field: value})
    assert response.status_code == 422, response.text
    assert any(
        e["loc"] == ["body", field] and e["type"] and e["msg"] for e in response.json()["detail"]
    )
    assert await snapshot(migrated_database) == before


@pytest.mark.parametrize("field", list(BODY))
async def test_missing_body_field(client, migrated_database, field):
    body = {k: v for k, v in BODY.items() if k != field}
    before = await snapshot(migrated_database)
    response = await post(client, uuid4().hex, body)
    assert response.status_code == 422, response.text
    assert any(e["loc"] == ["body", field] for e in response.json()["detail"])
    assert await snapshot(migrated_database) == before


@pytest.mark.parametrize("key", [None, "", "   ", "k" * 101])
async def test_invalid_key(client, migrated_database, key):
    before = await snapshot(migrated_database)
    response = await post(client, key)
    assert response.status_code == 422, response.text
    assert any(
        e["loc"][0] == "header" and e["type"] and e["msg"] for e in response.json()["detail"]
    )
    assert await snapshot(migrated_database) == before


@pytest.mark.parametrize("raw_amount", ["1.0", "1e0", "125000.1"])
async def test_raw_fractional_or_exponent_number(client, migrated_database, raw_amount):
    before = await snapshot(migrated_database)
    raw = '{"amount_minor":' + raw_amount + ',"currency":"RUB","recipient":"demo"}'
    response = await client.post(
        "/payments",
        content=raw,
        headers={"Content-Type": "application/json", "Idempotency-Key": uuid4().hex},
    )
    assert response.status_code == 422, response.text
    assert any(e["loc"] == ["body", "amount_minor"] for e in response.json()["detail"])
    assert await snapshot(migrated_database) == before


@pytest.mark.parametrize("amount", [1, MAX_AMOUNT])
@pytest.mark.parametrize("recipient", ["x", "x" * 255, "я" * 255, " " + "я" * 255 + " "])
@pytest.mark.parametrize("key", ["k", "k" * 100, " " + "k" * 100 + " "])
async def test_valid_boundaries_preserve_exact_values(
    client, migrated_database, amount, recipient, key
):
    await assert_created(
        client, migrated_database, key, {**BODY, "amount_minor": amount, "recipient": recipient}
    )


async def test_distinct_keys_create_distinct_payments(client, migrated_database):
    first = await post(client, "case-sensitive")
    second = await post(client, "Case-sensitive")
    assert first.status_code == second.status_code == 202
    assert first.json()["payment_id"] != second.json()["payment_id"]
    assert tuple(map(len, await snapshot(migrated_database))) == (2, 2, 2)


async def test_missing_payment_does_not_change_existing_data(client, migrated_database):
    await assert_created(client, migrated_database, uuid4().hex, BODY)
    before = await snapshot(migrated_database)
    response = await client.get(f"/payments/{uuid4()}")
    assert response.status_code == 404
    assert response.json() == {
        "error": {"code": "PAYMENT_NOT_FOUND", "message": "Платёж не найден"}
    }
    assert await snapshot(migrated_database) == before
