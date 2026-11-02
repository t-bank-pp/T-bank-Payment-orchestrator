import pytest

BODY = {"amount_minor": 125000, "currency": "RUB", "recipient": "merchant-demo-001"}


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("amount_minor", 0),
        ("amount_minor", -1),
        ("amount_minor", 1.5),
        ("amount_minor", 1.0),
        ("amount_minor", "125000"),
        ("amount_minor", True),
        ("amount_minor", False),
        ("amount_minor", None),
        ("amount_minor", 9223372036854775808),
        ("currency", "USD"),
        ("currency", "rub"),
        ("currency", " RUB "),
        ("currency", None),
        ("recipient", ""),
        ("recipient", " \t\n "),
        ("recipient", "я" * 256),
        ("recipient", None),
        ("recipient", 123),
    ],
)
async def test_invalid_fields_are_422_before_opening_database(client, field, value):
    response = await client.post(
        "/payments", headers={"Idempotency-Key": "validation"}, json={**BODY, field: value}
    )
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["body", field]


@pytest.mark.parametrize("field", BODY)
async def test_missing_body_fields_are_rejected(client, field):
    body = {name: value for name, value in BODY.items() if name != field}
    response = await client.post("/payments", headers={"Idempotency-Key": "validation"}, json=body)
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["body", field]


@pytest.mark.parametrize("key", [None, "", "   ", "k" * 101])
async def test_invalid_idempotency_header_is_rejected(client, key):
    headers = {} if key is None else {"Idempotency-Key": key}
    response = await client.post("/payments", headers=headers, json=BODY)
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["header", "Idempotency-Key"]


@pytest.mark.parametrize(
    "content",
    [
        '{"amount_minor":1e0,"currency":"RUB","recipient":"demo"}',
        '{"amount_minor":',
        "null",
        "[]",
    ],
)
async def test_invalid_raw_json_is_rejected(client, content):
    response = await client.post(
        "/payments",
        headers={"Idempotency-Key": "validation", "Content-Type": "application/json"},
        content=content,
    )
    assert response.status_code == 422
    assert isinstance(response.json()["detail"], list)


async def test_unexpected_body_fields_are_rejected(client):
    response = await client.post(
        "/payments", headers={"Idempotency-Key": "validation"}, json={**BODY, "status": "succeeded"}
    )
    assert response.status_code == 422
