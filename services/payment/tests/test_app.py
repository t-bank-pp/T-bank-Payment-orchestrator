import httpx
from payment.db.session import Database


async def test_docs_and_liveness_start_without_opening_a_database_connection(
    client: httpx.AsyncClient,
):
    assert (await client.get("/docs")).status_code == 200
    schema = (await client.get("/openapi.json")).json()
    assert set(schema["paths"]) == {
        "/health/live",
        "/health/ready",
        "/payments/{payment_id}",
    }
    operation = schema["paths"]["/payments/{payment_id}"]["get"]
    assert operation["parameters"][0]["schema"]["format"] == "uuid"
    for status, model in (("200", "PaymentResponse"), ("404", "ErrorResponse")):
        assert operation["responses"][status]["content"]["application/json"]["schema"] == {
            "$ref": f"#/components/schemas/{model}"
        }
    assert schema["components"]["schemas"]["PaymentResponse"]["properties"]["attempts"][
        "items"
    ] == {"$ref": "#/components/schemas/AttemptResponse"}
    assert (await client.get("/health/live")).json() == {"status": "ok"}


async def test_invalid_payment_uuid_is_rejected_before_querying_database(client: httpx.AsyncClient):
    response = await client.get("/payments/not-a-uuid")
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["path", "payment_id"]


async def test_readiness_returns_503_when_database_is_unavailable(
    monkeypatch, client: httpx.AsyncClient
):
    async def unavailable(self):
        raise OSError("connection refused")

    monkeypatch.setattr(Database, "check_connection", unavailable)
    response = await client.get("/health/ready")
    assert response.status_code == 503
    assert response.json() == {"status": "unavailable"}
    assert (await client.get("/health/live")).status_code == 200
