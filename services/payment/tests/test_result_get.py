import json
from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest
from contracts import AttemptResult, decode_execute_attempt
from payment.config import Settings
from payment.consumer import store_record
from payment.db.models import OutboxMessage
from payment.main import create_app
from sqlalchemy import select

from .test_result_consumer import record_for

pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "outcome,status",
    [("succeeded", "succeeded"), ("failed", "failed"), ("unknown", "needs_review")],
)
async def test_get_reflects_event_and_duplicate(migrated_database, outcome, status):
    db = migrated_database
    app = create_app(
        Settings(database_url=db.engine.url.render_as_string(hide_password=False), _env_file=None)
    )
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://payment"
        ) as client,
    ):
        created = await client.post(
            "/payments",
            headers={"Idempotency-Key": uuid4().hex},
            json={"amount_minor": 125000, "currency": "RUB", "recipient": "demo"},
        )
        assert created.status_code == 202
        payment_id = created.json()["payment_id"]
        before = (await client.get("/payments/" + payment_id)).json()
        assert before["status"] == "in_progress"
        assert len(before["attempts"]) == 1 and before["attempts"][0]["outcome"] is None
        async with db.sessions() as session:
            payload = await session.scalar(
                select(OutboxMessage.payload).where(OutboxMessage.message_key == payment_id)
            )
        event = AttemptResult.new(
            decode_execute_attempt(json.dumps(payload).encode()),
            occurred_at=datetime.now(UTC),
            outcome=outcome,
            reason_code="GET_TEST",
        )
        assert await store_record(db, record_for(event)) == "accepted"
        response = await client.get("/payments/" + payment_id)
        assert response.status_code == 200
        after = response.json()
        assert after["status"] == status
        assert after["attempts"] == [{**before["attempts"][0], "outcome": outcome}]
        assert await store_record(db, record_for(event, offset=1)) == "duplicate"
        assert (await client.get("/payments/" + payment_id)).json() == after
