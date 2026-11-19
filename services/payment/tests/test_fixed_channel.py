from uuid import uuid4

import pytest
from payment.config import Settings
from payment_test_support import api_client

pytestmark = pytest.mark.integration


async def test_api_channel_setting_only_affects_new_payment(migrated_database):
    url = migrated_database.engine.url.render_as_string(hide_password=False)
    key = uuid4().hex
    body = {"amount_minor": 125000, "currency": "RUB", "recipient": "demo"}
    original = None
    for channel in ("B", "C"):
        config = Settings(database_url=url, fixed_channel_id=channel, _env_file=None)
        async with api_client(config) as client:
            response = await client.post("/payments", headers={"Idempotency-Key": key}, json=body)
            assert response.status_code == 202
            if original is None:
                original = response.json()["payment_id"]
            assert response.json()["payment_id"] == original
            saved = (await client.get("/payments/" + original)).json()
            assert len(saved["attempts"]) == 1 and saved["attempts"][0]["channel_id"] == "B"
