from uuid import UUID

import pytest
from payment.db.models import Payment
from payment_test_support import payment_values
from sqlalchemy import insert

from .tbpo12_support import snapshot

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("amount", [1, 9223372036854775807])
@pytest.mark.parametrize("recipient", ["x", "я" * 255])
async def test_get_preserves_boundary_values_without_writing(
    client, migrated_database, amount, recipient
):
    values = payment_values(amount_minor=amount, recipient=recipient)
    async with migrated_database.engine.begin() as conn:
        await conn.execute(insert(Payment).values(**values))
    before = await snapshot(migrated_database)
    response = await client.get(f"/payments/{values['id']}")
    assert response.status_code == 200
    assert response.json()["amount_minor"] == amount
    assert type(response.json()["amount_minor"]) is int
    assert response.json()["recipient"] == recipient
    assert UUID(response.json()["payment_id"]) == values["id"]
    assert await snapshot(migrated_database) == before
