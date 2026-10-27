from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

import httpx
from payment.config import Settings
from payment.main import create_app


@asynccontextmanager
async def api_client(settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield client


def payment_values(**overrides: Any) -> dict[str, Any]:
    return {
        "id": uuid4(),
        "idempotency_key": uuid4().hex,
        "request_hash": "0" * 64,
        "amount_minor": 125000,
        "currency": "RUB",
        "recipient": "merchant-demo-001",
        **overrides,
    }
