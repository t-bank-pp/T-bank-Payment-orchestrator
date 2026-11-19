import httpx
import pytest
from executor.config import Settings
from executor.main import create_app


@pytest.mark.parametrize(
    "timestamp",
    ["9" * 400, "-" + "9" * 400, "not-a-time", ""],
    ids=["huge-future", "huge-past", "text", "empty"],
)
async def test_invalid_callback_timestamp_is_401_without_database(timestamp):
    settings = Settings(
        database_url="postgresql+asyncpg://test:test@127.0.0.1:1/test",
        callback_secrets={"A": "a" * 32},
        _env_file=None,
    )
    app = create_app(settings)
    app.state.settings = settings
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://executor",
    ) as client:
        response = await client.post(
            "/callbacks/A",
            content=b"{}",
            headers={"X-Callback-Timestamp": timestamp, "X-Callback-Signature": "invalid"},
        )
    assert response.status_code == 401
