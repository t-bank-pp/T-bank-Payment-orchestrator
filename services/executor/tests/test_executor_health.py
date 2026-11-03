import httpx
import pytest
from executor.config import Settings
from executor.main import create_app


@pytest.mark.integration
async def test_live_and_ready_with_own_database(executor_database):
    app = create_app(
        Settings(
            database_url=executor_database.engine.url.render_as_string(hide_password=False),
            _env_file=None,
        )
    )
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://executor"
        ) as client:
            assert (await client.get("/health/live")).status_code == 200
            assert (await client.get("/health/ready")).json() == {"status": "ok"}


async def test_live_survives_unavailable_database_and_ready_is_503():
    app = create_app(
        Settings(
            database_url="postgresql+asyncpg://hidden:secret@127.0.0.1:1/unavailable",
            database_connect_timeout=0.1,
            _env_file=None,
        )
    )
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://executor"
        ) as client:
            assert (await client.get("/health/live")).status_code == 200
            response = await client.get("/health/ready")
            assert response.status_code == 503
            assert response.json() == {"status": "unavailable"}


def test_configuration_is_independent_from_payment(monkeypatch):
    monkeypatch.setenv("PAYMENT_DATABASE_URL", "not-an-executor-url")
    monkeypatch.setenv("EXECUTOR_DATABASE_URL", "postgresql+asyncpg://demo:demo@localhost/executor")
    settings = Settings(_env_file=None)
    assert settings.database_url.get_secret_value().endswith("/executor")
    assert "demo:demo" not in str(settings)
