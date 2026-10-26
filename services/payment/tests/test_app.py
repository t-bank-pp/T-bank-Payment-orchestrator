import httpx
from payment.config import Settings
from payment.db.session import Database
from payment.main import create_app


async def test_docs_and_liveness_start_without_opening_a_database_connection():
    settings = Settings(
        database_url="postgresql+asyncpg://demo:demo@127.0.0.1:1/unreachable",
        _env_file=None,
    )
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            assert (await client.get("/docs")).status_code == 200
            schema = (await client.get("/openapi.json")).json()
            assert set(schema["paths"]) == {"/health/live", "/health/ready"}
            assert (await client.get("/health/live")).json() == {"status": "ok"}


async def test_readiness_returns_503_when_database_is_unavailable(monkeypatch):
    async def unavailable(self):
        raise OSError("connection refused")

    monkeypatch.setattr(Database, "check_connection", unavailable)
    settings = Settings(
        database_url="postgresql+asyncpg://demo:demo@127.0.0.1:1/unreachable",
        _env_file=None,
    )
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.get("/health/ready")
            assert response.status_code == 503
            assert response.json() == {"status": "unavailable"}
            assert (await client.get("/health/live")).status_code == 200
