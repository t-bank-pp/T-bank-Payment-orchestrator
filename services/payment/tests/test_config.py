import pytest
from payment.config import Settings
from pydantic import ValidationError


def test_environment_overrides_dotenv(monkeypatch, tmp_path):
    dotenv = tmp_path / ".env"
    dotenv.write_text(
        "PAYMENT_DATABASE_URL=postgresql+asyncpg://demo:password@localhost/demo\n"
        "PAYMENT_APP_NAME=from-file\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("PAYMENT_DATABASE_URL", raising=False)
    monkeypatch.setenv("PAYMENT_APP_NAME", "from-environment")
    settings = Settings(_env_file=dotenv)
    assert settings.app_name == "from-environment"
    assert settings.database_url.get_secret_value().endswith("/demo")
    assert "password" not in repr(settings)


def test_database_url_is_required(monkeypatch):
    monkeypatch.delenv("PAYMENT_DATABASE_URL", raising=False)
    with pytest.raises(ValidationError):
        Settings(_env_file=None)


@pytest.mark.parametrize("url", ["sqlite:///demo", "postgresql://localhost/demo", "not-a-url"])
def test_only_asyncpg_urls_are_accepted(url):
    with pytest.raises(ValidationError):
        Settings(database_url=url, _env_file=None)
