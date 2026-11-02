import pytest
from payment.config import Settings
from payment.db.session import Database
from pydantic import SecretStr


@pytest.fixture
def app_settings(migrated_database: Database) -> Settings:
    return Settings(
        database_url=SecretStr(migrated_database.engine.url.render_as_string(hide_password=False)),
        _env_file=None,
    )
