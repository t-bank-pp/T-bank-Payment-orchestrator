from functools import lru_cache

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PAYMENT_", env_file=".env", extra="ignore")

    database_url: SecretStr
    app_name: str = "Payment service"
    database_connect_timeout: float = Field(default=3, gt=0, le=30)
    kafka_bootstrap_servers: str = Field(default="127.0.0.1:19092", min_length=1)
    publisher_send_timeout: float = Field(default=5, gt=0, le=60)
    publisher_poll_interval: float = Field(default=0.5, gt=0, le=60)
    publisher_retry_interval: float = Field(default=1, gt=0, le=60)
    publisher_shutdown_timeout: float = Field(default=5, gt=0, le=60)

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, value: SecretStr) -> SecretStr:
        try:
            url = make_url(value.get_secret_value())
        except ArgumentError:
            raise ValueError("Expected a postgresql+asyncpg database URL") from None
        if url.drivername != "postgresql+asyncpg" or not url.host or not url.database:
            raise ValueError("Expected postgresql+asyncpg://user:password@host:port/database")
        return value


@lru_cache
def get_settings() -> Settings:
    return Settings()
