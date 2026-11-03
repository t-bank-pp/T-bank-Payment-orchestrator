from functools import lru_cache

from pydantic import AnyHttpUrl, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="EXECUTOR_", env_file=".env", extra="ignore")

    database_url: SecretStr
    app_name: str = "Executor service"
    database_connect_timeout: float = Field(default=3, gt=0, le=30)
    kafka_bootstrap_servers: str = Field(default="127.0.0.1:19092", min_length=1)
    consumer_group: str = Field(default="executor-attempts-v1", min_length=1)
    consumer_request_timeout: float = Field(default=5, ge=1, le=60)
    consumer_processing_timeout: float = Field(default=10, gt=0, le=60)
    consumer_poll_interval: float = Field(default=1, gt=0, le=5)
    consumer_retry_interval: float = Field(default=1, gt=0, le=60)
    consumer_shutdown_timeout: float = Field(default=5, gt=0, le=60)
    channel_a_base_url: AnyHttpUrl = AnyHttpUrl("http://127.0.0.1:18080")
    execution_request_timeout: float = Field(default=5, gt=0, le=30)
    execution_database_timeout: float = Field(default=5, gt=0, le=30)
    execution_lease_seconds: float = Field(default=60, gt=0, le=600)
    execution_poll_interval: float = Field(default=0.5, gt=0, le=30)

    @model_validator(mode="after")
    def validate_execution_lease(self) -> "Settings":
        if self.execution_lease_seconds <= (
            self.execution_request_timeout + 2 * self.execution_database_timeout
        ):
            raise ValueError("Execution lease must exceed request timeout plus two DB timeouts")
        return self

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
