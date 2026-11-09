from datetime import UTC, datetime
from typing import Literal, Self
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

from contracts.commands import ExecuteAttempt, NonBlankStr
from contracts.envelope import Envelope

type AttemptOutcome = Literal["succeeded", "failed", "unknown"]


class AttemptResultData(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    channel_id: NonBlankStr
    outcome: AttemptOutcome
    reason_code: str = Field(min_length=1, max_length=100)
    provider_operation_id: str | None = Field(default=None, min_length=1, max_length=255)


class AttemptResult(Envelope):
    type: Literal["AttemptResult"]
    version: Literal[1]
    data: AttemptResultData

    @field_validator("version", mode="before")
    @classmethod
    def require_integer_version(cls, value: object) -> object:
        if type(value) is not int:
            raise ValueError("version must be an integer")
        return value

    @field_validator("occurred_at")
    @classmethod
    def as_utc(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)

    @classmethod
    def new(
        cls,
        command: ExecuteAttempt,
        *,
        occurred_at: datetime,
        outcome: AttemptOutcome,
        reason_code: str,
        provider_operation_id: str | None = None,
    ) -> Self:
        return cls(
            event_id=uuid4(),
            occurred_at=occurred_at,
            payment_id=command.payment_id,
            attempt_id=command.attempt_id,
            correlation_id=command.correlation_id,
            type="AttemptResult",
            version=1,
            data=AttemptResultData(
                channel_id=command.data.channel_id,
                outcome=outcome,
                reason_code=reason_code,
                provider_operation_id=provider_operation_id,
            ),
        )
