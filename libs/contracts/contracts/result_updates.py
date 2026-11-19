from datetime import datetime
from typing import Annotated, Literal

from pydantic import Field, RootModel, StrictInt, field_validator

from contracts.envelope import Envelope
from contracts.events import AttemptResult, AttemptResultData


class AttemptResultUpdate(Envelope):
    """Schema v2 adds an attempt state version without changing existing v1 events."""

    type: Literal["AttemptResult"]
    version: Literal[2]
    state_version: Annotated[StrictInt, Field(ge=2)]
    data: AttemptResultData

    @field_validator("occurred_at")
    @classmethod
    def as_utc(cls, value: datetime) -> datetime:
        return AttemptResult.as_utc(value)

    @field_validator("version", mode="before")
    @classmethod
    def require_integer_version(cls, value: object) -> object:
        if type(value) is not int:
            raise ValueError("version must be an integer")
        return value

    @classmethod
    def from_result(cls, result: AttemptResult, state_version: int) -> "AttemptResultUpdate":
        return cls.model_validate(
            {**result.model_dump(), "version": 2, "state_version": state_version}
        )


type AttemptResultEvent = AttemptResult | AttemptResultUpdate


class _ResultEvent(RootModel[AttemptResultEvent]):
    pass


def decode_attempt_result_event(raw: bytes) -> AttemptResultEvent:
    return _ResultEvent.model_validate_json(raw).root
