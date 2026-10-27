from datetime import UTC, datetime
from typing import Annotated, Literal, Self
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StringConstraints

from contracts.envelope import Envelope

NonBlankStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class ExecuteAttemptData(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    channel_id: NonBlankStr
    amount_minor: Annotated[StrictInt, Field(gt=0)]
    currency: Literal["RUB"]
    recipient: NonBlankStr


class ExecuteAttempt(Envelope):
    type: Literal["ExecuteAttempt"]
    version: Literal[1]
    data: ExecuteAttemptData

    @classmethod
    def new(
        cls,
        payment_id: UUID,
        attempt_id: UUID,
        data: ExecuteAttemptData,
        correlation_id: UUID | None = None,
    ) -> Self:
        return cls(
            event_id=uuid4(),
            occurred_at=datetime.now(UTC),
            payment_id=payment_id,
            attempt_id=attempt_id,
            correlation_id=payment_id if correlation_id is None else correlation_id,
            type="ExecuteAttempt",
            version=1,
            data=data,
        )
