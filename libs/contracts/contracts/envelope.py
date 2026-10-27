from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict


class Envelope(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    event_id: UUID
    occurred_at: AwareDatetime
    payment_id: UUID
    attempt_id: UUID
    correlation_id: UUID
