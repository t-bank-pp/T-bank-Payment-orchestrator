from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field

PaymentStatus = Literal["routing", "in_progress", "pending", "succeeded", "failed", "needs_review"]
AttemptOutcome = Literal["succeeded", "failed", "unknown"]


class AttemptResponse(BaseModel):
    attempt_id: UUID
    number: int = Field(gt=0)
    channel_id: str
    outcome: AttemptOutcome | None = Field(
        description="Результат попытки; null, если событие с результатом ещё не получено."
    )


class PaymentResponse(BaseModel):
    payment_id: UUID
    amount_minor: int = Field(gt=0, description="Сумма в копейках, целое число.")
    currency: str
    recipient: str
    status: PaymentStatus
    attempts: list[AttemptResponse] = Field(description="Попытки по возрастанию number.")


class ErrorDetail(BaseModel):
    code: Literal["PAYMENT_NOT_FOUND"]
    message: str


class ErrorResponse(BaseModel):
    error: ErrorDetail
