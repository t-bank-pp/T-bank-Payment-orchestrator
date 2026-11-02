from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

PaymentStatus = Literal["routing", "in_progress", "pending", "succeeded", "failed", "needs_review"]
AttemptOutcome = Literal["succeeded", "failed", "unknown"]
IdempotencyKey = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=100)
]


class CreatePaymentRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount_minor: int = Field(strict=True, gt=0, le=9223372036854775807)
    currency: Literal["RUB"]
    recipient: Annotated[
        str, StringConstraints(strip_whitespace=True, min_length=1, max_length=255)
    ]


class CreatePaymentResponse(BaseModel):
    payment_id: UUID


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
    code: Literal["PAYMENT_NOT_FOUND", "IDEMPOTENCY_CONFLICT", "INTERNAL_ERROR"]
    message: str


class ErrorResponse(BaseModel):
    error: ErrorDetail
