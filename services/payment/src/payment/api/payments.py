from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from payment.api.schemas import (
    AttemptResponse,
    CreatePaymentRequest,
    CreatePaymentResponse,
    ErrorDetail,
    ErrorResponse,
    IdempotencyKey,
    PaymentResponse,
)
from payment.db.queries import find_payment_with_attempts
from payment.db.session import get_session
from payment.use_cases.create_payment import IdempotencyConflict, create_payment

router = APIRouter(prefix="/payments", tags=["Payments"])


@router.post(
    "",
    status_code=202,
    response_model=CreatePaymentResponse,
    summary="Создать платёж или получить ID повторного запроса",
    responses={
        409: {"model": ErrorResponse, "description": "IDEMPOTENCY_CONFLICT"},
        500: {"model": ErrorResponse, "description": "INTERNAL_ERROR"},
    },
)
async def post_payment(
    body: CreatePaymentRequest,
    idempotency_key: Annotated[IdempotencyKey, Header(alias="Idempotency-Key")],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> CreatePaymentResponse | JSONResponse:
    try:
        payment_id = await create_payment(
            session,
            idempotency_key=idempotency_key,
            amount_minor=body.amount_minor,
            currency=body.currency,
            recipient=body.recipient,
        )
    except IdempotencyConflict:
        error = ErrorResponse(
            error=ErrorDetail(
                code="IDEMPOTENCY_CONFLICT", message="Ключ уже использован с другими данными"
            )
        )
        return JSONResponse(status_code=409, content=error.model_dump())
    return CreatePaymentResponse(payment_id=payment_id)


@router.get(
    "/{payment_id}",
    response_model=PaymentResponse,
    summary="Получить платёж и его попытки",
    responses={404: {"model": ErrorResponse, "description": "Платёж не найден"}},
)
async def get_payment(
    payment_id: UUID,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> PaymentResponse | JSONResponse:
    result = await find_payment_with_attempts(session, payment_id)
    if result is None:
        error = ErrorResponse(
            error=ErrorDetail(code="PAYMENT_NOT_FOUND", message="Платёж не найден")
        )
        return JSONResponse(status_code=404, content=error.model_dump())

    payment, attempts = result
    return PaymentResponse.model_validate(
        {
            "payment_id": payment.id,
            "amount_minor": payment.amount_minor,
            "currency": payment.currency,
            "recipient": payment.recipient,
            "status": payment.status,
            "attempts": [
                AttemptResponse.model_validate(
                    {
                        "attempt_id": attempt.attempt_id,
                        "number": attempt.number,
                        "channel_id": attempt.channel_id,
                        "outcome": attempt.outcome,
                    }
                )
                for attempt in attempts
            ],
        }
    )
