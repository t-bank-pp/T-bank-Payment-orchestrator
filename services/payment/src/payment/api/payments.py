from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from payment.api.schemas import AttemptResponse, ErrorDetail, ErrorResponse, PaymentResponse
from payment.db.queries import find_payment_with_attempts
from payment.db.session import get_session

router = APIRouter(prefix="/payments", tags=["Payments"])


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
