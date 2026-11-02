from sqlalchemy import ScalarResult, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from payment.db.models import Payment


async def insert_payment_if_new(
    session: AsyncSession,
    *,
    idempotency_key: str,
    request_hash: str,
    amount_minor: int,
    currency: str,
    recipient: str,
) -> Payment | None:
    # A concurrent insert of the same key waits for its transaction to finish.
    statement = (
        insert(Payment)
        .values(
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            amount_minor=amount_minor,
            currency=currency,
            recipient=recipient,
            status="in_progress",
        )
        .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
        .returning(Payment)
    )
    result: ScalarResult[Payment] = await session.scalars(statement)
    return result.one_or_none()


async def find_payment_by_key(session: AsyncSession, idempotency_key: str) -> Payment | None:
    statement = select(Payment).where(Payment.idempotency_key == idempotency_key)
    result: ScalarResult[Payment] = await session.scalars(statement)
    return result.one_or_none()
