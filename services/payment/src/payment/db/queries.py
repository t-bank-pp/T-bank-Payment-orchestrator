from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from payment.db.models import Payment, PaymentAttempt


async def find_payment_with_attempts(
    session: AsyncSession, payment_id: UUID
) -> tuple[Payment, list[PaymentAttempt]] | None:
    # One statement gives a consistent snapshot of the payment and its attempts.
    statement = (
        select(Payment, PaymentAttempt)
        .outerjoin(PaymentAttempt, PaymentAttempt.payment_id == Payment.id)
        .where(Payment.id == payment_id)
        .order_by(PaymentAttempt.number)
    )
    rows = (await session.execute(statement)).tuples().all()
    if not rows:
        return None
    return rows[0][0], [attempt for _, attempt in rows if attempt is not None]
