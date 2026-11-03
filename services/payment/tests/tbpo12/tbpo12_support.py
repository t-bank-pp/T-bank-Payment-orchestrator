from payment.db.models import OutboxMessage, Payment, PaymentAttempt
from sqlalchemy import select


async def snapshot(database):
    # Each case owns an isolated migrated DB. Read committed state via another connection.
    async with database.engine.connect() as conn:
        result = []
        for model, pk in (
            (Payment, Payment.id),
            (PaymentAttempt, PaymentAttempt.attempt_id),
            (OutboxMessage, OutboxMessage.id),
        ):
            rows = await conn.execute(select(model.__table__).order_by(pk))
            result.append([dict(row) for row in rows.mappings()])
        return tuple(result)
