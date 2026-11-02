import hashlib
import json
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from payment.db.payments import find_payment_by_key, insert_payment_if_new
from payment.use_cases.create_first_attempt import create_first_attempt


class IdempotencyConflict(Exception):
    pass


async def create_payment(
    session: AsyncSession,
    *,
    idempotency_key: str,
    amount_minor: int,
    currency: str,
    recipient: str,
) -> UUID:
    """Accept validated, normalized fields and commit the whole payment atomically."""
    canonical_body = json.dumps(
        {"amount_minor": amount_minor, "currency": currency, "recipient": recipient},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    request_hash = hashlib.sha256(canonical_body.encode("utf-8")).hexdigest()

    async with session.begin():
        payment = await insert_payment_if_new(
            session,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            amount_minor=amount_minor,
            currency=currency,
            recipient=recipient,
        )
        if payment is None:
            # A new statement sees the committed winner at READ COMMITTED isolation.
            existing = await find_payment_by_key(session, idempotency_key)
            if existing is None:
                raise RuntimeError("Conflicting payment disappeared")
            if existing.request_hash != request_hash:
                raise IdempotencyConflict
            return existing.id

        await create_first_attempt(session, payment)
        return payment.id
