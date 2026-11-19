import asyncio
import hashlib
import hmac
import json
import time
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError, field_validator
from sqlalchemy.dialects.postgresql import insert

from executor.channel_a import ExecutionResult
from executor.config import Settings
from executor.confirmation import ConfirmationConflict, apply_confirmation
from executor.db.models import CallbackReceipt
from executor.db.session import Database

router = APIRouter()


class Callback(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    event_id: UUID
    occurred_at: AwareDatetime
    payment_id: UUID
    attempt_id: UUID
    channel_id: str = Field(min_length=1, max_length=100)
    outcome: Literal["succeeded", "failed"]
    reason_code: str = Field(min_length=1, max_length=100)
    provider_operation_id: str = Field(min_length=1, max_length=255)

    @field_validator("channel_id", "reason_code", "provider_operation_id")
    @classmethod
    def reject_nul(cls, value: str) -> str:
        if "\x00" in value:
            raise ValueError("NUL is not supported in callback text fields")
        return value

    @field_validator("occurred_at")
    @classmethod
    def as_utc(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)


def sign_callback(secret: str, timestamp: str, body: bytes) -> str:
    return hmac.new(secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()


async def accept_callback(database: Database, callback: Callback) -> bool:
    payload = callback.model_dump(mode="json")
    payload_hash = hashlib.sha256(
        json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()
    async with database.sessions() as session, session.begin():
        # Validate the operation before inserting its FK-backed receipt.
        applied = await apply_confirmation(
            session,
            attempt_id=callback.attempt_id,
            payment_id=callback.payment_id,
            channel_id=callback.channel_id,
            result=ExecutionResult(
                callback.outcome, callback.reason_code, callback.provider_operation_id, payload
            ),
        )
        inserted = await session.scalar(
            insert(CallbackReceipt)
            .values(
                event_id=callback.event_id,
                attempt_id=callback.attempt_id,
                payload_hash=payload_hash,
                payload=payload,
            )
            .on_conflict_do_nothing(index_elements=[CallbackReceipt.event_id])
            .returning(CallbackReceipt.event_id)
        )
        if inserted is None:
            previous = await session.get(CallbackReceipt, callback.event_id)
            if previous is None or previous.payload_hash != payload_hash:
                raise ConfirmationConflict("CALLBACK_EVENT_CONFLICT")
    return applied


@router.post("/callbacks/{channel_id}", tags=["Callbacks"])
async def receive_callback(channel_id: str, request: Request) -> dict[str, object]:
    settings: Settings = request.app.state.settings
    secret = settings.callback_secrets.get(channel_id)
    if secret is None:
        raise HTTPException(503, "Callbacks are not configured for this channel")
    timestamp = request.headers.get("X-Callback-Timestamp", "")
    signature = request.headers.get("X-Callback-Signature", "")
    try:
        signed_at = int(timestamp)
    except ValueError:
        raise HTTPException(401, "Invalid callback signature") from None
    if abs(time.time() - signed_at) > settings.callback_max_age:
        raise HTTPException(401, "Callback timestamp is outside the accepted window")
    body = b""
    async for chunk in request.stream():
        body += chunk
        if len(body) > 16384:
            raise HTTPException(413, "Callback body is too large")
    expected = sign_callback(secret.get_secret_value(), timestamp, body)
    if not hmac.compare_digest(signature.encode(), expected.encode()):
        raise HTTPException(401, "Invalid callback signature")
    try:
        callback = Callback.model_validate_json(body)
    except ValidationError:
        raise HTTPException(422, "Invalid callback payload") from None
    if callback.channel_id != channel_id:
        raise HTTPException(409, "Callback channel does not match URL")
    database: Database = request.app.state.database
    try:
        async with asyncio.timeout(settings.execution_database_timeout):
            applied = await accept_callback(database, callback)
    except ConfirmationConflict as exc:
        code = 404 if str(exc) == "ATTEMPT_NOT_FOUND" else 409
        raise HTTPException(code, str(exc)) from None
    return {"status": "accepted", "applied": applied}
