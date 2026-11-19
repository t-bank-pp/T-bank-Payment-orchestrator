import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Annotated, Literal
from uuid import UUID

from contracts.commands import NonBlankStr
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from executor.channel_b import TransferReply
from executor.channel_c import JobReply
from executor.demo_channel import DemoSettings, DemoStore, LedgerRequest


class ChannelBSettings(DemoSettings):
    database: Path = Path("/tmp/tbank-channel-b.sqlite3")


class ChannelCSettings(DemoSettings):
    database: Path = Path("/tmp/tbank-channel-c.sqlite3")
    callback_delay: float = Field(default=1, ge=0, le=30)


class Amount(BaseModel):
    model_config = ConfigDict(extra="forbid")
    minor: int = Field(strict=True, gt=0, le=9223372036854775807)
    currency: Literal["RUB"]


class TransferRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    transfer_ref: UUID
    payment_ref: UUID
    amount: Amount
    beneficiary: NonBlankStr


class JobReference(BaseModel):
    model_config = ConfigDict(extra="forbid")
    attempt_id: UUID
    payment_id: UUID


class JobRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    reference: JobReference
    value: Amount
    recipient: NonBlankStr


def demo_app(title: str, store: DemoStore) -> FastAPI:
    app = FastAPI(title=title)

    @app.get("/health/ready")
    def ready() -> dict[str, str]:
        with closing(sqlite3.connect(store.path, timeout=5)) as conn:
            conn.execute("SELECT count(*) FROM operations").fetchone()
        return {"status": "ok"}

    return app


def create_b_app(settings: DemoSettings | None = None) -> FastAPI:
    settings = settings or ChannelBSettings()
    store = DemoStore(settings.database)
    app = demo_app("Synthetic channel B", store)

    def response(attempt_id: UUID) -> TransferReply:
        result = store.read(attempt_id)
        return TransferReply.model_validate(
            {
                "transfer_ref": result.attempt_id,
                "transfer_state": "DONE" if result.outcome == "succeeded" else "REJECTED",
                "explanation": result.reason_code,
                "operation_ref": result.provider_operation_id,
            }
        )

    @app.post("/transfers", response_model=TransferReply)
    def execute(
        body: TransferRequest, key: Annotated[str, Header(alias="X-Transfer-Key")]
    ) -> TransferReply:
        if key != str(body.transfer_ref):
            raise HTTPException(422, "X-Transfer-Key must equal transfer_ref")
        store.execute(
            LedgerRequest(
                attempt_id=body.transfer_ref,
                payment_id=body.payment_ref,
                channel_id="B",
                amount_minor=body.amount.minor,
                currency=body.amount.currency,
                recipient=body.beneficiary,
            )
        )
        return response(body.transfer_ref)

    @app.get("/transfers/{attempt_id}", response_model=TransferReply)
    def status(attempt_id: UUID) -> TransferReply:
        return response(attempt_id)

    return app


def create_c_app(settings: ChannelCSettings | None = None) -> FastAPI:
    settings = settings or ChannelCSettings()
    store = DemoStore(settings.database)
    app = demo_app("Synthetic async channel C", store)

    @app.post("/jobs", status_code=202, response_model=JobReply)
    def execute(body: JobRequest, key: Annotated[str, Header(alias="Idempotency-Key")]) -> JobReply:
        if key != str(body.reference.attempt_id):
            raise HTTPException(422, "Idempotency-Key must equal attempt_id")
        result = store.execute(
            LedgerRequest(
                attempt_id=body.reference.attempt_id,
                payment_id=body.reference.payment_id,
                channel_id="C",
                amount_minor=body.value.minor,
                currency=body.value.currency,
                recipient=body.recipient,
            ),
            callback_delay=settings.callback_delay,
        )
        return JobReply(
            attempt_id=result.attempt_id,
            job_id=result.provider_operation_id,
            phase="accepted",
            reason="CHANNEL_PENDING",
        )

    @app.get("/jobs/by-attempt/{attempt_id}", response_model=JobReply)
    def status(attempt_id: UUID) -> JobReply:
        result = store.read(attempt_id)
        return JobReply(
            attempt_id=result.attempt_id,
            job_id=result.provider_operation_id,
            phase="settled" if result.outcome == "succeeded" else "rejected",
            reason=result.reason_code,
        )

    return app
