"""Development-only channel A. Synthetic ledger, no connection to real payments."""

import hashlib
import json
import sqlite3
import time
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Literal
from uuid import UUID, uuid4

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, ConfigDict, Field, StringConstraints
from pydantic_settings import BaseSettings, SettingsConfigDict

from executor.channel_a import ChannelReply


class DemoSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DEMO_CHANNEL_", extra="ignore")
    database: Path = Path("/tmp/tbank-channel-a.sqlite3")
    delay_seconds: float = Field(default=8, ge=0, le=30)


class LedgerRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    attempt_id: UUID
    payment_id: UUID
    channel_id: Literal["A", "B", "C"]
    amount_minor: int = Field(strict=True, gt=0, le=9223372036854775807)
    currency: Literal["RUB"]
    recipient: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class ChannelRequest(LedgerRequest):
    channel_id: Literal["A"]


class DemoStore:
    def __init__(self, path: Path):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(path, timeout=5)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS operations ("
                "attempt_id TEXT PRIMARY KEY, request_hash TEXT NOT NULL, "
                "request_json TEXT NOT NULL, result_json TEXT NOT NULL, "
                "charged_amount_minor INTEGER NOT NULL CHECK(charged_amount_minor >= 0))"
            )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS callback_outbox ("
                "attempt_id TEXT PRIMARY KEY, event_id TEXT NOT NULL UNIQUE, "
                "payload_json TEXT NOT NULL, available_at REAL NOT NULL, delivered_at REAL)"
            )
            conn.commit()

    def execute(
        self, request: LedgerRequest, *, callback_delay: float | None = None
    ) -> ChannelReply:
        body = json.dumps(request.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        body_hash = hashlib.sha256(body.encode()).hexdigest()
        with closing(sqlite3.connect(self.path, timeout=5, isolation_level=None)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT request_hash,result_json FROM operations WHERE attempt_id=?",
                    (str(request.attempt_id),),
                ).fetchone()
                if row:
                    if row[0] != body_hash:
                        raise HTTPException(409, "Attempt ID already used with different data")
                    result = ChannelReply.model_validate_json(row[1])
                else:
                    declined = request.recipient == "demo-decline"
                    result = ChannelReply(
                        attempt_id=request.attempt_id,
                        outcome="failed" if declined else "succeeded",
                        reason_code="DEMO_DECLINED" if declined else "DEMO_APPROVED",
                        provider_operation_id=str(uuid4()),
                    )
                    conn.execute(
                        "INSERT INTO operations VALUES (?,?,?,?,?)",
                        (
                            str(request.attempt_id),
                            body_hash,
                            body,
                            result.model_dump_json(),
                            0 if declined else request.amount_minor,
                        ),
                    )
                if callback_delay is not None:
                    payload = {
                        "event_id": str(uuid4()),
                        "occurred_at": datetime.now(UTC).isoformat(),
                        "payment_id": str(request.payment_id),
                        "attempt_id": str(request.attempt_id),
                        "channel_id": request.channel_id,
                        "outcome": result.outcome,
                        "reason_code": result.reason_code,
                        "provider_operation_id": result.provider_operation_id,
                    }
                    conn.execute(
                        "INSERT INTO callback_outbox "
                        "(attempt_id,event_id,payload_json,available_at) VALUES (?,?,?,?) "
                        "ON CONFLICT(attempt_id) DO NOTHING",
                        (
                            str(request.attempt_id),
                            payload["event_id"],
                            json.dumps(payload),
                            time.time() + callback_delay,
                        ),
                    )
                conn.commit()
                return result
            except BaseException:
                conn.rollback()
                raise

    def read(self, attempt_id: UUID) -> ChannelReply:
        with closing(sqlite3.connect(self.path, timeout=5)) as conn:
            row = conn.execute(
                "SELECT result_json FROM operations WHERE attempt_id=?", (str(attempt_id),)
            ).fetchone()
        if row is None:
            raise HTTPException(404, "Operation not found")
        return ChannelReply.model_validate_json(row[0])

    def pending_callback(self) -> tuple[str, str] | None:
        with closing(sqlite3.connect(self.path, timeout=5)) as conn:
            row = conn.execute(
                "SELECT attempt_id,payload_json FROM callback_outbox "
                "WHERE delivered_at IS NULL AND available_at<=? ORDER BY available_at LIMIT 1",
                (time.time(),),
            ).fetchone()
        if row is None:
            return None
        return str(row[0]), str(row[1])

    def mark_callback_delivered(self, attempt_id: str) -> None:
        with closing(sqlite3.connect(self.path, timeout=5)) as conn:
            conn.execute(
                "UPDATE callback_outbox SET delivered_at=? "
                "WHERE attempt_id=? AND delivered_at IS NULL",
                (time.time(), attempt_id),
            )
            conn.commit()

    def retry_callback(self, attempt_id: str, delay: float) -> None:
        with closing(sqlite3.connect(self.path, timeout=5)) as conn:
            conn.execute(
                "UPDATE callback_outbox SET available_at=? "
                "WHERE attempt_id=? AND delivered_at IS NULL",
                (time.time() + delay, attempt_id),
            )
            conn.commit()


def create_app(settings: DemoSettings | None = None) -> FastAPI:
    settings = settings or DemoSettings()
    store = DemoStore(settings.database)
    app = FastAPI(title="Synthetic channel A", version="0.1.0")

    @app.get("/health/ready")
    def ready() -> dict[str, str]:
        with closing(sqlite3.connect(settings.database, timeout=5)) as conn:
            conn.execute("SELECT count(*) FROM operations").fetchone()
        return {"status": "ok"}

    @app.post("/attempts", response_model=ChannelReply)
    def execute(
        request: ChannelRequest, key: Annotated[str, Header(alias="Idempotency-Key")]
    ) -> ChannelReply:
        if key != str(request.attempt_id):
            raise HTTPException(422, "Idempotency-Key must equal attempt_id")
        result = store.execute(request)
        # The ledger is committed before delaying the response: simulate a lost acknowledgement.
        if request.recipient == "demo-timeout":
            time.sleep(settings.delay_seconds)
        return result

    @app.get("/attempts/{attempt_id}", response_model=ChannelReply)
    def read(attempt_id: UUID) -> ChannelReply:
        return store.read(attempt_id)

    return app
