import asyncio
from dataclasses import dataclass
from typing import Any, Literal
from uuid import UUID

import httpx
from contracts import ExecuteAttempt
from pydantic import BaseModel, ConfigDict, Field, ValidationError


@dataclass(frozen=True)
class ExecutionResult:
    outcome: Literal["succeeded", "failed", "unknown"]
    reason_code: str
    provider_operation_id: str | None = None
    payload: dict[str, Any] | None = None


class ChannelReply(BaseModel):
    model_config = ConfigDict(extra="forbid")
    attempt_id: UUID
    outcome: Literal["succeeded", "failed", "unknown", "pending"]
    reason_code: str = Field(min_length=1, max_length=100)
    provider_operation_id: str = Field(min_length=1, max_length=255)


class ChannelA:
    """Adapter for the documented synthetic channel contract, not a bank API."""

    def __init__(self, client: httpx.AsyncClient, timeout: float):
        self.client, self.timeout = client, timeout

    async def execute(self, command: ExecuteAttempt) -> ExecutionResult:
        body = {
            **command.data.model_dump(mode="json"),
            "payment_id": str(command.payment_id),
            "attempt_id": str(command.attempt_id),
        }
        try:
            # A total deadline in addition to HTTPX's per-phase limits.
            async with asyncio.timeout(self.timeout):
                response = await self.client.post(
                    "/attempts",
                    json=body,
                    headers={"Idempotency-Key": str(command.attempt_id)},
                    timeout=self.timeout,
                )
            if response.status_code != 200:
                return ExecutionResult("unknown", "CHANNEL_HTTP_ERROR")
            reply = ChannelReply.model_validate_json(response.content)
            if reply.attempt_id != command.attempt_id:
                return ExecutionResult("unknown", "CHANNEL_ATTEMPT_MISMATCH")
            outcome: Literal["succeeded", "failed", "unknown"] = "unknown"
            if reply.outcome == "succeeded":
                outcome = "succeeded"
            elif reply.outcome == "failed":
                outcome = "failed"
            return ExecutionResult(
                outcome,
                reply.reason_code,
                reply.provider_operation_id,
                reply.model_dump(mode="json"),
            )
        except (TimeoutError, httpx.TimeoutException):
            return ExecutionResult("unknown", "CHANNEL_TIMEOUT")
        except httpx.RequestError:
            return ExecutionResult("unknown", "CHANNEL_UNAVAILABLE")
        except ValidationError:
            return ExecutionResult("unknown", "CHANNEL_RESPONSE_INVALID")
