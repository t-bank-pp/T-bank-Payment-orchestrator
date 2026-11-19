from typing import Literal
from uuid import UUID

import httpx
from contracts import ExecuteAttempt
from pydantic import BaseModel, ConfigDict, Field

from executor.http_channel import ExecutionResult as ExecutionResult
from executor.http_channel import HttpChannel


class ChannelReply(BaseModel):
    model_config = ConfigDict(extra="forbid")
    attempt_id: UUID
    outcome: Literal["succeeded", "failed", "unknown", "pending"]
    reason_code: str = Field(min_length=1, max_length=100)
    provider_operation_id: str = Field(min_length=1, max_length=255)


class ChannelA(HttpChannel):
    """Adapter for the documented synthetic channel contract, not a bank API."""

    async def execute(self, command: ExecuteAttempt) -> ExecutionResult:
        body = {
            **command.data.model_dump(mode="json"),
            "payment_id": str(command.payment_id),
            "attempt_id": str(command.attempt_id),
        }
        return await self._request(
            command,
            "POST",
            "/attempts",
            body=body,
            headers={"Idempotency-Key": str(command.attempt_id)},
        )

    async def status(self, command: ExecuteAttempt) -> ExecutionResult:
        return await self._request(command, "GET", f"/attempts/{command.attempt_id}")

    def _read_reply(self, command: ExecuteAttempt, response: httpx.Response) -> ExecutionResult:
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
            outcome, reply.reason_code, reply.provider_operation_id, reply.model_dump(mode="json")
        )
