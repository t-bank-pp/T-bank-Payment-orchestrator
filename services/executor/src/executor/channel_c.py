from typing import Literal
from uuid import UUID

import httpx
from contracts import ExecuteAttempt
from pydantic import BaseModel, ConfigDict, Field

from executor.http_channel import ExecutionResult, HttpChannel


class JobReply(BaseModel):
    model_config = ConfigDict(extra="forbid")

    attempt_id: UUID
    job_id: str = Field(min_length=1, max_length=255)
    phase: Literal["accepted", "pending", "settled", "rejected"]
    reason: str = Field(min_length=1, max_length=100)


class ChannelC(HttpChannel):
    async def execute(self, command: ExecuteAttempt) -> ExecutionResult:
        return await self._request(
            command,
            "POST",
            "/jobs",
            body={
                "reference": {
                    "attempt_id": str(command.attempt_id),
                    "payment_id": str(command.payment_id),
                },
                "value": {"minor": command.data.amount_minor, "currency": command.data.currency},
                "recipient": command.data.recipient,
            },
            headers={"Idempotency-Key": str(command.attempt_id)},
        )

    async def status(self, command: ExecuteAttempt) -> ExecutionResult:
        return await self._request(command, "GET", f"/jobs/by-attempt/{command.attempt_id}")

    def _read_reply(self, command: ExecuteAttempt, response: httpx.Response) -> ExecutionResult:
        if response.status_code not in {200, 202}:
            return ExecutionResult("unknown", "CHANNEL_HTTP_ERROR")
        reply = JobReply.model_validate_json(response.content)
        if reply.attempt_id != command.attempt_id:
            return ExecutionResult("unknown", "CHANNEL_ATTEMPT_MISMATCH")
        outcome: Literal["succeeded", "failed", "unknown"] = "unknown"
        if reply.phase == "settled":
            outcome = "succeeded"
        elif reply.phase == "rejected":
            outcome = "failed"
        return ExecutionResult(outcome, reply.reason, reply.job_id, reply.model_dump(mode="json"))
