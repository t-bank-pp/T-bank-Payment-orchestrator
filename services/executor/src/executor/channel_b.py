from typing import Literal
from uuid import UUID

import httpx
from contracts import ExecuteAttempt
from pydantic import BaseModel, ConfigDict, Field

from executor.http_channel import ExecutionResult, HttpChannel


class TransferReply(BaseModel):
    model_config = ConfigDict(extra="forbid")

    transfer_ref: UUID
    transfer_state: Literal["DONE", "REJECTED", "PENDING"]
    explanation: str = Field(min_length=1, max_length=100)
    operation_ref: str = Field(min_length=1, max_length=255)


class ChannelB(HttpChannel):
    async def execute(self, command: ExecuteAttempt) -> ExecutionResult:
        return await self._request(
            command,
            "POST",
            "/transfers",
            body={
                "transfer_ref": str(command.attempt_id),
                "payment_ref": str(command.payment_id),
                "amount": {"minor": command.data.amount_minor, "currency": command.data.currency},
                "beneficiary": command.data.recipient,
            },
            headers={"X-Transfer-Key": str(command.attempt_id)},
        )

    async def status(self, command: ExecuteAttempt) -> ExecutionResult:
        return await self._request(command, "GET", f"/transfers/{command.attempt_id}")

    def _read_reply(self, command: ExecuteAttempt, response: httpx.Response) -> ExecutionResult:
        if response.status_code != 200:
            return ExecutionResult("unknown", "CHANNEL_HTTP_ERROR")
        reply = TransferReply.model_validate_json(response.content)
        if reply.transfer_ref != command.attempt_id:
            return ExecutionResult("unknown", "CHANNEL_ATTEMPT_MISMATCH")
        outcome: Literal["succeeded", "failed", "unknown"] = "unknown"
        if reply.transfer_state == "DONE":
            outcome = "succeeded"
        elif reply.transfer_state == "REJECTED":
            outcome = "failed"
        return ExecutionResult(
            outcome, reply.explanation, reply.operation_ref, reply.model_dump(mode="json")
        )
