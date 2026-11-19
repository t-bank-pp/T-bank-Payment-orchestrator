import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Literal

import httpx
from contracts import ExecuteAttempt
from pydantic import ValidationError


@dataclass(frozen=True)
class ExecutionResult:
    outcome: Literal["succeeded", "failed", "unknown"]
    reason_code: str
    provider_operation_id: str | None = None
    payload: dict[str, Any] | None = None


class HttpChannel(ABC):
    def __init__(self, client: httpx.AsyncClient, timeout: float):
        self.client, self.timeout = client, timeout

    async def _request(
        self,
        command: ExecuteAttempt,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> ExecutionResult:
        try:
            async with asyncio.timeout(self.timeout):
                response = await self.client.request(
                    method,
                    path,
                    json=body,
                    headers=headers,
                    timeout=self.timeout,
                )
            return self._read_reply(command, response)
        except (TimeoutError, httpx.TimeoutException):
            return ExecutionResult("unknown", "CHANNEL_TIMEOUT")
        except httpx.RequestError:
            return ExecutionResult("unknown", "CHANNEL_UNAVAILABLE")
        except ValidationError:
            return ExecutionResult("unknown", "CHANNEL_RESPONSE_INVALID")

    @abstractmethod
    def _read_reply(self, command: ExecuteAttempt, response: httpx.Response) -> ExecutionResult: ...
