from collections.abc import AsyncIterator, Mapping
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Protocol

import httpx
from contracts import ExecuteAttempt

from executor.channel_a import ChannelA
from executor.channel_b import ChannelB
from executor.channel_c import ChannelC
from executor.config import Settings
from executor.http_channel import ExecutionResult


class ChannelAdapter(Protocol):
    async def execute(self, command: ExecuteAttempt) -> ExecutionResult: ...

    async def status(self, command: ExecuteAttempt) -> ExecutionResult: ...


def select_channel[T](source: T | Mapping[str, T], channel_id: str) -> T | None:
    if isinstance(source, Mapping):
        return source.get(channel_id)
    # Existing callers supplying one adapter keep their channel A behavior.
    if channel_id == "A":
        return source
    return None


@asynccontextmanager
async def channel_clients(
    settings: Settings, timeout: float
) -> AsyncIterator[dict[str, ChannelAdapter]]:
    async with AsyncExitStack() as stack:
        clients = []
        for url in (
            settings.channel_a_base_url,
            settings.channel_b_base_url,
            settings.channel_c_base_url,
        ):
            clients.append(
                await stack.enter_async_context(
                    httpx.AsyncClient(base_url=str(url), follow_redirects=False, trust_env=False)
                )
            )
        yield {
            "A": ChannelA(clients[0], timeout),
            "B": ChannelB(clients[1], timeout),
            "C": ChannelC(clients[2], timeout),
        }
