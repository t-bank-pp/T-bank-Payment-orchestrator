import asyncio
import logging
import signal
import time
from pathlib import Path

import httpx
from pydantic import AnyHttpUrl, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from executor.callbacks import sign_callback
from executor.demo_channel import DemoStore

logger = logging.getLogger(__name__)


class CallbackDeliverySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DEMO_CALLBACK_", extra="ignore")
    database: Path = Path("/tmp/tbank-channel-c.sqlite3")
    url: AnyHttpUrl = AnyHttpUrl("http://127.0.0.1:8001/callbacks/C")
    secret: SecretStr = Field(min_length=32)
    request_timeout: float = Field(default=5, gt=0, le=30)
    retry_interval: float = Field(default=1, gt=0, le=60)
    poll_interval: float = Field(default=0.5, gt=0, le=30)


async def deliver_next(
    store: DemoStore, client: httpx.AsyncClient, settings: CallbackDeliverySettings
) -> bool:
    pending = await asyncio.to_thread(store.pending_callback)
    if pending is None:
        return False
    attempt_id, payload = pending
    body, timestamp = payload.encode(), str(int(time.time()))
    accepted = False
    try:
        async with asyncio.timeout(settings.request_timeout):
            response = await client.post(
                str(settings.url),
                content=body,
                timeout=settings.request_timeout,
                headers={
                    "Content-Type": "application/json",
                    "X-Callback-Timestamp": timestamp,
                    "X-Callback-Signature": sign_callback(
                        settings.secret.get_secret_value(), timestamp, body
                    ),
                },
            )
            accepted = response.status_code == 200
    except (TimeoutError, httpx.RequestError):
        logger.warning("Demo callback delivery uncertain: attempt_id=%s", attempt_id)
    if accepted:
        await asyncio.to_thread(store.mark_callback_delivered, attempt_id)
    else:
        await asyncio.to_thread(store.retry_callback, attempt_id, settings.retry_interval)
    return True


async def main() -> None:
    settings = CallbackDeliverySettings()
    store = DemoStore(settings.database)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        async with httpx.AsyncClient(follow_redirects=False, trust_env=False) as client:
            while not stop.is_set():
                try:
                    worked = await deliver_next(store, client, settings)
                except Exception as exc:
                    logger.warning("Demo callback cycle failed: %s", type(exc).__name__)
                    worked = False
                if not worked:
                    try:
                        async with asyncio.timeout(settings.poll_interval):
                            await stop.wait()
                    except TimeoutError:
                        pass
    finally:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
