import asyncio
from uuid import uuid4

import httpx
import pytest
from executor.channel_a import ChannelA

from .test_registration import command


@pytest.mark.parametrize("outcome", ["succeeded", "failed", "pending", "unknown"])
async def test_adapter_sends_stable_attempt_key_and_validates_result(outcome):
    cmd = command()

    def handler(request):
        import json

        body = json.loads(request.content)
        assert request.headers["Idempotency-Key"] == str(cmd.attempt_id)
        assert body["attempt_id"] == str(cmd.attempt_id)
        assert body["payment_id"] == str(cmd.payment_id)
        assert body["amount_minor"] == cmd.data.amount_minor
        assert body["channel_id"] == "A"
        return httpx.Response(
            200,
            json={
                "attempt_id": str(cmd.attempt_id),
                "outcome": outcome,
                "reason_code": "RESULT",
                "provider_operation_id": "operation-1",
            },
        )

    async with httpx.AsyncClient(
        base_url="http://channel", transport=httpx.MockTransport(handler)
    ) as client:
        result = await ChannelA(client, 1).execute(cmd)
    assert result.outcome == (outcome if outcome in {"succeeded", "failed"} else "unknown")
    assert result.provider_operation_id == "operation-1"


@pytest.mark.parametrize(
    "status,body,reason",
    [
        (500, {}, "CHANNEL_HTTP_ERROR"),
        (409, {}, "CHANNEL_HTTP_ERROR"),
        (200, {}, "CHANNEL_RESPONSE_INVALID"),
        (
            200,
            {
                "attempt_id": str(uuid4()),
                "outcome": "succeeded",
                "reason_code": "OK",
                "provider_operation_id": "x",
            },
            "CHANNEL_ATTEMPT_MISMATCH",
        ),
    ],
)
async def test_uncertain_http_response_is_never_reported_as_failed(status, body, reason):
    async with httpx.AsyncClient(
        base_url="http://channel",
        transport=httpx.MockTransport(lambda _: httpx.Response(status, json=body)),
    ) as client:
        result = await ChannelA(client, 1).execute(command())
    assert result.outcome == "unknown" and result.reason_code == reason


@pytest.mark.parametrize(
    "error,reason",
    [
        (httpx.ConnectError("offline"), "CHANNEL_UNAVAILABLE"),
        (httpx.ReadTimeout("late"), "CHANNEL_TIMEOUT"),
    ],
)
async def test_network_error_is_unknown(error, reason):
    def handler(_):
        raise error

    async with httpx.AsyncClient(
        base_url="http://channel", transport=httpx.MockTransport(handler)
    ) as client:
        result = await ChannelA(client, 1).execute(command())
    assert result.outcome == "unknown" and result.reason_code == reason


async def test_total_deadline_applies_even_if_transport_does_not_enforce_timeouts():
    async def handler(_):
        await asyncio.Event().wait()

    async with httpx.AsyncClient(
        base_url="http://channel", transport=httpx.MockTransport(handler)
    ) as client:
        result = await ChannelA(client, 0.02).execute(command())
    assert result.outcome == "unknown" and result.reason_code == "CHANNEL_TIMEOUT"
