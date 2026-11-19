import json
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import ValidationError

from contracts import (
    ATTEMPT_EVENTS,
    AttemptResult,
    ExecuteAttempt,
    ExecuteAttemptData,
    decode_attempt_result,
    encode,
)

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"


@pytest.mark.parametrize(
    ("outcome", "reason_code"),
    [
        ("succeeded", "DEMO_APPROVED"),
        ("failed", "DEMO_DECLINED"),
        ("unknown", "CHANNEL_TIMEOUT"),
    ],
)
def test_examples_match_contract(outcome: str, reason_code: str) -> None:
    result = decode_attempt_result((EXAMPLES / f"attempt_result_{outcome}.json").read_bytes())

    assert ATTEMPT_EVENTS == "attempt.events"
    assert result.type == "AttemptResult"
    assert result.version == 1
    assert result.data.outcome == outcome
    assert result.data.reason_code == reason_code
    assert (result.data.provider_operation_id is None) == (outcome == "unknown")
    assert result.occurred_at.utcoffset() == timedelta(0)
    assert decode_attempt_result(encode(result)) == result


def test_new_result_uses_command_identity_and_finish_time() -> None:
    command = ExecuteAttempt.new(
        payment_id=uuid4(),
        attempt_id=uuid4(),
        correlation_id=uuid4(),
        data=ExecuteAttemptData(
            channel_id="A", amount_minor=150000, currency="RUB", recipient="demo"
        ),
    )
    finished_at = datetime(2026, 10, 26, 17, tzinfo=timezone(timedelta(hours=5)))
    result = AttemptResult.new(
        command,
        occurred_at=finished_at,
        outcome="unknown",
        reason_code="CHANNEL_TIMEOUT",
    )

    assert result.event_id != command.event_id
    assert result.payment_id == command.payment_id
    assert result.attempt_id == command.attempt_id
    assert result.correlation_id == command.correlation_id
    assert result.data.channel_id == command.data.channel_id
    assert result.occurred_at == finished_at
    assert result.occurred_at.tzinfo is UTC
    assert json.loads(encode(result))["occurred_at"] == "2026-10-26T12:00:00Z"


@pytest.mark.parametrize(
    "changes",
    [
        {"type": "ExecuteAttempt"},
        {"version": 2},
        {"version": True},
        {"version": 1.0},
        {"occurred_at": "2026-10-26T12:00:00"},
        {"data": {"channel_id": "A", "outcome": "pending", "reason_code": "PENDING"}},
        {"data": {"channel_id": "A", "outcome": "unknown", "reason_code": ""}},
        {
            "data": {
                "channel_id": "A",
                "outcome": "unknown",
                "reason_code": "CHANNEL_TIMEOUT",
                "provider_operation_id": "",
            },
        },
    ],
)
def test_rejects_invalid_contract(changes: dict[str, object]) -> None:
    payload = json.loads((EXAMPLES / "attempt_result_unknown.json").read_bytes())

    with pytest.raises(ValidationError):
        decode_attempt_result(json.dumps({**payload, **changes}).encode())
