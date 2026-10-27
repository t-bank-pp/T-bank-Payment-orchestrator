import json
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from contracts import (
    ATTEMPT_COMMANDS,
    ExecuteAttempt,
    ExecuteAttemptData,
    decode_execute_attempt,
    encode,
)

EXAMPLE_PATH = Path(__file__).resolve().parent.parent / "examples" / "execute_attempt.json"


def load_example() -> dict[str, Any]:
    payload: dict[str, Any] = json.loads(EXAMPLE_PATH.read_text(encoding="utf-8"))
    return payload


def to_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload).encode("utf-8")


def make_data() -> ExecuteAttemptData:
    return ExecuteAttemptData(
        channel_id="A",
        amount_minor=150000,
        currency="RUB",
        recipient="recipient-001",
    )


def test_topic_name() -> None:
    assert ATTEMPT_COMMANDS == "attempt.commands"


def test_example_is_valid() -> None:
    message = decode_execute_attempt(EXAMPLE_PATH.read_bytes())

    assert message.type == "ExecuteAttempt"
    assert message.version == 1
    assert message.correlation_id == message.payment_id
    assert message.data.channel_id == "A"
    assert message.data.amount_minor == 150000
    assert message.data.currency == "RUB"
    assert message.data.recipient == "recipient-001"
    assert message.occurred_at.utcoffset() is not None


def test_round_trip_preserves_model() -> None:
    original = ExecuteAttempt.new(payment_id=uuid4(), attempt_id=uuid4(), data=make_data())

    restored = decode_execute_attempt(encode(original))

    assert restored == original
    assert restored.event_id == original.event_id
    assert restored.attempt_id == original.attempt_id
    assert restored.payment_id == original.payment_id


def test_encode_format() -> None:
    message = decode_execute_attempt(EXAMPLE_PATH.read_bytes())

    payload = json.loads(encode(message).decode("utf-8"))

    assert payload["event_id"] == str(message.event_id)
    assert payload["payment_id"] == str(message.payment_id)
    assert payload["occurred_at"] == "2026-10-26T12:00:00Z"


def test_reencoding_is_stable() -> None:
    message = ExecuteAttempt.new(payment_id=uuid4(), attempt_id=uuid4(), data=make_data())

    first = encode(message)
    second = encode(decode_execute_attempt(first))

    assert first == second


def test_new_generates_identifiers() -> None:
    payment_id = uuid4()
    attempt_id = uuid4()

    first = ExecuteAttempt.new(payment_id=payment_id, attempt_id=attempt_id, data=make_data())
    second = ExecuteAttempt.new(payment_id=payment_id, attempt_id=attempt_id, data=make_data())

    assert isinstance(first.event_id, UUID)
    assert first.event_id.version == 4
    assert first.event_id != second.event_id
    assert first.correlation_id == payment_id
    assert first.attempt_id == attempt_id
    assert second.attempt_id == attempt_id
    assert first.occurred_at.utcoffset() is not None


def test_new_keeps_explicit_correlation_id() -> None:
    correlation_id = uuid4()

    message = ExecuteAttempt.new(
        payment_id=uuid4(),
        attempt_id=uuid4(),
        data=make_data(),
        correlation_id=correlation_id,
    )

    assert message.correlation_id == correlation_id


@pytest.mark.parametrize(
    "field",
    [
        "event_id",
        "occurred_at",
        "payment_id",
        "attempt_id",
        "correlation_id",
        "type",
        "version",
        "data",
    ],
)
def test_missing_required_field_is_rejected(field: str) -> None:
    payload = load_example()
    del payload[field]

    with pytest.raises(ValidationError):
        decode_execute_attempt(to_bytes(payload))


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("data", "amount_minor"), "150000"),
        (("data", "amount_minor"), 1500.5),
        (("data", "amount_minor"), True),
        (("data", "amount_minor"), 0),
        (("data", "amount_minor"), -1),
        (("data", "currency"), "USD"),
        (("type",), "Other"),
        (("version",), 2),
        (("event_id",), "not-a-uuid"),
        (("occurred_at",), "2026-10-26T12:00:00"),
        (("data", "recipient"), ""),
        (("data", "recipient"), "   "),
        (("data", "channel_id"), ""),
        (("data", "channel_id"), "   "),
    ],
)
def test_invalid_value_is_rejected(path: tuple[str, ...], value: Any) -> None:
    payload = load_example()
    target = payload
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value

    with pytest.raises(ValidationError):
        decode_execute_attempt(to_bytes(payload))


def test_unknown_top_level_field_is_rejected() -> None:
    payload = load_example()
    payload["unexpected"] = "value"

    with pytest.raises(ValidationError):
        decode_execute_attempt(to_bytes(payload))


def test_unknown_data_field_is_rejected() -> None:
    payload = load_example()
    payload["data"]["unexpected"] = "value"

    with pytest.raises(ValidationError):
        decode_execute_attempt(to_bytes(payload))


def test_model_is_immutable() -> None:
    message = decode_execute_attempt(EXAMPLE_PATH.read_bytes())

    with pytest.raises(ValidationError):
        message.attempt_id = uuid4()  # type: ignore[misc]

    with pytest.raises(ValidationError):
        message.data.amount_minor = 1  # type: ignore[misc]
