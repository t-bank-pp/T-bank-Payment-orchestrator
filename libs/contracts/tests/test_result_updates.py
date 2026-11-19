import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from contracts import (
    AttemptResultUpdate,
    decode_attempt_result,
    decode_attempt_result_event,
    encode,
)

EXAMPLE = Path(__file__).resolve().parent.parent / "examples/attempt_result_unknown.json"


def test_v1_keeps_original_wire_representation():
    event = decode_attempt_result(EXAMPLE.read_bytes())
    assert decode_attempt_result_event(encode(event)) == event
    assert "state_version" not in json.loads(encode(event))


def test_versioned_update_round_trip():
    original = decode_attempt_result(EXAMPLE.read_bytes())
    updated = AttemptResultUpdate.from_result(original, 2)
    assert updated.version == updated.state_version == 2
    assert decode_attempt_result_event(encode(updated)) == updated
    with pytest.raises(ValidationError):
        decode_attempt_result(encode(updated))


@pytest.mark.parametrize(
    "field,value",
    [("version", True), ("version", 2.0), ("state_version", 1), ("state_version", True)],
)
def test_invalid_update_versions_are_rejected(field, value):
    original = decode_attempt_result(EXAMPLE.read_bytes())
    updated = AttemptResultUpdate.from_result(original, 2)
    with pytest.raises(ValidationError):
        decode_attempt_result_event(
            json.dumps({**json.loads(encode(updated)), field: value}).encode()
        )
