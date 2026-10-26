from contracts.commands import ExecuteAttempt
from contracts.envelope import Envelope


def encode(message: Envelope) -> bytes:
    return message.model_dump_json().encode("utf-8")


def decode_execute_attempt(raw: bytes) -> ExecuteAttempt:
    return ExecuteAttempt.model_validate_json(raw)
