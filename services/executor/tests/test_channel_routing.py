import pytest
from executor.execution import execute_next

from .test_consumer import settings
from .test_execution import state
from .test_extra_channels import extra_command
from .test_registration import accept

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("channel", ["B", "UNSUPPORTED"])
async def test_route_is_bound_to_command_and_unknown_does_not_start_another_call(
    executor_database, channel
):
    db = executor_database
    cmd = extra_command(channel)
    await accept(db, cmd)
    calls = []

    class Adapter:
        def __init__(self, name):
            self.name = name

        async def execute(self, command):
            calls.append(self.name)
            raise TimeoutError("provider outcome is unknown")

    adapters = {"A": Adapter("A"), "B": Adapter("B")}
    assert await execute_next(db, adapters, settings())
    assert not await execute_next(db, adapters, settings())
    assert calls == (["B"] if channel == "B" else [])
    assert (await state(db, cmd.attempt_id)).state == "unknown"
