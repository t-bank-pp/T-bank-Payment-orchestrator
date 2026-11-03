import pytest
from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError

pytestmark = pytest.mark.integration


async def test_migration_round_trip_and_metadata_match(executor_database, executor_migrate):
    db = executor_database
    url = db.engine.url.render_as_string(hide_password=False)
    executor_migrate(url, "check")
    async with db.engine.connect() as conn:
        names = await conn.run_sync(lambda c: inspect(c).get_table_names())
        assert set(names) == {"alembic_version", "executor_attempts", "inbox"}
    await db.engine.dispose()
    executor_migrate(url, "downgrade", "base")
    async with db.engine.connect() as conn:
        names = await conn.run_sync(lambda c: inspect(c).get_table_names())
        assert set(names) == {"alembic_version"}
    await db.engine.dispose()
    executor_migrate(url, "upgrade", "head")
    executor_migrate(url, "check")


async def test_inbox_rejects_orphan_attempt(executor_database):
    async with executor_database.engine.connect() as conn:
        with pytest.raises(IntegrityError):
            await conn.execute(
                text(
                    "INSERT INTO inbox (event_id, attempt_id, payload_hash, payload) "
                    "VALUES (gen_random_uuid(), gen_random_uuid(), repeat('0',64), '{}'::jsonb)"
                )
            )
