import os
import subprocess
import sys
from collections.abc import AsyncIterator
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest
from payment.config import Settings
from payment.db.session import Database
from pydantic import SecretStr
from sqlalchemy.engine import make_url

ROOT = Path(__file__).resolve().parents[3]


def run_alembic(database_url: str, *arguments: str) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "PAYMENT_DATABASE_URL": database_url}
    result = subprocess.run(
        [sys.executable, "-m", "alembic", *arguments],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return result


@pytest.fixture
async def empty_database() -> AsyncIterator[str]:
    maintenance_url = os.getenv("TEST_DATABASE_URL")
    if not maintenance_url:
        pytest.skip("Set TEST_DATABASE_URL to run PostgreSQL integration tests")
    url = make_url(maintenance_url)
    admin = await asyncpg.connect(
        url.set(drivername="postgresql").render_as_string(hide_password=False),
        timeout=5,
    )
    name = "tbpo5_test_" + uuid4().hex
    try:
        await admin.execute(f'CREATE DATABASE "{name}"')
        try:
            yield url.set(database=name).render_as_string(hide_password=False)
        finally:
            # Only this fixture's randomly named database is removed.
            await admin.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = $1 AND pid <> pg_backend_pid()",
                name,
            )
            await admin.execute(f'DROP DATABASE "{name}"')
    finally:
        await admin.close()


@pytest.fixture
async def migrated_database(empty_database: str) -> AsyncIterator[Database]:
    run_alembic(empty_database, "upgrade", "head")
    database = Database(Settings(database_url=SecretStr(empty_database), _env_file=None))
    try:
        yield database
    finally:
        await database.close()
