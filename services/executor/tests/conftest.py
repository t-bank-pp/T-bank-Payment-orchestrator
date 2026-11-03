import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest
from executor.config import Settings
from executor.db.session import Database
from sqlalchemy.engine import make_url

ROOT = Path(__file__).resolve().parents[3]


def migrate(url, *args):
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "-c", "executor-alembic.ini", *args],
        cwd=ROOT,
        env={**os.environ, "EXECUTOR_DATABASE_URL": url},
        text=True,
        capture_output=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return result


@pytest.fixture
def executor_migrate():
    return migrate


@pytest.fixture
async def executor_database():
    maintenance_url = os.getenv("TEST_EXECUTOR_DATABASE_URL")
    if not maintenance_url:
        pytest.skip("Set TEST_EXECUTOR_DATABASE_URL to run executor PostgreSQL tests")
    url = make_url(maintenance_url)
    admin = await asyncpg.connect(
        url.set(drivername="postgresql").render_as_string(hide_password=False), timeout=5
    )
    name = "tbpo15_test_" + uuid4().hex
    database = None
    try:
        await admin.execute(f'CREATE DATABASE "{name}"')
        try:
            database_url = url.set(database=name).render_as_string(hide_password=False)
            migrate(database_url, "upgrade", "head")
            database = Database(Settings(database_url=database_url, _env_file=None))
            yield database
        finally:
            if database:
                await database.close()
            await admin.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = $1 AND pid <> pg_backend_pid()",
                name,
            )
            await admin.execute(f'DROP DATABASE "{name}"')
    finally:
        await admin.close()


@pytest.fixture
async def consumer_payment_database(executor_database):
    """A second isolated DB for the payment side of the Kafka integration check."""
    from payment.config import Settings as PaymentSettings
    from payment.db.session import Database as PaymentDatabase

    url = executor_database.engine.url
    admin = await asyncpg.connect(
        url.set(drivername="postgresql", database="postgres").render_as_string(hide_password=False)
    )
    name = "tbpo16_payment_test_" + uuid4().hex
    database = None
    try:
        await admin.execute(f'CREATE DATABASE "{name}"')
        try:
            payment_url = url.set(database=name).render_as_string(hide_password=False)
            result = subprocess.run(
                [sys.executable, "-m", "alembic", "upgrade", "head"],
                cwd=ROOT,
                env={**os.environ, "PAYMENT_DATABASE_URL": payment_url},
                text=True,
                capture_output=True,
                timeout=30,
            )
            assert result.returncode == 0, result.stdout + result.stderr
            database = PaymentDatabase(PaymentSettings(database_url=payment_url, _env_file=None))
            yield database
        finally:
            if database:
                await database.close()
            await admin.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = $1 AND pid <> pg_backend_pid()",
                name,
            )
            await admin.execute(f'DROP DATABASE "{name}"')
    finally:
        await admin.close()
