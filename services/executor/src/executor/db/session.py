import asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from executor.config import Settings


class Database:
    def __init__(self, settings: Settings) -> None:
        self.connection_timeout = settings.database_connect_timeout
        self.engine = create_async_engine(
            settings.database_url.get_secret_value(),
            pool_pre_ping=True,
            connect_args={"timeout": self.connection_timeout},
        )
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)

    async def check_connection(self) -> None:
        async with asyncio.timeout(self.connection_timeout):
            async with self.engine.connect() as connection:
                await connection.execute(text("SELECT 1"))

    async def close(self) -> None:
        await self.engine.dispose()
