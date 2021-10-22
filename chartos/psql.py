import asyncpg
from typing import cast
from fastapi import FastAPI

from fastapi.dependencies.utils import is_gen_callable, is_async_gen_callable
from .utils import AsyncProcess, process_dependable

class PSQLPool(AsyncProcess):
    def __init__(self, settings):
        self.pool = None
        self.settings = settings

    async def on_startup(self):
        self.pool = await asyncpg.create_pool(**self.settings)

    async def on_shutdown(self):
        await self.pool.close()

    @process_dependable
    async def get(self) -> asyncpg.Connection:
        async with self.pool.acquire() as con:
            yield con
