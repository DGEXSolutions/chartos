import asyncpg
from typing import cast
from fastapi import FastAPI


async def get_psql() -> asyncpg.Connection:
    raise NotImplementedError
    # this yield is there to ensure the function is an async generator,
    # just like its dynamicaly injected override
    yield cast(asyncpg.Connection, None)


def setup_psql(
        app: FastAPI,
        pool_settings
) -> None:
    pool: asyncpg.Pool = None

    @app.on_event("startup")
    async def psql_pool_startup():
        nonlocal pool
        pool = await asyncpg.create_pool(**pool_settings)

    @app.on_event("shutdown")
    async def psql_pool_shutdown():
        await pool.close()

    async def override_get_psql():
        async with pool.acquire() as con:
            yield con
    app.dependency_overrides[get_psql] = override_get_psql
