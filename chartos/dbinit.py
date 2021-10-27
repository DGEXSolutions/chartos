import asyncpg
from fastapi import FastAPI
from .config import Layer
from .utils import AsyncProcess, process_dependable


def init_layer(conn, layer: Layer):
    table_name = layer.pg_table_name()

    # create the table if it doesn't exist
    fields_sig = ', '.join(field.pg_signature() for field in layer.fields)
    query = f"CREATE TABLE IF NOT EXISTS {table_name} ({fields_sig});"
    await conn.execute(query)

    # add the missing columns
    add_missing_cols = ", ".join(
        "ADD COLUMN IF NOT EXISTS {field.pg_signature()}"
        for field in layer.fields
    )
    query = f"ALTER TABLE {table_name} {add_missing_cols};"
    await conn.execute(query)

    # add indexes on geographic fields used in views
    geo_fields = {view.on_field for view in layer.views}
    for geo_field in geo_fields:
        index_name = f"{table_name}_{geo_field.name}_spgist"
        await conn.execute((
            f'CREATE INDEX IF NOT EXISTS "{index_name}" ON {table_name} '
            f'USING SPGIST ({geo_field.pg_name});'
        ))


class DBInit(AsyncProcess):
    def __init__(self, config, psql_pool):
        self.config = config
        self.psql_pool = psql_pool

    async def on_startup(self):
        async with self.psql_pool.acquire() as conn:
            for layer in self.config.layers.values():
                init_layer(conn, layer)

    async def on_shutdown(self):
        pass
