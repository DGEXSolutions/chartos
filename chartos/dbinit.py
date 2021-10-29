import asyncpg
from fastapi import FastAPI
from .config import Layer
from .utils import AsyncProcess, process_dependable


tilebbox_func = """
    create or replace function TileBBox (z int, x int, y int, srid int = 3857)
        returns geometry
        language plpgsql immutable as
    $func$
    declare
        max numeric := 20037508.34;
        res numeric := (max*2)/(2^z);
        bbox geometry;
    begin
        bbox := ST_MakeEnvelope(
            -max + (x * res),
            max - (y * res),
            -max + (x * res) + res,
            max - (y * res) - res,
            3857
        );
        if srid = 3857 then
            return bbox;
        else
            return ST_Transform(bbox, srid);
        end if;
    end;
    $func$
"""


async def init_layer(conn, layer: Layer):
    table_name = layer.pg_table_name()

    # create the table if it doesn't exist
    fields_sig = ', '.join(field.pg_signature() for field in layer.fields.values())
    query = f"CREATE TABLE IF NOT EXISTS {table_name} ({fields_sig});"
    await conn.execute(query)

    # add the missing columns
    version_col = "ADD COLUMN IF NOT EXISTS version varchar"
    user_cols = ", ".join(
        f"ADD COLUMN IF NOT EXISTS {field.pg_signature()}"
        for field in layer.fields.values()
    )
    await conn.execute(f"ALTER TABLE {table_name} {version_col}, {user_cols};")

    # add indexes on geographic fields used in views
    geo_fields = {view.on_field for view in layer.views.values()}
    for geo_field in geo_fields:
        index_name = f"{table_name}_{geo_field.name}_spgist"
        await conn.execute(
            f'CREATE INDEX IF NOT EXISTS "{index_name}" ON {table_name} '
            f'USING SPGIST ({geo_field.pg_name()});'
        )
    # add the version index
    await conn.execute(
        f"CREATE INDEX IF NOT EXISTS {table_name}_version "
        f"ON {table_name} (\"version\");"
    )

    # add the TileBBox utility
    await conn.execute(tilebbox_func)



class DBInit(AsyncProcess):
    def __init__(self, config, psql_pool):
        self.config = config
        self.psql_pool = psql_pool

    async def on_startup(self):
        async with self.psql_pool.acquire() as conn:
            try:
                for layer in self.config.layers.values():
                    await init_layer(conn, layer)
            finally:
                await conn.reload_schema_state()

    async def on_shutdown(self):
        pass
