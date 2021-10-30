import yaml
import asyncpg
import shapely.wkb
from fastapi import FastAPI, Depends
from .settings import Settings, get_settings, get_env_settings
from .config import Config, get_config
from .serialized_config import SerializedConfig
from .psql import PSQLPool
from .dbinit import DBInit
from .redis import RedisPool
from typing import Optional

from .views import router as view_router
from .truncate import router as truncate_router
from .modify import router as modify_router


def read_config(settings: Settings) -> Config:
    with open(settings.config_path) as f:
        config_data = yaml.safe_load(f)
        raw_config = SerializedConfig.parse_obj(config_data)
        return Config.parse(raw_config)


def encode_geometry(geometry):
    if not hasattr(geometry, '__geo_interface__'):
        raise TypeError('{g} does not conform to '
                        'the geo interface'.format(g=geometry))
    shape = shapely.geometry.shape(geometry)
    return shapely.wkb.dumps(shape)


async def init_psql_conn(conn: asyncpg.Connection):
    await conn.set_type_codec(
        'geometry',  # also works for 'geography'
        encoder=encode_geometry,
        decoder=shapely.wkb.loads,
        format='binary',
    )


def make_app(settings: Optional[Settings] = None) -> FastAPI:
    if settings is None:
        settings = get_env_settings()
    # create the application
    app = FastAPI()
    app.include_router(view_router)
    app.include_router(truncate_router)
    app.include_router(modify_router)

    # parse the configuration and setup dep injection
    config = read_config(settings)
    get_config.setup(app, config)
    get_settings.setup(app, settings)

    # setup the redis pool process
    RedisPool.setup(app, settings.redis_url)

    # setup the postgresql pool process
    psql_settings = {
        **settings.psql_settings(),
        "init": init_psql_conn,
    }
    psql_pool = PSQLPool.setup(app, psql_settings)

    # initialize the database initialization process
    DBInit.setup(app, config, psql_pool)
    return app
