import yaml
from fastapi import FastAPI, Depends
from .views import router as view_router
from .settings import Settings, get_settings, get_env_settings
from .config import Config, get_config
from .serialized_config import SerializedConfig
from .psql import PSQLPool
from .dbinit import DBInit
from .redis import RedisPool
from typing import Optional


def read_config(settings: Settings) -> Config:
    with open(settings.config_path) as f:
        config_data = yaml.safe_load(f)
        raw_config = SerializedConfig.parse_obj(config_data)
        return Config.parse(raw_config)


def make_app(settings: Settings) -> FastAPI:
    app = FastAPI()
    app.include_router(view_router)
    config = read_config(settings)
    get_config.setup(app, config)
    get_settings.setup(app, settings)
    RedisPool.setup(app, settings.redis_url)
    psql_pool = PSQLPool.setup(app, settings.psql_settings())
    DBInit.setup(app, config, psql_pool)
    return app


app = make_app(get_env_settings())
