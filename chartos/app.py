import yaml
from fastapi import FastAPI, Depends
from .views import router as view_router
from .settings import Settings, get_settings
from .config import Config, get_config
from .serialized_config import SerializedConfig
from .psql import PSQLPool
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
    get_config.setup(app, read_config(settings))
    PSQLPool.setup(app, settings.psql_settings())
    RedisPool.setup(app, settings.redis_url)
    return app


app = make_app(get_settings())
