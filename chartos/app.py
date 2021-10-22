import yaml
from fastapi import FastAPI, Depends
from .views import router as view_router
from .settings import Settings, get_settings
from .config import Config
from .serialized_config import SerializedConfig
from .psql import setup_psql
from typing import Optional


def get_config(settings: Settings) -> Config:
    with open(settings.config_path) as f:
        config_data = yaml.safe_load(f)
        raw_config = SerializedConfig.parse_obj(config_data)
        return Config.parse(raw_config)


def make_app(settings: Settings) -> FastAPI:
    # TODO: inject into views which require it
    config = get_config()

    app = FastAPI()
    app.include_router(view_router)
    setup_psql(app, settings.psql_settings())
    return app


app = make_app(get_settings())
