from typing import Optional
from pydantic import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    config_path: str = "examples/layer.yml"
    psql_dsn: str
    psql_user: Optional[str] = None
    psql_password: Optional[str] = None
    redis_url: str

    def psql_settings(self):
        return {
            "dsn": self.psql_dsn,
            "user": self.psql_user,
            "password": self.psql_password,
        }


@lru_cache()
def get_settings():
    return Settings()
