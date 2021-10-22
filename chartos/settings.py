from pydantic import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    config_path: str = "layer.yml"
    psql_dsn: str
    psql_user: str
    psql_password: str

    def psql_settings(self):
        return {
            "psql_dsn": self.psql_dsn,
            "psql_user": self.psql_user,
            "psql_password": self.psql_password,
        }


@lru_cache()
def get_settings():
    return Settings()
