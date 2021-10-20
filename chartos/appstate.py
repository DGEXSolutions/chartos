from fastapi import Depends
from dataclasses import dataclass
from .settings import Settings, get_settings

@dataclass
class AppState:
    settings: Settings


async def get_appstate(settings = Depends(get_settings)) -> AppState:
    yield AppState(settings)
