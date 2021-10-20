from fastapi import APIRouter, Depends
from .appstate import AppState, get_appstate


router = APIRouter()


@router.get("/info")
async def info(app_state: AppState = Depends(get_appstate)):
    return {
        "app_name": app_state.settings.app_name,
    }
