from fastapi import APIRouter, Depends
from dataclasses import asdict as dataclass_as_dict
from .psql import get_psql

router = APIRouter()


@router.get("/info")
async def info(psql = Depends(get_psql)):
    return {
        "app_name": dataclass_as_dict(app_state.config)
    }
