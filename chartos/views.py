from fastapi import APIRouter, Depends
from dataclasses import asdict as dataclass_as_dict
from .psql import PSQLPool
from .config import config


router = APIRouter()


@router.get("/info")
async def info(config = Depends(config), psql = Depends(PSQLPool.get)):
    print(psql)
    print(await psql.execute("select 1;"))
    return "ok"
