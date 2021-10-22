from typing import Dict, List
from collections import defaultdict
from fastapi import APIRouter, Depends
from dataclasses import asdict as dataclass_as_dict
from .psql import PSQLPool
from .config import Config, get_config


router = APIRouter()


@router.get("/health")
async def health(psql=Depends(PSQLPool.get)):
    await psql.execute("select 1;")
    return ""


@router.get("/info")
async def info(config: Config = Depends(get_config)):
    return {
        config.name: {
            "layers": [
                {
                    "name": layer.name,
                    "description": layer.description,
                    "versioned": layer.versioned,
                    "views": list(layer.views.keys()),
                }
                for layer in config.layers.values()
            ]
        }
    }
