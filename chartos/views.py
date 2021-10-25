from typing import Dict, List
from collections import defaultdict
from fastapi import APIRouter, Depends
from dataclasses import asdict as dataclass_as_dict
from .config import Config, get_config
from .settings import Settings, get_settings
from .psql import PSQLPool
from .redis import RedisPool


router = APIRouter()


@router.get("/health")
async def health(
        psql=Depends(PSQLPool.get),
        redis=Depends(RedisPool.get),
):
    await psql.execute("select 1;")
    await redis.ping()
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



@router.get("/layer/{layer_slug}/mvt/{view_slug}")
async def mvt_view_metadata(
        layer_slug: str,
        view_slug: str,
        config: Config = Depends(get_config),
        settings: Settings = Depends(get_settings),
):
    layer = config.layers[layer_slug]
    view = layer.views[view_slug]
    tiles_url_pattern = (
        f"{settings.protocol}://{settings.root_url}"
        f"/tile/{layer_slug}/{view_slug}"
        "/{z}/{x}/{y}/"
    )
    return {
        'type': 'vector',
        'name': layer.name,
        'promoteId': {layer.name: layer.id_field.name},
        'scheme': 'xyz',
        'tiles': [tiles_url_pattern],
        'attribution': layer.attribution or "",
        'minzoom': 0,
        'maxzoom': settings.max_zoom,
    }
