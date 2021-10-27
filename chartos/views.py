from typing import Dict, List
from collections import defaultdict
from fastapi import APIRouter, Depends, HTTPException
from dataclasses import asdict as dataclass_as_dict
from .config import Config, get_config
from .settings import Settings, get_settings
from .psql import PSQLPool
from .redis import RedisPool
from fastapi.responses import Response
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


class ProtobufResponse(Response):
    media_type = "application/x-protobuf"


@router.get(
    "/tile/{layer_slug}/{view_slug}/{z}/{x}/{y}/",
    response_class=ProtobufResponse
)
async def mvt_view_tile(
        layer_slug: str,
        view_slug: str,
        z: int, x: int, y: int,
        config: Config = Depends(get_config),
        settings: Settings = Depends(get_settings),
        psql=Depends(PSQLPool.get),
        redis=Depends(RedisPool.get),
):
    layer = config.layers[layer_slug]
    view = layer.views[view_slug]

    # try to fetch the tile from the cache
    cache_key = f'{view.get_cache_location()}.tile/{z}/{x}/{y}'
    tile_data = await redis.get(cache_key)
    if tile_data is not None:
        return tile_data

    # if the key isn't found, compute build the tile
    tile_data = await mvt_query(psql, layer, view, z, x, y)
    if tile_data is None:
        raise HTTPException(status_code=404, detail='no tile found')

    # store the tile in the cache
    await redis.set(cache_key, tile_data, expire=view.cache_duration)
    return tile_data


async def mvt_query(psql, layer, view, z, x, y):
    view_field_names = ", ".join(field.pg_name() for field in view.fields)
    on_field_name = view.on_field.pg_name()
    mvt_layer_name = f"'{layer.name}'"
    tile_content_subquery = (
        "SELECT "
        # select all the fields the user requested
        f"{view_field_names}, "
        # along with the geometry the view is based on, converted to MVT
        f"ST_AsMVTGeom({on_field_name}, bbox.geom, 4096, 64, true) AS MVTGeom "
        # read from the table corresponding to the layer, as well as the bbox
        # the bbox table is built by the WITH clause of the top-level query
        f"FROM {layer.pg_table_name()}, bbox "
        # we only want objects which are inside the tile BBox
        f"WHERE {on_field_name} && bbox.geom "
        # exclude geometry collections
        f"AND ST_GeometryType({on_field_name}) != 'ST_GeometryCollection'"
    )
    query = (
        # prepare the bbox of the tile for use in the tile content subquery
        "WITH bbox AS (SELECT TileBBox($1, $2, $3, 3857) AS geom), "
        # find all objects in the tile
        f"tile_content AS ({tile_content_subquery}"
        # package those inside an MVT tile
        f"SELECT ST_AsMVT(tile_content, {mvt_layer_name}) FROM tile_content"
    )
    return await psql.execute(query, z, x, y)
