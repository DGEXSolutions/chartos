from fastapi import APIRouter, Depends
from .config import Config, get_config
from .psql import PSQLPool
from .redis import RedisPool
from fastapi.responses import JSONResponse
from .layer_cache import invalidate_full_layer_cache


router = APIRouter()


@router.post("push/{layer_slug}/truncate/")
async def truncate(
        layer_slug: str,
        version: str,
        psql=Depends(PSQLPool.get),
        redis=Depends(RedisPool.get),
        config: Config = Depends(get_config),
):
    layer = config.layers[layer_slug]
    await psql.execute(f'DELETE FROM {layer.pg_table_name()} WHERE version = $1;', version)
    await invalidate_full_layer_cache(redis, layer.name, version)
    return JSONResponse(status_code=201, content={'impacted_tiles': {'geo': ['*'], 'sch': ['*'], }})
