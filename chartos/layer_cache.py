from typing import Set, Tuple, Optional
from .config import View
from math import floor, pi, tan, atan, sinh, degrees, radians, asinh
from dataclasses import dataclass


@dataclass(eq=True, frozen=True)
class AffectedTile:
    x: int
    y: int
    z: int

    def to_json(self):
        return {"x": self.x, "y": self.y, "z": self.z}


def get_cache_location(layer, view, version):
    return f"chartis.layer.{layer.name}.{view.name}.version_{version}"


def get_xy(lat: float, lon: float, zoom: int) -> Tuple[int, int]:
    n = 2.0 ** zoom
    x = floor((lon + 180.) / 360. * n)
    y = floor((1. - asinh(tan(radians(lat))) / pi) / 2. * n)
    return x, y


def get_nw_deg(z: int, x: int, y: int):
    n = 2.0 ** z
    lon_deg = x / n * 360.0 - 180.0
    lat_rad = atan(sinh(pi * (1 - 2 * y / n)))
    return degrees(lat_rad), lon_deg



"""
def add_affected_tiles_sub(prepared: PreparedGeometry, z: int, x: int, y: int, tiles):
    if z > settings.MAX_ZOOM:
        return
    lat_max, long_min = get_nw_deg(z, x, y)
    lat_min, long_max = get_nw_deg(z, x + 1, y + 1)
    bbox = Polygon.from_bbox((long_min, lat_min, long_max, lat_max))
    if prepared.intersects(bbox):
        tiles.add((x, y, z))
        for sub_x in range(x * 2, x * 2 + 2):
            for sub_y in range(y * 2, y * 2 + 2):
                add_affected_tiles_sub(prepared, z + 1, sub_x, sub_y, tiles)
"""


def add_affected_tiles(geom, tiles):
    # geom: GEOSGeometry
    prepared = geom.transform(4326, clone=True).prepared
    add_affected_tiles_sub(prepared, 0, 0, 0, tiles)


async def invalidate_cache(redis, view: View, tiles: Set[Tuple[int, int, int]]):
    if len(tiles) == 0:
        return
    keys = []
    for x, y, z in tiles:
        keys.append(f'{view.get_cache_location()}.tile/{z}/{x}/{y}')
    await redis.delete(*tuple(keys))


async def invalidate_full_layer_cache(redis, layer_slug: str, version_id: Optional[int]):
    """
    Invalidate cache for a whole layer

    Args:
        layer_slug (str): The layer for which the cache has to be invalidated.
    """
    key_pattern = f'chartis.layer.{layer_slug}.*'
    if version_id is not None:
        key_pattern += f'.version_{version_id}'
    key_pattern += '.tile/*'

    delete_args = {'keys': await redis.keys(key_pattern)}
    await redis.delete(*delete_args)
