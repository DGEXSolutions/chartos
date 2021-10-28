from collections import defaultdict
from functools import reduce
from math import floor, pi, tan, atan, sinh, degrees, radians, asinh
from typing import Tuple, Set, List, Optional, Dict
from fastapi import APIRouter, Depends, HTTPException
from .config import Config, get_config
from .settings import Settings, get_settings
from .psql import PSQLPool
from .redis import RedisPool
from fastapi.responses import Response


# from chartis.utils.encoder import DecimalEncoder
# from chartis.utils.parse import GeoJSONParser


router = APIRouter()


CRS_DEFINITION = {'type': 'name', 'properties': {'name': 'EPSG:3857'}}


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


def add_affected_tiles(geom: GEOSGeometry, tiles):
    prepared = geom.transform(4326, clone=True).prepared
    add_affected_tiles_sub(prepared, 0, 0, 0, tiles)


def python_array_to_postgres_array(array: List) -> str:
    return f"{{{json_dumps(array)[1:-1]}}}"


async def invalidate_cache(view: View, tiles: Set[Tuple[int, int, int]]):
    if len(tiles) == 0:
        return
    keys = []
    for x, y, z in tiles:
        keys.append(f'{view.get_cache_location()}.tile/{z}/{x}/{y}')
    redis = await get_redis()
    await redis.delete(*tuple(keys))


async def invalidate_full_layer_cache(layer_slug: str, version_id: Optional[int]):
    """
    Invalidate cache for a whole layer

    Args:
        layer_slug (str): The layer for which the cache has to be invalidated.
    """
    key_pattern = f'chartis.layer.{layer_slug}.*'
    if version_id is not None:
        key_pattern += f'.version_{version_id}'
    key_pattern += '.tile/*'

    redis = await get_redis()
    delete_args = {'keys': await redis.keys(key_pattern)}
    await redis.delete(*delete_args)


def get_version_insert_field(version: Optional[int]) -> Tuple[str, str]:
    if version is None:
        return '', ''
    return ', "version"', ', %s'


def get_version_where_clause(version: Optional[int]) -> str:
    if version is None:
        return ''
    return f' AND "version" = \'{version}\''


async def insert_row(layer: Layer, row, affected_tiles, cursor, version: Optional[int]):
    fields_str, values_str = get_version_insert_field(version)
    values = [] if version is None else [str(version)]
    for key in row.keys():
        field = layer.get_field_by_name(key)
        if field is None or row[key] is None or row[key] == 'None':
            continue
        fields_str += f', {field.to_update_column()}'
        values_str += ', %s'
        if field.is_geographic():
            if 'crs' not in row[key]:
                row[key]['crs'] = CRS_DEFINITION
            geom = GEOSGeometry(json_dumps(row[key], cls=DecimalEncoder))
            assert geom.srid == 3857
            values.append(geom.ewkt)

            for view in filter(lambda view: view.is_geographic, layer.views.values()):
                if view.geom_field.name != field.name:
                    continue
                add_affected_tiles(geom, affected_tiles[view.name])
        else:
            if field.type[-2:] == '[]':
                values.append(python_array_to_postgres_array(row[key]))
            elif isinstance(row[key], dict) or isinstance(row[key], list):
                values.append(json_dumps(row[key]))
            else:
                values.append(row[key])
    return [cursor.mogrify(f'INSERT INTO "{layer.name}" ({fields_str[2:]}) VALUES ({values_str[2:]})', values)]


async def delete_row(layer: Layer, row, cursor, version: Optional[int]):
    fetch_geom_query = defaultdict(list)
    version_where = get_version_where_clause(version)
    for view in filter(lambda view: view.is_geographic, layer.views.values()):
        fetch_geom_query[view.name].append(cursor.mogrify(
            f'SELECT "{view.geom_field.name}" FROM "{layer.name}" WHERE "{layer.id_field.name}" = %s{version_where}',
            [row[layer.id_field.name]]))
    q = cursor.mogrify(f'DELETE FROM "{layer.name}" WHERE "{layer.id_field.name}" = %s{version_where}',
                       [row[layer.id_field.name]])
    return [q], fetch_geom_query


async def update_row(layer: Layer, row, affected_tiles, cursor, version: Optional[int]):
    fields = ''
    values = []
    version_where = get_version_where_clause(version)
    for key in row.keys():
        field = layer.get_field_by_name(key)
        if field is None or row[key] is None or row[key] == 'None':
            continue
        fields += f', {field.to_update_column()}=%s'
        if layer.is_field_name_geometry(key):
            if 'crs' not in row[key]:
                row[key]['crs'] = CRS_DEFINITION
            geom = GEOSGeometry(json_dumps(row[key], cls=DecimalEncoder))
            assert geom.srid == 3857
            values.append(geom.ewkt)

            for view in filter(lambda view: view.is_geographic, layer.views.values()):
                if view.geom_field.name != field.name:
                    continue
                add_affected_tiles(geom, affected_tiles[view.name])
        else:
            if field.type[-2:] == '[]':
                values.append(python_array_to_postgres_array(row[key]))
            elif isinstance(row[key], dict) or isinstance(row[key], list):
                values.append(json_dumps(row[key]))
            else:
                values.append(row[key])
    values.append(row[layer.id_field.name])

    fetch_geom_query = defaultdict(list)
    for view in filter(lambda view: view.is_geographic, layer.views.values()):
        fetch_geom_query[view.name].append(cursor.mogrify(
            f'SELECT "{view.geom_field.name}" FROM "{layer.name}" WHERE "{layer.id_field.name}" = %s{version_where}',
            [row[layer.id_field.name]]))
    q = cursor.mogrify(f'UPDATE "{layer.name}" SET {fields[2:]} WHERE "{layer.id_field.name}" = %s{version_where}',
                       values)
    return [q], fetch_geom_query


@sync_to_async
def fetch_geom(fetch_geom_query, affected_tiles):
    for view, queries in fetch_geom_query.items():
        query = reduce(lambda acc, v: f'{acc} UNION {v.decode()}' if len(acc) != 0 else v.decode(), queries, '')
        with connection.cursor() as cursor:
            cursor.execute(query)
            for row in cursor.fetchall():
                # if there's no geographic data for this view, no tiles returned
                if row[0] is None:
                    continue
                geom = GEOSGeometry(row[0])
                add_affected_tiles(geom, affected_tiles[view])


@sync_to_async
def apply_changes(change_query: List[str]):
    if len(change_query) == 0:
        return
    query = reduce(lambda acc, v: f'{acc}; {v.decode()}', change_query, '')
    with connection.cursor() as cursor:
        cursor.execute(query[2:])


def merge_defaultdict(dict_a, dict_b):
    dict = defaultdict(list)
    for dict_l in [dict_a, dict_b]:
        for key, value in dict_l.items():
            dict[key] += value
    return dict


@sync_to_async
def truncate_layer(layer: Layer, version_id: Optional[int]):
    query = f'DELETE FROM "{layer.name}" WHERE "version" = \'{version_id}\';'


class PushView(AsyncAPIView):
    parser_classes = [JSONParser, GeoJSONParser]

    async def post(self, request, layer_slug, change_type, *args, **kwargs):
        fields_name_set = set(map(lambda field: field.name, layer.fields))
        no_impacted_tiles = 'no_impacted_tiles' in request.GET
        if change_type in ['insert', 'update', 'delete']:
            if not isinstance(request.data, list):
                raise ParseError("A list was expected on the payload.")
        elif change_type in ['truncate']:
            if request.data != {}:
                raise ParseError("No data is expected on truncate.")
        else:
            raise ParseError({
                "details": f"Change type `{change_type}` unavailable.",
                "choices": ["insert", "update", "delete", "truncate"]
            })

        # check each entry
        for row in request.data:
            if layer.id_field.name not in row.keys():
                raise ParseError({
                    "details": f"Key `{layer.id_field.name}` is required but not found.",
                    "choices": list(row.keys())
                })
            for view in layer.views.values():
                if view.is_geographic and view.geom_field.name not in row.keys() and change_type == 'insert':
                    raise ParseError({
                        "details": f"Key `{view.geom_field.name}` is required but not found.",
                        "choices": list(row.keys())
                    })

            for key in row.keys():
                if key not in fields_name_set:
                    raise ParseError({
                        "details": f"Key `{key}` is required but not found.",
                        "choices": list(fields_name_set)
                    })

        # apply change
        affected_tiles = defaultdict(set)

        @sync_to_async
        def wrap():
            change_query, fetch_geom_query = [], defaultdict(list)
            with connection.cursor() as cursor:
                for row in request.data:
                    if change_type == 'insert':
                        change_query += async_to_sync(insert_row)(layer, row, affected_tiles, cursor, version)
                    if change_type == 'delete':
                        delete_tuple = async_to_sync(delete_row)(layer, row, cursor, version)
                        lc, lq, = delete_tuple
                        change_query += lc
                        fetch_geom_query = merge_defaultdict(fetch_geom_query, lq)
                    if change_type == 'update':
                        update_tuple = async_to_sync(update_row)(layer, row, affected_tiles, cursor, version)
                        lc, lq = update_tuple
                        change_query += lc
                        fetch_geom_query = merge_defaultdict(fetch_geom_query, lq)
            return change_query, fetch_geom_query

        if change_type == 'truncate':
            await truncate_layer(layer, version)
            await invalidate_full_layer_cache(layer.name, version)
            return Response({'impacted_tiles': {'geo': ['*'], 'sch': ['*'], }}, status=201)
        else:
            warp_tuple = await wrap()
            change_query, fetch_geom_query = warp_tuple

            await fetch_geom(fetch_geom_query, affected_tiles)
            await apply_changes(change_query)

            export_impacted_tiles = {}
            for view_name, tiles in affected_tiles.items():
                await invalidate_cache(layer.views[view_name], tiles)
                export_impacted_tiles[view_name] = map(lambda c: {'z': c[2], 'x': c[0], 'y': c[1]}, tiles)

            return Response({'impacted_tiles': export_impacted_tiles} if not no_impacted_tiles else {}, status=201)
