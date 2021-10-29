from dataclasses import dataclass
from collections import defaultdict
from functools import reduce
from typing import Tuple, Set, List, Optional, Dict
from fastapi import APIRouter, Depends, HTTPException
from .config import Config, get_config, Layer
from .settings import Settings, get_settings
from .psql import PSQLPool
from .redis import RedisPool
from fastapi.responses import Response
from enum import Enum
from typing import List, Dict, Any
from .layer_cache import invalidate_cache, invalidate_full_layer_cache, add_affected_tiles, AffectedTile

# from chartis.utils.encoder import DecimalEncoder
# from chartis.utils.parse import GeoJSONParser


router = APIRouter()


CRS_DEFINITION = {'type': 'name', 'properties': {'name': 'EPSG:3857'}}



def python_array_to_postgres_array(array: List) -> str:
    return f"{{{json_dumps(array)[1:-1]}}}"


async def insert_row(psql, layer: Layer, row, affected_tiles, version: str):
    fields_str, values_str = get_version_insert_field(version)
    values = [] if version is None else [str(version)]
    for key in row.keys():
        field = layer.fields[key]
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

            for view in layer.views.values():
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


async def delete_row(psql, layer: Layer, row, version: str):
    fetch_geom_query = defaultdict(list)
    version_where = get_version_where_clause(version)
    for view in layer.views.values():
        fetch_geom_query[view.name].append(cursor.mogrify(
            f'SELECT {view.on_field.pg_name()} FROM {layer.pg_table_name()} WHERE "{layer.id_field.name}" = %s{version_where}',
            [row[layer.id_field.name]]))
    q = cursor.mogrify(f'DELETE FROM "{layer.name}" WHERE "{layer.id_field.name}" = %s{version_where}',
                       [row[layer.id_field.name]])
    return [q], fetch_geom_query


async def update_row(psql, layer: Layer, row, affected_tiles, version: str):
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



def merge_defaultdict(dict_a, dict_b):
    dict = defaultdict(list)
    for dict_l in [dict_a, dict_b]:
        for key, value in dict_l.items():
            dict[key] += value
    return dict



class ChangeType(str, Enum):
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


def validate_payload(layer: Layer, payload: List[Dict[str, Any]], change_type: ChangeType):
    valid_fields = layer.fields.keys()
    mandatory_fields = {layer.id_field.name}

    if change_type is ChangeType.INSERT:
        for view in layer.views:
            mandatory_fields.add(view.on_field.name)

    # check each entry
    for row in payload:
        for field_name in row.keys():
            if field_name not in valid_fields:
                raise HTTPException(status_code=400, detail={
                    "details": f"Unknown field name `{field_name}`",
                    "choices": list(valid_fields)
                })

        for mandatory_field in mandatory_fields:
            if mandatory_field not in row.keys():
                raise HTTPException(status_code=400, detail={
                    "details": f"Key `{layer.id_field.name}` is required but not found.",
                    "choices": list(row.keys())
                })


@router.post('/push/{layer_slug}/{change_type}/')
async def push(
        layer_slug: str,
        change_type: ChangeType,
        payload: List[Dict[str, Any]],
        config: Config = Depends(get_config),
        psql=Depends(PSQLPool.get),
        redis=Depends(RedisPool.get),
):
    layer = config.layers[layer_slug]
    validate_payload(payload)

    # the list of affected tiles is built from both queries (for insertions
    # and updates) and the result of the update
    affected_tiles: Dict[str, AffectedTile] = defaultdict(set)

    change_query = []
    # the key is the view, the value is the list of affected tiles
    fetch_geom_query = defaultdict(list)
    for row in payload:
        if change_type == 'insert':
            change_query += await insert_row(layer, row, affected_tiles, cursor, version)
        if change_type == 'delete':
            lc, lq = await delete_row(layer, row, cursor, version)
            change_query += lc
            fetch_geom_query = merge_defaultdict(fetch_geom_query, lq)
        if change_type == 'update':
            lc, lq = await update_row(layer, row, affected_tiles, cursor, version)
            change_query += lc
            fetch_geom_query = merge_defaultdict(fetch_geom_query, lq)

    # fetch the geometry
    for view, queries in fetch_geom_query.items():
        query = " UNION ".join(v.decode() for v in queries)
        await psql.execute(query)
        async for row in cursor.fetchall():
            # if there's no geographic data for this view, no tiles returned
            if row[0] is None:
                continue
            geom = row[0]
            add_affected_tiles(geom, affected_tiles[view])

    if change_query:
        await psql.execute("".join(f"{v.decode()};" for v in change_query))

    export_impacted_tiles = {}
    for view_name, tiles in affected_tiles.items():
        await invalidate_cache(layer.views[view_name], tiles)
        export_impacted_tiles[view_name] = [{'z': z, 'x': x, 'y': y} for x, y, z in tiles]

    return Response({'impacted_tiles': export_impacted_tiles}, status=201)
