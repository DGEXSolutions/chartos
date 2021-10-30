import pytest
import shapely.geometry

from .test_data import campus_sncf_mercator


async def get_tile(client, z, x, y, version="test"):
    url = f"/tile/osrd_track_section/geo/{z}/{x}/{y}/?version={version}"
    res = await client.get(url)
    assert res.status_code == 200
    return res


@pytest.mark.asyncio
async def test_insert(client):
    response = await get_tile(client, 14, 8299, 5632)
    assert response.read() == b""

    insert_payload = [
        {"entity_id": 1, "geom_geo": shapely.geometry.mapping(campus_sncf_mercator)}
    ]
    response = await client.post(
        "/push/osrd_track_section/insert/?version=test",
        json=insert_payload
    )
    assert response.status_code == 200

    response = await get_tile(client, 14, 8299, 5632)
    single_item_tile = response.read()
    assert single_item_tile != b""
