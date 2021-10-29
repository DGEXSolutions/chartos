import pytest


@pytest.mark.asyncio
async def test_read_main(client):
    insert_payload = [
        {"entity_id": 1}
    ]
    response = await client.post(
        "/push/osrd_track_section/insert/?version=test",
        json=insert_payload
    )
    assert response.status_code == 200

    # response = client.get("/tile/osrd_track_section/geo/0/0/0/?version=test")
    # assert response.status_code == 200
    # assert response.json() == {"msg": "Hello World"}
