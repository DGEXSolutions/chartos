import pytest
from chartos.layer_cache import find_affected_tiles
from .test_data import ref_tiles, campus_sncf_mercator


def test_find_affected_tiles():
    tiles = {
        (tile.x, tile.y, tile.z)
        for tile
        in find_affected_tiles(18, campus_sncf_mercator)
    }
    assert tiles == ref_tiles
