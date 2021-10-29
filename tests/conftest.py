import httpx
import pytest
from chartos import make_app, get_env_settings
from asgi_lifespan import LifespanManager


@pytest.fixture
def settings():
    return get_env_settings()


@pytest.fixture
async def app(settings):
    app = make_app(settings)

    async with LifespanManager(app):
        yield app


@pytest.fixture
async def client(app, settings):
    async with httpx.AsyncClient(app=app, base_url=settings.root_url) as client:
        yield client
