from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from asgi_lifespan import LifespanManager

from log_service.config import Settings
from log_service.main import create_app


@pytest.mark.parametrize("failed_step", ["ensure_index", "start"])
async def test_failed_startup_releases_resources(failed_step):
    storage = SimpleNamespace(ensure_index=AsyncMock(), close=AsyncMock())
    publisher = SimpleNamespace(start=AsyncMock(), stop=AsyncMock())
    operation = storage.ensure_index if failed_step == "ensure_index" else publisher.start
    operation.side_effect = RuntimeError("Startup failed")
    app = create_app(Settings(_env_file=None), storage=storage, publisher=publisher)

    with pytest.raises(RuntimeError, match="Startup failed"):
        async with LifespanManager(app):
            pass

    storage.close.assert_awaited_once()
    publisher.stop.assert_awaited_once()


async def test_storage_is_closed_even_if_publisher_shutdown_fails():
    storage = SimpleNamespace(ensure_index=AsyncMock(), close=AsyncMock())
    publisher = SimpleNamespace(
        start=AsyncMock(), stop=AsyncMock(side_effect=RuntimeError("Stop failed"))
    )
    app = create_app(Settings(_env_file=None), storage=storage, publisher=publisher)

    with pytest.raises(RuntimeError, match="Stop failed"):
        async with LifespanManager(app):
            pass

    storage.close.assert_awaited_once()
