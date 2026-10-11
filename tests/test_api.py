import asyncio
from contextlib import asynccontextmanager
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager

from log_service.config import Settings
from log_service.kafka import PublishingError
from log_service.main import create_app
from log_service.models import LogEvent
from log_service.storage import ElasticsearchStorage


class FakePublisher:
    async def start(self):
        pass

    async def stop(self):
        pass

    async def send(self, event):
        pass


def handle_request(request):
    if request.method == "PUT":
        return httpx.Response(200, json={"acknowledged": True})
    if request.url.path == "/":
        return httpx.Response(200, json={"version": {"number": "8.19"}})
    if request.url.path.endswith("/_search"):
        return httpx.Response(200, json={"hits": {"total": {"value": 0}, "hits": []}})
    return httpx.Response(404, json={"found": False})


def make_storage(handler=handle_request):
    return ElasticsearchStorage(
        Settings(_env_file=None),
        client=httpx.AsyncClient(
            base_url="http://elasticsearch", transport=httpx.MockTransport(handler)
        ),
    )


@asynccontextmanager
async def open_client(storage, publisher):
    async with LifespanManager(create_app(storage=storage, publisher=publisher)) as manager:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=manager.app), base_url="http://testserver"
        ) as test_client:
            yield test_client


@pytest_asyncio.fixture
async def client():
    async with open_client(make_storage(), FakePublisher()) as test_client:
        yield test_client


async def test_empty_search_and_missing_log(client):
    response = await client.get("/logs")
    assert response.status_code == 200
    assert response.json() == {"items": [], "total": 0, "offset": 0, "limit": 50}
    assert (await client.get(f"/logs/{uuid4()}")).status_code == 404


@pytest.mark.parametrize(
    "params",
    [
        {"limit": 0},
        {"offset": -1},
        {"offset": 9999, "limit": 2},
        {"level": "UNKNOWN"},
        {"from_time": "2026-10-10T10:00:00"},
        {"from_time": "2026-10-11T10:00:00Z", "to_time": "2026-10-10T10:00:00Z"},
    ],
)
async def test_invalid_search_is_rejected(client, params):
    assert (await client.get("/logs", params=params)).status_code == 422


async def test_health(client):
    assert (await client.get("/health")).json() == {"status": "ok", "elasticsearch": True}


async def test_elasticsearch_failure_returns_503():
    def handle(request):
        if request.method == "PUT":
            return httpx.Response(200, json={"acknowledged": True})
        return httpx.Response(429, json={"error": "too many requests"})

    async with open_client(make_storage(handle), FakePublisher()) as test_client:
        assert (await test_client.get("/logs")).status_code == 503
        assert (await test_client.get("/health")).status_code == 503


@pytest.mark.parametrize("changes", [{"service": " "}, {"message": ""}, {"level": "INVALID"}])
def test_invalid_log_schema(changes):
    with pytest.raises(ValueError):
        LogEvent.model_validate({"service": "demo", "message": "Started", **changes})


async def test_publishing_and_validation(client):
    event_id = str(uuid4())
    response = await client.post("/logs", json={"id": event_id, "service": "demo", "message": "Started"})
    assert response.status_code == 202
    assert response.json() == {"id": event_id, "status": "queued"}
    assert (await client.post("/logs", json={"service": "demo", "message": " "})).status_code == 422


async def test_kafka_failure_returns_503():
    class OfflinePublisher(FakePublisher):
        async def send(self, event):
            raise PublishingError("Offline")

    async with open_client(make_storage(), OfflinePublisher()) as test_client:
        response = await test_client.post("/logs", json={"service": "demo", "message": "Started"})
        assert response.status_code == 503


async def test_waiting_for_kafka_does_not_block_other_requests():
    class DelayedPublisher(FakePublisher):
        def __init__(self):
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        async def send(self, event):
            if event.message == "Slow message":
                self.started.set()
                await self.release.wait()

    publisher = DelayedPublisher()
    async with open_client(make_storage(), publisher) as test_client:
        pending = asyncio.create_task(
            test_client.post("/logs", json={"service": "demo", "message": "Slow message"})
        )
        try:
            await asyncio.wait_for(publisher.started.wait(), timeout=2)
            health = await asyncio.wait_for(test_client.get("/health"), timeout=2)
            other_log = await asyncio.wait_for(
                test_client.post("/logs", json={"service": "demo", "message": "Fast message"}),
                timeout=2,
            )
            assert health.status_code == 200
            assert other_log.status_code == 202
            assert not pending.done()
        finally:
            publisher.release.set()
            response = await asyncio.wait_for(pending, timeout=2)
        assert response.status_code == 202
