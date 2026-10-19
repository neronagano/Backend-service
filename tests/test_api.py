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

    async def is_healthy(self):
        return True


def handle_request(request):
    if request.method == "PUT":
        return httpx.Response(200, json={"acknowledged": True})
    if request.url.path.startswith("/_cluster/health/"):
        return httpx.Response(200, json={"status": "yellow", "timed_out": False})
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
async def open_client(storage, publisher, settings=None):
    configuration = settings if settings is not None else Settings(_env_file=None)
    async with LifespanManager(
        create_app(configuration, storage=storage, publisher=publisher)
    ) as manager:
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


async def test_liveness_does_not_depend_on_elasticsearch():
    def handle(request):
        if request.method == "PUT":
            return httpx.Response(200, json={"acknowledged": True})
        return httpx.Response(503, json={"error": "offline"})

    async with open_client(make_storage(handle), FakePublisher()) as test_client:
        assert (await test_client.get("/health")).status_code == 503
        response = await test_client.get("/live")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


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
    response = await client.post(
        "/logs", json={"id": event_id, "service": "demo", "message": "Started"}
    )
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


async def test_api_key_protects_logs_but_not_health_checks():
    settings = Settings(_env_file=None, api_key="a-long-test-secret-key")
    async with open_client(make_storage(), FakePublisher(), settings) as test_client:
        for path in ("/logs", f"/logs/{uuid4()}"):
            assert (await test_client.get(path)).status_code == 401
        assert (
            await test_client.post("/logs", json={"service": "demo", "message": "One"})
        ).status_code == 401
        assert (await test_client.get("/logs", headers={"X-API-Key": "wrong"})).status_code == 401
        assert (
            await test_client.get("/logs", headers={"X-API-Key": "a-long-test-secret-key"})
        ).status_code == 200
        assert (await test_client.get("/ready")).status_code == 200
        assert (await test_client.get("/live")).status_code == 200


async def test_large_content_length_is_rejected_without_reading_body():
    async with open_client(make_storage(), FakePublisher()) as test_client:
        response = await test_client.post(
            "/logs", content=b"{}", headers={"Content-Length": "999999"}
        )
        assert response.status_code == 413


async def test_chunked_body_limit_is_enforced():
    async def chunks():
        for _ in range(3):
            yield b"x" * 512

    settings = Settings(_env_file=None, max_request_bytes=1024)
    async with open_client(make_storage(), FakePublisher(), settings) as test_client:
        assert (await test_client.post("/logs", content=chunks())).status_code == 413


async def test_documentation_can_be_disabled():
    async with open_client(
        make_storage(), FakePublisher(), Settings(_env_file=None, docs_enabled=False)
    ) as test_client:
        assert (await test_client.get("/docs")).status_code == 404
        assert (await test_client.get("/openapi.json")).status_code == 404
        assert (await test_client.get("/live")).status_code == 200


async def test_readiness_detects_kafka_outage():
    class OfflinePublisher(FakePublisher):
        async def is_healthy(self):
            return False

    async with open_client(make_storage(), OfflinePublisher()) as test_client:
        response = await test_client.get("/ready")
        assert response.status_code == 503
        assert response.json() == {"status": "unavailable", "elasticsearch": True, "kafka": False}
        assert (await test_client.get("/live")).status_code == 200


async def test_slow_request_body_is_rejected():
    async def chunks():
        yield b"{"
        await asyncio.Event().wait()

    settings = Settings(_env_file=None, body_read_timeout=0.02)
    async with open_client(make_storage(), FakePublisher(), settings) as test_client:
        assert (await test_client.post("/logs", content=chunks())).status_code == 408
