from uuid import uuid4

import httpx
import pytest
from fastapi.testclient import TestClient

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


@pytest.fixture
def client():
    def handle(request):
        if request.method == "PUT":
            return httpx.Response(200, json={"acknowledged": True})
        if request.url.path == "/":
            return httpx.Response(200, json={"version": {"number": "8.19"}})
        if request.url.path.endswith("/_search"):
            return httpx.Response(200, json={"hits": {"total": {"value": 0}, "hits": []}})
        return httpx.Response(404, json={"found": False})

    storage = ElasticsearchStorage(
        Settings(_env_file=None),
        client=httpx.AsyncClient(base_url="http://elasticsearch", transport=httpx.MockTransport(handle)),
    )
    with TestClient(create_app(storage=storage, publisher=FakePublisher())) as test_client:
        yield test_client


def test_empty_search_and_missing_log(client):
    response = client.get("/logs")
    assert response.status_code == 200
    assert response.json() == {"items": [], "total": 0, "offset": 0, "limit": 50}
    assert client.get(f"/logs/{uuid4()}").status_code == 404


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
def test_invalid_search_is_rejected(client, params):
    assert client.get("/logs", params=params).status_code == 422


def test_health(client):
    assert client.get("/health").json() == {"status": "ok", "elasticsearch": True}


def test_elasticsearch_failure_returns_503():
    def handle(request):
        if request.method == "PUT":
            return httpx.Response(200, json={"acknowledged": True})
        return httpx.Response(429, json={"error": "too many requests"})

    storage = ElasticsearchStorage(
        Settings(_env_file=None),
        client=httpx.AsyncClient(base_url="http://elasticsearch", transport=httpx.MockTransport(handle)),
    )
    with TestClient(create_app(storage=storage, publisher=FakePublisher())) as test_client:
        assert test_client.get("/logs").status_code == 503
        assert test_client.get("/health").status_code == 503


@pytest.mark.parametrize("changes", [{"service": " "}, {"message": ""}, {"level": "INVALID"}])
def test_invalid_log_schema(changes):
    with pytest.raises(ValueError):
        LogEvent.model_validate({"service": "demo", "message": "Started", **changes})


def test_publishing_and_validation(client):
    event_id = str(uuid4())
    response = client.post("/logs", json={"id": event_id, "service": "demo", "message": "Started"})
    assert response.status_code == 202
    assert response.json() == {"id": event_id, "status": "queued"}
    assert client.post("/logs", json={"service": "demo", "message": " "}).status_code == 422


def test_kafka_failure_returns_503():
    class OfflinePublisher(FakePublisher):
        async def send(self, event):
            raise PublishingError("Offline")

    storage = ElasticsearchStorage(
        Settings(_env_file=None),
        client=httpx.AsyncClient(
            base_url="http://elasticsearch",
            transport=httpx.MockTransport(lambda request: httpx.Response(200, json={})),
        ),
    )
    with TestClient(create_app(storage=storage, publisher=OfflinePublisher())) as test_client:
        assert test_client.post("/logs", json={"service": "demo", "message": "Started"}).status_code == 503
