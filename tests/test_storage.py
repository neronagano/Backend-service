import json

import httpx
import pytest

from log_service.config import Settings
from log_service.models import LogEvent
from log_service.storage import ElasticsearchStorage, StorageError


async def test_bulk_partial_failure_is_not_treated_as_success():
    def handle(request):
        return httpx.Response(
            200,
            json={"errors": True, "items": [{"index": {"status": 201}}, {"index": {"status": 429, "error": {"type": "rejected"}}}]},
        )

    storage = ElasticsearchStorage(
        Settings(_env_file=None),
        client=httpx.AsyncClient(base_url="http://elasticsearch", transport=httpx.MockTransport(handle)),
    )
    try:
        with pytest.raises(StorageError):
            await storage.write_batch([LogEvent(service="demo", message="One"), LogEvent(service="demo", message="Two")])
    finally:
        await storage.close()


async def test_bulk_requests_are_bounded_and_replay_uses_same_ids():
    documents = {}
    request_sizes = []

    def handle(request):
        request_sizes.append(len(request.content))
        lines = request.content.decode("utf-8").splitlines()
        for action, source in zip(lines[0::2], lines[1::2], strict=True):
            documents[json.loads(action)["index"]["_id"]] = json.loads(source)
        return httpx.Response(200, json={"errors": False, "items": [{"index": {"status": 201}} for _ in lines[0::2]]})

    storage = ElasticsearchStorage(
        Settings(_env_file=None, bulk_max_bytes=131072),
        client=httpx.AsyncClient(base_url="http://elasticsearch", transport=httpx.MockTransport(handle)),
    )
    events = [LogEvent(service="demo", message="x" * 16000) for _ in range(10)]
    try:
        await storage.write_batch(events)
        await storage.write_batch(events)
    finally:
        await storage.close()
    assert len(request_sizes) == 4
    assert max(request_sizes) <= 131072
    assert len(documents) == 10
    assert set(documents) == {str(event.id) for event in events}
