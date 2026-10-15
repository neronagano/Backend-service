from copy import deepcopy
import json

import httpx
import pytest

from log_service.config import Settings
from log_service.schema import INDEX_MAPPING, LOG_PROPERTIES
from log_service.storage import ElasticsearchStorage, PermanentStorageError


def make_storage(mapping, calls, *, auto_migrate=True):
    def handle(request):
        calls.append(request)
        if request.method == "GET":
            return httpx.Response(200, json={"app-logs": {"mappings": mapping}})
        if request.url.path == "/app-logs":
            return httpx.Response(400, json={"error": {"type": "resource_already_exists_exception"}})
        return httpx.Response(200, json={"acknowledged": True})

    return ElasticsearchStorage(
        Settings(_env_file=None, auto_migrate=auto_migrate),
        client=httpx.AsyncClient(base_url="http://elasticsearch", transport=httpx.MockTransport(handle)),
    )


async def test_legacy_index_is_upgraded_without_deleting_documents():
    calls = []
    mapping = {"properties": LOG_PROPERTIES, "_meta": {"owner": "payments"}}
    storage = make_storage(mapping, calls)
    try:
        await storage.ensure_index()
    finally:
        await storage.close()
    assert [(r.method, r.url.path) for r in calls] == [
        ("PUT", "/app-logs"), ("GET", "/app-logs/_mapping"), ("PUT", "/app-logs/_mapping"),
    ]
    assert json.loads(calls[-1].content)["_meta"] == {"owner": "payments", "log_service_schema_version": 1}


async def test_current_schema_does_not_trigger_another_migration():
    calls = []
    storage = make_storage(INDEX_MAPPING, calls)
    try:
        await storage.migrate_index()
    finally:
        await storage.close()
    assert len(calls) == 2


@pytest.mark.parametrize("version", [0, 2, "1", True])
async def test_check_schema_is_read_only_and_rejects_missing_or_newer_version(version):
    calls = []
    mapping = deepcopy(INDEX_MAPPING)
    mapping["_meta"]["log_service_schema_version"] = version
    storage = make_storage(mapping, calls, auto_migrate=False)
    try:
        with pytest.raises(PermanentStorageError):
            await storage.ensure_index()
    finally:
        await storage.close()
    assert [r.method for r in calls] == ["GET"]


async def test_incompatible_mapping_is_rejected_before_writing():
    calls = []
    mapping = {"properties": {"service": {"type": "text"}}}
    storage = make_storage(mapping, calls)
    try:
        with pytest.raises(PermanentStorageError, match="service"):
            await storage.migrate_index()
    finally:
        await storage.close()
    assert not any(r.url.path.endswith("/_mapping") and r.method == "PUT" for r in calls)
