import json
from datetime import datetime
from uuid import UUID

import httpx

from log_service.config import Settings
from log_service.models import LogEvent, LogLevel, LogPage
from log_service.schema import INDEX_MAPPING, LOG_PROPERTIES, SCHEMA_VERSION


class StorageError(Exception):
    pass


class PermanentStorageError(StorageError):
    """An index/configuration error that retrying cannot fix."""


class DocumentRejected(StorageError):
    def __init__(self, rejected: list[tuple[LogEvent, str]]):
        self.rejected = rejected
        super().__init__(f"Elasticsearch rejected {len(rejected)} documents")


class ElasticsearchStorage:
    def __init__(self, settings: Settings, client: httpx.AsyncClient | None = None):
        self.index = settings.elasticsearch_index
        self.max_bulk_bytes = settings.bulk_max_bytes
        self.auto_migrate = settings.auto_migrate
        self.client = client or httpx.AsyncClient(
            base_url=settings.elasticsearch_url,
            timeout=settings.request_timeout,
            limits=httpx.Limits(max_connections=20),
        )

    async def close(self) -> None:
        await self.client.aclose()

    async def request(self, method: str, path: str, **kwargs) -> httpx.Response:
        try:
            return await self.client.request(method, path, **kwargs)
        except httpx.HTTPError as exc:
            raise StorageError("Cannot reach Elasticsearch") from exc

    @staticmethod
    def read_json(response: httpx.Response) -> dict:
        try:
            data = response.json()
        except ValueError as exc:
            raise StorageError("Invalid Elasticsearch response") from exc
        if not isinstance(data, dict):
            raise StorageError("Expected an Elasticsearch JSON object")
        return data

    @staticmethod
    def check_response(response: httpx.Response, operation: str) -> None:
        if response.is_success:
            return
        error = (
            StorageError
            if response.status_code in (408, 429) or response.status_code >= 500
            else PermanentStorageError
        )
        raise error(f"{operation}: HTTP {response.status_code}")

    async def ensure_index(self) -> None:
        if self.auto_migrate:
            await self.migrate_index()
        else:
            await self.check_schema()

    async def migrate_index(self) -> None:
        response = await self.request(
            "PUT",
            f"/{self.index}",
            json={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": INDEX_MAPPING,
            },
        )
        if response.is_success:
            if self.read_json(response).get("acknowledged") is not True:
                raise StorageError("Log index creation was not acknowledged")
            return
        error = self.read_json(response).get("error", {})
        if response.status_code == 400 and isinstance(error, dict):
            if error.get("type") == "resource_already_exists_exception":
                mapping = await self.read_mapping()
                version = self.validate_mapping(mapping)
                if version == SCHEMA_VERSION:
                    return
                response = await self.request(
                    "PUT",
                    f"/{self.index}/_mapping",
                    json={
                        **INDEX_MAPPING,
                        "_meta": {**mapping.get("_meta", {}), **INDEX_MAPPING["_meta"]},
                    },
                )
                self.check_response(response, "Cannot update log schema")
                if self.read_json(response).get("acknowledged") is not True:
                    raise StorageError("Log schema migration was not acknowledged")
                return
        self.check_response(response, "Cannot create log index")

    async def read_mapping(self) -> dict:
        response = await self.request("GET", f"/{self.index}/_mapping")
        self.check_response(response, "Cannot read log schema")
        data = self.read_json(response)
        if set(data) != {self.index}:
            raise PermanentStorageError(
                "Expected a concrete log index; aliases require an explicit migration"
            )
        try:
            mapping = data[self.index]["mappings"]
            if not isinstance(mapping, dict):
                raise TypeError
            return mapping
        except (KeyError, TypeError) as exc:
            raise StorageError("Invalid log index mapping") from exc

    @staticmethod
    def validate_mapping(mapping: dict) -> int:
        properties = mapping.get("properties", {})
        metadata = mapping.get("_meta", {})
        if not isinstance(properties, dict) or not isinstance(metadata, dict):
            raise PermanentStorageError("Invalid log index schema metadata")
        version = metadata.get("log_service_schema_version", 0)
        if type(version) is not int or not 0 <= version <= SCHEMA_VERSION:
            raise PermanentStorageError(
                "Log schema is newer than this application or has an invalid version"
            )
        for name, expected in LOG_PROPERTIES.items():
            actual = properties.get(name)
            if actual is None and version == 0:
                continue
            if not isinstance(actual, dict) or actual.get("type") != expected["type"]:
                raise PermanentStorageError(
                    f"Incompatible mapping for field {name}; reindex before upgrading"
                )
        if version and mapping.get("dynamic") != "strict":
            raise PermanentStorageError("The log index must use a strict mapping")
        return version

    async def check_schema(self) -> None:
        if self.validate_mapping(await self.read_mapping()) != SCHEMA_VERSION:
            raise PermanentStorageError(
                "Log schema needs migration: run python -m log_service.migrations"
            )

    async def is_healthy(self) -> bool:
        try:
            response = await self.request(
                "GET",
                f"/_cluster/health/{self.index}",
                params={"wait_for_status": "yellow", "timeout": "2s"},
            )
            data = self.read_json(response)
            return (
                response.is_success
                and not data.get("timed_out", False)
                and data.get("status") in ("green", "yellow")
            )
        except StorageError:
            return False

    async def write_batch(self, events: list[LogEvent]) -> None:
        chunks = []
        chunk_events = []
        size = 0
        for event in events:
            action = json.dumps({"index": {"_id": str(event.id)}}, separators=(",", ":"))
            entry = (action + "\n" + event.model_dump_json() + "\n").encode("utf-8")
            if len(entry) > self.max_bulk_bytes:
                raise DocumentRejected([(event, "document_too_large")])
            if chunks and size + len(entry) > self.max_bulk_bytes:
                await self.send_bulk(b"".join(chunks), chunk_events)
                chunks, chunk_events, size = [], [], 0
            chunks.append(entry)
            chunk_events.append(event)
            size += len(entry)
        if chunks:
            await self.send_bulk(b"".join(chunks), chunk_events)

    async def send_bulk(self, content: bytes, events: list[LogEvent]) -> None:
        response = await self.request(
            "POST",
            f"/{self.index}/_bulk",
            content=content,
            headers={"Content-Type": "application/x-ndjson"},
        )
        self.check_response(response, "Bulk write failed")
        data = self.read_json(response)
        items = data.get("items", [])
        if not isinstance(items, list) or len(items) != len(events):
            raise StorageError("Incomplete bulk response")
        rejected = []
        for event, item in zip(events, items, strict=True):
            result = item.get("index") if isinstance(item, dict) else None
            status = result.get("status") if isinstance(result, dict) else None
            if type(status) is not int:
                raise StorageError("Invalid bulk item status")
            if status in (408, 429) or status >= 500:
                raise StorageError(f"A bulk item failed: status {status}")
            if status in (401, 403, 404):
                raise PermanentStorageError(
                    f"Bulk index is unavailable or access is denied: status {status}"
                )
            if status not in (200, 201) or result.get("error"):
                rejected.append((event, f"index_rejected_http_{status}"))
        if rejected:
            raise DocumentRejected(rejected)
        if data.get("errors"):
            raise StorageError("Some logs were not indexed")

    async def search(
        self,
        *,
        service: str | None,
        level: LogLevel | None,
        q: str | None,
        from_time: datetime | None,
        to_time: datetime | None,
        offset: int,
        limit: int,
    ) -> LogPage:
        filters = []
        if service:
            filters.append({"term": {"service": service}})
        if level:
            filters.append({"term": {"level": level.value}})
        bounds = {}
        if from_time:
            bounds["gte"] = from_time.isoformat()
        if to_time:
            bounds["lte"] = to_time.isoformat()
        if bounds:
            filters.append({"range": {"timestamp": bounds}})
        must = [{"match": {"message": {"query": q, "operator": "and"}}}] if q else []
        response = await self.request(
            "POST",
            f"/{self.index}/_search",
            json={
                "query": {"bool": {"filter": filters, "must": must}},
                "sort": [{"timestamp": "desc"}, {"id": "asc"}],
                "from": offset,
                "size": limit,
                "track_total_hits": True,
            },
        )
        self.check_response(response, "Log search failed")
        try:
            hits = self.read_json(response)["hits"]
            return LogPage(
                items=[LogEvent.model_validate(hit["_source"]) for hit in hits["hits"]],
                total=hits["total"]["value"],
                offset=offset,
                limit=limit,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise StorageError("Invalid Elasticsearch search response") from exc

    async def get(self, log_id: UUID) -> LogEvent | None:
        response = await self.request("GET", f"/{self.index}/_doc/{log_id}")
        if response.status_code == 404:
            if self.read_json(response).get("found") is False:
                return None
            raise PermanentStorageError("The log index is missing")
        self.check_response(response, "Cannot read log")
        try:
            return LogEvent.model_validate(self.read_json(response)["_source"])
        except (KeyError, TypeError, ValueError) as exc:
            raise StorageError("Invalid Elasticsearch document response") from exc
