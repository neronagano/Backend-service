import json
from datetime import datetime
from uuid import UUID

import httpx

from log_service.config import Settings
from log_service.models import LogEvent, LogLevel, LogPage


class StorageError(Exception):
    pass


class ElasticsearchStorage:
    def __init__(self, settings: Settings, client: httpx.AsyncClient | None = None):
        self.index = settings.elasticsearch_index
        self.max_bulk_bytes = settings.bulk_max_bytes
        self.client = client or httpx.AsyncClient(
            base_url=settings.elasticsearch_url,
            timeout=10,
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
            return response.json()
        except ValueError as exc:
            raise StorageError("Invalid Elasticsearch response") from exc

    async def ensure_index(self) -> None:
        response = await self.request(
            "PUT",
            f"/{self.index}",
            json={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {
                    "dynamic": "strict",
                    "properties": {
                        "id": {"type": "keyword"},
                        "timestamp": {"type": "date"},
                        "service": {"type": "keyword"},
                        "level": {"type": "keyword"},
                        "message": {"type": "text"},
                        "trace_id": {"type": "keyword"},
                    },
                },
            },
        )
        if response.is_success:
            return
        error = self.read_json(response).get("error", {})
        if response.status_code == 400 and isinstance(error, dict):
            if error.get("type") == "resource_already_exists_exception":
                return
        raise StorageError(f"Cannot create log index: HTTP {response.status_code}")

    async def is_healthy(self) -> bool:
        try:
            return (await self.request("GET", "/")).is_success
        except StorageError:
            return False

    async def write_batch(self, events: list[LogEvent]) -> None:
        chunks = []
        size = 0
        for event in events:
            action = json.dumps({"index": {"_id": str(event.id)}}, separators=(",", ":"))
            entry = (action + "\n" + event.model_dump_json() + "\n").encode("utf-8")
            if len(entry) > self.max_bulk_bytes:
                raise StorageError("A log exceeds the bulk request size limit")
            if chunks and size + len(entry) > self.max_bulk_bytes:
                await self.send_bulk(b"".join(chunks), len(chunks))
                chunks, size = [], 0
            chunks.append(entry)
            size += len(entry)
        if chunks:
            await self.send_bulk(b"".join(chunks), len(chunks))

    async def send_bulk(self, content: bytes, expected_count: int) -> None:
        response = await self.request(
            "POST",
            f"/{self.index}/_bulk",
            content=content,
            headers={"Content-Type": "application/x-ndjson"},
        )
        if not response.is_success:
            raise StorageError(f"Bulk write failed: HTTP {response.status_code}")
        data = self.read_json(response)
        items = data.get("items", [])
        if len(items) != expected_count:
            raise StorageError("Incomplete bulk response")
        for item in items:
            result = item.get("index", {})
            if result.get("status") not in (200, 201) or result.get("error"):
                raise StorageError(f"A bulk item failed: status {result.get('status')}")
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
        if not response.is_success:
            raise StorageError(f"Log search failed: HTTP {response.status_code}")
        hits = self.read_json(response)["hits"]
        return LogPage(
            items=[LogEvent.model_validate(hit["_source"]) for hit in hits["hits"]],
            total=hits["total"]["value"],
            offset=offset,
            limit=limit,
        )

    async def get(self, log_id: UUID) -> LogEvent | None:
        response = await self.request("GET", f"/{self.index}/_doc/{log_id}")
        if response.status_code == 404:
            return None
        if not response.is_success:
            raise StorageError(f"Cannot read log: HTTP {response.status_code}")
        return LogEvent.model_validate(self.read_json(response)["_source"])
