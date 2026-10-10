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
