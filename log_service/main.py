from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import AwareDatetime

from log_service.config import Settings, get_settings
from log_service.models import LogEvent, LogLevel, LogPage
from log_service.storage import ElasticsearchStorage, StorageError


def create_app(
    settings: Settings | None = None, storage: ElasticsearchStorage | None = None
) -> FastAPI:
    configuration = settings or get_settings()

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        store = storage or ElasticsearchStorage(configuration)
        try:
            await store.ensure_index()
            application.state.storage = store
            yield
        finally:
            await store.close()

    application = FastAPI(
        title="Log Service",
        description="Collect and search application logs",
        version="0.1.0",
        lifespan=lifespan,
    )

    @application.exception_handler(StorageError)
    async def storage_error_handler(request: Request, exc: StorageError):
        return JSONResponse(status_code=503, content={"detail": "Elasticsearch is unavailable"})

    @application.get("/health", tags=["health"])
    async def health(request: Request):
        healthy = await request.app.state.storage.is_healthy()
        return JSONResponse(
            status_code=200 if healthy else 503,
            content={"status": "ok" if healthy else "unavailable", "elasticsearch": healthy},
        )

    @application.get("/logs", response_model=LogPage, tags=["logs"])
    async def search_logs(
        request: Request,
        service: Annotated[str | None, Query(min_length=1, max_length=128)] = None,
        level: LogLevel | None = None,
        q: Annotated[str | None, Query(min_length=1, max_length=1000)] = None,
        from_time: AwareDatetime | None = None,
        to_time: AwareDatetime | None = None,
        offset: Annotated[int, Query(ge=0)] = 0,
        limit: Annotated[int, Query(ge=1, le=1000)] = 50,
    ):
        if offset + limit > 10000:
            raise HTTPException(422, "Pagination window must not exceed 10000 logs")
        if from_time and to_time and from_time > to_time:
            raise HTTPException(422, "from_time must not be later than to_time")
        return await request.app.state.storage.search(
            service=service,
            level=level,
            q=q,
            from_time=from_time,
            to_time=to_time,
            offset=offset,
            limit=limit,
        )

    @application.get("/logs/{log_id}", response_model=LogEvent, tags=["logs"])
    async def get_log(log_id: UUID, request: Request):
        event = await request.app.state.storage.get(log_id)
        if event is None:
            raise HTTPException(404, "Log not found")
        return event

    return application


app = create_app()
