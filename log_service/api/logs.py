from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import AwareDatetime

from log_service.api.dependencies import get_publisher, get_storage
from log_service.kafka import KafkaLogPublisher
from log_service.models import AcceptedLog, LogEvent, LogLevel, LogPage
from log_service.storage import ElasticsearchStorage

router = APIRouter(prefix="/logs", tags=["logs"])


@router.post("", response_model=AcceptedLog, status_code=202)
async def publish_log(
    event: LogEvent,
    publisher: Annotated[KafkaLogPublisher, Depends(get_publisher)],
):
    await publisher.send(event)
    return AcceptedLog(id=event.id)


@router.get("", response_model=LogPage)
async def search_logs(
    storage: Annotated[ElasticsearchStorage, Depends(get_storage)],
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
    return await storage.search(
        service=service,
        level=level,
        q=q,
        from_time=from_time,
        to_time=to_time,
        offset=offset,
        limit=limit,
    )


@router.get("/{log_id}", response_model=LogEvent)
async def get_log(
    log_id: UUID,
    storage: Annotated[ElasticsearchStorage, Depends(get_storage)],
):
    event = await storage.get(log_id)
    if event is None:
        raise HTTPException(404, "Log not found")
    return event
