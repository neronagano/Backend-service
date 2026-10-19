import asyncio
from typing import Annotated

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from log_service.api.dependencies import get_publisher, get_storage
from log_service.kafka import KafkaLogPublisher
from log_service.storage import ElasticsearchStorage

router = APIRouter(tags=["health"])


@router.get("/live")
async def live():
    return {"status": "ok"}


@router.get("/health")
async def health(storage: Annotated[ElasticsearchStorage, Depends(get_storage)]):
    healthy = await storage.is_healthy()
    return JSONResponse(
        status_code=200 if healthy else 503,
        content={"status": "ok" if healthy else "unavailable", "elasticsearch": healthy},
    )


@router.get("/ready")
async def ready(
    storage: Annotated[ElasticsearchStorage, Depends(get_storage)],
    publisher: Annotated[KafkaLogPublisher, Depends(get_publisher)],
):
    elasticsearch, kafka = await asyncio.gather(storage.is_healthy(), publisher.is_healthy())
    healthy = elasticsearch and kafka
    return JSONResponse(
        status_code=200 if healthy else 503,
        content={
            "status": "ok" if healthy else "unavailable",
            "elasticsearch": elasticsearch,
            "kafka": kafka,
        },
    )
