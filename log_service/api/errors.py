import logging

from fastapi import Request
from fastapi.responses import JSONResponse

from log_service.kafka import PublishingError
from log_service.storage import StorageError

logger = logging.getLogger(__name__)


async def storage_error_handler(request: Request, exc: StorageError) -> JSONResponse:
    logger.warning("Elasticsearch request failed: %s", exc)
    return JSONResponse(status_code=503, content={"detail": "Elasticsearch is unavailable"})


async def publishing_error_handler(request: Request, exc: PublishingError) -> JSONResponse:
    logger.warning("Kafka publishing failed: %s", exc)
    return JSONResponse(status_code=503, content={"detail": "Kafka is unavailable"})
