from typing import Annotated

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from log_service.api.dependencies import get_storage
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
