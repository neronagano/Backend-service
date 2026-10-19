from secrets import compare_digest
from typing import Annotated

from fastapi import HTTPException, Request, Security
from fastapi.security import APIKeyHeader

from log_service.kafka import KafkaLogPublisher
from log_service.storage import ElasticsearchStorage

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def require_api_key(
    request: Request, key: Annotated[str | None, Security(api_key_header)]
) -> None:
    expected = request.app.state.settings.api_key
    if expected is not None and (
        key is None
        or not compare_digest(key.encode("utf-8"), expected.get_secret_value().encode("utf-8"))
    ):
        raise HTTPException(
            401, "Invalid or missing API key", headers={"WWW-Authenticate": "APIKey"}
        )


async def get_storage(request: Request) -> ElasticsearchStorage:
    return request.app.state.storage


async def get_publisher(request: Request) -> KafkaLogPublisher:
    return request.app.state.publisher
