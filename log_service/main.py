import asyncio
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager

from fastapi import FastAPI

from log_service.api.errors import publishing_error_handler, storage_error_handler
from log_service.api.health import router as health_router
from log_service.api.logs import router as logs_router
from log_service.api.middleware import RequestSizeLimit
from log_service.config import Settings, get_settings
from log_service.kafka import KafkaLogPublisher, PublishingError
from log_service.storage import ElasticsearchStorage, StorageError


def create_app(
    settings: Settings | None = None,
    storage: ElasticsearchStorage | None = None,
    publisher: KafkaLogPublisher | None = None,
) -> FastAPI:
    configuration = settings if settings is not None else get_settings()

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        async with AsyncExitStack() as stack:
            store = storage if storage is not None else ElasticsearchStorage(configuration)
            stack.push_async_callback(store.close)
            sender = publisher if publisher is not None else KafkaLogPublisher(configuration)
            stack.push_async_callback(sender.stop)
            async with asyncio.timeout(configuration.startup_timeout):
                await store.ensure_index()
                await sender.start()
            application.state.storage = store
            application.state.publisher = sender
            yield

    application = FastAPI(
        title="Log Service",
        description="Collect and search application logs",
        version="0.1.0",
        lifespan=lifespan,
        docs_url="/docs" if configuration.docs_enabled else None,
        redoc_url=None,
        openapi_url="/openapi.json" if configuration.docs_enabled else None,
    )
    application.state.settings = configuration
    application.add_middleware(
        RequestSizeLimit,
        max_bytes=configuration.max_request_bytes,
        timeout=configuration.body_read_timeout,
    )
    application.add_exception_handler(StorageError, storage_error_handler)
    application.add_exception_handler(PublishingError, publishing_error_handler)
    application.include_router(logs_router)
    application.include_router(health_router)
    return application


app = create_app()
