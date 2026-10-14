from fastapi import Request

from log_service.kafka import KafkaLogPublisher
from log_service.storage import ElasticsearchStorage


async def get_storage(request: Request) -> ElasticsearchStorage:
    return request.app.state.storage


async def get_publisher(request: Request) -> KafkaLogPublisher:
    return request.app.state.publisher
