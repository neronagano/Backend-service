import asyncio
import logging

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from log_service.config import Settings
from log_service.models import LogEvent

logger = logging.getLogger(__name__)


class PublishingError(Exception):
    pass


class KafkaLogPublisher:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.settings.kafka_bootstrap_servers,
            enable_idempotence=True,
            acks="all",
            compression_type="gzip",
            linger_ms=10,
            request_timeout_ms=10000,
        )
        await self.producer.start()

    async def stop(self) -> None:
        if self.producer is not None:
            try:
                await asyncio.wait_for(self.producer.stop(), timeout=10)
            except TimeoutError:
                logger.warning("Kafka producer shutdown timed out")

    async def send(self, event: LogEvent) -> None:
        if self.producer is None:
            raise PublishingError("Kafka producer is not running")
        try:
            await asyncio.wait_for(
                self.producer.send_and_wait(
                    self.settings.kafka_topic,
                    key=event.service.encode("utf-8"),
                    value=event.model_dump_json().encode("utf-8"),
                ),
                timeout=10,
            )
        except (KafkaError, TimeoutError) as exc:
            raise PublishingError("Cannot publish log to Kafka") from exc
