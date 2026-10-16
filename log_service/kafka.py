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
    def __init__(self, settings: Settings, *, topic: str | None = None):
        self.settings = settings
        self.topic = topic if topic is not None else settings.kafka_topic
        self.producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.settings.kafka_bootstrap_servers,
            enable_idempotence=True,
            acks="all",
            compression_type="gzip",
            linger_ms=10,
            request_timeout_ms=10000,
            max_request_size=2097152,
        )
        async with asyncio.timeout(self.settings.startup_timeout):
            await self.producer.start()
            await self.producer.partitions_for(self.topic)

    async def stop(self) -> None:
        if self.producer is not None:
            producer, self.producer = self.producer, None
            try:
                await asyncio.wait_for(producer.stop(), timeout=self.settings.shutdown_timeout)
            except TimeoutError:
                logger.warning("Kafka producer shutdown timed out")

    async def send(self, event: LogEvent) -> None:
        await self.send_raw(
            key=event.service.encode("utf-8"),
            value=event.model_dump_json().encode("utf-8"),
        )

    async def send_raw(self, *, key: bytes, value: bytes | None, headers: list | None = None) -> None:
        if self.producer is None:
            raise PublishingError("Kafka producer is not running")
        try:
            await asyncio.wait_for(
                self.producer.send_and_wait(
                    self.topic, key=key, value=value, headers=headers,
                ),
                timeout=self.settings.publish_timeout,
            )
        except (KafkaError, TimeoutError) as exc:
            raise PublishingError("Cannot publish log to Kafka") from exc
