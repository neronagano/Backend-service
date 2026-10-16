from datetime import UTC, datetime

from log_service.config import Settings
from log_service.kafka import KafkaLogPublisher


class DeadLetterPublisher(KafkaLogPublisher):
    """Keep the original value and key; identify retries by source coordinates."""

    def __init__(self, settings: Settings):
        super().__init__(settings, topic=settings.kafka_dlq_topic)

    async def send_record(self, partition, message, reason: str) -> None:
        headers = list(message.headers or [])
        metadata = {
            "dlq.source_topic": partition.topic,
            "dlq.source_partition": str(partition.partition),
            "dlq.source_offset": str(message.offset),
            "dlq.source_timestamp": str(message.timestamp),
            "dlq.reason": reason,
            "dlq.failed_at": datetime.now(UTC).isoformat(),
        }
        # Reserved headers are replaced so an external source cannot spoof them.
        headers = [(name, value) for name, value in headers if not name.startswith("dlq.")]
        headers.extend((name, value.encode("utf-8")) for name, value in metadata.items())
        if message.key is not None:
            headers.append(("dlq.source_key", message.key))
        await self.send_raw(
            key=f"{partition.topic}:{partition.partition}:{message.offset}".encode("utf-8"),
            value=message.value,
            headers=headers,
        )
