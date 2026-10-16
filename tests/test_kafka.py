import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiokafka import TopicPartition
from aiokafka.errors import KafkaError

from log_service.config import Settings
from log_service.dlq import DeadLetterPublisher
from log_service.kafka import KafkaLogPublisher, PublishingError
from log_service.models import LogEvent


async def test_dlq_preserves_original_bytes_and_source_coordinates():
    publisher = DeadLetterPublisher(Settings(_env_file=None))
    publisher.producer = SimpleNamespace(send_and_wait=AsyncMock())
    record = SimpleNamespace(value=b"\xff invalid json", key=b"original", offset=42, timestamp=123,
                             headers=[("trace", b"abc"), ("dlq.source_offset", b"spoof")])
    await publisher.send_record(TopicPartition("app-logs", 2), record, "invalid_event")
    args, kwargs = publisher.producer.send_and_wait.call_args
    assert args == ("app-logs-dlq",)
    assert kwargs["value"] == record.value
    assert kwargs["key"] == b"app-logs:2:42"
    headers = dict(kwargs["headers"])
    assert headers["dlq.source_offset"] == b"42"
    assert headers["dlq.source_key"] == b"original"
    assert headers["trace"] == b"abc"
    assert headers["dlq.reason"] == b"invalid_event"


async def test_kafka_failure_becomes_publishing_error():
    publisher = KafkaLogPublisher(Settings(_env_file=None))
    publisher.producer = SimpleNamespace(send_and_wait=AsyncMock(side_effect=KafkaError("Offline")))
    with pytest.raises(PublishingError):
        await publisher.send(LogEvent(service="demo", message="One"))


async def test_publish_wait_is_bounded():
    publisher = KafkaLogPublisher(Settings(_env_file=None, publish_timeout=0.02))

    async def send(*args, **kwargs):
        await asyncio.Event().wait()

    publisher.producer = SimpleNamespace(send_and_wait=send)
    with pytest.raises(PublishingError):
        await publisher.send(LogEvent(service="demo", message="One"))


async def test_stopped_publisher_rejects_new_messages():
    publisher = KafkaLogPublisher(Settings(_env_file=None))
    producer = SimpleNamespace(stop=AsyncMock())
    publisher.producer = producer
    await publisher.stop()
    await publisher.stop()
    producer.stop.assert_awaited_once()
    with pytest.raises(PublishingError):
        await publisher.send(LogEvent(service="demo", message="One"))


@pytest.mark.parametrize("values", [
    {"kafka_topic": "bad topic"}, {"kafka_dlq_topic": ".."}, {"kafka_dlq_topic": "app-logs"},
    {"worker_batch_timeout": 0}, {"publish_timeout": -1},
])
def test_invalid_kafka_configuration_is_rejected(values):
    with pytest.raises(ValueError):
        Settings(_env_file=None, **values)
