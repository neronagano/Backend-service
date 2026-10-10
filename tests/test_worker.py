import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiokafka import TopicPartition

from log_service.models import LogEvent
from log_service.storage import StorageError
from log_service.worker import process_records


def make_message(offset):
    event = LogEvent(service="demo", message=f"Message {offset}")
    return SimpleNamespace(offset=offset, value=event.model_dump_json().encode())


async def test_retry_finishes_before_offsets_are_committed(monkeypatch):
    calls = []

    async def write_batch(events):
        calls.append("write")
        if calls == ["write"]:
            raise StorageError("One bulk item failed")

    async def commit(offsets):
        calls.append("commit")
        assert offsets == {TopicPartition("app-logs", 0): 3, TopicPartition("app-logs", 1): 8}

    monkeypatch.setattr(asyncio, "sleep", AsyncMock())
    consumer = SimpleNamespace(commit=commit)
    storage = SimpleNamespace(write_batch=write_batch)
    await process_records(
        consumer,
        storage,
        {TopicPartition("app-logs", 0): [make_message(1), make_message(2)], TopicPartition("app-logs", 1): [make_message(7)]},
    )
    assert calls == ["write", "write", "commit"]


@pytest.mark.parametrize(
    "value",
    [b'{"service":"demo","message":""}', b'{"service":"demo","message":"Missing event id"}'],
)
async def test_invalid_kafka_message_is_not_acknowledged(value):
    consumer = SimpleNamespace(commit=AsyncMock())
    storage = SimpleNamespace(write_batch=AsyncMock())
    bad_message = SimpleNamespace(offset=5, value=value)
    with pytest.raises(ValueError):
        await process_records(consumer, storage, {TopicPartition("app-logs", 0): [bad_message]})
    consumer.commit.assert_not_awaited()
    storage.write_batch.assert_not_awaited()


async def test_interrupted_retry_does_not_commit(monkeypatch):
    consumer = SimpleNamespace(commit=AsyncMock())
    storage = SimpleNamespace(write_batch=AsyncMock(side_effect=StorageError("Offline")))
    monkeypatch.setattr(asyncio, "sleep", AsyncMock(side_effect=asyncio.CancelledError))
    with pytest.raises(asyncio.CancelledError):
        await process_records(consumer, storage, {TopicPartition("app-logs", 0): [make_message(5)]})
    consumer.commit.assert_not_awaited()
