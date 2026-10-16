import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiokafka import TopicPartition

from log_service.kafka import PublishingError
from log_service.models import LogEvent
from log_service.storage import DocumentRejected, PermanentStorageError, StorageError
from log_service.worker import process_records
from log_service import worker
from log_service.config import Settings

PARTITION = TopicPartition("app-logs", 0)


def make_message(offset, *, value=None):
    event = LogEvent(service="demo", message=f"Message {offset}")
    return SimpleNamespace(offset=offset, value=value if value is not None else event.model_dump_json().encode(),
                           key=b"demo", headers=[], timestamp=123456789)


def make_dlq():
    return SimpleNamespace(send_record=AsyncMock())


async def test_retry_finishes_before_offsets_are_committed(monkeypatch):
    calls = []

    async def write_batch(events):
        calls.append("write")
        if calls == ["write"]:
            raise StorageError("One bulk item failed")

    async def commit(offsets):
        calls.append("commit")
        assert offsets == {PARTITION: 3, TopicPartition("app-logs", 1): 8}

    monkeypatch.setattr(asyncio, "sleep", AsyncMock())
    await process_records(
        SimpleNamespace(commit=commit), SimpleNamespace(write_batch=write_batch),
        {PARTITION: [make_message(1), make_message(2)], TopicPartition("app-logs", 1): [make_message(7)]}, make_dlq(),
    )
    assert calls == ["write", "write", "commit"]


@pytest.mark.parametrize("value", [b"not-json", b'{"service":"demo","message":"Missing id"}', b"null"])
async def test_invalid_event_is_saved_to_dlq_before_commit(value):
    order = []
    message = make_message(5, value=value)
    storage = SimpleNamespace(write_batch=AsyncMock())

    async def send(partition, record, reason):
        assert partition == PARTITION
        assert record is message
        assert reason == "invalid_event"
        order.append("dlq")

    async def commit(offsets):
        assert offsets == {PARTITION: 6}
        order.append("commit")

    await process_records(SimpleNamespace(commit=commit), storage, {PARTITION: [message]}, SimpleNamespace(send_record=send))
    assert order == ["dlq", "commit"]
    storage.write_batch.assert_not_awaited()


async def test_dlq_failure_never_acknowledges_source():
    consumer = SimpleNamespace(commit=AsyncMock())
    storage = SimpleNamespace(write_batch=AsyncMock())
    dead_letters = make_dlq()
    dead_letters.send_record.side_effect = PublishingError("Offline")
    with pytest.raises(PublishingError):
        await process_records(consumer, storage, {PARTITION: [make_message(5, value=b"broken")]}, dead_letters)
    consumer.commit.assert_not_awaited()
    storage.write_batch.assert_not_awaited()


async def test_permanent_storage_failure_does_not_retry_or_commit():
    consumer = SimpleNamespace(commit=AsyncMock())
    storage = SimpleNamespace(write_batch=AsyncMock(side_effect=PermanentStorageError("Access denied")))
    dead_letters = make_dlq()
    with pytest.raises(PermanentStorageError):
        await process_records(consumer, storage, {PARTITION: [make_message(5)]}, dead_letters)
    storage.write_batch.assert_awaited_once()
    consumer.commit.assert_not_awaited()
    dead_letters.send_record.assert_not_awaited()


async def test_rejected_document_does_not_discard_valid_documents():
    indexed = []
    order = []

    async def write(events):
        order.append("write")
        rejected = [(event, "mapping_error") for event in events if event.message == "Message 6"]
        if rejected:
            raise DocumentRejected(rejected)
        indexed.extend(events)

    async def send(*args):
        order.append("dlq")

    async def commit(offsets):
        assert offsets == {PARTITION: 8}
        order.append("commit")

    messages = [make_message(5), make_message(6), make_message(7)]
    dead_letters = SimpleNamespace(send_record=AsyncMock(side_effect=send))
    await process_records(SimpleNamespace(commit=commit), SimpleNamespace(write_batch=write), {PARTITION: messages}, dead_letters)
    assert [event.message for event in indexed] == ["Message 5", "Message 7"]
    dead_letters.send_record.assert_awaited_once_with(PARTITION, messages[1], "mapping_error")
    assert order == ["write", "dlq", "write", "commit"]


async def test_interrupted_retry_does_not_commit(monkeypatch):
    consumer = SimpleNamespace(commit=AsyncMock())
    storage = SimpleNamespace(write_batch=AsyncMock(side_effect=StorageError("Offline")))
    monkeypatch.setattr(asyncio, "sleep", AsyncMock(side_effect=asyncio.CancelledError))
    with pytest.raises(asyncio.CancelledError):
        await process_records(consumer, storage, {PARTITION: [make_message(5)]}, make_dlq())
    consumer.commit.assert_not_awaited()


async def test_batch_timeout_leaves_offsets_uncommitted():
    consumer = SimpleNamespace(commit=AsyncMock())

    async def write(events):
        await asyncio.Event().wait()

    with pytest.raises(TimeoutError):
        await process_records(consumer, SimpleNamespace(write_batch=write), {PARTITION: [make_message(5)]}, make_dlq(), timeout=0.02)
    consumer.commit.assert_not_awaited()


async def test_empty_batch_does_not_commit():
    consumer = SimpleNamespace(commit=AsyncMock())
    storage = SimpleNamespace(write_batch=AsyncMock())
    await process_records(consumer, storage, {PARTITION: []}, make_dlq())
    storage.write_batch.assert_not_awaited()
    consumer.commit.assert_not_awaited()


async def test_failed_worker_startup_closes_every_resource(monkeypatch):
    storage = SimpleNamespace(ensure_index=AsyncMock(), close=AsyncMock())
    publisher = SimpleNamespace(start=AsyncMock(), stop=AsyncMock())
    consumer = SimpleNamespace(start=AsyncMock(side_effect=RuntimeError("Consumer failed")),
                               stop=AsyncMock(side_effect=RuntimeError("Stop failed")))
    monkeypatch.setattr(worker, "ElasticsearchStorage", lambda settings: storage)
    monkeypatch.setattr(worker, "DeadLetterPublisher", lambda settings: publisher)
    monkeypatch.setattr(worker, "AIOKafkaConsumer", lambda *args, **kwargs: consumer)
    with pytest.raises(RuntimeError, match="Stop failed"):
        await worker.run_worker(Settings(_env_file=None))
    consumer.stop.assert_awaited_once()
    publisher.stop.assert_awaited_once()
    storage.close.assert_awaited_once()
