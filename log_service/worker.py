import asyncio
import logging
import os
import random
import signal
from collections import defaultdict
from contextlib import AsyncExitStack
from time import monotonic

from aiokafka import AIOKafkaConsumer

from log_service.config import Settings, get_settings
from log_service.dlq import DeadLetterPublisher
from log_service.models import KafkaEvent
from log_service.storage import (
    DocumentRejected,
    ElasticsearchStorage,
    PermanentStorageError,
    StorageError,
)

logger = logging.getLogger(__name__)


async def process_records(
    consumer,
    storage: ElasticsearchStorage,
    records: dict,
    dead_letters: DeadLetterPublisher,
    *,
    timeout: float = 120,
) -> None:
    async with asyncio.timeout(timeout):
        events = []
        sources = {}
        offsets = {}
        rejected_count = 0
        for partition, messages in records.items():
            for message in messages:
                try:
                    event = KafkaEvent.model_validate_json(message.value)
                except (ValueError, TypeError):
                    await dead_letters.send_record(partition, message, "invalid_event")
                    rejected_count += 1
                    logger.warning(
                        "Invalid event sent to DLQ: topic=%s partition=%d offset=%d",
                        partition.topic,
                        partition.partition,
                        message.offset,
                    )
                else:
                    events.append(event)
                    sources[id(event)] = (partition, message)
            if messages:
                offsets[partition] = messages[-1].offset + 1
        if not offsets:
            return

        delay = 1.0
        while events:
            try:
                await storage.write_batch(events)
                break
            except DocumentRejected as exc:
                rejected_ids = set()
                for event, reason in exc.rejected:
                    partition, message = sources[id(event)]
                    await dead_letters.send_record(partition, message, reason)
                    rejected_ids.add(id(event))
                    rejected_count += 1
                    logger.warning(
                        "Rejected document sent to DLQ: topic=%s partition=%d offset=%d reason=%s",
                        partition.topic,
                        partition.partition,
                        message.offset,
                        reason,
                    )
                events = [event for event in events if id(event) not in rejected_ids]
            except PermanentStorageError:
                raise
            except StorageError as exc:
                wait = min(delay + random.random(), 10)
                logger.warning("Bulk write failed; retrying in %.1fs: %s", wait, exc)
                await asyncio.sleep(wait)
                delay = min(delay * 2, 10)

        await consumer.commit(offsets)
        logger.info("Committed batch: indexed=%d dead_letters=%d", len(events), rejected_count)


async def stop_consumer(consumer, timeout: float) -> None:
    try:
        await asyncio.wait_for(consumer.stop(), timeout=timeout)
    except TimeoutError:
        logger.warning("Kafka consumer shutdown timed out; uncommitted records will be replayed")


async def run_worker(settings: Settings | None = None) -> None:
    configuration = settings if settings is not None else get_settings()
    async with AsyncExitStack() as stack:
        if os.name != "nt":
            loop = asyncio.get_running_loop()
            task = asyncio.current_task()
            loop.add_signal_handler(signal.SIGTERM, task.cancel)
            stack.callback(loop.remove_signal_handler, signal.SIGTERM)

        storage = ElasticsearchStorage(configuration)
        stack.push_async_callback(storage.close)
        dead_letters = DeadLetterPublisher(configuration)
        stack.push_async_callback(dead_letters.stop)
        consumer = AIOKafkaConsumer(
            configuration.kafka_topic,
            bootstrap_servers=configuration.kafka_bootstrap_servers,
            group_id=configuration.kafka_group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=configuration.batch_size,
            max_poll_interval_ms=600000,
            fetch_max_bytes=8388608,
        )
        stack.push_async_callback(stop_consumer, consumer, configuration.shutdown_timeout)
        async with asyncio.timeout(configuration.startup_timeout):
            await storage.ensure_index()
            await dead_letters.start()
            await consumer.start()
        logger.info(
            "Worker started: topic=%s dlq=%s batch_size=%d",
            configuration.kafka_topic,
            configuration.kafka_dlq_topic,
            configuration.batch_size,
        )
        pending = defaultdict(list)
        count = 0
        first_message_at = None
        while True:
            remaining = configuration.flush_interval
            if first_message_at is not None:
                remaining -= monotonic() - first_message_at
            received = await consumer.getmany(
                timeout_ms=max(1, min(1000, int(remaining * 1000))),
                max_records=configuration.batch_size - count,
            )
            for partition, messages in received.items():
                if messages:
                    if first_message_at is None:
                        first_message_at = monotonic()
                    pending[partition].extend(messages)
                    count += len(messages)
            due = (
                first_message_at is not None
                and monotonic() - first_message_at >= configuration.flush_interval
            )
            if count and (count >= configuration.batch_size or due):
                await process_records(
                    consumer,
                    storage,
                    pending,
                    dead_letters,
                    timeout=configuration.worker_batch_timeout,
                )
                pending.clear()
                count, first_message_at = 0, None


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    try:
        asyncio.run(run_worker())
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Worker stopped; pending records remain uncommitted")


if __name__ == "__main__":
    main()
