import asyncio
import logging
import os
import random
import signal
from collections import defaultdict
from time import monotonic

from aiokafka import AIOKafkaConsumer

from log_service.config import Settings, get_settings
from log_service.models import KafkaEvent
from log_service.storage import ElasticsearchStorage, StorageError

logger = logging.getLogger(__name__)


async def process_records(consumer, storage: ElasticsearchStorage, records: dict) -> None:
    events = []
    offsets = {}
    for partition, messages in records.items():
        if messages:
            events.extend(KafkaEvent.model_validate_json(message.value) for message in messages)
            offsets[partition] = messages[-1].offset + 1
    if not events:
        return

    delay = 1.0
    while True:
        try:
            await storage.write_batch(events)
            break
        except StorageError as exc:
            wait = min(delay + random.random(), 10)
            logger.warning("Bulk write failed; retrying in %.1fs: %s", wait, exc)
            await asyncio.sleep(wait)
            delay = min(delay * 2, 10)

    await consumer.commit(offsets)
    logger.info("Indexed %d logs", len(events))


async def run_worker(settings: Settings | None = None) -> None:
    configuration = settings or get_settings()
    if os.name != "nt":
        task = asyncio.current_task()
        asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, task.cancel)

    storage = ElasticsearchStorage(configuration)
    consumer = AIOKafkaConsumer(
        configuration.kafka_topic,
        bootstrap_servers=configuration.kafka_bootstrap_servers,
        group_id=configuration.kafka_group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=configuration.batch_size,
    )
    try:
        await storage.ensure_index()
        await consumer.start()
        logger.info("Worker started: topic=%s, batch_size=%d", configuration.kafka_topic, configuration.batch_size)
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
            due = first_message_at is not None and monotonic() - first_message_at >= configuration.flush_interval
            if count and (count >= configuration.batch_size or due):
                await process_records(consumer, storage, pending)
                pending.clear()
                count, first_message_at = 0, None
    finally:
        await consumer.stop()
        await storage.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        asyncio.run(run_worker())
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Worker stopped")


if __name__ == "__main__":
    main()
