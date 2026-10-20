"""Send an invalid event and verify its arrival in the running worker's DLQ."""

import asyncio
from contextlib import AsyncExitStack
from uuid import uuid4

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

from log_service.config import get_settings


async def check() -> None:
    settings = get_settings()
    async with AsyncExitStack() as stack:
        consumer = AIOKafkaConsumer(
            settings.kafka_dlq_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=None,
            enable_auto_commit=False,
            auto_offset_reset="latest",
        )
        stack.push_async_callback(consumer.stop)
        producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers, acks="all")
        stack.push_async_callback(producer.stop)
        async with asyncio.timeout(60):
            await consumer.start()
            await producer.start()
            partitions = consumer.partitions_for_topic(settings.kafka_dlq_topic)
            if not partitions:
                raise RuntimeError("DLQ topic does not exist; start Kafka and the worker first")
            assigned = [TopicPartition(settings.kafka_dlq_topic, number) for number in partitions]
            consumer.unsubscribe()
            consumer.assign(assigned)
            await consumer.seek_to_end(*assigned)

            value = b"invalid-dlq-probe-" + uuid4().hex.encode("ascii")
            source = await producer.send_and_wait(
                settings.kafka_topic, value=value, key=b"dlq-probe"
            )
            expected_key = f"{source.topic}:{source.partition}:{source.offset}".encode("utf-8")
            while True:
                record = await consumer.getone()
                if record.key != expected_key:
                    continue
                headers = dict(record.headers)
                assert record.value == value
                assert headers["dlq.reason"] == b"invalid_event"
                assert headers["dlq.source_key"] == b"dlq-probe"
                print(
                    f"Passed: invalid event preserved in {settings.kafka_dlq_topic}; source={expected_key.decode()}"
                )
                break


if __name__ == "__main__":
    asyncio.run(check())
