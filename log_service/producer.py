import argparse
import asyncio

from log_service.config import get_settings
from log_service.kafka import KafkaLogPublisher
from log_service.models import LogEvent, LogLevel


async def produce(count: int, service: str, interval: float) -> None:
    publisher = KafkaLogPublisher(get_settings())
    try:
        await publisher.start()
        for number in range(1, count + 1):
            level = LogLevel.ERROR if number % 10 == 0 else LogLevel.INFO
            event = LogEvent(service=service, level=level, message=f"Demo log {number}")
            await publisher.send(event)
            print(f"{event.id} {event.level} {event.message}", flush=True)
            if interval:
                await asyncio.sleep(interval)
    finally:
        await publisher.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Send demo logs directly to Kafka")
    parser.add_argument("--count", type=int, default=20)
    parser.add_argument("--service", default="demo-service")
    parser.add_argument("--interval", type=float, default=0)
    args = parser.parse_args()
    if args.count < 1 or args.interval < 0:
        parser.error("count must be positive and interval must not be negative")
    asyncio.run(produce(args.count, args.service, args.interval))


if __name__ == "__main__":
    main()
