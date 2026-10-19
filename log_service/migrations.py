"""Create or upgrade the log index without dropping documents."""

import argparse
import asyncio

from log_service.config import get_settings
from log_service.schema import SCHEMA_VERSION
from log_service.storage import ElasticsearchStorage


async def migrate(check_only: bool = False) -> None:
    storage = ElasticsearchStorage(get_settings())
    try:
        if check_only:
            await storage.check_schema()
        else:
            await storage.migrate_index()
            await storage.check_schema()
        print(f"Log index {storage.index}: schema version {SCHEMA_VERSION}")
    finally:
        await storage.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Check the schema without changing it")
    asyncio.run(migrate(parser.parse_args().check))


if __name__ == "__main__":
    main()
