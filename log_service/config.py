from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    elasticsearch_url: str = "http://localhost:9200"
    elasticsearch_index: str = Field(
        default="app-logs", pattern=r"^[a-z0-9][a-z0-9_-]*$", max_length=128
    )
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = Field(default="app-logs", min_length=1, max_length=249)
    kafka_group_id: str = "log-indexer"
    batch_size: int = Field(default=200, ge=1, le=1000)
    flush_interval: float = Field(default=1, gt=0, le=30)
    bulk_max_bytes: int = Field(default=2097152, ge=131072, le=8388608)


@lru_cache
def get_settings() -> Settings:
    return Settings()
