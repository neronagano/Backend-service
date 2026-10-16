from functools import lru_cache
import re

from pydantic import Field, field_validator, model_validator
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
    kafka_dlq_topic: str = Field(default="app-logs-dlq", min_length=1, max_length=249)
    batch_size: int = Field(default=200, ge=1, le=1000)
    flush_interval: float = Field(default=1, gt=0, le=30)
    bulk_max_bytes: int = Field(default=2097152, ge=131072, le=8388608)
    auto_migrate: bool = True
    request_timeout: float = Field(default=10, gt=0, le=120)
    startup_timeout: float = Field(default=30, gt=0, le=120)
    publish_timeout: float = Field(default=10, gt=0, le=60)
    shutdown_timeout: float = Field(default=10, gt=0, le=60)
    worker_batch_timeout: float = Field(default=120, gt=0, le=240)

    @field_validator("kafka_topic", "kafka_dlq_topic")
    @classmethod
    def validate_topic(cls, value: str) -> str:
        if value in (".", "..") or not re.fullmatch(r"[A-Za-z0-9._-]+", value):
            raise ValueError("Invalid Kafka topic name")
        return value

    @model_validator(mode="after")
    def validate_topics(self):
        if self.kafka_topic == self.kafka_dlq_topic:
            raise ValueError("Source and dead-letter topics must be different")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
