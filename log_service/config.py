from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    elasticsearch_url: str = "http://localhost:9200"
    elasticsearch_index: str = Field(
        default="app-logs", pattern=r"^[a-z0-9][a-z0-9_-]*$", max_length=128
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
