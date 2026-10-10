from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID, uuid4

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


class LogLevel(StrEnum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class LogEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    id: UUID = Field(default_factory=uuid4)
    timestamp: AwareDatetime = Field(default_factory=lambda: datetime.now(UTC))
    service: str = Field(min_length=1, max_length=128)
    level: LogLevel = LogLevel.INFO
    message: str = Field(min_length=1, max_length=16384)
    trace_id: str | None = Field(default=None, max_length=128)


class LogPage(BaseModel):
    items: list[LogEvent]
    total: int
    offset: int
    limit: int


class KafkaEvent(LogEvent):
    id: UUID = Field(...)
    timestamp: AwareDatetime = Field(...)


class AcceptedLog(BaseModel):
    id: UUID
    status: str = "queued"
