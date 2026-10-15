"""Versioned, additive Elasticsearch schema. Existing documents are never deleted."""

SCHEMA_VERSION = 1

LOG_PROPERTIES = {
    "id": {"type": "keyword"},
    "timestamp": {"type": "date"},
    "service": {"type": "keyword"},
    "level": {"type": "keyword"},
    "message": {"type": "text"},
    "trace_id": {"type": "keyword"},
}

INDEX_MAPPING = {
    "dynamic": "strict",
    "_meta": {"log_service_schema_version": SCHEMA_VERSION},
    "properties": LOG_PROPERTIES,
}
