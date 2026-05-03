"""
Schema definition and validation for user-events.
Acts as the single source of truth for the event contract across producer and Spark job.
"""
import jsonschema
from jsonschema import validate, ValidationError

EVENT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "UserEvent",
    "type": "object",
    "required": ["event_id", "user_id", "event_type", "product_id", "amount", "timestamp"],
    "properties": {
        "event_id":   {"type": "string", "pattern": "^evt-[0-9]+$"},
        "user_id":    {"type": "integer", "minimum": 1},
        "event_type": {"type": "string", "enum": ["click", "view", "purchase", "add_to_cart"]},
        "product_id": {"type": "string", "pattern": "^prod-[0-9]+$"},
        "amount":     {"type": "number", "minimum": 0},
        "timestamp":  {"type": "string", "format": "date-time"},
    },
    "additionalProperties": False,
}

# PySpark StructType mirror (used in spark_streaming/stream_processor.py)
SPARK_SCHEMA_DDL = """
    event_id   STRING,
    user_id    INT,
    event_type STRING,
    product_id STRING,
    amount     DOUBLE,
    timestamp  TIMESTAMP
"""


def validate_event(event: dict) -> None:
    """Raises jsonschema.ValidationError if the event does not conform to EVENT_SCHEMA."""
    try:
        validate(instance=event, schema=EVENT_SCHEMA)
    except ValidationError as exc:
        raise ValidationError(f"Schema violation: {exc.message}") from exc
