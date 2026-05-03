"""Unit tests for the Spark stream processor (schema enforcement + transform logic)."""
import pytest
from unittest.mock import MagicMock, patch


def test_valid_event_types():
    from spark_streaming.stream_processor import VALID_EVENT_TYPES
    assert "purchase" in VALID_EVENT_TYPES
    assert "unknown" not in VALID_EVENT_TYPES


def test_event_struct_fields():
    from spark_streaming.stream_processor import EVENT_STRUCT
    field_names = {f.name for f in EVENT_STRUCT.fields}
    assert {"event_id", "user_id", "event_type", "product_id", "amount", "timestamp"} == field_names
