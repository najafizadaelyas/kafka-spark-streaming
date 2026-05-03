"""Unit tests for schema validation."""
import pytest
from jsonschema import ValidationError
from schemas.event_schema import validate_event


VALID_EVENT = {
    "event_id":   "evt-123456",
    "user_id":    42,
    "event_type": "purchase",
    "product_id": "prod-99",
    "amount":     19.99,
    "timestamp":  "2024-01-15T10:30:00+00:00",
}


def test_valid_event_passes():
    validate_event(VALID_EVENT)  # should not raise


def test_invalid_event_type_raises():
    bad = {**VALID_EVENT, "event_type": "unknown"}
    with pytest.raises(ValidationError):
        validate_event(bad)


def test_negative_amount_raises():
    bad = {**VALID_EVENT, "amount": -5.0}
    with pytest.raises(ValidationError):
        validate_event(bad)


def test_missing_required_field_raises():
    bad = {k: v for k, v in VALID_EVENT.items() if k != "event_id"}
    with pytest.raises(ValidationError):
        validate_event(bad)


def test_extra_field_raises():
    bad = {**VALID_EVENT, "extra_field": "oops"}
    with pytest.raises(ValidationError):
        validate_event(bad)


def test_invalid_event_id_pattern_raises():
    bad = {**VALID_EVENT, "event_id": "bad-id"}
    with pytest.raises(ValidationError):
        validate_event(bad)
