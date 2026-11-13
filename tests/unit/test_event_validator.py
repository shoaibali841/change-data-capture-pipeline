"""Unit tests for CDC Event Schema Validation.

Tests cover:
- Valid INSERT, UPDATE, and DELETE events
- Missing required fields (customer_id, operation, event_time, source_sequence)
- Invalid UUID format
- Invalid operation type (not in INSERT, UPDATE, DELETE)
- Invalid timestamp format
- Negative source sequence
- Missing customer name and email for INSERT operations
- Partial customer attributes for UPDATE operations
- Mixed batch validation with error isolation
- Preservation of raw payload and generator metadata
"""

from typing import Any, Dict
import pytest
from src.ingestion.event_reader import ParsedRecord
from src.validation.event_schema import CDCEvent, OperationType
from src.validation.event_validator import (
    EventValidator,
    RejectedEvent,
    ValidatedEvent,
    ValidationBatchResult,
)


@pytest.fixture
def validator() -> EventValidator:
    """Fixture providing an EventValidator instance."""
    return EventValidator()


@pytest.fixture
def base_valid_insert_dict() -> Dict[str, Any]:
    """Fixture returning a valid INSERT event dictionary."""
    return {
        "event_id": "bdd640fb-0667-4ad1-9c80-317fa3b1799d",
        "customer_id": "CUST-0001",
        "operation": "INSERT",
        "event_time": "2026-11-13T10:00:00Z",
        "source_sequence": 100,
        "payload": {
            "customer_id": "CUST-0001",
            "customer_name": "Allison Hill",
            "email": "allison@example.com",
            "city": "New York",
        },
        "_record_type": "valid",
        "_test_category": "normal_insert",
    }


def test_valid_insert_event(validator: EventValidator, base_valid_insert_dict: Dict[str, Any]) -> None:
    """Verify valid INSERT event passes validation and preserves metadata."""
    outcome = validator.validate_record(base_valid_insert_dict, source_file="test.json", record_number=1)
    assert isinstance(outcome, ValidatedEvent)
    assert outcome.event.operation == OperationType.INSERT
    assert outcome.event.customer_id == "CUST-0001"
    assert outcome.event.payload["customer_name"] == "Allison Hill"
    # Metadata preserved
    assert outcome.event.model_extra["_record_type"] == "valid"
    assert outcome.record_number == 1
    assert outcome.source_file == "test.json"


def test_valid_update_event_partial_attributes(validator: EventValidator) -> None:
    """Verify valid UPDATE event allows partial attributes (e.g. only city updated)."""
    update_dict = {
        "event_id": "6c307511-b2b9-437a-a8df-6ec4ce4a2bbd",
        "customer_id": "CUST-0001",
        "operation": "UPDATE",
        "event_time": "2026-11-13T10:05:00Z",
        "source_sequence": 110,
        "payload": {
            "city": "San Francisco",
        },
    }
    outcome = validator.validate_record(update_dict)
    assert isinstance(outcome, ValidatedEvent)
    assert outcome.event.operation == OperationType.UPDATE
    assert outcome.event.payload["city"] == "San Francisco"


def test_valid_delete_event(validator: EventValidator) -> None:
    """Verify valid DELETE event with minimal payload."""
    delete_dict = {
        "event_id": "e2acf72f-9e57-4f7a-a0ee-89aed453dd32",
        "customer_id": "CUST-0001",
        "operation": "DELETE",
        "event_time": "2026-11-13T10:10:00Z",
        "source_sequence": 120,
        "payload": {"customer_id": "CUST-0001"},
    }
    outcome = validator.validate_record(delete_dict)
    assert isinstance(outcome, ValidatedEvent)
    assert outcome.event.operation == OperationType.DELETE


def test_missing_customer_id(validator: EventValidator, base_valid_insert_dict: Dict[str, Any]) -> None:
    """Verify event is rejected when customer_id is missing or blank."""
    data = dict(base_valid_insert_dict)
    del data["customer_id"]

    outcome = validator.validate_record(data)
    assert isinstance(outcome, RejectedEvent)
    assert "customer_id" in outcome.rejection_reason

    # Blank customer_id test
    data_blank = dict(base_valid_insert_dict)
    data_blank["customer_id"] = "   "
    outcome_blank = validator.validate_record(data_blank)
    assert isinstance(outcome_blank, RejectedEvent)
    assert "customer_id" in outcome_blank.rejection_reason


def test_invalid_uuid(validator: EventValidator, base_valid_insert_dict: Dict[str, Any]) -> None:
    """Verify event is rejected when event_id is not a valid UUID."""
    data = dict(base_valid_insert_dict)
    data["event_id"] = "not-a-valid-uuid"

    outcome = validator.validate_record(data)
    assert isinstance(outcome, RejectedEvent)
    assert "event_id" in outcome.rejection_reason
    assert outcome.event_id == "not-a-valid-uuid"


def test_invalid_operation(validator: EventValidator, base_valid_insert_dict: Dict[str, Any]) -> None:
    """Verify event is rejected when operation is not INSERT, UPDATE, or DELETE."""
    data = dict(base_valid_insert_dict)
    data["operation"] = "UPSERT"

    outcome = validator.validate_record(data)
    assert isinstance(outcome, RejectedEvent)
    assert "operation" in outcome.rejection_reason


def test_invalid_timestamp(validator: EventValidator, base_valid_insert_dict: Dict[str, Any]) -> None:
    """Verify event is rejected when event_time is not a parseable datetime."""
    data = dict(base_valid_insert_dict)
    data["event_time"] = "invalid-date-format-tomorrow"

    outcome = validator.validate_record(data)
    assert isinstance(outcome, RejectedEvent)
    assert "event_time" in outcome.rejection_reason


def test_negative_source_sequence(validator: EventValidator, base_valid_insert_dict: Dict[str, Any]) -> None:
    """Verify event is rejected when source_sequence is negative."""
    data = dict(base_valid_insert_dict)
    data["source_sequence"] = -5

    outcome = validator.validate_record(data)
    assert isinstance(outcome, RejectedEvent)
    assert "source_sequence" in outcome.rejection_reason


def test_insert_missing_customer_name_or_email(validator: EventValidator, base_valid_insert_dict: Dict[str, Any]) -> None:
    """Verify INSERT is rejected when customer_name or email is missing in payload."""
    # Missing customer_name
    data_no_name = dict(base_valid_insert_dict)
    data_no_name["payload"] = {"email": "user@example.com", "city": "Denver"}
    outcome1 = validator.validate_record(data_no_name)
    assert isinstance(outcome1, RejectedEvent)
    assert "customer_name" in outcome1.rejection_reason

    # Missing email
    data_no_email = dict(base_valid_insert_dict)
    data_no_email["payload"] = {"customer_name": "Bob Smith", "city": "Denver"}
    outcome2 = validator.validate_record(data_no_email)
    assert isinstance(outcome2, RejectedEvent)
    assert "email" in outcome2.rejection_reason


def test_parsed_record_integration(validator: EventValidator) -> None:
    """Verify validator works seamlessly with ParsedRecord from the ingestion layer."""
    raw_str = '{"event_id": "bdd640fb-0667-4ad1-9c80-317fa3b1799d", "customer_id": "C-1", "operation": "INSERT", "event_time": "2026-11-13T10:00:00Z", "source_sequence": 1, "payload": {"customer_name": "A", "email": "a@b.com"}}'
    rec = ParsedRecord(
        source_file="sample_events.json",
        record_number=42,
        raw_payload=raw_str,
        data={
            "event_id": "bdd640fb-0667-4ad1-9c80-317fa3b1799d",
            "customer_id": "C-1",
            "operation": "INSERT",
            "event_time": "2026-11-13T10:00:00Z",
            "source_sequence": 1,
            "payload": {"customer_name": "A", "email": "a@b.com"},
        },
    )

    outcome = validator.validate_record(rec)
    assert isinstance(outcome, ValidatedEvent)
    assert outcome.source_file == "sample_events.json"
    assert outcome.record_number == 42
    assert outcome.raw_payload == raw_str


def test_batch_validation_error_isolation(validator: EventValidator, base_valid_insert_dict: Dict[str, Any]) -> None:
    """Verify batch validation isolates errors and processes all records."""
    records = [
        base_valid_insert_dict,  # 1. Valid INSERT
        {"event_id": "bad-id", "customer_id": "CUST-2"},  # 2. Invalid (bad UUID, missing fields)
        {
            "event_id": "23b8c1e9-3924-46de-beb1-3b9046685257",
            "customer_id": "CUST-3",
            "operation": "UPDATE",
            "event_time": "2026-11-13T10:02:00Z",
            "source_sequence": 105,
            "payload": {"city": "Chicago"},
        },  # 3. Valid UPDATE
        {
            "event_id": "bd9c66b3-ad3c-4d6d-9a3d-1fa7bc8960a9",
            "customer_id": "CUST-4",
            "operation": "UNKNOWN_OP",
            "event_time": "2026-11-13T10:03:00Z",
            "source_sequence": 106,
            "payload": {},
        },  # 4. Invalid operation
        {
            "event_id": "972a8469-1641-4f82-8b9d-2434e465e150",
            "customer_id": "CUST-5",
            "operation": "DELETE",
            "event_time": "2026-11-13T10:04:00Z",
            "source_sequence": 107,
            "payload": {},
        },  # 5. Valid DELETE
    ]

    result: ValidationBatchResult = validator.validate_batch(records)
    assert result.total_records == 5
    assert result.valid_count == 3
    assert result.rejected_count == 2
    assert len(result.valid_events) == 3
    assert len(result.rejected_events) == 2

    # Check rejected record numbers
    rejected_recs = [r.record_number for r in result.rejected_events]
    assert rejected_recs == [2, 4]

