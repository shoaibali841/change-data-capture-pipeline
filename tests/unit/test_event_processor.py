"""Unit tests for Core CDC Event Processing Logic (src/processing/event_processor.py).

Verifies:
- INSERT creates a customer
- UPDATE modifies supplied fields only, preserving existing attributes
- DELETE performs soft deletion with is_deleted=True
- Exact duplicate events are ignored without duplicate effects
- Conflicting event ID reuse with different content is detected
- Out-of-order events do not overwrite newer current state
- Out-of-order events remain preserved in history
- Equal ordering values are handled deterministically
- Multiple customers are processed independently
- Reprocessing the exact same batch produces zero additional state changes (idempotence)
"""

from datetime import datetime, timezone
import pytest
from src.processing.event_processor import EventProcessor, ProcessStatus


@pytest.fixture
def processor() -> EventProcessor:
    """Fixture providing a fresh in-memory EventProcessor instance."""
    return EventProcessor()


def test_insert_creates_customer(processor: EventProcessor) -> None:
    """Verify an INSERT event creates a new active customer record."""
    event = {
        "event_id": "11111111-1111-1111-1111-111111111111",
        "customer_id": "CUST-001",
        "operation": "INSERT",
        "event_time": "2026-11-13T10:00:00Z",
        "source_sequence": 100,
        "payload": {
            "customer_name": "Alice Smith",
            "email": "alice@example.com",
            "city": "Seattle",
        },
    }

    result = processor.process_event(event)
    assert result.status == ProcessStatus.APPLIED_INSERT

    cust = processor.get_customer("CUST-001")
    assert cust is not None
    assert cust.customer_id == "CUST-001"
    assert cust.customer_name == "Alice Smith"
    assert cust.email == "alice@example.com"
    assert cust.city == "Seattle"
    assert cust.is_deleted is False
    assert cust.last_source_sequence == 100
    assert cust.last_event_id == "11111111-1111-1111-1111-111111111111"

    # Check history
    assert len(processor.history) == 1
    assert processor.history[0].event_id == "11111111-1111-1111-1111-111111111111"
    assert processor.history[0].old_values is None
    assert processor.history[0].new_values["customer_name"] == "Alice Smith"


def test_update_modifies_supplied_fields_only(processor: EventProcessor) -> None:
    """Verify UPDATE applies only supplied attributes while preserving omitted ones."""
    insert_event = {
        "event_id": "11111111-1111-1111-1111-111111111111",
        "customer_id": "CUST-001",
        "operation": "INSERT",
        "event_time": "2026-11-13T10:00:00Z",
        "source_sequence": 100,
        "payload": {
            "customer_name": "Alice Smith",
            "email": "alice@example.com",
            "city": "Seattle",
        },
    }
    processor.process_event(insert_event)

    # UPDATE only city; name and email must be preserved
    update_event = {
        "event_id": "22222222-2222-2222-2222-222222222222",
        "customer_id": "CUST-001",
        "operation": "UPDATE",
        "event_time": "2026-11-13T10:05:00Z",
        "source_sequence": 110,
        "payload": {
            "city": "San Francisco",
        },
    }
    result = processor.process_event(update_event)
    assert result.status == ProcessStatus.APPLIED_UPDATE

    cust = processor.get_customer("CUST-001")
    assert cust is not None
    assert cust.customer_name == "Alice Smith"  # Preserved
    assert cust.email == "alice@example.com"      # Preserved
    assert cust.city == "San Francisco"         # Updated
    assert cust.last_source_sequence == 110
    assert cust.last_event_id == "22222222-2222-2222-2222-222222222222"

    # Check history captures old and new values
    update_hist = processor.history[1]
    assert update_hist.old_values["city"] == "Seattle"
    assert update_hist.new_values["city"] == "San Francisco"


def test_delete_performs_soft_deletion(processor: EventProcessor) -> None:
    """Verify DELETE marks is_deleted=True and preserves existing attributes."""
    insert_event = {
        "event_id": "11111111-1111-1111-1111-111111111111",
        "customer_id": "CUST-001",
        "operation": "INSERT",
        "event_time": "2026-11-13T10:00:00Z",
        "source_sequence": 100,
        "payload": {
            "customer_name": "Bob Vance",
            "email": "bob@vance.com",
            "city": "Scranton",
        },
    }
    processor.process_event(insert_event)

    delete_event = {
        "event_id": "33333333-3333-3333-3333-333333333333",
        "customer_id": "CUST-001",
        "operation": "DELETE",
        "event_time": "2026-11-13T10:15:00Z",
        "source_sequence": 120,
        "payload": {"customer_id": "CUST-001"},
    }
    result = processor.process_event(delete_event)
    assert result.status == ProcessStatus.APPLIED_DELETE

    cust = processor.get_customer("CUST-001")
    assert cust is not None
    assert cust.is_deleted is True
    # Attributes are preserved
    assert cust.customer_name == "Bob Vance"
    assert cust.email == "bob@vance.com"
    assert cust.city == "Scranton"
    assert cust.last_source_sequence == 120
    assert cust.last_event_id == "33333333-3333-3333-3333-333333333333"

    # Verify history
    del_hist = processor.history[1]
    assert del_hist.old_values["is_deleted"] is False
    assert del_hist.new_values["is_deleted"] is True


def test_exact_duplicate_is_ignored(processor: EventProcessor) -> None:
    """Verify an exact duplicate event produces DUPLICATE_IGNORED and no state side-effects."""
    insert_event = {
        "event_id": "11111111-1111-1111-1111-111111111111",
        "customer_id": "CUST-001",
        "operation": "INSERT",
        "event_time": "2026-11-13T10:00:00Z",
        "source_sequence": 100,
        "payload": {"customer_name": "Charlie", "email": "charlie@example.com"},
    }

    res1 = processor.process_event(insert_event)
    assert res1.status == ProcessStatus.APPLIED_INSERT
    assert len(processor.history) == 1

    # Send identical duplicate
    res2 = processor.process_event(insert_event)
    assert res2.status == ProcessStatus.DUPLICATE_IGNORED
    # History must not receive a duplicate entry
    assert len(processor.history) == 1
    assert processor.metrics.duplicate_events == 1


def test_conflicting_event_id_detected(processor: EventProcessor) -> None:
    """Verify reusing an event ID with differing content is flagged as a conflict."""
    event1 = {
        "event_id": "11111111-1111-1111-1111-111111111111",
        "customer_id": "CUST-001",
        "operation": "INSERT",
        "event_time": "2026-11-13T10:00:00Z",
        "source_sequence": 100,
        "payload": {"customer_name": "David", "email": "david@example.com"},
    }
    processor.process_event(event1)

    # Reuse same event_id with different payload / customer
    conflicting_event = {
        "event_id": "11111111-1111-1111-1111-111111111111",
        "customer_id": "CUST-001",
        "operation": "UPDATE",
        "event_time": "2026-11-13T10:05:00Z",
        "source_sequence": 105,
        "payload": {"city": "New York"},
    }
    res = processor.process_event(conflicting_event)
    assert res.status == ProcessStatus.CONFLICTING_EVENT
    assert "Conflicting event detected" in res.message
    assert processor.metrics.conflicting_events == 1

    # Current state should not have updated
    cust = processor.get_customer("CUST-001")
    assert cust.last_source_sequence == 100


def test_out_of_order_event_does_not_overwrite_current_state(processor: EventProcessor) -> None:
    """Verify a late-arriving older event does not mutate current state."""
    # 1. Insert at sequence 100, time 10:00
    processor.process_event({
        "event_id": "11111111-1111-1111-1111-111111111111",
        "customer_id": "CUST-001",
        "operation": "INSERT",
        "event_time": "2026-11-13T10:00:00Z",
        "source_sequence": 100,
        "payload": {"customer_name": "Eva", "email": "eva@example.com", "city": "Boston"},
    })

    # 2. Update at sequence 120, time 10:20
    processor.process_event({
        "event_id": "22222222-2222-2222-2222-222222222222",
        "customer_id": "CUST-001",
        "operation": "UPDATE",
        "event_time": "2026-11-13T10:20:00Z",
        "source_sequence": 120,
        "payload": {"city": "Miami"},
    })

    # Current state is Miami at sequence 120
    assert processor.get_customer("CUST-001").city == "Miami"

    # 3. Late arriving event with sequence 110, time 10:10 (older than 120)
    stale_event = {
        "event_id": "33333333-3333-3333-3333-333333333333",
        "customer_id": "CUST-001",
        "operation": "UPDATE",
        "event_time": "2026-11-13T10:10:00Z",
        "source_sequence": 110,
        "payload": {"city": "Old Town"},
    }
    res = processor.process_event(stale_event)
    assert res.status == ProcessStatus.STALE_EVENT_PRESERVED

    # Current state must still be Miami with sequence 120
    cust = processor.get_customer("CUST-001")
    assert cust.city == "Miami"
    assert cust.last_source_sequence == 120
    assert cust.last_event_id == "22222222-2222-2222-2222-222222222222"


def test_out_of_order_event_remains_available_for_history(processor: EventProcessor) -> None:
    """Verify accepted unique stale events are preserved in the append-only history."""
    processor.process_event({
        "event_id": "11111111-1111-1111-1111-111111111111",
        "customer_id": "CUST-001",
        "operation": "INSERT",
        "event_time": "2026-11-13T10:00:00Z",
        "source_sequence": 100,
        "payload": {"customer_name": "Frank", "email": "frank@example.com"},
    })

    processor.process_event({
        "event_id": "22222222-2222-2222-2222-222222222222",
        "customer_id": "CUST-001",
        "operation": "UPDATE",
        "event_time": "2026-11-13T10:20:00Z",
        "source_sequence": 120,
        "payload": {"city": "Austin"},
    })

    # Out-of-order event
    stale_event = {
        "event_id": "33333333-3333-3333-3333-333333333333",
        "customer_id": "CUST-001",
        "operation": "UPDATE",
        "event_time": "2026-11-13T10:10:00Z",
        "source_sequence": 110,
        "payload": {"city": "Dallas"},
    }
    processor.process_event(stale_event)

    # History must contain all 3 events
    assert len(processor.history) == 3
    event_ids_in_history = [h.event_id for h in processor.history]
    assert "33333333-3333-3333-3333-333333333333" in event_ids_in_history

    stale_hist = [h for h in processor.history if h.event_id == "33333333-3333-3333-3333-333333333333"][0]
    assert stale_hist.source_sequence == 110
    assert stale_hist.new_values["city"] == "Dallas"


def test_equal_ordering_values_handled_deterministically(processor: EventProcessor) -> None:
    """Verify events with identical timestamp and sequence are ordered deterministically using event_id tie-breaker."""
    # First event with lower UUID lexicographically
    event_a = {
        "event_id": "11111111-1111-1111-1111-111111111111",
        "customer_id": "CUST-001",
        "operation": "INSERT",
        "event_time": "2026-11-13T10:00:00Z",
        "source_sequence": 100,
        "payload": {"customer_name": "Grace A", "email": "graceA@example.com", "city": "City A"},
    }
    # Second event with same event_time and source_sequence, but higher UUID lexicographically
    event_b = {
        "event_id": "99999999-9999-9999-9999-999999999999",
        "customer_id": "CUST-001",
        "operation": "UPDATE",
        "event_time": "2026-11-13T10:00:00Z",
        "source_sequence": 100,
        "payload": {"city": "City B"},
    }

    res_a = processor.process_event(event_a)
    assert res_a.status == ProcessStatus.APPLIED_INSERT

    # Event B has higher UUID, so (time, seq, id_B) > (time, seq, id_A) -> Newer
    res_b = processor.process_event(event_b)
    assert res_b.status == ProcessStatus.APPLIED_UPDATE
    assert processor.get_customer("CUST-001").city == "City B"

    # Now an event with a lower UUID ("00000000-...") arriving after B:
    event_c = {
        "event_id": "00000000-0000-0000-0000-000000000000",
        "customer_id": "CUST-001",
        "operation": "UPDATE",
        "event_time": "2026-11-13T10:00:00Z",
        "source_sequence": 100,
        "payload": {"city": "City C"},
    }
    # (time, seq, id_C) < (time, seq, id_B) -> Stale
    res_c = processor.process_event(event_c)
    assert res_c.status == ProcessStatus.STALE_EVENT_PRESERVED
    # Current state should remain City B
    assert processor.get_customer("CUST-001").city == "City B"


def test_multiple_customers_processed_independently(processor: EventProcessor) -> None:
    """Verify operations on different customers do not interfere with each other."""
    events = [
        {
            "event_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "customer_id": "CUST-100",
            "operation": "INSERT",
            "event_time": "2026-11-13T10:00:00Z",
            "source_sequence": 1,
            "payload": {"customer_name": "Cust One", "email": "one@example.com", "city": "City 1"},
        },
        {
            "event_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "customer_id": "CUST-200",
            "operation": "INSERT",
            "event_time": "2026-11-13T10:01:00Z",
            "source_sequence": 2,
            "payload": {"customer_name": "Cust Two", "email": "two@example.com", "city": "City 2"},
        },
        {
            "event_id": "cccccccc-cccc-cccc-cccc-cccccccccccc",
            "customer_id": "CUST-100",
            "operation": "DELETE",
            "event_time": "2026-11-13T10:02:00Z",
            "source_sequence": 3,
            "payload": {"customer_id": "CUST-100"},
        },
    ]

    results = processor.process_batch(events)
    assert len(results) == 3
    assert results[0].status == ProcessStatus.APPLIED_INSERT
    assert results[1].status == ProcessStatus.APPLIED_INSERT
    assert results[2].status == ProcessStatus.APPLIED_DELETE

    cust1 = processor.get_customer("CUST-100")
    cust2 = processor.get_customer("CUST-200")

    assert cust1 is not None and cust1.is_deleted is True
    assert cust2 is not None and cust2.is_deleted is False
    assert cust2.customer_name == "Cust Two"


def test_reprocessing_same_batch_produces_no_state_changes(processor: EventProcessor) -> None:
    """Verify strict idempotence: reprocessing a batch causes no mutations or history additions."""
    batch = [
        {
            "event_id": "11111111-1111-1111-1111-111111111111",
            "customer_id": "CUST-001",
            "operation": "INSERT",
            "event_time": "2026-11-13T10:00:00Z",
            "source_sequence": 10,
            "payload": {"customer_name": "Initial", "email": "init@example.com", "city": "A"},
        },
        {
            "event_id": "22222222-2222-2222-2222-222222222222",
            "customer_id": "CUST-001",
            "operation": "UPDATE",
            "event_time": "2026-11-13T10:05:00Z",
            "source_sequence": 20,
            "payload": {"city": "B"},
        },
    ]

    # First run
    results1 = processor.process_batch(batch)
    assert len(results1) == 2
    assert results1[0].status == ProcessStatus.APPLIED_INSERT
    assert results1[1].status == ProcessStatus.APPLIED_UPDATE
    assert len(processor.history) == 2
    assert processor.get_customer("CUST-001").city == "B"

    # Capture customer state snapshot
    cust_snapshot = processor.get_customer("CUST-001").to_dict()

    # Second run with exact same batch
    results2 = processor.process_batch(batch)
    assert len(results2) == 2
    assert results2[0].status == ProcessStatus.DUPLICATE_IGNORED
    assert results2[1].status == ProcessStatus.DUPLICATE_IGNORED

    # History count must not increase
    assert len(processor.history) == 2
    # Customer state must be identical
    assert processor.get_customer("CUST-001").to_dict() == cust_snapshot

