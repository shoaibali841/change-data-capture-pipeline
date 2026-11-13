"""PostgreSQL Integration Tests for CDC Repository (src/publishing/repository.py).

Tests verify:
- Pipeline run creation, metric tracking, and completion.
- Customer insertion, updates, and partial attribute preservation.
- Soft deletion (is_deleted=True while retaining attributes).
- Event history recording (JSONB old_values/new_values, append-only stale preservation).
- Duplicate event protection (exact duplicates skipped).
- Conflicting event ID reuse detection.
- Quarantine payload and error logging.
- Atomic transaction rollbacks on failure.
"""

from datetime import datetime, timedelta, timezone
from typing import Generator
from uuid import uuid4
import pytest
from sqlalchemy import text

from src.database.session import get_db_session, get_test_db_session, verify_test_database
from src.processing.event_processor import ProcessStatus
from src.publishing.repository import CDCRepository
from src.validation.event_schema import CDCEvent


@pytest.fixture
def repo() -> CDCRepository:
    """Fixture providing a CDCRepository instance."""
    return CDCRepository()


@pytest.fixture
def test_run_id(repo: CDCRepository) -> Generator[str, None, None]:
    """Fixture creating an isolated test pipeline run and ensuring safe teardown."""
    run_id = uuid4()
    with get_test_db_session() as session:
        repo.create_pipeline_run(run_id=run_id, session=session)

    yield str(run_id)

    # Safe isolated cleanup: verify test database and remove only data created for this test run
    with get_test_db_session() as session:
        verify_test_database(session)
        session.execute(
            text("DELETE FROM customer_event_history WHERE run_id = :run_id"),
            {"run_id": run_id},
        )
        session.execute(
            text("DELETE FROM event_quarantine WHERE run_id = :run_id"),
            {"run_id": run_id},
        )
        session.execute(
            text("DELETE FROM pipeline_runs WHERE run_id = :run_id"),
            {"run_id": run_id},
        )


@pytest.fixture(autouse=True)
def cleanup_test_customers() -> Generator[None, None, None]:
    """Teardown fixture cleaning up test customer rows with prefix TEST-."""
    yield
    with get_test_db_session() as session:
        verify_test_database(session)
        session.execute(
            text("DELETE FROM customers_current WHERE customer_id LIKE 'TEST-CUST-%'")
        )


def test_pipeline_run_lifecycle(repo: CDCRepository) -> None:
    """Verify pipeline_runs lifecycle: creation, metrics update, and completion."""
    run_id = uuid4()
    with get_db_session() as session:
        created_id = repo.create_pipeline_run(run_id=run_id, session=session)
        assert created_id == run_id

        # Verify created state
        run = repo.get_pipeline_run(run_id, session=session)
        assert run is not None
        assert run["status"] == "RUNNING"
        assert run["completed_at"] is None

        # Complete run with metrics
        metrics = {
            "input_count": 100,
            "accepted_count": 95,
            "inserted_count": 50,
            "updated_count": 40,
            "deleted_count": 5,
            "duplicate_count": 3,
            "stale_event_count": 2,
            "rejected_count": 5,
        }
        repo.complete_pipeline_run(
            run_id=run_id,
            status="COMPLETED",
            metrics=metrics,
            session=session,
        )

        completed_run = repo.get_pipeline_run(run_id, session=session)
        assert completed_run is not None
        assert completed_run["status"] == "COMPLETED"
        assert completed_run["completed_at"] is not None
        assert completed_run["duration_seconds"] is not None
        assert completed_run["duration_seconds"] >= 0
        assert completed_run["inserted_count"] == 50
        assert completed_run["rejected_count"] == 5

    # Cleanup
    with get_db_session() as session:
        session.execute(
            text("DELETE FROM pipeline_runs WHERE run_id = :run_id"),
            {"run_id": run_id},
        )


def test_customer_insertion_and_updates(repo: CDCRepository, test_run_id: str) -> None:
    """Verify customer insertion and subsequent updates in PostgreSQL."""
    cust_id = "TEST-CUST-0001"
    base_time = datetime(2026, 11, 13, 10, 0, 0, tzinfo=timezone.utc)

    # 1. INSERT event
    insert_event = CDCEvent(
        event_id=uuid4(),
        customer_id=cust_id,
        operation="INSERT",
        event_time=base_time,
        source_sequence=10,
        payload={"customer_name": "Test User", "email": "test@example.com", "city": "Denver"},
    )

    with get_db_session() as session:
        status, state = repo.apply_event_mutation(
            insert_event, run_id=test_run_id, source_file="test.json", session=session
        )
        assert status == ProcessStatus.APPLIED_INSERT
        assert state is not None
        assert state["customer_id"] == cust_id
        assert state["customer_name"] == "Test User"
        assert state["city"] == "Denver"
        assert state["is_deleted"] is False

    # 2. UPDATE event (modifies city and email)
    update_event = CDCEvent(
        event_id=uuid4(),
        customer_id=cust_id,
        operation="UPDATE",
        event_time=base_time + timedelta(minutes=5),
        source_sequence=20,
        payload={"city": "Boulder", "email": "boulder@example.com"},
    )

    with get_db_session() as session:
        status, state = repo.apply_event_mutation(
            update_event, run_id=test_run_id, source_file="test.json", session=session
        )
        assert status == ProcessStatus.APPLIED_UPDATE
        assert state["city"] == "Boulder"
        assert state["email"] == "boulder@example.com"
        assert state["customer_name"] == "Test User"  # Preserved
        assert state["last_source_sequence"] == 20


def test_partial_update_preservation(repo: CDCRepository, test_run_id: str) -> None:
    """Verify partial updates preserve omitted attributes in current state."""
    cust_id = "TEST-CUST-0002"
    base_time = datetime(2026, 11, 13, 10, 0, 0, tzinfo=timezone.utc)

    with get_db_session() as session:
        # INSERT full profile
        repo.apply_event_mutation(
            CDCEvent(
                event_id=uuid4(),
                customer_id=cust_id,
                operation="INSERT",
                event_time=base_time,
                source_sequence=100,
                payload={"customer_name": "Original Name", "email": "orig@example.com", "city": "Original City"},
            ),
            run_id=test_run_id,
            session=session,
        )

        # UPDATE only city (name and email omitted)
        repo.apply_event_mutation(
            CDCEvent(
                event_id=uuid4(),
                customer_id=cust_id,
                operation="UPDATE",
                event_time=base_time + timedelta(minutes=2),
                source_sequence=110,
                payload={"city": "Updated City"},
            ),
            run_id=test_run_id,
            session=session,
        )

        current = repo.get_customer_current(cust_id, session=session)
        assert current is not None
        assert current["customer_name"] == "Original Name"  # Preserved
        assert current["email"] == "orig@example.com"        # Preserved
        assert current["city"] == "Updated City"            # Updated


def test_soft_deletion(repo: CDCRepository, test_run_id: str) -> None:
    """Verify DELETE sets is_deleted=True while retaining customer attributes."""
    cust_id = "TEST-CUST-0003"
    base_time = datetime(2026, 11, 13, 10, 0, 0, tzinfo=timezone.utc)

    with get_db_session() as session:
        # INSERT
        repo.apply_event_mutation(
            CDCEvent(
                event_id=uuid4(),
                customer_id=cust_id,
                operation="INSERT",
                event_time=base_time,
                source_sequence=50,
                payload={"customer_name": "Delete Me", "email": "delete@example.com", "city": "Austin"},
            ),
            run_id=test_run_id,
            session=session,
        )

        # DELETE
        status, state = repo.apply_event_mutation(
            CDCEvent(
                event_id=uuid4(),
                customer_id=cust_id,
                operation="DELETE",
                event_time=base_time + timedelta(minutes=10),
                source_sequence=60,
                payload={"customer_id": cust_id},
            ),
            run_id=test_run_id,
            session=session,
        )
        assert status == ProcessStatus.APPLIED_DELETE
        assert state["is_deleted"] is True
        assert state["customer_name"] == "Delete Me"  # Retained
        assert state["email"] == "delete@example.com"  # Retained
        assert state["city"] == "Austin"              # Retained


def test_event_history_insertion_and_stale_preservation(repo: CDCRepository, test_run_id: str) -> None:
    """Verify history captures JSONB snapshots and preserves stale events without state overwriting."""
    cust_id = "TEST-CUST-0004"
    base_time = datetime(2026, 11, 13, 10, 0, 0, tzinfo=timezone.utc)

    with get_db_session() as session:
        # 1. Insert at sequence 100
        ev1 = CDCEvent(
            event_id=uuid4(),
            customer_id=cust_id,
            operation="INSERT",
            event_time=base_time,
            source_sequence=100,
            payload={"customer_name": "Seq 100", "email": "100@example.com", "city": "City 100"},
        )
        repo.apply_event_mutation(ev1, run_id=test_run_id, session=session)

        # 2. Update at sequence 120 (time + 20 min)
        ev2 = CDCEvent(
            event_id=uuid4(),
            customer_id=cust_id,
            operation="UPDATE",
            event_time=base_time + timedelta(minutes=20),
            source_sequence=120,
            payload={"city": "City 120"},
        )
        repo.apply_event_mutation(ev2, run_id=test_run_id, session=session)

        # Check history has 2 records with JSONB
        h2 = repo.get_event_from_history(ev2.event_id, session=session)
        assert h2 is not None
        assert h2["old_values"]["city"] == "City 100"
        assert h2["new_values"]["city"] == "City 120"

        # 3. Late arriving event at sequence 110 (time + 10 min) < 120
        stale_ev = CDCEvent(
            event_id=uuid4(),
            customer_id=cust_id,
            operation="UPDATE",
            event_time=base_time + timedelta(minutes=10),
            source_sequence=110,
            payload={"city": "Stale City 110"},
        )
        status, _ = repo.apply_event_mutation(stale_ev, run_id=test_run_id, session=session)
        assert status == ProcessStatus.STALE_EVENT_PRESERVED

        # History must contain the stale event
        stale_hist = repo.get_event_from_history(stale_ev.event_id, session=session)
        assert stale_hist is not None
        assert stale_hist["source_sequence"] == 110
        assert stale_hist["new_values"]["city"] == "Stale City 110"

        # Current state must still reflect sequence 120, NOT 110
        current = repo.get_customer_current(cust_id, session=session)
        assert current["city"] == "City 120"
        assert current["last_source_sequence"] == 120


def test_duplicate_event_protection(repo: CDCRepository, test_run_id: str) -> None:
    """Verify exact duplicate events are skipped without creating duplicate history."""
    cust_id = "TEST-CUST-0005"
    ev_id = uuid4()
    base_time = datetime(2026, 11, 13, 10, 0, 0, tzinfo=timezone.utc)

    event = CDCEvent(
        event_id=ev_id,
        customer_id=cust_id,
        operation="INSERT",
        event_time=base_time,
        source_sequence=1,
        payload={"customer_name": "Dup User", "email": "dup@example.com"},
    )

    with get_db_session() as session:
        # First execution
        res1, _ = repo.apply_event_mutation(event, run_id=test_run_id, session=session)
        assert res1 == ProcessStatus.APPLIED_INSERT

        # Second execution with exact same event
        res2, _ = repo.apply_event_mutation(event, run_id=test_run_id, session=session)
        assert res2 == ProcessStatus.DUPLICATE_IGNORED

        # Verify exactly one history entry exists
        count = session.execute(
            text("SELECT count(*) FROM customer_event_history WHERE event_id = :ev_id"),
            {"ev_id": ev_id},
        ).scalar()
        assert count == 1


def test_conflicting_event_id_detection(repo: CDCRepository, test_run_id: str) -> None:
    """Verify conflicting reuse of an event ID with differing content is detected."""
    cust_id = "TEST-CUST-0006"
    ev_id = uuid4()
    base_time = datetime(2026, 11, 13, 10, 0, 0, tzinfo=timezone.utc)

    event1 = CDCEvent(
        event_id=ev_id,
        customer_id=cust_id,
        operation="INSERT",
        event_time=base_time,
        source_sequence=1,
        payload={"customer_name": "Original", "email": "orig@example.com"},
    )

    with get_db_session() as session:
        repo.apply_event_mutation(event1, run_id=test_run_id, session=session)

        # Same event_id, different sequence and payload
        event_conflict = CDCEvent(
            event_id=ev_id,
            customer_id=cust_id,
            operation="UPDATE",
            event_time=base_time + timedelta(minutes=5),
            source_sequence=99,
            payload={"city": "Conflict City"},
        )
        status, _ = repo.apply_event_mutation(event_conflict, run_id=test_run_id, session=session)
        assert status == ProcessStatus.CONFLICTING_EVENT


def test_quarantine_insertion(repo: CDCRepository, test_run_id: str) -> None:
    """Verify malformed and rejected records are stored in event_quarantine."""
    raw_bad_payload = "CORRUPTED_JSON_STREAM_RAW_LINE_$$$"
    reason = "JSONDecodeError: Expecting value at line 1 col 1"

    with get_db_session() as session:
        quarantine_id = repo.insert_quarantine(
            raw_payload=raw_bad_payload,
            run_id=test_run_id,
            rejection_reason=reason,
            source_file="malformed_events.jsonl",
            session=session,
        )
        assert quarantine_id > 0

        # Verify in DB
        row = session.execute(
            text("SELECT * FROM event_quarantine WHERE quarantine_id = :qid"),
            {"qid": quarantine_id},
        ).mappings().first()
        assert row is not None
        assert row["raw_payload"] == raw_bad_payload
        assert row["rejection_reason"] == reason
        assert row["source_file"] == "malformed_events.jsonl"


def test_transaction_rollback_after_simulated_failure(repo: CDCRepository, test_run_id: str) -> None:
    """Verify database writes are atomically rolled back when a transaction encounters an error."""
    cust_id = "TEST-CUST-ROLLBACK"
    ev_id = uuid4()
    base_time = datetime(2026, 11, 13, 10, 0, 0, tzinfo=timezone.utc)

    event = CDCEvent(
        event_id=ev_id,
        customer_id=cust_id,
        operation="INSERT",
        event_time=base_time,
        source_sequence=1,
        payload={"customer_name": "Rollback Test", "email": "rollback@example.com"},
    )

    # Deliberate failure inside the transaction context
    with pytest.raises(RuntimeError, match="Simulated fatal pipeline failure"):
        with get_db_session() as session:
            repo.apply_event_mutation(event, run_id=test_run_id, session=session)
            # Raise exception before transaction commits
            raise RuntimeError("Simulated fatal pipeline failure")

    # Verify that neither customers_current nor customer_event_history persisted the change
    with get_db_session() as session:
        cust = repo.get_customer_current(cust_id, session=session)
        assert cust is None, "Customer should not exist after transaction rollback"

        hist = repo.get_event_from_history(ev_id, session=session)
        assert hist is None, "History record should not exist after transaction rollback"

