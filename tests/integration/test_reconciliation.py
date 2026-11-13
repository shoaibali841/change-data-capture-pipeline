"""Integration Tests for CDC Data Reconciliation and Integrity Audits.

Verifies:
- Reconciliation handles an empty database cleanly.
- A clean pipeline execution passes all reconciliation audits.
- Safe pipeline reruns do not alter customer state or event history counts.
- Soft-deleted customers and attribute retention are correctly verified.
- Deliberate duplicate history events are accurately detected.
- Inconsistent deletion metadata and state misalignments are detected.
- Invalid customer attributes are detected.
- Orphaned event history records referencing nonexistent pipeline runs are detected.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Generator
from uuid import uuid4
import pytest
from sqlalchemy import text

from src.database.session import get_db_session, get_test_db_session, safe_test_cleanup
from src.monitoring.reconciliation import CDCReconciler
from src.pipeline import CDCPipeline


@pytest.fixture(autouse=True)
def clean_database() -> Generator[None, None, None]:
    """Clean all tables before and after each test on the verified test database."""
    def cleanup():
        with get_test_db_session() as session:
            safe_test_cleanup(session)

    cleanup()
    yield
    cleanup()


def test_empty_database_reconciliation() -> None:
    """Verify reconciliation suite handles an empty database gracefully."""
    reconciler = CDCReconciler()
    summary = reconciler.run_reconciliation()

    assert summary.all_passed is True
    assert summary.total_customers == 0
    assert summary.active_customers == 0
    assert summary.soft_deleted_customers == 0
    assert summary.total_history_events == 0
    assert summary.total_quarantined_events == 0
    assert summary.stale_events_count == 0

    for name, check in summary.checks.items():
        assert check.passed is True, f"Check {name} failed on empty database"
        assert check.violation_count == 0


def test_clean_pipeline_run_passes_reconciliation() -> None:
    """Verify that a successful pipeline run on sample data passes 100% of reconciliation audits."""
    sample_file = Path("data/input/sample_events.json")
    malformed_file = Path("data/input/malformed_events.jsonl")

    if not sample_file.exists() or not malformed_file.exists():
        pytest.skip("Input files do not exist.")

    pipeline = CDCPipeline()
    run_id = uuid4()
    pipeline.run(input_files=[sample_file, malformed_file], run_id=run_id)

    reconciler = CDCReconciler()
    summary = reconciler.run_reconciliation()

    # High-level metrics verification
    assert summary.all_passed is True
    assert summary.total_customers == 10
    assert summary.active_customers == 8
    assert summary.soft_deleted_customers == 2
    assert summary.total_history_events == 22
    assert summary.total_quarantined_events == 7

    # Operation counts
    assert summary.history_by_operation.get("INSERT") == 10
    assert summary.history_by_operation.get("UPDATE") == 10
    assert summary.history_by_operation.get("DELETE") == 2

    # Verify all individual integrity checks passed with 0 violations
    for name, check in summary.checks.items():
        assert check.passed is True, f"Check '{name}' failed with {check.violation_count} violations: {check.explanation}"
        assert check.violation_count == 0


def test_safe_rerun_idempotency_reconciliation() -> None:
    """Verify that a pipeline rerun preserves customer state and history counts."""
    sample_file = Path("data/input/sample_events.json")
    if not sample_file.exists():
        pytest.skip("sample_events.json does not exist.")

    pipeline = CDCPipeline()

    # First execution
    run1_id = uuid4()
    pipeline.run(input_files=[sample_file], run_id=run1_id)

    reconciler = CDCReconciler()
    summary1 = reconciler.run_reconciliation()
    assert summary1.all_passed is True
    assert summary1.total_customers == 10
    assert summary1.total_history_events == 22

    # Second execution (same input file)
    run2_id = uuid4()
    pipeline.run(input_files=[sample_file], run_id=run2_id)

    summary2 = reconciler.run_reconciliation()
    assert summary2.all_passed is True

    # Customer and history counts must not change on rerun
    assert summary2.total_customers == summary1.total_customers
    assert summary2.active_customers == summary1.active_customers
    assert summary2.soft_deleted_customers == summary1.soft_deleted_customers
    assert summary2.total_history_events == summary1.total_history_events

    # Event conservation must pass for both completed runs
    assert summary2.checks["event_conservation_per_run"].passed is True
    assert summary2.runs_by_status.get("COMPLETED") == 2


def test_soft_deleted_customer_audit() -> None:
    """Verify that soft-deleted customers are correctly identified and retain prior attributes."""
    sample_file = Path("data/input/sample_events.json")
    if not sample_file.exists():
        pytest.skip("sample_events.json does not exist.")

    pipeline = CDCPipeline()
    pipeline.run(input_files=[sample_file])

    reconciler = CDCReconciler()
    summary = reconciler.run_reconciliation()

    # Verify customer breakdown
    assert summary.total_customers == 10
    assert summary.soft_deleted_customers == 2

    # Verify soft-deleted customer attribute retention check passed
    assert summary.checks["soft_delete_attribute_retention"].passed is True
    assert summary.checks["deletion_metadata_consistency"].passed is True

    # Inspect specific soft-deleted customer records in PostgreSQL
    with get_db_session() as session:
        rows = session.execute(
            text("SELECT customer_id, customer_name, email, is_deleted FROM customers_current WHERE is_deleted = TRUE")
        ).mappings().all()

        deleted_ids = {r["customer_id"] for r in rows}
        assert deleted_ids == {"CUST-0009", "CUST-0010"}

        for r in rows:
            assert len(r["customer_name"]) > 0
            assert len(r["email"]) > 0


def test_detection_of_duplicate_history_events() -> None:
    """Verify that duplicate event IDs in customer_event_history are detected by reconciliation."""
    run_id = uuid4()
    dup_event_id = uuid4()
    now_dt = datetime.now(timezone.utc)

    with get_db_session() as session:
        # Create pipeline run
        session.execute(
            text("INSERT INTO pipeline_runs (run_id, status) VALUES (:run_id, 'COMPLETED');"),
            {"run_id": run_id},
        )
        # Temporarily drop PK constraint to introduce duplicate
        session.execute(text("ALTER TABLE customer_event_history DROP CONSTRAINT customer_event_history_pkey;"))
        try:
            # Insert first event
            session.execute(
                text("""
                    INSERT INTO customer_event_history (
                        event_id, customer_id, operation, event_time, source_sequence, run_id
                    ) VALUES (
                        :event_id, 'CUST-DUP-01', 'INSERT', :event_time, 1, :run_id
                    );
                """),
                {"event_id": dup_event_id, "event_time": now_dt, "run_id": run_id},
            )
            # Insert duplicate event ID
            session.execute(
                text("""
                    INSERT INTO customer_event_history (
                        event_id, customer_id, operation, event_time, source_sequence, run_id
                    ) VALUES (
                        :event_id, 'CUST-DUP-01', 'UPDATE', :event_time, 2, :run_id
                    );
                """),
                {"event_id": dup_event_id, "event_time": now_dt, "run_id": run_id},
            )

            reconciler = CDCReconciler(session=session)
            check = reconciler.check_duplicate_event_ids()

            assert check.passed is False
            assert check.violation_count == 1
            assert check.violations[0]["event_id"] == dup_event_id
            assert check.violations[0]["cnt"] == 2

        finally:
            # Cleanup and restore PK constraint
            session.execute(
                text("DELETE FROM customer_event_history WHERE event_id = :event_id;"),
                {"event_id": dup_event_id},
            )
            session.execute(text("ALTER TABLE customer_event_history ADD CONSTRAINT customer_event_history_pkey PRIMARY KEY (event_id);"))


def test_detection_of_inconsistent_deletion_metadata() -> None:
    """Verify that inconsistent is_deleted flags are flagged as violations."""
    run_id = uuid4()
    event_id = uuid4()
    now_dt = datetime.now(timezone.utc)

    with get_db_session() as session:
        # 1. Setup valid run and history event (operation = INSERT)
        session.execute(
            text("INSERT INTO pipeline_runs (run_id, status) VALUES (:run_id, 'COMPLETED');"),
            {"run_id": run_id},
        )
        session.execute(
            text("""
                INSERT INTO customer_event_history (
                    event_id, customer_id, operation, event_time, source_sequence, run_id
                ) VALUES (
                    :event_id, 'CUST-INCONSISTENT-01', 'INSERT', :event_time, 10, :run_id
                );
            """),
            {"event_id": event_id, "event_time": now_dt, "run_id": run_id},
        )

        # 2. Deliberately set customers_current.is_deleted = TRUE even though latest event was INSERT
        session.execute(
            text("""
                INSERT INTO customers_current (
                    customer_id, customer_name, email, city, is_deleted,
                    last_event_time, last_source_sequence, last_event_id
                ) VALUES (
                    'CUST-INCONSISTENT-01', 'Inconsistent User', 'inc@example.com', 'Testville',
                    TRUE, :event_time, 10, :event_id
                );
            """),
            {"event_time": now_dt, "event_id": event_id},
        )

        reconciler = CDCReconciler(session=session)
        check = reconciler.check_deletion_metadata_consistency()

        assert check.passed is False
        assert check.violation_count == 1
        assert check.violations[0]["customer_id"] == "CUST-INCONSISTENT-01"
        assert check.violations[0]["current_is_deleted"] is True
        assert check.violations[0]["latest_event_operation"] == "INSERT"


def test_detection_of_current_state_misalignment() -> None:
    """Verify that current state metadata out of sync with latest event is detected."""
    run_id = uuid4()
    event_id = uuid4()
    now_dt = datetime.now(timezone.utc)

    with get_db_session() as session:
        session.execute(
            text("INSERT INTO pipeline_runs (run_id, status) VALUES (:run_id, 'COMPLETED');"),
            {"run_id": run_id},
        )
        session.execute(
            text("""
                INSERT INTO customer_event_history (
                    event_id, customer_id, operation, event_time, source_sequence, run_id
                ) VALUES (
                    :event_id, 'CUST-MISALIGNED-01', 'INSERT', :event_time, 100, :run_id
                );
            """),
            {"event_id": event_id, "event_time": now_dt, "run_id": run_id},
        )

        # Set customers_current last_source_sequence to 99 instead of 100
        session.execute(
            text("""
                INSERT INTO customers_current (
                    customer_id, customer_name, email, city, is_deleted,
                    last_event_time, last_source_sequence, last_event_id
                ) VALUES (
                    'CUST-MISALIGNED-01', 'Misaligned User', 'mis@example.com', 'Testville',
                    FALSE, :event_time, 99, :event_id
                );
            """),
            {"event_time": now_dt, "event_id": event_id},
        )

        reconciler = CDCReconciler(session=session)
        check = reconciler.check_current_state_alignment()

        assert check.passed is False
        assert check.violation_count == 1
        assert check.violations[0]["customer_id"] == "CUST-MISALIGNED-01"
        assert check.violations[0]["current_last_sequence"] == 99
        assert check.violations[0]["history_latest_sequence"] == 100


def test_detection_of_invalid_customer_attributes() -> None:
    """Verify that active customers with missing or whitespace-only name/email are detected."""
    now_dt = datetime.now(timezone.utc)
    ev_id = uuid4()

    with get_db_session() as session:
        session.execute(
            text("""
                INSERT INTO customers_current (
                    customer_id, customer_name, email, city, is_deleted,
                    last_event_time, last_source_sequence, last_event_id
                ) VALUES (
                    'CUST-BLANK-01', '   ', 'blank@example.com', 'Testville',
                    FALSE, :event_time, 1, :event_id
                );
            """),
            {"event_time": now_dt, "event_id": ev_id},
        )

        reconciler = CDCReconciler(session=session)
        check = reconciler.check_active_customer_attributes()

        assert check.passed is False
        assert check.violation_count == 1
        assert check.violations[0]["customer_id"] == "CUST-BLANK-01"


def test_detection_of_orphaned_records() -> None:
    """Verify that events referencing nonexistent pipeline runs are detected."""
    nonexistent_run_id = uuid4()
    ev_id = uuid4()
    now_dt = datetime.now(timezone.utc)

    with get_db_session() as session:
        # Temporarily drop foreign key constraint to simulate orphaned record
        session.execute(text("ALTER TABLE customer_event_history DROP CONSTRAINT customer_event_history_run_id_fkey;"))
        try:
            session.execute(
                text("""
                    INSERT INTO customer_event_history (
                        event_id, customer_id, operation, event_time, source_sequence, run_id
                    ) VALUES (
                        :event_id, 'CUST-ORPHAN-01', 'INSERT', :event_time, 1, :run_id
                    );
                """),
                {"event_id": ev_id, "event_time": now_dt, "run_id": nonexistent_run_id},
            )

            reconciler = CDCReconciler(session=session)
            check = reconciler.check_orphaned_history_records()

            assert check.passed is False
            assert check.violation_count == 1
            assert check.violations[0]["event_id"] == ev_id

        finally:
            session.execute(
                text("DELETE FROM customer_event_history WHERE event_id = :event_id;"),
                {"event_id": ev_id},
            )
            session.execute(text("ALTER TABLE customer_event_history ADD CONSTRAINT customer_event_history_run_id_fkey FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id) ON DELETE RESTRICT;"))

