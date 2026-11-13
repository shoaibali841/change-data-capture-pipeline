"""Integration tests for the Main CDC Pipeline Orchestrator (src/pipeline.py).

Tests verify:
- Complete end-to-end pipeline execution using generated sample inputs.
- Correct customer current-state updates and soft deletions.
- Full event history recording (including out-of-order events).
- Quarantine store capturing both unparseable lines and schema rejections.
- Pipeline run metadata and counters accurately recorded.
- Safe idempotent reruns without duplicate state mutations.
- Failure handling and atomic rollbacks with FAILED run status recording.
- Correct isolation when processing mixed valid and invalid batches.
"""

import json
from pathlib import Path
from typing import Generator
from uuid import UUID, uuid4
import pytest
from sqlalchemy import text

from src.database.session import get_db_session, get_test_db_session, safe_test_cleanup
from src.pipeline import CDCPipeline
from src.publishing.repository import CDCRepository


@pytest.fixture(autouse=True)
def clean_database_state() -> Generator[None, None, None]:
    """Clean all test tables before and after each test on the verified test database."""
    def cleanup():
        with get_test_db_session() as session:
            safe_test_cleanup(session)

    cleanup()
    yield
    cleanup()


def test_complete_pipeline_run_with_generated_dataset() -> None:
    """Verify end-to-end pipeline execution on data/input/sample_events.json & malformed_events.jsonl."""
    sample_file = Path("data/input/sample_events.json")
    malformed_file = Path("data/input/malformed_events.jsonl")

    if not sample_file.exists() or not malformed_file.exists():
        pytest.skip("Input files do not exist.")

    pipeline = CDCPipeline()
    run_id = uuid4()
    summary = pipeline.run(
        input_files=[sample_file, malformed_file],
        run_id=run_id,
    )

    # 1. Pipeline Run Assertions
    assert summary["status"] == "COMPLETED"
    assert summary["run_id"] == str(run_id)
    assert summary["duration_seconds"] >= 0.0

    m = summary["metrics"]
    assert m["input_count"] == 32       # 30 in sample_events.json + 2 in malformed_events.jsonl
    assert m["accepted_count"] == 22    # 10 inserts + 8 updates + 2 deletes + 2 stale events
    assert m["inserted_count"] == 10
    assert m["updated_count"] == 8
    assert m["deleted_count"] == 2
    assert m["stale_event_count"] == 2  # Out-of-order events preserved in history
    assert m["duplicate_count"] == 3    # 3 duplicate events
    assert m["rejected_count"] == 7     # 2 malformed JSON + 5 schema validation rejections

    # 2. Database Verification
    with get_db_session() as session:
        repo = CDCRepository(session)

        # Check pipeline_runs table
        db_run = repo.get_pipeline_run(run_id, session=session)
        assert db_run is not None
        assert db_run["status"] == "COMPLETED"
        assert db_run["input_count"] == 32
        assert db_run["accepted_count"] == 22
        assert db_run["rejected_count"] == 7

        # Check customers_current table: 10 total customers
        cust_rows = session.execute(
            text("SELECT customer_id, customer_name, email, city, is_deleted FROM customers_current ORDER BY customer_id")
        ).mappings().all()
        assert len(cust_rows) == 10

        # Check soft deletions: CUST-0009 and CUST-0010 were deleted
        deleted_custs = [c for c in cust_rows if c["is_deleted"] is True]
        deleted_ids = {c["customer_id"] for c in deleted_custs}
        assert deleted_ids == {"CUST-0009", "CUST-0010"}

        # Deleted customers still preserve their names and emails
        for dc in deleted_custs:
            assert len(dc["customer_name"]) > 0
            assert len(dc["email"]) > 0

        # Active customers
        active_custs = [c for c in cust_rows if c["is_deleted"] is False]
        assert len(active_custs) == 8

        # Check customer_event_history: exactly 22 accepted events
        hist_count = session.execute(text("SELECT count(*) FROM customer_event_history WHERE run_id = :rid"), {"rid": run_id}).scalar()
        assert hist_count == 22

        # Check event_quarantine: exactly 7 quarantined records
        quar_rows = session.execute(
            text("SELECT quarantine_id, event_id, raw_payload, rejection_reason, source_file FROM event_quarantine WHERE run_id = :rid"),
            {"rid": run_id},
        ).mappings().all()
        assert len(quar_rows) == 7

        # Check that reasons are informative
        reasons = [r["rejection_reason"] for r in quar_rows]
        assert any("JSONDecodeError" in r for r in reasons)
        assert any("customer_id" in r for r in reasons)
        assert any("operation" in r for r in reasons)
        assert any("event_id" in r for r in reasons)
        assert any("source_sequence" in r for r in reasons)


def test_pipeline_safe_rerun_idempotency() -> None:
    """Verify executing the pipeline twice against the same data does not cause duplicate state mutations."""
    sample_file = Path("data/input/sample_events.json")
    if not sample_file.exists():
        pytest.skip("sample_events.json does not exist.")

    pipeline = CDCPipeline()

    # Run 1
    run1_id = uuid4()
    summary1 = pipeline.run(input_files=[sample_file], run_id=run1_id)
    assert summary1["status"] == "COMPLETED"
    assert summary1["metrics"]["accepted_count"] == 22

    # Snapshot current customers table
    with get_db_session() as session:
        custs_run1 = session.execute(
            text("SELECT customer_id, customer_name, email, city, is_deleted, last_event_id FROM customers_current ORDER BY customer_id")
        ).mappings().all()
        hist_count_run1 = session.execute(text("SELECT count(*) FROM customer_event_history")).scalar()
        assert hist_count_run1 == 22

    # Run 2 (Rerun with the same input file)
    run2_id = uuid4()
    summary2 = pipeline.run(input_files=[sample_file], run_id=run2_id)
    assert summary2["status"] == "COMPLETED"

    # All previously accepted events are now detected as exact duplicates!
    m2 = summary2["metrics"]
    assert m2["accepted_count"] == 0
    assert m2["inserted_count"] == 0
    assert m2["updated_count"] == 0
    assert m2["deleted_count"] == 0
    assert m2["duplicate_count"] == 25  # 22 previous events + 3 duplicate events

    # Snapshot customer state after run 2
    with get_db_session() as session:
        custs_run2 = session.execute(
            text("SELECT customer_id, customer_name, email, city, is_deleted, last_event_id FROM customers_current ORDER BY customer_id")
        ).mappings().all()
        hist_count_run2 = session.execute(text("SELECT count(*) FROM customer_event_history")).scalar()

        # State is 100% identical and no duplicate history rows were added
        assert custs_run1 == custs_run2
        assert hist_count_run1 == hist_count_run2 == 22


def test_pipeline_failure_handling_and_status_recording(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Verify that a fatal pipeline exception marks pipeline_runs as FAILED and rolls back the batch."""
    test_file = tmp_path / "events.json"
    test_file.write_text(
        json.dumps([
            {
                "event_id": str(uuid4()),
                "customer_id": "CUST-FAIL-01",
                "operation": "INSERT",
                "event_time": "2026-11-13T10:00:00Z",
                "source_sequence": 1,
                "payload": {"customer_name": "Fail Test", "email": "fail@test.com"},
            }
        ]),
        encoding="utf-8",
    )

    pipeline = CDCPipeline()
    run_id = uuid4()

    # Simulate an error during database mutation
    def simulated_error(*args, **kwargs):
        raise RuntimeError("Simulated database failure during batch execution")

    monkeypatch.setattr(CDCRepository, "apply_batch_mutations", simulated_error)
    monkeypatch.setattr(CDCRepository, "apply_event_mutation", simulated_error)

    with pytest.raises(RuntimeError, match="Simulated database failure during batch execution"):
        pipeline.run(input_files=[test_file], run_id=run_id)

    # Verify run record in PostgreSQL is marked as FAILED with error message
    with get_db_session() as session:
        repo = CDCRepository(session)
        run_row = repo.get_pipeline_run(run_id, session=session)
        assert run_row is not None
        assert run_row["status"] == "FAILED"
        assert "Simulated database failure" in run_row["error_message"]

        # Verify transaction rollback: no customer or history records were committed
        cust = repo.get_customer_current("CUST-FAIL-01", session=session)
        assert cust is None
        h_count = session.execute(text("SELECT count(*) FROM customer_event_history WHERE run_id = :rid"), {"rid": run_id}).scalar()
        assert h_count == 0


def test_mixed_valid_and_invalid_custom_stream(tmp_path: Path) -> None:
    """Verify a stream containing mixed inserts, updates, duplicates, and malformed items."""
    stream_file = tmp_path / "mixed_stream.jsonl"
    ev1_id = str(uuid4())
    lines = [
        # Valid Insert
        json.dumps({
            "event_id": ev1_id,
            "customer_id": "STREAM-01",
            "operation": "INSERT",
            "event_time": "2026-11-13T10:00:00Z",
            "source_sequence": 1,
            "payload": {"customer_name": "Stream One", "email": "stream1@test.com", "city": "City 1"},
        }),
        # Malformed raw text line
        "INVALID_RAW_JSON_SYNTAX_LINE_<<<>>>",
        # Valid Update
        json.dumps({
            "event_id": str(uuid4()),
            "customer_id": "STREAM-01",
            "operation": "UPDATE",
            "event_time": "2026-11-13T10:05:00Z",
            "source_sequence": 2,
            "payload": {"city": "Updated City 1"},
        }),
        # Schema Invalid (missing customer_id)
        json.dumps({
            "event_id": str(uuid4()),
            "operation": "INSERT",
            "event_time": "2026-11-13T10:06:00Z",
            "source_sequence": 3,
            "payload": {"customer_name": "Anon", "email": "anon@test.com"},
        }),
        # Exact Duplicate of ev1
        json.dumps({
            "event_id": ev1_id,
            "customer_id": "STREAM-01",
            "operation": "INSERT",
            "event_time": "2026-11-13T10:00:00Z",
            "source_sequence": 1,
            "payload": {"customer_name": "Stream One", "email": "stream1@test.com", "city": "City 1"},
        }),
    ]
    stream_file.write_text("\n".join(lines), encoding="utf-8")

    pipeline = CDCPipeline()
    run_id = uuid4()
    summary = pipeline.run(input_files=[stream_file], run_id=run_id)

    assert summary["status"] == "COMPLETED"
    m = summary["metrics"]
    assert m["input_count"] == 5
    assert m["accepted_count"] == 2   # 1 insert + 1 update
    assert m["inserted_count"] == 1
    assert m["updated_count"] == 1
    assert m["duplicate_count"] == 1  # 1 duplicate
    assert m["rejected_count"] == 2   # 1 malformed JSON + 1 schema rejection

    with get_db_session() as session:
        repo = CDCRepository(session)
        cust = repo.get_customer_current("STREAM-01", session=session)
        assert cust is not None
        assert cust["city"] == "Updated City 1"
        assert cust["last_source_sequence"] == 2


def test_empty_input_file(tmp_path: Path) -> None:
    """Verify pipeline completes cleanly with zero records on an empty input file."""
    empty_file = tmp_path / "empty.json"
    empty_file.write_text("[]", encoding="utf-8")

    pipeline = CDCPipeline()
    run_id = uuid4()
    summary = pipeline.run(input_files=[empty_file], run_id=run_id)

    assert summary["status"] == "COMPLETED"
    assert summary["metrics"]["input_count"] == 0
    assert summary["metrics"]["accepted_count"] == 0
    assert summary["metrics"]["rejected_count"] == 0
    assert summary["duration_seconds"] >= 0.0

    with get_db_session() as session:
        repo = CDCRepository(session)
        run = repo.get_pipeline_run(run_id, session=session)
        assert run["status"] == "COMPLETED"
        assert run["input_count"] == 0


def test_empty_input_directory(tmp_path: Path) -> None:
    """Verify pipeline completes cleanly with zero records on an empty input directory."""
    empty_dir = tmp_path / "empty_dir"
    empty_dir.mkdir(parents=True, exist_ok=True)

    pipeline = CDCPipeline(input_dir=empty_dir)
    run_id = uuid4()
    summary = pipeline.run(run_id=run_id)

    assert summary["status"] == "COMPLETED"
    assert summary["metrics"]["input_count"] == 0
    assert summary["metrics"]["accepted_count"] == 0
    assert summary["metrics"]["rejected_count"] == 0


def test_successful_execution_after_failed_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Verify pipeline recovers cleanly and succeeds on a subsequent run after a fatal failure."""
    valid_file = tmp_path / "valid_event.json"
    valid_file.write_text(
        json.dumps([
            {
                "event_id": str(uuid4()),
                "customer_id": "RECOVER-CUST-01",
                "operation": "INSERT",
                "event_time": "2026-11-13T10:00:00Z",
                "source_sequence": 1,
                "payload": {"customer_name": "Recover User", "email": "recover@test.com"},
            }
        ]),
        encoding="utf-8",
    )

    pipeline = CDCPipeline()

    # 1. First Run: fails due to simulated error
    run1_id = uuid4()
    def fail_mutation(*args, **kwargs):
        raise RuntimeError("Fatal DB crash simulation")

    orig_batch = CDCRepository.apply_batch_mutations
    orig_mutation = CDCRepository.apply_event_mutation
    monkeypatch.setattr(CDCRepository, "apply_batch_mutations", fail_mutation)
    monkeypatch.setattr(CDCRepository, "apply_event_mutation", fail_mutation)

    with pytest.raises(RuntimeError):
        pipeline.run(input_files=[valid_file], run_id=run1_id)

    with get_db_session() as session:
        repo = CDCRepository(session)
        run1 = repo.get_pipeline_run(run1_id, session=session)
        assert run1["status"] == "FAILED"

    # 2. Second Run: restore normal repository behavior and rerun
    monkeypatch.setattr(CDCRepository, "apply_batch_mutations", orig_batch)
    monkeypatch.setattr(CDCRepository, "apply_event_mutation", orig_mutation)
    run2_id = uuid4()
    summary2 = pipeline.run(input_files=[valid_file], run_id=run2_id)

    assert summary2["status"] == "COMPLETED"
    assert summary2["metrics"]["accepted_count"] == 1
    assert summary2["metrics"]["inserted_count"] == 1

    with get_db_session() as session:
        repo = CDCRepository(session)
        run2 = repo.get_pipeline_run(run2_id, session=session)
        assert run2["status"] == "COMPLETED"
        assert run2["inserted_count"] == 1

        cust = repo.get_customer_current("RECOVER-CUST-01", session=session)
        assert cust is not None
        assert cust["customer_name"] == "Recover User"


def test_accurate_duration_measurement(tmp_path: Path) -> None:
    """Verify duration_seconds accurately records monotonic wall-clock elapsed time."""
    test_file = tmp_path / "timed_events.json"
    test_file.write_text(
        json.dumps([
            {
                "event_id": str(uuid4()),
                "customer_id": "TIMED-01",
                "operation": "INSERT",
                "event_time": "2026-11-13T10:00:00Z",
                "source_sequence": 1,
                "payload": {"customer_name": "Timer Test", "email": "timer@test.com"},
            }
        ]),
        encoding="utf-8",
    )

    pipeline = CDCPipeline()
    run_id = uuid4()
    summary = pipeline.run(input_files=[test_file], run_id=run_id)

    assert summary["duration_seconds"] > 0.0

    with get_db_session() as session:
        repo = CDCRepository(session)
        db_run = repo.get_pipeline_run(run_id, session=session)
        assert db_run["duration_seconds"] is not None
        assert float(db_run["duration_seconds"]) > 0.0
        # Precision check: verify summary duration matches database stored duration
        assert abs(float(db_run["duration_seconds"]) - summary["duration_seconds"]) < 1e-4


