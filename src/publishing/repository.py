"""PostgreSQL Repository for CDC Pipeline Persistence.

Implements database persistence operations using SQLAlchemy 2.0:
- Pipeline run metadata, lifecycle management, and metric updates.
- Materialized customers_current state with deterministic out-of-order checks,
  partial attribute merging, and soft deletion.
- Append-only customer_event_history with exact duplicate protection and conflict detection.
- event_quarantine store for malformed and invalid event payloads with lineage traceability.
"""

from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, List, Optional, Tuple, Union
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.database.session import get_db_session
from src.processing.event_processor import ProcessStatus
from src.validation.event_schema import CDCEvent, OperationType

logger = logging.getLogger(__name__)


class CDCRepository:
    """Repository handling atomic persistence for CDC pipeline entities."""

    def __init__(self, session: Optional[Session] = None) -> None:
        """Initialize repository with optional external session.

        Args:
            session: Optional existing SQLAlchemy session for transaction scoping.
        """
        self._external_session = session

    def _get_active_session(self, session: Optional[Session]) -> Session:
        """Determine active session: method-provided > instance-provided.

        Raises:
            RuntimeError: If no session was provided to method or instance.
        """
        s = session or self._external_session
        if s is None:
            raise RuntimeError(
                "No active database session available. "
                "Provide a session to the repository constructor or the method call."
            )
        return s

    # =========================================================================
    # 1. Pipeline Runs Operations
    # =========================================================================

    def create_pipeline_run(
        self,
        run_id: Optional[Union[UUID, str]] = None,
        session: Optional[Session] = None,
    ) -> UUID:
        """Create a new pipeline run record with status 'RUNNING'.

        Args:
            run_id: Optional predetermined UUID. If None, generates a new UUID.
            session: Optional active session.

        Returns:
            UUID: The created pipeline run identifier.
        """
        s = self._get_active_session(session)
        target_run_id = UUID(str(run_id)) if run_id else uuid4()

        stmt = text(
            """
            INSERT INTO pipeline_runs (
                run_id, started_at, status, input_count, accepted_count,
                inserted_count, updated_count, deleted_count, duplicate_count,
                stale_event_count, rejected_count
            ) VALUES (
                :run_id, CURRENT_TIMESTAMP, 'RUNNING', 0, 0, 0, 0, 0, 0, 0, 0
            );
            """
        )
        s.execute(stmt, {"run_id": target_run_id})
        logger.info("Created pipeline_run %s with status RUNNING", target_run_id)
        return target_run_id

    def complete_pipeline_run(
        self,
        run_id: Union[UUID, str],
        status: str = "COMPLETED",
        metrics: Optional[Dict[str, int]] = None,
        duration_seconds: Optional[float] = None,
        error_message: Optional[str] = None,
        session: Optional[Session] = None,
    ) -> None:
        """Finalize a pipeline run, updating status, duration, and event counters.

        Args:
            run_id: UUID of the target run.
            status: 'COMPLETED' or 'FAILED'.
            metrics: Optional dictionary containing counter values.
            duration_seconds: Optional wall-clock duration in seconds from monotonic timer.
            error_message: Optional failure explanation.
            session: Optional active session.
        """
        s = self._get_active_session(session)
        target_run_id = UUID(str(run_id))
        m = metrics or {}

        if duration_seconds is not None:
            # Use reliable monotonic timer duration passed by the orchestrator
            stmt = text(
                """
                UPDATE pipeline_runs
                SET 
                    completed_at = CURRENT_TIMESTAMP,
                    status = :status,
                    input_count = :input_count,
                    accepted_count = :accepted_count,
                    inserted_count = :inserted_count,
                    updated_count = :updated_count,
                    deleted_count = :deleted_count,
                    duplicate_count = :duplicate_count,
                    stale_event_count = :stale_event_count,
                    rejected_count = :rejected_count,
                    error_message = :error_message,
                    duration_seconds = :duration_seconds
                WHERE run_id = :run_id;
                """
            )
            params = {
                "run_id": target_run_id,
                "status": status,
                "input_count": m.get("input_count", 0),
                "accepted_count": m.get("accepted_count", 0),
                "inserted_count": m.get("inserted_count", 0),
                "updated_count": m.get("updated_count", 0),
                "deleted_count": m.get("deleted_count", 0),
                "duplicate_count": m.get("duplicate_count", 0),
                "stale_event_count": m.get("stale_event_count", 0),
                "rejected_count": m.get("rejected_count", 0),
                "error_message": error_message,
                "duration_seconds": round(duration_seconds, 4),
            }
        else:
            # Fallback to PostgreSQL clock_timestamp() vs transaction-frozen CURRENT_TIMESTAMP
            stmt = text(
                """
                UPDATE pipeline_runs
                SET 
                    completed_at = CURRENT_TIMESTAMP,
                    status = :status,
                    input_count = :input_count,
                    accepted_count = :accepted_count,
                    inserted_count = :inserted_count,
                    updated_count = :updated_count,
                    deleted_count = :deleted_count,
                    duplicate_count = :duplicate_count,
                    stale_event_count = :stale_event_count,
                    rejected_count = :rejected_count,
                    error_message = :error_message,
                    duration_seconds = EXTRACT(EPOCH FROM (clock_timestamp() - started_at))
                WHERE run_id = :run_id;
                """
            )
            params = {
                "run_id": target_run_id,
                "status": status,
                "input_count": m.get("input_count", 0),
                "accepted_count": m.get("accepted_count", 0),
                "inserted_count": m.get("inserted_count", 0),
                "updated_count": m.get("updated_count", 0),
                "deleted_count": m.get("deleted_count", 0),
                "duplicate_count": m.get("duplicate_count", 0),
                "stale_event_count": m.get("stale_event_count", 0),
                "rejected_count": m.get("rejected_count", 0),
                "error_message": error_message,
            }

        s.execute(stmt, params)
        logger.info("Updated pipeline_run %s to status %s (duration=%ss)", target_run_id, status, params.get("duration_seconds"))

    def get_pipeline_run(
        self,
        run_id: Union[UUID, str],
        session: Optional[Session] = None,
    ) -> Optional[Dict[str, Any]]:
        """Fetch pipeline run record by run_id."""
        s = self._get_active_session(session)
        target_run_id = UUID(str(run_id))
        stmt = text(
            """
            SELECT run_id, started_at, completed_at, status, input_count,
                   accepted_count, inserted_count, updated_count, deleted_count,
                   duplicate_count, stale_event_count, rejected_count,
                   error_message, duration_seconds
            FROM pipeline_runs
            WHERE run_id = :run_id;
            """
        )
        row = s.execute(stmt, {"run_id": target_run_id}).mappings().first()
        return dict(row) if row else None

    # =========================================================================
    # 2. Event History & Deduplication Operations
    # =========================================================================

    def get_event_from_history(
        self,
        event_id: Union[UUID, str],
        session: Optional[Session] = None,
    ) -> Optional[Dict[str, Any]]:
        """Fetch historical event by event_id."""
        s = self._get_active_session(session)
        target_event_id = UUID(str(event_id))
        stmt = text(
            """
            SELECT event_id, customer_id, operation, old_values, new_values,
                   event_time, source_sequence, processed_at, source_file, run_id
            FROM customer_event_history
            WHERE event_id = :event_id;
            """
        )
        row = s.execute(stmt, {"event_id": target_event_id}).mappings().first()
        return dict(row) if row else None

    def check_duplicate_or_conflict(
        self,
        event: CDCEvent,
        session: Optional[Session] = None,
    ) -> Tuple[bool, bool]:
        """Check if an event is an exact duplicate or a conflicting reuse of an event ID.

        Returns:
            Tuple[bool, bool]: (is_duplicate, is_conflict)
        """
        existing = self.get_event_from_history(event.event_id, session=session)
        if not existing:
            return False, False

        # Compare canonical attributes
        op_str = event.operation.value if hasattr(event.operation, "value") else str(event.operation)
        matches_customer = existing["customer_id"] == event.customer_id
        matches_op = existing["operation"] == op_str
        matches_seq = existing["source_sequence"] == event.source_sequence

        # Time match: compare epoch seconds or ISO
        db_time = existing["event_time"]
        ev_time = event.event_time
        matches_time = abs((db_time - ev_time).total_seconds()) < 1e-3

        if matches_customer and matches_op and matches_seq and matches_time:
            return True, False  # Exact duplicate
        return False, True  # Conflicting event

    def insert_event_history(
        self,
        event_id: Union[UUID, str],
        customer_id: str,
        operation: str,
        event_time: datetime,
        source_sequence: int,
        run_id: Union[UUID, str],
        old_values: Optional[Dict[str, Any]] = None,
        new_values: Optional[Dict[str, Any]] = None,
        source_file: Optional[str] = None,
        session: Optional[Session] = None,
    ) -> None:
        """Persist an accepted unique event into customer_event_history."""
        s = self._get_active_session(session)
        stmt = text(
            """
            INSERT INTO customer_event_history (
                event_id, customer_id, operation, old_values, new_values,
                event_time, source_sequence, processed_at, source_file, run_id
            ) VALUES (
                :event_id, :customer_id, :operation, CAST(:old_values AS jsonb),
                CAST(:new_values AS jsonb), :event_time, :source_sequence,
                CURRENT_TIMESTAMP, :source_file, :run_id
            );
            """
        )
        s.execute(
            stmt,
            {
                "event_id": UUID(str(event_id)),
                "customer_id": customer_id,
                "operation": operation,
                "old_values": json.dumps(old_values) if old_values is not None else None,
                "new_values": json.dumps(new_values) if new_values is not None else None,
                "event_time": event_time,
                "source_sequence": source_sequence,
                "source_file": source_file,
                "run_id": UUID(str(run_id)),
            },
        )

    # =========================================================================
    # 3. Current Customer State Operations
    # =========================================================================

    def get_customer_current(
        self,
        customer_id: str,
        for_update: bool = False,
        session: Optional[Session] = None,
    ) -> Optional[Dict[str, Any]]:
        """Fetch current customer materialized state, optionally acquiring a row-level lock."""
        s = self._get_active_session(session)
        lock_clause = " FOR UPDATE" if for_update else ""
        stmt = text(
            f"""
            SELECT customer_id, customer_name, email, city, is_deleted,
                   last_event_time, last_source_sequence, last_event_id, updated_at
            FROM customers_current
            WHERE customer_id = :customer_id{lock_clause};
            """
        )
        row = s.execute(stmt, {"customer_id": customer_id}).mappings().first()
        return dict(row) if row else None

    def upsert_customer_current(
        self,
        customer_id: str,
        customer_name: str,
        email: str,
        city: Optional[str],
        is_deleted: bool,
        last_event_time: datetime,
        last_source_sequence: int,
        last_event_id: Union[UUID, str],
        session: Optional[Session] = None,
    ) -> None:
        """Upsert customer record in customers_current table."""
        s = self._get_active_session(session)
        stmt = text(
            """
            INSERT INTO customers_current (
                customer_id, customer_name, email, city, is_deleted,
                last_event_time, last_source_sequence, last_event_id, updated_at
            ) VALUES (
                :customer_id, :customer_name, :email, :city, :is_deleted,
                :last_event_time, :last_source_sequence, :last_event_id, CURRENT_TIMESTAMP
            )
            ON CONFLICT (customer_id) DO UPDATE
            SET 
                customer_name = EXCLUDED.customer_name,
                email = EXCLUDED.email,
                city = EXCLUDED.city,
                is_deleted = EXCLUDED.is_deleted,
                last_event_time = EXCLUDED.last_event_time,
                last_source_sequence = EXCLUDED.last_source_sequence,
                last_event_id = EXCLUDED.last_event_id,
                updated_at = CURRENT_TIMESTAMP;
            """
        )
        s.execute(
            stmt,
            {
                "customer_id": customer_id,
                "customer_name": customer_name,
                "email": email,
                "city": city,
                "is_deleted": is_deleted,
                "last_event_time": last_event_time,
                "last_source_sequence": last_source_sequence,
                "last_event_id": UUID(str(last_event_id)),
            },
        )

    # =========================================================================
    # 4. Atomic Mutation Processor
    # =========================================================================

    def apply_event_mutation(
        self,
        event: CDCEvent,
        run_id: Union[UUID, str],
        source_file: Optional[str] = None,
        session: Optional[Session] = None,
    ) -> Tuple[ProcessStatus, Optional[Dict[str, Any]]]:
        """Atomically process and persist a validated CDC event into PostgreSQL.

        Enforces:
        - Exact duplicate check: skips state mutation and history duplication.
        - Conflict detection: flags re-used event IDs with differing payloads.
        - Out-of-order check: preserves stale events in history without mutating state.
        - State update: partial updates, soft deletion, and audit snapshotting.

        Returns:
            Tuple[ProcessStatus, Optional[Dict[str, Any]]]: Processing status and current customer state.
        """
        s = self._get_active_session(session)
        event_id = UUID(str(event.event_id))
        cust_id = event.customer_id
        target_run_id = UUID(str(run_id))
        op_str = event.operation.value if hasattr(event.operation, "value") else str(event.operation)

        # 1. Duplicate & Conflict Check
        is_dup, is_conflict = self.check_duplicate_or_conflict(event, session=s)
        if is_dup:
            current_state = self.get_customer_current(cust_id, session=s)
            return ProcessStatus.DUPLICATE_IGNORED, current_state
        if is_conflict:
            current_state = self.get_customer_current(cust_id, session=s)
            return ProcessStatus.CONFLICTING_EVENT, current_state

        # 2. Lock and retrieve current customer state
        current_state = self.get_customer_current(cust_id, for_update=True, session=s)

        # 3. Check for Out-of-Order (stale) arrivals
        if current_state is not None:
            incoming_order = (event.event_time, event.source_sequence, str(event_id))
            current_order = (
                current_state["last_event_time"],
                current_state["last_source_sequence"],
                str(current_state["last_event_id"]),
            )

            if incoming_order < current_order:
                # Stale event: preserve in customer_event_history, do NOT mutate customers_current
                self.insert_event_history(
                    event_id=event_id,
                    customer_id=cust_id,
                    operation=op_str,
                    event_time=event.event_time,
                    source_sequence=event.source_sequence,
                    run_id=target_run_id,
                    old_values=None,
                    new_values=event.payload,
                    source_file=source_file,
                    session=s,
                )
                return ProcessStatus.STALE_EVENT_PRESERVED, current_state

        # 4. Prepare New State & Audit Snapshots
        old_values = (
            {
                "customer_name": current_state["customer_name"],
                "email": current_state["email"],
                "city": current_state["city"],
                "is_deleted": current_state["is_deleted"],
            }
            if current_state
            else None
        )

        if event.operation == OperationType.INSERT:
            name = event.payload.get("customer_name") or (current_state["customer_name"] if current_state else "")
            email = event.payload.get("email") or (current_state["email"] if current_state else "")
            city = event.payload.get("city") if "city" in event.payload else (current_state["city"] if current_state else None)
            is_del = False
            status = ProcessStatus.APPLIED_INSERT

        elif event.operation == OperationType.UPDATE:
            name = (
                event.payload["customer_name"]
                if "customer_name" in event.payload and event.payload["customer_name"] is not None
                else (current_state["customer_name"] if current_state else "")
            )
            email = (
                event.payload["email"]
                if "email" in event.payload and event.payload["email"] is not None
                else (current_state["email"] if current_state else "")
            )
            city = (
                event.payload["city"]
                if "city" in event.payload
                else (current_state["city"] if current_state else None)
            )
            is_del = current_state["is_deleted"] if current_state else False
            status = ProcessStatus.APPLIED_UPDATE

        elif event.operation == OperationType.DELETE:
            name = current_state["customer_name"] if current_state else ""
            email = current_state["email"] if current_state else ""
            city = current_state["city"] if current_state else None
            is_del = True
            status = ProcessStatus.APPLIED_DELETE

        else:
            raise ValueError(f"Unsupported operation: {event.operation}")

        # Update customers_current
        self.upsert_customer_current(
            customer_id=cust_id,
            customer_name=name,
            email=email,
            city=city,
            is_deleted=is_del,
            last_event_time=event.event_time,
            last_source_sequence=event.source_sequence,
            last_event_id=event_id,
            session=s,
        )

        new_values = {
            "customer_name": name,
            "email": email,
            "city": city,
            "is_deleted": is_del,
        }

        # Persist to customer_event_history
        self.insert_event_history(
            event_id=event_id,
            customer_id=cust_id,
            operation=op_str,
            event_time=event.event_time,
            source_sequence=event.source_sequence,
            run_id=target_run_id,
            old_values=old_values,
            new_values=new_values,
            source_file=source_file,
            session=s,
        )

        updated_state = self.get_customer_current(cust_id, session=s)
        return status, updated_state

    # =========================================================================
    # 5. Quarantine Operations
    # =========================================================================

    def insert_quarantine(
        self,
        raw_payload: str,
        run_id: Union[UUID, str],
        rejection_reason: str,
        event_id: Optional[Union[UUID, str]] = None,
        source_file: Optional[str] = None,
        session: Optional[Session] = None,
    ) -> int:
        """Insert a rejected or malformed event into event_quarantine.

        Returns:
            int: The generated quarantine_id.
        """
        s = self._get_active_session(session)
        target_run_id = UUID(str(run_id))

        target_event_id = None
        if event_id:
            try:
                target_event_id = UUID(str(event_id))
            except ValueError:
                # If event_id string is not a valid UUID, store None in the UUID column
                target_event_id = None

        stmt = text(
            """
            INSERT INTO event_quarantine (
                event_id, raw_payload, source_file, run_id,
                rejection_reason, rejected_at
            ) VALUES (
                :event_id, :raw_payload, :source_file, :run_id,
                :rejection_reason, CURRENT_TIMESTAMP
            )
            RETURNING quarantine_id;
            """
        )
        quarantine_id = s.execute(
            stmt,
            {
                "event_id": target_event_id,
                "raw_payload": raw_payload,
                "source_file": source_file,
                "run_id": target_run_id,
                "rejection_reason": rejection_reason,
            },
        ).scalar()

        logger.info(
            "Persisted record to event_quarantine (quarantine_id=%s, run_id=%s)",
            quarantine_id,
            target_run_id,
        )
        return int(quarantine_id)

    def insert_quarantine_batch(
        self,
        records: List[Dict[str, Any]],
        session: Optional[Session] = None,
    ) -> int:
        """Bulk insert rejected or malformed records into event_quarantine.

        Args:
            records: List of dicts with keys: raw_payload, run_id, rejection_reason, event_id, source_file.
            session: Optional active session.

        Returns:
            int: Number of records inserted.
        """
        if not records:
            return 0

        s = self._get_active_session(session)
        params = []
        for r in records:
            eid = r.get("event_id")
            target_eid = None
            if eid:
                try:
                    target_eid = UUID(str(eid))
                except ValueError:
                    target_eid = None
            params.append(
                {
                    "event_id": target_eid,
                    "raw_payload": r["raw_payload"],
                    "source_file": r.get("source_file"),
                    "run_id": UUID(str(r["run_id"])),
                    "rejection_reason": r["rejection_reason"],
                }
            )

        stmt = text(
            """
            INSERT INTO event_quarantine (
                event_id, raw_payload, source_file, run_id,
                rejection_reason, rejected_at
            ) VALUES (
                :event_id, :raw_payload, :source_file, :run_id,
                :rejection_reason, CURRENT_TIMESTAMP
            );
            """
        )
        s.execute(stmt, params)
        logger.info("Batch inserted %d records into event_quarantine", len(params))
        return len(params)

    def apply_batch_mutations(
        self,
        valid_events: List[Any],
        run_id: Union[UUID, str],
        session: Optional[Session] = None,
        chunk_size: int = 5000,
    ) -> Dict[str, int]:
        """Process and persist a batch of validated CDC events efficiently.

        Reduces database round-trips from O(N) to O(1) bulk operations:
        1. Bulk pre-fetches event history to identify duplicates and conflicts.
        2. Bulk pre-fetches and row-locks current customer states.
        3. Deterministically applies in-memory mutations respecting event stream ordering.
        4. Bulk upserts modified customer states.
        5. Bulk inserts accepted event history records.
        6. Bulk inserts any detected conflict records into quarantine.

        Returns:
            Dict[str, int]: Cumulative counters for the processed batch.
        """
        if not valid_events:
            return {
                "accepted_count": 0,
                "inserted_count": 0,
                "updated_count": 0,
                "deleted_count": 0,
                "duplicate_count": 0,
                "stale_event_count": 0,
                "rejected_count": 0,
            }

        s = self._get_active_session(session)
        target_run_id = UUID(str(run_id))

        # Chunk large batches to optimize PostgreSQL memory and query parameter limits
        if len(valid_events) > chunk_size:
            total_metrics = {
                "accepted_count": 0,
                "inserted_count": 0,
                "updated_count": 0,
                "deleted_count": 0,
                "duplicate_count": 0,
                "stale_event_count": 0,
                "rejected_count": 0,
            }
            for i in range(0, len(valid_events), chunk_size):
                chunk = valid_events[i : i + chunk_size]
                chunk_metrics = self.apply_batch_mutations(
                    chunk, run_id=target_run_id, session=s, chunk_size=chunk_size
                )
                for k in total_metrics:
                    total_metrics[k] += chunk_metrics[k]
            return total_metrics

        # 1. Bulk lookup for duplicates/conflicts in customer_event_history
        event_uuids = [UUID(str(ve.event.event_id)) for ve in valid_events]
        hist_lookup_stmt = text(
            """
            SELECT event_id, customer_id, operation, event_time, source_sequence
            FROM customer_event_history
            WHERE event_id = ANY(:event_ids);
            """
        )
        existing_hist_rows = s.execute(hist_lookup_stmt, {"event_ids": event_uuids}).mappings().all()
        seen_history: Dict[UUID, Dict[str, Any]] = {
            r["event_id"]: dict(r) for r in existing_hist_rows
        }

        # 2. Bulk lookup and row-lock for customer states
        customer_ids = list({ve.event.customer_id for ve in valid_events})
        cust_lookup_stmt = text(
            """
            SELECT customer_id, customer_name, email, city, is_deleted,
                   last_event_time, last_source_sequence, last_event_id
            FROM customers_current
            WHERE customer_id = ANY(:customer_ids)
            FOR UPDATE;
            """
        )
        existing_cust_rows = s.execute(cust_lookup_stmt, {"customer_ids": customer_ids}).mappings().all()
        current_state_map: Dict[str, Dict[str, Any]] = {
            r["customer_id"]: dict(r) for r in existing_cust_rows
        }

        metrics = {
            "accepted_count": 0,
            "inserted_count": 0,
            "updated_count": 0,
            "deleted_count": 0,
            "duplicate_count": 0,
            "stale_event_count": 0,
            "rejected_count": 0,
        }

        dirty_customers: Dict[str, Dict[str, Any]] = {}
        history_batch: List[Dict[str, Any]] = []
        conflicts_to_quarantine: List[Dict[str, Any]] = []

        for ve in valid_events:
            ev = ve.event
            ev_id = UUID(str(ev.event_id))
            cust_id = ev.customer_id
            op_str = ev.operation.value if hasattr(ev.operation, "value") else str(ev.operation)

            # Check duplicate / conflict against seen_history
            if ev_id in seen_history:
                existing = seen_history[ev_id]
                matches_cust = existing["customer_id"] == cust_id
                matches_op = existing["operation"] == op_str
                matches_seq = existing["source_sequence"] == ev.source_sequence
                time_diff = abs((existing["event_time"] - ev.event_time).total_seconds())
                matches_time = time_diff < 1e-3

                if matches_cust and matches_op and matches_seq and matches_time:
                    metrics["duplicate_count"] += 1
                    continue
                else:
                    metrics["rejected_count"] += 1
                    conflicts_to_quarantine.append(
                        {
                            "raw_payload": ve.raw_payload,
                            "run_id": target_run_id,
                            "rejection_reason": "Conflicting event: Event ID already exists with different content",
                            "event_id": str(ev_id),
                            "source_file": ve.source_file,
                        }
                    )
                    continue

            # Record event in seen_history cache for intra-batch duplicate detection
            seen_history[ev_id] = {
                "event_id": ev_id,
                "customer_id": cust_id,
                "operation": op_str,
                "event_time": ev.event_time,
                "source_sequence": ev.source_sequence,
            }

            # Check out-of-order against current_state_map
            current_cust = current_state_map.get(cust_id)
            if current_cust is not None:
                incoming_order = (ev.event_time, ev.source_sequence, str(ev_id))
                current_order = (
                    current_cust["last_event_time"],
                    current_cust["last_source_sequence"],
                    str(current_cust["last_event_id"]),
                )

                if incoming_order < current_order:
                    # Stale: preserve in history, do not mutate state
                    history_batch.append(
                        {
                            "event_id": ev_id,
                            "customer_id": cust_id,
                            "operation": op_str,
                            "old_values": None,
                            "new_values": json.dumps(ev.payload),
                            "event_time": ev.event_time,
                            "source_sequence": ev.source_sequence,
                            "source_file": ve.source_file,
                            "run_id": target_run_id,
                        }
                    )
                    metrics["stale_event_count"] += 1
                    metrics["accepted_count"] += 1
                    continue

            # Apply state mutation
            old_values = (
                {
                    "customer_name": current_cust["customer_name"],
                    "email": current_cust["email"],
                    "city": current_cust["city"],
                    "is_deleted": current_cust["is_deleted"],
                }
                if current_cust
                else None
            )

            if ev.operation == OperationType.INSERT:
                name = ev.payload.get("customer_name") or (current_cust["customer_name"] if current_cust else "")
                email = ev.payload.get("email") or (current_cust["email"] if current_cust else "")
                city = ev.payload.get("city") if "city" in ev.payload else (current_cust["city"] if current_cust else None)
                is_del = False
                metrics["inserted_count"] += 1
                metrics["accepted_count"] += 1

            elif ev.operation == OperationType.UPDATE:
                name = (
                    ev.payload["customer_name"]
                    if "customer_name" in ev.payload and ev.payload["customer_name"] is not None
                    else (current_cust["customer_name"] if current_cust else "")
                )
                email = (
                    ev.payload["email"]
                    if "email" in ev.payload and ev.payload["email"] is not None
                    else (current_cust["email"] if current_cust else "")
                )
                city = (
                    ev.payload["city"]
                    if "city" in ev.payload
                    else (current_cust["city"] if current_cust else None)
                )
                is_del = current_cust["is_deleted"] if current_cust else False
                metrics["updated_count"] += 1
                metrics["accepted_count"] += 1

            elif ev.operation == OperationType.DELETE:
                name = current_cust["customer_name"] if current_cust else ""
                email = current_cust["email"] if current_cust else ""
                city = current_cust["city"] if current_cust else None
                is_del = True
                metrics["deleted_count"] += 1
                metrics["accepted_count"] += 1

            # Update in-memory current state
            new_state = {
                "customer_id": cust_id,
                "customer_name": name,
                "email": email,
                "city": city,
                "is_deleted": is_del,
                "last_event_time": ev.event_time,
                "last_source_sequence": ev.source_sequence,
                "last_event_id": ev_id,
            }
            current_state_map[cust_id] = new_state
            dirty_customers[cust_id] = new_state

            new_values = {
                "customer_name": name,
                "email": email,
                "city": city,
                "is_deleted": is_del,
            }

            history_batch.append(
                {
                    "event_id": ev_id,
                    "customer_id": cust_id,
                    "operation": op_str,
                    "old_values": json.dumps(old_values) if old_values is not None else None,
                    "new_values": json.dumps(new_values) if new_values is not None else None,
                    "event_time": ev.event_time,
                    "source_sequence": ev.source_sequence,
                    "source_file": ve.source_file,
                    "run_id": target_run_id,
                }
            )

        # 3. Batch DB Execution
        if conflicts_to_quarantine:
            self.insert_quarantine_batch(conflicts_to_quarantine, session=s)

        if dirty_customers:
            upsert_stmt = text(
                """
                INSERT INTO customers_current (
                    customer_id, customer_name, email, city, is_deleted,
                    last_event_time, last_source_sequence, last_event_id, updated_at
                ) VALUES (
                    :customer_id, :customer_name, :email, :city, :is_deleted,
                    :last_event_time, :last_source_sequence, :last_event_id, CURRENT_TIMESTAMP
                )
                ON CONFLICT (customer_id) DO UPDATE
                SET 
                    customer_name = EXCLUDED.customer_name,
                    email = EXCLUDED.email,
                    city = EXCLUDED.city,
                    is_deleted = EXCLUDED.is_deleted,
                    last_event_time = EXCLUDED.last_event_time,
                    last_source_sequence = EXCLUDED.last_source_sequence,
                    last_event_id = EXCLUDED.last_event_id,
                    updated_at = CURRENT_TIMESTAMP;
                """
            )
            s.execute(upsert_stmt, list(dirty_customers.values()))

        if history_batch:
            hist_insert_stmt = text(
                """
                INSERT INTO customer_event_history (
                    event_id, customer_id, operation, old_values, new_values,
                    event_time, source_sequence, processed_at, source_file, run_id
                ) VALUES (
                    :event_id, :customer_id, :operation, CAST(:old_values AS jsonb),
                    CAST(:new_values AS jsonb), :event_time, :source_sequence,
                    CURRENT_TIMESTAMP, :source_file, :run_id
                );
                """
            )
            s.execute(hist_insert_stmt, history_batch)

        return metrics

