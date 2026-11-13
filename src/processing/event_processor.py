"""Core Change Data Capture (CDC) Event Processing Engine.

Provides deterministic state management for CDC customer mutations:
- Handles INSERT, UPDATE, and DELETE operations.
- Enforces out-of-order resolution using (event_time, source_sequence, event_id) ordering tuples.
- Guarantees soft deletion without destroying customer history.
- Performs duplicate detection (idempotent skips) and identifies conflicting event ID reuses.
- Maintains append-only audit trail preserving all accepted unique events (including stale arrivals).
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

from src.validation.event_schema import CDCEvent, OperationType
from src.validation.event_validator import ValidatedEvent

logger = logging.getLogger(__name__)


class ProcessStatus(str, Enum):
    """Execution status for an individual event processing attempt."""

    APPLIED_INSERT = "APPLIED_INSERT"
    APPLIED_UPDATE = "APPLIED_UPDATE"
    APPLIED_DELETE = "APPLIED_DELETE"
    DUPLICATE_IGNORED = "DUPLICATE_IGNORED"
    CONFLICTING_EVENT = "CONFLICTING_EVENT"
    STALE_EVENT_PRESERVED = "STALE_EVENT_PRESERVED"


@dataclass
class CustomerState:
    """Materialized in-memory current state for a customer."""

    customer_id: str
    customer_name: str
    email: str
    city: Optional[str]
    is_deleted: bool
    last_event_time: datetime
    last_source_sequence: int
    last_event_id: str
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        """Convert state to serializable dictionary."""
        data = asdict(self)
        data["last_event_time"] = self.last_event_time.isoformat()
        data["updated_at"] = self.updated_at.isoformat()
        return data


@dataclass
class CustomerEventHistoryRecord:
    """Immutable audit trail record preserving every accepted unique CDC event."""

    event_id: str
    customer_id: str
    operation: str
    old_values: Optional[Dict[str, Any]]
    new_values: Optional[Dict[str, Any]]
    event_time: datetime
    source_sequence: int
    source_file: Optional[str] = None
    processed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        """Convert history entry to serializable dictionary."""
        data = asdict(self)
        data["event_time"] = self.event_time.isoformat()
        data["processed_at"] = self.processed_at.isoformat()
        return data


@dataclass
class EventProcessingResult:
    """Outcome report for a single processed CDC event."""

    event_id: str
    customer_id: str
    status: ProcessStatus
    message: str
    history_record: Optional[CustomerEventHistoryRecord] = None
    customer_state: Optional[CustomerState] = None


@dataclass
class BatchProcessingMetrics:
    """Aggregated processing metrics across a batch."""

    total_events: int = 0
    applied_inserts: int = 0
    applied_updates: int = 0
    applied_deletes: int = 0
    duplicate_events: int = 0
    stale_events: int = 0
    conflicting_events: int = 0
    history_events_preserved: int = 0

    def summary(self) -> Dict[str, int]:
        """Return metrics as a dictionary."""
        return asdict(self)


class EventProcessor:
    """Core CDC Event Processor independent of database persistence."""

    def __init__(self) -> None:
        """Initialize processor with clean state storage."""
        # customer_id -> CustomerState
        self.customers: Dict[str, CustomerState] = {}
        # event_id (str) -> canonical content dict
        self.seen_events: Dict[str, Dict[str, Any]] = {}
        # Append-only event history list
        self.history: List[CustomerEventHistoryRecord] = []
        # Cumulative metrics
        self.metrics = BatchProcessingMetrics()

    @staticmethod
    def _extract_canonical_content(event: CDCEvent) -> Dict[str, Any]:
        """Generate canonical content dictionary to detect exact duplicate vs conflicting event IDs."""
        op_str = event.operation.value if hasattr(event.operation, "value") else str(event.operation)
        return {
            "customer_id": event.customer_id,
            "operation": op_str,
            "event_time": event.event_time.isoformat(),
            "source_sequence": event.source_sequence,
            "payload": event.payload,
        }

    def process_event(
        self,
        event_input: Union[ValidatedEvent, CDCEvent, Dict[str, Any]],
        source_file: Optional[str] = None,
    ) -> EventProcessingResult:
        """Process a single CDC event following ordering, deduplication, and state rules.

        Args:
            event_input: ValidatedEvent instance, CDCEvent instance, or dictionary.
            source_file: Optional source filename for lineage.

        Returns:
            EventProcessingResult: Detailed outcome of the processing step.
        """
        # Unpack input to CDCEvent model
        src_file = source_file
        if isinstance(event_input, ValidatedEvent):
            event = event_input.event
            src_file = event_input.source_file
        elif isinstance(event_input, CDCEvent):
            event = event_input
        elif isinstance(event_input, dict):
            event = CDCEvent.model_validate(event_input)
        else:
            raise TypeError(f"Unsupported event input type: {type(event_input).__name__}")

        event_id_str = str(event.event_id)
        cust_id = event.customer_id
        canonical_content = self._extract_canonical_content(event)

        # --------------------------------------------------------------------
        # 1. Duplicate & Conflict Detection
        # --------------------------------------------------------------------
        if event_id_str in self.seen_events:
            existing_content = self.seen_events[event_id_str]
            if existing_content == canonical_content:
                # Exact duplicate: idempotently skip side-effects
                self.metrics.duplicate_events += 1
                return EventProcessingResult(
                    event_id=event_id_str,
                    customer_id=cust_id,
                    status=ProcessStatus.DUPLICATE_IGNORED,
                    message="Exact duplicate event detected and ignored without side-effects.",
                    customer_state=self.customers.get(cust_id),
                )
            else:
                # Conflicting event: reuses event ID with different content
                self.metrics.conflicting_events += 1
                logger.warning(
                    "Conflict detected: Event ID %s re-used with differing payload/attributes.",
                    event_id_str,
                )
                return EventProcessingResult(
                    event_id=event_id_str,
                    customer_id=cust_id,
                    status=ProcessStatus.CONFLICTING_EVENT,
                    message="Conflicting event detected: Event ID already exists with different content.",
                    customer_state=self.customers.get(cust_id),
                )

        # Register event ID as seen
        self.seen_events[event_id_str] = canonical_content

        # --------------------------------------------------------------------
        # 2. Out-of-Order & State Evaluation
        # --------------------------------------------------------------------
        current_cust = self.customers.get(cust_id)
        incoming_order_tuple = (event.event_time, event.source_sequence, event_id_str)

        if current_cust is not None:
            current_order_tuple = (
                current_cust.last_event_time,
                current_cust.last_source_sequence,
                current_cust.last_event_id,
            )

            # Check if incoming event is older than current materialized state
            if incoming_order_tuple < current_order_tuple:
                # Stale / Out-of-order event
                # Rule: Preserve in append-only history, but DO NOT overwrite current state
                history_record = CustomerEventHistoryRecord(
                    event_id=event_id_str,
                    customer_id=cust_id,
                    operation=canonical_content["operation"],
                    old_values=None,
                    new_values=event.payload,
                    event_time=event.event_time,
                    source_sequence=event.source_sequence,
                    source_file=src_file,
                )
                self.history.append(history_record)
                self.metrics.stale_events += 1
                self.metrics.history_events_preserved += 1

                logger.info(
                    "Stale event preserved in history without mutating current state for customer %s (event_id=%s)",
                    cust_id,
                    event_id_str,
                )
                return EventProcessingResult(
                    event_id=event_id_str,
                    customer_id=cust_id,
                    status=ProcessStatus.STALE_EVENT_PRESERVED,
                    message="Stale out-of-order event preserved in history; current state unchanged.",
                    history_record=history_record,
                    customer_state=current_cust,
                )

        # --------------------------------------------------------------------
        # 3. Apply Current-State Mutations (Newer or First-time Event)
        # --------------------------------------------------------------------
        op = event.operation
        old_values_snapshot: Optional[Dict[str, Any]] = None

        if current_cust is not None:
            old_values_snapshot = {
                "customer_name": current_cust.customer_name,
                "email": current_cust.email,
                "city": current_cust.city,
                "is_deleted": current_cust.is_deleted,
            }

        if op == OperationType.INSERT:
            # Create or update customer state
            name = event.payload.get("customer_name")
            email = event.payload.get("email")
            city = event.payload.get("city")

            # If re-inserting or updating existing customer
            if current_cust is None:
                new_state = CustomerState(
                    customer_id=cust_id,
                    customer_name=name or "",
                    email=email or "",
                    city=city,
                    is_deleted=False,
                    last_event_time=event.event_time,
                    last_source_sequence=event.source_sequence,
                    last_event_id=event_id_str,
                )
            else:
                new_state = current_cust
                new_state.customer_name = name or current_cust.customer_name
                new_state.email = email or current_cust.email
                if city is not None:
                    new_state.city = city
                new_state.is_deleted = False
                new_state.last_event_time = event.event_time
                new_state.last_source_sequence = event.source_sequence
                new_state.last_event_id = event_id_str
                new_state.updated_at = datetime.now(timezone.utc)

            self.customers[cust_id] = new_state
            self.metrics.applied_inserts += 1
            status = ProcessStatus.APPLIED_INSERT
            status_msg = "INSERT applied: customer state created or re-activated."

        elif op == OperationType.UPDATE:
            # Apply only supplied attributes, preserving existing attributes
            if current_cust is None:
                # Late insert or partial initial state
                new_state = CustomerState(
                    customer_id=cust_id,
                    customer_name=event.payload.get("customer_name", ""),
                    email=event.payload.get("email", ""),
                    city=event.payload.get("city"),
                    is_deleted=False,
                    last_event_time=event.event_time,
                    last_source_sequence=event.source_sequence,
                    last_event_id=event_id_str,
                )
            else:
                new_state = current_cust
                if "customer_name" in event.payload and event.payload["customer_name"] is not None:
                    new_state.customer_name = event.payload["customer_name"]
                if "email" in event.payload and event.payload["email"] is not None:
                    new_state.email = event.payload["email"]
                if "city" in event.payload:
                    new_state.city = event.payload["city"]

                new_state.last_event_time = event.event_time
                new_state.last_source_sequence = event.source_sequence
                new_state.last_event_id = event_id_str
                new_state.updated_at = datetime.now(timezone.utc)

            self.customers[cust_id] = new_state
            self.metrics.applied_updates += 1
            status = ProcessStatus.APPLIED_UPDATE
            status_msg = "UPDATE applied: supplied customer fields modified."

        elif op == OperationType.DELETE:
            # Soft delete: set is_deleted = True, preserve last known attributes
            if current_cust is None:
                new_state = CustomerState(
                    customer_id=cust_id,
                    customer_name="",
                    email="",
                    city=None,
                    is_deleted=True,
                    last_event_time=event.event_time,
                    last_source_sequence=event.source_sequence,
                    last_event_id=event_id_str,
                )
            else:
                new_state = current_cust
                new_state.is_deleted = True
                new_state.last_event_time = event.event_time
                new_state.last_source_sequence = event.source_sequence
                new_state.last_event_id = event_id_str
                new_state.updated_at = datetime.now(timezone.utc)

            self.customers[cust_id] = new_state
            self.metrics.applied_deletes += 1
            status = ProcessStatus.APPLIED_DELETE
            status_msg = "DELETE applied: customer soft-deleted, attributes preserved."

        else:
            raise ValueError(f"Unsupported operation type: {op}")

        # Capture updated new values for audit history
        new_values_snapshot = {
            "customer_name": self.customers[cust_id].customer_name,
            "email": self.customers[cust_id].email,
            "city": self.customers[cust_id].city,
            "is_deleted": self.customers[cust_id].is_deleted,
        }

        # Append to event history
        history_record = CustomerEventHistoryRecord(
            event_id=event_id_str,
            customer_id=cust_id,
            operation=canonical_content["operation"],
            old_values=old_values_snapshot,
            new_values=new_values_snapshot,
            event_time=event.event_time,
            source_sequence=event.source_sequence,
            source_file=src_file,
        )
        self.history.append(history_record)
        self.metrics.history_events_preserved += 1

        return EventProcessingResult(
            event_id=event_id_str,
            customer_id=cust_id,
            status=status,
            message=status_msg,
            history_record=history_record,
            customer_state=self.customers[cust_id],
        )

    def process_batch(
        self,
        events: List[Union[ValidatedEvent, CDCEvent, Dict[str, Any]]],
        source_file: Optional[str] = None,
    ) -> List[EventProcessingResult]:
        """Process a collection of CDC events sequentially.

        Args:
            events: List of event records.
            source_file: Optional source file name.

        Returns:
            List[EventProcessingResult]: Results for each processed event.
        """
        results: List[EventProcessingResult] = []
        for ev in events:
            self.metrics.total_events += 1
            results.append(self.process_event(ev, source_file=source_file))
        return results

    def get_customer(self, customer_id: str) -> Optional[CustomerState]:
        """Retrieve current materialized state for a customer."""
        return self.customers.get(customer_id)

    def reset_state(self) -> None:
        """Clear all in-memory state and metrics."""
        self.customers.clear()
        self.seen_events.clear()
        self.history.clear()
        self.metrics = BatchProcessingMetrics()

