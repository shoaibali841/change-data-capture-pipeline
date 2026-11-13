"""CDC Data Reconciliation and Integrity Audit Suite.

Executes comprehensive reconciliation queries against PostgreSQL to verify:
1. Current customer state consistency and attribute retention.
2. Alignment of customer materialized state with latest accepted events.
3. Event history immutability, uniqueness, and referential integrity.
4. Soft deletion correctness (is_deleted matches latest DELETE event).
5. Event conservation per pipeline run (input = accepted + duplicate + rejected).
6. Detection of orphaned, corrupted, or conflicting state.
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime
import json
import logging
from typing import Any, Dict, List, Optional
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from src.database.session import get_db_session

logger = logging.getLogger(__name__)


@dataclass
class CheckResult:
    """Result of an individual integrity check."""

    name: str
    description: str
    passed: bool
    violation_count: int
    violations: List[Dict[str, Any]] = field(default_factory=list)
    explanation: Optional[str] = None


@dataclass
class ReconciliationSummary:
    """Consolidated reconciliation audit summary."""

    timestamp: str
    total_customers: int
    active_customers: int
    soft_deleted_customers: int
    total_history_events: int
    total_quarantined_events: int
    runs_by_status: Dict[str, int]
    history_by_operation: Dict[str, int]
    quarantine_by_reason: Dict[str, int]
    stale_events_count: int
    checks: Dict[str, CheckResult]
    all_passed: bool

    def to_dict(self) -> Dict[str, Any]:
        """Convert summary to dictionary format."""
        data = asdict(self)
        return data


class CDCReconciler:
    """Data reconciliation and audit engine for CDC pipeline state in PostgreSQL."""

    def __init__(self, session: Optional[Session] = None, engine: Optional[Engine] = None) -> None:
        """Initialize reconciler.
        
        Args:
            session: Optional pre-existing SQLAlchemy session.
            engine: Optional custom SQLAlchemy engine override.
        """
        self.session = session
        self.engine = engine

    def _execute_query(self, query_str: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a query and return rows as dictionaries."""
        if self.session is not None:
            result = self.session.execute(text(query_str), params or {})
            return [dict(r) for r in result.mappings().all()]
        with get_db_session(engine=self.engine) as s:
            result = s.execute(text(query_str), params or {})
            return [dict(r) for r in result.mappings().all()]

    # =========================================================================
    # Aggregation & Status Queries
    # =========================================================================

    def get_pipeline_runs_breakdown(self) -> Dict[str, int]:
        """Count pipeline runs grouped by status."""
        query = "SELECT status, COUNT(*) AS cnt FROM pipeline_runs GROUP BY status;"
        rows = self._execute_query(query)
        return {r["status"]: int(r["cnt"]) for r in rows}

    def get_customer_counts(self) -> Dict[str, int]:
        """Get counts for total, active, and soft-deleted customers."""
        query = """
        SELECT 
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE NOT is_deleted) AS active,
            COUNT(*) FILTER (WHERE is_deleted) AS deleted
        FROM customers_current;
        """
        rows = self._execute_query(query)
        if not rows:
            return {"total": 0, "active": 0, "deleted": 0}
        r = rows[0]
        return {
            "total": int(r["total"] or 0),
            "active": int(r["active"] or 0),
            "deleted": int(r["deleted"] or 0),
        }

    def get_history_by_operation(self) -> Dict[str, int]:
        """Count event history records grouped by operation."""
        query = "SELECT operation, COUNT(*) AS cnt FROM customer_event_history GROUP BY operation;"
        rows = self._execute_query(query)
        return {r["operation"]: int(r["cnt"]) for r in rows}

    def get_quarantine_by_reason(self) -> Dict[str, int]:
        """Count quarantined records grouped by rejection reason."""
        query = "SELECT rejection_reason, COUNT(*) AS cnt FROM event_quarantine GROUP BY rejection_reason;"
        rows = self._execute_query(query)
        return {r["rejection_reason"]: int(r["cnt"]) for r in rows}

    def get_stale_events_count(self) -> int:
        """Count out-of-order events preserved in history without mutating state."""
        query = """
        SELECT COUNT(*) AS cnt
        FROM customer_event_history ceh
        JOIN customers_current c ON ceh.customer_id = c.customer_id
        WHERE (ceh.event_time, ceh.source_sequence, ceh.event_id) < (c.last_event_time, c.last_source_sequence, c.last_event_id);
        """
        rows = self._execute_query(query)
        return int(rows[0]["cnt"]) if rows else 0

    # =========================================================================
    # Integrity Audit Checks
    # =========================================================================

    def check_duplicate_event_ids(self) -> CheckResult:
        """Verify that event_id is strictly unique in customer_event_history."""
        query = """
        SELECT event_id, COUNT(*) AS cnt
        FROM customer_event_history
        GROUP BY event_id
        HAVING COUNT(*) > 1;
        """
        violations = self._execute_query(query)
        passed = len(violations) == 0
        explanation = None if passed else f"Found {len(violations)} event IDs with duplicate occurrences in history."
        return CheckResult(
            name="duplicate_event_ids",
            description="Event IDs in customer_event_history must be strictly unique",
            passed=passed,
            violation_count=len(violations),
            violations=violations,
            explanation=explanation,
        )

    def check_orphaned_history_records(self) -> CheckResult:
        """Verify that all history records reference an existing pipeline run."""
        query = """
        SELECT ceh.event_id, ceh.customer_id, ceh.run_id
        FROM customer_event_history ceh
        LEFT JOIN pipeline_runs pr ON ceh.run_id = pr.run_id
        WHERE pr.run_id IS NULL;
        """
        violations = self._execute_query(query)
        passed = len(violations) == 0
        explanation = None if passed else f"Found {len(violations)} history events referencing nonexistent pipeline runs."
        return CheckResult(
            name="orphaned_history_records",
            description="All customer_event_history rows must reference a valid pipeline_run",
            passed=passed,
            violation_count=len(violations),
            violations=violations,
            explanation=explanation,
        )

    def check_orphaned_quarantine_records(self) -> CheckResult:
        """Verify that all quarantine records reference an existing pipeline run."""
        query = """
        SELECT eq.quarantine_id, eq.run_id, eq.rejection_reason
        FROM event_quarantine eq
        LEFT JOIN pipeline_runs pr ON eq.run_id = pr.run_id
        WHERE pr.run_id IS NULL;
        """
        violations = self._execute_query(query)
        passed = len(violations) == 0
        explanation = None if passed else f"Found {len(violations)} quarantine records referencing nonexistent pipeline runs."
        return CheckResult(
            name="orphaned_quarantine_records",
            description="All event_quarantine rows must reference a valid pipeline_run",
            passed=passed,
            violation_count=len(violations),
            violations=violations,
            explanation=explanation,
        )

    def check_deletion_metadata_consistency(self) -> CheckResult:
        """Verify that is_deleted strictly matches whether the latest event was DELETE.
        
        Ordering rule: (event_time, source_sequence, event_id).
        """
        query = """
        WITH ranked_events AS (
            SELECT 
                customer_id,
                operation,
                event_time,
                source_sequence,
                event_id,
                ROW_NUMBER() OVER (
                    PARTITION BY customer_id
                    ORDER BY event_time DESC, source_sequence DESC, event_id DESC
                ) AS rn
            FROM customer_event_history
        )
        SELECT 
            c.customer_id,
            c.is_deleted AS current_is_deleted,
            r.operation AS latest_event_operation,
            c.last_event_time,
            r.event_time AS latest_event_time,
            c.last_source_sequence,
            r.source_sequence AS latest_source_sequence
        FROM customers_current c
        JOIN ranked_events r ON c.customer_id = r.customer_id AND r.rn = 1
        WHERE (c.is_deleted = TRUE AND r.operation != 'DELETE')
           OR (c.is_deleted = FALSE AND r.operation = 'DELETE');
        """
        violations = self._execute_query(query)
        passed = len(violations) == 0
        explanation = (
            None
            if passed
            else f"Found {len(violations)} customers where is_deleted does not match their latest event operation."
        )
        return CheckResult(
            name="deletion_metadata_consistency",
            description="customers_current.is_deleted must be TRUE iff the latest accepted event was DELETE",
            passed=passed,
            violation_count=len(violations),
            violations=violations,
            explanation=explanation,
        )

    def check_current_state_alignment(self) -> CheckResult:
        """Verify that customers_current matches the MAX(event_time, source_sequence, event_id) in history."""
        query = """
        WITH latest_history AS (
            SELECT 
                customer_id,
                event_id,
                operation,
                event_time,
                source_sequence,
                ROW_NUMBER() OVER (
                    PARTITION BY customer_id 
                    ORDER BY event_time DESC, source_sequence DESC, event_id DESC
                ) AS rn
            FROM customer_event_history
        )
        SELECT 
            c.customer_id,
            c.last_event_id AS current_last_event_id,
            lh.event_id AS history_latest_event_id,
            c.last_event_time AS current_last_event_time,
            lh.event_time AS history_latest_event_time,
            c.last_source_sequence AS current_last_sequence,
            lh.source_sequence AS history_latest_sequence
        FROM customers_current c
        JOIN latest_history lh ON c.customer_id = lh.customer_id AND lh.rn = 1
        WHERE c.last_event_id != lh.event_id
           OR c.last_event_time != lh.event_time
           OR c.last_source_sequence != lh.source_sequence;
        """
        violations = self._execute_query(query)
        passed = len(violations) == 0
        explanation = (
            None
            if passed
            else f"Found {len(violations)} customers where current-state metadata does not match latest accepted history event."
        )
        return CheckResult(
            name="current_state_alignment",
            description="customers_current metadata must match the latest accepted event in customer_event_history",
            passed=passed,
            violation_count=len(violations),
            violations=violations,
            explanation=explanation,
        )

    def check_active_customer_attributes(self) -> CheckResult:
        """Verify that every active customer has valid required attributes (name, email)."""
        query = """
        SELECT customer_id, customer_name, email, city, is_deleted
        FROM customers_current
        WHERE is_deleted = FALSE
          AND (customer_name IS NULL OR TRIM(customer_name) = '' 
               OR email IS NULL OR TRIM(email) = '');
        """
        violations = self._execute_query(query)
        passed = len(violations) == 0
        explanation = None if passed else f"Found {len(violations)} active customers missing required name or email."
        return CheckResult(
            name="active_customer_attributes",
            description="Active customers must have valid required customer_name and email",
            passed=passed,
            violation_count=len(violations),
            violations=violations,
            explanation=explanation,
        )

    def check_soft_delete_attribute_retention(self) -> CheckResult:
        """Verify that soft-deleted customers retain their prior attributes (not blanked out)."""
        query = """
        SELECT customer_id, customer_name, email, city, is_deleted
        FROM customers_current
        WHERE is_deleted = TRUE
          AND (customer_name IS NULL OR TRIM(customer_name) = '' 
               OR email IS NULL OR TRIM(email) = '');
        """
        violations = self._execute_query(query)
        passed = len(violations) == 0
        explanation = (
            None
            if passed
            else f"Found {len(violations)} soft-deleted customers with blanked-out previous attributes."
        )
        return CheckResult(
            name="soft_delete_attribute_retention",
            description="Soft-deleted customers must retain previous non-empty customer_name and email",
            passed=passed,
            violation_count=len(violations),
            violations=violations,
            explanation=explanation,
        )

    def check_event_conservation_per_run(self) -> CheckResult:
        """Verify that for every COMPLETED run: input_count = accepted + duplicate + rejected."""
        query = """
        SELECT 
            run_id,
            status,
            input_count,
            accepted_count,
            duplicate_count,
            rejected_count,
            (accepted_count + duplicate_count + rejected_count) AS total_accounted_for,
            (input_count - (accepted_count + duplicate_count + rejected_count)) AS discrepancy
        FROM pipeline_runs
        WHERE status = 'COMPLETED'
          AND input_count != (accepted_count + duplicate_count + rejected_count);
        """
        violations = self._execute_query(query)
        passed = len(violations) == 0
        explanation = (
            None
            if passed
            else f"Found {len(violations)} pipeline runs where input_count != accepted + duplicate + rejected."
        )
        return CheckResult(
            name="event_conservation_per_run",
            description="For completed runs, input_count must equal accepted_count + duplicate_count + rejected_count",
            passed=passed,
            violation_count=len(violations),
            violations=violations,
            explanation=explanation,
        )

    # =========================================================================
    # Full Reconciliation Audit Execution
    # =========================================================================

    def run_reconciliation(self) -> ReconciliationSummary:
        """Execute all reconciliation queries and integrity audits."""
        cust_counts = self.get_customer_counts()
        runs_status = self.get_pipeline_runs_breakdown()
        hist_ops = self.get_history_by_operation()
        quar_reasons = self.get_quarantine_by_reason()
        stale_count = self.get_stale_events_count()

        total_hist = sum(hist_ops.values())
        total_quar = sum(quar_reasons.values())

        checks = {
            "duplicate_event_ids": self.check_duplicate_event_ids(),
            "orphaned_history_records": self.check_orphaned_history_records(),
            "orphaned_quarantine_records": self.check_orphaned_quarantine_records(),
            "deletion_metadata_consistency": self.check_deletion_metadata_consistency(),
            "current_state_alignment": self.check_current_state_alignment(),
            "active_customer_attributes": self.check_active_customer_attributes(),
            "soft_delete_attribute_retention": self.check_soft_delete_attribute_retention(),
            "event_conservation_per_run": self.check_event_conservation_per_run(),
        }

        all_passed = all(c.passed for c in checks.values())

        summary = ReconciliationSummary(
            timestamp=datetime.utcnow().isoformat() + "Z",
            total_customers=cust_counts["total"],
            active_customers=cust_counts["active"],
            soft_deleted_customers=cust_counts["deleted"],
            total_history_events=total_hist,
            total_quarantined_events=total_quar,
            runs_by_status=runs_status,
            history_by_operation=hist_ops,
            quarantine_by_reason=quar_reasons,
            stale_events_count=stale_count,
            checks=checks,
            all_passed=all_passed,
        )

        return summary

    def print_summary(self, summary: ReconciliationSummary) -> None:
        """Print a structured, human-readable terminal report of reconciliation results."""
        print("\n=======================================================")
        print("CDC Pipeline Data Reconciliation & Integrity Audit")
        print("=======================================================")
        print(f"Audit Timestamp:          {summary.timestamp}")
        print(f"Overall Status:           {'PASSED' if summary.all_passed else 'FAILED'}")
        print("-------------------------------------------------------")
        print(f"Customers Summary:")
        print(f"  * Total Customers:      {summary.total_customers}")
        print(f"  * Active Customers:     {summary.active_customers}")
        print(f"  * Soft-Deleted:         {summary.soft_deleted_customers}")
        print(f"Events Summary:")
        print(f"  * Event History Total:  {summary.total_history_events}")
        for op, count in summary.history_by_operation.items():
            print(f"      - {op:<7}:           {count}")
        print(f"  * Stale Events:         {summary.stale_events_count}")
        print(f"  * Quarantined Events:   {summary.total_quarantined_events}")
        print(f"Pipeline Runs by Status:  {summary.runs_by_status}")
        print("-------------------------------------------------------")
        print("Integrity Audit Checks:")
        for name, check in summary.checks.items():
            status_tag = "[PASS]" if check.passed else "[FAIL]"
            print(f"  {status_tag} {check.name:<32} (violations: {check.violation_count})")
            if not check.passed and check.explanation:
                print(f"         Reason: {check.explanation}")
        print("=======================================================\n")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    reconciler = CDCReconciler()
    summary = reconciler.run_reconciliation()
    reconciler.print_summary(summary)

