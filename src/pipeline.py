"""Main CDC Pipeline Orchestrator.

Integrates ingestion, validation, CDC processing, and PostgreSQL persistence into
an atomic, fault-tolerant execution flow.

Workflow:
1. Initialize a pipeline run record with status 'RUNNING'.
2. Ingest JSON and JSONL files via EventReader.
3. Validate parsed records via EventValidator.
4. Quarantine malformed records and schema-violating events.
5. Apply valid CDC mutations via CDCRepository (deduplication, out-of-order resolution, soft deletion).
6. Persist append-only event history and update materialized customer state.
7. Record execution metrics, duration, and mark run COMPLETED or FAILED.
"""

from datetime import datetime, timezone
import logging
import os
from pathlib import Path
import time
from typing import Any, Dict, List, Optional, Union
from uuid import UUID, uuid4

from dotenv import load_dotenv
from sqlalchemy.engine import Engine

from src.database.connection import check_connection
from src.database.session import get_db_session
from src.ingestion.event_reader import EventReader, IngestionResult, MalformedRecord, ParsedRecord
from src.processing.event_processor import ProcessStatus
from src.publishing.repository import CDCRepository
from src.validation.event_validator import EventValidator, RejectedEvent, ValidatedEvent

load_dotenv()
logger = logging.getLogger(__name__)


class CDCPipeline:
    """End-to-End Change Data Capture (CDC) Pipeline Orchestrator."""

    def __init__(
        self,
        input_dir: Optional[Union[Path, str]] = None,
        engine: Optional[Engine] = None,
    ) -> None:
        """Initialize pipeline with components.

        Args:
            input_dir: Optional input directory override. Defaults to INPUT_DATA_DIR from .env or 'data/input'.
            engine: Optional custom SQLAlchemy engine override (e.g. for testing). Defaults to application engine.
        """
        env_dir = os.getenv("INPUT_DATA_DIR", "data/input")
        self.input_dir = Path(input_dir) if input_dir else Path(env_dir)
        self.engine = engine

        self.reader = EventReader(input_dir=self.input_dir)
        self.validator = EventValidator()

    def run(
        self,
        input_files: Optional[List[Union[Path, str]]] = None,
        run_id: Optional[Union[UUID, str]] = None,
    ) -> Dict[str, Any]:
        """Execute the complete end-to-end CDC pipeline.

        Args:
            input_files: Optional specific files to ingest. If None, scans self.input_dir.
            run_id: Optional predetermined run UUID.

        Returns:
            Dict[str, Any]: Comprehensive execution report including metrics, counts, and status.

        Raises:
            ConnectionError: If database connectivity check fails.
            Exception: Re-raises any fatal execution exception after recording FAILED run status.
        """
        # 0. Healthcheck DB connectivity
        if not check_connection(engine=self.engine):
            raise ConnectionError("Cannot execute CDC pipeline: database connectivity check failed.")

        target_run_id = UUID(str(run_id)) if run_id else uuid4()
        start_perf = time.perf_counter()
        started_at_dt = datetime.now(timezone.utc)

        # 1. Initialize Pipeline Run (in its own transaction to guarantee durability)
        with get_db_session(engine=self.engine) as session:
            repo = CDCRepository(session)
            repo.create_pipeline_run(run_id=target_run_id, session=session)

        logger.info(
            "Started CDC pipeline run %s at %s",
            target_run_id,
            started_at_dt.isoformat(),
        )

        try:
            # 2. Ingestion
            if input_files is not None:
                ingestion_results: List[IngestionResult] = [
                    self.reader.read_file(f) for f in input_files
                ]
            else:
                ingestion_results = self.reader.read_directory(self.input_dir)

            total_input_count = sum(r.total_records for r in ingestion_results)
            all_parsed: List[ParsedRecord] = []
            all_malformed: List[MalformedRecord] = []

            for r in ingestion_results:
                all_parsed.extend(r.parsed_records)
                all_malformed.extend(r.malformed_records)

            logger.info(
                "Ingestion completed: %d total input records (%d parsed, %d malformed)",
                total_input_count,
                len(all_parsed),
                len(all_malformed),
            )

            # 3. Schema Validation
            val_result = self.validator.validate_batch(all_parsed)
            logger.info(
                "Validation completed: %d valid events, %d rejected events",
                val_result.valid_count,
                val_result.rejected_count,
            )

            # 4. Atomic Database Mutations & Quarantine Persistence
            metrics = {
                "input_count": total_input_count,
                "accepted_count": 0,
                "inserted_count": 0,
                "updated_count": 0,
                "deleted_count": 0,
                "duplicate_count": 0,
                "stale_event_count": 0,
                "rejected_count": len(all_malformed) + val_result.rejected_count,
            }

            with get_db_session(engine=self.engine) as session:
                repo = CDCRepository(session)

                # 4a. Batch quarantine syntactically malformed and schema-rejected records
                quarantine_records: List[Dict[str, Any]] = []
                for mal in all_malformed:
                    quarantine_records.append(
                        {
                            "raw_payload": mal.raw_payload,
                            "run_id": target_run_id,
                            "rejection_reason": mal.error_reason,
                            "event_id": None,
                            "source_file": mal.source_file,
                        }
                    )
                for rej in val_result.rejected_events:
                    quarantine_records.append(
                        {
                            "raw_payload": rej.raw_payload,
                            "run_id": target_run_id,
                            "rejection_reason": rej.rejection_reason,
                            "event_id": rej.event_id,
                            "source_file": rej.source_file,
                        }
                    )

                if quarantine_records:
                    repo.insert_quarantine_batch(quarantine_records, session=session)

                # 4b. Apply valid CDC mutations in high-throughput batch mode
                batch_metrics = repo.apply_batch_mutations(
                    valid_events=val_result.valid_events,
                    run_id=target_run_id,
                    session=session,
                )

                metrics["accepted_count"] += batch_metrics["accepted_count"]
                metrics["inserted_count"] += batch_metrics["inserted_count"]
                metrics["updated_count"] += batch_metrics["updated_count"]
                metrics["deleted_count"] += batch_metrics["deleted_count"]
                metrics["duplicate_count"] += batch_metrics["duplicate_count"]
                metrics["stale_event_count"] += batch_metrics["stale_event_count"]
                metrics["rejected_count"] += batch_metrics["rejected_count"]

                # Calculate accurate wall-clock elapsed duration using monotonic clock
                elapsed_seconds = time.perf_counter() - start_perf

                # 4d. Finalize run status as COMPLETED
                repo.complete_pipeline_run(
                    run_id=target_run_id,
                    status="COMPLETED",
                    metrics=metrics,
                    duration_seconds=elapsed_seconds,
                    session=session,
                )

            # 5. Fetch completed run metadata for final summary
            with get_db_session(engine=self.engine) as session:
                repo = CDCRepository(session)
                run_record = repo.get_pipeline_run(target_run_id, session=session)

            summary = {
                "run_id": str(target_run_id),
                "status": "COMPLETED",
                "started_at": run_record["started_at"].isoformat() if run_record else started_at_dt.isoformat(),
                "completed_at": run_record["completed_at"].isoformat() if run_record and run_record["completed_at"] else datetime.now(timezone.utc).isoformat(),
                "duration_seconds": float(run_record["duration_seconds"]) if run_record and run_record["duration_seconds"] is not None else round(elapsed_seconds, 4),
                "metrics": metrics,
            }

            self._log_summary(summary)
            return summary

        except Exception as exc:
            elapsed_seconds = time.perf_counter() - start_perf
            logger.error("CDC Pipeline execution failed for run %s: %s", target_run_id, exc)
            # Record failure in pipeline_runs in an isolated transaction
            try:
                with get_db_session(engine=self.engine) as session:
                    repo = CDCRepository(session)
                    repo.complete_pipeline_run(
                        run_id=target_run_id,
                        status="FAILED",
                        duration_seconds=elapsed_seconds,
                        error_message=str(exc),
                        session=session,
                    )
            except Exception as inner_exc:
                logger.error("Could not record failure status in pipeline_runs: %s", inner_exc)
            raise

    def _log_summary(self, summary: Dict[str, Any]) -> None:
        """Print and log a human-readable execution summary."""
        m = summary["metrics"]
        logger.info(
            "CDC Pipeline Run %s COMPLETED in %.4fs (Input: %d, Accepted: %d, Duplicates: %d, Quarantined: %d)",
            summary["run_id"],
            summary["duration_seconds"],
            m["input_count"],
            m["accepted_count"],
            m["duplicate_count"],
            m["rejected_count"],
        )
        print("\n=======================================================")
        print("CDC Pipeline Execution Summary")
        print("=======================================================")
        print(f"Run ID:                  {summary['run_id']}")
        print(f"Status:                  {summary['status']}")
        print(f"Started At:              {summary.get('started_at')}")
        print(f"Completed At:            {summary.get('completed_at')}")
        print(f"Duration:                {summary['duration_seconds']:.4f}s")
        print(f"Total Input Records:     {m['input_count']}")
        print(f"  - Accepted Events:     {m['accepted_count']}")
        print(f"      * Inserts:         {m['inserted_count']}")
        print(f"      * Updates:         {m['updated_count']}")
        print(f"      * Deletes:         {m['deleted_count']}")
        print(f"      * Stale (History): {m['stale_event_count']}")
        print(f"  - Duplicate Records:   {m['duplicate_count']}")
        print(f"  - Quarantined Records: {m['rejected_count']}")
        print("=======================================================\n")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    pipeline = CDCPipeline()
    pipeline.run()

