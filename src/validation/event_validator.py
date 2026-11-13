"""Event Validator for CDC Pipeline.

Validates parsed event records using Pydantic CDCEvent models.
Separates schema-compliant events from rejected events, preserves raw payloads
and ingestion provenance (source file and record sequence), and formats diagnostic
validation error reasons for quarantine routing.
"""

from dataclasses import asdict, dataclass, field
import json
import logging
from typing import Any, Dict, List, Optional, Union

from pydantic import ValidationError

from src.ingestion.event_reader import ParsedRecord
from src.validation.event_schema import CDCEvent

logger = logging.getLogger(__name__)


@dataclass
class ValidatedEvent:
    """Represents an event that has successfully passed Pydantic schema validation."""

    source_file: str
    record_number: int
    raw_payload: str
    event: CDCEvent

    def to_dict(self) -> Dict[str, Any]:
        """Convert validated event to serializable dictionary."""
        return {
            "source_file": self.source_file,
            "record_number": self.record_number,
            "raw_payload": self.raw_payload,
            "event": self.event.model_dump(mode="json"),
        }


@dataclass
class RejectedEvent:
    """Represents an event that failed Pydantic schema validation."""

    source_file: str
    record_number: int
    raw_payload: str
    rejection_reason: str
    error_details: List[Dict[str, Any]] = field(default_factory=list)
    event_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert rejected event to serializable dictionary."""
        return asdict(self)


@dataclass
class ValidationBatchResult:
    """Encapsulates validation outcome across a collection of records."""

    total_records: int = 0
    valid_count: int = 0
    rejected_count: int = 0
    valid_events: List[ValidatedEvent] = field(default_factory=list)
    rejected_events: List[RejectedEvent] = field(default_factory=list)

    def summary(self) -> Dict[str, Any]:
        """Return a structured summary dictionary."""
        return {
            "total_records": self.total_records,
            "valid_count": self.valid_count,
            "rejected_count": self.rejected_count,
            "valid_percentage": (
                round((self.valid_count / self.total_records * 100), 2)
                if self.total_records > 0
                else 0.0
            ),
        }


class EventValidator:
    """Validates raw/parsed CDC event records against CDCEvent schema."""

    def __init__(self) -> None:
        """Initialize EventValidator."""
        pass

    def validate_record(
        self,
        record: Union[ParsedRecord, Dict[str, Any]],
        source_file: str = "unknown",
        record_number: int = 0,
        raw_payload: Optional[str] = None,
    ) -> Union[ValidatedEvent, RejectedEvent]:
        """Validate an individual parsed event record.

        Args:
            record: ParsedRecord instance or raw dictionary.
            source_file: Fallback source file name if record is a dict.
            record_number: Fallback record index if record is a dict.
            raw_payload: Fallback raw payload text if record is a dict.

        Returns:
            ValidatedEvent if valid, RejectedEvent if schema validation failed.
        """
        if isinstance(record, ParsedRecord):
            src_file = record.source_file
            rec_num = record.record_number
            raw_data_str = record.raw_payload
            data_dict = record.data
        elif isinstance(record, dict):
            src_file = source_file
            rec_num = record_number
            raw_data_str = raw_payload or json.dumps(record, ensure_ascii=False)
            data_dict = record
        else:
            return RejectedEvent(
                source_file=source_file,
                record_number=record_number,
                raw_payload=str(record),
                rejection_reason=f"Unsupported record type: {type(record).__name__}. Expected ParsedRecord or dict.",
            )

        # Attempt to extract event_id if present for traceability
        extracted_event_id: Optional[str] = None
        raw_event_id = data_dict.get("event_id")
        if raw_event_id is not None:
            extracted_event_id = str(raw_event_id)

        try:
            validated_model = CDCEvent.model_validate(data_dict)
            return ValidatedEvent(
                source_file=src_file,
                record_number=rec_num,
                raw_payload=raw_data_str,
                event=validated_model,
            )
        except ValidationError as exc:
            error_reasons = []
            error_details = []
            for err in exc.errors():
                loc = " -> ".join(str(elem) for elem in err.get("loc", []))
                msg = err.get("msg", "Invalid value")
                field_str = f"Field '{loc}': " if loc else ""
                error_reasons.append(f"{field_str}{msg}")
                error_details.append(
                    {
                        "location": loc,
                        "message": msg,
                        "type": err.get("type", "unknown"),
                    }
                )

            formatted_reason = "; ".join(error_reasons)
            logger.debug(
                "Validation rejected record #%d in %s: %s",
                rec_num,
                src_file,
                formatted_reason,
            )

            return RejectedEvent(
                source_file=src_file,
                record_number=rec_num,
                raw_payload=raw_data_str,
                rejection_reason=formatted_reason,
                error_details=error_details,
                event_id=extracted_event_id,
            )
        except Exception as exc:
            logger.error("Unexpected error validating record #%d: %s", rec_num, exc)
            return RejectedEvent(
                source_file=src_file,
                record_number=rec_num,
                raw_payload=raw_data_str,
                rejection_reason=f"Unexpected validation exception: {str(exc)}",
                event_id=extracted_event_id,
            )

    def validate_batch(
        self, records: List[Union[ParsedRecord, Dict[str, Any]]]
    ) -> ValidationBatchResult:
        """Validate a collection of event records independently.

        Continues processing after individual validation failures.

        Args:
            records: List of ParsedRecord instances or dictionaries.

        Returns:
            ValidationBatchResult: Containing partitioned valid and rejected events.
        """
        result = ValidationBatchResult(total_records=len(records))

        for idx, rec in enumerate(records, start=1):
            outcome = self.validate_record(rec, record_number=idx)
            if isinstance(outcome, ValidatedEvent):
                result.valid_events.append(outcome)
            else:
                result.rejected_events.append(outcome)

        result.valid_count = len(result.valid_events)
        result.rejected_count = len(result.rejected_events)

        logger.info(
            "Batch validation completed: %d total, %d valid, %d rejected",
            result.total_records,
            result.valid_count,
            result.rejected_count,
        )
        return result


def print_validation_summary(result: ValidationBatchResult) -> None:
    """Print a clean execution summary of validation results."""
    print("\n=======================================================")
    print("CDC Event Validation - Execution Summary")
    print("=======================================================")
    print(f"Total Records Evaluated: {result.total_records}")
    print(f"  - Valid Records:       {result.valid_count}")
    print(f"  - Rejected Records:    {result.rejected_count}")
    print(f"Validation Pass Rate:    {result.summary()['valid_percentage']}%")
    if result.rejected_events:
        print("\nSample Validation Rejections:")
        for idx, rej in enumerate(result.rejected_events[:5], start=1):
            print(f"  [{idx}] Record #{rej.record_number} ({rej.source_file}):")
            print(f"      Reason:   {rej.rejection_reason}")
            print(f"      Event ID: {rej.event_id}")
    print("=======================================================\n")


if __name__ == "__main__":
    from src.ingestion.event_reader import EventReader

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    reader = EventReader()
    batch_results = reader.read_directory("data/input")

    all_parsed_records = []
    for br in batch_results:
        all_parsed_records.extend(br.parsed_records)

    validator = EventValidator()
    val_result = validator.validate_batch(all_parsed_records)
    print_validation_summary(val_result)

