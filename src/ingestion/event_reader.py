"""CDC Event Ingestion Layer.

Reads and ingests CDC event records from JSON and JSONL files.
Separates syntactically valid JSON from malformed records, preserves original
raw payloads, and tracks source file traceability and record sequence numbers.
"""

from dataclasses import asdict, dataclass, field
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


@dataclass
class ParsedRecord:
    """Represents a syntactically valid JSON record extracted from an input source."""

    source_file: str
    record_number: int
    raw_payload: str
    data: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """Convert parsed record to dictionary."""
        return asdict(self)


@dataclass
class MalformedRecord:
    """Represents an unparseable or malformed input record."""

    source_file: str
    record_number: int
    raw_payload: str
    error_reason: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert malformed record to dictionary."""
        return asdict(self)


@dataclass
class IngestionResult:
    """Encapsulates ingestion results for a specific input file or source."""

    source_file: str
    total_records: int = 0
    parsed_count: int = 0
    malformed_count: int = 0
    parsed_records: List[ParsedRecord] = field(default_factory=list)
    malformed_records: List[MalformedRecord] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def summary(self) -> Dict[str, Any]:
        """Return a structured summary dictionary."""
        return {
            "source_file": self.source_file,
            "total_records": self.total_records,
            "parsed_count": self.parsed_count,
            "malformed_count": self.malformed_count,
            "warnings": self.warnings,
        }


class EventReader:
    """Robust event reader for JSON and JSONL data sources."""

    def __init__(self, input_dir: Optional[Union[Path, str]] = None) -> None:
        """Initialize EventReader.

        Args:
            input_dir: Optional directory path. Defaults to INPUT_DATA_DIR from .env or 'data/input'.
        """
        env_dir = os.getenv("INPUT_DATA_DIR", "data/input")
        self.input_dir = Path(input_dir) if input_dir else Path(env_dir)

    def read_file(self, file_path: Union[Path, str]) -> IngestionResult:
        """Read and parse a JSON or JSONL file.

        Handles missing files, empty files, and syntax errors without raising unhandled exceptions.

        Args:
            file_path: Path to the target file.

        Returns:
            IngestionResult: Object containing parsed records, malformed records, and counts.
        """
        path = Path(file_path)
        result = IngestionResult(source_file=str(path))

        # Check for missing file
        if not path.exists():
            warning_msg = f"File not found: {path}"
            logger.warning(warning_msg)
            result.warnings.append(warning_msg)
            return result

        if not path.is_file():
            warning_msg = f"Path is not a regular file: {path}"
            logger.warning(warning_msg)
            result.warnings.append(warning_msg)
            return result

        # Check for empty file
        if path.stat().st_size == 0:
            warning_msg = f"File is empty: {path}"
            logger.info(warning_msg)
            result.warnings.append(warning_msg)
            return result

        # Determine parsing strategy based on file format
        if path.suffix.lower() in [".jsonl", ".ndjson", ".txt"]:
            self._read_jsonl(path, result)
        else:
            self._read_json(path, result)

        result.total_records = len(result.parsed_records) + len(result.malformed_records)
        result.parsed_count = len(result.parsed_records)
        result.malformed_count = len(result.malformed_records)

        logger.info(
            "Ingestion completed for %s: %d total, %d parsed, %d malformed",
            path.name,
            result.total_records,
            result.parsed_count,
            result.malformed_count,
        )
        return result

    def _read_jsonl(self, path: Path, result: IngestionResult) -> None:
        """Read JSONL line by line, preserving raw payload and isolating errors."""
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line_no, raw_line in enumerate(f, start=1):
                clean_line = raw_line.strip()
                if not clean_line:
                    # Ignore purely empty lines
                    continue

                try:
                    data = json.loads(clean_line)
                    if isinstance(data, dict):
                        # Preserve metadata keys (_record_type, _test_category) intact
                        result.parsed_records.append(
                            ParsedRecord(
                                source_file=str(path),
                                record_number=line_no,
                                raw_payload=clean_line,
                                data=data,
                            )
                        )
                    else:
                        result.malformed_records.append(
                            MalformedRecord(
                                source_file=str(path),
                                record_number=line_no,
                                raw_payload=clean_line,
                                error_reason="Root JSON value is not a JSON object",
                            )
                        )
                except json.JSONDecodeError as exc:
                    result.malformed_records.append(
                        MalformedRecord(
                            source_file=str(path),
                            record_number=line_no,
                            raw_payload=clean_line,
                            error_reason=f"JSONDecodeError: {exc.msg} at line {exc.lineno} col {exc.colno}",
                        )
                    )
                except Exception as exc:
                    result.malformed_records.append(
                        MalformedRecord(
                            source_file=str(path),
                            record_number=line_no,
                            raw_payload=clean_line,
                            error_reason=f"Unexpected parsing error: {str(exc)}",
                        )
                    )

    def _read_json(self, path: Path, result: IngestionResult) -> None:
        """Read standard JSON file containing an array of records or a single record."""
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()

            if not content.strip():
                result.warnings.append(f"File contains only whitespace: {path}")
                return

            try:
                data = json.loads(content)
            except json.JSONDecodeError as exc:
                # If entire JSON file is syntactically invalid, treat the file as a malformed record
                # so the pipeline does not crash and the raw payload can be quarantined.
                result.malformed_records.append(
                    MalformedRecord(
                        source_file=str(path),
                        record_number=1,
                        raw_payload=content,
                        error_reason=f"JSONDecodeError: {exc.msg} at line {exc.lineno} col {exc.colno}",
                    )
                )
                return

            if isinstance(data, list):
                for idx, item in enumerate(data, start=1):
                    raw_item_str = json.dumps(item, ensure_ascii=False)
                    if isinstance(item, dict):
                        # Preserve metadata keys (_record_type, _test_category) intact
                        result.parsed_records.append(
                            ParsedRecord(
                                source_file=str(path),
                                record_number=idx,
                                raw_payload=raw_item_str,
                                data=item,
                            )
                        )
                    else:
                        result.malformed_records.append(
                            MalformedRecord(
                                source_file=str(path),
                                record_number=idx,
                                raw_payload=raw_item_str,
                                error_reason="Element in JSON array is not a JSON object",
                            )
                        )
            elif isinstance(data, dict):
                result.parsed_records.append(
                    ParsedRecord(
                        source_file=str(path),
                        record_number=1,
                        raw_payload=content,
                        data=data,
                    )
                )
            else:
                result.malformed_records.append(
                    MalformedRecord(
                        source_file=str(path),
                        record_number=1,
                        raw_payload=content,
                        error_reason="Root JSON value is neither an array nor an object",
                    )
                )

        except Exception as exc:
            result.malformed_records.append(
                MalformedRecord(
                    source_file=str(path),
                    record_number=1,
                    raw_payload="",
                    error_reason=f"Failed to read file: {str(exc)}",
                )
            )

    def read_directory(
        self, dir_path: Optional[Union[Path, str]] = None
    ) -> List[IngestionResult]:
        """Read all JSON and JSONL files from a directory.

        Args:
            dir_path: Target directory. Defaults to self.input_dir.

        Returns:
            List[IngestionResult]: Ingestion results for all discovered files.
        """
        target_dir = Path(dir_path) if dir_path else self.input_dir
        results: List[IngestionResult] = []

        if not target_dir.exists():
            logger.warning("Input directory does not exist: %s", target_dir)
            return results

        files = sorted(
            [
                p
                for p in target_dir.iterdir()
                if p.is_file() and p.suffix.lower() in [".json", ".jsonl", ".ndjson"]
            ]
        )

        for file_path in files:
            results.append(self.read_file(file_path))

        return results


def print_ingestion_summary(results: List[IngestionResult]) -> None:
    """Print structured summary of ingestion results."""
    total_in = sum(r.total_records for r in results)
    total_parsed = sum(r.parsed_count for r in results)
    total_malformed = sum(r.malformed_count for r in results)

    print("\n=======================================================")
    print("CDC Event Ingestion - Execution Summary")
    print("=======================================================")
    print(f"Files Processed:         {len(results)}")
    print(f"Total Records Ingested:  {total_in}")
    print(f"  - Successfully Parsed: {total_parsed}")
    print(f"  - Malformed Records:   {total_malformed}")
    print("-------------------------------------------------------")
    for r in results:
        status_info = f"Parsed: {r.parsed_count}, Malformed: {r.malformed_count}"
        if r.warnings:
            status_info += f" (Warnings: {'; '.join(r.warnings)})"
        print(f"  * {Path(r.source_file).name:<26} -> {status_info}")
    print("=======================================================\n")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    reader = EventReader()
    batch_results = reader.read_directory()
    print_ingestion_summary(batch_results)
