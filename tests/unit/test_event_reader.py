"""Unit tests for the CDC Event Ingestion Layer (src/ingestion/event_reader.py).

Verifies handling of:
- Normal JSON array files
- Malformed JSONL lines
- Missing files
- Empty files
- Fully malformed JSON files
- Mixed valid and malformed JSONL streams
"""

from pathlib import Path
import pytest
from src.ingestion.event_reader import EventReader, IngestionResult


@pytest.fixture
def reader() -> EventReader:
    """Fixture providing an EventReader instance."""
    return EventReader()


def test_ingest_sample_events_json(reader: EventReader) -> None:
    """Verify reading the generated sample_events.json file."""
    sample_file = Path("data/input/sample_events.json")
    if not sample_file.exists():
        pytest.skip("sample_events.json does not exist.")

    result = reader.read_file(sample_file)
    assert result.total_records == 30
    assert result.parsed_count == 30
    assert result.malformed_count == 0
    assert len(result.warnings) == 0

    # Verify first record fields
    first = result.parsed_records[0]
    assert first.record_number == 1
    assert first.source_file == str(sample_file)
    assert first.data["operation"] == "INSERT"
    assert first.data["customer_id"] == "CUST-0001"
    # Generator metadata preserved
    assert first.data["_record_type"] == "valid"
    assert first.data["_test_category"] == "normal_insert"


def test_ingest_malformed_events_jsonl(reader: EventReader) -> None:
    """Verify reading malformed_events.jsonl where lines are unparseable."""
    malformed_file = Path("data/input/malformed_events.jsonl")
    if not malformed_file.exists():
        pytest.skip("malformed_events.jsonl does not exist.")

    result = reader.read_file(malformed_file)
    assert result.total_records == 2
    assert result.parsed_count == 0
    assert result.malformed_count == 2

    # Check that error reasons are recorded and raw payloads preserved
    for idx, rec in enumerate(result.malformed_records, start=1):
        assert rec.record_number == idx
        assert len(rec.raw_payload) > 0
        assert "JSONDecodeError" in rec.error_reason or "error" in rec.error_reason.lower()


def test_ingest_missing_file(reader: EventReader) -> None:
    """Verify missing file handling does not raise an exception."""
    missing_file = Path("data/input/non_existent_file_xyz.json")
    result = reader.read_file(missing_file)

    assert result.total_records == 0
    assert result.parsed_count == 0
    assert result.malformed_count == 0
    assert len(result.warnings) == 1
    assert "File not found" in result.warnings[0]


def test_ingest_empty_file(reader: EventReader, tmp_path: Path) -> None:
    """Verify empty file handling returns clean warning without errors."""
    empty_file = tmp_path / "empty_stream.json"
    empty_file.write_text("", encoding="utf-8")

    result = reader.read_file(empty_file)
    assert result.total_records == 0
    assert result.parsed_count == 0
    assert result.malformed_count == 0
    assert len(result.warnings) == 1
    assert "File is empty" in result.warnings[0]


def test_ingest_malformed_json_file(reader: EventReader, tmp_path: Path) -> None:
    """Verify a malformed JSON file does not crash the parser and captures raw content."""
    broken_json = tmp_path / "broken.json"
    corrupted_content = '{"event_id": "abc", "payload": [1, 2, '
    broken_json.write_text(corrupted_content, encoding="utf-8")

    result = reader.read_file(broken_json)
    assert result.total_records == 1
    assert result.parsed_count == 0
    assert result.malformed_count == 1
    assert result.malformed_records[0].raw_payload == corrupted_content
    assert "JSONDecodeError" in result.malformed_records[0].error_reason


def test_ingest_mixed_jsonl_lines(reader: EventReader, tmp_path: Path) -> None:
    """Verify mixed valid and malformed JSONL lines in a single file."""
    mixed_file = tmp_path / "stream_with_errors.jsonl"
    lines = [
        '{"event_id": "uuid-1", "customer_id": "CUST-01", "operation": "INSERT"}',
        'THIS IS COMPLETELY CORRUPTED RAW LINE',
        '{"event_id": "uuid-2", "customer_id": "CUST-02", "operation": "UPDATE"}',
        '{"broken_json": [1, 2, 3',
        '{"event_id": "uuid-3", "customer_id": "CUST-03", "operation": "DELETE"}',
    ]
    mixed_file.write_text("\n".join(lines), encoding="utf-8")

    result = reader.read_file(mixed_file)
    assert result.total_records == 5
    assert result.parsed_count == 3
    assert result.malformed_count == 2

    # Check parsed records line numbers
    parsed_line_nums = [r.record_number for r in result.parsed_records]
    assert parsed_line_nums == [1, 3, 5]
    assert result.parsed_records[0].data["event_id"] == "uuid-1"
    assert result.parsed_records[1].data["event_id"] == "uuid-2"
    assert result.parsed_records[2].data["event_id"] == "uuid-3"

    # Check malformed records line numbers and traceability
    malformed_line_nums = [r.record_number for r in result.malformed_records]
    assert malformed_line_nums == [2, 4]
    assert result.malformed_records[0].raw_payload == "THIS IS COMPLETELY CORRUPTED RAW LINE"
    assert result.malformed_records[1].raw_payload == '{"broken_json": [1, 2, 3'


def test_read_directory(reader: EventReader) -> None:
    """Verify directory scanner discovers and processes both input files."""
    results = reader.read_directory("data/input")
    assert len(results) >= 2

    file_names = [Path(r.source_file).name for r in results]
    assert "sample_events.json" in file_names
    assert "malformed_events.jsonl" in file_names

