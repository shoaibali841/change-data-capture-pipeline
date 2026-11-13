"""Synthetic CDC Event Generator for CDC Pipeline Hackathon.

Generates reproducible streams of Change Data Capture (CDC) events including
INSERT, UPDATE, DELETE operations, duplicate events, out-of-order events,
and schema-invalid events. Also generates raw malformed records separately.

Configured via YAML and seeded for deterministic, reproducible outputs.
"""

import copy
import json
import logging
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import yaml
from faker import Faker

logger = logging.getLogger(__name__)


class CDCEventGenerator:
    """Generates synthetic Change Data Capture (CDC) events for testing pipelines."""

    def __init__(
        self,
        config_path: Optional[Path | str] = None,
        seed: Optional[int] = None,
    ) -> None:
        """Initialize the generator with configuration and deterministic random seed.

        Args:
            config_path: Path to the generator YAML configuration file.
            seed: Optional integer random seed to override configuration.
        """
        self.config_path = (
            Path(config_path) if config_path else Path("config/generator.yaml")
        )
        self.config = self._load_config()

        # Seed precedence: explicit argument > config file > default 42
        self.seed: int = (
            seed if seed is not None else int(self.config.get("seed", 42))
        )
        self._initialize_randomness(self.seed)

        self.fake = Faker()
        Faker.seed(self.seed)

        self.base_time = datetime(2026, 11, 13, 10, 0, 0, tzinfo=timezone.utc)

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration parameters from YAML file."""
        if self.config_path.exists():
            with open(self.config_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        logger.warning(
            "Config file %s not found. Using default parameters.", self.config_path
        )
        return {
            "seed": 42,
            "event_counts": {
                "num_customers": 10,
                "insert_count": 10,
                "update_count": 8,
                "delete_count": 2,
                "duplicate_count": 3,
                "out_of_order_count": 2,
                "invalid_count": 5,
                "malformed_count": 2,
            },
            "output_paths": {
                "sample_events": "data/input/sample_events.json",
                "malformed_events": "data/input/malformed_events.jsonl",
            },
        }

    def _initialize_randomness(self, seed: int) -> None:
        """Initialize random state for reproducibility."""
        random.seed(seed)

    def _generate_deterministic_uuid(self) -> str:
        """Generate a deterministic RFC 4122 UUIDv4 from the seeded random state.

        Returns:
            str: Valid UUIDv4 string representation.
        """
        return str(uuid.UUID(int=random.getrandbits(128), version=4))

    def _format_timestamp(self, dt: datetime) -> str:
        """Format datetime to ISO 8601 UTC string."""
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _generate_customer_payload(self, customer_id: str) -> Dict[str, Any]:
        """Generate synthetic customer information using Faker."""
        return {
            "customer_id": customer_id,
            "customer_name": self.fake.name(),
            "email": self.fake.email(),
            "city": self.fake.city(),
        }

    def generate_events(self) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Generate a complete reproducible dataset of CDC events and malformed records.

        Returns:
            Tuple containing:
                - List of structured event dictionaries (valid, duplicate, out-of-order, and schema-invalid)
                - List of raw malformed JSON string records
        """
        counts = self.config.get("event_counts", {})
        num_customers = counts.get("num_customers", 10)
        insert_count = min(counts.get("insert_count", 10), num_customers)
        update_count = counts.get("update_count", 8)
        delete_count = counts.get("delete_count", 2)
        duplicate_count = counts.get("duplicate_count", 3)
        out_of_order_count = counts.get("out_of_order_count", 2)
        invalid_count = counts.get("invalid_count", 5)
        malformed_count = counts.get("malformed_count", 2)

        events: List[Dict[str, Any]] = []
        customer_state: Dict[str, Dict[str, Any]] = {}
        customer_latest_seq: Dict[str, int] = {}
        customer_latest_time: Dict[str, datetime] = {}

        global_seq = 100
        current_time = self.base_time

        # --------------------------------------------------------------------
        # 1. Normal INSERT events
        # --------------------------------------------------------------------
        for i in range(insert_count):
            cust_id = f"CUST-{i + 1:04d}"
            payload = self._generate_customer_payload(cust_id)
            customer_state[cust_id] = payload

            current_time += timedelta(minutes=2)
            global_seq += 10
            customer_latest_seq[cust_id] = global_seq
            customer_latest_time[cust_id] = current_time

            insert_event = {
                "event_id": self._generate_deterministic_uuid(),
                "customer_id": cust_id,
                "operation": "INSERT",
                "event_time": self._format_timestamp(current_time),
                "source_sequence": global_seq,
                "payload": payload,
                "_record_type": "valid",
                "_test_category": "normal_insert",
            }
            events.append(insert_event)

        # --------------------------------------------------------------------
        # 2. Normal UPDATE events
        # --------------------------------------------------------------------
        eligible_customers = list(customer_state.keys())
        for i in range(update_count):
            cust_id = eligible_customers[i % len(eligible_customers)]
            # Modify city or email
            updated_payload = copy.deepcopy(customer_state[cust_id])
            if i % 2 == 0:
                updated_payload["city"] = self.fake.city()
            else:
                updated_payload["email"] = self.fake.email()
            customer_state[cust_id] = updated_payload

            current_time += timedelta(minutes=3)
            global_seq += 10
            customer_latest_seq[cust_id] = global_seq
            customer_latest_time[cust_id] = current_time

            update_event = {
                "event_id": self._generate_deterministic_uuid(),
                "customer_id": cust_id,
                "operation": "UPDATE",
                "event_time": self._format_timestamp(current_time),
                "source_sequence": global_seq,
                "payload": updated_payload,
                "_record_type": "valid",
                "_test_category": "normal_update",
            }
            events.append(update_event)

        # --------------------------------------------------------------------
        # 3. Normal DELETE events
        # --------------------------------------------------------------------
        for i in range(min(delete_count, len(eligible_customers))):
            cust_id = eligible_customers[-(i + 1)]
            current_time += timedelta(minutes=5)
            global_seq += 10
            customer_latest_seq[cust_id] = global_seq
            customer_latest_time[cust_id] = current_time

            delete_event = {
                "event_id": self._generate_deterministic_uuid(),
                "customer_id": cust_id,
                "operation": "DELETE",
                "event_time": self._format_timestamp(current_time),
                "source_sequence": global_seq,
                "payload": {"customer_id": cust_id},
                "_record_type": "valid",
                "_test_category": "normal_delete",
            }
            events.append(delete_event)

        # --------------------------------------------------------------------
        # 4. DUPLICATE events (identical event_id and payload appended later)
        # --------------------------------------------------------------------
        valid_events_pool = [e for e in events if e["_record_type"] == "valid"]
        for i in range(min(duplicate_count, len(valid_events_pool))):
            source_event = valid_events_pool[i]
            duplicate_event = copy.deepcopy(source_event)
            duplicate_event["_record_type"] = "duplicate"
            duplicate_event["_test_category"] = "duplicate_event"
            events.append(duplicate_event)

        # --------------------------------------------------------------------
        # 5. OUT-OF-ORDER / LATE-ARRIVING events
        # Older event timestamp and source sequence, but arrives later in sequence
        # --------------------------------------------------------------------
        for i in range(out_of_order_count):
            target_cust_id = eligible_customers[i % len(eligible_customers)]
            latest_time = customer_latest_time[target_cust_id]
            latest_seq = customer_latest_seq[target_cust_id]

            # Create an event that occurred earlier than the latest event for this customer
            stale_time = latest_time - timedelta(minutes=15)
            stale_seq = max(1, latest_seq - 5)

            late_event = {
                "event_id": self._generate_deterministic_uuid(),
                "customer_id": target_cust_id,
                "operation": "UPDATE",
                "event_time": self._format_timestamp(stale_time),
                "source_sequence": stale_seq,
                "payload": {
                    "customer_id": target_cust_id,
                    "customer_name": "Late Arriving State",
                    "email": "late.arrival@example.com",
                    "city": "Stale City",
                },
                "_record_type": "out_of_order",
                "_test_category": "late_arriving_event",
            }
            events.append(late_event)

        # --------------------------------------------------------------------
        # 6. DELIBERATELY INVALID events (Schema / Constraint Violations)
        # --------------------------------------------------------------------
        invalid_templates = [
            # A. Missing customer_id
            {
                "event_id": self._generate_deterministic_uuid(),
                "operation": "INSERT",
                "event_time": self._format_timestamp(self.base_time + timedelta(minutes=50)),
                "source_sequence": 901,
                "payload": {"customer_name": "Anonymous User", "email": "anon@example.com"},
                "_record_type": "invalid",
                "_test_category": "missing_customer_id",
            },
            # B. Missing event_time
            {
                "event_id": self._generate_deterministic_uuid(),
                "customer_id": "CUST-9991",
                "operation": "UPDATE",
                "source_sequence": 902,
                "payload": {"customer_id": "CUST-9991", "city": "Timeless City"},
                "_record_type": "invalid",
                "_test_category": "missing_event_time",
            },
            # C. Invalid operation type
            {
                "event_id": self._generate_deterministic_uuid(),
                "customer_id": "CUST-9992",
                "operation": "UPSERT",  # Not in ('INSERT', 'UPDATE', 'DELETE')
                "event_time": self._format_timestamp(self.base_time + timedelta(minutes=52)),
                "source_sequence": 903,
                "payload": {"customer_id": "CUST-9992", "customer_name": "Bad Op"},
                "_record_type": "invalid",
                "_test_category": "invalid_operation",
            },
            # D. Invalid UUID
            {
                "event_id": "invalid-uuid-string-12345",
                "customer_id": "CUST-9993",
                "operation": "INSERT",
                "event_time": self._format_timestamp(self.base_time + timedelta(minutes=53)),
                "source_sequence": 904,
                "payload": {"customer_id": "CUST-9993", "email": "baduuid@example.com"},
                "_record_type": "invalid",
                "_test_category": "invalid_uuid",
            },
            # E. Negative source_sequence
            {
                "event_id": self._generate_deterministic_uuid(),
                "customer_id": "CUST-9994",
                "operation": "UPDATE",
                "event_time": self._format_timestamp(self.base_time + timedelta(minutes=54)),
                "source_sequence": -100,  # Negative sequence violates check constraint
                "payload": {"customer_id": "CUST-9994", "city": "Negative Town"},
                "_record_type": "invalid",
                "_test_category": "negative_source_sequence",
            },
        ]

        for i in range(invalid_count):
            template = invalid_templates[i % len(invalid_templates)]
            events.append(copy.deepcopy(template))

        # --------------------------------------------------------------------
        # 7. MALFORMED JSON records (Unparseable raw text)
        # Kept separate because malformed JSON cannot be parsed in a valid JSON file.
        # --------------------------------------------------------------------
        malformed_records: List[str] = [
            '{"event_id": "malformed-uuid-001", "customer_id": "CUST-0001", "operation": "INSERT", "payload": {unclosed_object',
            'CORRUPTED_RAW_STREAM_NON_JSON_RECORD_SEQUENCE_9999',
            '{"event_id": "malformed-uuid-003", "customer_id": "CUST-0002", "operation": "UPDATE", missing_quotes_here}',
        ][:malformed_count]

        return events, malformed_records

    def save_to_disk(
        self,
        output_dir: Optional[Path | str] = None,
    ) -> Dict[str, Any]:
        """Generate events and write them to disk.

        Args:
            output_dir: Optional root directory override.

        Returns:
            Dict containing generation summary statistics and output file paths.
        """
        paths_config = self.config.get("output_paths", {})
        sample_path = Path(paths_config.get("sample_events", "data/input/sample_events.json"))
        malformed_path = Path(paths_config.get("malformed_events", "data/input/malformed_events.jsonl"))

        if output_dir:
            out_base = Path(output_dir)
            sample_path = out_base / sample_path.name
            malformed_path = out_base / malformed_path.name

        # Ensure parent directories exist
        sample_path.parent.mkdir(parents=True, exist_ok=True)
        malformed_path.parent.mkdir(parents=True, exist_ok=True)

        events, malformed_records = self.generate_events()

        # Write sample events as valid JSON
        with open(sample_path, "w", encoding="utf-8") as f:
            json.dump(events, f, indent=2)

        # Write malformed records as separate raw text lines
        with open(malformed_path, "w", encoding="utf-8") as f:
            for rec in malformed_records:
                f.write(rec.strip() + "\n")

        valid_count = sum(1 for e in events if e.get("_record_type") == "valid")
        duplicate_count = sum(1 for e in events if e.get("_record_type") == "duplicate")
        out_of_order_count = sum(1 for e in events if e.get("_record_type") == "out_of_order")
        invalid_count = sum(1 for e in events if e.get("_record_type") == "invalid")
        total_records = len(events) + len(malformed_records)

        summary = {
            "seed": self.seed,
            "total_records": total_records,
            "structured_events": len(events),
            "valid_records": valid_count,
            "duplicate_records": duplicate_count,
            "out_of_order_records": out_of_order_count,
            "invalid_records": invalid_count,
            "malformed_records": len(malformed_records),
            "sample_events_path": str(sample_path),
            "malformed_events_path": str(malformed_path),
        }

        return summary

    def generate_scaled_dataset(
        self,
        total_count: int = 1000,
        output_file: Optional[Union[Path, str]] = None,
        malformed_file: Optional[Union[Path, str]] = None,
    ) -> Dict[str, Any]:
        """Generate a large-scale, reproducible CDC dataset streamed as JSONL.

        Efficiently scales to 1,000, 10,000, or 100,000+ events while preserving
        exact deterministic properties, lifecycles, and error distributions.

        Args:
            total_count: Desired target event volume.
            output_file: Destination path for structured JSONL events.
            malformed_file: Destination path for malformed text lines.

        Returns:
            Dict containing generation summary statistics.
        """
        out_path = Path(output_file) if output_file else Path(f"data/input/scaled_{total_count}.jsonl")
        mal_path = Path(malformed_file) if malformed_file else Path(f"data/input/scaled_{total_count}_malformed.jsonl")

        out_path.parent.mkdir(parents=True, exist_ok=True)
        mal_path.parent.mkdir(parents=True, exist_ok=True)

        num_customers = max(10, int(total_count * 0.30))
        insert_count = num_customers
        update_count = max(5, int(total_count * 0.50))
        delete_count = max(2, int(total_count * 0.05))
        duplicate_count = max(2, int(total_count * 0.05))
        out_of_order_count = max(2, int(total_count * 0.05))
        invalid_count = max(2, int(total_count * 0.04))
        malformed_count = max(1, int(total_count * 0.01))

        # Pre-generate fast pool to accelerate high-volume synthetic attribute creation
        pool_names = [self.fake.name() for _ in range(min(500, num_customers))]
        pool_cities = [self.fake.city() for _ in range(min(100, num_customers))]
        pool_domains = ["example.com", "test.org", "enterprise.net", "dataflow.io"]

        events_written = 0
        customer_state: Dict[str, Dict[str, Any]] = {}
        customer_latest_seq: Dict[str, int] = {}
        customer_latest_time: Dict[str, datetime] = {}

        global_seq = 100
        current_time = self.base_time

        duplicate_candidates: List[Dict[str, Any]] = []

        with open(out_path, "w", encoding="utf-8") as f_out:
            # 1. INSERT events
            for i in range(insert_count):
                cust_id = f"CUST-{i + 1:06d}"
                payload = {
                    "customer_id": cust_id,
                    "customer_name": pool_names[i % len(pool_names)],
                    "email": f"user{i + 1}@{pool_domains[i % len(pool_domains)]}",
                    "city": pool_cities[i % len(pool_cities)],
                }
                customer_state[cust_id] = payload
                current_time += timedelta(seconds=2)
                global_seq += 1
                customer_latest_seq[cust_id] = global_seq
                customer_latest_time[cust_id] = current_time

                ev = {
                    "event_id": self._generate_deterministic_uuid(),
                    "customer_id": cust_id,
                    "operation": "INSERT",
                    "event_time": self._format_timestamp(current_time),
                    "source_sequence": global_seq,
                    "payload": payload,
                    "_record_type": "valid",
                    "_test_category": "scaled_insert",
                }
                f_out.write(json.dumps(ev) + "\n")
                events_written += 1
                if len(duplicate_candidates) < duplicate_count * 2:
                    duplicate_candidates.append(ev)

            # 2. UPDATE events
            cust_keys = list(customer_state.keys())
            for i in range(update_count):
                cust_id = cust_keys[i % len(cust_keys)]
                updated_payload = dict(customer_state[cust_id])
                if i % 2 == 0:
                    updated_payload["city"] = pool_cities[(i + 7) % len(pool_cities)]
                else:
                    updated_payload["email"] = f"updated_{i}@{pool_domains[i % len(pool_domains)]}"

                customer_state[cust_id] = updated_payload
                current_time += timedelta(seconds=3)
                global_seq += 1
                customer_latest_seq[cust_id] = global_seq
                customer_latest_time[cust_id] = current_time

                ev = {
                    "event_id": self._generate_deterministic_uuid(),
                    "customer_id": cust_id,
                    "operation": "UPDATE",
                    "event_time": self._format_timestamp(current_time),
                    "source_sequence": global_seq,
                    "payload": updated_payload,
                    "_record_type": "valid",
                    "_test_category": "scaled_update",
                }
                f_out.write(json.dumps(ev) + "\n")
                events_written += 1
                if len(duplicate_candidates) < duplicate_count * 2:
                    duplicate_candidates.append(ev)

            # 3. DELETE events
            for i in range(delete_count):
                cust_id = cust_keys[-(i + 1)]
                current_time += timedelta(seconds=5)
                global_seq += 1
                customer_latest_seq[cust_id] = global_seq
                customer_latest_time[cust_id] = current_time

                ev = {
                    "event_id": self._generate_deterministic_uuid(),
                    "customer_id": cust_id,
                    "operation": "DELETE",
                    "event_time": self._format_timestamp(current_time),
                    "source_sequence": global_seq,
                    "payload": {"customer_id": cust_id},
                    "_record_type": "valid",
                    "_test_category": "scaled_delete",
                }
                f_out.write(json.dumps(ev) + "\n")
                events_written += 1

            # 4. DUPLICATE events
            for i in range(duplicate_count):
                src_ev = duplicate_candidates[i % len(duplicate_candidates)]
                dup_ev = copy.deepcopy(src_ev)
                dup_ev["_record_type"] = "duplicate"
                dup_ev["_test_category"] = "scaled_duplicate"
                f_out.write(json.dumps(dup_ev) + "\n")
                events_written += 1

            # 5. OUT-OF-ORDER events
            for i in range(out_of_order_count):
                target_cust = cust_keys[i % len(cust_keys)]
                stale_time = customer_latest_time[target_cust] - timedelta(minutes=10)
                stale_seq = max(1, customer_latest_seq[target_cust] - 5)

                ev = {
                    "event_id": self._generate_deterministic_uuid(),
                    "customer_id": target_cust,
                    "operation": "UPDATE",
                    "event_time": self._format_timestamp(stale_time),
                    "source_sequence": stale_seq,
                    "payload": {
                        "customer_id": target_cust,
                        "city": "Stale Out of Order City",
                    },
                    "_record_type": "out_of_order",
                    "_test_category": "scaled_out_of_order",
                }
                f_out.write(json.dumps(ev) + "\n")
                events_written += 1

            # 6. INVALID events
            for i in range(invalid_count):
                if i % 3 == 0:
                    inv_ev = {
                        "event_id": self._generate_deterministic_uuid(),
                        "operation": "INSERT",
                        "event_time": self._format_timestamp(current_time),
                        "source_sequence": global_seq + i + 1,
                        "payload": {"customer_name": "No Cust ID"},
                        "_record_type": "invalid",
                        "_test_category": "missing_customer_id",
                    }
                elif i % 3 == 1:
                    inv_ev = {
                        "event_id": "bad-uuid-token-xyz",
                        "customer_id": f"CUST-INV-{i}",
                        "operation": "UPDATE",
                        "event_time": self._format_timestamp(current_time),
                        "source_sequence": global_seq + i + 1,
                        "payload": {"city": "Bad UUID City"},
                        "_record_type": "invalid",
                        "_test_category": "invalid_uuid",
                    }
                else:
                    inv_ev = {
                        "event_id": self._generate_deterministic_uuid(),
                        "customer_id": f"CUST-INV-{i}",
                        "operation": "INVALID_OP",
                        "event_time": self._format_timestamp(current_time),
                        "source_sequence": global_seq + i + 1,
                        "payload": {"city": "Bad Op City"},
                        "_record_type": "invalid",
                        "_test_category": "invalid_operation",
                    }
                f_out.write(json.dumps(inv_ev) + "\n")
                events_written += 1

        # 7. MALFORMED JSON lines
        with open(mal_path, "w", encoding="utf-8") as f_mal:
            for i in range(malformed_count):
                f_mal.write(f'{{"event_id": "malformed-{i}", "raw_unclosed: [1, 2,\n')

        total_all = events_written + malformed_count
        summary = {
            "seed": self.seed,
            "total_records": total_all,
            "structured_events": events_written,
            "inserts": insert_count,
            "updates": update_count,
            "deletes": delete_count,
            "duplicates": duplicate_count,
            "out_of_order": out_of_order_count,
            "invalid": invalid_count,
            "malformed": malformed_count,
            "output_file": str(out_path),
            "malformed_file": str(mal_path),
        }
        return summary


def run_generator(seed: Optional[int] = None) -> Dict[str, Any]:
    """Execute generator and print structured summary."""
    generator = CDCEventGenerator(seed=seed)
    summary = generator.save_to_disk()

    print("\n=======================================================")
    print("CDC Synthetic Event Generator - Execution Summary")
    print("=======================================================")
    print(f"Random Seed:             {summary['seed']}")
    print(f"Total Records Generated: {summary['total_records']}")
    print(f"  - Valid Records:       {summary['valid_records']}")
    print(f"  - Duplicate Records:   {summary['duplicate_records']}")
    print(f"  - Out-of-Order Events: {summary['out_of_order_records']}")
    print(f"  - Invalid Records:     {summary['invalid_records']}")
    print(f"  - Malformed Records:   {summary['malformed_records']}")
    print(f"Sample Events File:      {summary['sample_events_path']}")
    print(f"Malformed Events File:   {summary['malformed_events_path']}")
    print("=======================================================\n")

    return summary


if __name__ == "__main__":
    run_generator()

