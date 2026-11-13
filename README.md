# Change Data Capture (CDC) Pipeline

## Project Overview

This project implements a local, PostgreSQL-backed Change Data Capture
(CDC) pipeline for customer data. It processes `INSERT`, `UPDATE`, and
`DELETE` events, keeps a current view of each customer, and preserves an
auditable history of accepted events.

The pipeline also handles duplicate and late-arriving events, validates
incoming records, and stores invalid or unprocessable records in a
quarantine table. It includes reproducible synthetic data, automated
tests, reconciliation checks, and performance measurements.

The solution is designed as a clear, maintainable data-engineering
implementation that can be run locally.

## Problem and Objective

When customer records change in a source system, downstream data must
reflect the latest valid state without losing the history of what
happened. Reprocessing the same event must not create unintended
duplicates, and an older event must not overwrite newer information.

The objective of this project is to:

-   Process customer insert, update, and delete events.
-   Maintain the latest customer state in PostgreSQL.
-   Preserve accepted changes in an event-history table.
-   Detect and skip duplicate events.
-   Identify stale or out-of-order events and prevent them from
    overwriting newer state.
-   Validate incoming data and quarantine rejected records with a
    reason.
-   Record pipeline run information and processing results.
-   Demonstrate correctness through automated tests and reconciliation
    checks.
-   Measure performance and document an optimization.

## Architecture and Data Flow

The pipeline follows this sequence:

``` text
JSON / JSONL input
        |
        v
     Ingestion
        |
        v
 Schema validation
        |
        +----------------------+
        |                      |
        v                      v
   Valid events          Invalid records
        |                      |
        v                      v
  CDC processing           Quarantine
        |
        v
 Duplicate and event-order checks
        |
        v
 PostgreSQL persistence
        |
        +-------------------------------+
        |               |               |
        v               v               v
 customers_current  customer_event_history  pipeline_runs
        |
        v
 Reconciliation and validation
```

### Main components

  -----------------------------------------------------------------------
  Component                           Responsibility
  ----------------------------------- -----------------------------------
  `src/generator/`                    Creates reproducible synthetic
                                      customer events.

  `src/ingestion/`                    Reads JSON and JSONL input and
                                      retains source-record context.

  `src/validation/`                   Checks event structure, required
                                      values, and operation-specific
                                      rules.

  `src/processing/`                   Applies CDC rules, duplicate
                                      detection, and event-order
                                      handling.

  `src/publishing/`                   Persists current state, event
                                      history, quarantine records, and
                                      run information.

  `src/database/`                     Manages database connections and
                                      sessions.

  `src/monitoring/`                   Runs reconciliation checks and
                                      performance measurements.

  `src/pipeline.py`                   Coordinates the end-to-end
                                      pipeline.

  `sql/`                              Contains database schema and
                                      reconciliation queries.

  `tests/`                            Contains unit and integration
                                      tests.
  -----------------------------------------------------------------------

## Data Model

The project uses four main PostgreSQL tables:

  -----------------------------------------------------------------------
  Table                               Purpose
  ----------------------------------- -----------------------------------
  `customers_current`                 Holds the latest known state for
                                      each customer.

  `customer_event_history`            Preserves accepted customer change
                                      events for audit purposes.

  `event_quarantine`                  Stores invalid or unprocessable
                                      records and their rejection
                                      details.

  `pipeline_runs`                     Records pipeline execution
                                      information and processing status.
  -----------------------------------------------------------------------

The database schema in `sql/schema.sql` is the source of truth for
column names, data types, keys, and constraints.

### Processing rules

  -----------------------------------------------------------------------
  Event type or condition             Pipeline behaviour
  ----------------------------------- -----------------------------------
  Valid, newer `INSERT`               Creates the customer's
                                      current-state record and records
                                      the event in history.

  Valid, newer `UPDATE`               Updates the supplied customer
                                      attributes and records the event in
                                      history.

  Valid, newer `DELETE`               Soft-deletes the customer and
                                      records the delete event in
                                      history.

  Duplicate event                     Skips the repeated event so it is
                                      not applied again.

  Stale or late event                 Retains the event in history as not
                                      applied; current customer state
                                      remains unchanged.

  Invalid or unprocessable event      Stores the record in quarantine
                                      with its rejection reason.
  -----------------------------------------------------------------------

### Event ordering and duplicate handling

The event processor uses event-ordering information to determine whether
an incoming event should change the current customer state. The
implementation considers event time, source sequence, and event
identity. Duplicate event identities are detected so that replaying an
event does not apply the same change again.

A stale event can remain available in history for audit purposes without
replacing a newer customer value.

### Delete handling

Deletes use a soft-delete approach. The customer record remains in
`customers_current` with its deletion state recorded, rather than being
physically removed. This keeps the record available for audit and
reconciliation.

### Identifiers and naming

The project uses consistent `snake_case` database naming and stable
event/customer identifiers. Timestamp and identifier validation is
handled by the event schema and validation modules. Run and source
context are retained where provided by the input and pipeline.

## Project Structure

``` text
CandidateWorkspace/
├── config/
│   └── generator.yaml
├── data/
│   ├── input/
│   └── benchmark/
│       └── benchmark_results.json
├── docs/
├── sql/
│   ├── schema.sql
│   └── reconciliation.sql
├── src/
│   ├── database/
│   ├── generator/
│   ├── ingestion/
│   ├── monitoring/
│   ├── processing/
│   ├── publishing/
│   ├── validation/
│   └── pipeline.py
├── tests/
│   ├── unit/
│   └── integration/
├── .env.example
├── .gitignore
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Technology Stack

-   Python 3.10
-   PostgreSQL 16
-   Docker Compose
-   SQLAlchemy
-   Pydantic
-   PyYAML
-   python-dotenv
-   Faker
-   pytest
-   pytest-cov

## Prerequisites

Install the following before running the project:

-   Python 3.10 or later
-   Docker Desktop with Docker Compose
-   Git
-   PostgreSQL client (`psql`) or pgAdmin for database inspection

The commands below use Windows PowerShell.

## Configuration

Create a local environment file from the example:

``` powershell
Copy-Item .env.example .env
```

Set the database values in `.env`. The local project configuration used
during development maps host port `5433` to PostgreSQL container port
`5432`.

Example configuration (use local credentials; do not commit them):

``` dotenv
DB_HOST=localhost
DB_PORT=5433
DB_NAME=cdc_db
DB_USER=cdc_user
DB_PASSWORD=your_local_database_password

TEST_DB_HOST=localhost
TEST_DB_PORT=5433
TEST_DB_NAME=cdc_test_db
TEST_DB_USER=cdc_user
TEST_DB_PASSWORD=your_local_test_database_password
```

Use the variable names defined in `.env.example` and
`src/database/connection.py` if they differ in your checkout.

**Keep the databases separate:**

-   `cdc_db` is the application database.
-   `cdc_test_db` is reserved for integration tests and isolated
    benchmark cleanup.

Do not point test cleanup at `cdc_db`. Keep `.env` out of Git and use
placeholders only in `.env.example`.

## Setup

### 1. Open the project directory

``` powershell
cd C:\CandidateWorkspace
```

### 2. Create and activate a virtual environment

``` powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 3. Install dependencies

``` powershell
pip install -r requirements.txt
```

### 4. Configure environment variables

``` powershell
Copy-Item .env.example .env
```

Edit `.env` with the local database settings.

### 5. Start PostgreSQL

``` powershell
docker compose up -d
docker compose ps
```

Confirm that the PostgreSQL container is running and healthy.

### 6. Initialize the database schema

Apply `sql/schema.sql` to the application database and, separately, to
the test database when setting up a fresh environment. For example, with
`psql` available in PowerShell:

``` powershell
Get-Content .\sql\schema.sql | psql -h localhost -p 5433 -U cdc_user -d cdc_db
Get-Content .\sql\schema.sql | psql -h localhost -p 5433 -U cdc_user -d cdc_test_db
```

Use the host, port, database, and user values configured for your
environment. If the schema is already initialized, inspect the database
before applying setup scripts again. Do not drop databases or Docker
volumes as a routine setup step.

## Running the Project

Run commands from the project root with the virtual environment
activated and `.env` configured.

### Generate synthetic events

``` powershell
python -m src.generator.data_generator
```

The generator uses `config/generator.yaml` to define its configurable
generation settings. It produces reproducible synthetic customer events
for pipeline execution and testing.

The generated scenarios include normal inserts, updates and deletes,
along with duplicate, stale, invalid, and malformed records.

### Execute the CDC pipeline

``` powershell
python -m src.pipeline
```

The pipeline reads the configured input, validates records, processes
valid events, persists the resulting state and history, and records run
outcomes.

### Run reconciliation

``` powershell
python -m src.monitoring.reconciliation
```

Reconciliation checks the persisted data for consistency. The SQL
versions of the checks are also available in:

``` text
sql/reconciliation.sql
```

Run reconciliation against the database you intend to inspect. The
reconciliation process is for checking data consistency; it should not
be used to clean or reset application data.

## Testing

Run the complete automated test suite:

``` powershell
python -m pytest -v
```

Run the tests with coverage:

``` powershell
python -m pytest --cov=src --cov-report=term-missing
```

Integration tests use the dedicated `cdc_test_db` database. The test
setup includes safeguards to prevent tests from silently falling back to
the application database.

The verified test run for this project completed with **54 passed**. A
subsequent coverage run also completed with **54 passed** and reported
**61% total source coverage**. These figures describe the recorded
verification run and may change if the code or tests are updated.

## Performance Measurement

The project includes performance measurement functionality in
`src/monitoring/benchmark.py`. Recorded benchmark output is saved in:

``` text
data/benchmark/benchmark_results.json
```

The saved results include the following optimized measurements:

    Input events   Elapsed time            Throughput
  -------------- -------------- ---------------------
           1,000       0.3750 s   2,666.71 events/sec
          10,000       4.2634 s   2,345.53 events/sec
          50,000      24.3467 s   2,053.67 events/sec
         100,000      41.7718 s   2,393.96 events/sec

The recorded 1,000-event row-by-row baseline was 4.7995 seconds, or
208.35 events/sec. The optimized 1,000-event run was approximately
**12.8 times faster**.

For the 100,000-event run, the recorded phase breakdown was:

  Phase                Time   Share
  ------------- ----------- -------
  Ingestion        2.7721 s    6.6%
  Validation       4.3850 s   10.5%
  Persistence     34.6147 s   82.9%

These are measurements from the saved benchmark results. Actual
performance depends on the machine, PostgreSQL configuration, and
workload. Benchmark cleanup must remain isolated to `cdc_test_db`; do
not run destructive benchmark operations against `cdc_db`.

## Reconciliation Results

The verified application-database reconciliation reported:

-   10 customers: 8 active and 2 soft-deleted.
-   22 event-history records: 10 inserts, 10 updates, and 2 deletes.
-   12 stale events.
-   7 quarantined events.
-   1 completed pipeline run.
-   All 8 integrity checks passed with zero violations.

These figures represent the verified sample run. Results will vary when
different input data is processed.

## Assumptions and Design Choices

-   Customer change events are supplied as JSON or JSONL records.
-   Each event contains the identifiers and ordering information
    required by the event schema.
-   PostgreSQL stores both the latest customer state and the event audit
    history.
-   Invalid or unprocessable records are retained separately in
    quarantine.
-   Deletes are represented as soft deletes so customer records remain
    traceable.
-   Event ordering information is used to prevent older events from
    overwriting newer state.
-   Synthetic data is used so the project can be run and reviewed
    without relying on private or external source data.
-   The solution is intentionally local-first and uses PostgreSQL rather
    than requiring a distributed streaming platform.

## Known Limitations and Next Steps

The current implementation is a locally runnable CDC solution. Before
using it in a production environment, further work would be needed
around concurrent first-time inserts, broader schema-change handling,
and operational retry and alerting policies. These areas should be
evaluated against the requirements of the target environment.

As a future enhancement, Apache Airflow could be used to orchestrate
the existing pipeline. An Airflow DAG could schedule ingestion,
validation, CDC processing, persistence, and reconciliation as
dependent tasks, with task-level retries and execution monitoring.
Airflow is a possible extension; it is not part of the current
implementation or a requirement for this submission.

## Submission Contents

The project submission includes:

1.  Python source code and dependency/setup files.
2.  Reproducible synthetic sample data and its generator.
3.  Automated unit and integration tests.
4.  PostgreSQL schema and reconciliation SQL.
5.  Saved benchmark results.
6.  README, data model information, and design documentation.
7.  Test and reconciliation execution evidence.

## Security

-   Do not commit `.env` or real database credentials.
-   Keep `.env.example` limited to placeholder values.
-   Do not include secrets in screenshots, logs, or reports.
-   Keep the application and test databases separate.
-   Do not run destructive test or benchmark cleanup against `cdc_db`.
