-- ============================================================================
-- Change Data Capture (CDC) Pipeline - PostgreSQL Database Schema
-- Target: PostgreSQL 16+
-- Script: sql/schema.sql
-- ============================================================================


-- ============================================================================
-- 1. Pipeline Runs Table
-- Tracks metadata, execution status, event counters, and metrics for each run.
-- ============================================================================
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING' 
        CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED')),
    input_count INTEGER NOT NULL DEFAULT 0 
        CHECK (input_count >= 0),
    accepted_count INTEGER NOT NULL DEFAULT 0 
        CHECK (accepted_count >= 0),
    inserted_count INTEGER NOT NULL DEFAULT 0 
        CHECK (inserted_count >= 0),
    updated_count INTEGER NOT NULL DEFAULT 0 
        CHECK (updated_count >= 0),
    deleted_count INTEGER NOT NULL DEFAULT 0 
        CHECK (deleted_count >= 0),
    duplicate_count INTEGER NOT NULL DEFAULT 0 
        CHECK (duplicate_count >= 0),
    stale_event_count INTEGER NOT NULL DEFAULT 0 
        CHECK (stale_event_count >= 0),
    rejected_count INTEGER NOT NULL DEFAULT 0 
        CHECK (rejected_count >= 0),
    error_message TEXT,
    duration_seconds NUMERIC(10, 4) 
        CHECK (duration_seconds IS NULL OR duration_seconds >= 0)
);

COMMENT ON TABLE pipeline_runs IS 'Tracks pipeline run audit logs, execution metrics, and event counters';
COMMENT ON COLUMN pipeline_runs.run_id IS 'Unique identifier for the pipeline execution run';
COMMENT ON COLUMN pipeline_runs.status IS 'Run status: RUNNING, COMPLETED, or FAILED';
COMMENT ON COLUMN pipeline_runs.stale_event_count IS 'Count of out-of-order events appended to history but skipped for current-state';


-- ============================================================================
-- 2. Customers Current-State Table
-- Maintains the latest materialized state for each customer.
-- Supports soft deletion and out-of-order resolution via source sequence.
-- ============================================================================
CREATE TABLE IF NOT EXISTS customers_current (
    customer_id VARCHAR(64) PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    city VARCHAR(255),
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    last_event_time TIMESTAMPTZ NOT NULL,
    last_source_sequence BIGINT NOT NULL 
        CHECK (last_source_sequence >= 0),
    last_event_id UUID NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE customers_current IS 'Current materialized state of customers; updated only when event sequence >= current sequence';
COMMENT ON COLUMN customers_current.customer_id IS 'Unique business identifier for the customer';
COMMENT ON COLUMN customers_current.email IS 'Customer email address (non-unique to support multiple customer profiles)';
COMMENT ON COLUMN customers_current.is_deleted IS 'Soft deletion flag set to TRUE upon applying a DELETE event';
COMMENT ON COLUMN customers_current.last_event_time IS 'Timestamp of the latest event applied to this customer record';
COMMENT ON COLUMN customers_current.last_source_sequence IS 'Deterministic sequence number of the latest applied event for ordering';
COMMENT ON COLUMN customers_current.last_event_id IS 'UUID of the latest applied CDC event';


-- ============================================================================
-- 3. Customer Event History Table
-- Append-only audit log of all accepted unique CDC events.
-- Late-arriving and out-of-order events are preserved here for full traceability.
-- ============================================================================
CREATE TABLE IF NOT EXISTS customer_event_history (
    event_id UUID PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL,
    operation VARCHAR(10) NOT NULL 
        CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
    old_values JSONB,
    new_values JSONB,
    event_time TIMESTAMPTZ NOT NULL,
    source_sequence BIGINT NOT NULL 
        CHECK (source_sequence >= 0),
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(255),
    run_id UUID NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE RESTRICT
);

COMMENT ON TABLE customer_event_history IS 'Append-only audit history preserving all accepted unique CDC events';
COMMENT ON COLUMN customer_event_history.event_id IS 'Unique identifier per CDC event used for deduplication';
COMMENT ON COLUMN customer_event_history.operation IS 'CDC operation type: INSERT, UPDATE, or DELETE';
COMMENT ON COLUMN customer_event_history.old_values IS 'Previous attribute state before the change (JSONB format)';
COMMENT ON COLUMN customer_event_history.new_values IS 'New attribute state after the change (JSONB format)';
COMMENT ON COLUMN customer_event_history.source_sequence IS 'Monotonically increasing sequence number from the event source';
COMMENT ON COLUMN customer_event_history.run_id IS 'Reference to the pipeline run that ingested and processed this event';


-- ============================================================================
-- 4. Event Quarantine Table
-- Captures rejected/malformed events with diagnostic reasons and traceability.
-- Raw payload is stored as TEXT to preserve malformed JSON without parsing errors.
-- ============================================================================
CREATE TABLE IF NOT EXISTS event_quarantine (
    quarantine_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id UUID,
    raw_payload TEXT NOT NULL,
    source_file VARCHAR(255),
    run_id UUID NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE RESTRICT,
    rejection_reason TEXT NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE event_quarantine IS 'Quarantine store for unparseable, invalid, or schema-violating incoming events';
COMMENT ON COLUMN event_quarantine.quarantine_id IS 'Autoincrementing identity key for quarantined items';
COMMENT ON COLUMN event_quarantine.event_id IS 'Nullable event UUID extracted if event headers were parseable';
COMMENT ON COLUMN event_quarantine.raw_payload IS 'Raw text representation of the rejected message';
COMMENT ON COLUMN event_quarantine.rejection_reason IS 'Descriptive explanation of the validation or schema failure';
COMMENT ON COLUMN event_quarantine.run_id IS 'Reference to the pipeline run during which the quarantine occurred';


-- ============================================================================
-- 5. Indexes
-- Optimized for audit queries, foreign key lookups, and sequence reconciliation.
-- ============================================================================

-- Fast lookup of all historical mutations for a given customer
CREATE INDEX IF NOT EXISTS idx_customer_event_history_customer_id 
    ON customer_event_history (customer_id);

-- Time-range filtering and temporal ordering in event history
CREATE INDEX IF NOT EXISTS idx_customer_event_history_event_time 
    ON customer_event_history (event_time);

-- Run-level event tracking and pipeline audit reconciliation
CREATE INDEX IF NOT EXISTS idx_customer_event_history_run_id 
    ON customer_event_history (run_id);

-- Run-level quarantine tracking and reporting
CREATE INDEX IF NOT EXISTS idx_event_quarantine_run_id 
    ON event_quarantine (run_id);

-- Fast temporal queries and latency tracking on current-state customer records
CREATE INDEX IF NOT EXISTS idx_customers_current_last_event_time 
    ON customers_current (last_event_time);

