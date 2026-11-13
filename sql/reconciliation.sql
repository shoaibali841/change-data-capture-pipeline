-- ============================================================================
-- Change Data Capture (CDC) Pipeline - Data Reconciliation & Audit Queries
-- Target: PostgreSQL 16+
-- Script: sql/reconciliation.sql
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Pipeline Run Counts by Status
-- Summarizes all historical and active execution batches.
-- ----------------------------------------------------------------------------
SELECT 
    status, 
    COUNT(*) AS run_count,
    COALESCE(SUM(input_count), 0) AS total_input,
    COALESCE(SUM(accepted_count), 0) AS total_accepted,
    COALESCE(SUM(duplicate_count), 0) AS total_duplicates,
    COALESCE(SUM(stale_event_count), 0) AS total_stale,
    COALESCE(SUM(rejected_count), 0) AS total_quarantined
FROM pipeline_runs
GROUP BY status
ORDER BY status;

-- ----------------------------------------------------------------------------
-- 2. Current Customer Record Counts & Status Breakdown (Active vs Soft-Deleted)
-- ----------------------------------------------------------------------------
SELECT 
    COUNT(*) AS total_customers,
    COUNT(*) FILTER (WHERE NOT is_deleted) AS active_customers,
    COUNT(*) FILTER (WHERE is_deleted) AS soft_deleted_customers
FROM customers_current;

-- ----------------------------------------------------------------------------
-- 3. Event History Counts by Operation
-- ----------------------------------------------------------------------------
SELECT 
    operation, 
    COUNT(*) AS event_count,
    COUNT(DISTINCT customer_id) AS distinct_customers
FROM customer_event_history
GROUP BY operation
ORDER BY operation;

-- ----------------------------------------------------------------------------
-- 4. Duplicate Event IDs in History (Audit: Must return 0 rows)
-- ----------------------------------------------------------------------------
SELECT 
    event_id, 
    COUNT(*) AS occurrence_count
FROM customer_event_history
GROUP BY event_id
HAVING COUNT(*) > 1;

-- ----------------------------------------------------------------------------
-- 5. Events Linked to Nonexistent Pipeline Runs (Audit: Must return 0 rows)
-- Detects orphaned events not associated with an existing pipeline run.
-- ----------------------------------------------------------------------------
SELECT 
    ceh.event_id, 
    ceh.customer_id, 
    ceh.run_id
FROM customer_event_history ceh
LEFT JOIN pipeline_runs pr ON ceh.run_id = pr.run_id
WHERE pr.run_id IS NULL;

-- ----------------------------------------------------------------------------
-- 6. Quarantined Event Counts by Rejection Reason
-- ----------------------------------------------------------------------------
SELECT 
    rejection_reason, 
    COUNT(*) AS quarantined_count
FROM event_quarantine
GROUP BY rejection_reason
ORDER BY quarantined_count DESC;

-- ----------------------------------------------------------------------------
-- 7. Customers with Inconsistent Deletion Metadata (Audit: Must return 0 rows)
-- Verifies that is_deleted strictly matches whether the latest event was DELETE.
-- ----------------------------------------------------------------------------
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

-- ----------------------------------------------------------------------------
-- 8. Current State Alignment with Latest Accepted Event (Audit: Must return 0 rows)
-- Verifies that customers_current matches the MAX(event_time, source_sequence, event_id)
-- from customer_event_history according to CDC deterministic tie-breaking.
-- ----------------------------------------------------------------------------
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

-- ----------------------------------------------------------------------------
-- 9. Active Customers with Missing Required Attributes (Audit: Must return 0 rows)
-- Active customers must have valid non-empty customer_name and email.
-- ----------------------------------------------------------------------------
SELECT 
    customer_id, 
    customer_name, 
    email, 
    city, 
    is_deleted
FROM customers_current
WHERE is_deleted = FALSE
  AND (customer_name IS NULL OR TRIM(customer_name) = '' 
       OR email IS NULL OR TRIM(email) = '');

-- ----------------------------------------------------------------------------
-- 10. Soft-Deleted Customers Retaining Previous Attributes (Audit: Must return 0 rows)
-- Soft-deleted customers must retain their previous customer_name and email.
-- ----------------------------------------------------------------------------
SELECT 
    customer_id, 
    customer_name, 
    email, 
    city, 
    is_deleted
FROM customers_current
WHERE is_deleted = TRUE
  AND (customer_name IS NULL OR TRIM(customer_name) = '' 
       OR email IS NULL OR TRIM(email) = '');

-- ----------------------------------------------------------------------------
-- 11. Stale / Out-of-Order Events Preserved in History (Audit Verification)
-- Identifies all accepted events that arrived with older timestamps/sequences
-- and were preserved in history without corrupting newer materialized state.
-- ----------------------------------------------------------------------------
SELECT 
    ceh.event_id,
    ceh.customer_id,
    ceh.operation,
    ceh.event_time AS stale_event_time,
    ceh.source_sequence AS stale_sequence,
    c.last_event_time AS current_latest_time,
    c.last_source_sequence AS current_latest_sequence
FROM customer_event_history ceh
JOIN customers_current c ON ceh.customer_id = c.customer_id
WHERE (ceh.event_time, ceh.source_sequence, ceh.event_id) < (c.last_event_time, c.last_source_sequence, c.last_event_id)
ORDER BY ceh.customer_id, ceh.event_time;

-- ----------------------------------------------------------------------------
-- 12. Total Ingested vs Processed Event Reconciliation (Per Pipeline Run)
-- Validates event conservation: input_count = accepted + duplicate + rejected.
-- ----------------------------------------------------------------------------
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

