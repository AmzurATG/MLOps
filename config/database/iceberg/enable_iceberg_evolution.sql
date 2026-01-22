-- =============================================================================
-- Enable Iceberg Schema Evolution
-- =============================================================================
-- This script enables schema evolution on all Iceberg tables to allow:
-- - Adding new columns without recreating tables
-- - Column type widening (e.g., int -> bigint)
-- - Column reordering
--
-- Run this script via: docker exec exp-trino trino < scripts/enable_iceberg_evolution.sql
-- =============================================================================

-- -----------------------------------------------------------------------------
-- GOLD LAYER: ML Training Data
-- -----------------------------------------------------------------------------
ALTER TABLE iceberg.gold.fraud_training_data
SET PROPERTIES (
    'write.parquet.schema-evolution.enabled' = 'true',
    'write.update.mode' = 'merge-on-read',
    'write.delete.mode' = 'merge-on-read'
);

-- -----------------------------------------------------------------------------
-- SILVER LAYER: Cleaned/Enriched Data
-- -----------------------------------------------------------------------------
ALTER TABLE iceberg.silver.transactions_cleaned
SET PROPERTIES (
    'write.parquet.schema-evolution.enabled' = 'true',
    'write.update.mode' = 'merge-on-read',
    'write.delete.mode' = 'merge-on-read'
);

ALTER TABLE iceberg.silver.customer_features
SET PROPERTIES (
    'write.parquet.schema-evolution.enabled' = 'true'
);

ALTER TABLE iceberg.silver.terminal_risk_scores
SET PROPERTIES (
    'write.parquet.schema-evolution.enabled' = 'true'
);

-- -----------------------------------------------------------------------------
-- MONITORING LAYER: Drift Reports & Performance Metrics
-- -----------------------------------------------------------------------------
ALTER TABLE iceberg.monitoring.drift_reports
SET PROPERTIES (
    'write.parquet.schema-evolution.enabled' = 'true'
);

ALTER TABLE iceberg.monitoring.performance_metrics
SET PROPERTIES (
    'write.parquet.schema-evolution.enabled' = 'true'
);

ALTER TABLE iceberg.monitoring.retraining_history
SET PROPERTIES (
    'write.parquet.schema-evolution.enabled' = 'true'
);

ALTER TABLE iceberg.monitoring.predictions
SET PROPERTIES (
    'write.parquet.schema-evolution.enabled' = 'true'
);

-- -----------------------------------------------------------------------------
-- METADATA LAYER: Schema Version Tracking
-- -----------------------------------------------------------------------------

-- Add schema_version column to snapshot_cursor for tracking
-- This allows correlating data snapshots with schema versions
ALTER TABLE iceberg.metadata.snapshot_cursor
ADD COLUMN IF NOT EXISTS schema_version VARCHAR;

-- Create schema evolution audit table
CREATE TABLE IF NOT EXISTS iceberg.metadata.schema_evolution_audit (
    audit_id VARCHAR,
    table_name VARCHAR,
    change_type VARCHAR,           -- 'column_added', 'column_removed', 'type_changed'
    column_name VARCHAR,
    old_type VARCHAR,
    new_type VARCHAR,
    changed_at TIMESTAMP,
    changed_by VARCHAR,
    contract_version VARCHAR,      -- Links to FeatureContract version
    partition_date DATE
) WITH (
    format = 'PARQUET',
    partitioning = ARRAY['partition_date']
);

-- -----------------------------------------------------------------------------
-- VERIFICATION QUERIES
-- -----------------------------------------------------------------------------
-- Run these to verify schema evolution is enabled:

-- Check table properties:
-- SHOW CREATE TABLE iceberg.gold.fraud_training_data;

-- View table history:
-- SELECT * FROM iceberg.gold."fraud_training_data$history" ORDER BY made_current_at DESC LIMIT 10;

-- View table snapshots:
-- SELECT * FROM iceberg.gold."fraud_training_data$snapshots" ORDER BY committed_at DESC LIMIT 10;

-- View schema changes:
-- SELECT * FROM iceberg.metadata.schema_evolution_audit ORDER BY changed_at DESC;
