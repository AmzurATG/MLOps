-- =============================================================================
-- Iceberg Monitoring Tables DDL
-- =============================================================================
--
-- This script creates tables required for Iceberg snapshot monitoring and
-- automated rollback detection.
--
-- Tables:
--   1. snapshot_cursor - Tracks last known snapshot per table
--   2. iceberg_rollback_events - Audit trail of detected rollbacks
--
-- Usage:
--   Run via Trino CLI or include in docker-compose init
--
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Snapshot Cursor Table
-- -----------------------------------------------------------------------------
-- Tracks the last known snapshot ID for each monitored table.
-- Used by iceberg_snapshot_sensor to detect when a rollback occurs.

CREATE SCHEMA IF NOT EXISTS iceberg_dev.metadata;

CREATE TABLE IF NOT EXISTS iceberg_dev.metadata.snapshot_cursor (
    table_name VARCHAR COMMENT 'Full table name (catalog.schema.table)',
    last_snapshot_id BIGINT COMMENT 'Last observed snapshot ID',
    last_snapshot_timestamp TIMESTAMP COMMENT 'Timestamp of last snapshot',
    checked_at TIMESTAMP COMMENT 'When this cursor was last updated'
) WITH (
    format = 'PARQUET'
);

-- -----------------------------------------------------------------------------
-- 2. Iceberg Rollback Events Table
-- -----------------------------------------------------------------------------
-- Audit trail of all detected Iceberg rollback events.
-- Populated by iceberg_snapshot_sensor when rollback is detected.

CREATE TABLE IF NOT EXISTS iceberg_dev.metadata.iceberg_rollback_events (
    event_id VARCHAR COMMENT 'Unique event identifier (ROLLBACK_xxxxx)',
    table_name VARCHAR COMMENT 'Table where rollback was detected',
    previous_snapshot_id BIGINT COMMENT 'Snapshot ID before rollback',
    current_snapshot_id BIGINT COMMENT 'Snapshot ID after rollback',
    detected_at TIMESTAMP COMMENT 'When rollback was detected',
    status VARCHAR COMMENT 'Event status: detected, syncing, synced, failed',
    dagster_run_id VARCHAR COMMENT 'Associated Dagster run ID',
    partition_date DATE COMMENT 'Partition column for efficient querying'
) WITH (
    format = 'PARQUET',
    partitioning = ARRAY['partition_date']
);

-- -----------------------------------------------------------------------------
-- 3. Rollback History Table (if not exists)
-- -----------------------------------------------------------------------------
-- Complete audit trail of rollback sync operations.
-- Created by rollback_sync.py but included here for completeness.

CREATE TABLE IF NOT EXISTS iceberg_dev.monitoring.rollback_history (
    rollback_id VARCHAR COMMENT 'Unique rollback operation ID',
    direction VARCHAR COMMENT 'Rollback direction: lakefs_to_iceberg or iceberg_to_all',
    source_table VARCHAR COMMENT 'Table that triggered the rollback',
    target_snapshot_id BIGINT COMMENT 'Target snapshot rolled back to',
    previous_snapshot_id BIGINT COMMENT 'Previous snapshot before rollback',
    upstream_synced VARCHAR COMMENT 'List of upstream tables synced',
    downstream_synced VARCHAR COMMENT 'List of downstream tables synced',
    labelstudio_deleted INTEGER COMMENT 'Number of Label Studio tasks deleted',
    lakefs_commit VARCHAR COMMENT 'Associated LakeFS commit if any',
    success BOOLEAN COMMENT 'Whether rollback sync succeeded',
    errors VARCHAR COMMENT 'Error messages if any',
    started_at TIMESTAMP COMMENT 'When rollback sync started',
    completed_at TIMESTAMP COMMENT 'When rollback sync completed',
    partition_date DATE COMMENT 'Partition column'
) WITH (
    format = 'PARQUET',
    partitioning = ARRAY['partition_date']
);

-- -----------------------------------------------------------------------------
-- Verification Queries
-- -----------------------------------------------------------------------------
-- Run these to verify tables were created:
--
-- SHOW TABLES IN iceberg_dev.metadata;
-- DESCRIBE iceberg_dev.metadata.snapshot_cursor;
-- DESCRIBE iceberg_dev.metadata.iceberg_rollback_events;
-- DESCRIBE iceberg_dev.monitoring.rollback_history;
