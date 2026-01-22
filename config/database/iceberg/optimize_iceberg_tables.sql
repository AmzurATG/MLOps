-- =============================================================================
-- Iceberg Table Optimizations
-- =============================================================================
-- This script adds sorting and other optimizations to Iceberg tables.
--
-- BENEFITS:
-- - Sorted data improves Parquet row group pruning (faster customer lookups)
-- - Better compression for sorted columns
-- - Faster queries with WHERE clauses on sorted columns
--
-- Run this script via: docker exec exp-trino trino < config/database/iceberg/optimize_iceberg_tables.sql
-- =============================================================================

-- -----------------------------------------------------------------------------
-- GOLD LAYER: Sort by customer_id for faster customer lookups
-- -----------------------------------------------------------------------------
-- Customer-centric queries (velocity features, history lookups) benefit
-- from having data sorted by customer_id within each date partition.

ALTER TABLE iceberg_dev.gold.fraud_transactions
SET PROPERTIES (
    sorted_by = ARRAY['customer_id']
);

-- Also set for the training data table if it exists
-- ALTER TABLE iceberg_dev.gold.fraud_training_data
-- SET PROPERTIES (
--     sorted_by = ARRAY['customer_id']
-- );

-- -----------------------------------------------------------------------------
-- SILVER LAYER: Sort by customer_id for join performance
-- -----------------------------------------------------------------------------

ALTER TABLE iceberg_dev.silver.fraud_transactions
SET PROPERTIES (
    sorted_by = ARRAY['customer_id']
);

-- -----------------------------------------------------------------------------
-- EVALUATION LAYER: Sort by customer_id for evaluation queries
-- -----------------------------------------------------------------------------

ALTER TABLE iceberg_dev.evaluation.requests
SET PROPERTIES (
    sorted_by = ARRAY['customer_id']
);

ALTER TABLE iceberg_dev.evaluation.batch_results
SET PROPERTIES (
    sorted_by = ARRAY['model_stage', 'model_version']
);

ALTER TABLE iceberg_dev.evaluation.streaming_results
SET PROPERTIES (
    sorted_by = ARRAY['customer_id']
);

-- -----------------------------------------------------------------------------
-- COMPACTION RECOMMENDATIONS
-- -----------------------------------------------------------------------------
-- After adding sorting, run compaction to rewrite data files in sorted order.
-- This is especially important for existing data.
--
-- Run these commands to compact tables (can take time for large tables):
--
-- CALL iceberg_dev.system.rewrite_data_files(
--     table => 'gold.fraud_transactions',
--     strategy => 'sort',
--     sort_order => 'customer_id ASC NULLS LAST'
-- );
--
-- Or use the optimize procedure:
-- ALTER TABLE iceberg_dev.gold.fraud_transactions EXECUTE optimize;

-- -----------------------------------------------------------------------------
-- VERIFICATION QUERIES
-- -----------------------------------------------------------------------------
-- Check table properties to verify sorting is configured:
--
-- SHOW CREATE TABLE iceberg_dev.gold.fraud_transactions;
--
-- Check file sizes and distribution:
-- SELECT
--     file_path,
--     record_count,
--     file_size_in_bytes / 1024 / 1024 as size_mb
-- FROM iceberg_dev.gold."fraud_transactions$files"
-- ORDER BY file_size_in_bytes DESC
-- LIMIT 20;
--
-- Check partition stats:
-- SELECT * FROM iceberg_dev.gold."fraud_transactions$partitions";
