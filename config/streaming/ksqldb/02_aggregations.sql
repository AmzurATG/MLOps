-- =============================================================================
-- KSQLDB WINDOWED AGGREGATION TABLES
-- Real-time velocity and aggregated features using tumbling windows
-- =============================================================================

-- =============================================================================
-- 5-MINUTE VELOCITY TABLE (Tumbling Window)
-- High-frequency velocity detection for rapid fraud patterns
-- =============================================================================

CREATE TABLE IF NOT EXISTS velocity_5min AS
SELECT
    customer_id,

    -- Transaction counts
    COUNT(*) AS tx_count_5min,

    -- Amount aggregations
    SUM(transaction_amount) AS amount_sum_5min,
    AVG(transaction_amount) AS avg_amount_5min,
    MAX(transaction_amount) AS max_amount_5min,
    MIN(transaction_amount) AS min_amount_5min,

    -- Distinct counts (velocity indicators)
    COUNT_DISTINCT(device_used) AS unique_devices_5min,
    COUNT_DISTINCT(shipping_country) AS unique_countries_5min,
    COUNT_DISTINCT(ip_prefix) AS unique_ip_prefixes_5min,
    COUNT_DISTINCT(payment_method) AS unique_payment_methods_5min,

    -- Risk flags sum
    SUM(address_mismatch) AS address_mismatch_count_5min,
    SUM(risky_payment) AS risky_payment_count_5min,
    SUM(risky_category) AS risky_category_count_5min,

    -- Window boundaries
    WINDOWSTART AS window_start,
    WINDOWEND AS window_end

FROM transactions_enriched
WINDOW TUMBLING (SIZE 5 MINUTES)
GROUP BY customer_id
EMIT CHANGES;

-- =============================================================================
-- 1-HOUR VELOCITY TABLE (Tumbling Window)
-- Medium-term pattern detection
-- =============================================================================

CREATE TABLE IF NOT EXISTS velocity_1h AS
SELECT
    customer_id,

    -- Transaction counts
    COUNT(*) AS tx_count_1h,

    -- Amount aggregations
    SUM(transaction_amount) AS amount_sum_1h,
    AVG(transaction_amount) AS avg_amount_1h,
    MAX(transaction_amount) AS max_amount_1h,
    MIN(transaction_amount) AS min_amount_1h,

    -- Distinct counts
    COUNT_DISTINCT(device_used) AS unique_devices_1h,
    COUNT_DISTINCT(ip_address) AS unique_ips_1h,
    COUNT_DISTINCT(shipping_country) AS unique_countries_1h,
    COUNT_DISTINCT(product_category) AS unique_categories_1h,

    -- Risk flags sum
    SUM(is_night) AS night_tx_count_1h,
    SUM(address_mismatch) AS address_mismatch_count_1h,

    -- Window boundaries
    WINDOWSTART AS window_start,
    WINDOWEND AS window_end

FROM transactions_enriched
WINDOW TUMBLING (SIZE 1 HOUR)
GROUP BY customer_id
EMIT CHANGES;

-- =============================================================================
-- 24-HOUR VELOCITY TABLE (Tumbling Window)
-- Daily pattern detection
-- =============================================================================

CREATE TABLE IF NOT EXISTS velocity_24h AS
SELECT
    customer_id,

    -- Transaction counts
    COUNT(*) AS tx_count_24h,

    -- Amount aggregations
    SUM(transaction_amount) AS amount_sum_24h,
    AVG(transaction_amount) AS avg_amount_24h,
    MAX(transaction_amount) AS max_amount_24h,
    MIN(transaction_amount) AS min_amount_24h,

    -- Distinct counts
    COUNT_DISTINCT(shipping_country) AS unique_countries_24h,
    COUNT_DISTINCT(payment_method) AS unique_payment_methods_24h,
    COUNT_DISTINCT(device_used) AS unique_devices_24h,
    COUNT_DISTINCT(ip_address) AS unique_ips_24h,

    -- Risk flags sum
    SUM(is_weekend) AS weekend_tx_count_24h,
    SUM(is_night) AS night_tx_count_24h,
    SUM(address_mismatch) AS address_mismatch_count_24h,

    -- Window boundaries
    WINDOWSTART AS window_start,
    WINDOWEND AS window_end

FROM transactions_enriched
WINDOW TUMBLING (SIZE 24 HOURS)
GROUP BY customer_id
EMIT CHANGES;

-- =============================================================================
-- GLOBAL CUSTOMER AGGREGATES (Session Window)
-- Running totals for customer behavioral features
-- =============================================================================

CREATE TABLE IF NOT EXISTS customer_aggregates AS
SELECT
    customer_id,

    -- Lifetime stats
    COUNT(*) AS total_transactions,
    SUM(transaction_amount) AS total_spend,
    AVG(transaction_amount) AS avg_transaction_amount,
    MAX(transaction_amount) AS max_transaction_amount,

    -- Latest activity tracking
    MAX(ROWTIME) AS last_transaction_time,

    -- Category preferences
    COUNT_DISTINCT(product_category) AS unique_categories_all_time,
    COUNT_DISTINCT(payment_method) AS unique_payment_methods_all_time

FROM transactions_enriched
WINDOW SESSION (30 DAYS)
GROUP BY customer_id
EMIT CHANGES;
