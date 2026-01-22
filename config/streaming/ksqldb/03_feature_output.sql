-- =============================================================================
-- KSQLDB FEATURE OUTPUT STREAM
-- Outputs enriched transactions for Redis consumer
-- =============================================================================
--
-- ARCHITECTURE NOTE:
-- ksqlDB cannot join non-windowed streams to windowed tables directly.
-- Instead, we output the enriched transaction data and the streaming consumer
-- can look up velocity features from Redis (populated by the velocity tables
-- via a separate consumer) at inference time.
--
-- Data Flow:
-- 1. transactions_enriched → fraud_streaming_features → Kafka → Redis (per-txn)
-- 2. velocity_5min/1h/24h → Kafka → Redis (aggregates, queried at inference)
-- =============================================================================

-- =============================================================================
-- OUTPUT TO KAFKA TOPIC
-- Creates the fraud.streaming.features topic consumed by Redis sensor
-- Reads directly from transactions_enriched (no windowed table joins)
-- =============================================================================

CREATE STREAM IF NOT EXISTS fraud_streaming_features
WITH (
    KAFKA_TOPIC = 'fraud.streaming.features',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 3,
    REPLICAS = 1
) AS
SELECT
    -- Transaction identifiers
    transaction_id,
    customer_id,
    transaction_amount AS amount,

    -- Enriched fields for risk assessment
    shipping_country,
    billing_country,
    address_mismatch,
    is_night,
    is_weekend,
    risky_payment,
    risky_category,
    ip_prefix,

    -- Original transaction fields needed for velocity computation
    device_used,
    payment_method,
    product_category,
    transaction_hour,

    -- Event timestamp for ordering
    ROWTIME AS event_timestamp

FROM transactions_enriched
EMIT CHANGES;
