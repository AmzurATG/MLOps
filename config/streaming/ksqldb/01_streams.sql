-- =============================================================================
-- KSQLDB STREAM DEFINITIONS
-- Source streams from Debezium CDC and enriched transformations
-- =============================================================================

-- =============================================================================
-- SOURCE STREAM: Raw transactions from Debezium CDC
-- Reads from the Kafka topic populated by Debezium MySQL connector
-- =============================================================================

CREATE STREAM IF NOT EXISTS transactions_raw (
    transaction_id VARCHAR KEY,
    customer_id VARCHAR,
    transaction_amount DOUBLE,
    transaction_date VARCHAR,
    payment_method VARCHAR,
    product_category VARCHAR,
    quantity INT,
    customer_age INT,
    customer_location VARCHAR,
    device_used VARCHAR,
    ip_address VARCHAR,
    shipping_address VARCHAR,
    billing_address VARCHAR,
    is_fraudulent INT,
    account_age_days INT,
    transaction_hour INT
) WITH (
    KAFKA_TOPIC = 'fraud.demo.fraud_transactions',
    VALUE_FORMAT = 'JSON',
    TIMESTAMP = 'transaction_date',
    TIMESTAMP_FORMAT = 'yyyy-MM-dd HH:mm:ss'
);

-- =============================================================================
-- ENRICHED STREAM: Add computed fields for feature engineering
-- Extracts country from addresses, computes time-based flags
-- =============================================================================

CREATE STREAM IF NOT EXISTS transactions_enriched AS
SELECT
    transaction_id,
    customer_id,
    transaction_amount,
    transaction_date,
    payment_method,
    product_category,
    quantity,
    customer_age,
    customer_location,
    device_used,
    ip_address,
    shipping_address,
    billing_address,
    is_fraudulent,
    account_age_days,
    transaction_hour,

    -- Extract country from addresses (last part after comma)
    CASE
        WHEN shipping_address IS NOT NULL AND LEN(shipping_address) > 0
        THEN TRIM(SPLIT(shipping_address, ',')[ARRAY_LENGTH(SPLIT(shipping_address, ',')) - 1])
        ELSE 'Unknown'
    END AS shipping_country,

    CASE
        WHEN billing_address IS NOT NULL AND LEN(billing_address) > 0
        THEN TRIM(SPLIT(billing_address, ',')[ARRAY_LENGTH(SPLIT(billing_address, ',')) - 1])
        ELSE 'Unknown'
    END AS billing_country,

    -- Address mismatch flag (potential fraud indicator)
    CASE
        WHEN shipping_address IS NOT NULL
         AND billing_address IS NOT NULL
         AND shipping_address != billing_address
        THEN 1
        ELSE 0
    END AS address_mismatch,

    -- Night transaction flag (10pm - 5am)
    CASE
        WHEN transaction_hour >= 22 OR transaction_hour < 5
        THEN 1
        ELSE 0
    END AS is_night,

    -- Weekend flag (simplified - not available in ksqlDB without UDF)
    -- Note: DAYOFWEEK not available in ksqlDB, would need custom UDF
    0 AS is_weekend,

    -- Risky payment method flag
    CASE
        WHEN payment_method IN ('credit_card', 'wallet')
        THEN 1
        ELSE 0
    END AS risky_payment,

    -- Risky category flag
    CASE
        WHEN product_category IN ('Electronics', 'Luxury', 'Digital')
        THEN 1
        ELSE 0
    END AS risky_category,

    -- IP prefix (first octet for geo analysis)
    CASE
        WHEN ip_address IS NOT NULL AND LEN(ip_address) > 0
        THEN SPLIT(ip_address, '.')[1]
        ELSE '0'
    END AS ip_prefix

FROM transactions_raw
EMIT CHANGES;
