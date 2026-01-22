-- ═══════════════════════════════════════════════════════════════════════════════
-- EVALUATION TABLES FOR STREAMING (MySQL)
-- ═══════════════════════════════════════════════════════════════════════════════
--
-- This creates MySQL tables for streaming evaluation via Debezium CDC.
-- Debezium watches these tables and sends CDC events to Kafka.
--
-- ARCHITECTURE:
--   MySQL (evaluation_requests) → Debezium → Kafka → Consumer → API → Results
--
-- PRIMITIVES ONLY:
--   - No pre-computed features
--   - Features fetched from Feast by API
--
-- Usage:
--   docker exec -i exp-mysql mysql -uroot -prootpassword demo < scripts/init-evaluation-mysql.sql
-- ═══════════════════════════════════════════════════════════════════════════════

USE demo;

-- ═══════════════════════════════════════════════════════════════════════════════
-- EVALUATION REQUESTS TABLE (PRIMITIVES ONLY)
-- ═══════════════════════════════════════════════════════════════════════════════

DROP TABLE IF EXISTS evaluation_requests;

CREATE TABLE evaluation_requests (
    -- Identifiers
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    request_id VARCHAR(64) NOT NULL,
    transaction_id VARCHAR(64) NOT NULL,
    customer_id VARCHAR(32) NOT NULL,
    
    -- PRIMITIVES ONLY (features fetched from Feast by API)
    amount DECIMAL(15, 2) NOT NULL,
    quantity INT DEFAULT 1,
    country VARCHAR(64) DEFAULT 'US',
    device_type VARCHAR(32) DEFAULT 'desktop',
    payment_method VARCHAR(32) DEFAULT 'credit_card',
    category VARCHAR(64) DEFAULT 'Other',
    
    -- Ground truth for evaluation (nullable if unknown)
    actual_label TINYINT DEFAULT NULL,
    
    -- Metadata
    request_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id VARCHAR(64) DEFAULT NULL,
    source VARCHAR(32) DEFAULT 'manual',
    processed TINYINT DEFAULT 0,
    
    -- Indexes for queries
    INDEX idx_customer_id (customer_id),
    INDEX idx_transaction_id (transaction_id),
    INDEX idx_request_timestamp (request_timestamp),
    INDEX idx_batch_id (batch_id),
    INDEX idx_processed (processed)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ═══════════════════════════════════════════════════════════════════════════════
-- EVALUATION RESULTS TABLE (for local storage, optional)
-- ═══════════════════════════════════════════════════════════════════════════════

DROP TABLE IF EXISTS evaluation_results;

CREATE TABLE evaluation_results (
    -- Identifiers
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    result_id VARCHAR(64) NOT NULL,
    request_id VARCHAR(64) NOT NULL,
    transaction_id VARCHAR(64) NOT NULL,
    customer_id VARCHAR(32) NOT NULL,
    
    -- Model info
    model_name VARCHAR(128),
    model_version VARCHAR(32),
    model_stage VARCHAR(32),
    run_id VARCHAR(64),
    
    -- Prediction
    fraud_probability DECIMAL(10, 6),
    fraud_prediction TINYINT,
    risk_level VARCHAR(16),
    
    -- Ground truth comparison
    actual_label TINYINT,
    is_correct TINYINT,
    
    -- Performance metrics
    feature_latency_ms DECIMAL(10, 3),
    inference_latency_ms DECIMAL(10, 3),
    total_latency_ms DECIMAL(10, 3),
    feature_source VARCHAR(32),
    
    -- Timestamps
    request_timestamp TIMESTAMP,
    result_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Indexes
    INDEX idx_request_id (request_id),
    INDEX idx_model_stage (model_stage),
    INDEX idx_result_timestamp (result_timestamp),
    INDEX idx_is_correct (is_correct)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ═══════════════════════════════════════════════════════════════════════════════
-- SAMPLE DATA (for testing)
-- ═══════════════════════════════════════════════════════════════════════════════

-- Insert a few test records (PRIMITIVES ONLY)
INSERT INTO evaluation_requests 
    (request_id, transaction_id, customer_id, amount, quantity, country, device_type, payment_method, category, actual_label, batch_id, source)
VALUES
    (UUID(), 'EVAL-TEST-000001', 'CUST-000001', 1500.00, 1, 'US', 'mobile', 'credit_card', 'Electronics', 0, 'test_batch', 'init_script'),
    (UUID(), 'EVAL-TEST-000002', 'CUST-000002', 3500.00, 2, 'UK', 'desktop', 'wallet', 'Travel', 1, 'test_batch', 'init_script'),
    (UUID(), 'EVAL-TEST-000003', 'CUST-000003', 250.00, 1, 'CA', 'tablet', 'debit_card', 'Clothing', 0, 'test_batch', 'init_script'),
    (UUID(), 'EVAL-TEST-000004', 'CUST-000004', 4800.00, 3, 'AU', 'mobile', 'wallet', 'Electronics', 1, 'test_batch', 'init_script'),
    (UUID(), 'EVAL-TEST-000005', 'CUST-000005', 120.00, 1, 'US', 'desktop', 'credit_card', 'Food', 0, 'test_batch', 'init_script');

SELECT 'Evaluation tables created successfully!' AS status;
SELECT COUNT(*) AS sample_records FROM evaluation_requests;