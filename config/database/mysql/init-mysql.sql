-- =============================================================================
-- Fraud Detection Demo - MySQL Source Data
-- Base features for fraud detection pipeline
-- UPDATED: Dynamic dates ending TODAY for fresh 30d aggregates
-- =============================================================================

USE demo;

-- Create fraud_transactions table with BASE FEATURES only
-- Built features computed in Jupyter feature engineering step
DROP TABLE IF EXISTS fraud_transactions;

CREATE TABLE fraud_transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    transaction_amount DECIMAL(12, 2) NOT NULL,
    transaction_date DATETIME NOT NULL,
    payment_method VARCHAR(50),
    product_category VARCHAR(50),
    quantity INT DEFAULT 1,
    customer_age INT,
    customer_location VARCHAR(100),
    device_used VARCHAR(50),
    ip_address VARCHAR(50),
    shipping_address VARCHAR(200),
    billing_address VARCHAR(200),
    is_fraudulent TINYINT DEFAULT 0 COMMENT '1=Fraud, 0=Legit',
    account_age_days INT,
    transaction_hour INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_customer (customer_id),
    INDEX idx_date (transaction_date),
    INDEX idx_fraud (is_fraudulent)
) ENGINE=InnoDB;

-- =============================================================================
-- STORED PROCEDURE: Generate Synthetic Fraud Data
-- Mimics the Python generator logic with risk factors
-- UPDATED: Uses dynamic dates ending TODAY
-- =============================================================================

DROP PROCEDURE IF EXISTS generate_fraud_data;

DELIMITER //

CREATE PROCEDURE generate_fraud_data(IN num_customers INT, IN txns_per_customer INT)
BEGIN
    DECLARE i INT DEFAULT 0;
    DECLARE j INT DEFAULT 0;
    DECLARE cust_id VARCHAR(50);
    DECLARE tx_id VARCHAR(50);
    DECLARE end_date DATETIME;
    DECLARE start_date DATETIME;
    DECLARE tx_date DATETIME;
    DECLARE tx_amount DECIMAL(12, 2);
    DECLARE payment VARCHAR(50);
    DECLARE category VARCHAR(50);
    DECLARE qty INT;
    DECLARE age INT;
    DECLARE home_country VARCHAR(10);
    DECLARE home_city VARCHAR(50);
    DECLARE ship_country VARCHAR(10);
    DECLARE ship_city VARCHAR(50);
    DECLARE device VARCHAR(50);
    DECLARE ip VARCHAR(50);
    DECLARE acct_age INT;
    DECLARE tx_hour INT;
    DECLARE is_fraud TINYINT;
    DECLARE risk_score DECIMAL(8,4);
    DECLARE typical_amount DECIMAL(12,2);
    DECLARE days_span INT DEFAULT 180;
    
    -- Risk factors
    DECLARE is_high_amount TINYINT;
    DECLARE is_new_account TINYINT;
    DECLARE is_very_new TINYINT;
    DECLARE address_mismatch TINYINT;
    DECLARE high_risk_ship TINYINT;
    DECLARE is_weird_hour TINYINT;
    DECLARE risky_pay TINYINT;
    DECLARE risky_cat TINYINT;
    
    -- Dynamic dates: END = NOW, START = 6 months ago
    SET end_date = NOW();
    SET start_date = DATE_SUB(end_date, INTERVAL days_span DAY);
    
    -- Countries
    SET @countries = 'US,GB,DE,FR,IN,SG,AU,CA,BR,ZA';
    SET @high_risk = 'NG,PK,RU,BR';
    SET @cities = 'New York,London,Berlin,Paris,Hyderabad,Singapore,Sydney,Toronto,Sao Paulo,Johannesburg,Lagos,Karachi,Moscow';
    SET @payments = 'credit_card,debit_card,wallet,bank_transfer,cod';
    SET @risky_payments = 'credit_card,wallet';
    SET @categories = 'Electronics,Fashion,Grocery,Home,Luxury,Digital';
    SET @risky_cats = 'Electronics,Luxury,Digital';
    SET @devices = 'mobile,desktop,tablet';
    
    WHILE i < num_customers DO
        SET cust_id = CONCAT('CUST-', LPAD(i, 6, '0'));
        SET age = FLOOR(18 + RAND() * 50);
        SET typical_amount = 500 + RAND() * 3000;
        
        -- Pick home country (mostly safe)
        SET home_country = ELT(FLOOR(1 + RAND() * 10), 'US','GB','DE','FR','IN','SG','AU','CA','BR','ZA');
        SET home_city = CASE home_country
            WHEN 'US' THEN 'New York'
            WHEN 'GB' THEN 'London'
            WHEN 'DE' THEN 'Berlin'
            WHEN 'FR' THEN 'Paris'
            WHEN 'IN' THEN 'Hyderabad'
            WHEN 'SG' THEN 'Singapore'
            WHEN 'AU' THEN 'Sydney'
            WHEN 'CA' THEN 'Toronto'
            WHEN 'BR' THEN 'Sao Paulo'
            WHEN 'ZA' THEN 'Johannesburg'
            ELSE 'City'
        END;
        
        -- Account age: mix of new and established
        IF RAND() < 0.3 THEN
            SET acct_age = FLOOR(RAND() * 14);  -- New accounts (0-14 days)
        ELSE
            SET acct_age = FLOOR(14 + RAND() * 350);  -- Established accounts
        END IF;
        
        SET device = ELT(FLOOR(1 + RAND() * 3), 'mobile', 'desktop', 'tablet');
        SET ip = CONCAT(FLOOR(1 + RAND() * 254), '.', FLOOR(1 + RAND() * 254), '.', FLOOR(1 + RAND() * 254), '.', FLOOR(1 + RAND() * 254));
        
        SET j = 0;
        WHILE j < txns_per_customer DO
            SET tx_id = CONCAT('TX-', LPAD(i, 6, '0'), '-', LPAD(j, 4, '0'));
            
            -- Distribute transactions across time range, with more recent bias
            IF RAND() < 0.4 THEN
                -- 40% of transactions in last 30 days (for fresh aggregates!)
                SET tx_date = DATE_SUB(end_date, INTERVAL FLOOR(RAND() * 30) DAY);
            ELSE
                -- 60% distributed across full 6 months
                SET tx_date = DATE_ADD(start_date, INTERVAL FLOOR(RAND() * days_span) DAY);
            END IF;
            SET tx_date = DATE_ADD(tx_date, INTERVAL FLOOR(RAND() * 86400) SECOND);
            SET tx_hour = HOUR(tx_date);
            
            -- Amount: occasional spikes
            IF RAND() < 0.15 THEN
                SET tx_amount = typical_amount * (3 + RAND() * 4);
            ELSE
                SET tx_amount = typical_amount * (0.5 + RAND());
            END IF;
            SET tx_amount = ROUND(GREATEST(50, LEAST(tx_amount, 50000)), 2);
            
            SET payment = ELT(FLOOR(1 + RAND() * 5), 'credit_card', 'debit_card', 'wallet', 'bank_transfer', 'cod');
            SET category = ELT(FLOOR(1 + RAND() * 6), 'Electronics', 'Fashion', 'Grocery', 'Home', 'Luxury', 'Digital');
            SET qty = FLOOR(1 + RAND() * 5);
            
            -- Shipping country: mostly home, sometimes different
            IF RAND() < 0.85 THEN
                SET ship_country = home_country;
                SET ship_city = home_city;
            ELSE
                -- Sometimes high-risk
                IF RAND() < 0.3 THEN
                    SET ship_country = ELT(FLOOR(1 + RAND() * 4), 'NG', 'PK', 'RU', 'BR');
                ELSE
                    SET ship_country = ELT(FLOOR(1 + RAND() * 10), 'US','GB','DE','FR','IN','SG','AU','CA','BR','ZA');
                END IF;
                SET ship_city = CASE ship_country
                    WHEN 'NG' THEN 'Lagos'
                    WHEN 'PK' THEN 'Karachi'
                    WHEN 'RU' THEN 'Moscow'
                    WHEN 'US' THEN 'New York'
                    WHEN 'GB' THEN 'London'
                    WHEN 'DE' THEN 'Berlin'
                    WHEN 'FR' THEN 'Paris'
                    WHEN 'IN' THEN 'Hyderabad'
                    WHEN 'SG' THEN 'Singapore'
                    WHEN 'AU' THEN 'Sydney'
                    WHEN 'CA' THEN 'Toronto'
                    WHEN 'BR' THEN 'Sao Paulo'
                    WHEN 'ZA' THEN 'Johannesburg'
                    ELSE 'City'
                END;
            END IF;
            
            -- Calculate risk factors
            SET is_high_amount = IF(tx_amount > typical_amount * 3, 1, 0);
            SET is_new_account = IF(acct_age < 7, 1, 0);
            SET is_very_new = IF(acct_age < 2, 1, 0);
            SET address_mismatch = IF(ship_country != home_country, 1, 0);
            SET high_risk_ship = IF(FIND_IN_SET(ship_country, 'NG,PK,RU') > 0, 1, 0);
            SET is_weird_hour = IF(tx_hour >= 22 OR tx_hour <= 5, 1, 0);
            SET risky_pay = IF(FIND_IN_SET(payment, 'credit_card,wallet') > 0, 1, 0);
            SET risky_cat = IF(FIND_IN_SET(category, 'Electronics,Luxury,Digital') > 0, 1, 0);
            
            -- Risk score -> fraud probability (logistic-like)
            SET risk_score = -2.8 
                + 2.2 * is_high_amount
                + 1.5 * is_new_account
                + 2.0 * is_very_new
                + 2.0 * address_mismatch
                + 1.8 * high_risk_ship
                + 1.0 * is_weird_hour
                + 0.6 * risky_pay
                + 0.5 * risky_cat
                + (RAND() - 0.5) * 0.4;
            
            -- Sigmoid approximation for fraud decision
            SET is_fraud = IF(RAND() < (1 / (1 + EXP(-risk_score))), 1, 0);
            
            -- Insert transaction
            INSERT INTO fraud_transactions (
                transaction_id, customer_id, transaction_amount, transaction_date,
                payment_method, product_category, quantity, customer_age,
                customer_location, device_used, ip_address,
                shipping_address, billing_address,
                is_fraudulent, account_age_days, transaction_hour
            ) VALUES (
                tx_id, cust_id, tx_amount, tx_date,
                payment, category, qty, age + FLOOR(j/12),
                CONCAT(home_city, ', ', home_country),
                device,
                ip,
                CONCAT(ship_city, ', ', ship_country),
                CONCAT(home_city, ', ', home_country),
                is_fraud, acct_age + j, tx_hour
            );
            
            SET j = j + 1;
        END WHILE;
        
        SET i = i + 1;
    END WHILE;
    
    SELECT CONCAT('Generated ', i * txns_per_customer, ' transactions for ', i, ' customers') AS result;
END //

DELIMITER ;

-- =============================================================================
-- Generate initial dataset: 500 customers x 20 transactions = 10,000 records
-- =============================================================================

CALL generate_fraud_data(500, 20);

-- Show summary statistics
SELECT 
    COUNT(*) as total_transactions,
    SUM(is_fraudulent) as fraud_count,
    ROUND(AVG(is_fraudulent) * 100, 2) as fraud_rate_pct,
    ROUND(AVG(transaction_amount), 2) as avg_amount,
    COUNT(DISTINCT customer_id) as unique_customers,
    MIN(transaction_date) as earliest_date,
    MAX(transaction_date) as latest_date
FROM fraud_transactions;

-- Show last 30 days stats (important for fresh aggregates!)
SELECT 
    COUNT(*) as last_30d_transactions,
    SUM(is_fraudulent) as last_30d_fraud,
    ROUND(AVG(is_fraudulent) * 100, 2) as last_30d_fraud_rate
FROM fraud_transactions
WHERE transaction_date >= DATE_SUB(NOW(), INTERVAL 30 DAY);

-- Show sample data
SELECT * FROM fraud_transactions ORDER BY transaction_date DESC LIMIT 10;
