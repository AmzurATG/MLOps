#!/usr/bin/env python3
"""
Generate synthetic fraud detection data for demo.
Creates 10,000 transactions with 95% legitimate, 5% fraud.
"""

import pymysql
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import random
import string

# Configuration
MYSQL_HOST = "exp-mysql"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "rootpassword"
MYSQL_DATABASE = "demo"
MYSQL_TABLE = "fraud_transactions"

# Data generation parameters
N_SAMPLES = 10000
FRAUD_RATE = 0.05  # 5% fraud


def generate_customer_id():
    """Generate random customer ID."""
    return f"CUST{random.randint(1, 1000):04d}"


def generate_transaction_data(n_samples=N_SAMPLES, fraud_rate=FRAUD_RATE):
    """Generate synthetic transaction data."""
    
    np.random.seed(42)
    
    # Determine fraud labels
    n_fraud = int(n_samples * fraud_rate)
    n_legit = n_samples - n_fraud
    
    data = []
    
    # Generate legitimate transactions
    for i in range(n_legit):
        customer_id = generate_customer_id()
        
        # Legitimate transactions: normal amounts, normal patterns
        transaction = {
            'transaction_id': f"TXN{i+1:08d}",
            'customer_id': customer_id,
            'transaction_amount': float(np.random.lognormal(4, 1.5)),  # ~$100 avg
            'merchant_category': random.choice(['groceries', 'gas', 'restaurant', 'retail', 'online']),
            'transaction_date': datetime.now() - timedelta(minutes=random.randint(0, 1440)),
            'device_used': random.choice(['mobile', 'desktop', 'pos']),
            'location_country': 'US',
            'customer_age': int(np.random.normal(35, 12)),
            'is_fraud': 0
        }
        data.append(transaction)
    
    # Generate fraudulent transactions
    for i in range(n_fraud):
        customer_id = generate_customer_id()
        
        # Fraudulent transactions: higher amounts, suspicious patterns
        transaction = {
            'transaction_id': f"TXN{n_legit + i + 1:08d}",
            'customer_id': customer_id,
            'transaction_amount': float(np.random.lognormal(6, 2)),  # ~$500 avg (higher!)
            'merchant_category': random.choice(['electronics', 'jewelry', 'online', 'international']),
            'transaction_date': datetime.now() - timedelta(minutes=random.randint(0, 1440)),
            'device_used': random.choice(['mobile', 'desktop', 'new_device']),
            'location_country': random.choice(['US', 'CN', 'RU', 'NG']),  # International
            'customer_age': int(np.random.normal(35, 12)),
            'is_fraud': 1  # FRAUD!
        }
        data.append(transaction)
    
    # Shuffle
    random.shuffle(data)
    
    return pd.DataFrame(data)


def create_table(connection):
    """Create transactions table if not exists."""
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {MYSQL_TABLE} (
        transaction_id VARCHAR(50) PRIMARY KEY,
        customer_id VARCHAR(50) NOT NULL,
        transaction_amount DECIMAL(10, 2) NOT NULL,
        merchant_category VARCHAR(50),
        transaction_date DATETIME NOT NULL,
        device_used VARCHAR(50),
        location_country VARCHAR(10),
        customer_age INT,
        is_fraud TINYINT(1) DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_customer (customer_id),
        INDEX idx_date (transaction_date),
        INDEX idx_fraud (is_fraud)
    );
    """
    
    with connection.cursor() as cursor:
        cursor.execute(create_table_sql)
    connection.commit()
    print(f"✅ Table {MYSQL_TABLE} created/verified")


def insert_data(connection, df):
    """Insert transaction data into MySQL."""
    
    insert_sql = f"""
    INSERT INTO {MYSQL_TABLE} 
    (transaction_id, customer_id, transaction_amount, merchant_category, 
     transaction_date, device_used, location_country, customer_age, is_fraud)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        transaction_amount = VALUES(transaction_amount),
        is_fraud = VALUES(is_fraud)
    """
    
    # Convert DataFrame to list of tuples
    records = df.to_records(index=False).tolist()
    
    with connection.cursor() as cursor:
        cursor.executemany(insert_sql, records)
    connection.commit()
    
    print(f"✅ Inserted {len(df)} transactions into {MYSQL_TABLE}")


def main():
    """Main execution."""
    
    print("=" * 60)
    print("Fraud Detection Sample Data Generator")
    print("=" * 60)
    
    # Generate data
    print(f"\n📊 Generating {N_SAMPLES} transactions ({FRAUD_RATE:.1%} fraud)...")
    df = generate_transaction_data(n_samples=N_SAMPLES, fraud_rate=FRAUD_RATE)
    
    print(f"✅ Generated {len(df)} transactions")
    print(f"   - Legitimate: {len(df[df['is_fraud'] == 0])}")
    print(f"   - Fraud: {len(df[df['is_fraud'] == 1])}")
    
    # Connect to MySQL
    print(f"\n🔌 Connecting to MySQL at {MYSQL_HOST}...")
    connection = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    
    print(f"✅ Connected to MySQL database: {MYSQL_DATABASE}")
    
    # Create table
    print(f"\n📋 Creating table {MYSQL_TABLE}...")
    create_table(connection)
    
    # Insert data
    print(f"\n💾 Inserting data into {MYSQL_TABLE}...")
    insert_data(connection, df)
    
    # Verify
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) as count FROM {MYSQL_TABLE}")
        result = cursor.fetchone()
        total_count = result['count']
        
        cursor.execute(f"SELECT COUNT(*) as count FROM {MYSQL_TABLE} WHERE is_fraud = 1")
        result = cursor.fetchone()
        fraud_count = result['count']
    
    print(f"\n✅ Data loaded successfully!")
    print(f"   Total transactions: {total_count}")
    print(f"   Fraudulent: {fraud_count} ({fraud_count/total_count:.1%})")
    
    connection.close()
    
    print("\n" + "=" * 60)
    print("✅ Sample data generation complete!")
    print("=" * 60)
    print("\nNext steps:")
    print("1. Debezium will capture these changes to Kafka")
    print("2. Flink will process streaming features")
    print("3. Features will be written to Redis")
    print("4. Feast will serve features for inference")


if __name__ == "__main__":
    main()