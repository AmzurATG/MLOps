#!/usr/bin/env python3
"""
Evaluation Data Generator
═══════════════════════════════════════════════════════════════════════════════

Generates evaluation requests with PRIMITIVES ONLY.
Features are fetched from Feast by the API during evaluation.

Usage:
    # Generate 1000 evaluation requests
    python generate_evaluation_data.py --count 1000 --fraud-rate 0.1
    
    # Generate from CSV file
    python generate_evaluation_data.py --from-csv evaluation_data.csv
    
    # Generate for specific customers
    python generate_evaluation_data.py --customers CUST-000001,CUST-000002 --count 100

Environment Variables:
    TRINO_HOST          Trino host (default: localhost)
    TRINO_PORT          Trino port (default: 18080)
    TRINO_USER          Trino user (default: admin)
"""

import os
import sys
import uuid
import random
import argparse
import logging
from datetime import datetime
from typing import List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "18080"))
TRINO_USER = os.getenv("TRINO_USER", "admin")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg_dev")

EVALUATION_TABLE = f"{TRINO_CATALOG}.evaluation.requests"

# Primitive value options
COUNTRIES = ["US", "UK", "CA", "AU", "DE", "FR", "JP", "BR", "IN", "MX", "Sydney, AU", "London, UK"]
DEVICE_TYPES = ["desktop", "mobile", "tablet"]
PAYMENT_METHODS = ["credit_card", "debit_card", "wallet", "bank_transfer"]
CATEGORIES = ["Electronics", "Clothing", "Home", "Food", "Travel", "Entertainment", "Sports", "Beauty"]


# ═══════════════════════════════════════════════════════════════════════════════
# TRINO CONNECTION
# ═══════════════════════════════════════════════════════════════════════════════

def get_trino_connection():
    """Get Trino connection."""
    try:
        from trino.dbapi import connect
        conn = connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG,
            schema="evaluation",
        )
        return conn
    except ImportError:
        logger.error("trino package not installed. Run: pip install trino")
        sys.exit(1)


def execute_query(conn, query: str) -> List:
    """Execute a query and return results."""
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()


def execute_ddl(conn, query: str):
    """Execute DDL statement."""
    cursor = conn.cursor()
    cursor.execute(query)


# ═══════════════════════════════════════════════════════════════════════════════
# DATA GENERATION
# ═══════════════════════════════════════════════════════════════════════════════

def get_customer_ids(conn, limit: int = 100) -> List[str]:
    """Get real customer IDs from Gold table (ensures Feast has features)."""
    query = f"""
    SELECT DISTINCT customer_id
    FROM {TRINO_CATALOG}.gold.fraud_training_data
    LIMIT {limit}
    """
    rows = execute_query(conn, query)
    return [row[0] for row in rows] if rows else []


def generate_random_request(
    customer_id: str,
    batch_id: str,
    fraud_rate: float = 0.1,
    index: int = 0,
) -> dict:
    """Generate a single random evaluation request."""
    now = datetime.now()
    
    # Random primitives
    amount = round(random.uniform(10, 5000), 2)
    quantity = random.randint(1, 5)
    country = random.choice(COUNTRIES)
    device_type = random.choice(DEVICE_TYPES)
    payment_method = random.choice(PAYMENT_METHODS)
    category = random.choice(CATEGORIES)
    
    # Simulated ground truth with configurable fraud rate
    # Increase fraud probability for risky patterns
    fraud_prob = fraud_rate
    if amount > 2000:
        fraud_prob += 0.15
    if payment_method == "wallet":
        fraud_prob += 0.1
    if device_type == "mobile" and amount > 1000:
        fraud_prob += 0.05
        
    actual_label = 1 if random.random() < min(fraud_prob, 0.9) else 0
    
    return {
        "request_id": str(uuid.uuid4()),
        "transaction_id": f"EVAL-{now.strftime('%Y%m%d%H%M%S')}-{index:06d}",
        "customer_id": customer_id,
        "amount": amount,
        "quantity": quantity,
        "country": country,
        "device_type": device_type,
        "payment_method": payment_method,
        "category": category,
        "actual_label": actual_label,
        "request_timestamp": now.strftime('%Y-%m-%d %H:%M:%S'),
        "request_date": now.strftime('%Y-%m-%d'),
        "batch_id": batch_id,
        "source": "script",
    }


def insert_requests(conn, requests: List[dict], batch_size: int = 500):
    """Insert evaluation requests into Trino."""
    total = len(requests)
    inserted = 0
    
    for i in range(0, total, batch_size):
        batch = requests[i:i + batch_size]
        
        values = []
        for req in batch:
            values.append(f"""(
                '{req["request_id"]}',
                '{req["transaction_id"]}',
                '{req["customer_id"]}',
                {req["amount"]},
                {req["quantity"]},
                '{req["country"]}',
                '{req["device_type"]}',
                '{req["payment_method"]}',
                '{req["category"]}',
                {req["actual_label"]},
                TIMESTAMP '{req["request_timestamp"]}',
                DATE '{req["request_date"]}',
                '{req["batch_id"]}',
                '{req["source"]}'
            )""")
        
        insert_sql = f"""
        INSERT INTO {EVALUATION_TABLE} (
            request_id, transaction_id, customer_id,
            amount, quantity, country, device_type, payment_method, category,
            actual_label, request_timestamp, request_date, batch_id, source
        ) VALUES {', '.join(values)}
        """
        
        execute_ddl(conn, insert_sql)
        inserted += len(batch)
        logger.info(f"  Inserted {inserted}/{total} requests")
    
    return inserted


def load_from_csv(filepath: str) -> List[dict]:
    """Load evaluation data from CSV file."""
    import pandas as pd
    
    df = pd.read_csv(filepath)
    
    required_cols = ["customer_id", "amount"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"CSV missing required column: {col}")
    
    now = datetime.now()
    batch_id = f"csv_{now.strftime('%Y%m%d_%H%M%S')}"
    
    requests = []
    for idx, row in df.iterrows():
        req = {
            "request_id": str(uuid.uuid4()),
            "transaction_id": row.get("transaction_id", f"CSV-{idx:06d}"),
            "customer_id": row["customer_id"],
            "amount": float(row["amount"]),
            "quantity": int(row.get("quantity", 1)),
            "country": row.get("country", "US"),
            "device_type": row.get("device_type", "desktop"),
            "payment_method": row.get("payment_method", "credit_card"),
            "category": row.get("category", "Other"),
            "actual_label": int(row.get("actual_label", row.get("is_fraud", 0))),
            "request_timestamp": now.strftime('%Y-%m-%d %H:%M:%S'),
            "request_date": now.strftime('%Y-%m-%d'),
            "batch_id": batch_id,
            "source": "csv",
        }
        requests.append(req)
    
    return requests


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Generate evaluation requests (PRIMITIVES ONLY)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Generate 1000 random requests
    python generate_evaluation_data.py --count 1000

    # Generate with specific fraud rate
    python generate_evaluation_data.py --count 500 --fraud-rate 0.2

    # Load from CSV
    python generate_evaluation_data.py --from-csv test_data.csv

    # Use specific customers
    python generate_evaluation_data.py --customers CUST-000001,CUST-000002
        """
    )
    
    parser.add_argument("--count", type=int, default=1000, help="Number of requests to generate")
    parser.add_argument("--fraud-rate", type=float, default=0.1, help="Base fraud rate (0.0-1.0)")
    parser.add_argument("--batch-id", type=str, help="Custom batch ID")
    parser.add_argument("--customers", type=str, help="Comma-separated customer IDs")
    parser.add_argument("--from-csv", type=str, help="Load data from CSV file")
    parser.add_argument("--dry-run", action="store_true", help="Don't insert, just show what would be generated")
    
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("EVALUATION DATA GENERATOR (PRIMITIVES ONLY)")
    logger.info("=" * 60)
    logger.info(f"Trino: {TRINO_HOST}:{TRINO_PORT}")
    logger.info(f"Table: {EVALUATION_TABLE}")
    
    # Connect to Trino
    if not args.dry_run:
        conn = get_trino_connection()
        logger.info("✓ Connected to Trino")
    else:
        conn = None
        logger.info("DRY RUN - No data will be inserted")
    
    # Generate or load data
    if args.from_csv:
        logger.info(f"Loading from CSV: {args.from_csv}")
        requests = load_from_csv(args.from_csv)
    else:
        # Get customer IDs
        if args.customers:
            customer_ids = args.customers.split(",")
            logger.info(f"Using specified customers: {customer_ids}")
        elif conn:
            customer_ids = get_customer_ids(conn)
            logger.info(f"Loaded {len(customer_ids)} customer IDs from Gold table")
        else:
            customer_ids = [f"CUST-{i:06d}" for i in range(10)]
            logger.info(f"Using default customer IDs (dry run)")
        
        if not customer_ids:
            logger.error("No customer IDs available. Check Gold table or specify --customers")
            sys.exit(1)
        
        # Generate requests
        batch_id = args.batch_id or f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Generating {args.count} requests (fraud_rate={args.fraud_rate}, batch_id={batch_id})")
        
        requests = []
        for i in range(args.count):
            customer_id = random.choice(customer_ids)
            req = generate_random_request(
                customer_id=customer_id,
                batch_id=batch_id,
                fraud_rate=args.fraud_rate,
                index=i,
            )
            requests.append(req)
    
    # Summary
    fraud_count = sum(1 for r in requests if r["actual_label"] == 1)
    logger.info(f"Generated {len(requests)} requests ({fraud_count} frauds, {len(requests) - fraud_count} non-frauds)")
    
    # Show sample
    logger.info("\nSample request (PRIMITIVES ONLY):")
    sample = requests[0]
    for key, value in sample.items():
        logger.info(f"  {key}: {value}")
    
    # Insert
    if not args.dry_run and conn:
        logger.info(f"\nInserting into {EVALUATION_TABLE}...")
        inserted = insert_requests(conn, requests)
        logger.info(f"✓ Inserted {inserted} evaluation requests")
        
        # Verify
        count_result = execute_query(conn, f"SELECT COUNT(*) FROM {EVALUATION_TABLE}")
        total_count = count_result[0][0] if count_result else 0
        logger.info(f"✓ Total requests in table: {total_count}")
        
        conn.close()
    
    logger.info("\n" + "=" * 60)
    logger.info("NEXT STEPS:")
    logger.info("=" * 60)
    logger.info("1. Run batch evaluation in Dagster:")
    logger.info("   dagster job execute -j batch_evaluation_job")
    logger.info("")
    logger.info("2. Or via API directly:")
    logger.info("   curl -X POST http://localhost:18002/evaluate \\")
    logger.info("     -H 'Content-Type: application/json' \\")
    logger.info("     -d '{\"transaction\": {...}, \"model_stages\": [\"Production\"]}'")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()