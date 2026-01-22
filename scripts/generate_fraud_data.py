#!/usr/bin/env python
"""
Generate synthetic fraud transaction data and load into MySQL.
Supports multiple modes for demo/testing:
  - historical: 6 months of data ending today
  - recent: Last 30 days only (for fresh aggregates)
  - incremental: Add new transactions to existing data
  - realtime: Generate transactions for "today" only (streaming simulation)
"""
import os
import math
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

import numpy as np
import pandas as pd

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

N_CUSTOMERS = int(os.getenv("FRAUD_N_CUSTOMERS", "10"))
MIN_TX_PER_CUSTOMER = int(os.getenv("FRAUD_MIN_TX", "10"))
MAX_TX_PER_CUSTOMER = int(os.getenv("FRAUD_MAX_TX", "300"))

# Data generation mode
# - historical: 6 months of data ending today
# - recent: last 30 days only  
# - incremental: add to existing data
# - realtime: today only (for streaming demo)
DATA_MODE = os.getenv("FRAUD_DATA_MODE", "historical").lower()

# Time configuration
HISTORICAL_DAYS = int(os.getenv("FRAUD_HISTORICAL_DAYS", "180"))
RECENT_DAYS = int(os.getenv("FRAUD_RECENT_DAYS", "30"))

# Dynamic base date - ends TODAY
END_DATE = datetime.now().replace(hour=23, minute=59, second=59)

RUN_ID = os.getenv("FRAUD_RUN_ID") or datetime.utcnow().strftime("%Y%m%d%H%M%S")

# Risk factors
COUNTRIES = ["US", "GB", "DE", "FR", "IN", "SG", "AU", "CA", "BR", "ZA"]
HIGH_RISK_COUNTRIES = ["NG", "PK", "RU", "BR"]
PAYMENT_METHODS = ["credit_card", "debit_card", "wallet", "bank_transfer", "cod"]
RISKY_PAYMENT_METHODS = ["credit_card", "wallet"]
PRODUCT_CATEGORIES = ["Electronics", "Fashion", "Grocery", "Home", "Luxury", "Digital"]
RISKY_CATEGORIES = ["Electronics", "Luxury", "Digital"]
DEVICES = ["mobile", "desktop", "tablet"]

CITY_BY_COUNTRY = {
    "US": "New York", "GB": "London", "DE": "Berlin", "FR": "Paris",
    "IN": "Hyderabad", "SG": "Singapore", "AU": "Sydney", "CA": "Toronto",
    "BR": "Sao Paulo", "ZA": "Johannesburg", "NG": "Lagos", "PK": "Karachi", "RU": "Moscow",
}

random.seed(42)
np.random.seed(42)


def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def random_date(start: datetime, end: datetime) -> datetime:
    """Generate random datetime between start and end."""
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)


def random_ip() -> str:
    return ".".join(str(random.randint(1, 254)) for _ in range(4))


def get_date_range(mode: str) -> tuple:
    """Get start and end dates based on mode."""
    if mode == "historical":
        # 6 months ending today
        start = END_DATE - timedelta(days=HISTORICAL_DAYS)
        end = END_DATE
    elif mode == "recent":
        # Last 30 days
        start = END_DATE - timedelta(days=RECENT_DAYS)
        end = END_DATE
    elif mode == "realtime":
        # Today only
        start = END_DATE.replace(hour=0, minute=0, second=0)
        end = END_DATE
    else:  # incremental - last 7 days
        start = END_DATE - timedelta(days=7)
        end = END_DATE
    
    return start, end


def generate_customers(n_customers: int, start_date: datetime) -> pd.DataFrame:
    """Generate customer profiles."""
    rows = []
    for i in range(n_customers):
        cust_id = f"CUST-{i:06d}"
        country = random.choice(COUNTRIES)
        city = CITY_BY_COUNTRY.get(country, "City")
        age = int(np.clip(np.random.normal(35, 10), 18, 80))
        
        # Signup date: some before start, some recent
        if random.random() < 0.7:
            # Existing customer - signed up before data window
            signup_offset_days = random.randint(30, 365)
            signup_date = start_date - timedelta(days=signup_offset_days)
        else:
            # New customer - signed up within data window
            signup_offset_days = random.randint(0, 14)
            signup_date = start_date + timedelta(days=signup_offset_days)
        
        typical_amount = float(np.clip(np.random.normal(2000, 1500), 200, 20000))
        preferred_device = random.choice(DEVICES)
        
        rows.append({
            "Customer ID": cust_id,
            "Home Country": country,
            "Home City": city,
            "Customer Age Base": age,
            "Signup Date": signup_date,
            "Typical Amount": typical_amount,
            "Preferred Device": preferred_device,
        })
    return pd.DataFrame(rows)


def generate_transactions_for_customer(
    profile: pd.Series, 
    start_date: datetime, 
    end_date: datetime,
    min_tx: int = None,
    max_tx: int = None,
) -> List[Dict[str, Any]]:
    """Generate transactions for a single customer within date range."""
    
    cust_id = profile["Customer ID"]
    country = profile["Home Country"]
    city = profile["Home City"]
    age_base = profile["Customer Age Base"]
    signup_date = profile["Signup Date"]
    typical_amount = profile["Typical Amount"]
    preferred_device = profile["Preferred Device"]
    
    min_tx = min_tx or MIN_TX_PER_CUSTOMER
    max_tx = max_tx or MAX_TX_PER_CUSTOMER
    
    # Adjust transaction count based on date range
    days_span = (end_date - start_date).days
    if days_span < 30:
        # Fewer transactions for shorter periods
        n_tx = random.randint(max(1, min_tx // 3), max(2, max_tx // 3))
    else:
        n_tx = random.randint(min_tx, max_tx)
    
    # Generate sorted transaction dates
    tx_dates = sorted(random_date(start_date, end_date) for _ in range(n_tx))
    
    rows = []
    last_tx_date = None
    ip_base = random_ip()
    
    for j, tx_dt in enumerate(tx_dates):
        tx_id = f"TX-{RUN_ID}-{cust_id.split('-')[1]}-{j:04d}"
        tx_hour = tx_dt.hour
        
        # Amount: occasional spikes (more likely for fraud)
        if random.random() < 0.15:
            amount = float(np.clip(
                np.random.normal(typical_amount * 5, typical_amount * 2), 
                500, 100000
            ))
        else:
            amount = float(np.clip(
                np.random.normal(typical_amount, typical_amount * 0.5), 
                50, 20000
            ))
        
        quantity = int(np.clip(np.random.poisson(2), 1, 10))
        days_since_start = (tx_dt - start_date).days
        customer_age = int(age_base + days_since_start / 365.0 + np.random.normal(0, 1))
        account_age_days = max((tx_dt - signup_date).days, 0)
        
        payment_method = random.choice(PAYMENT_METHODS)
        product_category = random.choice(PRODUCT_CATEGORIES)
        device_used = preferred_device if random.random() < 0.7 else random.choice(DEVICES)
        
        # Address logic
        billing_country = country
        if random.random() < 0.85:
            shipping_country = country
        else:
            # Sometimes different, occasionally high-risk
            if random.random() < 0.3:
                shipping_country = random.choice(HIGH_RISK_COUNTRIES)
            else:
                shipping_country = random.choice(COUNTRIES)
        
        billing_city = CITY_BY_COUNTRY.get(billing_country, "City")
        shipping_city = CITY_BY_COUNTRY.get(shipping_country, "City")
        
        billing_address = f"{billing_city}, {billing_country}"
        shipping_address = f"{shipping_city}, {shipping_country}"
        customer_location = f"{billing_city}, {billing_country}"
        
        # IP address (slight variation)
        last_octet = random.randint(1, 254)
        ip_address = ".".join(ip_base.split(".")[:3] + [str(last_octet)])
        
        # Gap since last transaction
        if last_tx_date is None:
            gap_days = 9999.0
        else:
            gap_days = (tx_dt - last_tx_date).total_seconds() / 86400.0
        last_tx_date = tx_dt
        
        # ═══════════════════════════════════════════════════════════════════
        # RISK FACTORS → FRAUD PROBABILITY
        # ═══════════════════════════════════════════════════════════════════
        
        is_new_customer = 1 if account_age_days < 7 else 0
        is_very_new = 1 if account_age_days < 2 else 0
        is_high_amount = 1 if amount > typical_amount * 3 else 0
        is_weird_hour = 1 if tx_hour >= 22 or tx_hour <= 5 else 0
        high_risk_shipping = 1 if shipping_country in HIGH_RISK_COUNTRIES else 0
        address_mismatch = 1 if shipping_country != billing_country else 0
        high_velocity = 1 if gap_days < 0.25 else 0  # < 6 hours
        risky_payment = 1 if payment_method in RISKY_PAYMENT_METHODS else 0
        risky_category = 1 if product_category in RISKY_CATEGORIES else 0
        
        # Logistic regression-style risk score
        risk_score = (
            -2.8  # baseline (low fraud rate)
            + 2.2 * is_high_amount
            + 1.5 * is_new_customer
            + 2.0 * is_very_new
            + 2.0 * address_mismatch
            + 1.8 * high_risk_shipping
            + 1.0 * is_weird_hour
            + 0.8 * high_velocity
            + 0.6 * risky_payment
            + 0.5 * risky_category
            + np.random.normal(0, 0.2)  # noise
        )
        
        p_fraud = sigmoid(risk_score)
        is_fraud = 1 if random.random() < p_fraud else 0
        
        rows.append({
            "transaction_id": tx_id,
            "customer_id": cust_id,
            "transaction_amount": round(amount, 2),
            "transaction_date": tx_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "payment_method": payment_method,
            "product_category": product_category,
            "quantity": quantity,
            "customer_age": customer_age,
            "customer_location": customer_location,
            "device_used": device_used,
            "ip_address": ip_address,
            "shipping_address": shipping_address,
            "billing_address": billing_address,
            "is_fraudulent": is_fraud,
            "account_age_days": account_age_days,
            "transaction_hour": tx_hour,
        })
    
    return rows


def generate_fraud_data(
    n_customers: int = None,
    mode: str = None,
    start_date: datetime = None,
    end_date: datetime = None,
) -> pd.DataFrame:
    """
    Generate full fraud dataset.
    
    Args:
        n_customers: Number of customers (default from env)
        mode: historical, recent, realtime, incremental
        start_date: Override start date
        end_date: Override end date
    """
    n_customers = n_customers or N_CUSTOMERS
    mode = mode or DATA_MODE
    
    if start_date is None or end_date is None:
        start_date, end_date = get_date_range(mode)
    
    print(f"[FraudGen] Mode: {mode}")
    print(f"[FraudGen] Date range: {start_date.date()} to {end_date.date()}")
    print(f"[FraudGen] Customers: {n_customers}")
    
    customers = generate_customers(n_customers, start_date)
    all_rows: List[Dict[str, Any]] = []
    
    for _, profile in customers.iterrows():
        rows = generate_transactions_for_customer(profile, start_date, end_date)
        all_rows.extend(rows)
    
    df = pd.DataFrame(all_rows)
    df = df.sort_values(["customer_id", "transaction_date"]).reset_index(drop=True)
    
    return df


def load_to_mysql(
    df: pd.DataFrame, 
    host: str = None, 
    port: int = None,
    mode: str = None,
) -> int:
    """
    Load DataFrame to MySQL fraud_transactions table.
    
    Args:
        df: Transaction data
        host: MySQL host
        port: MySQL port
        mode: 'replace' (delete all first) or 'append' (add to existing)
    """
    import pymysql
    
    host = host or os.getenv("MYSQL_HOST", "localhost")
    port = port or int(os.getenv("MYSQL_PORT", "13307"))
    user = os.getenv("MYSQL_USER", "root")
    password = os.getenv("MYSQL_PASSWORD", "rootpassword")
    database = os.getenv("MYSQL_DATABASE", "demo")
    mode = mode or os.getenv("FRAUD_LOAD_MODE", "replace").lower()
    
    print(f"[FraudGen] Connecting to MySQL: {host}:{port}/{database}")
    
    conn = pymysql.connect(
        host=host, 
        port=port, 
        user=user, 
        password=password, 
        database=database
    )
    cursor = conn.cursor()
    
    if mode == "replace":
        print("[FraudGen] Mode: REPLACE (deleting existing data)")
        cursor.execute("DELETE FROM fraud_transactions")
    elif mode == "append":
        print("[FraudGen] Mode: APPEND (keeping existing data)")
    elif mode == "upsert":
        print("[FraudGen] Mode: UPSERT (update existing, insert new)")
        # For upsert, we'll use INSERT ... ON DUPLICATE KEY UPDATE
    else:
        raise ValueError("FRAUD_LOAD_MODE must be 'replace', 'append', or 'upsert'")
    
    # Insert SQL
    if mode == "upsert":
        insert_sql = """
        INSERT INTO fraud_transactions (
            transaction_id, customer_id, transaction_amount, transaction_date,
            payment_method, product_category, quantity, customer_age,
            customer_location, device_used, ip_address,
            shipping_address, billing_address,
            is_fraudulent, account_age_days, transaction_hour
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            transaction_amount = VALUES(transaction_amount),
            is_fraudulent = VALUES(is_fraudulent),
            updated_at = CURRENT_TIMESTAMP
        """
    else:
        insert_sql = """
        INSERT INTO fraud_transactions (
            transaction_id, customer_id, transaction_amount, transaction_date,
            payment_method, product_category, quantity, customer_age,
            customer_location, device_used, ip_address,
            shipping_address, billing_address,
            is_fraudulent, account_age_days, transaction_hour
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
    
    batch_size = 1000
    inserted = 0
    
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]
        values = [
            (
                row['transaction_id'], row['customer_id'], row['transaction_amount'],
                row['transaction_date'], row['payment_method'], row['product_category'],
                row['quantity'], row['customer_age'], row['customer_location'],
                row['device_used'], row['ip_address'], row['shipping_address'],
                row['billing_address'], row['is_fraudulent'], row['account_age_days'],
                row['transaction_hour']
            )
            for _, row in batch.iterrows()
        ]
        cursor.executemany(insert_sql, values)
        inserted += len(values)
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return inserted


def print_stats(df: pd.DataFrame):
    """Print dataset statistics."""
    print("\n" + "═" * 60)
    print("DATASET STATISTICS")
    print("═" * 60)
    print(f"Total transactions: {len(df):,}")
    print(f"Unique customers: {df['customer_id'].nunique():,}")
    print(f"Date range: {df['transaction_date'].min()} to {df['transaction_date'].max()}")
    print(f"Fraud rate: {df['is_fraudulent'].mean():.2%}")
    print(f"Avg amount: ${df['transaction_amount'].mean():,.2f}")
    print(f"Max amount: ${df['transaction_amount'].max():,.2f}")
    
    # Recent data stats (important for aggregates)
    df['tx_date'] = pd.to_datetime(df['transaction_date'])
    cutoff_30d = datetime.now() - timedelta(days=30)
    recent = df[df['tx_date'] >= cutoff_30d]
    print(f"\nLast 30 days:")
    print(f"  Transactions: {len(recent):,}")
    print(f"  Fraud rate: {recent['is_fraudulent'].mean():.2%}" if len(recent) > 0 else "  No data")
    print("═" * 60 + "\n")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate fraud transaction data")
    parser.add_argument("--customers", "-n", type=int, default=N_CUSTOMERS,
                       help="Number of customers")
    parser.add_argument("--mode", "-m", choices=["historical", "recent", "realtime", "incremental"],
                       default=DATA_MODE, help="Data generation mode")
    parser.add_argument("--output", "-o", default="fraud_transactions.csv",
                       help="Output CSV path")
    parser.add_argument("--load-mysql", action="store_true",
                       help="Load to MySQL after generation")
    parser.add_argument("--mysql-mode", choices=["replace", "append", "upsert"],
                       default="replace", help="MySQL load mode")
    
    args = parser.parse_args()
    
    print(f"\n🔄 Generating fraud data...")
    print(f"   Customers: {args.customers}")
    print(f"   Mode: {args.mode}")
    
    df = generate_fraud_data(n_customers=args.customers, mode=args.mode)
    print_stats(df)
    
    # Save to CSV
    df.to_csv(args.output, index=False)
    print(f"✅ Saved to {args.output}")
    
    # Load to MySQL
    if args.load_mysql or os.getenv("LOAD_TO_MYSQL", "").lower() in ("true", "1", "yes"):
        count = load_to_mysql(df, mode=args.mysql_mode)
        print(f"✅ Loaded {count:,} rows to MySQL")


if __name__ == "__main__":
    main()
