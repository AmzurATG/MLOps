"""
Feast Feature Definitions for fraud_detection
AUTO-GENERATED FROM YAML - DO NOT EDIT DIRECTLY

Version: 2.0.0
"""

from datetime import timedelta
from feast import Entity, FeatureView, Field, FeatureService, ValueType
from feast.infra.offline_stores.contrib.trino_offline_store.trino_source import TrinoSource
from feast.types import Float64, Int64, String

# ═══════════════════════════════════════════════════════════════════════════════
# ENTITIES
# ═══════════════════════════════════════════════════════════════════════════════

customer = Entity(
    name="customer_id",
    join_keys=["customer_id"],
    value_type=ValueType.STRING,
    description="Unique customer identifier"
)

transaction = Entity(
    name="transaction_id",
    join_keys=["transaction_id"],
    value_type=ValueType.STRING,
    description="Unique transaction identifier"
)

# ═══════════════════════════════════════════════════════════════════════════════
# DATA SOURCE
# ═══════════════════════════════════════════════════════════════════════════════

feature_source = TrinoSource(
    name="fraud_detection_source",
    table="iceberg_dev.gold.fraud_training_data",
    timestamp_field="event_timestamp",
)

# ═══════════════════════════════════════════════════════════════════════════════
# FEATURE VIEWS
# ═══════════════════════════════════════════════════════════════════════════════

primitives_fv = FeatureView(
    name="primitives",
    entities=[customer, transaction],
    ttl=timedelta(days=90),
    schema=[
        Field(name="amount", dtype=Float64),
        Field(name="quantity", dtype=Int64),
        Field(name="country", dtype=String),
        Field(name="device_type", dtype=String),
        Field(name="payment_method", dtype=String),
        Field(name="category", dtype=String),
        Field(name="account_age_days", dtype=Int64),
        Field(name="tx_hour", dtype=Int64),
        Field(name="tx_dayofweek", dtype=Int64),
        Field(name="customer_age", dtype=Int64),
        Field(name="ip_address", dtype=String),
    ],
    source=feature_source,
    online=True,
)

calendar_fv = FeatureView(
    name="calendar",
    entities=[transaction],
    ttl=timedelta(days=90),
    schema=[
        Field(name="transaction_day", dtype=Int64),
        Field(name="transaction_month", dtype=Int64),
        Field(name="is_weekend", dtype=Int64),
        Field(name="is_night", dtype=Int64),
    ],
    source=feature_source,
    online=True,
)

address_fv = FeatureView(
    name="address",
    entities=[transaction],
    ttl=timedelta(days=90),
    schema=[
        Field(name="shipping_country", dtype=String),
        Field(name="billing_country", dtype=String),
        Field(name="address_mismatch", dtype=Int64),
        Field(name="high_risk_shipping", dtype=Int64),
    ],
    source=feature_source,
    online=True,
)

customer_behavioral_fv = FeatureView(
    name="customer_behavioral",
    entities=[customer],
    ttl=timedelta(days=90),
    schema=[
        Field(name="transactions_before", dtype=Int64),
        Field(name="total_spend_before", dtype=Float64),
        Field(name="avg_amount_before", dtype=Float64),
        Field(name="customer_tenure_days", dtype=Int64),
        Field(name="days_since_last_tx", dtype=Float64),
        Field(name="is_new_customer", dtype=Int64),
        Field(name="is_very_new_account", dtype=Int64),
    ],
    source=feature_source,
    online=True,
)

customer_spending_fv = FeatureView(
    name="customer_spending",
    entities=[customer],
    ttl=timedelta(days=90),
    schema=[
        Field(name="total_spend_7d", dtype=Float64),
        Field(name="total_spend_30d", dtype=Float64),
        Field(name="tx_count_7d", dtype=Int64),
        Field(name="tx_count_30d", dtype=Int64),
        Field(name="avg_amount_30d", dtype=Float64),
        Field(name="max_amount_30d", dtype=Float64),
        Field(name="num_countries_90d", dtype=Int64),
        Field(name="num_payment_methods_30d", dtype=Int64),
    ],
    source=feature_source,
    online=True,
)

risk_indicators_fv = FeatureView(
    name="risk_indicators",
    entities=[transaction],
    ttl=timedelta(days=90),
    schema=[
        Field(name="amount_vs_avg", dtype=Float64),
        Field(name="is_high_amount_vs_hist", dtype=Int64),
        Field(name="high_velocity", dtype=Int64),
        Field(name="risky_payment", dtype=Int64),
        Field(name="risky_category", dtype=Int64),
        Field(name="ip_prefix", dtype=String),
    ],
    source=feature_source,
    online=True,
)

# ═══════════════════════════════════════════════════════════════════════════════
# FEATURE SERVICE
# ═══════════════════════════════════════════════════════════════════════════════

fraud_detection_service = FeatureService(
    name="fraud_detection_v1",
    features=[primitives_fv, calendar_fv, address_fv, customer_behavioral_fv, customer_spending_fv, risk_indicators_fv],
    description="Fraud detection feature set for e-commerce transactions"
)
