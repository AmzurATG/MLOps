"""
Streaming Consumer: Kafka → Redis
Consumes streaming features from ksqlDB via Kafka and writes to Redis in Feast format.

This sensor consumes real-time features computed by ksqlDB:
- Enriched transaction features (address_mismatch, is_night, risky_payment, etc.)
- Computes velocity features (5min, 1h, 24h) using Redis sorted sets

The features are stored in Redis as hashes for fast retrieval during inference.

Architecture:
- ksqlDB outputs enriched per-transaction data to fraud.streaming.features
- This consumer tracks velocity using Redis sorted sets (ZADD with timestamp scores)
- Velocity counts are computed on each transaction and stored in customer hash
"""

from dagster import sensor, SkipReason, SensorEvaluationContext
from kafka import KafkaConsumer
import json
import redis
import logging
from datetime import datetime
import time
import os

logger = logging.getLogger(__name__)

# Configuration (from environment or defaults)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "exp-kafka:9092")
KAFKA_TOPIC = os.getenv("STREAMING_FEATURES_TOPIC", "fraud.streaming.features")
REDIS_HOST = os.getenv("REDIS_HOST", "exp-redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_KEY_PREFIX = "feast:streaming:"
REDIS_VELOCITY_PREFIX = "feast:velocity:"
REDIS_TTL = 3600  # 1 hour
VELOCITY_TTL = 86400  # 24 hours for velocity sorted sets

# Time windows in seconds
WINDOW_5MIN = 300
WINDOW_1H = 3600
WINDOW_24H = 86400

# Feature mapping from ksqlDB enriched output to Redis hash fields
KSQLDB_FEATURE_MAPPING = {
    # Enriched features from ksqlDB
    "ADDRESS_MISMATCH": "address_mismatch",
    "IS_NIGHT": "is_night",
    "IS_WEEKEND": "is_weekend",
    "RISKY_PAYMENT": "risky_payment",
    "RISKY_CATEGORY": "risky_category",
    "SHIPPING_COUNTRY": "shipping_country",
    "BILLING_COUNTRY": "billing_country",
    "IP_PREFIX": "ip_prefix",
    "DEVICE_USED": "device_used",
    "PAYMENT_METHOD": "payment_method",
    "PRODUCT_CATEGORY": "product_category",
    "TRANSACTION_HOUR": "transaction_hour",
    # Also check lowercase versions
    "address_mismatch": "address_mismatch",
    "is_night": "is_night",
    "is_weekend": "is_weekend",
    "risky_payment": "risky_payment",
    "risky_category": "risky_category",
    "shipping_country": "shipping_country",
    "billing_country": "billing_country",
    "ip_prefix": "ip_prefix",
    "device_used": "device_used",
    "payment_method": "payment_method",
    "product_category": "product_category",
    "transaction_hour": "transaction_hour",
}


def compute_velocity_features(redis_client, customer_id: str, current_time: float, amount: float) -> dict:
    """
    Compute velocity features using Redis sorted sets.

    Uses ZADD to track transactions with timestamp scores, then ZCOUNT to get
    counts within time windows.
    """
    velocity_key = f"{REDIS_VELOCITY_PREFIX}{customer_id}:txns"
    amount_key = f"{REDIS_VELOCITY_PREFIX}{customer_id}:amounts"

    # Add current transaction to sorted set (score = timestamp)
    redis_client.zadd(velocity_key, {f"{current_time}": current_time})
    redis_client.zadd(amount_key, {f"{current_time}": amount})

    # Set TTL on velocity keys (24h + buffer)
    redis_client.expire(velocity_key, VELOCITY_TTL + 3600)
    redis_client.expire(amount_key, VELOCITY_TTL + 3600)

    # Clean up old entries (older than 24h)
    cutoff_24h = current_time - WINDOW_24H
    redis_client.zremrangebyscore(velocity_key, 0, cutoff_24h)
    redis_client.zremrangebyscore(amount_key, 0, cutoff_24h)

    # Compute velocity counts for each window
    cutoff_5min = current_time - WINDOW_5MIN
    cutoff_1h = current_time - WINDOW_1H

    tx_count_5min = redis_client.zcount(velocity_key, cutoff_5min, current_time)
    tx_count_1h = redis_client.zcount(velocity_key, cutoff_1h, current_time)
    tx_count_24h = redis_client.zcount(velocity_key, cutoff_24h, current_time)

    # Get amounts for sum/avg calculations
    amounts_24h = redis_client.zrangebyscore(amount_key, cutoff_24h, current_time, withscores=True)
    amount_values = [float(score) for _, score in amounts_24h]

    amount_sum_24h = sum(amount_values) if amount_values else 0.0
    avg_amount_24h = amount_sum_24h / len(amount_values) if amount_values else 0.0
    max_amount_24h = max(amount_values) if amount_values else 0.0

    # Compute velocity score (0-1)
    if tx_count_5min > 5:
        velocity_score = 1.0
    elif tx_count_5min > 3:
        velocity_score = 0.9
    elif tx_count_1h > 20:
        velocity_score = 0.8
    elif tx_count_1h > 10:
        velocity_score = 0.6
    elif tx_count_24h > 50:
        velocity_score = 0.5
    elif tx_count_24h > 30:
        velocity_score = 0.4
    else:
        velocity_score = tx_count_5min * 0.1

    # Compute flags
    high_velocity_flag = 1 if (tx_count_5min > 3 or tx_count_1h > 15) else 0
    amount_anomaly_flag = 1 if (avg_amount_24h > 0 and amount > avg_amount_24h * 3) else 0

    return {
        "tx_count_5min": tx_count_5min,
        "tx_count_1h": tx_count_1h,
        "tx_count_24h": tx_count_24h,
        "amount_sum_24h": round(amount_sum_24h, 2),
        "avg_amount_24h": round(avg_amount_24h, 2),
        "max_amount_24h": round(max_amount_24h, 2),
        "velocity_score": round(velocity_score, 2),
        "high_velocity_flag": high_velocity_flag,
        "amount_anomaly_flag": amount_anomaly_flag,
    }


@sensor(
    name="kafka_to_redis_sensor",
    minimum_interval_seconds=10,
    description="Consumes streaming features from Kafka and writes to Redis with velocity computation"
)
def kafka_to_redis_sensor(context: SensorEvaluationContext):
    """
    Sensor that consumes messages from Kafka and writes to Redis.
    Computes velocity features using Redis sorted sets.
    Runs every 10 seconds.
    """

    try:
        # Connect to Kafka
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',  # Only consume new messages
            enable_auto_commit=True,
            group_id='dagster_redis_consumer',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=5000  # Poll for 5 seconds max
        )

        # Connect to Redis
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

        # Consume messages
        messages_processed = 0
        for message in consumer:
            try:
                data = message.value

                # Extract customer_id (check both cases)
                customer_id = data.get('CUSTOMER_ID') or data.get('customer_id')
                if not customer_id:
                    logger.warning(f"Message missing customer_id: {data}")
                    continue

                # Get amount for velocity computation
                amount = float(data.get('AMOUNT') or data.get('amount') or 0)

                # Build Redis key in Feast format
                redis_key = f"{REDIS_KEY_PREFIX}{customer_id}"

                # Current timestamp
                current_time = time.time()

                # Compute velocity features using Redis sorted sets
                velocity_features = compute_velocity_features(
                    redis_client, customer_id, current_time, amount
                )

                # Build features dict from ksqlDB enriched message
                features = {'_timestamp': datetime.now().isoformat()}

                # Add enriched features from ksqlDB
                for ksql_field, redis_field in KSQLDB_FEATURE_MAPPING.items():
                    value = data.get(ksql_field)
                    if value is not None:
                        features[redis_field] = str(value)

                # Add computed velocity features
                for key, value in velocity_features.items():
                    features[key] = str(value)

                # Store transaction-level info
                transaction_id = data.get('TRANSACTION_ID') or data.get('transaction_id')
                if transaction_id:
                    features['_last_transaction_id'] = str(transaction_id)
                if amount:
                    features['_last_amount'] = str(amount)

                # Write to Redis as hash
                redis_client.hset(redis_key, mapping=features)

                # Set expiry (1 hour)
                redis_client.expire(redis_key, REDIS_TTL)

                messages_processed += 1

            except Exception as e:
                logger.error(f"Error processing message: {e}")
                continue

        consumer.close()
        redis_client.close()

        if messages_processed > 0:
            context.log.info(f"Processed {messages_processed} streaming feature messages to Redis")
            return SkipReason(f"Processed {messages_processed} messages successfully")
        else:
            return SkipReason("No new messages to process")

    except Exception as e:
        logger.error(f"Kafka consumer error: {e}")
        return SkipReason(f"Error: {e}")