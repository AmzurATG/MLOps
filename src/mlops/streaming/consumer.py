"""
Fraud Streaming Consumer: Kafka → Redis (Event-Driven)
==============================================================================

Standalone event-driven consumer that processes streaming features from ksqlDB
and writes enriched data to Redis for real-time fraud inference.

This replaces the Dagster sensor-based polling approach with immediate
event-driven processing for sub-second latency.

Architecture:
    Kafka: fraud.streaming.features (from ksqlDB)
              ↓ (immediate, event-driven)
    This Consumer
              ↓
    Redis: feast:streaming:{customer_id} (hash - latest features)
           feast:velocity:{customer_id}:txns (sorted set - tx timestamps)
           feast:velocity:{customer_id}:amounts (sorted set - amounts)

Features:
    - Event-driven consumption (no polling delay)
    - Graceful shutdown with signal handlers
    - Velocity computation using Redis sorted sets
    - Configurable via environment variables
    - Consumer group support for horizontal scaling

SCHEMA EVOLUTION:
    - Typed field definitions with validation
    - Unknown field detection and logging
    - Type coercion with fallback defaults
    - Schema drift monitoring via metrics
"""

import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import redis
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("fraud_streaming_consumer")


@dataclass
class Config:
    """Consumer configuration from environment variables."""

    kafka_bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "exp-kafka:9092")
    )
    kafka_topic: str = field(
        default_factory=lambda: os.getenv(
            "STREAMING_FEATURES_TOPIC", "fraud.streaming.features"
        )
    )
    kafka_consumer_group: str = field(
        default_factory=lambda: os.getenv(
            "KAFKA_CONSUMER_GROUP", "fraud-streaming-consumer"
        )
    )
    redis_host: str = field(
        default_factory=lambda: os.getenv("REDIS_HOST", "exp-redis")
    )
    redis_port: int = field(
        default_factory=lambda: int(os.getenv("REDIS_PORT", "6379"))
    )

    # Redis key prefixes
    redis_streaming_prefix: str = "feast:streaming:"
    redis_velocity_prefix: str = "feast:velocity:"

    # TTL settings (seconds)
    redis_ttl: int = 3600  # 1 hour for streaming features
    velocity_ttl: int = 86400  # 24 hours for velocity sorted sets

    # Time windows for velocity computation (seconds)
    window_5min: int = 300
    window_1h: int = 3600
    window_24h: int = 86400


# =============================================================================
# SCHEMA EVOLUTION: Typed Field Definitions
# =============================================================================

@dataclass
class FieldDefinition:
    """
    Typed field definition for schema evolution.

    Enables:
    - Type validation and coercion
    - Required field checking
    - Default value handling
    - Schema drift detection
    """
    ksqldb_name: str  # Field name in ksqlDB message (uppercase)
    redis_name: str  # Field name in Redis hash
    field_type: str  # "int", "float", "str", "bool"
    required: bool = False  # If True, log warning when missing
    default: Any = None  # Default value when missing

    def coerce(self, value: Any) -> Tuple[Any, Optional[str]]:
        """
        Coerce value to expected type.

        Returns:
            Tuple of (coerced_value, warning_message or None)
        """
        if value is None:
            return self.default, None if not self.required else f"Missing required: {self.ksqldb_name}"

        try:
            if self.field_type == "int":
                return int(float(value)), None  # Handle "3.0" -> 3
            elif self.field_type == "float":
                return float(value), None
            elif self.field_type == "str":
                return str(value), None
            elif self.field_type == "bool":
                if isinstance(value, str):
                    return 1 if value.lower() in ("true", "1", "yes") else 0, None
                return int(bool(value)), None
            return str(value), None
        except (ValueError, TypeError) as e:
            return self.default, f"Coercion failed for {self.ksqldb_name}: {e}"


# Typed field mapping with schema evolution support
TYPED_FEATURE_MAPPING: Dict[str, FieldDefinition] = {
    # Binary flags (int)
    "ADDRESS_MISMATCH": FieldDefinition("ADDRESS_MISMATCH", "address_mismatch", "int", False, 0),
    "IS_NIGHT": FieldDefinition("IS_NIGHT", "is_night", "int", False, 0),
    "IS_WEEKEND": FieldDefinition("IS_WEEKEND", "is_weekend", "int", False, 0),
    "RISKY_PAYMENT": FieldDefinition("RISKY_PAYMENT", "risky_payment", "int", False, 0),
    "RISKY_CATEGORY": FieldDefinition("RISKY_CATEGORY", "risky_category", "int", False, 0),
    "HIGH_RISK_SHIPPING": FieldDefinition("HIGH_RISK_SHIPPING", "high_risk_shipping", "int", False, 0),

    # Categorical strings
    "SHIPPING_COUNTRY": FieldDefinition("SHIPPING_COUNTRY", "shipping_country", "str", False, "US"),
    "BILLING_COUNTRY": FieldDefinition("BILLING_COUNTRY", "billing_country", "str", False, "US"),
    "IP_PREFIX": FieldDefinition("IP_PREFIX", "ip_prefix", "str", False, "0"),
    "DEVICE_USED": FieldDefinition("DEVICE_USED", "device_used", "str", False, "desktop"),
    "PAYMENT_METHOD": FieldDefinition("PAYMENT_METHOD", "payment_method", "str", False, "credit_card"),
    "PRODUCT_CATEGORY": FieldDefinition("PRODUCT_CATEGORY", "product_category", "str", False, "Electronics"),

    # Numeric fields
    "TRANSACTION_HOUR": FieldDefinition("TRANSACTION_HOUR", "transaction_hour", "int", False, 12),
    "AMOUNT": FieldDefinition("AMOUNT", "amount", "float", True, 0.0),

    # Transaction metadata (required)
    "CUSTOMER_ID": FieldDefinition("CUSTOMER_ID", "customer_id", "str", True, ""),
    "TRANSACTION_ID": FieldDefinition("TRANSACTION_ID", "transaction_id", "str", True, ""),
}

# Add lowercase variants for backward compatibility
_lowercase_mappings = {}
for key, defn in list(TYPED_FEATURE_MAPPING.items()):
    lower_key = key.lower()
    if lower_key != key:
        _lowercase_mappings[lower_key] = FieldDefinition(
            lower_key, defn.redis_name, defn.field_type, defn.required, defn.default
        )
TYPED_FEATURE_MAPPING.update(_lowercase_mappings)

# Legacy simple mapping for backward compatibility
KSQLDB_FEATURE_MAPPING = {k: v.redis_name for k, v in TYPED_FEATURE_MAPPING.items()}

# SCHEMA EVOLUTION: Schema version for streaming consumer
# Bump when TYPED_FEATURE_MAPPING changes
STREAMING_SCHEMA_VERSION = "1.0.0"


class FraudStreamingConsumer:
    """
    Event-driven Kafka consumer for fraud streaming features.

    Consumes enriched transaction data from ksqlDB, computes velocity features
    using Redis sorted sets, and stores combined features for real-time inference.
    """

    def __init__(self, config: Config):
        self.config = config
        self.running = False
        self.consumer: Optional[KafkaConsumer] = None
        self.redis_client: Optional[redis.Redis] = None

        # Metrics
        self.messages_processed = 0
        self.errors = 0
        self.start_time: Optional[float] = None

        # SCHEMA EVOLUTION: Track unknown fields for drift detection
        self._unknown_fields_seen: Dict[str, int] = {}
        self._schema_warnings: List[str] = []
        self._type_coercion_count: int = 0

    # =========================================================================
    # SCHEMA EVOLUTION: Typed Field Mapping
    # =========================================================================

    def validate_and_map_features(
        self, data: Dict[str, Any]
    ) -> Tuple[Dict[str, str], List[str]]:
        """
        Validate and map features from ksqlDB message with type coercion.

        SCHEMA EVOLUTION: Provides:
        - Type validation and coercion
        - Unknown field detection (schema drift)
        - Missing required field warnings
        - Detailed error/warning messages

        Args:
            data: Raw message from ksqlDB

        Returns:
            Tuple of (features_dict, warnings_list)
        """
        features: Dict[str, str] = {}
        warnings: List[str] = []
        known_fields: set = set()

        for field_name, defn in TYPED_FEATURE_MAPPING.items():
            # Check both exact match and lowercase
            value = data.get(field_name) or data.get(field_name.lower())

            # Coerce to expected type
            coerced_value, warning = defn.coerce(value)

            if warning:
                warnings.append(warning)
                self._schema_warnings.append(warning)

            # Only set if we have a real value (not None or empty string)
            # This prevents lowercase variants from overwriting uppercase matches
            if coerced_value not in (None, ""):
                # Redis expects strings - don't overwrite existing values
                if defn.redis_name not in features or not features[defn.redis_name]:
                    features[defn.redis_name] = str(coerced_value)

            known_fields.add(field_name)
            known_fields.add(field_name.lower())

        # SCHEMA EVOLUTION: Detect unknown fields (potential schema drift)
        unknown_fields = set(data.keys()) - known_fields
        # Exclude known metadata fields
        metadata_fields = {"ROWTIME", "ROWKEY", "rowtime", "rowkey", "event_timestamp"}
        unknown_fields -= metadata_fields

        if unknown_fields:
            for field in unknown_fields:
                self._unknown_fields_seen[field] = self._unknown_fields_seen.get(field, 0) + 1
                # Log only first occurrence and every 1000th occurrence
                if self._unknown_fields_seen[field] == 1:
                    logger.warning(f"SCHEMA_DRIFT: Unknown field detected: {field}")
                    warnings.append(f"Unknown field: {field}")

        return features, warnings

    def get_schema_stats(self) -> Dict[str, Any]:
        """
        Get schema evolution statistics for monitoring.

        Returns dict with unknown fields, warnings count, etc.
        """
        return {
            "unknown_fields_seen": dict(self._unknown_fields_seen),
            "total_warnings": len(self._schema_warnings),
            "type_coercions": self._type_coercion_count,
            "recent_warnings": self._schema_warnings[-10:],  # Last 10 warnings
        }

    def setup_signal_handlers(self):
        """Register signal handlers for graceful shutdown."""
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        logger.info("Signal handlers registered for graceful shutdown")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        sig_name = signal.Signals(signum).name
        logger.info(f"Received {sig_name}, initiating graceful shutdown...")
        self.running = False

    def connect_kafka(self) -> bool:
        """Initialize Kafka consumer connection."""
        try:
            self.consumer = KafkaConsumer(
                self.config.kafka_topic,
                bootstrap_servers=self.config.kafka_bootstrap_servers,
                group_id=self.config.kafka_consumer_group,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=1000,  # 1 second timeout for checking shutdown
            )
            logger.info(
                f"Connected to Kafka at {self.config.kafka_bootstrap_servers}, "
                f"topic: {self.config.kafka_topic}, "
                f"group: {self.config.kafka_consumer_group}"
            )
            return True
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False

    def connect_redis(self) -> bool:
        """Initialize Redis connection with connection pooling."""
        try:
            # Use connection pool for better performance under high load
            self._redis_pool = redis.ConnectionPool(
                host=self.config.redis_host,
                port=self.config.redis_port,
                max_connections=20,
                decode_responses=True,
                socket_timeout=5.0,
                socket_connect_timeout=5.0,
            )
            self.redis_client = redis.Redis(connection_pool=self._redis_pool)
            self.redis_client.ping()
            logger.info(
                f"Connected to Redis at {self.config.redis_host}:{self.config.redis_port} "
                f"(pool_size=20)"
            )
            return True
        except redis.RedisError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            return False

    def compute_velocity_features(
        self, customer_id: str, current_time: float, amount: float
    ) -> Dict[str, Any]:
        """
        Compute velocity features using Redis sorted sets with pipelining.

        OPTIMIZED: Uses Redis pipeline to batch all operations into a single
        round-trip, reducing latency from 11 ops to 1.

        Args:
            customer_id: Customer identifier
            current_time: Current Unix timestamp
            amount: Transaction amount

        Returns:
            Dict with velocity features
        """
        velocity_key = f"{self.config.redis_velocity_prefix}{customer_id}:txns"
        amount_key = f"{self.config.redis_velocity_prefix}{customer_id}:amounts"

        # Compute cutoff times
        cutoff_5min = current_time - self.config.window_5min
        cutoff_1h = current_time - self.config.window_1h
        cutoff_24h = current_time - self.config.window_24h

        # Use pipeline to batch all Redis operations into single round-trip
        pipe = self.redis_client.pipeline(transaction=False)

        # Add current transaction to sorted sets
        pipe.zadd(velocity_key, {f"{current_time}": current_time})  # 0
        pipe.zadd(amount_key, {f"{current_time}": amount})  # 1

        # Set TTL on velocity keys (24h + buffer)
        pipe.expire(velocity_key, self.config.velocity_ttl + 3600)  # 2
        pipe.expire(amount_key, self.config.velocity_ttl + 3600)  # 3

        # Clean up old entries (older than 24h) - periodic cleanup optimization
        # Only cleanup every 100 messages to reduce overhead
        if self.messages_processed % 100 == 0:
            pipe.zremrangebyscore(velocity_key, 0, cutoff_24h)  # 4 (optional)
            pipe.zremrangebyscore(amount_key, 0, cutoff_24h)  # 5 (optional)

        # Get velocity counts for each window
        pipe.zcount(velocity_key, cutoff_5min, current_time)  # 4 or 6
        pipe.zcount(velocity_key, cutoff_1h, current_time)  # 5 or 7
        pipe.zcount(velocity_key, cutoff_24h, current_time)  # 6 or 8

        # Get amounts for sum/avg calculations
        pipe.zrangebyscore(amount_key, cutoff_24h, current_time, withscores=True)  # 7 or 9

        # Execute pipeline - single round trip!
        results = pipe.execute()

        # Parse results based on whether cleanup was included
        if self.messages_processed % 100 == 0:
            # With cleanup: indices 6, 7, 8, 9
            tx_count_5min = results[6]
            tx_count_1h = results[7]
            tx_count_24h = results[8]
            amounts_24h = results[9]
        else:
            # Without cleanup: indices 4, 5, 6, 7
            tx_count_5min = results[4]
            tx_count_1h = results[5]
            tx_count_24h = results[6]
            amounts_24h = results[7]

        # Process amount values
        amount_values = [float(score) for _, score in amounts_24h] if amounts_24h else []
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
        amount_anomaly_flag = (
            1 if (avg_amount_24h > 0 and amount > avg_amount_24h * 3) else 0
        )

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

    def process_message(self, data: Dict[str, Any]) -> bool:
        """
        Process a single Kafka message with schema evolution support.

        SCHEMA EVOLUTION: Uses validate_and_map_features() for:
        - Type validation and coercion
        - Unknown field detection (schema drift)
        - Missing required field warnings

        Args:
            data: Deserialized message data from Kafka

        Returns:
            True if processed successfully, False otherwise
        """
        try:
            # SCHEMA EVOLUTION: Use typed validation for all features
            mapped_features, schema_warnings = self.validate_and_map_features(data)

            # Log schema warnings (first occurrences only to avoid spam)
            for warning in schema_warnings:
                if warning not in self._schema_warnings[-100:]:  # Dedup recent warnings
                    logger.debug(f"Schema warning: {warning}")

            # Extract customer_id from mapped features
            customer_id = mapped_features.get("customer_id")
            if not customer_id:
                logger.warning(f"Message missing customer_id: {data}")
                return False

            # Get amount for velocity computation (already coerced to string)
            amount = float(mapped_features.get("amount", "0"))

            # Build Redis key in Feast format
            redis_key = f"{self.config.redis_streaming_prefix}{customer_id}"

            # Current timestamp
            current_time = time.time()

            # Compute velocity features using Redis sorted sets
            velocity_features = self.compute_velocity_features(
                customer_id, current_time, amount
            )

            # Build features dict with timestamp
            features = {"_timestamp": datetime.now().isoformat()}

            # SCHEMA EVOLUTION: Add validated/coerced features from typed mapping
            features.update(mapped_features)

            # Add computed velocity features
            for key, value in velocity_features.items():
                features[key] = str(value)

            # Store transaction-level info
            transaction_id = mapped_features.get("transaction_id")
            if transaction_id:
                features["_last_transaction_id"] = str(transaction_id)
            if amount:
                features["_last_amount"] = str(amount)

            # Write to Redis as hash with expiry - pipelined for efficiency
            pipe = self.redis_client.pipeline(transaction=False)
            pipe.hset(redis_key, mapping=features)
            pipe.expire(redis_key, self.config.redis_ttl)
            pipe.execute()

            return True

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False

    def run(self):
        """
        Main consumer loop.

        Continuously consumes messages from Kafka and writes to Redis.
        Handles graceful shutdown on SIGTERM/SIGINT.
        """
        self.setup_signal_handlers()

        # Connect to Kafka with retry
        max_retries = 10
        retry_delay = 5

        for attempt in range(max_retries):
            if self.connect_kafka():
                break
            logger.warning(
                f"Kafka connection attempt {attempt + 1}/{max_retries} failed, "
                f"retrying in {retry_delay}s..."
            )
            time.sleep(retry_delay)
        else:
            logger.error("Failed to connect to Kafka after all retries")
            return

        # Connect to Redis with retry
        for attempt in range(max_retries):
            if self.connect_redis():
                break
            logger.warning(
                f"Redis connection attempt {attempt + 1}/{max_retries} failed, "
                f"retrying in {retry_delay}s..."
            )
            time.sleep(retry_delay)
        else:
            logger.error("Failed to connect to Redis after all retries")
            return

        self.running = True
        self.start_time = time.time()
        logger.info("Starting consumer loop...")

        while self.running:
            try:
                # Poll for messages (with 1s timeout to check shutdown flag)
                for message in self.consumer:
                    if not self.running:
                        break

                    if self.process_message(message.value):
                        self.messages_processed += 1

                        # Log progress periodically
                        if self.messages_processed % 100 == 0:
                            elapsed = time.time() - self.start_time
                            rate = self.messages_processed / elapsed if elapsed > 0 else 0
                            logger.info(
                                f"Processed {self.messages_processed} messages "
                                f"({rate:.1f} msg/s), errors: {self.errors}"
                            )

                        # SCHEMA EVOLUTION: Log schema stats every 1000 messages
                        if self.messages_processed % 1000 == 0:
                            stats = self.get_schema_stats()
                            if stats["unknown_fields_seen"]:
                                logger.warning(
                                    f"SCHEMA_EVOLUTION stats: "
                                    f"unknown_fields={stats['unknown_fields_seen']}, "
                                    f"warnings={stats['total_warnings']}"
                                )
                    else:
                        self.errors += 1

            except StopIteration:
                # consumer_timeout_ms triggered, no messages available
                continue
            except KafkaError as e:
                logger.error(f"Kafka error: {e}")
                self.errors += 1
                time.sleep(1)
            except Exception as e:
                logger.error(f"Unexpected error in consumer loop: {e}")
                self.errors += 1
                time.sleep(1)

        self.shutdown()

    def shutdown(self):
        """Clean up resources on shutdown."""
        logger.info("Shutting down consumer...")

        elapsed = time.time() - self.start_time if self.start_time else 0
        logger.info(
            f"Final stats: {self.messages_processed} messages processed, "
            f"{self.errors} errors, runtime: {elapsed:.1f}s"
        )

        # SCHEMA EVOLUTION: Log final schema stats
        schema_stats = self.get_schema_stats()
        if schema_stats["unknown_fields_seen"] or schema_stats["total_warnings"]:
            logger.warning(
                f"SCHEMA_EVOLUTION final stats: "
                f"schema_version={STREAMING_SCHEMA_VERSION}, "
                f"unknown_fields={schema_stats['unknown_fields_seen']}, "
                f"total_warnings={schema_stats['total_warnings']}"
            )
        else:
            logger.info(f"SCHEMA_EVOLUTION: No drift detected (version={STREAMING_SCHEMA_VERSION})")

        if self.consumer:
            try:
                self.consumer.close()
                logger.info("Kafka consumer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")

        if self.redis_client:
            try:
                self.redis_client.close()
                logger.info("Redis connection closed")
            except Exception as e:
                logger.error(f"Error closing Redis connection: {e}")

        logger.info("Shutdown complete")


def main():
    """Entry point for the consumer."""
    logger.info("=" * 60)
    logger.info("Fraud Streaming Consumer - Event-Driven")
    logger.info(f"Schema Version: {STREAMING_SCHEMA_VERSION}")
    logger.info("=" * 60)

    config = Config()
    logger.info(f"Configuration:")
    logger.info(f"  Kafka: {config.kafka_bootstrap_servers}")
    logger.info(f"  Topic: {config.kafka_topic}")
    logger.info(f"  Consumer Group: {config.kafka_consumer_group}")
    logger.info(f"  Redis: {config.redis_host}:{config.redis_port}")
    logger.info(f"  Schema: v{STREAMING_SCHEMA_VERSION} ({len(TYPED_FEATURE_MAPPING)//2} typed fields)")

    consumer = FraudStreamingConsumer(config)
    consumer.run()


if __name__ == "__main__":
    main()
