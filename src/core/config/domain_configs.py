"""
Domain Configuration Dictionaries
=================================

Dictionary configurations for LLMOps, retraining, and monitoring.
These are maintained for backward compatibility with existing code.

For MLOps and CVOps, use the Pydantic settings directly:

    from src.core.config import settings
    catalog = settings.infra.trino_catalog
    experiment = settings.mlops.mlflow_experiment
    repo = settings.cvops.repo
"""

from typing import Dict, Any, List
from src.core.config.settings import settings, TRINO_CATALOG


# =============================================================================
# LAZY CONFIG BUILDERS
# =============================================================================
# These functions build configs on demand, ensuring Pydantic has loaded .env


def _build_llmops_config() -> Dict[str, Any]:
    """Build LLMOPS_CONFIG from Pydantic settings."""
    return {
        # LakeFS
        "repo": settings.llmops.repo,
        "branch": settings.llmops.branch,
        "corpus_prefix": settings.llmops.corpus_prefix,
        "embeddings_prefix": settings.llmops.embeddings_prefix,
        "finetuning_prefix": "finetuning/",

        # Embedding model
        "embedding_model": settings.llmops.embedding_model,
        "embedding_device": settings.llmops.embedding_device,
        "embedding_batch_size": settings.llmops.embedding_batch_size,

        # Vector store
        "vector_store_type": settings.llmops.vector_store_type,
        "vector_store_url": "",
        "vector_collection": settings.llmops.vector_collection,
    }


def _build_retrain_config() -> Dict[str, Any]:
    """Build RETRAIN_CONFIG from Pydantic settings."""
    return {
        # Performance thresholds
        "drift_threshold": settings.mlops.retrain_drift_threshold,
        "f1_threshold": settings.mlops.retrain_f1_threshold,
        "accuracy_threshold": settings.mlops.retrain_accuracy_threshold,

        # Data thresholds
        "min_new_labels": settings.mlops.retrain_min_new_labels,
        "min_samples_for_trigger": settings.mlops.retrain_min_samples_for_trigger,

        # Cooldown
        "cooldown_hours": settings.mlops.retrain_cooldown_hours,

        # Feature flags
        "enabled": settings.mlops.retrain_enabled,
        "auto_promote": settings.mlops.retrain_auto_promote,

        # Sensor settings
        "sensor_interval_seconds": settings.mlops.retrain_sensor_interval_seconds,

        # Job to trigger
        "retrain_job_name": settings.mlops.retrain_job_name,
    }


def _build_shadow_mode_config() -> Dict[str, Any]:
    """Build SHADOW_MODE_CONFIG from Pydantic settings."""
    return {
        # Enable/disable shadow mode
        "enabled": settings.mlops.shadow_enabled,

        # Shadow model stage (usually Staging)
        "shadow_stage": settings.mlops.shadow_stage,

        # Logging settings
        "log_all_predictions": settings.mlops.shadow_log_all_predictions,
        "log_disagreements_only": settings.mlops.shadow_log_disagreements_only,

        # Agreement threshold for alerts (0.0-1.0)
        "agreement_alert_threshold": settings.mlops.shadow_agreement_alert_threshold,

        # Score difference threshold for alerts
        "score_diff_alert_threshold": settings.mlops.shadow_score_diff_alert_threshold,
    }


def _build_performance_config() -> Dict[str, Any]:
    """Build PERFORMANCE_CONFIG from Pydantic settings."""
    catalog = settings.infra.trino_catalog
    return {
        # Rolling windows
        "windows": settings.mlops.performance_windows,

        # Alert thresholds
        "f1_threshold": settings.mlops.performance_f1_threshold,
        "accuracy_threshold": settings.mlops.performance_accuracy_threshold,

        # Minimum samples before alerting
        "min_samples": settings.mlops.performance_min_samples,

        # Tables (use TRINO_CATALOG)
        "metrics_table": f"{catalog}.{settings.mlops.performance_metrics_schema}.{settings.mlops.performance_metrics_table}",
        "history_table": f"{catalog}.{settings.mlops.performance_metrics_schema}.{settings.mlops.performance_history_table}",
    }


def _build_iceberg_snapshot_config() -> Dict[str, Any]:
    """Build ICEBERG_SNAPSHOT_CONFIG from Pydantic settings."""
    catalog = settings.infra.trino_catalog
    return {
        # Enable/disable sensor
        "enabled": settings.mlops.iceberg_sensor_enabled,

        # Polling interval in seconds
        "poll_interval_seconds": settings.mlops.iceberg_poll_interval_seconds,

        # Tables to monitor for rollback events (use TRINO_CATALOG)
        "monitored_tables": [
            f"{catalog}.{settings.mlops.gold_schema}.{settings.mlops.gold_table}",
            f"{catalog}.{settings.mlops.silver_schema}.{settings.mlops.silver_table}",
        ],

        # Cursor table for tracking last known snapshots (use TRINO_CATALOG)
        "cursor_table": f"{catalog}.{settings.mlops.iceberg_cursor_schema}.{settings.mlops.iceberg_cursor_table}",

        # Event table for audit trail (use TRINO_CATALOG)
        "event_table": f"{catalog}.{settings.mlops.iceberg_event_schema}.{settings.mlops.iceberg_event_table}",

        # Rollback history table (use TRINO_CATALOG)
        "history_table": f"{catalog}.{settings.mlops.iceberg_history_schema}.{settings.mlops.iceberg_history_table}",
    }


def _build_ksqldb_config() -> Dict[str, Any]:
    """Build KSQLDB_CONFIG from Pydantic settings."""
    return {
        # ksqlDB Server
        "server_url": settings.infra.ksqldb_server_url,
        "enabled": settings.mlops.streaming_enabled,

        # Kafka topics
        "source_topic": settings.mlops.streaming_source_topic,
        "output_topic": settings.mlops.streaming_output_topic,

        # Window configurations (in seconds)
        "windows": {"velocity_5min": 300, "velocity_1h": 3600, "velocity_24h": 86400},

        # Velocity thresholds for risk scoring
        "velocity_thresholds": {
            "high_5min": settings.mlops.velocity_high_5min,
            "medium_5min": settings.mlops.velocity_medium_5min,
            "high_1h": settings.mlops.velocity_high_1h,
            "medium_1h": settings.mlops.velocity_medium_1h,
            "high_24h": settings.mlops.velocity_high_24h,
        },

        # Multi-entity thresholds
        "multi_entity_thresholds": {
            "countries_5min": settings.mlops.multi_country_threshold,
            "devices_5min": settings.mlops.multi_device_threshold,
            "ips_1h": settings.mlops.multi_ip_threshold,
        },

        # Streams and tables created by ksqlDB
        "streams": ["TRANSACTIONS_RAW", "TRANSACTIONS_ENRICHED", "STREAMING_FEATURES", "FRAUD_STREAMING_FEATURES"],
        "tables": ["VELOCITY_5MIN", "VELOCITY_1H", "VELOCITY_24H", "CUSTOMER_AGGREGATES"],
    }


# =============================================================================
# LAZY CONFIG WRAPPER CLASS
# =============================================================================

class _LazyConfigDict:
    """
    A wrapper that builds config dicts lazily on first access.
    This ensures Pydantic settings have loaded .env before we build configs.
    """

    def __init__(self, builder):
        self._builder = builder
        self._cache = None

    def _get_dict(self) -> Dict[str, Any]:
        if self._cache is None:
            self._cache = self._builder()
        return self._cache

    def __getitem__(self, key):
        return self._get_dict()[key]

    def __contains__(self, key):
        return key in self._get_dict()

    def __iter__(self):
        return iter(self._get_dict())

    def __len__(self):
        return len(self._get_dict())

    def get(self, key, default=None):
        return self._get_dict().get(key, default)

    def keys(self):
        return self._get_dict().keys()

    def values(self):
        return self._get_dict().values()

    def items(self):
        return self._get_dict().items()

    def __repr__(self):
        return repr(self._get_dict())


# =============================================================================
# EXPORTED CONFIGURATION DICTIONARIES
# =============================================================================

LLMOPS_CONFIG = _LazyConfigDict(_build_llmops_config)
RETRAIN_CONFIG = _LazyConfigDict(_build_retrain_config)
SHADOW_MODE_CONFIG = _LazyConfigDict(_build_shadow_mode_config)
PERFORMANCE_CONFIG = _LazyConfigDict(_build_performance_config)
ICEBERG_SNAPSHOT_CONFIG = _LazyConfigDict(_build_iceberg_snapshot_config)
KSQLDB_CONFIG = _LazyConfigDict(_build_ksqldb_config)


# =============================================================================
# FRAUD FEATURE DEFINITIONS (static - no env vars needed)
# =============================================================================

FRAUD_FEATURES = {
    # Base columns from MySQL source
    "base_columns": [
        "transaction_id",
        "customer_id",
        "transaction_amount",
        "transaction_date",
        "payment_method",
        "product_category",
        "quantity",
        "customer_age",
        "customer_location",
        "device_used",
        "ip_address",
        "shipping_address",
        "billing_address",
        "is_fraudulent",  # Source label: 1=Fraud, 0=Legit
        "account_age_days",
        "transaction_hour",
    ],

    # Built features (computed in Jupyter)
    "built_features": [
        "shipping_country",
        "billing_country",
        "address_mismatch",
        "high_risk_shipping",
        "is_night",
        "is_weekend",
        "transactions_before",
        "total_spend_before",
        "avg_amount_before",
        "customer_tenure_days",
        "days_since_last_tx",
        "amount_vs_avg",
        "is_new_customer",
        "is_very_new_account",
        "high_velocity",
        "is_high_amount_vs_hist",
        "risky_payment",
        "risky_category",
        "ip_prefix",
    ],
}
