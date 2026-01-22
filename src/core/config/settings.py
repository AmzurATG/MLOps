"""
Unified Configuration for MLOps/CVOps/LLMOps Platform

Centralized, type-safe configuration using Pydantic Settings.
All environment variables are loaded once at startup and validated.

Usage:
    from src.core.config import settings

    # Access infrastructure settings
    catalog = settings.infra.trino_catalog

    # Access domain-specific settings
    experiment = settings.mlops.mlflow_experiment
    streaming_enabled = settings.mlops.streaming_enabled
"""

import os
from typing import List, Optional, Dict
from functools import lru_cache

try:
    from pydantic_settings import BaseSettings, SettingsConfigDict
except ImportError:
    from pydantic import BaseSettings
    SettingsConfigDict = None


# =============================================================================
# INFRASTRUCTURE SETTINGS (Core infrastructure shared across domains)
# =============================================================================

class InfraSettings(BaseSettings):
    """Core infrastructure configuration."""

    # Trino / Iceberg
    trino_host: str = "exp-trino"
    trino_port: int = 8080
    trino_catalog: str = "iceberg_dev"
    trino_user: str = "trino"

    # Redis
    redis_host: str = "exp-redis"
    redis_port: int = 6379
    redis_db: int = 0

    # MLflow
    mlflow_tracking_uri: str = "http://exp-mlflow:5000"

    # LakeFS
    lakefs_endpoint: str = "http://exp-lakefs:8000"
    lakefs_access_key: str = "AKIAIOSFOLKFSSAMPLES"
    lakefs_secret_key: str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

    # MinIO
    minio_endpoint: str = "http://exp-minio:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"

    # Kafka
    kafka_bootstrap_servers: str = "exp-kafka:9092"

    # ksqlDB
    ksqldb_server_url: str = "http://exp-ksqldb-server:8088"

    # Label Studio
    labelstudio_url: str = "http://exp-label-studio:8080"
    labelstudio_token: Optional[str] = None

    if SettingsConfigDict:
        model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    else:
        class Config:
            env_file = ".env"
            extra = "ignore"


# =============================================================================
# MLOPS SETTINGS (Fraud Detection - consolidated)
# =============================================================================

class MLOpsSettings(BaseSettings):
    """Fraud detection configuration with all sub-configs consolidated."""

    # === LakeFS repos ===
    bronze_repo: str = "bronze"
    warehouse_repo: str = "warehouse"
    dev_branch: str = "dev"
    main_branch: str = "main"

    # === Tables (schema.table - catalog from InfraSettings) ===
    bronze_schema: str = "bronze"
    bronze_table: str = "fraud_transactions"
    silver_schema: str = "silver"
    silver_table: str = "fraud_transactions"
    gold_schema: str = "gold"
    gold_table: str = "fraud_training_data"

    # === Primary key / label ===
    primary_key: str = "transaction_id"
    label_column: str = "is_fraudulent"

    # === MLflow ===
    mlflow_experiment: str = "fraud-detection"
    mlflow_model_name: str = "fraud-detector"

    # === Feature engineering ===
    high_risk_countries: List[str] = ["NG", "PK", "RU", "BR"]
    risky_payment_methods: List[str] = ["credit_card", "wallet"]
    risky_categories: List[str] = ["Electronics", "Luxury", "Digital"]

    # === Training ===
    min_training_samples: int = 50

    # === Label Studio ===
    ls_project_name: str = "Fraud Detection Review"

    # === Feast ===
    feast_repo_path: str = "/app/feast_repo"
    feature_service: str = "fraud_detection_v1"

    # === Streaming (ksqlDB) - consolidated from StreamingSettings ===
    streaming_enabled: bool = True
    streaming_source_topic: str = "fraud.demo.fraud_transactions"
    streaming_output_topic: str = "fraud.streaming.features"
    streaming_redis_prefix: str = "feast:streaming:"
    streaming_redis_ttl_seconds: int = 3600
    streaming_sensor_interval_seconds: int = 10

    # Velocity thresholds
    velocity_high_5min: int = 5
    velocity_medium_5min: int = 3
    velocity_high_1h: int = 20
    velocity_medium_1h: int = 10
    velocity_high_24h: int = 50
    velocity_medium_24h: int = 30

    # Multi-entity thresholds
    multi_country_threshold: int = 1
    multi_device_threshold: int = 1
    multi_ip_threshold: int = 3

    # Risk thresholds
    risk_high_threshold: float = 0.8
    risk_medium_threshold: float = 0.5
    max_streaming_boost: float = 0.5

    # === Shadow Mode - consolidated from ShadowModeSettings ===
    shadow_enabled: bool = False
    shadow_stage: str = "Staging"
    shadow_log_all_predictions: bool = True
    shadow_log_disagreements_only: bool = False
    shadow_agreement_alert_threshold: float = 0.9
    shadow_score_diff_alert_threshold: float = 0.2

    # === Auto-Retrain - consolidated from RetrainSettings ===
    retrain_enabled: bool = True
    retrain_drift_threshold: float = 0.3
    retrain_f1_threshold: float = 0.7
    retrain_accuracy_threshold: float = 0.8
    retrain_min_new_labels: int = 100
    retrain_min_samples_for_trigger: int = 50
    retrain_cooldown_hours: int = 24
    retrain_auto_promote: bool = False
    retrain_sensor_interval_seconds: int = 300
    retrain_job_name: str = "feature_pipeline_job"

    # === Performance Monitoring - consolidated from PerformanceSettings ===
    performance_windows: List[str] = ["1h", "24h", "7d"]
    performance_f1_threshold: float = 0.7
    performance_accuracy_threshold: float = 0.8
    performance_min_samples: int = 50
    performance_metrics_schema: str = "monitoring"
    performance_metrics_table: str = "performance_metrics"
    performance_history_table: str = "retraining_history"

    # === Iceberg Snapshot - consolidated from IcebergSnapshotSettings ===
    iceberg_sensor_enabled: bool = True
    iceberg_poll_interval_seconds: int = 30
    iceberg_cursor_schema: str = "metadata"
    iceberg_cursor_table: str = "snapshot_cursor"
    iceberg_event_schema: str = "metadata"
    iceberg_event_table: str = "iceberg_rollback_events"
    iceberg_history_schema: str = "metadata"
    iceberg_history_table: str = "rollback_history"

    if SettingsConfigDict:
        model_config = SettingsConfigDict(env_file=".env", env_prefix="MLOPS_", extra="ignore")
    else:
        class Config:
            env_file = ".env"
            env_prefix = "MLOPS_"
            extra = "ignore"

    # === Computed properties for backward compatibility ===

    @property
    def velocity_thresholds(self) -> Dict[str, int]:
        """Velocity thresholds dict (backward compatible)."""
        return {
            "high_5min": self.velocity_high_5min,
            "medium_5min": self.velocity_medium_5min,
            "high_1h": self.velocity_high_1h,
            "medium_1h": self.velocity_medium_1h,
            "high_24h": self.velocity_high_24h,
            "medium_24h": self.velocity_medium_24h,
        }

    @property
    def multi_entity_thresholds(self) -> Dict[str, int]:
        """Multi-entity thresholds dict (backward compatible)."""
        return {
            "countries_5min": self.multi_country_threshold,
            "devices_5min": self.multi_device_threshold,
            "ips_1h": self.multi_ip_threshold,
        }


# =============================================================================
# CVOPS SETTINGS (Computer Vision)
# =============================================================================

class CVOpsSettings(BaseSettings):
    """Computer vision configuration."""

    # === LakeFS ===
    repo: str = "cv-data"
    branch: str = "dev"
    raw_prefix: str = "raw/images/"
    processed_prefix: str = "processed/"
    use_lakefs_storage: bool = True

    # === MinIO buckets ===
    raw_bucket: str = "cv-raw"
    models_bucket: str = "models"
    artifacts_bucket: str = "artifacts"

    # === Tables (schema.table - catalog from InfraSettings) ===
    manifest_schema: str = "bronze"
    manifest_table: str = "cv_image_manifest"
    detections_schema: str = "silver"
    detections_table: str = "cv_detections"
    annotations_schema: str = "cv"
    annotations_table: str = "annotations"
    gold_schema: str = "gold"
    gold_table: str = "cv_training_data"

    # === YOLO ===
    yolo_model_path: str = "yolov8n.pt"
    yolo_confidence: float = 0.5
    yolo_device: str = "cpu"
    yolo_batch_size: int = 8
    yolo_dummy_mode: bool = True
    yolo_iou_threshold: float = 0.45

    # === MLflow ===
    mlflow_experiment: str = "cvops_object_detection"

    # === Training ===
    train_split: float = 0.8
    val_split: float = 0.1
    test_split: float = 0.1
    epochs: int = 100
    batch_size: int = 16
    image_size: int = 640
    min_training_samples: int = 100

    # === Webhook ===
    webhook_secret: str = "cvops-webhook-secret"
    webhook_port: int = 5001
    verify_webhook_signature: bool = False

    # === Kafka ===
    kafka_ingest_topic: str = "cv.images.ingest"
    kafka_dlq_topic: str = "cv.images.dlq"
    kafka_validated_topic: str = "cv.images.validated"
    kafka_consumer_group: str = "cvops-ingest-consumer"

    # === Label Studio ===
    ls_project_name: str = "CVOps Object Detection"
    ls_batch_size: int = 100

    if SettingsConfigDict:
        model_config = SettingsConfigDict(env_file=".env", env_prefix="CVOPS_", extra="ignore")
    else:
        class Config:
            env_file = ".env"
            env_prefix = "CVOPS_"
            extra = "ignore"


# =============================================================================
# LLMOPS SETTINGS (LLM/RAG - minimal, placeholder)
# =============================================================================

class LLMOpsSettings(BaseSettings):
    """LLM/RAG configuration (placeholder)."""

    repo: str = "llm-data"
    branch: str = "dev"
    corpus_prefix: str = "corpus/"
    embeddings_prefix: str = "embeddings/"
    embedding_model: str = "all-MiniLM-L6-v2"
    embedding_device: str = "cpu"
    embedding_batch_size: int = 32
    vector_store_type: str = "chromadb"
    vector_collection: str = "default"

    if SettingsConfigDict:
        model_config = SettingsConfigDict(env_file=".env", env_prefix="LLMOPS_", extra="ignore")
    else:
        class Config:
            env_file = ".env"
            env_prefix = "LLMOPS_"
            extra = "ignore"


# =============================================================================
# UNIFIED SETTINGS
# =============================================================================

class Settings:
    """
    Unified settings container providing access to all configuration.

    Usage:
        from src.core.config import settings

        catalog = settings.infra.trino_catalog
        experiment = settings.mlops.mlflow_experiment
        streaming = settings.mlops.streaming_enabled
    """

    def __init__(self):
        self._infra: Optional[InfraSettings] = None
        self._mlops: Optional[MLOpsSettings] = None
        self._cvops: Optional[CVOpsSettings] = None
        self._llmops: Optional[LLMOpsSettings] = None

    @property
    def infra(self) -> InfraSettings:
        if self._infra is None:
            self._infra = InfraSettings()
        return self._infra

    @property
    def mlops(self) -> MLOpsSettings:
        if self._mlops is None:
            self._mlops = MLOpsSettings()
        return self._mlops

    @property
    def cvops(self) -> CVOpsSettings:
        if self._cvops is None:
            self._cvops = CVOpsSettings()
        return self._cvops

    @property
    def llmops(self) -> LLMOpsSettings:
        if self._llmops is None:
            self._llmops = LLMOpsSettings()
        return self._llmops

    # Helper methods
    def table_name(self, schema: str, table: str) -> str:
        """Build fully qualified table name."""
        return f"{self.infra.trino_catalog}.{schema}.{table}"

    def mlops_bronze_table(self) -> str:
        return self.table_name(self.mlops.bronze_schema, self.mlops.bronze_table)

    def mlops_silver_table(self) -> str:
        return self.table_name(self.mlops.silver_schema, self.mlops.silver_table)

    def mlops_gold_table(self) -> str:
        return self.table_name(self.mlops.gold_schema, self.mlops.gold_table)


# Singleton instance
settings = Settings()


# =============================================================================
# UTILITY EXPORTS
# =============================================================================

def get_catalog() -> str:
    """Get the current Trino catalog."""
    return settings.infra.trino_catalog


def table_name(schema: str, table: str) -> str:
    """Build fully qualified table name."""
    return settings.table_name(schema, table)


# Commonly used constant
TRINO_CATALOG = settings.infra.trino_catalog


# =============================================================================
# SQL UTILITIES (for safe query building)
# =============================================================================

def escape_sql_string(value: str) -> str:
    """
    Escape a string value for safe SQL interpolation.

    This escapes single quotes by doubling them, which is the standard
    SQL escape mechanism. Use this for all user-controlled or external
    data that will be interpolated into SQL queries.

    Example:
        >>> escape_sql_string("O'Brien")
        "O''Brien"
    """
    if value is None:
        return ""
    return str(value).replace("'", "''")


def validate_identifier(name: str, identifier_type: str = "identifier") -> str:
    """
    Validate and return a SQL identifier (table name, column name, etc.).

    Only allows alphanumeric characters and underscores.
    Raises ValueError if the identifier contains invalid characters.

    Args:
        name: The identifier to validate
        identifier_type: Description for error messages (e.g., "table name")

    Returns:
        The validated identifier

    Raises:
        ValueError: If the identifier contains invalid characters
    """
    import re
    if not name:
        raise ValueError(f"Empty {identifier_type}")

    # Allow alphanumeric, underscore, and dots (for qualified names like schema.table)
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_\.]*$', name):
        raise ValueError(
            f"Invalid {identifier_type}: '{name}'. "
            f"Only alphanumeric characters, underscores, and dots allowed, must start with letter or underscore."
        )
    return name


def escape_sql_list(values: list) -> str:
    """
    Escape a list of values for SQL IN clause.

    Example:
        >>> escape_sql_list(["a", "b'c", "d"])
        "'a', 'b''c', 'd'"
    """
    if not values:
        return "''"
    return ", ".join(f"'{escape_sql_string(v)}'" for v in values)
