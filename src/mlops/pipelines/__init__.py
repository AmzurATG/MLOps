"""
MLOps Pipelines Module
======================

Dagster pipelines for fraud detection data processing.

Pipeline Flow:
    Init → Bronze → Jupyter/Silver → Label Studio → Gold → Iceberg Optimization → Promotion

Assets:
- mlops_init_lakefs: Initialize LakeFS repos and Nessie branches
- mlops_init_iceberg_optimizations: Create metadata tables and apply sorting
- mlops_bronze_ingestion: Ingest raw transactions from LakeFS
- mlops_notify_jupyter: Trigger Jupyter processing
- mlops_ensure_review_columns: Add review columns to silver
- mlops_export_to_labelstudio: Export data for labeling
- mlops_notify_labelstudio: Notify Label Studio
- mlops_merge_annotations: Merge human annotations
- mlops_gold_table: Create training-ready dataset
- mlops_promote_to_production: Merge dev to main

Usage:
    from src.mlops.pipelines import MLOPS_ASSETS, MLOPS_JOBS, MLOPS_SENSORS
"""

# =============================================================================
# CORE ASSETS
# =============================================================================

from src.mlops.pipelines.mlops import (
    mlops_init_lakefs,
    mlops_init_iceberg_optimizations,
    mlops_airbyte_sync,
    mlops_promote_to_production,
    mlops_ingestion_job,
    mlops_promotion_job,
)

from src.mlops.pipelines.mlops_bronze import (
    mlops_bronze_ingestion,
)

from src.mlops.pipelines.mlops_jupyter import (
    mlops_notify_jupyter,
    silver_table_sensor,
    mlops_labelstudio_export_job,
    JUPYTER_ASSETS,
    JUPYTER_SENSORS,
    JUPYTER_JOBS,
)

from src.mlops.pipelines.mlops_labelstudio import (
    mlops_ensure_review_columns,
    mlops_export_to_labelstudio,
    mlops_notify_labelstudio,
    mlops_merge_annotations,
    labelstudio_annotations_sensor,
    mlops_labelstudio_merge_job,
    LABELSTUDIO_ASSETS,
    LABELSTUDIO_SENSORS,
    LABELSTUDIO_JOBS,
)

from src.mlops.pipelines.mlops_gold import (
    mlops_gold_table,
    GOLD_ASSETS,
)

# =============================================================================
# OPTIONAL MODULES (Safe imports with fallbacks)
# =============================================================================

# Feature Pipeline
try:
    from src.mlops.pipelines.feature_pipeline import (
        FEATURE_PIPELINE_ASSETS,
        FEATURE_PIPELINE_JOBS,
    )
except ImportError:
    FEATURE_PIPELINE_ASSETS = []
    FEATURE_PIPELINE_JOBS = []

# Quality Assets
try:
    from src.mlops.pipelines.quality_assets import (
        bronze_quality_check,
        silver_quality_check,
        gold_quality_check,
        training_quality_gate,
    )
    QUALITY_ASSETS = [bronze_quality_check, silver_quality_check, gold_quality_check, training_quality_gate]
except ImportError:
    QUALITY_ASSETS = []

# Drift Monitoring
try:
    from src.mlops.pipelines.drift_monitoring import (
        drift_monitoring_report,
        feature_drift_analysis,
        data_quality_monitoring,
        monitoring_summary,
    )
    MONITORING_ASSETS = [drift_monitoring_report, feature_drift_analysis, data_quality_monitoring, monitoring_summary]
except ImportError:
    MONITORING_ASSETS = []

# Evaluation
try:
    from src.mlops.pipelines.mlops_evaluation import (
        EVALUATION_ASSETS,
        EVALUATION_JOBS,
        EVALUATION_SCHEDULES,
    )
except ImportError:
    EVALUATION_ASSETS = []
    EVALUATION_JOBS = []
    EVALUATION_SCHEDULES = []

# Performance Monitoring
try:
    from src.mlops.pipelines.performance_monitoring import (
        PERFORMANCE_MONITORING_ASSETS,
        performance_monitoring_job,
    )
    PERFORMANCE_ASSETS = PERFORMANCE_MONITORING_ASSETS
    PERFORMANCE_JOBS = [performance_monitoring_job] if performance_monitoring_job else []
except ImportError:
    PERFORMANCE_ASSETS = []
    PERFORMANCE_JOBS = []

# Streaming
try:
    from src.mlops.pipelines.streaming_consumer import kafka_to_redis_sensor
    STREAMING_SENSORS = [kafka_to_redis_sensor] if kafka_to_redis_sensor else []
except ImportError:
    STREAMING_SENSORS = []

# Auto-retrain
try:
    from src.mlops.pipelines.auto_retrain_sensor import AUTO_RETRAIN_SENSORS
except ImportError:
    AUTO_RETRAIN_SENSORS = []

# Iceberg Snapshot Sensor
try:
    from src.mlops.pipelines.iceberg_snapshot_sensor import ICEBERG_SNAPSHOT_SENSORS
except ImportError:
    ICEBERG_SNAPSHOT_SENSORS = []

# Rollback Sync
try:
    from src.mlops.pipelines.rollback_sync import ROLLBACK_SYNC_JOBS
except ImportError:
    ROLLBACK_SYNC_JOBS = []

# Maintenance
try:
    from src.mlops.pipelines.mlops_maintenance import (
        lakefs_webhook_sensor,
        lakefs_webhook_cleanup_job,
    )
    WEBHOOK_SENSORS = [lakefs_webhook_sensor] if lakefs_webhook_sensor else []
    WEBHOOK_JOBS = [lakefs_webhook_cleanup_job] if lakefs_webhook_cleanup_job else []
except ImportError:
    WEBHOOK_SENSORS = []
    WEBHOOK_JOBS = []

# Checks
try:
    from src.mlops.pipelines.checks import MLOPS_CHECKS
except ImportError:
    MLOPS_CHECKS = []

# =============================================================================
# AGGREGATED EXPORTS
# =============================================================================

# Core assets (always available)
MLOPS_CORE_ASSETS = [
    mlops_init_lakefs,
    mlops_airbyte_sync,
    mlops_bronze_ingestion,
    mlops_notify_jupyter,
    mlops_ensure_review_columns,
    mlops_export_to_labelstudio,
    mlops_notify_labelstudio,
    mlops_merge_annotations,
    mlops_gold_table,
    mlops_init_iceberg_optimizations,
    mlops_promote_to_production,
]

# Core jobs
MLOPS_CORE_JOBS = [
    mlops_ingestion_job,
    mlops_labelstudio_export_job,
    mlops_labelstudio_merge_job,
    mlops_promotion_job,
]

# Core sensors
MLOPS_CORE_SENSORS = [
    silver_table_sensor,
    labelstudio_annotations_sensor,
]

# All assets (core + optional)
MLOPS_ASSETS = (
    MLOPS_CORE_ASSETS +
    FEATURE_PIPELINE_ASSETS +
    QUALITY_ASSETS +
    MONITORING_ASSETS +
    EVALUATION_ASSETS +
    PERFORMANCE_ASSETS
)

# All jobs (core + optional)
MLOPS_JOBS = (
    MLOPS_CORE_JOBS +
    FEATURE_PIPELINE_JOBS +
    WEBHOOK_JOBS +
    EVALUATION_JOBS +
    PERFORMANCE_JOBS +
    ROLLBACK_SYNC_JOBS
)

# All sensors (core + optional)
MLOPS_SENSORS = (
    MLOPS_CORE_SENSORS +
    STREAMING_SENSORS +
    WEBHOOK_SENSORS +
    AUTO_RETRAIN_SENSORS +
    ICEBERG_SNAPSHOT_SENSORS
)

# Schedules
MLOPS_SCHEDULES = EVALUATION_SCHEDULES

# Checks
MLOPS_ALL_CHECKS = MLOPS_CHECKS

# =============================================================================
# __all__ EXPORTS
# =============================================================================

__all__ = [
    # Core assets
    "mlops_init_lakefs",
    "mlops_init_iceberg_optimizations",
    "mlops_airbyte_sync",
    "mlops_bronze_ingestion",
    "mlops_notify_jupyter",
    "mlops_ensure_review_columns",
    "mlops_export_to_labelstudio",
    "mlops_notify_labelstudio",
    "mlops_merge_annotations",
    "mlops_gold_table",
    "mlops_promote_to_production",
    # Core jobs
    "mlops_ingestion_job",
    "mlops_promotion_job",
    "mlops_labelstudio_export_job",
    "mlops_labelstudio_merge_job",
    # Core sensors
    "silver_table_sensor",
    "labelstudio_annotations_sensor",
    # Aggregated lists
    "MLOPS_ASSETS",
    "MLOPS_JOBS",
    "MLOPS_SENSORS",
    "MLOPS_SCHEDULES",
    "MLOPS_ALL_CHECKS",
    "MLOPS_CORE_ASSETS",
    "MLOPS_CORE_JOBS",
    "MLOPS_CORE_SENSORS",
    # Optional modules
    "FEATURE_PIPELINE_ASSETS",
    "FEATURE_PIPELINE_JOBS",
    "QUALITY_ASSETS",
    "MONITORING_ASSETS",
    "EVALUATION_ASSETS",
    "EVALUATION_JOBS",
    "EVALUATION_SCHEDULES",
    "PERFORMANCE_ASSETS",
    "STREAMING_SENSORS",
    "AUTO_RETRAIN_SENSORS",
    "ICEBERG_SNAPSHOT_SENSORS",
    "ROLLBACK_SYNC_JOBS",
    "WEBHOOK_SENSORS",
    "WEBHOOK_JOBS",
]
