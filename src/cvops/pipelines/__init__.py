"""
CVOps Module - Computer Vision Operations
=========================================

Complete CVOps pipeline for object detection following MLOps patterns.

Components:
- resources.py: CV-specific resources (YOLO, image storage, etc.)
- assets.py: Dagster pipeline assets
- sensors.py: LakeFS webhook sensors and triggers
- mlflow_lineage.py: MLflow lineage tracking (aligned with MLOps)
- checks.py: Asset checks for data quality
- monitoring_utils.py: Prometheus metrics utilities

Usage:
    from src.pipelines.cvops import CVOPS_ASSETS, CVOPS_JOBS, CVOPS_RESOURCES, CVOPS_SENSORS, CVOPS_CHECKS
"""

from src.cvops.pipelines.resources import (
    # Resources
    YOLOModelResource,
    CVImageStorageResource,
    CVIcebergResource,
    CVKafkaResource,
    CVKafkaIngestResource,
    CVOPS_RESOURCES,

    # Utilities
    compute_image_quality,
    compute_perceptual_hash,
    preprocess_image,
)

from src.cvops.pipelines.assets import (
    # Assets
    cvops_init_lakefs,
    cvops_init_tables,
    cvops_create_manifest,
    cvops_run_detections,
    cvops_export_to_labelstudio,
    cvops_merge_annotations,
    cvops_create_training_data,
    cvops_train_model,
    # Kafka ingestion assets
    cvops_init_kafka_topics,
    cvops_batch_ingest_scan,

    # Jobs
    cvops_init_job,
    cvops_ingestion_job,
    cvops_annotation_job,
    cvops_training_job,
    cvops_full_job,
    cvops_kafka_ingestion_job,

    # Collections
    CVOPS_ASSETS,
    CVOPS_JOBS,

    # Config
    CVTrainingConfig,
    BatchIngestConfig,
)

from src.cvops.pipelines.sensors import (
    # Sensors
    cvops_webhook_sensor,
    cvops_image_backlog_sensor,
    cvops_annotation_sensor,
    # Kafka ingestion sensors
    cvops_kafka_lag_sensor,
    cvops_kafka_dlq_sensor,
    cvops_kafka_ingest_health_sensor,

    # Cleanup job and op
    cvops_cleanup_job,
    cvops_cleanup_reverted_commit,

    # Collections
    CVOPS_SENSORS,
)

from src.cvops.pipelines.checks import (
    CVOPS_CHECKS,
)

from src.cvops.pipelines.mlflow_lineage import (
    # Context managers
    mlflow_cvops_run,

    # Stage-specific loggers
    log_cvops_bronze_lineage,
    log_cvops_silver_lineage,
    log_cvops_annotation_lineage,
    log_cvops_gold_lineage,
    log_cvops_training_lineage,
    log_cvops_inference_lineage,

    # Helpers
    generate_data_version,
    get_iceberg_snapshot,
    get_data_version_from_table,
    find_cvops_runs_by_data_version,
    find_cvops_training_data_lineage,
)

from src.cvops.pipelines.monitoring_utils import (
    # Decorators
    track_detection,
    track_cvops_pipeline,

    # Logging helpers
    log_detection_batch,
    log_model_metrics,
    log_cv_data_quality,
    log_cv_data_stats,
    log_class_distribution,
    log_annotation_stats,
    log_image_drift,
    set_model_loaded,
    set_service_status,
)

__all__ = [
    # Resources
    "YOLOModelResource",
    "CVImageStorageResource",
    "CVIcebergResource",
    "CVKafkaResource",
    "CVKafkaIngestResource",
    "CVOPS_RESOURCES",

    # Utilities
    "compute_image_quality",
    "compute_perceptual_hash",
    "preprocess_image",

    # Assets
    "cvops_init_lakefs",
    "cvops_init_tables",
    "cvops_create_manifest",
    "cvops_run_detections",
    "cvops_export_to_labelstudio",
    "cvops_merge_annotations",
    "cvops_create_training_data",
    "cvops_train_model",
    # Kafka ingestion assets
    "cvops_init_kafka_topics",
    "cvops_batch_ingest_scan",

    # Jobs
    "cvops_init_job",
    "cvops_ingestion_job",
    "cvops_annotation_job",
    "cvops_training_job",
    "cvops_full_job",
    "cvops_cleanup_job",
    "cvops_kafka_ingestion_job",

    # Sensors
    "cvops_webhook_sensor",
    "cvops_image_backlog_sensor",
    "cvops_annotation_sensor",
    "cvops_cleanup_reverted_commit",
    # Kafka ingestion sensors
    "cvops_kafka_lag_sensor",
    "cvops_kafka_dlq_sensor",
    "cvops_kafka_ingest_health_sensor",

    # Checks
    "CVOPS_CHECKS",

    # Collections
    "CVOPS_ASSETS",
    "CVOPS_JOBS",
    "CVOPS_SENSORS",

    # Config
    "CVTrainingConfig",
    "BatchIngestConfig",

    # MLflow lineage
    "mlflow_cvops_run",
    "log_cvops_bronze_lineage",
    "log_cvops_silver_lineage",
    "log_cvops_annotation_lineage",
    "log_cvops_gold_lineage",
    "log_cvops_training_lineage",
    "log_cvops_inference_lineage",
    "generate_data_version",
    "get_iceberg_snapshot",
    "find_cvops_runs_by_data_version",

    # Monitoring
    "track_detection",
    "track_cvops_pipeline",
    "log_detection_batch",
    "log_model_metrics",
    "log_cv_data_quality",
    "log_cv_data_stats",
]
