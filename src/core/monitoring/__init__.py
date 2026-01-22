"""
Core Monitoring Module
======================

Unified observability patterns for MLOps, CVOps, and LLMOps.

Usage:
    from src.core.monitoring import (
        get_or_create_counter,
        get_or_create_histogram,
        get_or_create_gauge,
        track_execution,
        MetricRegistry,
        MonitoringMixin,
    )

MLOps-specific metrics:
    from src.core.monitoring import (
        track_prediction,
        track_pipeline,
        log_feature_values,
        log_data_stats,
        log_data_quality,
    )
"""

from src.core.monitoring.metrics import (
    # Prometheus enabled flag
    PROMETHEUS_ENABLED,

    # Safe metric creation helpers
    get_or_create_counter,
    get_or_create_histogram,
    get_or_create_gauge,
    get_or_create_summary,
    get_or_create_metric,

    # Standard buckets
    LATENCY_BUCKETS_FAST,
    LATENCY_BUCKETS_MEDIUM,
    LATENCY_BUCKETS_SLOW,
    PROBABILITY_BUCKETS,
    SIZE_BUCKETS_SMALL,
    SIZE_BUCKETS_LARGE,
    COUNT_BUCKETS,

    # Decorators
    track_execution,
    track_async_execution,
    timed,

    # Common metrics
    pipeline_execution_counter,
    pipeline_execution_duration,
    data_quality_check,
    data_quality_score,
    row_count_gauge,
    feature_freshness_seconds,
    feature_null_rate,
    feature_drift_psi,
    dataset_drift_detected,
    error_counter,

    # Classes
    MetricRegistry,
    MonitoringMixin,
    OperationTracker,

    # Utility functions
    safe_observe,
    safe_increment,
    safe_set_gauge,
    get_metrics_response,
)

__all__ = [
    # Flag
    "PROMETHEUS_ENABLED",
    # Helpers
    "get_or_create_counter",
    "get_or_create_histogram",
    "get_or_create_gauge",
    "get_or_create_summary",
    "get_or_create_metric",
    # Buckets
    "LATENCY_BUCKETS_FAST",
    "LATENCY_BUCKETS_MEDIUM",
    "LATENCY_BUCKETS_SLOW",
    "PROBABILITY_BUCKETS",
    "SIZE_BUCKETS_SMALL",
    "SIZE_BUCKETS_LARGE",
    "COUNT_BUCKETS",
    # Decorators
    "track_execution",
    "track_async_execution",
    "timed",
    # Metrics
    "pipeline_execution_counter",
    "pipeline_execution_duration",
    "data_quality_check",
    "data_quality_score",
    "row_count_gauge",
    "feature_freshness_seconds",
    "feature_null_rate",
    "feature_drift_psi",
    "dataset_drift_detected",
    "error_counter",
    # Classes
    "MetricRegistry",
    "MonitoringMixin",
    "OperationTracker",
    # Utilities
    "safe_observe",
    "safe_increment",
    "safe_set_gauge",
    "get_metrics_response",
    # MLOps-specific (from mlops_metrics)
    "track_prediction",
    "track_pipeline",
    "log_feature_values",
    "log_data_stats",
    "log_data_quality",
    "log_model_performance",
    "log_fraud_metrics",
]

# MLOps-specific metrics (backward compatible with src.pipelines.monitoring_utils)
from src.core.monitoring.mlops_metrics import (
    track_prediction,
    track_pipeline,
    log_feature_values,
    log_data_stats,
    log_data_quality,
    log_model_performance,
    log_fraud_metrics,
    # MLOps metrics
    prediction_counter,
    prediction_latency,
    prediction_confidence,
    feature_value_gauge,
    feature_drift_gauge,
    dataset_drift_gauge,
    prediction_drift_gauge,
    data_quality_gauge,
    data_row_count,
    data_last_updated,
    pipeline_duration,
    model_precision,
    model_recall,
    model_f1,
    service_up,
    fraud_rate_gauge,
    false_positive_estimate_gauge,
)
