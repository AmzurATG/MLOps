# src/core/monitoring/mlops_metrics.py
"""
MLOps Monitoring Utilities
==========================

Monitoring utilities for MLOps (fraud detection) platform.

Usage:
    from src.core.monitoring import (
        track_prediction,
        track_pipeline,
        log_feature_values,
        log_data_stats,
        log_data_quality,
    )
"""

from functools import wraps
import time
from typing import Callable, Dict, Any
import logging

# Import shared utilities from metrics module (avoid circular import)
from src.core.monitoring.metrics import (
    get_or_create_counter,
    get_or_create_histogram,
    get_or_create_gauge,
    get_or_create_summary,
    LATENCY_BUCKETS_FAST,
    LATENCY_BUCKETS_MEDIUM,
    PROBABILITY_BUCKETS,
    track_execution,
    safe_observe,
    safe_increment,
    safe_set_gauge,
    # Re-export common metrics
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
)

logger = logging.getLogger(__name__)

# Backward compatibility aliases
_get_or_create_counter = get_or_create_counter
_get_or_create_histogram = get_or_create_histogram
_get_or_create_gauge = get_or_create_gauge


# ============================================================================
# STANDARD METRICS (Reusable across all services)
# ============================================================================

# Model Prediction Metrics
prediction_counter = _get_or_create_counter(
    'ml_predictions_total',
    'Total number of predictions',
    ['model_name', 'model_version', 'prediction_class', 'status']
)

prediction_latency = _get_or_create_histogram(
    'ml_prediction_latency_seconds',
    'Prediction latency in seconds',
    ['model_name', 'model_version'],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
)

prediction_confidence = _get_or_create_histogram(
    'ml_prediction_confidence',
    'Prediction confidence score',
    ['model_name', 'model_version'],
    buckets=[0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
)

# Feature Metrics
feature_value_gauge = _get_or_create_gauge(
    'feature_value',
    'Current feature value',
    ['feature_name', 'model_name']
)

feature_null_rate = _get_or_create_gauge(
    'feature_null_rate',
    'Percentage of null values in feature',
    ['feature_name', 'layer']
)

# Drift Metrics
feature_drift_gauge = _get_or_create_gauge(
    'feature_drift_psi',
    'Population Stability Index for feature',
    ['feature']  # Changed from ['feature_name', 'model_name'] to match Grafana dashboard
)

dataset_drift_gauge = _get_or_create_gauge(
    'dataset_drift_detected',
    'Whether dataset drift is detected (0 or 1)',
    ['model_name']
)

prediction_drift_gauge = _get_or_create_gauge(
    'prediction_drift_score',
    'Prediction distribution drift score',
    ['model_name']
)

# Data Quality Metrics
data_quality_gauge = _get_or_create_gauge(
    'data_quality_passed',
    'Data quality check passed (0 or 1)',
    ['layer', 'check_name']
)

data_row_count = _get_or_create_gauge(
    'data_row_count',
    'Number of rows in dataset',
    ['layer', 'table_name']
)

data_last_updated = _get_or_create_gauge(
    'data_last_updated_timestamp',
    'Unix timestamp of last data update',
    ['layer', 'table_name']
)

# Pipeline Metrics (MLOps-specific, unique names to avoid conflict with core)
pipeline_duration = _get_or_create_histogram(
    'mlops_pipeline_duration_seconds',
    'Pipeline execution duration',
    ['pipeline_name', 'asset_name', 'status'],
    buckets=[1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600]
)

pipeline_execution_counter = _get_or_create_counter(
    'mlops_pipeline_executions_total',
    'Total pipeline executions',
    ['pipeline_name', 'asset_name', 'status']
)

# Model Performance Metrics (delayed, when labels arrive)
model_precision = _get_or_create_gauge(
    'model_precision',
    'Model precision',
    ['model_name', 'model_version', 'label_delay']
)

model_recall = _get_or_create_gauge(
    'model_recall',
    'Model recall',
    ['model_name', 'model_version', 'label_delay']
)

model_f1 = _get_or_create_gauge(
    'model_f1_score',
    'Model F1 score',
    ['model_name', 'model_version', 'label_delay']
)

# Infrastructure Metrics
service_up = _get_or_create_gauge(
    'service_up',
    'Service is up (1) or down (0)',
    ['service_name']
)

# Business Metrics
fraud_rate_gauge = _get_or_create_gauge(
    'fraud_detection_rate',
    'Current fraud detection rate',
    ['time_window']
)

false_positive_estimate_gauge = _get_or_create_gauge(
    'fraud_false_positive_estimate',
    'Estimated false positive count',
    ['time_window']
)

# ============================================================================
# DECORATORS FOR AUTOMATIC INSTRUMENTATION
# ============================================================================

def track_prediction(model_name: str, model_version: str):
    """
    Decorator to track prediction metrics automatically.
    
    Example:
        @track_prediction(model_name="fraud_detector", model_version="v1.0")
        def predict(features):
            prediction = model.predict(features)
            confidence = model.predict_proba(features)[0][1]
            return {"prediction": prediction, "confidence": confidence}
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            status = "success"
            prediction_class = "unknown"
            
            try:
                result = func(*args, **kwargs)
                
                # Extract prediction and confidence
                if isinstance(result, dict):
                    prediction_class = str(result.get("prediction", "unknown"))
                    confidence = result.get("confidence", 0.0)
                    
                    # Track metrics
                    prediction_confidence.labels(
                        model_name=model_name,
                        model_version=model_version
                    ).observe(confidence)
                
                return result
                
            except Exception as e:
                status = "error"
                logger.error(f"Prediction error: {e}")
                raise
                
            finally:
                # Track latency
                duration = time.time() - start_time
                prediction_latency.labels(
                    model_name=model_name,
                    model_version=model_version
                ).observe(duration)
                
                # Track count
                prediction_counter.labels(
                    model_name=model_name,
                    model_version=model_version,
                    prediction_class=prediction_class,
                    status=status
                ).inc()
        
        return wrapper
    return decorator


def track_pipeline(pipeline_name: str, asset_name: str):
    """
    Decorator to track Dagster pipeline/asset execution.
    
    Example:
        @asset
        @track_pipeline(pipeline_name="mlops", asset_name="feature_engineering")
        def compute_features():
            # ... computation
            return features
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            status = "success"
            
            try:
                result = func(*args, **kwargs)
                return result
                
            except Exception as e:
                status = "error"
                logger.error(f"Pipeline error: {e}")
                raise
                
            finally:
                duration = time.time() - start_time
                
                # Track duration
                pipeline_duration.labels(
                    pipeline_name=pipeline_name,
                    asset_name=asset_name,
                    status=status
                ).observe(duration)
                
                # Track execution count
                pipeline_execution_counter.labels(
                    pipeline_name=pipeline_name,
                    asset_name=asset_name,
                    status=status
                ).inc()
        
        return wrapper
    return decorator


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def log_feature_values(features: Dict[str, float], model_name: str):
    """
    Log feature values to Prometheus for monitoring.
    
    Args:
        features: Dict of feature_name -> feature_value
        model_name: Name of the model using these features
    """
    for feature_name, feature_value in features.items():
        if feature_value is not None:
            feature_value_gauge.labels(
                feature_name=feature_name,
                model_name=model_name
            ).set(float(feature_value))


def log_data_quality(layer: str, check_name: str, passed: bool):
    """
    Log data quality check results.
    
    Args:
        layer: Data layer (bronze, silver, gold)
        check_name: Name of the quality check
        passed: Whether check passed (True/False)
    """
    data_quality_gauge.labels(
        layer=layer,
        check_name=check_name
    ).set(1 if passed else 0)


def log_data_stats(layer: str, table_name: str, row_count: int):
    """
    Log data statistics.
    
    Args:
        layer: Data layer (bronze, silver, gold)
        table_name: Table name
        row_count: Number of rows
    """
    data_row_count.labels(
        layer=layer,
        table_name=table_name
    ).set(row_count)
    
    data_last_updated.labels(
        layer=layer,
        table_name=table_name
    ).set(time.time())


def log_model_performance(
    model_name: str,
    model_version: str,
    precision: float,
    recall: float,
    f1: float,
    label_delay: str = "30d"
):
    """
    Log model performance metrics when labels arrive.
    
    Args:
        model_name: Model name
        model_version: Model version
        precision: Precision score
        recall: Recall score
        f1: F1 score
        label_delay: Label delay (e.g., "1h", "7d", "30d")
    """
    model_precision.labels(
        model_name=model_name,
        model_version=model_version,
        label_delay=label_delay
    ).set(precision)
    
    model_recall.labels(
        model_name=model_name,
        model_version=model_version,
        label_delay=label_delay
    ).set(recall)
    
    model_f1.labels(
        model_name=model_name,
        model_version=model_version,
        label_delay=label_delay
    ).set(f1)


def log_fraud_metrics(fraud_rate: float, false_positive_count: int, time_window: str = "1h"):
    """
    Log fraud-specific business metrics.

    Args:
        fraud_rate: Current fraud detection rate (0.0 to 1.0)
        false_positive_count: Estimated false positive count
        time_window: Time window for metrics
    """
    fraud_rate_gauge.labels(time_window=time_window).set(fraud_rate)
    false_positive_estimate_gauge.labels(time_window=time_window).set(false_positive_count)


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

"""
# In your FastAPI endpoint:
from src.core.monitoring import track_prediction, log_feature_values

@app.post("/predict")
@track_prediction(model_name="fraud_detector", model_version="v1.0")
def predict_fraud(transaction: Transaction):
    features = get_features(transaction)
    
    # Log feature values
    log_feature_values(features, model_name="fraud_detector")
    
    # Make prediction
    prediction = model.predict(features)
    confidence = model.predict_proba(features)[0][1]
    
    return {
        "prediction": prediction,
        "confidence": confidence
    }

# In your Dagster asset:
from src.core.monitoring import track_pipeline, log_data_stats

@asset
@track_pipeline(pipeline_name="mlops", asset_name="bronze_to_silver")
def transform_bronze_to_silver():
    df = load_bronze_data()
    
    # Transform
    df_silver = transform(df)
    
    # Log stats
    log_data_stats(
        layer="silver",
        table_name="transactions",
        row_count=len(df_silver)
    )
    
    return df_silver
"""