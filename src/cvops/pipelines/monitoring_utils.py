"""
CVOps Monitoring Utilities
==========================

Monitoring utilities for CVOps (computer vision) platform.
Uses shared utilities from src.core.monitoring.

Standardized Prometheus metrics collection for:
- Detection predictions
- Model performance (mAP, precision, recall)
- Image processing metrics
- Pipeline execution

For shared utilities, decorators, and base classes, see:
    from src.core.monitoring import (
        get_or_create_counter,
        get_or_create_histogram,
        track_execution,
        MonitoringMixin,
    )
"""

from prometheus_client import Counter, Histogram, Gauge, Summary
from functools import wraps
import time
from typing import Callable, Dict, Any
import logging

# Import shared utilities from core
from src.core.monitoring import (
    get_or_create_counter,
    get_or_create_histogram,
    get_or_create_gauge,
    LATENCY_BUCKETS_MEDIUM,
    LATENCY_BUCKETS_SLOW,
    PROBABILITY_BUCKETS,
    SIZE_BUCKETS_LARGE,
    COUNT_BUCKETS,
    track_execution,
    safe_observe,
    safe_increment,
    safe_set_gauge,
    # Re-export common metrics for convenience
    pipeline_execution_counter,
    pipeline_execution_duration,
    data_quality_check,
    error_counter,
)

logger = logging.getLogger(__name__)

# ============================================================================
# CVOPS-SPECIFIC METRICS
# ============================================================================

# Detection Metrics
cv_detection_counter = Counter(
    'cv_detections_total',
    'Total number of object detections',
    ['model_name', 'model_version', 'class_name', 'status']
)

cv_detection_latency = Histogram(
    'cv_detection_latency_seconds',
    'Detection inference latency in seconds',
    ['model_name', 'model_version'],
    buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
)

cv_detection_confidence = Histogram(
    'cv_detection_confidence',
    'Detection confidence scores',
    ['model_name', 'model_version', 'class_name'],
    buckets=[0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
)

cv_objects_per_image = Histogram(
    'cv_objects_per_image',
    'Number of objects detected per image',
    ['model_name'],
    buckets=[0, 1, 2, 5, 10, 20, 50, 100]
)

# Image Processing Metrics
cv_images_processed = Counter(
    'cv_images_processed_total',
    'Total images processed',
    ['stage', 'status']  # stage: ingestion, detection, annotation, training
)

cv_image_size_bytes = Histogram(
    'cv_image_size_bytes',
    'Image file sizes in bytes',
    ['source_type'],
    buckets=[1e4, 5e4, 1e5, 5e5, 1e6, 5e6, 1e7]  # 10KB to 10MB
)

cv_image_dimensions = Histogram(
    'cv_image_dimensions_pixels',
    'Image dimensions (width * height)',
    ['source_type'],
    buckets=[1e5, 5e5, 1e6, 2e6, 5e6, 1e7]  # 100K to 10M pixels
)

# Model Performance Metrics
cv_model_map50 = Gauge(
    'cv_model_map50',
    'Model mAP@0.5',
    ['model_name', 'model_version']
)

cv_model_map50_95 = Gauge(
    'cv_model_map50_95',
    'Model mAP@0.5:0.95',
    ['model_name', 'model_version']
)

cv_model_precision = Gauge(
    'cv_model_precision',
    'Model precision',
    ['model_name', 'model_version', 'class_name']
)

cv_model_recall = Gauge(
    'cv_model_recall',
    'Model recall',
    ['model_name', 'model_version', 'class_name']
)

# Data Quality Metrics
cv_data_quality = Gauge(
    'cv_data_quality_passed',
    'Data quality check passed (0 or 1)',
    ['layer', 'check_name']
)

cv_image_count = Gauge(
    'cv_image_count',
    'Number of images in dataset',
    ['layer', 'table_name']
)

cv_annotation_count = Gauge(
    'cv_annotation_count',
    'Number of annotations',
    ['source', 'class_name']  # source: human, model
)

cv_class_distribution = Gauge(
    'cv_class_distribution',
    'Number of samples per class',
    ['dataset', 'class_name']
)

# Drift Metrics
cv_feature_drift = Gauge(
    'cv_feature_drift_score',
    'Feature drift score (e.g., image brightness, contrast)',
    ['feature_name']
)

cv_prediction_drift = Gauge(
    'cv_prediction_drift_detected',
    'Prediction drift detected (0 or 1)',
    ['model_name']
)

# Pipeline Metrics (same pattern as MLOps)
cv_pipeline_duration = Histogram(
    'cv_pipeline_duration_seconds',
    'CVOps pipeline execution duration',
    ['pipeline_name', 'asset_name', 'status'],
    buckets=[1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600]
)

cv_pipeline_execution_counter = Counter(
    'cv_pipeline_executions_total',
    'Total CVOps pipeline executions',
    ['pipeline_name', 'asset_name', 'status']
)

# Infrastructure Metrics
cv_service_up = Gauge(
    'cv_service_up',
    'CV service is up (1) or down (0)',
    ['service_name']
)

cv_model_loaded = Gauge(
    'cv_model_loaded',
    'Currently loaded model info',
    ['model_name', 'model_version', 'device']
)

cv_gpu_memory_used = Gauge(
    'cv_gpu_memory_used_bytes',
    'GPU memory used for inference',
    ['device']
)


# ============================================================================
# DECORATORS FOR AUTOMATIC INSTRUMENTATION
# ============================================================================

def track_detection(model_name: str, model_version: str):
    """
    Decorator to track detection metrics automatically.
    
    Example:
        @track_detection(model_name="yolov8n", model_version="v1.0")
        def detect(image):
            detections = model.predict(image)
            return {"detections": detections, "count": len(detections)}
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            status = "success"
            
            try:
                result = func(*args, **kwargs)
                
                # Extract detection info
                if isinstance(result, dict):
                    detections = result.get("detections", [])
                    count = result.get("count", len(detections))
                    
                    # Track objects per image
                    cv_objects_per_image.labels(
                        model_name=model_name
                    ).observe(count)
                    
                    # Track per-class detections
                    for det in detections:
                        class_name = det.get("class", "unknown")
                        confidence = det.get("confidence", 0.0)
                        
                        cv_detection_counter.labels(
                            model_name=model_name,
                            model_version=model_version,
                            class_name=class_name,
                            status=status
                        ).inc()
                        
                        cv_detection_confidence.labels(
                            model_name=model_name,
                            model_version=model_version,
                            class_name=class_name
                        ).observe(confidence)
                
                return result
                
            except Exception as e:
                status = "error"
                logger.error(f"Detection error: {e}")
                raise
                
            finally:
                duration = time.time() - start_time
                cv_detection_latency.labels(
                    model_name=model_name,
                    model_version=model_version
                ).observe(duration)
        
        return wrapper
    return decorator


def track_cvops_pipeline(pipeline_name: str, asset_name: str):
    """
    Decorator to track CVOps pipeline/asset execution.
    Mirrors track_pipeline from MLOps.
    
    Example:
        @asset
        @track_cvops_pipeline(pipeline_name="cvops", asset_name="run_detections")
        def run_detections():
            # ... computation
            return results
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
                logger.error(f"CVOps pipeline error: {e}")
                raise
                
            finally:
                duration = time.time() - start_time
                
                cv_pipeline_duration.labels(
                    pipeline_name=pipeline_name,
                    asset_name=asset_name,
                    status=status
                ).observe(duration)
                
                cv_pipeline_execution_counter.labels(
                    pipeline_name=pipeline_name,
                    asset_name=asset_name,
                    status=status
                ).inc()
        
        return wrapper
    return decorator


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def log_detection_batch(
    model_name: str,
    model_version: str,
    images_processed: int,
    total_detections: int,
    class_counts: Dict[str, int],
    avg_latency_ms: float
):
    """
    Log batch detection metrics.
    
    Args:
        model_name: Model name
        model_version: Model version
        images_processed: Number of images processed
        total_detections: Total detections
        class_counts: Dict of class_name -> count
        avg_latency_ms: Average latency per image in ms
    """
    # Log images processed
    cv_images_processed.labels(
        stage="detection",
        status="success"
    ).inc(images_processed)
    
    # Log detections per class
    for class_name, count in class_counts.items():
        cv_detection_counter.labels(
            model_name=model_name,
            model_version=model_version,
            class_name=class_name,
            status="success"
        ).inc(count)
    
    # Log average latency (convert to seconds)
    cv_detection_latency.labels(
        model_name=model_name,
        model_version=model_version
    ).observe(avg_latency_ms / 1000.0)


def log_model_metrics(
    model_name: str,
    model_version: str,
    map50: float,
    map50_95: float,
    per_class_metrics: Dict[str, Dict[str, float]] = None
):
    """
    Log model performance metrics.
    
    Args:
        model_name: Model name
        model_version: Model version
        map50: mAP@0.5
        map50_95: mAP@0.5:0.95
        per_class_metrics: Dict of class_name -> {precision, recall}
    """
    cv_model_map50.labels(
        model_name=model_name,
        model_version=model_version
    ).set(map50)
    
    cv_model_map50_95.labels(
        model_name=model_name,
        model_version=model_version
    ).set(map50_95)
    
    if per_class_metrics:
        for class_name, metrics in per_class_metrics.items():
            if 'precision' in metrics:
                cv_model_precision.labels(
                    model_name=model_name,
                    model_version=model_version,
                    class_name=class_name
                ).set(metrics['precision'])
            
            if 'recall' in metrics:
                cv_model_recall.labels(
                    model_name=model_name,
                    model_version=model_version,
                    class_name=class_name
                ).set(metrics['recall'])


def log_cv_data_quality(layer: str, check_name: str, passed: bool):
    """
    Log CVOps data quality check results.
    
    Args:
        layer: Data layer (bronze, silver, gold)
        check_name: Name of the quality check
        passed: Whether check passed
    """
    cv_data_quality.labels(
        layer=layer,
        check_name=check_name
    ).set(1 if passed else 0)


def log_cv_data_stats(layer: str, table_name: str, image_count: int):
    """
    Log CVOps data statistics.
    
    Args:
        layer: Data layer (bronze, silver, gold)
        table_name: Table name
        image_count: Number of images
    """
    cv_image_count.labels(
        layer=layer,
        table_name=table_name
    ).set(image_count)


def log_class_distribution(dataset: str, distribution: Dict[str, int]):
    """
    Log class distribution for a dataset.
    
    Args:
        dataset: Dataset name (e.g., "training", "validation")
        distribution: Dict of class_name -> count
    """
    for class_name, count in distribution.items():
        cv_class_distribution.labels(
            dataset=dataset,
            class_name=class_name
        ).set(count)


def log_annotation_stats(source: str, class_counts: Dict[str, int]):
    """
    Log annotation statistics.
    
    Args:
        source: Annotation source ("human" or "model")
        class_counts: Dict of class_name -> count
    """
    for class_name, count in class_counts.items():
        cv_annotation_count.labels(
            source=source,
            class_name=class_name
        ).set(count)


def log_image_drift(feature_name: str, drift_score: float):
    """
    Log image feature drift (e.g., brightness, contrast changes).
    
    Args:
        feature_name: Feature name (brightness, contrast, etc.)
        drift_score: Drift score (0-1)
    """
    cv_feature_drift.labels(
        feature_name=feature_name
    ).set(drift_score)


def set_model_loaded(model_name: str, model_version: str, device: str):
    """
    Set currently loaded model info.
    
    Args:
        model_name: Model name
        model_version: Model version
        device: Device (cpu, cuda:0, etc.)
    """
    cv_model_loaded.labels(
        model_name=model_name,
        model_version=model_version,
        device=device
    ).set(1)


def set_service_status(service_name: str, is_up: bool):
    """
    Set CV service status.
    
    Args:
        service_name: Service name
        is_up: Whether service is up
    """
    cv_service_up.labels(
        service_name=service_name
    ).set(1 if is_up else 0)


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

"""
# In your FastAPI detection endpoint:
from src.pipelines.cvops.monitoring_utils import track_detection, log_detection_batch

@app.post("/detect")
@track_detection(model_name="yolov8n", model_version="v1.0")
def detect_objects(image: UploadFile):
    # Process image
    detections = model.predict(image)
    
    return {
        "detections": detections,
        "count": len(detections)
    }

# In your Dagster asset:
from src.pipelines.cvops.monitoring_utils import track_cvops_pipeline, log_cv_data_stats

@asset
@track_cvops_pipeline(pipeline_name="cvops", asset_name="create_manifest")
def cvops_create_manifest():
    # Process images
    images = process_images()
    
    # Log stats
    log_cv_data_stats(
        layer="bronze",
        table_name="image_metadata",
        image_count=len(images)
    )
    
    return images

# After training:
from src.pipelines.cvops.monitoring_utils import log_model_metrics

log_model_metrics(
    model_name="yolov8n",
    model_version="v2.0",
    map50=0.85,
    map50_95=0.72,
    per_class_metrics={
        "person": {"precision": 0.9, "recall": 0.88},
        "car": {"precision": 0.85, "recall": 0.82}
    }
)
"""
