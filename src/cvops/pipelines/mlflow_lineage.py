"""
CVOps MLflow Lineage Tracking (Aligned with MLOps mlflow_lineage.py)
====================================================================

All CVOps pipeline stages log to a SINGLE experiment (cvops_object_detection) with:
- Tags to identify stage (bronze, silver, gold, annotation, training, inference)
- Shared data_version to link related runs
- Iceberg snapshot tracking
- Parent/child relationships for full traceability

Note: Core functionality is provided by UnifiedLineageTracker in pipelines.core.
This module provides backward-compatible stage-specific helpers for CVOps.
"""

import os
from datetime import datetime
from typing import Dict, List, Optional, Any

# Import shared functionality from core lineage module
from src.core.lineage import (
    # Stage constants
    STAGE_BRONZE,
    STAGE_SILVER,
    STAGE_ANNOTATION,
    STAGE_GOLD,
    STAGE_TRAINING,
    STAGE_INFERENCE,
    # Helpers
    get_iceberg_snapshot,
    generate_data_version as _base_generate_data_version,
    # Main tracker class
    UnifiedLineageTracker,
    create_cvops_tracker,
)
from src.core.lineage.mlflow_lineage import (
    get_table_snapshot_count,
    get_data_version_from_table,
)

# Re-export for backward compatibility
__all__ = [
    "STAGE_BRONZE",
    "STAGE_SILVER",
    "STAGE_ANNOTATION",
    "STAGE_GOLD",
    "STAGE_TRAINING",
    "STAGE_INFERENCE",
    "get_iceberg_snapshot",
    "get_table_snapshot_count",
    "generate_data_version",
    "get_data_version_from_table",
    "mlflow_cvops_run",
    "log_lineage_artifact",
    "log_cvops_bronze_lineage",
    "log_cvops_silver_lineage",
    "log_cvops_annotation_lineage",
    "log_cvops_gold_lineage",
    "log_cvops_training_lineage",
    "log_cvops_inference_lineage",
    "find_cvops_runs_by_data_version",
    "find_cvops_training_data_lineage",
]

# Configuration (backward compatibility)
CVOPS_EXPERIMENT = os.getenv("CVOPS_MLFLOW_EXPERIMENT", "cvops_object_detection")
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://exp-mlflow:5000")

# Create a singleton tracker for this domain
_tracker = None

def _get_tracker() -> UnifiedLineageTracker:
    """Get or create the CVOps tracker singleton."""
    global _tracker
    if _tracker is None:
        _tracker = create_cvops_tracker()
    return _tracker


def generate_data_version(commits: List[str], timestamp: datetime = None) -> str:
    """
    Generate a unique data version from commit IDs.
    CVOps uses 'cv_' prefix instead of 'dv_'.
    """
    base_version = _base_generate_data_version(commits, timestamp)
    # Replace dv_ prefix with cv_ for CVOps
    return base_version.replace("dv_", "cv_", 1)


# =============================================================================
# BACKWARD COMPATIBLE WRAPPERS
# =============================================================================

def mlflow_cvops_run(
    stage: str,
    run_name: str = None,
    data_version: str = None,
    dagster_run_id: str = None,
    parent_run_id: str = None,
    tags: Dict[str, str] = None,
):
    """
    Context manager for unified MLflow logging across all CVOps pipeline stages.
    Delegates to UnifiedLineageTracker.pipeline_run().
    """
    tracker = _get_tracker()
    return tracker.pipeline_run(
        stage=stage,
        run_name=run_name or f"cvops_{stage}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        data_version=data_version,
        dagster_run_id=dagster_run_id,
        parent_run_id=parent_run_id,
        tags=tags,
    )


def log_lineage_artifact(lineage_data: Dict[str, Any], artifact_name: str = "lineage.json"):
    """Log lineage data as a JSON artifact."""
    tracker = _get_tracker()
    tracker.log_lineage_artifact(lineage_data, artifact_name)


# =============================================================================
# STAGE-SPECIFIC HELPERS (CVOps versions)
# =============================================================================

def log_cvops_bronze_lineage(
    context,
    trino,
    commits: List[Dict],
    total_images: int,
    total_bytes: int,
    duration: float,
    metadata_table: str = None,
    errors: List[str] = None,
):
    """
    Log CVOps Bronze (image ingestion) to unified experiment.
    Mirrors log_bronze_lineage from MLOps.
    
    Args:
        context: Dagster context
        trino: Trino resource for snapshot queries
        commits: List of {source, commit, images, bytes}
        total_images: Total images ingested
        total_bytes: Total bytes processed
        duration: Duration in seconds
        metadata_table: Image metadata table name
        errors: List of errors
    """
    import mlflow
    
    commit_ids = [c.get('commit', '') for c in commits if c.get('commit')]
    data_version = generate_data_version(commit_ids) if commit_ids else None
    
    snapshot_id = get_iceberg_snapshot(trino, metadata_table) if metadata_table and trino else None
    
    with mlflow_cvops_run(
        stage=STAGE_BRONZE,
        data_version=data_version,
        dagster_run_id=context.run_id,
        tags={"layer": "bronze", "domain": "cvops"}
    ):
        # Parameters
        mlflow.log_param("total_images", total_images)
        mlflow.log_param("total_bytes_mb", round(total_bytes / (1024*1024), 2))
        mlflow.log_param("data_version", data_version)
        mlflow.log_param("commits_count", len(commits))
        
        if metadata_table:
            mlflow.log_param("metadata_table", metadata_table)
        
        if snapshot_id:
            mlflow.log_param("iceberg_snapshot_id", snapshot_id)
            mlflow.set_tag("iceberg_snapshot", snapshot_id)
        
        # Metrics
        mlflow.log_metric("images_ingested", total_images)
        mlflow.log_metric("bytes_processed_mb", total_bytes / (1024*1024))
        mlflow.log_metric("duration_seconds", duration)
        mlflow.log_metric("images_per_second", total_images / duration if duration > 0 else 0)
        
        if errors:
            mlflow.log_metric("error_count", len(errors))
        
        # Lineage artifact
        log_lineage_artifact({
            "stage": "bronze",
            "domain": "cvops",
            "data_version": data_version,
            "iceberg_snapshot_id": snapshot_id,
            "total_images": total_images,
            "total_bytes": total_bytes,
            "commits": commits[:20],
            "errors": errors[:10] if errors else [],
            "duration_seconds": duration
        })
        
        context.log.info(f"✓ Logged CVOps Bronze lineage (data_version: {data_version})")
        
        return data_version


def log_cvops_silver_lineage(
    context,
    trino,
    detections_table: str,
    images_processed: int,
    total_detections: int,
    model_name: str,
    model_version: str,
    source_commits: List[str],
    duration: float,
):
    """
    Log CVOps Silver (detection) processing to unified experiment.
    Mirrors log_silver_lineage from MLOps.
    """
    import mlflow
    
    data_version = generate_data_version(source_commits)
    snapshot_id = get_iceberg_snapshot(trino, detections_table) if trino else None
    
    with mlflow_cvops_run(
        stage=STAGE_SILVER,
        data_version=data_version,
        dagster_run_id=context.run_id,
        tags={"layer": "silver", "table": detections_table, "model": model_name}
    ):
        mlflow.log_param("detections_table", detections_table)
        mlflow.log_param("data_version", data_version)
        mlflow.log_param("model_name", model_name)
        mlflow.log_param("model_version", model_version)
        mlflow.log_param("source_commits", len(source_commits))
        
        if snapshot_id:
            mlflow.log_param("iceberg_snapshot_id", snapshot_id)
            mlflow.set_tag("iceberg_snapshot", snapshot_id)
        
        mlflow.log_metric("images_processed", images_processed)
        mlflow.log_metric("total_detections", total_detections)
        mlflow.log_metric("avg_detections_per_image", total_detections / images_processed if images_processed > 0 else 0)
        mlflow.log_metric("duration_seconds", duration)
        
        log_lineage_artifact({
            "stage": "silver",
            "domain": "cvops",
            "data_version": data_version,
            "iceberg_snapshot_id": snapshot_id,
            "detections_table": detections_table,
            "images_processed": images_processed,
            "total_detections": total_detections,
            "model_name": model_name,
            "model_version": model_version,
            "source_commits": source_commits[:20],
            "duration_seconds": duration
        })
        
        context.log.info(f"✓ Logged CVOps Silver lineage (data_version: {data_version})")
        
        return data_version


def log_cvops_annotation_lineage(
    context,
    trino,
    annotations_merged: int,
    annotations_table: str,
    labelstudio_project_id: int,
    duration: float,
    source_commits: List[str] = None,
):
    """
    Log CVOps annotation merge to unified experiment.
    Mirrors log_annotation_lineage from MLOps.
    """
    import mlflow
    
    data_version = generate_data_version(source_commits) if source_commits else None
    snapshot_id = get_iceberg_snapshot(trino, annotations_table) if trino else None
    
    with mlflow_cvops_run(
        stage=STAGE_ANNOTATION,
        data_version=data_version,
        dagster_run_id=context.run_id,
        tags={"layer": "annotation", "labelstudio_project": str(labelstudio_project_id)}
    ):
        mlflow.log_param("annotations_table", annotations_table)
        mlflow.log_param("labelstudio_project_id", labelstudio_project_id)
        if data_version:
            mlflow.log_param("data_version", data_version)
        
        if snapshot_id:
            mlflow.log_param("iceberg_snapshot_id", snapshot_id)
            mlflow.set_tag("iceberg_snapshot", snapshot_id)
        
        mlflow.log_metric("annotations_merged", annotations_merged)
        mlflow.log_metric("duration_seconds", duration)
        
        log_lineage_artifact({
            "stage": "annotation",
            "domain": "cvops",
            "data_version": data_version,
            "iceberg_snapshot_id": snapshot_id,
            "annotations_table": annotations_table,
            "annotations_merged": annotations_merged,
            "labelstudio_project_id": labelstudio_project_id,
            "duration_seconds": duration
        })
        
        context.log.info(f"✓ Logged CVOps annotation lineage (snapshot: {snapshot_id})")
        
        return data_version


def log_cvops_gold_lineage(
    context,
    trino,
    gold_table: str,
    records_promoted: int,
    class_distribution: Dict[str, int],
    source_commits: List[str],
    duration: float,
):
    """
    Log CVOps Gold (training data) promotion to unified experiment.
    Mirrors log_gold_lineage from MLOps.
    """
    import mlflow
    
    data_version = generate_data_version(source_commits)
    snapshot_id = get_iceberg_snapshot(trino, gold_table) if trino else None
    snapshot_count = get_table_snapshot_count(trino, gold_table) if trino else 0
    
    with mlflow_cvops_run(
        stage=STAGE_GOLD,
        data_version=data_version,
        dagster_run_id=context.run_id,
        tags={"layer": "gold", "table": gold_table}
    ):
        mlflow.log_param("gold_table", gold_table)
        mlflow.log_param("data_version", data_version)
        
        if snapshot_id:
            mlflow.log_param("iceberg_snapshot_id", snapshot_id)
            mlflow.set_tag("iceberg_snapshot", snapshot_id)
        
        mlflow.log_metric("records_promoted", records_promoted)
        mlflow.log_metric("duration_seconds", duration)
        mlflow.log_metric("snapshot_count", snapshot_count)
        
        # Log class distribution
        total_samples = sum(class_distribution.values())
        for class_name, count in class_distribution.items():
            mlflow.log_metric(f"class_{class_name}_count", count)
            if total_samples > 0:
                mlflow.log_metric(f"class_{class_name}_ratio", count / total_samples)
        
        mlflow.log_metric("num_classes", len(class_distribution))
        
        log_lineage_artifact({
            "stage": "gold",
            "domain": "cvops",
            "data_version": data_version,
            "iceberg_snapshot_id": snapshot_id,
            "snapshot_count": snapshot_count,
            "gold_table": gold_table,
            "records_promoted": records_promoted,
            "class_distribution": class_distribution,
            "source_commits": source_commits[:20],
            "duration_seconds": duration
        })
        
        context.log.info(f"✓ Logged CVOps Gold lineage (data_version: {data_version}, snapshot: {snapshot_id})")
        
        return data_version


def log_cvops_training_lineage(
    context,
    model_name: str,
    data_version: str,
    gold_table: str,
    training_images: int,
    val_images: int,
    metrics: Dict[str, float],
    model_params: Dict[str, Any],
    duration: float,
):
    """
    Log CVOps model training to unified experiment.
    Links to data via data_version.
    Mirrors log_training_lineage from MLOps.
    """
    import mlflow
    
    with mlflow_cvops_run(
        stage=STAGE_TRAINING,
        data_version=data_version,
        dagster_run_id=context.run_id,
        tags={
            "layer": "training",
            "model_name": model_name,
            "model_type": "object_detection"
        }
    ):
        # Data lineage
        mlflow.log_param("data_version", data_version)
        mlflow.log_param("gold_table", gold_table)
        mlflow.log_param("training_images", training_images)
        mlflow.log_param("val_images", val_images)
        
        # Model params
        for key, value in model_params.items():
            mlflow.log_param(f"model_{key}", value)
        
        # Metrics (mAP, precision, recall, etc.)
        for key, value in metrics.items():
            mlflow.log_metric(key, value)
        
        mlflow.log_metric("duration_seconds", duration)
        
        # Lineage artifact
        log_lineage_artifact({
            "stage": "training",
            "domain": "cvops",
            "model_name": model_name,
            "data_version": data_version,
            "gold_table": gold_table,
            "training_images": training_images,
            "val_images": val_images,
            "metrics": metrics,
            "model_params": model_params,
            "duration_seconds": duration
        })
        
        context.log.info(f"✓ Logged CVOps training lineage (data_version: {data_version})")


def log_cvops_inference_lineage(
    context,
    model_name: str,
    model_version: str,
    images_processed: int,
    total_detections: int,
    avg_latency_ms: float,
    duration: float,
):
    """
    Log CVOps inference batch to unified experiment.
    """
    import mlflow
    
    with mlflow_cvops_run(
        stage=STAGE_INFERENCE,
        dagster_run_id=context.run_id if hasattr(context, 'run_id') else None,
        tags={
            "layer": "inference",
            "model_name": model_name,
            "model_version": model_version
        }
    ):
        mlflow.log_param("model_name", model_name)
        mlflow.log_param("model_version", model_version)
        
        mlflow.log_metric("images_processed", images_processed)
        mlflow.log_metric("total_detections", total_detections)
        mlflow.log_metric("avg_latency_ms", avg_latency_ms)
        mlflow.log_metric("duration_seconds", duration)
        mlflow.log_metric("throughput_images_per_sec", images_processed / duration if duration > 0 else 0)
        
        context.log.info(f"✓ Logged CVOps inference lineage")


# =============================================================================
# QUERY HELPERS
# =============================================================================

def find_cvops_runs_by_data_version(data_version: str) -> List[Dict]:
    """
    Find all MLflow runs associated with a CVOps data version.
    Returns full pipeline lineage for that data.
    """
    import mlflow
    
    mlflow.set_tracking_uri(MLFLOW_URI)
    
    runs = mlflow.search_runs(
        experiment_names=[CVOPS_EXPERIMENT],
        filter_string=f"tags.data_version = '{data_version}'",
        order_by=["start_time ASC"]
    )
    
    return runs.to_dict('records') if not runs.empty else []


def find_cvops_training_data_lineage(model_run_id: str) -> Dict:
    """
    Given a CVOps model training run, find the complete data lineage.
    """
    import mlflow
    
    mlflow.set_tracking_uri(MLFLOW_URI)
    
    run = mlflow.get_run(model_run_id)
    data_version = run.data.tags.get("data_version")
    
    if not data_version:
        return {"error": "No data_version tag found on training run"}
    
    related_runs = find_cvops_runs_by_data_version(data_version)
    
    lineage = {
        "data_version": data_version,
        "training_run_id": model_run_id,
        "domain": "cvops",
        "stages": {}
    }
    
    for run in related_runs:
        stage = run.get("tags.pipeline_stage", "unknown")
        if stage not in lineage["stages"]:
            lineage["stages"][stage] = []
        lineage["stages"][stage].append({
            "run_id": run.get("run_id"),
            "start_time": run.get("start_time"),
            "status": run.get("status")
        })
    
    return lineage
