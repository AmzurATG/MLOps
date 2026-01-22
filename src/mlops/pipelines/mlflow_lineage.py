"""
Unified MLflow Lineage Tracking for MLOps (Fraud Detection)

All pipeline stages log to a SINGLE experiment (fraud_detection) with:
- Tags to identify stage (bronze, silver, gold, annotation, training)
- Shared data_version to link related runs
- Parent/child relationships for full traceability

Benefits:
- Single place to see entire pipeline history
- Can trace: "Which data version → trained this model?"
- Unified search across all stages

Note: Core functionality is provided by UnifiedLineageTracker in src.core.lineage.
This module provides stage-specific helpers for MLOps pipeline logging.
"""

import os
from datetime import datetime
from typing import Dict, List, Optional, Any

# Import shared functionality from base module
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
    get_table_snapshot_count,
    generate_data_version,
    get_data_version_from_table,
    # Main tracker class
    UnifiedLineageTracker,
    create_mlops_tracker,
)

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
    "mlflow_pipeline_run",
    "log_lineage_artifact",
    "log_bronze_lineage",
    "log_silver_lineage",
    "log_annotation_lineage",
    "log_gold_lineage",
    "log_training_lineage",
    "find_runs_by_data_version",
    "find_training_data_lineage",
]

# Configuration
UNIFIED_EXPERIMENT = os.getenv("MLFLOW_EXPERIMENT", "fraud_detection")
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://exp-mlflow:5000")

# Create a singleton tracker for this domain
_tracker = None

def _get_tracker() -> UnifiedLineageTracker:
    """Get or create the MLOps tracker singleton."""
    global _tracker
    if _tracker is None:
        _tracker = create_mlops_tracker()
    return _tracker


# ═══════════════════════════════════════════════════════════════════════════════
# TRACKER WRAPPERS
# ═══════════════════════════════════════════════════════════════════════════════

def mlflow_pipeline_run(
    stage: str,
    run_name: str = None,
    data_version: str = None,
    dagster_run_id: str = None,
    parent_run_id: str = None,
    tags: Dict[str, str] = None,
):
    """
    Context manager for unified MLflow logging across all pipeline stages.
    Delegates to UnifiedLineageTracker.pipeline_run().
    """
    tracker = _get_tracker()
    return tracker.pipeline_run(
        stage=stage,
        run_name=run_name,
        data_version=data_version,
        dagster_run_id=dagster_run_id,
        parent_run_id=parent_run_id,
        tags=tags,
    )


def log_lineage_artifact(
    lineage_data: Dict[str, Any],
    artifact_name: str = "lineage.json"
):
    """Log lineage data as a JSON artifact."""
    tracker = _get_tracker()
    tracker.log_lineage_artifact(lineage_data, artifact_name)


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE-SPECIFIC HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def log_bronze_lineage(
    context,
    trino,
    commits: List[Dict],
    total_records: int,
    total_tables: int,
    duration: float,
    bronze_table: str = None,
    errors: List[str] = None,
):
    """
    Log Bronze ingestion to unified experiment.
    
    Args:
        context: Dagster context
        trino: Trino resource for snapshot queries
        commits: List of {source, commit, tables, records}
        total_records: Total records ingested
        total_tables: Total tables processed
        duration: Duration in seconds
        bronze_table: Bronze table name for snapshot lookup
        errors: List of errors
    """
    import mlflow
    
    # Generate data version from commits
    commit_ids = [c['commit'] for c in commits if c.get('commit')]
    data_version = generate_data_version(commit_ids)
    
    # Get Iceberg snapshot ID
    snapshot_id = None
    if bronze_table and trino:
        snapshot_id = get_iceberg_snapshot(trino, bronze_table)
    
    with mlflow_pipeline_run(
        stage=STAGE_BRONZE,
        data_version=data_version,
        dagster_run_id=context.run_id,
        tags={"layer": "bronze"}
    ):
        # Params
        mlflow.log_param("sources_count", len(commits))
        mlflow.log_param("tables_count", total_tables)
        mlflow.log_param("total_records", total_records)
        mlflow.log_param("data_version", data_version)
        
        # Snapshot tracking
        if snapshot_id:
            mlflow.log_param("iceberg_snapshot_id", snapshot_id)
            mlflow.set_tag("iceberg_snapshot", snapshot_id)
        
        if bronze_table:
            mlflow.log_param("bronze_table", bronze_table)
        
        # Log each commit (LakeFS commits)
        for i, commit_info in enumerate(commits[:10]):  # Limit to 10
            mlflow.log_param(f"lakefs_commit_{i}", commit_info.get('commit', '')[:12])
            mlflow.log_param(f"lakefs_commit_{i}_source", commit_info.get('source', ''))
        
        # Metrics
        mlflow.log_metric("records_ingested", total_records)
        mlflow.log_metric("tables_processed", total_tables)
        mlflow.log_metric("duration_seconds", duration)
        mlflow.log_metric("error_count", len(errors) if errors else 0)
        
        # Lineage artifact
        log_lineage_artifact({
            "stage": "bronze",
            "data_version": data_version,
            "iceberg_snapshot_id": snapshot_id,
            "bronze_table": bronze_table,
            "timestamp": datetime.now().isoformat(),
            "dagster_run_id": context.run_id,
            "commits": commits,
            "total_records": total_records,
            "total_tables": total_tables,
            "duration_seconds": duration,
            "errors": errors or []
        })
        
        context.log.info(f"✓ Logged Bronze lineage (data_version: {data_version})")
        
        return data_version


def log_silver_lineage(
    context,
    silver_table: str,
    records_processed: int,
    source_commits: List[str],
    duration: float,
):
    """Log Silver processing to unified experiment."""
    import mlflow
    
    data_version = generate_data_version(source_commits)
    
    with mlflow_pipeline_run(
        stage=STAGE_SILVER,
        data_version=data_version,
        dagster_run_id=context.run_id,
        tags={"layer": "silver", "table": silver_table}
    ):
        mlflow.log_param("silver_table", silver_table)
        mlflow.log_param("data_version", data_version)
        mlflow.log_param("source_commits", len(source_commits))
        
        mlflow.log_metric("records_processed", records_processed)
        mlflow.log_metric("duration_seconds", duration)
        
        log_lineage_artifact({
            "stage": "silver",
            "data_version": data_version,
            "silver_table": silver_table,
            "records_processed": records_processed,
            "source_commits": source_commits[:20],  # Limit
            "duration_seconds": duration
        })
        
        context.log.info(f"✓ Logged Silver lineage (data_version: {data_version})")
        
        return data_version


def log_annotation_lineage(
    context,
    trino,
    annotations_merged: int,
    silver_table: str,
    labelstudio_project_id: int,
    duration: float,
    source_commits: List[str] = None,
):
    """Log annotation merge to unified experiment."""
    import mlflow
    
    data_version = generate_data_version(source_commits) if source_commits else None
    
    # Get Iceberg snapshot ID
    snapshot_id = get_iceberg_snapshot(trino, silver_table) if trino else None
    
    with mlflow_pipeline_run(
        stage=STAGE_ANNOTATION,
        data_version=data_version,
        dagster_run_id=context.run_id,
        tags={"layer": "annotation", "labelstudio_project": str(labelstudio_project_id)}
    ):
        mlflow.log_param("silver_table", silver_table)
        mlflow.log_param("labelstudio_project_id", labelstudio_project_id)
        if data_version:
            mlflow.log_param("data_version", data_version)
        
        # Snapshot tracking
        if snapshot_id:
            mlflow.log_param("iceberg_snapshot_id", snapshot_id)
            mlflow.set_tag("iceberg_snapshot", snapshot_id)
        
        mlflow.log_metric("annotations_merged", annotations_merged)
        mlflow.log_metric("duration_seconds", duration)
        
        log_lineage_artifact({
            "stage": "annotation",
            "data_version": data_version,
            "iceberg_snapshot_id": snapshot_id,
            "silver_table": silver_table,
            "annotations_merged": annotations_merged,
            "labelstudio_project_id": labelstudio_project_id,
            "duration_seconds": duration
        })
        
        context.log.info(f"✓ Logged annotation lineage (snapshot: {snapshot_id})")
        
        return data_version


def log_gold_lineage(
    context,
    trino,
    gold_table: str,
    records_promoted: int,
    fraud_count: int,
    legit_count: int,
    source_commits: List[str],
    duration: float,
):
    """Log Gold promotion to unified experiment."""
    import mlflow
    
    data_version = generate_data_version(source_commits)
    
    # Get Iceberg snapshot ID
    snapshot_id = get_iceberg_snapshot(trino, gold_table) if trino else None
    snapshot_count = get_table_snapshot_count(trino, gold_table) if trino else 0
    
    with mlflow_pipeline_run(
        stage=STAGE_GOLD,
        data_version=data_version,
        dagster_run_id=context.run_id,
        tags={"layer": "gold", "table": gold_table}
    ):
        mlflow.log_param("gold_table", gold_table)
        mlflow.log_param("data_version", data_version)
        
        # Snapshot tracking
        if snapshot_id:
            mlflow.log_param("iceberg_snapshot_id", snapshot_id)
            mlflow.set_tag("iceberg_snapshot", snapshot_id)
        
        mlflow.log_metric("records_promoted", records_promoted)
        mlflow.log_metric("fraud_count", fraud_count)
        mlflow.log_metric("legit_count", legit_count)
        mlflow.log_metric("fraud_rate", fraud_count / records_promoted if records_promoted > 0 else 0)
        mlflow.log_metric("duration_seconds", duration)
        mlflow.log_metric("snapshot_count", snapshot_count)
        
        log_lineage_artifact({
            "stage": "gold",
            "data_version": data_version,
            "iceberg_snapshot_id": snapshot_id,
            "snapshot_count": snapshot_count,
            "gold_table": gold_table,
            "records_promoted": records_promoted,
            "fraud_count": fraud_count,
            "legit_count": legit_count,
            "source_commits": source_commits[:20],
            "duration_seconds": duration
        })
        
        context.log.info(f"✓ Logged Gold lineage (data_version: {data_version}, snapshot: {snapshot_id})")
        
        return data_version


def log_training_lineage(
    context,
    model_name: str,
    data_version: str,
    gold_table: str,
    training_records: int,
    metrics: Dict[str, float],
    model_params: Dict[str, Any],
    duration: float,
):
    """
    Log model training to unified experiment.
    Links to data via data_version.
    """
    import mlflow
    
    with mlflow_pipeline_run(
        stage=STAGE_TRAINING,
        data_version=data_version,
        dagster_run_id=context.run_id,
        tags={
            "layer": "training",
            "model_name": model_name,
            "model_type": "fraud_detection"
        }
    ):
        # Data lineage
        mlflow.log_param("data_version", data_version)
        mlflow.log_param("gold_table", gold_table)
        mlflow.log_param("training_records", training_records)
        
        # Model params
        for key, value in model_params.items():
            mlflow.log_param(f"model_{key}", value)
        
        # Metrics
        for key, value in metrics.items():
            mlflow.log_metric(key, value)
        
        mlflow.log_metric("duration_seconds", duration)
        
        # Lineage artifact
        log_lineage_artifact({
            "stage": "training",
            "model_name": model_name,
            "data_version": data_version,
            "gold_table": gold_table,
            "training_records": training_records,
            "metrics": metrics,
            "model_params": model_params,
            "duration_seconds": duration
        })
        
        context.log.info(f"✓ Logged training lineage (data_version: {data_version})")


# ═══════════════════════════════════════════════════════════════════════════════
# QUERY HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def find_runs_by_data_version(data_version: str) -> List[Dict]:
    """
    Find all MLflow runs associated with a data version.
    Returns full pipeline lineage for that data.
    """
    import mlflow
    
    mlflow.set_tracking_uri(MLFLOW_URI)
    
    runs = mlflow.search_runs(
        experiment_names=[UNIFIED_EXPERIMENT],
        filter_string=f"tags.data_version = '{data_version}'",
        order_by=["start_time ASC"]
    )
    
    return runs.to_dict('records') if not runs.empty else []


def find_training_data_lineage(model_run_id: str) -> Dict:
    """
    Given a model training run, find the complete data lineage.
    """
    import mlflow
    
    mlflow.set_tracking_uri(MLFLOW_URI)
    
    # Get the training run
    run = mlflow.get_run(model_run_id)
    data_version = run.data.tags.get("data_version")
    
    if not data_version:
        return {"error": "No data_version tag found on training run"}
    
    # Find all related runs
    related_runs = find_runs_by_data_version(data_version)
    
    # Organize by stage
    lineage = {
        "data_version": data_version,
        "training_run_id": model_run_id,
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