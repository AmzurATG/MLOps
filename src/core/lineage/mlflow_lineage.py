"""
Unified MLflow Lineage Base Module
==================================

Shared lineage tracking patterns used by both MLOps and CVOps.
Extracts common functionality to avoid code duplication.

Usage:
    from src.core.lineage import (
        UnifiedLineageTracker,
        get_iceberg_snapshot,
        generate_data_version,
    )

    # Create domain-specific tracker
    tracker = UnifiedLineageTracker(
        experiment_name="fraud_detection",
        domain="mlops"
    )

    # Log lineage
    with tracker.pipeline_run("bronze", data_version="dv_abc123") as run:
        mlflow.log_param("records", 1000)
"""

import os
import json
import hashlib
from datetime import datetime
from typing import Dict, List, Optional, Any
from contextlib import contextmanager

# Try to import from settings, fallback to env vars
try:
    from src.core.config import settings
    MLFLOW_URI = settings.infra.mlflow_tracking_uri
except ImportError:
    MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://exp-mlflow:5000")


# =============================================================================
# PIPELINE STAGE CONSTANTS
# =============================================================================

STAGE_BRONZE = "bronze"
STAGE_SILVER = "silver"
STAGE_ANNOTATION = "annotation"
STAGE_GOLD = "gold"
STAGE_TRAINING = "training"
STAGE_INFERENCE = "inference"


# =============================================================================
# ICEBERG SNAPSHOT HELPERS
# =============================================================================

def get_iceberg_snapshot(trino, full_table_name: str) -> Optional[str]:
    """
    Get current Iceberg snapshot ID for a table.

    Args:
        trino: Trino resource with execute_query method
        full_table_name: Full table name like "iceberg_dev.gold.fraud_transactions"

    Returns:
        Snapshot ID as string, or None if not found
    """
    try:
        parts = full_table_name.split(".")
        if len(parts) != 3:
            return None

        catalog, schema, table = parts

        snapshot_query = f"""
            SELECT snapshot_id
            FROM {catalog}.{schema}."{table}$snapshots"
            ORDER BY committed_at DESC
            LIMIT 1
        """

        result = trino.execute_query(snapshot_query)
        if result and result[0][0]:
            return str(result[0][0])
    except Exception:
        pass

    return None


def get_table_snapshot_count(trino, full_table_name: str) -> int:
    """Get total number of snapshots for a table."""
    try:
        parts = full_table_name.split(".")
        if len(parts) != 3:
            return 0

        catalog, schema, table = parts

        count_query = f"""
            SELECT COUNT(*)
            FROM {catalog}.{schema}."{table}$snapshots"
        """

        result = trino.execute_query(count_query)
        return result[0][0] if result else 0
    except Exception:
        return 0


# =============================================================================
# DATA VERSION GENERATION
# =============================================================================

def generate_data_version(commits: List[str], timestamp: datetime = None) -> str:
    """
    Generate a unique data version from commit IDs.
    This links all pipeline stages that process the same data.

    Args:
        commits: List of LakeFS commit IDs
        timestamp: Optional timestamp (defaults to now)

    Returns:
        data_version string like "dv_abc123_20251206"
    """
    if not commits:
        commits = ["no_commits"]

    # Hash the commits
    commit_hash = hashlib.sha256("".join(sorted(commits)).encode()).hexdigest()[:8]

    # Add date for readability
    ts = timestamp or datetime.now()
    date_str = ts.strftime("%Y%m%d")

    return f"dv_{commit_hash}_{date_str}"


def get_data_version_from_table(trino, table: str) -> Optional[str]:
    """
    Get the latest data version from a table's commits.
    """
    try:
        result = trino.execute_query(f"""
            SELECT DISTINCT source_lakefs_commit
            FROM {table}
            WHERE source_lakefs_commit IS NOT NULL
            ORDER BY source_lakefs_commit
            LIMIT 100
        """)
        commits = [row[0] for row in result if row[0]]
        if commits:
            return generate_data_version(commits)
    except Exception:
        pass
    return None


# =============================================================================
# UNIFIED LINEAGE TRACKER
# =============================================================================

class UnifiedLineageTracker:
    """
    Unified lineage tracker for MLOps, CVOps, and LLMOps.

    Provides consistent MLflow logging patterns across all domains.

    Usage:
        tracker = UnifiedLineageTracker(
            experiment_name="fraud_detection",
            domain="mlops"
        )

        # Use context manager for runs
        with tracker.pipeline_run("bronze", data_version="dv_xxx") as run:
            mlflow.log_param("records", 1000)

        # Or use stage-specific methods
        tracker.log_stage_lineage(
            stage="bronze",
            context=dagster_context,
            trino=trino_resource,
            data_version="dv_xxx",
            params={"records": 1000},
            metrics={"duration": 30.5}
        )
    """

    def __init__(
        self,
        experiment_name: str,
        domain: str = "generic",
        mlflow_uri: str = None,
    ):
        """
        Initialize the lineage tracker.

        Args:
            experiment_name: MLflow experiment name
            domain: Domain identifier (mlops, cvops, llmops)
            mlflow_uri: MLflow tracking URI (defaults to MLFLOW_URI)
        """
        self.experiment_name = experiment_name
        self.domain = domain
        self.mlflow_uri = mlflow_uri or MLFLOW_URI

    @contextmanager
    def pipeline_run(
        self,
        stage: str,
        run_name: str = None,
        data_version: str = None,
        dagster_run_id: str = None,
        parent_run_id: str = None,
        tags: Dict[str, str] = None,
    ):
        """
        Context manager for unified MLflow logging across all pipeline stages.

        All runs go to the SAME experiment with tags to identify stage.

        Args:
            stage: Pipeline stage (bronze, silver, gold, annotation, training)
            run_name: Optional run name (auto-generated if not provided)
            data_version: Links related runs across stages
            dagster_run_id: Dagster run ID for cross-reference
            parent_run_id: Parent MLflow run ID for nested runs
            tags: Additional tags

        Usage:
            with tracker.pipeline_run("bronze", data_version="dv_abc123") as run:
                mlflow.log_param("records", 1000)
                mlflow.log_metric("duration", 30.5)
        """
        import mlflow

        mlflow.set_tracking_uri(self.mlflow_uri)
        mlflow.set_experiment(self.experiment_name)

        # Generate run name if not provided
        if not run_name:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            run_name = f"{stage}_{timestamp}"

        # Start run (with parent if specified)
        run_kwargs = {"run_name": run_name}
        if parent_run_id:
            run_kwargs["nested"] = True

        with mlflow.start_run(**run_kwargs) as run:
            # Set standard tags
            mlflow.set_tag("pipeline_stage", stage)
            mlflow.set_tag("experiment_type", "pipeline")
            mlflow.set_tag("domain", self.domain)

            if data_version:
                mlflow.set_tag("data_version", data_version)

            if dagster_run_id:
                mlflow.set_tag("dagster_run_id", dagster_run_id)

            # Set additional tags
            if tags:
                for key, value in tags.items():
                    mlflow.set_tag(key, str(value))

            yield run

    def log_lineage_artifact(
        self,
        lineage_data: Dict[str, Any],
        artifact_name: str = "lineage.json"
    ):
        """
        Log lineage data as a JSON artifact.
        """
        import mlflow
        import tempfile

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(lineage_data, f, indent=2, default=str)
            temp_path = f.name

        mlflow.log_artifact(temp_path, "lineage")

    def log_stage_lineage(
        self,
        stage: str,
        context,
        trino,
        data_version: str = None,
        params: Dict[str, Any] = None,
        metrics: Dict[str, float] = None,
        tables: Dict[str, str] = None,
        tags: Dict[str, str] = None,
        additional_data: Dict[str, Any] = None,
    ) -> str:
        """
        Generic stage lineage logging.

        Args:
            stage: Pipeline stage
            context: Dagster context
            trino: Trino resource
            data_version: Data version (auto-generated if None)
            params: Parameters to log
            metrics: Metrics to log
            tables: Table names for snapshot lookup
            tags: Additional tags
            additional_data: Extra data for lineage artifact

        Returns:
            data_version used
        """
        import mlflow

        # Get snapshot IDs for tables
        snapshot_ids = {}
        if tables and trino:
            for name, table in tables.items():
                snapshot_ids[name] = get_iceberg_snapshot(trino, table)

        with self.pipeline_run(
            stage=stage,
            data_version=data_version,
            dagster_run_id=getattr(context, 'run_id', 'unknown'),
            tags=tags or {}
        ):
            # Log params
            if params:
                for key, value in params.items():
                    mlflow.log_param(key, value)

            if data_version:
                mlflow.log_param("data_version", data_version)

            # Log snapshot IDs
            for name, snapshot_id in snapshot_ids.items():
                if snapshot_id:
                    mlflow.log_param(f"{name}_snapshot_id", snapshot_id)
                    mlflow.set_tag(f"{name}_snapshot", snapshot_id)

            # Log metrics
            if metrics:
                for key, value in metrics.items():
                    mlflow.log_metric(key, value)

            # Log lineage artifact
            lineage_data = {
                "stage": stage,
                "domain": self.domain,
                "data_version": data_version,
                "timestamp": datetime.now().isoformat(),
                "dagster_run_id": getattr(context, 'run_id', 'unknown'),
                "snapshot_ids": snapshot_ids,
                "params": params or {},
                "metrics": metrics or {},
            }
            if additional_data:
                lineage_data.update(additional_data)

            self.log_lineage_artifact(lineage_data)

            if hasattr(context, 'log'):
                context.log.info(f"Logged {stage} lineage (data_version: {data_version})")

        return data_version


# =============================================================================
# DOMAIN-SPECIFIC TRACKER FACTORIES
# =============================================================================

def create_mlops_tracker() -> UnifiedLineageTracker:
    """Create tracker for MLOps (fraud detection)."""
    experiment = os.getenv("MLFLOW_EXPERIMENT", "fraud_detection")
    return UnifiedLineageTracker(experiment_name=experiment, domain="mlops")


def create_cvops_tracker() -> UnifiedLineageTracker:
    """Create tracker for CVOps (computer vision)."""
    experiment = os.getenv("CVOPS_MLFLOW_EXPERIMENT", "cvops_object_detection")
    return UnifiedLineageTracker(experiment_name=experiment, domain="cvops")


def create_llmops_tracker() -> UnifiedLineageTracker:
    """Create tracker for LLMOps (language models)."""
    experiment = os.getenv("LLMOPS_MLFLOW_EXPERIMENT", "llmops_rag")
    return UnifiedLineageTracker(experiment_name=experiment, domain="llmops")
