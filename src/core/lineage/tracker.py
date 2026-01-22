"""
Core Lineage Module
===================

Shared lineage collection patterns for all domains (MLOps, CVOps, LLMOps).
Ensures consistent lineage tracking across:
- LakeFS commits (physical data versioning)
- Nessie commits (catalog versioning)
- Iceberg snapshots (table versioning)
- Dagster runs (pipeline execution)
- MLflow runs (model training)
"""
from datetime import datetime
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field, asdict

from src.core.config import TRINO_CATALOG


@dataclass
class LineageInfo:
    """
    Comprehensive lineage information for a pipeline run.
    
    This class captures the complete lineage chain:
    Source → Bronze → Silver → Gold → Model
    
    Each layer tracks:
    - Table name and row counts
    - LakeFS commit (data version)
    - Iceberg snapshot (table version)
    - Domain-specific metadata
    """
    
    # Pipeline metadata
    domain: str = "unknown"
    dagster_run_id: str = "unknown"
    pipeline_timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    
    # Version control
    lakefs_repo: str = "unknown"
    lakefs_branch: str = "unknown"
    lakefs_commit: str = "unknown"
    nessie_branch: str = "unknown"
    nessie_commit: str = "unknown"
    
    # Bronze layer
    bronze_table: str = "unknown"
    bronze_row_count: int = 0
    bronze_iceberg_snapshot: str = "unknown"
    
    # Silver layer
    silver_table: str = "unknown"
    silver_row_count: int = 0
    silver_iceberg_snapshot: str = "unknown"
    
    # Gold layer
    gold_table: str = "unknown"
    gold_row_count: int = 0
    gold_iceberg_snapshot: str = "unknown"
    
    # Domain-specific metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return asdict(self)
    
    def to_mlflow_params(self) -> Dict[str, str]:
        """Convert to MLflow-compatible params (strings only, key length <= 250)."""
        params = {}
        for key, value in self.to_dict().items():
            if key == "metadata":
                continue
            str_value = str(value)
            if len(str_value) <= 250:
                params[f"lineage_{key}"] = str_value
        return params


def collect_lineage(
    domain: str,
    dagster_run_id: str,
    lakefs_resource,
    nessie_resource,
    trino_resource,
    bronze_table: str,
    silver_table: str,
    gold_table: str,
    lakefs_repo: str,
    lakefs_branch: str,
    logger=None,
) -> LineageInfo:
    """
    Collect comprehensive lineage information.
    
    This is the main entry point for lineage collection.
    Works for any domain (MLOps, CVOps, LLMOps) by accepting
    domain-specific table names.
    
    Args:
        domain: Domain identifier (mlops, cvops, llmops)
        dagster_run_id: Current Dagster run ID
        lakefs_resource: LakeFS resource for commit info
        nessie_resource: Nessie resource for catalog versioning
        trino_resource: Trino resource for table queries
        bronze_table: Full bronze table name (catalog.schema.table)
        silver_table: Full silver table name
        gold_table: Full gold table name
        lakefs_repo: LakeFS repository name
        lakefs_branch: LakeFS branch name
        logger: Optional logger for debug output
        
    Returns:
        LineageInfo with all collected data
    """
    lineage = LineageInfo(
        domain=domain,
        dagster_run_id=dagster_run_id,
        lakefs_repo=lakefs_repo,
        lakefs_branch=lakefs_branch,
        nessie_branch=lakefs_branch,
        bronze_table=bronze_table,
        silver_table=silver_table,
        gold_table=gold_table,
    )
    
    # Get LakeFS commit
    try:
        commit_info = lakefs_resource.get_commit(lakefs_repo, lakefs_branch)
        lineage.lakefs_commit = commit_info.get("id", "unknown")[:12] if commit_info else "unknown"
    except Exception as e:
        if logger:
            logger.warning(f"Could not get LakeFS commit: {e}")
    
    # Get Nessie commit
    try:
        lineage.nessie_commit = nessie_resource.get_commit_hash(lakefs_branch)[:12]
    except Exception as e:
        if logger:
            logger.warning(f"Could not get Nessie commit: {e}")
    
    # Query Bronze stats
    lineage.bronze_row_count, lineage.bronze_iceberg_snapshot = _get_table_stats(
        trino_resource, bronze_table, logger
    )
    
    # Query Silver stats
    lineage.silver_row_count, lineage.silver_iceberg_snapshot = _get_table_stats(
        trino_resource, silver_table, logger
    )
    
    # Query Gold stats
    lineage.gold_row_count, lineage.gold_iceberg_snapshot = _get_table_stats(
        trino_resource, gold_table, logger
    )
    
    if logger:
        _log_lineage(lineage, logger)
    
    return lineage


def _get_table_stats(trino_resource, table_name: str, logger=None) -> tuple:
    """Get row count and Iceberg snapshot for a table."""
    row_count = 0
    snapshot_id = "unknown"
    
    try:
        result = trino_resource.execute_query(f"SELECT COUNT(*) FROM {table_name}")
        row_count = result[0][0] if result else 0
        
        # Get Iceberg snapshot
        parts = table_name.split(".")
        if len(parts) == 3:
            snapshot_query = f'SELECT snapshot_id FROM {parts[0]}.{parts[1]}."{parts[2]}$snapshots" ORDER BY committed_at DESC LIMIT 1'
            snapshot_result = trino_resource.execute_query(snapshot_query)
            snapshot_id = str(snapshot_result[0][0]) if snapshot_result else "unknown"
    except Exception as e:
        if logger:
            logger.warning(f"Could not get stats for {table_name}: {e}")
    
    return row_count, snapshot_id


def _log_lineage(lineage: LineageInfo, logger):
    """Pretty-print lineage information."""
    logger.info("=" * 60)
    logger.info(f"PIPELINE LINEAGE - {lineage.domain.upper()}")
    logger.info("=" * 60)
    logger.info(f"  Dagster Run: {lineage.dagster_run_id}")
    logger.info(f"  LakeFS: {lineage.lakefs_repo}/{lineage.lakefs_branch} @ {lineage.lakefs_commit}")
    logger.info(f"  Nessie: {lineage.nessie_branch} @ {lineage.nessie_commit}")
    logger.info(f"  Bronze: {lineage.bronze_row_count:,} rows (snapshot: {lineage.bronze_iceberg_snapshot})")
    logger.info(f"  Silver: {lineage.silver_row_count:,} rows (snapshot: {lineage.silver_iceberg_snapshot})")
    logger.info(f"  Gold: {lineage.gold_row_count:,} rows (snapshot: {lineage.gold_iceberg_snapshot})")
    logger.info("=" * 60)


# =============================================================================
# DOMAIN-SPECIFIC LINEAGE HELPERS
# =============================================================================

def collect_mlops_lineage(
    dagster_run_id: str,
    lakefs_resource,
    nessie_resource,
    trino_resource,
    logger=None,
) -> LineageInfo:
    """
    Collect lineage for MLOps (Fraud Detection) domain.
    Uses default table names from configuration.
    """
    from src.core.config import settings, TRINO_CATALOG

    # Build full table names from settings
    bronze_table = f"{TRINO_CATALOG}.{settings.mlops.bronze_schema}.{settings.mlops.bronze_table}"
    silver_table = f"{TRINO_CATALOG}.{settings.mlops.silver_schema}.{settings.mlops.silver_table}"
    gold_table = f"{TRINO_CATALOG}.{settings.mlops.gold_schema}.{settings.mlops.gold_table}"

    return collect_lineage(
        domain="mlops",
        dagster_run_id=dagster_run_id,
        lakefs_resource=lakefs_resource,
        nessie_resource=nessie_resource,
        trino_resource=trino_resource,
        bronze_table=bronze_table,
        silver_table=silver_table,
        gold_table=gold_table,
        lakefs_repo=settings.mlops.bronze_repo,
        lakefs_branch=settings.mlops.dev_branch,
        logger=logger,
    )


def collect_cvops_lineage(
    dagster_run_id: str,
    lakefs_resource,
    nessie_resource,
    trino_resource,
    logger=None,
) -> LineageInfo:
    """
    Collect lineage for CVOps (Object Detection) domain.
    """
    from src.core.config import settings, TRINO_CATALOG

    # CV tables use standardized cv schema
    bronze_table = f"{TRINO_CATALOG}.cv.image_metadata"
    silver_table = f"{TRINO_CATALOG}.cv.detection_results"
    gold_table = f"{TRINO_CATALOG}.cv.training_data"

    lineage = collect_lineage(
        domain="cvops",
        dagster_run_id=dagster_run_id,
        lakefs_resource=lakefs_resource,
        nessie_resource=nessie_resource,
        trino_resource=trino_resource,
        bronze_table=bronze_table,
        silver_table=silver_table,
        gold_table=gold_table,
        lakefs_repo=settings.cvops.repo,
        lakefs_branch=settings.cvops.branch,
        logger=logger,
    )

    # Add CV-specific metadata
    try:
        # Get image count
        img_result = trino_resource.execute_query(
            f"SELECT COUNT(DISTINCT image_id) FROM {bronze_table}"
        )
        lineage.metadata["image_count"] = img_result[0][0] if img_result else 0

        # Get annotation count
        ann_result = trino_resource.execute_query(
            f"SELECT COUNT(*) FROM {silver_table}"
        )
        lineage.metadata["detection_count"] = ann_result[0][0] if ann_result else 0
    except Exception:
        pass

    return lineage


# =============================================================================
# MLFLOW LINEAGE INTEGRATION
# =============================================================================

def log_lineage_to_mlflow(lineage: LineageInfo, run_id: str = None):
    """
    Log lineage information to MLflow.
    
    Args:
        lineage: LineageInfo object
        run_id: Optional MLflow run ID (uses active run if None)
    """
    import mlflow
    
    params = lineage.to_mlflow_params()
    
    if run_id:
        client = mlflow.tracking.MlflowClient()
        for key, value in params.items():
            client.log_param(run_id, key, value)
    else:
        mlflow.log_params(params)


def log_lineage_to_iceberg(
    lineage: LineageInfo,
    trino_resource,
    table_name: str = None,
):
    """
    Log lineage information to an Iceberg table for historical tracking.
    """
    if table_name is None:
        table_name = f"{TRINO_CATALOG}.gold.pipeline_lineage"
    # Ensure table exists
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        lineage_id VARCHAR,
        domain VARCHAR,
        dagster_run_id VARCHAR,
        pipeline_timestamp TIMESTAMP,
        lakefs_commit VARCHAR,
        nessie_commit VARCHAR,
        bronze_row_count BIGINT,
        silver_row_count BIGINT,
        gold_row_count BIGINT,
        metadata VARCHAR
    )
    """
    trino_resource.execute_ddl(create_sql)
    
    # Insert lineage record
    import json
    import uuid
    
    insert_sql = f"""
    INSERT INTO {table_name} VALUES (
        '{str(uuid.uuid4())}',
        '{lineage.domain}',
        '{lineage.dagster_run_id}',
        TIMESTAMP '{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}',
        '{lineage.lakefs_commit}',
        '{lineage.nessie_commit}',
        {lineage.bronze_row_count},
        {lineage.silver_row_count},
        {lineage.gold_row_count},
        '{json.dumps(lineage.metadata).replace("'", "''")}'
    )
    """
    trino_resource.execute_ddl(insert_sql)
