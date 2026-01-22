"""
Feature Pipeline with YAML-based Registry and Full Feast Integration

This pipeline combines:
- YAML as single source of truth for feature definitions
- Feast for point-in-time correct training data (get_historical_features)
- Feast for online serving (materialize to Redis)
- Full lineage tracking (PostgreSQL + MLflow)

Pipeline Flow:
1. generate_feature_code: YAML → SQL + Python + Feast definitions
2. apply_feast_definitions: Register feature views in Feast
3. create_training_data_feast: Use Feast get_historical_features() for training
4. train_with_lineage: Train model with full lineage tracking
5. materialize_online_features: Push features to Redis for serving
"""

import os
import subprocess
from datetime import datetime, timedelta
from typing import Optional, Any, Dict, List
import pandas as pd

from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    Config,
    define_asset_job,
    AssetSelection,
)

from src.core.resources import (
    TrinoResource,
    LakeFSResource,
    NessieResource,
    FeastResource,
)
from src.core.config import TRINO_CATALOG
from src.mlops.pipelines.feature_transformer import FeatureContract


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def collect_pipeline_lineage(
    context: AssetExecutionContext,
    trino: TrinoResource,
    lakefs: LakeFSResource,
    nessie: NessieResource,
) -> Dict[str, Any]:
    """
    Collect comprehensive lineage information from all pipeline stages.
    
    Returns dict with:
    - Bronze layer: table, row_count, lakefs_commit, iceberg_snapshot
    - Silver layer: table, row_count, iceberg_snapshot, annotation stats
    - Gold layer: table, row_count, iceberg_snapshot, date range, label distribution
    - Pipeline: dagster_run_id, timestamps
    - Version control: lakefs_commit, nessie_commit
    """
    lineage = {
        # Pipeline metadata
        "dagster_run_id": context.run_id,
        "pipeline_timestamp": datetime.now().isoformat(),
        
        # Version control (defaults)
        "lakefs_commit": "unknown",
        "nessie_commit": "unknown",
        
        # Bronze layer
        "bronze_table": os.getenv("BRONZE_TABLE", f"{TRINO_CATALOG}.bronze.fraud_transactions"),
        "bronze_row_count": 0,
        "bronze_iceberg_snapshot": "unknown",

        # Silver layer
        "silver_table": os.getenv("SILVER_TABLE", f"{TRINO_CATALOG}.silver.fraud_transactions"),
        "silver_row_count": 0,
        "silver_iceberg_snapshot": "unknown",
        "silver_pending_count": 0,
        "silver_reviewed_count": 0,
        "silver_in_review_count": 0,
        
        # Gold layer
        "gold_table": os.getenv("GOLD_TABLE", f"{TRINO_CATALOG}.gold.fraud_transactions"),
        "gold_row_count": 0,
        "gold_iceberg_snapshot": "unknown",
        "gold_fraud_count": 0,
        "gold_legit_count": 0,
        "gold_date_min": "unknown",
        "gold_date_max": "unknown",
        
        # Label Studio
        "labelstudio_project_id": os.getenv("LABELSTUDIO_PROJECT_ID", "1"),
    }
    
    bronze_repo = os.getenv("LAKEHOUSE_BRONZE_REPO", "bronze")
    dev_branch = os.getenv("LAKEHOUSE_DEV_BRANCH", "dev")
    
    # Get LakeFS commit
    try:
        commit_info = lakefs.get_commit(bronze_repo, dev_branch)
        lineage["lakefs_commit"] = commit_info.get("id", "unknown") if commit_info else "unknown"
    except Exception as e:
        context.log.warning(f"Could not get LakeFS commit: {e}")
    
    # Get Nessie commit
    try:
        lineage["nessie_commit"] = nessie.get_commit_hash(dev_branch)
    except Exception as e:
        context.log.warning(f"Could not get Nessie commit: {e}")
    
    # Query Bronze stats (optional skip for performance at 1M+ scale)
    SKIP_LINEAGE_COUNTS = os.getenv("SKIP_LINEAGE_COUNTS", "false").lower() == "true"
    try:
        if SKIP_LINEAGE_COUNTS:
            lineage["bronze_row_count"] = -1  # Skipped for performance
        else:
            result = trino.execute_query(f"SELECT COUNT(*) FROM {lineage['bronze_table']}")
            lineage["bronze_row_count"] = result[0][0] if result else 0
        
        # Get Iceberg snapshot - parse table name for correct syntax
        # Format: catalog.schema."table$snapshots"
        parts = lineage['bronze_table'].split(".")
        if len(parts) == 3:
            snapshot_query = f'SELECT snapshot_id FROM {parts[0]}.{parts[1]}."{parts[2]}$snapshots" ORDER BY committed_at DESC LIMIT 1'
            snapshot_result = trino.execute_query(snapshot_query)
            lineage["bronze_iceberg_snapshot"] = str(snapshot_result[0][0]) if snapshot_result else "unknown"
    except Exception as e:
        context.log.warning(f"Could not get Bronze stats: {e}")
    
    # Query Silver stats
    try:
        if SKIP_LINEAGE_COUNTS:
            lineage["silver_row_count"] = -1  # Skipped for performance
        else:
            result = trino.execute_query(f"SELECT COUNT(*) FROM {lineage['silver_table']}")
            lineage["silver_row_count"] = result[0][0] if result else 0
        
        # Get Iceberg snapshot - parse table name for correct syntax
        parts = lineage['silver_table'].split(".")
        if len(parts) == 3:
            snapshot_query = f'SELECT snapshot_id FROM {parts[0]}.{parts[1]}."{parts[2]}$snapshots" ORDER BY committed_at DESC LIMIT 1'
            snapshot_result = trino.execute_query(snapshot_query)
            lineage["silver_iceberg_snapshot"] = str(snapshot_result[0][0]) if snapshot_result else "unknown"
        
        # Get annotation status counts
        status_result = trino.execute_query(f"""
            SELECT 
                review_status,
                COUNT(*) as cnt
            FROM {lineage['silver_table']}
            GROUP BY review_status
        """)
        for row in status_result or []:
            status, count = row[0], row[1]
            if status == 'pending':
                lineage["silver_pending_count"] = count
            elif status == 'reviewed':
                lineage["silver_reviewed_count"] = count
            elif status == 'in_review':
                lineage["silver_in_review_count"] = count
    except Exception as e:
        context.log.warning(f"Could not get Silver stats: {e}")
    
    # Query Gold stats
    try:
        if SKIP_LINEAGE_COUNTS:
            lineage["gold_row_count"] = -1  # Skipped for performance
        else:
            result = trino.execute_query(f"SELECT COUNT(*) FROM {lineage['gold_table']}")
            lineage["gold_row_count"] = result[0][0] if result else 0
        
        # Get Iceberg snapshot - parse table name for correct syntax
        parts = lineage['gold_table'].split(".")
        if len(parts) == 3:
            snapshot_query = f'SELECT snapshot_id FROM {parts[0]}.{parts[1]}."{parts[2]}$snapshots" ORDER BY committed_at DESC LIMIT 1'
            snapshot_result = trino.execute_query(snapshot_query)
            lineage["gold_iceberg_snapshot"] = str(snapshot_result[0][0]) if snapshot_result else "unknown"
        
        # Get label distribution and date range
        stats_result = trino.execute_query(f"""
            SELECT 
                SUM(CASE WHEN COALESCE(final_label, is_fraudulent) = 1 THEN 1 ELSE 0 END) as fraud_count,
                SUM(CASE WHEN COALESCE(final_label, is_fraudulent) = 0 THEN 1 ELSE 0 END) as legit_count,
                MIN(transaction_date) as date_min,
                MAX(transaction_date) as date_max
            FROM {lineage['gold_table']}
        """)
        if stats_result and stats_result[0]:
            lineage["gold_fraud_count"] = stats_result[0][0] or 0
            lineage["gold_legit_count"] = stats_result[0][1] or 0
            lineage["gold_date_min"] = str(stats_result[0][2]) if stats_result[0][2] else "unknown"
            lineage["gold_date_max"] = str(stats_result[0][3]) if stats_result[0][3] else "unknown"
    except Exception as e:
        context.log.warning(f"Could not get Gold stats: {e}")
    
    context.log.info("=" * 60)
    context.log.info("PIPELINE LINEAGE COLLECTED")
    context.log.info("=" * 60)
    context.log.info(f"  Dagster Run: {lineage['dagster_run_id']}")
    context.log.info(f"  LakeFS: {lineage['lakefs_commit'][:12]}...")
    context.log.info(f"  Nessie: {lineage['nessie_commit'][:12]}...")
    context.log.info(f"  Bronze: {lineage['bronze_row_count']:,} rows (snapshot: {lineage['bronze_iceberg_snapshot']})")
    context.log.info(f"  Silver: {lineage['silver_row_count']:,} rows (reviewed: {lineage['silver_reviewed_count']:,})")
    context.log.info(f"  Gold: {lineage['gold_row_count']:,} rows (fraud: {lineage['gold_fraud_count']:,}, legit: {lineage['gold_legit_count']:,})")
    context.log.info(f"  Date Range: {lineage['gold_date_min']} to {lineage['gold_date_max']}")
    context.log.info("=" * 60)
    
    return lineage


def log_lineage_to_mlflow(mlflow, lineage: Dict[str, Any], context: AssetExecutionContext):
    """Log all lineage information to MLflow."""
    
    # ═══════════════════════════════════════════════════════════════════
    # TAGS (for filtering/searching runs)
    # ═══════════════════════════════════════════════════════════════════
    mlflow.set_tag("dagster_run_id", lineage["dagster_run_id"])
    mlflow.set_tag("lakefs_commit", lineage["lakefs_commit"])
    mlflow.set_tag("nessie_commit", lineage["nessie_commit"])
    
    # ═══════════════════════════════════════════════════════════════════
    # PARAMS - Source Data Lineage
    # ═══════════════════════════════════════════════════════════════════
    
    # Bronze layer
    mlflow.log_param("bronze_table", lineage["bronze_table"])
    mlflow.log_param("bronze_row_count", lineage["bronze_row_count"])
    mlflow.log_param("bronze_iceberg_snapshot", lineage["bronze_iceberg_snapshot"])
    mlflow.log_param("bronze_lakefs_commit", lineage["lakefs_commit"])
    
    # Silver layer
    mlflow.log_param("silver_table", lineage["silver_table"])
    mlflow.log_param("silver_row_count", lineage["silver_row_count"])
    mlflow.log_param("silver_iceberg_snapshot", lineage["silver_iceberg_snapshot"])
    mlflow.log_param("silver_pending_count", lineage["silver_pending_count"])
    mlflow.log_param("silver_reviewed_count", lineage["silver_reviewed_count"])
    mlflow.log_param("silver_in_review_count", lineage["silver_in_review_count"])
    
    # Gold layer (training source)
    mlflow.log_param("gold_table", lineage["gold_table"])
    mlflow.log_param("gold_row_count", lineage["gold_row_count"])
    mlflow.log_param("gold_iceberg_snapshot", lineage["gold_iceberg_snapshot"])
    mlflow.log_param("gold_fraud_count", lineage["gold_fraud_count"])
    mlflow.log_param("gold_legit_count", lineage["gold_legit_count"])
    mlflow.log_param("training_data_start", lineage["gold_date_min"])
    mlflow.log_param("training_data_end", lineage["gold_date_max"])
    
    # Label Studio
    mlflow.log_param("labelstudio_project_id", lineage["labelstudio_project_id"])
    
    # ═══════════════════════════════════════════════════════════════════
    # ARTIFACT - Full lineage JSON for reproducibility
    # ═══════════════════════════════════════════════════════════════════
    import json
    lineage_path = "/tmp/pipeline_lineage.json"
    with open(lineage_path, 'w') as f:
        json.dump(lineage, f, indent=2, default=str)
    mlflow.log_artifact(lineage_path, "lineage")
    
    context.log.info("✓ Logged complete lineage to MLflow")


def to_serializable(obj: Any) -> Any:
    """Convert numpy/pandas types to JSON-serializable Python types."""
    import numpy as np
    
    if isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {str(k): to_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [to_serializable(v) for v in obj]
    elif pd.isna(obj):
        return None
    return obj


def safe_get_tracker(project_name: str):
    """Safely create lineage tracker, returns None if PostgreSQL unavailable."""
    try:
        from feature_registry.lineage import FeatureLineageTracker
        return FeatureLineageTracker(project_name=project_name)
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

class FeaturePipelineConfig(Config):
    """Configuration for the feature pipeline."""
    yaml_path: str = "/app/feature_registry/fraud_detection.yaml"
    

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1: GENERATE FEATURE CODE FROM YAML
# ═══════════════════════════════════════════════════════════════════════════════

@asset(
    group_name="feature_pipeline",
    deps=["mlops_gold_table"],
    description="Generate SQL, Python, and Feast code from YAML feature registry",
)
def generate_feature_code(
    context: AssetExecutionContext,
    config: FeaturePipelineConfig,
) -> MaterializeResult:
    """
    Step 1: Parse YAML and generate all code artifacts.
    
    Generates:
    - SQL for training table creation (backup/debugging)
    - Python transformer for inference
    - Feast feature view definitions
    """
    from feature_registry.generator import FeatureGenerator
    
    # Load and parse YAML
    generator = FeatureGenerator.from_yaml(config.yaml_path)
    project_name = generator.registry.project_name
    version = generator.registry.version
    
    context.log.info(f"Loaded feature registry: {project_name} v{version}")
    context.log.info(f"Features: {len(generator.registry.feature_order)}")
    
    # Generate all code
    sql_code = generator.generate_sql()
    python_code = generator.generate_python()
    feast_code = generator.generate_feast()
    
    # Save to temp directory
    output_dir = "/tmp/generated_features"
    os.makedirs(output_dir, exist_ok=True)
    
    sql_path = f"{output_dir}/{project_name}_features.sql"
    python_path = f"{output_dir}/{project_name}_transformer.py"
    feast_path = f"{output_dir}/{project_name}_feast.py"
    
    with open(sql_path, 'w') as f:
        f.write(sql_code)
    with open(python_path, 'w') as f:
        f.write(python_code)
    with open(feast_path, 'w') as f:
        f.write(feast_code)
    
    context.log.info(f"✓ Generated SQL: {sql_path} ({len(sql_code)} chars)")
    context.log.info(f"✓ Generated Python: {python_path} ({len(python_code)} chars)")
    context.log.info(f"✓ Generated Feast: {feast_path} ({len(feast_code)} chars)")
    
    # Register version in lineage tracker (optional - continues if DB unavailable)
    feature_version_id = "unknown"
    tracker = safe_get_tracker(project_name)
    if tracker:
        try:
            feature_version = tracker.register_feature_version(
                yaml_path=config.yaml_path,
                generated_sql=sql_code,
                generated_python=python_code,
            )
            feature_version_id = feature_version.version_id
            context.log.info(f"✓ Registered version: {feature_version_id}")
        except Exception as e:
            context.log.warning(f"Could not register lineage (continuing): {e}")
    else:
        context.log.warning("Lineage tracker unavailable (PostgreSQL not configured)")
    
    return MaterializeResult(
        metadata={
            "project_name": project_name,
            "version": version,
            "feature_count": len(generator.registry.feature_order),
            "feature_version_id": feature_version_id,
            "sql_path": sql_path,
            "python_path": python_path,
            "feast_path": feast_path,
        }
    )


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3: APPLY FEAST DEFINITIONS (after table exists)
# ═══════════════════════════════════════════════════════════════════════════════

@asset(
    group_name="feature_pipeline",
    deps=["create_training_data_feast"],  # Table must exist first for Feast validation
    description="Apply Feast feature view definitions",
)
def apply_feast_definitions(
    context: AssetExecutionContext,
    config: FeaturePipelineConfig,
    feast: FeastResource,
) -> MaterializeResult:
    """
    Step 3: Register feature views in Feast.
    
    IMPORTANT: This runs AFTER create_training_data_feast because
    Feast validates the source table exists during 'feast apply'.
    """
    from feature_registry.generator import FeatureGenerator
    
    generator = FeatureGenerator.from_yaml(config.yaml_path)
    project_name = generator.registry.project_name
    
    # Copy generated Feast definitions to feast_repo
    generated_feast_path = f"/tmp/generated_features/{project_name}_feast.py"
    feast_repo_path = "/app/feast_repo"
    target_path = f"{feast_repo_path}/generated_features.py"
    
    if not os.path.exists(generated_feast_path):
        raise FileNotFoundError(f"Generated Feast file not found: {generated_feast_path}")
    
    with open(generated_feast_path, 'r') as f:
        feast_code = f.read()
    
    with open(target_path, 'w') as f:
        f.write(feast_code)
    
    context.log.info(f"✓ Copied Feast definitions to: {target_path}")
    
    # Apply Trino patches FIRST
    feast._apply_trino_patches()
    context.log.info("✓ Trino compatibility patches applied")
    
    # Run feast apply via CLI (more reliable across Feast versions)
    context.log.info("Running feast apply...")
    try:
        result = subprocess.run(
            ["feast", "apply"],
            cwd=feast_repo_path,
            capture_output=True,
            text=True,
            timeout=120,
            env={**os.environ}  # Inherit environment
        )
        
        if result.stdout:
            context.log.info(f"Feast apply output: {result.stdout}")
        if result.stderr:
            context.log.warning(f"Feast apply stderr: {result.stderr}")
        
        if result.returncode != 0:
            raise RuntimeError(f"Feast apply failed with code {result.returncode}: {result.stderr}")
        
        context.log.info("✓ Feast apply completed")
        
    except subprocess.TimeoutExpired:
        raise RuntimeError("Feast apply timed out after 120 seconds")
    except FileNotFoundError:
        raise RuntimeError("feast CLI not found - ensure Feast is installed")
    
    # Force reload store to pick up new definitions
    feast._store = None
    store = feast.get_store()
    
    # Verify feature views are registered
    feature_views = store.list_feature_views()
    fv_names = [fv.name for fv in feature_views]
    
    if not fv_names:
        context.log.warning("No feature views found after apply - check generated_features.py")
    else:
        context.log.info(f"✓ Registered {len(fv_names)} feature views: {fv_names}")
    
    return MaterializeResult(
        metadata={
            "feast_definitions_path": target_path,
            "feature_views": fv_names,
            "feature_views_count": len(fv_names),
        }
    )


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3: CREATE TRAINING DATA VIA SQL (Computed Features)
# ═══════════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2: CREATE TRAINING DATA TABLE
# ═══════════════════════════════════════════════════════════════════════════════

@asset(
    group_name="feature_pipeline",
    deps=["generate_feature_code"],  # Depends on SQL generation, runs BEFORE Feast apply
    description="Create training data table using generated SQL with computed features",
)
def create_training_data_feast(
    context: AssetExecutionContext,
    config: FeaturePipelineConfig,
    trino: TrinoResource,
    feast: FeastResource,
    lakefs: LakeFSResource,
    nessie: NessieResource,
) -> MaterializeResult:
    """
    Step 3: Execute generated SQL to create training data with all computed features.
    
    This creates the fraud_training_data table with:
    - All primitive features
    - All window-based features (point-in-time correct via SQL)
    - All derived features
    
    Feast will then read from this table for online serving.
    """
    from feature_registry.generator import FeatureGenerator
    
    generator = FeatureGenerator.from_yaml(config.yaml_path)
    project_name = generator.registry.project_name
    output_table = generator.registry.output_table
    
    # Get source data lineage (non-critical - continue if fails)
    bronze_repo = os.getenv("LAKEHOUSE_BRONZE_REPO", "bronze")
    dev_branch = os.getenv("LAKEHOUSE_DEV_BRANCH", "dev")
    
    lakefs_commit = "unknown"
    nessie_commit = "unknown"
    
    try:
        commit_info = lakefs.get_commit(bronze_repo, dev_branch)
        lakefs_commit = commit_info.get("id", "unknown") if commit_info else "unknown"
    except Exception as e:
        context.log.warning(f"Could not get LakeFS commit: {e}")
    
    try:
        nessie_commit = nessie.get_commit_hash(dev_branch)
    except Exception as e:
        context.log.warning(f"Could not get Nessie commit: {e}")
    
    context.log.info(f"Source lineage: LakeFS={lakefs_commit[:8]}..., Nessie={nessie_commit[:8]}...")
    
    # Load generated SQL
    sql_path = f"/tmp/generated_features/{project_name}_features.sql"
    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"Generated SQL not found: {sql_path}")
    
    with open(sql_path, 'r') as f:
        generated_sql = f.read()
    
    # Drop existing table and recreate
    context.log.info(f"DROP TABLE IF EXISTS {output_table}")
    trino.execute_ddl(f"DROP TABLE IF EXISTS {output_table}")
    
    # Execute generated SQL to create training data
    context.log.info(f"Creating training table: {output_table}")
    start_time = datetime.now()
    
    try:
        trino.execute_ddl(generated_sql)
    except Exception as e:
        context.log.error(f"SQL execution failed: {e}")
        context.log.error(f"SQL (first 500 chars): {generated_sql[:500]}")
        raise
    
    duration = (datetime.now() - start_time).total_seconds()
    
    # Get row count
    row_count = trino.get_count(output_table)
    context.log.info(f"✓ Created {row_count:,} rows in {duration:.1f}s")
    
    # Now read the created table for training
    training_query = f"""
    SELECT *
    FROM {output_table}
    WHERE event_timestamp IS NOT NULL
    """
    
    context.log.info("Reading training data from created table (streaming for 1M+ scalability)...")

    # Use streaming method for large datasets to avoid OOM
    chunks = []
    for chunk_df in trino.execute_query_as_dataframe_streaming(training_query, chunk_size=100000):
        chunks.append(chunk_df)
        context.log.info(f"  Loaded chunk: {len(chunk_df)} rows")
    training_df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()

    context.log.info(f"✓ Training dataset: {len(training_df)} rows, {len(training_df.columns)} columns")
    
    # Save to parquet for training step
    output_path = "/tmp/feast_training_data.parquet"
    training_df.to_parquet(output_path)
    context.log.info(f"✓ Saved training dataset to {output_path}")
    
    # Update lineage (non-critical)
    feature_version_id = "unknown"
    tracker = safe_get_tracker(project_name)
    if tracker:
        try:
            python_path = f"/tmp/generated_features/{project_name}_transformer.py"
            generated_python = ""
            if os.path.exists(python_path):
                with open(python_path) as f:
                    generated_python = f.read()
            
            version = tracker.register_feature_version(
                yaml_path=config.yaml_path,
                generated_sql=generated_sql,
                generated_python=generated_python,
                source_lakefs_commit=lakefs_commit,
                source_nessie_commit=nessie_commit,
            )
            
            tracker.update_output_lineage(
                version_id=version.version_id,
                output_nessie_commit=nessie_commit,
                output_row_count=row_count,
            )
            
            feature_version_id = version.version_id
            context.log.info(f"✓ Updated lineage: {feature_version_id}")
        except Exception as e:
            context.log.warning(f"Could not update lineage (continuing): {e}")
    
    # Feature summary
    exclude_cols = ['transaction_id', 'event_timestamp', 'Label', 'customer_id']
    feature_cols = [c for c in training_df.columns if c not in exclude_cols]
    null_counts = training_df[feature_cols].isnull().sum()
    
    # Get label distribution
    label_col = 'Label' if 'Label' in training_df.columns else 'label' if 'label' in training_df.columns else None
    label_dist: Dict[str, int] = {}
    if label_col:
        label_dist = {str(k): int(v) for k, v in training_df[label_col].value_counts().to_dict().items()}
    
    # Get missing values
    missing: Dict[str, int] = {str(k): int(v) for k, v in null_counts[null_counts > 0].to_dict().items()}
    
    return MaterializeResult(
        metadata=to_serializable({
            "feature_version_id": feature_version_id,
            "output_table": output_table,
            "row_count": row_count,
            "feature_count": len(feature_cols),
            "label_distribution": label_dist,
            "missing_values": missing,
            "output_path": output_path,
            "source_lakefs_commit": lakefs_commit,
            "source_nessie_commit": nessie_commit,
            "duration_seconds": duration,
        })
    )


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 4: TRAIN MODEL WITH LINEAGE
# ═══════════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 4: TRAIN MODEL WITH LINEAGE
# ═══════════════════════════════════════════════════════════════════════════════

@asset(
    group_name="feature_pipeline",
    deps=["create_training_data_feast", "apply_feast_definitions"],  # Need both table and Feast
    description="Train fraud detection model with full lineage tracking",
)
def train_with_lineage(
    context: AssetExecutionContext,
    config: FeaturePipelineConfig,
    trino: TrinoResource,
    lakefs: LakeFSResource,
    nessie: NessieResource,
) -> MaterializeResult:
    """
    Step 4: Train model and log everything to MLflow.
    """
    import mlflow
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import classification_report, roc_auc_score, confusion_matrix
    from feature_registry.generator import FeatureGenerator
    
    generator = FeatureGenerator.from_yaml(config.yaml_path)
    project_name = generator.registry.project_name
    
    # Get feature version (non-critical)
    feature_version_id = "unknown"
    tracker = safe_get_tracker(project_name)
    if tracker:
        try:
            version = tracker.get_latest_version()
            if version:
                feature_version_id = version.version_id
        except Exception as e:
            context.log.warning(f"Could not get feature version: {e}")
    
    # Load training data
    training_data_path = "/tmp/feast_training_data.parquet"
    if not os.path.exists(training_data_path):
        raise FileNotFoundError(f"Training data not found: {training_data_path}")
    
    training_df = pd.read_parquet(training_data_path)
    context.log.info(f"Loaded {len(training_df)} training samples")
    context.log.info(f"Columns: {list(training_df.columns)}")
    
    # Find label column (could be 'label', 'Label', 'is_fraudulent', 'final_label')
    label_col = None
    for possible_label in ['label', 'Label', 'is_fraudulent', 'final_label']:
        if possible_label in training_df.columns:
            label_col = possible_label
            break
    
    if label_col is None:
        raise ValueError(f"No label column found. Available columns: {list(training_df.columns)}")
    
    context.log.info(f"Using label column: {label_col}")
    
    # Prepare features - exclude IDs, timestamps, and label
    exclude_cols = ['transaction_id', 'event_timestamp', 'customer_id', label_col]
    # Also exclude other potential label variants
    exclude_cols.extend(['label', 'Label', 'is_fraudulent', 'final_label'])
    exclude_cols = list(set(exclude_cols))  # Remove duplicates
    
    feature_cols = [c for c in training_df.columns if c not in exclude_cols]
    
    if not feature_cols:
        raise ValueError(f"No feature columns found. Available columns: {list(training_df.columns)}")
    
    # Use YAML-defined categorical columns (more reliable than dtype detection)
    yaml_categorical = generator.registry.categorical_columns
    categorical_cols = [c for c in yaml_categorical if c in feature_cols]
    
    if categorical_cols:
        context.log.info(f"Encoding {len(categorical_cols)} categorical features (from YAML): {categorical_cols}")
        for col in categorical_cols:
            # Convert to string first to handle mixed types
            training_df[col] = training_df[col].astype(str)
            training_df[col] = pd.Categorical(training_df[col]).codes
    
    X = training_df[feature_cols].fillna(0)
    
    # Convert Decimal types to float (Trino returns Decimal for numeric columns)
    for col in X.columns:
        if col not in categorical_cols:
            X[col] = X[col].astype(float)
    
    y = training_df[label_col].astype(int)  # Ensure int for classification
    
    context.log.info(f"Features: {len(feature_cols)}")
    context.log.info(f"Label distribution: {y.value_counts().to_dict()}")
    
    # Split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    context.log.info(f"Train: {len(X_train)}, Test: {len(X_test)}")
    
    # Setup MLflow
    mlflow_tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://exp-mlflow:5000")
    mlflow.set_tracking_uri(mlflow_tracking_uri)
    mlflow.set_experiment(project_name)
    
    # ═══════════════════════════════════════════════════════════════════
    # COLLECT COMPLETE PIPELINE LINEAGE
    # ═══════════════════════════════════════════════════════════════════
    lineage = collect_pipeline_lineage(context, trino, lakefs, nessie)
    
    with mlflow.start_run(run_name=f"{project_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
        # ═══════════════════════════════════════════════════════════════════
        # LOG COMPLETE LINEAGE (Source → Bronze → Silver → Gold → Model)
        # ═══════════════════════════════════════════════════════════════════
        log_lineage_to_mlflow(mlflow, lineage, context)
        
        # Feature registry lineage
        mlflow.log_param("feature_version_id", feature_version_id)
        mlflow.log_param("yaml_version", generator.registry.version)
        mlflow.log_param("feature_source", "feast_historical")
        mlflow.log_param("model_type", "RandomForest")
        mlflow.log_param("n_features", len(feature_cols))
        mlflow.log_param("n_train", len(X_train))
        mlflow.log_param("n_test", len(X_test))
        mlflow.log_param("categorical_features", ", ".join(categorical_cols) if categorical_cols else "none")
        
        # Log YAML as artifact
        if os.path.exists(config.yaml_path):
            mlflow.log_artifact(config.yaml_path)
        
        # Log generated code
        for suffix in ['_features.sql', '_transformer.py', '_feast.py']:
            path = f"/tmp/generated_features/{project_name}{suffix}"
            if os.path.exists(path):
                mlflow.log_artifact(path)
        
        # ═══════════════════════════════════════════════════════════════════
        # TRAIN MODEL
        # ═══════════════════════════════════════════════════════════════════
        context.log.info("Training RandomForest...")
        model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            min_samples_split=5,
            min_samples_leaf=2,
            random_state=42,
            n_jobs=-1,
            class_weight='balanced'
        )
        model.fit(X_train, y_train)
        context.log.info("✓ Model trained")
        
        # ═══════════════════════════════════════════════════════════════════
        # EVALUATE
        # ═══════════════════════════════════════════════════════════════════
        y_pred = model.predict(X_test)
        y_proba = model.predict_proba(X_test)[:, 1]
        
        auc = float(roc_auc_score(y_test, y_proba))
        report = classification_report(y_test, y_pred, output_dict=True)
        cm = confusion_matrix(y_test, y_pred)
        
        # Handle different key types in classification report (int vs str)
        fraud_key = 1 if 1 in report else '1'
        fraud_metrics = report.get(fraud_key, {})
        
        precision_fraud = float(fraud_metrics.get('precision', 0.0))
        recall_fraud = float(fraud_metrics.get('recall', 0.0))
        f1_fraud = float(fraud_metrics.get('f1-score', 0.0))
        accuracy = float(report.get('accuracy', 0.0))
        
        # Log metrics
        mlflow.log_metric("auc", auc)
        mlflow.log_metric("accuracy", accuracy)
        mlflow.log_metric("precision_fraud", precision_fraud)
        mlflow.log_metric("recall_fraud", recall_fraud)
        mlflow.log_metric("f1_fraud", f1_fraud)
        
        tn, fp, fn, tp = [int(x) for x in cm.ravel()]
        mlflow.log_metric("true_positives", tp)
        mlflow.log_metric("false_positives", fp)
        mlflow.log_metric("true_negatives", tn)
        mlflow.log_metric("false_negatives", fn)
        
        # Feature importance
        feature_importance = pd.DataFrame({
            'feature': feature_cols,
            'importance': model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        top_10 = feature_importance.head(10).to_dict('records')
        context.log.info("Top 10 features:")
        for feat in top_10:
            context.log.info(f"  {feat['feature']}: {feat['importance']:.4f}")
        
        feature_importance.to_csv("/tmp/feature_importance.csv", index=False)
        mlflow.log_artifact("/tmp/feature_importance.csv")
        
        # ═══════════════════════════════════════════════════════════════════
        # GENERATE FEATURE CONTRACT
        # ═══════════════════════════════════════════════════════════════════
        context.log.info("Generating feature contract...")
        
        # Use YAML-defined categorical columns (not auto-detected from data)
        yaml_categorical_cols = generator.registry.categorical_columns
        context.log.info(f"Categorical columns from YAML: {yaml_categorical_cols}")
        
        # Build category mappings with ALL possible values (not just training data)
        # Include common values to handle inference-time variations
        default_category_values = {
            "country": ["US", "UK", "DE", "FR", "CA", "AU", "IN", "BR", "NG", "PK", "RU", "CN", "JP", "MX", "XX"],
            "device_type": ["desktop", "mobile", "tablet", "unknown"],
            "payment_method": ["credit_card", "debit_card", "paypal", "crypto", "wallet", "bank_transfer", "unknown"],
            "category": ["electronics", "clothing", "food", "gift_cards", "digital", "luxury", "home", "sports", "unknown"],
            "ip_prefix": [],  # Will be built from data
            "shipping_country": ["US", "UK", "DE", "FR", "CA", "AU", "IN", "BR", "NG", "PK", "RU", "CN", "JP", "MX", "XX"],
            "billing_country": ["US", "UK", "DE", "FR", "CA", "AU", "IN", "BR", "NG", "PK", "RU", "CN", "JP", "MX", "XX"],
        }
        
        category_mappings = {}
        for col in yaml_categorical_cols:
            if col in training_df.columns:
                # Get unique values from training data
                training_values = set(str(v) for v in training_df[col].dropna().unique())
                # Combine with default values
                default_values = set(default_category_values.get(col, []))
                all_values = sorted(training_values | default_values)
                # Create mapping
                mapping = {str(val): idx for idx, val in enumerate(all_values)}
                category_mappings[col] = mapping
                context.log.info(f"  {col}: {len(mapping)} values")
        
        # Compute feature statistics (only for numeric columns)
        # Convert Decimal types to float (Trino returns Decimal)
        feature_stats = {}
        numeric_cols = [c for c in feature_cols if c not in yaml_categorical_cols]
        for col in numeric_cols:
            col_data = X[col].astype(float)  # Convert Decimal to float
            feature_stats[col] = {
                "mean": float(col_data.mean()) if not col_data.isna().all() else 0.0,
                "std": float(col_data.std()) if not col_data.isna().all() else 0.0,
                "min": float(col_data.min()) if not col_data.isna().all() else 0.0,
                "max": float(col_data.max()) if not col_data.isna().all() else 0.0,
            }
        
        # Classify features by type (based on YAML groups)
        primitive_features = []
        aggregate_features = []
        derived_features = []
        
        for group_name, group in generator.registry.feature_groups.items():
            for col in group.columns:
                if col.name in feature_cols:
                    if group_name == 'primitives':
                        # Only actual primitives from request
                        primitive_features.append(col.name)
                    elif group_name in ['customer_behavioral', 'customer_spending']:
                        # Aggregates from Feast
                        aggregate_features.append(col.name)
                    elif group_name in ['calendar', 'address', 'risk_indicators']:
                        # Computed at inference time
                        derived_features.append(col.name)
        
        # Get risk thresholds from YAML feature groups
        risk_thresholds = {}
        for group_name, group in generator.registry.feature_groups.items():
            for col in group.columns:
                if col.params:
                    for param_name, param_value in col.params.items():
                        if 'threshold' in param_name.lower() or param_name in ['multiplier', 'days', 'methods', 'categories', 'countries']:
                            risk_thresholds[f"{col.name}_{param_name}"] = param_value
        
        # Add default thresholds if not found
        if not risk_thresholds:
            risk_thresholds = {
                "high_amount_multiplier": 3.0,
                "high_velocity_days": 0.25,
                "risky_payment_methods": ["credit_card", "wallet"],
                "risky_categories": ["Electronics", "Luxury", "Digital"],
                "high_risk_countries": ["NG", "PK", "RU", "BR"],
            }
        
        # =====================================================================
        # SCHEMA EVOLUTION: Build contract with version tracking
        # =====================================================================

        # Try to load previous contract to detect schema changes
        previous_contract = None
        previous_schema_version = "1.0.0"
        try:
            # Try to load from latest production model
            import mlflow
            client = mlflow.tracking.MlflowClient()
            model_name = f"{project_name}_model"
            try:
                latest = client.get_latest_versions(model_name, stages=["Production", "Staging"])
                if latest:
                    run_id_prev = latest[0].run_id
                    contract_path_prev = mlflow.artifacts.download_artifacts(
                        run_id=run_id_prev,
                        artifact_path="feature_contract.json",
                    )
                    previous_contract = FeatureContract.load(contract_path_prev)
                    previous_schema_version = previous_contract.schema_version
                    context.log.info(f"📋 Loaded previous contract: v{previous_schema_version}")
            except Exception:
                pass
        except Exception as e:
            context.log.debug(f"Could not load previous contract: {e}")

        # Create new contract using FeatureContract class (with schema evolution)
        feature_contract = FeatureContract(
            feature_order=feature_cols,
            category_mappings=category_mappings,
            feature_stats=feature_stats,
            primitive_features=primitive_features,
            aggregate_features=aggregate_features,
            derived_features=derived_features,
            risk_thresholds=risk_thresholds,
            created_at=datetime.now().isoformat(),
            training_samples=len(X_train),
            model_version=generator.registry.version,
            schema_version=previous_schema_version,  # Start from previous
            min_compatible_version=previous_schema_version,
        )

        # SCHEMA EVOLUTION: Detect changes and bump version appropriately
        schema_evolution_log = []
        if previous_contract:
            diff = feature_contract.get_schema_diff(previous_contract)

            # Determine version bump type based on changes
            if diff["removed_features"] or diff["type_changes"]:
                # MAJOR: Breaking changes (features removed or types changed)
                feature_contract.bump_version("major")
                schema_evolution_log.append(f"MAJOR bump: removed={diff['removed_features']}, type_changes={list(diff['type_changes'].keys())}")
            elif diff["added_features"]:
                # MINOR: New features added (backward compatible)
                feature_contract.bump_version("minor")
                schema_evolution_log.append(f"MINOR bump: added={diff['added_features']}")
            elif diff["threshold_changes"] or diff["categorical_changes"]:
                # PATCH: Threshold or categorical tweaks
                feature_contract.bump_version("patch")
                schema_evolution_log.append(f"PATCH bump: thresholds={list(diff['threshold_changes'].keys())}, categories={list(diff['categorical_changes'].keys())}")

            # Log schema evolution details
            if schema_evolution_log:
                context.log.info(f"📊 Schema Evolution: {previous_schema_version} → {feature_contract.schema_version}")
                for log_entry in schema_evolution_log:
                    context.log.info(f"   {log_entry}")
        else:
            context.log.info(f"📋 New contract created: v{feature_contract.schema_version}")

        # Save contract to file using FeatureContract.save()
        contract_path = "/tmp/feature_contract.json"
        feature_contract.save(contract_path)

        # Log contract as artifact
        mlflow.log_artifact(contract_path)

        # Log schema evolution metadata to MLflow
        mlflow.log_param("schema_version", feature_contract.schema_version)
        mlflow.log_param("min_compatible_version", feature_contract.min_compatible_version)
        mlflow.log_param("feature_count", len(feature_cols))
        mlflow.log_param("categorical_count", len(category_mappings))

        context.log.info(f"✓ Feature contract saved (v{feature_contract.schema_version}, {len(feature_cols)} features, {len(category_mappings)} categorical)")
        
        # Log model
        mlflow.sklearn.log_model(
            model,
            "model",
            registered_model_name=f"{project_name}_model"
        )
        
        run_id = mlflow.active_run().info.run_id
        
        # Update lineage with MLflow run (non-critical)
        if tracker and feature_version_id != "unknown":
            try:
                tracker.update_mlflow_run(
                    version_id=feature_version_id,
                    mlflow_run_id=run_id,
                    mlflow_experiment=project_name,
                )
            except Exception as e:
                context.log.warning(f"Could not update lineage with MLflow run: {e}")
        
        context.log.info(f"""
✅ Model Training Complete

   🔗 Lineage:
   Feature Version: {feature_version_id}
   MLflow Run: {run_id}
   
   📊 Metrics:
   AUC: {auc:.4f}
   Accuracy: {accuracy:.4f}
   Precision: {precision_fraud:.4f}
   Recall: {recall_fraud:.4f}
   F1: {f1_fraud:.4f}
   
   Confusion Matrix:
   TP: {tp}, FP: {fp}
   TN: {tn}, FN: {fn}
        """)
        
        return MaterializeResult(
            metadata={
                "feature_version_id": feature_version_id,
                "mlflow_run_id": run_id,
                "auc": auc,
                "accuracy": accuracy,
                "precision": precision_fraud,
                "recall": recall_fraud,
                "f1": f1_fraud,
                "feature_count": len(feature_cols),
                "true_positives": tp,
                "false_positives": fp,
                "false_negatives": fn,
                "top_feature": top_10[0]['feature'] if top_10 else "unknown",
            }
        )


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 5: MATERIALIZE TO REDIS FOR ONLINE SERVING
# ═══════════════════════════════════════════════════════════════════════════════

@asset(
    group_name="feature_pipeline",
    deps=["train_with_lineage"],
    description="Materialize Feast features to Redis for online serving",
)
def materialize_online_features(
    context: AssetExecutionContext,
    feast: FeastResource,
    trino: TrinoResource,
) -> MaterializeResult:
    """
    Step 5: Materialize features to Redis for online serving.
    
    IMPORTANT: Uses dynamic date range from actual data, not hardcoded dates.
    """
    from datetime import timedelta
    
    # Get patched store
    store = feast.get_store()
    
    # Get feature views first
    feature_views = store.list_feature_views()
    fv_names = [fv.name for fv in feature_views]
    
    if not fv_names:
        context.log.warning("No feature views found to materialize")
        return MaterializeResult(
            metadata={
                "status": "no_feature_views",
                "materialized_count": 0,
                "materialized_views": [],
                "failed_views": [],
            }
        )
    
    context.log.info(f"Found {len(fv_names)} feature views: {fv_names}")
    
    # DYNAMIC DATE RANGE: Query actual data timestamps
    # This ensures materialization covers the real data, not hardcoded dates
    try:
        date_query = f"""
        SELECT
            MIN(event_timestamp) as min_ts,
            MAX(event_timestamp) as max_ts
        FROM {TRINO_CATALOG}.gold.fraud_training_data
        WHERE event_timestamp IS NOT NULL
        """
        date_df = trino.execute_query_as_dataframe(date_query)
        
        if date_df is not None and len(date_df) > 0 and date_df['min_ts'].iloc[0] is not None:
            # Use actual data range with some buffer
            min_ts = pd.to_datetime(date_df['min_ts'].iloc[0])
            max_ts = pd.to_datetime(date_df['max_ts'].iloc[0])
            
            # Add buffer: start 1 day before min, end 1 day after max (or now if max is recent)
            start_date = (min_ts - timedelta(days=1)).to_pydatetime()
            end_date = max(
                (max_ts + timedelta(days=1)).to_pydatetime(),
                datetime.now()
            )
            
            context.log.info(f"📅 Dynamic date range from data:")
            context.log.info(f"   Data min: {min_ts}")
            context.log.info(f"   Data max: {max_ts}")
            context.log.info(f"   Materialize: {start_date} to {end_date}")
        else:
            # Fallback: use last 6 months to today
            context.log.warning("Could not get data timestamps, using fallback range")
            end_date = datetime.now()
            start_date = end_date - timedelta(days=180)
            
    except Exception as e:
        context.log.warning(f"Could not query data timestamps: {e}")
        # Fallback: use last 6 months to today
        end_date = datetime.now()
        start_date = end_date - timedelta(days=180)
        context.log.info(f"Using fallback range: {start_date} to {end_date}")
    
    context.log.info(f"Materializing features from {start_date} to {end_date}")
    
    # Materialize each feature view
    materialized: List[str] = []
    failed: List[str] = []
    
    for fv_name in fv_names:
        context.log.info(f"Materializing {fv_name}...")
        try:
            store.materialize(
                start_date=start_date,
                end_date=end_date,
                feature_views=[fv_name]
            )
            materialized.append(fv_name)
            context.log.info(f"✓ {fv_name} materialized")
        except Exception as e:
            failed.append(fv_name)
            context.log.warning(f"⚠ {fv_name} failed: {e}")
    
    status = "success" if not failed else ("partial" if materialized else "failed")
    
    context.log.info(f"✅ Materialized {len(materialized)}/{len(fv_names)} feature views to Redis")
    if failed:
        context.log.warning(f"Failed views: {failed}")
    
    return MaterializeResult(
        metadata={
            "status": status,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "materialized_count": len(materialized),
            "materialized_views": materialized,
            "failed_views": failed,
        }
    )


# ═══════════════════════════════════════════════════════════════════════════════
# UTILITY ASSETS
# ═══════════════════════════════════════════════════════════════════════════════

@asset(
    group_name="feature_pipeline",
    description="Show feature change history",
)
def feature_change_history(
    context: AssetExecutionContext,
    config: FeaturePipelineConfig,
) -> MaterializeResult:
    """
    Utility: Show recent changes to features.
    """
    from feature_registry.generator import FeatureGenerator
    
    generator = FeatureGenerator.from_yaml(config.yaml_path)
    project_name = generator.registry.project_name
    
    tracker = safe_get_tracker(project_name)
    recent_changes: List[Dict[str, str]] = []
    change_count = 0
    
    if tracker:
        try:
            history = tracker.get_change_history(limit=20)
            change_count = len(history)
            
            context.log.info(f"Recent changes for {project_name}:")
            for change in history:
                context.log.info(f"  [{change.change_type}] {change.feature_name}: {change.description}")
                recent_changes.append({
                    "type": change.change_type,
                    "feature": change.feature_name,
                    "description": change.description,
                })
        except Exception as e:
            context.log.warning(f"Could not get change history: {e}")
    else:
        context.log.warning("Lineage tracker unavailable")
    
    return MaterializeResult(
        metadata={
            "project_name": project_name,
            "change_count": change_count,
            "recent_changes": recent_changes[:5],
        }
    )


# ═══════════════════════════════════════════════════════════════════════════════
# JOB DEFINITIONS
# ═══════════════════════════════════════════════════════════════════════════════

feature_pipeline_job = define_asset_job(
    name="feature_pipeline_job",
    selection=AssetSelection.groups("feature_pipeline"),
    description="""
    Complete feature pipeline:
    1. Generate code from YAML
    2. Apply Feast definitions
    3. Build training data via Feast (point-in-time correct)
    4. Train model with lineage
    5. Materialize to Redis
    """,
)


# ═══════════════════════════════════════════════════════════════════════════════
# EXPORTS
# ═══════════════════════════════════════════════════════════════════════════════

FEATURE_PIPELINE_ASSETS = [
    generate_feature_code,
    apply_feast_definitions,
    create_training_data_feast,
    train_with_lineage,
    materialize_online_features,
    feature_change_history,
]

FEATURE_PIPELINE_JOBS = [
    feature_pipeline_job,
]