"""
Jupyter Integration - Production Ready
Bronze → Jupyter (manual or auto via Papermill) → Silver

Environment Variables:
- SILVER_TABLE, BRONZE_TABLE: Table names
- LABELSTUDIO_MAX_IN_REVIEW: Rate limit threshold
- SENSOR_EXPORT_INTERVAL_SECONDS: Sensor check interval
- JUPYTER_AUTO_MODE: 'true' for Papermill automation, 'false' for manual
- JUPYTER_NOTEBOOK_PATH: Path to notebook for Papermill execution
"""
import os
from datetime import datetime
from dagster import (
    asset,
    sensor,
    AssetExecutionContext,
    SensorEvaluationContext,
    MaterializeResult,
    RunRequest,
    SkipReason,
    DefaultSensorStatus,
    AssetSelection,
    define_asset_job,
    MetadataValue,
    DagsterRunStatus,
    RunsFilter,
)
from src.core.resources import TrinoResource
from src.core.config import TRINO_CATALOG


def is_auto_mode() -> bool:
    """Check if Jupyter automation is enabled."""
    return os.getenv("JUPYTER_AUTO_MODE", "false").lower() == "true"


def run_notebook_papermill(notebook_path: str, output_path: str, parameters: dict, context) -> dict:
    """
    Run a Jupyter notebook via Papermill.

    Returns dict with execution results.
    """
    try:
        import papermill as pm
    except ImportError:
        raise RuntimeError(
            "Papermill not installed. Add 'papermill' to requirements or set JUPYTER_AUTO_MODE=false"
        )

    context.log.info(f"Running notebook via Papermill: {notebook_path}")
    context.log.info(f"Parameters: {parameters}")

    # Execute notebook
    pm.execute_notebook(
        notebook_path,
        output_path,
        parameters=parameters,
        kernel_name="python3",
        progress_bar=False,
    )

    context.log.info(f"✓ Notebook execution complete: {output_path}")
    return {"output_path": output_path, "status": "success"}


def get_env_int(key: str, default: int = None) -> int:
    """Get integer environment variable."""
    value = os.getenv(key)
    if value:
        return int(value)
    if default is not None:
        return default
    raise ValueError(f"Required environment variable {key} is not set")


@asset(
    group_name="mlops_jupyter",
    deps=["mlops_bronze_ingestion"],
    description="Bronze → Silver: Manual (Jupyter) or Auto (Papermill) based on JUPYTER_AUTO_MODE",
)
def mlops_notify_jupyter(
    context: AssetExecutionContext,
    trino: TrinoResource,
) -> MaterializeResult:
    """
    After Bronze ready, either:
    - Auto mode (JUPYTER_AUTO_MODE=true): Run notebook via Papermill
    - Manual mode (JUPYTER_AUTO_MODE=false): Notify user to open Jupyter
    """

    bronze_table = os.getenv("BRONZE_TABLE", f"{TRINO_CATALOG}.bronze.fraud_transactions")
    silver_table = os.getenv("SILVER_TABLE", f"{TRINO_CATALOG}.silver.fraud_transactions")
    auto_mode = is_auto_mode()

    # Get Bronze stats
    try:
        count = trino.get_count(bronze_table)

        # Fraud distribution
        fraud_count = 0
        legit_count = 0
        try:
            dist = trino.execute_query(f"""
                SELECT is_fraudulent, COUNT(*)
                FROM {bronze_table}
                GROUP BY is_fraudulent
            """)
            for row in dist:
                if row[0] == 1:
                    fraud_count = row[1]
                else:
                    legit_count = row[1]
        except Exception as e:
            context.log.warning(f"Could not calculate fraud distribution: {e}")

        fraud_rate = fraud_count / max(fraud_count + legit_count, 1) * 100

    except Exception as e:
        context.log.error(f"Failed to analyze Bronze table: {e}")
        raise

    context.log.info(f"""
══════════════════════════════════════════════════════════
📊 BRONZE DATA READY - {count:,} records
══════════════════════════════════════════════════════════
✓ Fraud: {fraud_count:,} ({fraud_rate:.1f}%)
✓ Legit: {legit_count:,}
✓ Mode: {'AUTO (Papermill)' if auto_mode else 'MANUAL (Jupyter)'}
══════════════════════════════════════════════════════════
    """)

    if auto_mode:
        # Run notebook via Papermill
        notebook_path = os.getenv(
            "JUPYTER_NOTEBOOK_PATH",
            "/app/notebooks/01_bronze_to_silver_FIXED.ipynb"
        )
        output_path = f"/tmp/notebook_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.ipynb"

        # Note: Notebook uses os.getenv() for config, so env vars from docker-compose are used
        # No parameters dict needed - env vars TRINO_HOST, TRINO_PORT, ICEBERG_CATALOG are already set
        parameters = {}

        try:
            result = run_notebook_papermill(notebook_path, output_path, parameters, context)

            # Verify Silver table was updated
            silver_count = trino.get_count(silver_table)
            context.log.info(f"✓ Silver table now has {silver_count:,} records")

            return MaterializeResult(
                metadata={
                    "mode": MetadataValue.text("auto"),
                    "bronze_rows": MetadataValue.int(count),
                    "silver_rows": MetadataValue.int(silver_count),
                    "fraud_rate": MetadataValue.float(fraud_rate),
                    "notebook_output": MetadataValue.path(output_path),
                }
            )
        except Exception as e:
            context.log.error(f"Papermill execution failed: {e}")
            raise

    else:
        # Manual mode - just notify
        context.log.info(f"""
📋 NEXT STEPS (Manual Mode):
   1. Open Jupyter: http://localhost:18888
   2. Run notebook: work/01_bronze_to_silver_FIXED.ipynb
   3. Re-run downstream assets after notebook completes

💡 To enable auto-execution, set JUPYTER_AUTO_MODE=true in .env
══════════════════════════════════════════════════════════
        """)

        return MaterializeResult(
            metadata={
                "mode": MetadataValue.text("manual"),
                "bronze_rows": MetadataValue.int(count),
                "fraud_rate": MetadataValue.float(fraud_rate),
                "next_step": MetadataValue.text("Run Jupyter notebook manually"),
            }
        )


# Job for Label Studio export
mlops_labelstudio_export_job = define_asset_job(
    name="mlops_labelstudio_export",
    selection=AssetSelection.assets(
        "mlops_ensure_review_columns",
        "mlops_export_to_labelstudio", 
        "mlops_notify_labelstudio",
    ),
    description="Export Silver to Label Studio",
)

# Sensor interval from env
_EXPORT_SENSOR_INTERVAL = get_env_int("SENSOR_EXPORT_INTERVAL_SECONDS", 300)


@sensor(
    job=mlops_labelstudio_export_job,
    default_status=DefaultSensorStatus.STOPPED,
    minimum_interval_seconds=_EXPORT_SENSOR_INTERVAL,
)
def silver_table_sensor(
    context: SensorEvaluationContext,
    trino: TrinoResource,
):
    """
    Triggers Label Studio export when pending rows exist.
    
    Deduplication relies on:
    1. Running job check (prevents concurrent runs)
    2. State-based checks (no pending = no trigger)
    
    No cursor used - allows automatic retry if job fails.
    """
    
    # Check for running jobs first - THIS is the primary deduplication
    try:
        running = context.instance.get_runs(
            filters=RunsFilter(
                job_name="mlops_labelstudio_export",
                statuses=[DagsterRunStatus.STARTED, DagsterRunStatus.QUEUED, DagsterRunStatus.STARTING],
            ),
            limit=1,
        )
        if running:
            return SkipReason(f"Export job already running: {running[0].run_id}")
    except Exception as e:
        context.log.warning(f"Could not check running jobs: {e}")
    
    silver_table = os.getenv("SILVER_TABLE", f"{TRINO_CATALOG}.silver.fraud_transactions")
    
    try:
        # Check table exists
        try:
            trino.execute_query(f"SELECT 1 FROM {silver_table} LIMIT 1")
        except:
            return SkipReason("Silver table does not exist yet")
        
        # Get counts
        pending = trino.execute_query(
            f"SELECT COUNT(*) FROM {silver_table} WHERE review_status = 'pending'"
        )[0][0]
        
        in_review = trino.execute_query(
            f"SELECT COUNT(*) FROM {silver_table} WHERE review_status = 'in_review'"
        )[0][0]
        
        context.log.info(f"Status: pending={pending}, in_review={in_review}")
        
        # State-based checks
        if pending == 0:
            return SkipReason("No pending records")
        
        # Rate limit check
        max_in_review = get_env_int("LABELSTUDIO_MAX_IN_REVIEW", 50000)
        if in_review >= max_in_review:
            return SkipReason(f"Rate limit: {in_review} >= {max_in_review}")
        
        # Trigger run - unique run_key ensures no Dagster-level deduplication
        return RunRequest(
            run_key=f"export_{pending}_{int(datetime.now().timestamp())}",
            tags={"pending": str(pending), "in_review": str(in_review)},
        )
        
    except Exception as e:
        context.log.error(f"Sensor error: {e}")
        return SkipReason(f"Error: {str(e)[:100]}")


JUPYTER_ASSETS = [mlops_notify_jupyter]
JUPYTER_SENSORS = [silver_table_sensor]
JUPYTER_JOBS = [mlops_labelstudio_export_job]