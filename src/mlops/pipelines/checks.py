"""
Asset Checks for Data Quality Validation
Production-ready checks with alerting support.
"""
import os
from dagster import asset_check, AssetCheckResult, AssetCheckSeverity
from src.core.resources import TrinoResource
from src.core.config import TRINO_CATALOG


def send_alert(message: str, severity: str = "error"):
    """Send alert to Slack/webhook (placeholder)."""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        print(f"[ALERT][{severity}] {message}")
        return
    
    # Production: send to Slack
    import requests
    try:
        requests.post(
            webhook_url,
            json={"text": f"[{severity.upper()}] {message}"},
            timeout=10,
        )
    except Exception as e:
        print(f"Alert failed: {e}")


# =============================================================================
# MLOPS CHECKS
# =============================================================================

@asset_check(asset="mlops_bronze_ingestion")
def mlops_bronze_not_empty(trino: TrinoResource):
    """Check bronze table has data."""
    table = os.getenv("MLOPS_BRONZE_TABLE", f"{TRINO_CATALOG}.bronze.fraud_transactions")
    count = trino.get_count(table)
    
    if count == 0:
        send_alert(f"Bronze table {table} is EMPTY!", "error")
    
    return AssetCheckResult(
        passed=count > 0,
        metadata={"row_count": count},
        severity=AssetCheckSeverity.ERROR,
    )


@asset_check(asset="mlops_silver_transform")
def mlops_silver_not_empty(trino: TrinoResource):
    """Check silver table has data."""
    table = os.getenv("MLOPS_SILVER_TABLE", f"{TRINO_CATALOG}.silver.features")
    count = trino.get_count(table)
    
    if count == 0:
        send_alert(f"Silver table {table} is EMPTY!", "error")
    
    return AssetCheckResult(
        passed=count > 0,
        metadata={"row_count": count},
        severity=AssetCheckSeverity.ERROR,
    )


@asset_check(asset="mlops_silver_transform")
def mlops_silver_no_null_pk(trino: TrinoResource):
    """Check primary key has no nulls."""
    table = os.getenv("MLOPS_SILVER_TABLE", f"{TRINO_CATALOG}.silver.features")
    pk = os.getenv("MLOPS_PRIMARY_KEY", "order_id")
    
    result = trino.execute_query(f"SELECT COUNT(*) FROM {table} WHERE {pk} IS NULL")
    null_count = result[0][0] if result else 0
    
    if null_count > 0:
        send_alert(f"Found {null_count} NULL primary keys in {table}!", "error")
    
    return AssetCheckResult(
        passed=null_count == 0,
        metadata={"null_pk_count": null_count},
        severity=AssetCheckSeverity.ERROR,
    )


@asset_check(asset="mlops_gold_with_annotations")
def mlops_gold_has_annotations(trino: TrinoResource):
    """Check gold table has annotated records."""
    table = os.getenv("MLOPS_GOLD_TABLE", f"{TRINO_CATALOG}.gold.training_data")
    count = trino.get_count(table)
    
    min_required = int(os.getenv("MIN_GOLD_ROWS", "10"))
    passed = count >= min_required
    
    if not passed:
        send_alert(f"Gold table has only {count} rows (need {min_required})", "warn")
    
    return AssetCheckResult(
        passed=passed,
        metadata={"row_count": count, "min_required": min_required},
        severity=AssetCheckSeverity.WARN,
    )


# =============================================================================
# CVOPS CHECKS (Import from modular package if available)
# =============================================================================

# Try to import comprehensive checks from modular CVOps package
try:
    from src.cvops.pipelines.checks import CVOPS_CHECKS
    print("[Dagster] CVOps checks loaded from modular package")
except ImportError:
    # Fallback to basic checks
    print("[Dagster] Using basic CVOps checks (modular package not available)")
    
    @asset_check(asset="cvops_create_manifest")
    def cvops_manifest_not_empty(trino: TrinoResource):
        """Check manifest table has images."""
        table = f"{TRINO_CATALOG}.bronze.cv_image_manifest"
        try:
            count = trino.get_count(table)
        except:
            # Try alternate table name
            try:
                count = trino.get_count(f"{TRINO_CATALOG}.cv.image_metadata")
            except:
                count = 0
        
        if count == 0:
            send_alert("CV manifest has no images!", "error")
        
        return AssetCheckResult(
            passed=count > 0,
            metadata={"image_count": count},
            severity=AssetCheckSeverity.ERROR,
        )


    @asset_check(asset="cvops_run_detections")
    def cvops_detections_exist(trino: TrinoResource):
        """Check detections table has data."""
        table = f"{TRINO_CATALOG}.silver.cv_detections"
        try:
            count = trino.get_count(table)
        except:
            # Try alternate table name
            try:
                count = trino.get_count(f"{TRINO_CATALOG}.cv.detection_results")
            except:
                count = 0
        
        return AssetCheckResult(
            passed=count > 0,
            metadata={"detection_count": count},
            severity=AssetCheckSeverity.WARN,
        )

    CVOPS_CHECKS = [
        cvops_manifest_not_empty,
        cvops_detections_exist,
    ]


# =============================================================================
# COLLECT ALL CHECKS
# =============================================================================

MLOPS_CHECKS = [
    mlops_bronze_not_empty,
    mlops_silver_not_empty,
    mlops_silver_no_null_pk,
    mlops_gold_has_annotations,
]

ALL_CHECKS = MLOPS_CHECKS + CVOPS_CHECKS
