"""
CVOps Asset Checks for Data Quality Validation
Aligned with MLOps checks.py patterns.

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
        print(f"[CVOPS ALERT][{severity}] {message}")
        return
    
    import requests
    try:
        requests.post(
            webhook_url,
            json={"text": f"[CVOPS][{severity.upper()}] {message}"},
            timeout=10,
        )
    except Exception as e:
        print(f"Alert failed: {e}")


# =============================================================================
# BRONZE LAYER CHECKS
# =============================================================================

@asset_check(asset="cvops_init_tables")
def cvops_schema_exists(trino: TrinoResource):
    """Check CV schema exists."""
    try:
        result = trino.execute_query("""
            SELECT COUNT(*) FROM information_schema.schemata 
            WHERE schema_name = 'cv'
        """)
        exists = result[0][0] > 0 if result else False
        
        if not exists:
            send_alert("CV schema does not exist!", "error")
        
        return AssetCheckResult(
            passed=exists,
            metadata={"schema": f"{TRINO_CATALOG}.cv"},
            severity=AssetCheckSeverity.ERROR,
        )
    except Exception as e:
        return AssetCheckResult(
            passed=False,
            metadata={"error": str(e)},
            severity=AssetCheckSeverity.ERROR,
        )


@asset_check(asset="cvops_create_manifest")
def cvops_image_metadata_not_empty(trino: TrinoResource):
    """Check image_metadata table has data."""
    table = f"{TRINO_CATALOG}.cv.image_metadata"
    try:
        count = trino.get_count(table)
        
        if count == 0:
            send_alert(f"CV image_metadata table is EMPTY!", "error")
        
        return AssetCheckResult(
            passed=count > 0,
            metadata={"image_count": count},
            severity=AssetCheckSeverity.ERROR,
        )
    except Exception as e:
        return AssetCheckResult(
            passed=False,
            metadata={"error": str(e)},
            severity=AssetCheckSeverity.ERROR,
        )


@asset_check(asset="cvops_create_manifest")
def cvops_no_duplicate_images(trino: TrinoResource):
    """Check for duplicate image IDs."""
    table = f"{TRINO_CATALOG}.cv.image_metadata"
    try:
        result = trino.execute_query(f"""
            SELECT COUNT(*) - COUNT(DISTINCT image_id) as duplicates
            FROM {table}
        """)
        duplicate_count = result[0][0] if result else 0
        
        if duplicate_count > 0:
            send_alert(f"Found {duplicate_count} duplicate images in CV manifest!", "warn")
        
        return AssetCheckResult(
            passed=duplicate_count == 0,
            metadata={"duplicate_count": duplicate_count},
            severity=AssetCheckSeverity.WARN,
        )
    except Exception as e:
        return AssetCheckResult(
            passed=False,
            metadata={"error": str(e)},
            severity=AssetCheckSeverity.WARN,
        )


# =============================================================================
# SILVER LAYER CHECKS (Detection Results)
# =============================================================================

@asset_check(asset="cvops_run_detections")
def cvops_detections_not_empty(trino: TrinoResource):
    """Check detection_results table has data."""
    table = f"{TRINO_CATALOG}.cv.detection_results"
    try:
        count = trino.get_count(table)
        
        return AssetCheckResult(
            passed=count > 0,
            metadata={"detection_count": count},
            severity=AssetCheckSeverity.WARN,
        )
    except Exception as e:
        return AssetCheckResult(
            passed=False,
            metadata={"error": str(e)},
            severity=AssetCheckSeverity.WARN,
        )


@asset_check(asset="cvops_run_detections")
def cvops_detections_have_valid_bbox(trino: TrinoResource):
    """Check all bounding boxes are valid (0-1 normalized or positive)."""
    table = f"{TRINO_CATALOG}.cv.detection_results"
    try:
        result = trino.execute_query(f"""
            SELECT COUNT(*) 
            FROM {table}
            WHERE bbox_x1 < 0 OR bbox_y1 < 0 
               OR bbox_x2 < bbox_x1 OR bbox_y2 < bbox_y1
               OR confidence < 0 OR confidence > 1
        """)
        invalid_count = result[0][0] if result else 0
        
        if invalid_count > 0:
            send_alert(f"Found {invalid_count} invalid bounding boxes!", "error")
        
        return AssetCheckResult(
            passed=invalid_count == 0,
            metadata={"invalid_bbox_count": invalid_count},
            severity=AssetCheckSeverity.ERROR,
        )
    except Exception as e:
        return AssetCheckResult(
            passed=False,
            metadata={"error": str(e)},
            severity=AssetCheckSeverity.ERROR,
        )


@asset_check(asset="cvops_run_detections")
def cvops_all_images_processed(trino: TrinoResource):
    """Check if there are unprocessed images."""
    try:
        result = trino.execute_query(f"""
            SELECT COUNT(*) FROM {TRINO_CATALOG}.cv.image_metadata
            WHERE processed = FALSE
        """)
        unprocessed = result[0][0] if result else 0
        
        # Allow some backlog but warn if too many
        threshold = int(os.getenv("CVOPS_UNPROCESSED_THRESHOLD", "1000"))
        passed = unprocessed < threshold
        
        if not passed:
            send_alert(f"{unprocessed} images waiting for detection!", "warn")
        
        return AssetCheckResult(
            passed=passed,
            metadata={"unprocessed_count": unprocessed, "threshold": threshold},
            severity=AssetCheckSeverity.WARN,
        )
    except Exception as e:
        return AssetCheckResult(
            passed=False,
            metadata={"error": str(e)},
            severity=AssetCheckSeverity.WARN,
        )


# =============================================================================
# ANNOTATION LAYER CHECKS
# =============================================================================

@asset_check(asset="cvops_merge_annotations")
def cvops_annotations_exist(trino: TrinoResource):
    """Check annotations table has data."""
    table = f"{TRINO_CATALOG}.cv.annotations"
    try:
        count = trino.get_count(table)
        
        return AssetCheckResult(
            passed=count > 0,
            metadata={"annotation_count": count},
            severity=AssetCheckSeverity.WARN,
        )
    except Exception as e:
        return AssetCheckResult(
            passed=False,
            metadata={"error": str(e)},
            severity=AssetCheckSeverity.WARN,
        )


@asset_check(asset="cvops_merge_annotations")
def cvops_annotations_have_valid_labels(trino: TrinoResource):
    """Check annotations have non-null class labels."""
    table = f"{TRINO_CATALOG}.cv.annotations"
    try:
        result = trino.execute_query(f"""
            SELECT COUNT(*) 
            FROM {table}
            WHERE class_label IS NULL OR class_label = ''
        """)
        null_labels = result[0][0] if result else 0
        
        if null_labels > 0:
            send_alert(f"Found {null_labels} annotations with null/empty labels!", "warn")
        
        return AssetCheckResult(
            passed=null_labels == 0,
            metadata={"null_label_count": null_labels},
            severity=AssetCheckSeverity.WARN,
        )
    except Exception as e:
        return AssetCheckResult(
            passed=False,
            metadata={"error": str(e)},
            severity=AssetCheckSeverity.WARN,
        )


# =============================================================================
# GOLD LAYER CHECKS (Training Data)
# =============================================================================

@asset_check(asset="cvops_create_training_data")
def cvops_training_data_sufficient(trino: TrinoResource):
    """Check training data has minimum required samples."""
    table = f"{TRINO_CATALOG}.cv.training_data"
    min_required = int(os.getenv("CVOPS_MIN_TRAINING_SAMPLES", "100"))
    
    try:
        count = trino.get_count(table)
        passed = count >= min_required
        
        if not passed:
            send_alert(f"Training data has only {count} samples (need {min_required})", "warn")
        
        return AssetCheckResult(
            passed=passed,
            metadata={"sample_count": count, "min_required": min_required},
            severity=AssetCheckSeverity.WARN,
        )
    except Exception as e:
        return AssetCheckResult(
            passed=False,
            metadata={"error": str(e)},
            severity=AssetCheckSeverity.WARN,
        )


@asset_check(asset="cvops_create_training_data")
def cvops_training_data_balanced(trino: TrinoResource):
    """Check class distribution is not severely imbalanced."""
    table = f"{TRINO_CATALOG}.cv.training_data"
    try:
        result = trino.execute_query(f"""
            SELECT class_label, COUNT(*) as count
            FROM {table}
            GROUP BY class_label
            ORDER BY count DESC
        """)
        
        if not result or len(result) < 2:
            return AssetCheckResult(
                passed=True,
                metadata={"message": "Single class or no data"},
                severity=AssetCheckSeverity.WARN,
            )
        
        counts = [row[1] for row in result]
        max_count = max(counts)
        min_count = min(counts)
        
        # Check if max class is more than 10x min class
        imbalance_ratio = max_count / min_count if min_count > 0 else float('inf')
        threshold = float(os.getenv("CVOPS_MAX_CLASS_IMBALANCE", "10.0"))
        passed = imbalance_ratio <= threshold
        
        if not passed:
            send_alert(f"Class imbalance ratio {imbalance_ratio:.1f}x exceeds {threshold}x!", "warn")
        
        return AssetCheckResult(
            passed=passed,
            metadata={
                "imbalance_ratio": round(imbalance_ratio, 2),
                "threshold": threshold,
                "class_distribution": {row[0]: row[1] for row in result}
            },
            severity=AssetCheckSeverity.WARN,
        )
    except Exception as e:
        return AssetCheckResult(
            passed=False,
            metadata={"error": str(e)},
            severity=AssetCheckSeverity.WARN,
        )


# =============================================================================
# TRAINING CHECKS
# =============================================================================

@asset_check(asset="cvops_train_model")
def cvops_model_metrics_logged(trino: TrinoResource):
    """Check model metrics were logged."""
    table = f"{TRINO_CATALOG}.cv.model_metrics"
    try:
        result = trino.execute_query(f"""
            SELECT COUNT(*) FROM {table}
            WHERE training_completed_at > CURRENT_TIMESTAMP - INTERVAL '7' DAY
        """)
        recent_trainings = result[0][0] if result else 0
        
        return AssetCheckResult(
            passed=recent_trainings > 0,
            metadata={"recent_training_runs": recent_trainings},
            severity=AssetCheckSeverity.WARN,
        )
    except Exception as e:
        return AssetCheckResult(
            passed=False,
            metadata={"error": str(e)},
            severity=AssetCheckSeverity.WARN,
        )


@asset_check(asset="cvops_train_model")
def cvops_model_performance_acceptable(trino: TrinoResource):
    """Check latest model has acceptable performance."""
    table = f"{TRINO_CATALOG}.cv.model_metrics"
    min_map50 = float(os.getenv("CVOPS_MIN_MAP50", "0.3"))
    
    try:
        result = trino.execute_query(f"""
            SELECT map50 FROM {table}
            ORDER BY training_completed_at DESC
            LIMIT 1
        """)
        
        if not result:
            return AssetCheckResult(
                passed=True,
                metadata={"message": "No training metrics found"},
                severity=AssetCheckSeverity.WARN,
            )
        
        map50 = result[0][0] if result[0][0] else 0
        passed = map50 >= min_map50
        
        if not passed:
            send_alert(f"Model mAP@0.5 ({map50:.3f}) below threshold ({min_map50})!", "warn")
        
        return AssetCheckResult(
            passed=passed,
            metadata={"map50": map50, "threshold": min_map50},
            severity=AssetCheckSeverity.WARN,
        )
    except Exception as e:
        return AssetCheckResult(
            passed=False,
            metadata={"error": str(e)},
            severity=AssetCheckSeverity.WARN,
        )


# =============================================================================
# LINEAGE CHECKS
# =============================================================================

@asset_check(asset="cvops_create_manifest")
def cvops_has_lakefs_commit(trino: TrinoResource):
    """Check images have source_lakefs_commit for lineage."""
    table = f"{TRINO_CATALOG}.cv.image_metadata"
    try:
        result = trino.execute_query(f"""
            SELECT 
                COUNT(*) as total,
                COUNT(source_lakefs_commit) as with_commit
            FROM {table}
        """)
        
        total = result[0][0] if result else 0
        with_commit = result[0][1] if result else 0
        
        if total == 0:
            return AssetCheckResult(
                passed=True,
                metadata={"message": "No data yet"},
                severity=AssetCheckSeverity.WARN,
            )
        
        coverage = with_commit / total if total > 0 else 0
        passed = coverage >= 0.95  # 95% should have commits
        
        if not passed:
            send_alert(f"Only {coverage*100:.1f}% of images have LakeFS commit tracking!", "warn")
        
        return AssetCheckResult(
            passed=passed,
            metadata={
                "total_images": total,
                "with_commit": with_commit,
                "coverage_percent": round(coverage * 100, 1)
            },
            severity=AssetCheckSeverity.WARN,
        )
    except Exception as e:
        return AssetCheckResult(
            passed=False,
            metadata={"error": str(e)},
            severity=AssetCheckSeverity.WARN,
        )


# =============================================================================
# COLLECT ALL CHECKS
# =============================================================================

CVOPS_CHECKS = [
    # Schema
    cvops_schema_exists,
    
    # Bronze
    cvops_image_metadata_not_empty,
    cvops_no_duplicate_images,
    cvops_has_lakefs_commit,
    
    # Silver
    cvops_detections_not_empty,
    cvops_detections_have_valid_bbox,
    cvops_all_images_processed,
    
    # Annotation
    cvops_annotations_exist,
    cvops_annotations_have_valid_labels,
    
    # Gold
    cvops_training_data_sufficient,
    cvops_training_data_balanced,
    
    # Training
    cvops_model_metrics_logged,
    cvops_model_performance_acceptable,
]
