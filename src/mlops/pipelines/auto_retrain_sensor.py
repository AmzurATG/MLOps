"""
Auto-Retrain Sensor
===============================================================================

Dagster sensor that monitors model performance and data drift to automatically
trigger retraining when conditions are met.

Triggers retraining when:
1. Drift severity > threshold (default 0.3)
2. F1 score < threshold (default 0.7)
3. New labeled data > threshold (default 100)
4. Cooldown period has passed (default 24h)

Usage:
    The sensor is automatically registered with Dagster and runs on a schedule.
    Configure via environment variables or RETRAIN_CONFIG in pipelines/config.py.
"""

import os
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple

from dagster import (
    sensor,
    SensorResult,
    RunRequest,
    SkipReason,
    SensorEvaluationContext,
    DefaultSensorStatus,
)

from src.core.config import RETRAIN_CONFIG, PERFORMANCE_CONFIG, TRINO_CATALOG


# =============================================================================
# TABLE DEFINITIONS
# =============================================================================

DRIFT_REPORTS_TABLE = f"{TRINO_CATALOG}.monitoring.drift_reports"
PERFORMANCE_METRICS_TABLE = PERFORMANCE_CONFIG.get("metrics_table", f"{TRINO_CATALOG}.monitoring.performance_metrics")
RETRAINING_HISTORY_TABLE = PERFORMANCE_CONFIG.get("history_table", f"{TRINO_CATALOG}.monitoring.retraining_history")
GOLD_TABLE = f"{TRINO_CATALOG}.gold.fraud_training_data"


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def ensure_monitoring_tables(trino) -> bool:
    """
    Ensure monitoring tables exist. Creates them if needed.

    Returns True if tables are ready, False if creation failed.
    """
    try:
        # Check/create retraining history table
        if not trino.table_exists(RETRAINING_HISTORY_TABLE):
            print(f"[AutoRetrain] Creating table: {RETRAINING_HISTORY_TABLE}")
            trino.execute_ddl(f"""
                CREATE TABLE IF NOT EXISTS {RETRAINING_HISTORY_TABLE} (
                    retrain_id VARCHAR,
                    triggered_at TIMESTAMP,
                    trigger_reasons VARCHAR,
                    drift_score DOUBLE,
                    f1_score DOUBLE,
                    new_labels_count INTEGER,
                    dagster_run_id VARCHAR,
                    status VARCHAR,
                    model_stage VARCHAR,
                    completed_at TIMESTAMP,
                    partition_date DATE
                ) WITH (
                    format = 'PARQUET',
                    partitioning = ARRAY['partition_date']
                )
            """)

        # Check/create performance metrics table
        if not trino.table_exists(PERFORMANCE_METRICS_TABLE):
            print(f"[AutoRetrain] Creating table: {PERFORMANCE_METRICS_TABLE}")
            trino.execute_ddl(f"""
                CREATE TABLE IF NOT EXISTS {PERFORMANCE_METRICS_TABLE} (
                    metric_id VARCHAR,
                    evaluation_timestamp TIMESTAMP,
                    model_stage VARCHAR,
                    window_hours INTEGER,
                    accuracy DOUBLE,
                    precision_score DOUBLE,
                    recall DOUBLE,
                    f1_score DOUBLE,
                    auc_roc DOUBLE,
                    true_positives INTEGER,
                    false_positives INTEGER,
                    true_negatives INTEGER,
                    false_negatives INTEGER,
                    partition_date DATE
                ) WITH (
                    format = 'PARQUET',
                    partitioning = ARRAY['partition_date']
                )
            """)

        return True

    except Exception as e:
        print(f"[AutoRetrain] Failed to ensure monitoring tables: {e}")
        return False


def get_last_retrain_time(trino) -> Optional[datetime]:
    """Get the timestamp of the last successful retrain."""
    try:
        if not trino.table_exists(RETRAINING_HISTORY_TABLE):
            return None

        query = f"""
        SELECT MAX(triggered_at)
        FROM {RETRAINING_HISTORY_TABLE}
        WHERE status IN ('completed', 'running', 'triggered')
        """
        result = trino.execute_query(query)
        if result and result[0][0]:
            return result[0][0]
    except Exception as e:
        print(f"[AutoRetrain] Failed to get last retrain time: {e}")
    return None


def check_cooldown(last_retrain: Optional[datetime]) -> Tuple[bool, float]:
    """
    Check if cooldown period has passed.

    Returns:
        (cooldown_passed, hours_remaining)
    """
    if last_retrain is None:
        return True, 0.0

    cooldown_hours = RETRAIN_CONFIG["cooldown_hours"]
    cooldown_end = last_retrain + timedelta(hours=cooldown_hours)
    now = datetime.now()

    if now >= cooldown_end:
        return True, 0.0
    else:
        remaining = (cooldown_end - now).total_seconds() / 3600
        return False, remaining


def check_drift_trigger(trino) -> Tuple[bool, float]:
    """
    Check if drift severity exceeds threshold.

    Returns:
        (trigger_met, drift_score)
    """
    try:
        if not trino.table_exists(DRIFT_REPORTS_TABLE):
            print("[AutoRetrain] Drift reports table does not exist, skipping drift check")
            return False, 0.0

        query = f"""
        SELECT drift_share
        FROM {DRIFT_REPORTS_TABLE}
        ORDER BY report_timestamp DESC
        LIMIT 1
        """
        result = trino.execute_query(query)
        if result and result[0][0] is not None:
            drift_score = float(result[0][0])
            threshold = RETRAIN_CONFIG["drift_threshold"]
            return drift_score > threshold, drift_score
    except Exception as e:
        print(f"[AutoRetrain] Drift check failed: {e}")

    return False, 0.0


def check_performance_trigger(trino) -> Tuple[bool, float, str]:
    """
    Check if performance has degraded below threshold.

    Returns:
        (trigger_met, metric_value, metric_name)
    """
    try:
        if not trino.table_exists(PERFORMANCE_METRICS_TABLE):
            print("[AutoRetrain] Performance metrics table does not exist, skipping performance check")
            return False, 0.0, ""

        query = f"""
        SELECT f1_score, accuracy
        FROM {PERFORMANCE_METRICS_TABLE}
        WHERE model_stage = 'Production'
          AND window_hours = 24
        ORDER BY evaluation_timestamp DESC
        LIMIT 1
        """
        result = trino.execute_query(query)
        if result and len(result) > 0:
            f1 = float(result[0][0] or 0)
            accuracy = float(result[0][1] or 0)

            f1_threshold = RETRAIN_CONFIG["f1_threshold"]
            accuracy_threshold = RETRAIN_CONFIG["accuracy_threshold"]

            if f1 > 0 and f1 < f1_threshold:
                return True, f1, "f1_score"
            if accuracy > 0 and accuracy < accuracy_threshold:
                return True, accuracy, "accuracy"
    except Exception as e:
        print(f"[AutoRetrain] Performance check failed: {e}")

    return False, 0.0, ""


def check_new_labels_trigger(trino) -> Tuple[bool, int]:
    """
    Check if enough new labels have been added since last retrain.

    Returns:
        (trigger_met, new_labels_count)
    """
    try:
        if not trino.table_exists(GOLD_TABLE):
            print("[AutoRetrain] Gold table does not exist, skipping new labels check")
            return False, 0

        # Get last retrain time
        last_retrain = get_last_retrain_time(trino)
        if last_retrain is None:
            # Use a reasonable default (7 days ago)
            last_retrain = datetime.now() - timedelta(days=7)

        # Count new labels in gold table since last retrain
        query = f"""
        SELECT COUNT(*)
        FROM {GOLD_TABLE}
        WHERE created_at >= TIMESTAMP '{last_retrain.strftime('%Y-%m-%d %H:%M:%S')}'
        """
        result = trino.execute_query(query)
        if result and result[0][0] is not None:
            new_count = int(result[0][0])
            threshold = RETRAIN_CONFIG["min_new_labels"]
            return new_count >= threshold, new_count
    except Exception as e:
        print(f"[AutoRetrain] New labels check failed: {e}")

    return False, 0


def record_retrain_trigger(
    trino,
    trigger_reasons: List[str],
    drift_score: float,
    f1_score: float,
    new_labels: int,
    dagster_run_id: str,
) -> str:
    """Record a retrain trigger event in the history table."""
    from src.core.config import escape_sql_string
    retrain_id = f"RETRAIN_{uuid.uuid4().hex[:12]}"
    now = datetime.now()

    try:
        # Ensure table exists
        ensure_monitoring_tables(trino)

        # Escape string values to prevent SQL injection
        trigger_reasons_safe = escape_sql_string(",".join(trigger_reasons))
        dagster_run_id_safe = escape_sql_string(dagster_run_id)

        query = f"""
        INSERT INTO {RETRAINING_HISTORY_TABLE} VALUES (
            '{retrain_id}',
            TIMESTAMP '{now.strftime('%Y-%m-%d %H:%M:%S')}',
            '{trigger_reasons_safe}',
            {drift_score},
            {f1_score},
            {new_labels},
            '{dagster_run_id_safe}',
            'triggered',
            'Production',
            NULL,
            DATE '{now.strftime('%Y-%m-%d')}'
        )
        """
        trino.execute_ddl(query)
        print(f"[AutoRetrain] Recorded trigger: {retrain_id}")
    except Exception as e:
        print(f"[AutoRetrain] Failed to record trigger: {e}")

    return retrain_id


# =============================================================================
# DAGSTER SENSOR
# =============================================================================

@sensor(
    name="auto_retrain_sensor",
    job_name="feature_pipeline_job",
    minimum_interval_seconds=RETRAIN_CONFIG["sensor_interval_seconds"],
    default_status=DefaultSensorStatus.STOPPED,  # Start manually to avoid accidental triggers
    description="Monitors performance and drift to trigger automatic retraining",
)
def auto_retrain_sensor(context: SensorEvaluationContext):
    """
    Sensor that checks for retraining triggers.

    Checks:
    1. Cooldown - Skip if within cooldown period
    2. Drift - Trigger if drift severity > threshold
    3. Performance - Trigger if F1 or accuracy below threshold
    4. New Labels - Trigger if enough new labeled data

    Only triggers if at least one condition is met AND cooldown has passed.
    """
    # Check if auto-retrain is enabled
    if not RETRAIN_CONFIG["enabled"]:
        return SkipReason("Auto-retrain is disabled via RETRAIN_ENABLED=false")

    # Get Trino connection
    try:
        from src.core.resources import TrinoResource
        trino = TrinoResource()
    except Exception as e:
        return SkipReason(f"Trino not available: {e}")

    # Ensure monitoring tables exist (creates them if needed)
    if not ensure_monitoring_tables(trino):
        context.log.warning("Could not ensure monitoring tables exist")

    # Check cooldown
    last_retrain = get_last_retrain_time(trino)
    cooldown_passed, hours_remaining = check_cooldown(last_retrain)

    if not cooldown_passed:
        # Update metrics
        try:
            from api.metrics import update_retrain_cooldown
            update_retrain_cooldown(hours_remaining)
        except ImportError:
            pass
        return SkipReason(f"Cooldown active: {hours_remaining:.1f} hours remaining")

    # Check triggers
    trigger_reasons = []
    drift_score = 0.0
    f1_score = 1.0
    new_labels = 0

    # Drift check
    drift_triggered, drift_score = check_drift_trigger(trino)
    if drift_triggered:
        trigger_reasons.append(f"drift:{drift_score:.3f}")
        context.log.warning(f"Drift trigger: {drift_score:.3f} > {RETRAIN_CONFIG['drift_threshold']}")

    # Performance check
    perf_triggered, perf_value, perf_metric = check_performance_trigger(trino)
    if perf_triggered:
        trigger_reasons.append(f"{perf_metric}:{perf_value:.3f}")
        context.log.warning(f"Performance trigger: {perf_metric}={perf_value:.3f}")
        if perf_metric == "f1_score":
            f1_score = perf_value

    # New labels check
    labels_triggered, new_labels = check_new_labels_trigger(trino)
    if labels_triggered:
        trigger_reasons.append(f"new_labels:{new_labels}")
        context.log.info(f"New labels trigger: {new_labels} >= {RETRAIN_CONFIG['min_new_labels']}")

    # Decide whether to trigger
    if not trigger_reasons:
        return SkipReason("No retrain triggers detected")

    # Generate run request
    run_key = f"auto_retrain_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Track the trigger in metrics
    try:
        from api.metrics import track_auto_retrain_trigger
        for reason in trigger_reasons:
            reason_type = reason.split(":")[0]
            track_auto_retrain_trigger(reason_type)
    except ImportError:
        pass

    # Record in history table
    retrain_id = record_retrain_trigger(
        trino=trino,
        trigger_reasons=trigger_reasons,
        drift_score=drift_score,
        f1_score=f1_score,
        new_labels=new_labels,
        dagster_run_id=run_key,
    )

    context.log.info(f"Triggering retrain: {retrain_id} - Reasons: {trigger_reasons}")

    return RunRequest(
        run_key=run_key,
        run_config={
            "ops": {
                "feature_pipeline_training": {
                    "config": {
                        "trigger_type": "auto",
                        "trigger_reasons": trigger_reasons,
                        "retrain_id": retrain_id,
                    }
                }
            }
        },
        tags={
            "trigger_type": "auto_retrain",
            "retrain_id": retrain_id,
            "drift_score": str(drift_score),
            "f1_score": str(f1_score),
            "new_labels": str(new_labels),
        },
    )


# =============================================================================
# MANUAL TRIGGER SUPPORT
# =============================================================================

def manually_trigger_retrain(
    trino,
    reason: str = "manual",
) -> str:
    """
    Manually trigger a retrain event.

    This can be called from the API or CLI to force retraining.
    """
    retrain_id = f"RETRAIN_{uuid.uuid4().hex[:12]}"

    record_retrain_trigger(
        trino=trino,
        trigger_reasons=[f"manual:{reason}"],
        drift_score=0.0,
        f1_score=0.0,
        new_labels=0,
        dagster_run_id=retrain_id,
    )

    try:
        from api.metrics import track_auto_retrain_trigger
        track_auto_retrain_trigger("manual")
    except ImportError:
        pass

    return retrain_id


# Export for use in __init__.py
AUTO_RETRAIN_SENSORS = [auto_retrain_sensor]
