"""
Performance Monitoring Pipeline
═══════════════════════════════════════════════════════════════════════════════

Dagster assets for computing and tracking model performance metrics.
Compares predictions to actual outcomes and stores results in Iceberg.

Assets:
    - performance_compute: Compute rolling performance metrics
    - performance_store: Store metrics to Iceberg
    - performance_alert_check: Check for performance degradation

Jobs:
    - performance_monitoring_job: Run all performance monitoring assets
"""

import os
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from dagster import (
    asset,
    AssetIn,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    define_asset_job,
    AssetSelection,
    Config,
)

from src.core.config import TRINO_CATALOG
from src.core.resources import TrinoResource


class PerformanceMonitoringConfig(Config):
    """Configuration for performance monitoring."""
    windows: List[str] = ["1h", "24h", "7d"]
    f1_threshold: float = 0.7
    accuracy_threshold: float = 0.8
    min_samples: int = 50


# =============================================================================
# ICEBERG TABLE SCHEMAS
# =============================================================================

PERFORMANCE_METRICS_TABLE = os.getenv(
    "PERFORMANCE_METRICS_TABLE",
    f"{TRINO_CATALOG}.monitoring.performance_metrics"
)

RETRAINING_HISTORY_TABLE = os.getenv(
    "RETRAINING_HISTORY_TABLE",
    f"{TRINO_CATALOG}.monitoring.retraining_history"
)

PERFORMANCE_METRICS_SCHEMA = """
CREATE TABLE IF NOT EXISTS {table} (
    metric_id VARCHAR,
    evaluation_timestamp TIMESTAMP(6),
    model_stage VARCHAR,
    model_version VARCHAR,
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
    total_predictions INTEGER,
    predictions_with_outcomes INTEGER,
    alert_triggered BOOLEAN,
    alert_type VARCHAR,
    evaluation_date DATE
) WITH (
    format = 'PARQUET',
    partitioning = ARRAY['evaluation_date']
)
"""

RETRAINING_HISTORY_SCHEMA = """
CREATE TABLE IF NOT EXISTS {table} (
    retrain_id VARCHAR,
    triggered_at TIMESTAMP(6),
    trigger_reasons VARCHAR,
    drift_score DOUBLE,
    performance_f1 DOUBLE,
    new_labels_count INTEGER,
    dagster_run_id VARCHAR,
    status VARCHAR,
    model_stage VARCHAR,
    completion_timestamp TIMESTAMP(6),
    trigger_date DATE
) WITH (
    format = 'PARQUET',
    partitioning = ARRAY['trigger_date']
)
"""


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def ensure_performance_tables(trino) -> bool:
    """Ensure performance monitoring tables exist."""
    try:
        # Create schema if needed
        trino.execute(f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.monitoring")

        # Create performance metrics table
        trino.execute(PERFORMANCE_METRICS_SCHEMA.format(table=PERFORMANCE_METRICS_TABLE))

        # Create retraining history table
        trino.execute(RETRAINING_HISTORY_SCHEMA.format(table=RETRAINING_HISTORY_TABLE))

        return True
    except Exception as e:
        print(f"[PerformanceMonitoring] Table creation error: {e}")
        return False


def calculate_metrics_from_streaming_results(
    trino,
    model_stage: str,
    window_hours: int,
) -> Optional[Dict[str, Any]]:
    """
    Calculate performance metrics from streaming evaluation results.

    Queries the streaming_results table for predictions with actual labels.
    """
    try:
        from src.core.config import escape_sql_string
        # Escape model_stage to prevent SQL injection
        model_stage_safe = escape_sql_string(model_stage)
        # Query streaming results with actual labels
        query = f"""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN is_correct = 1 THEN 1 ELSE 0 END) as correct,
            SUM(CASE WHEN fraud_prediction = 1 AND actual_label = 1 THEN 1 ELSE 0 END) as tp,
            SUM(CASE WHEN fraud_prediction = 1 AND actual_label = 0 THEN 1 ELSE 0 END) as fp,
            SUM(CASE WHEN fraud_prediction = 0 AND actual_label = 0 THEN 1 ELSE 0 END) as tn,
            SUM(CASE WHEN fraud_prediction = 0 AND actual_label = 1 THEN 1 ELSE 0 END) as fn,
            AVG(fraud_probability) as avg_score
        FROM {TRINO_CATALOG}.evaluation.streaming_results
        WHERE model_stage = '{model_stage_safe}'
          AND actual_label IS NOT NULL
          AND result_timestamp >= CURRENT_TIMESTAMP - INTERVAL '{window_hours}' HOUR
        """

        result = trino.execute(query)
        if not result:
            return None

        row = result[0]
        total = row[0] or 0
        correct = row[1] or 0
        tp = row[2] or 0
        fp = row[3] or 0
        tn = row[4] or 0
        fn = row[5] or 0

        if total == 0:
            return None

        # Calculate metrics
        accuracy = (tp + tn) / total if total > 0 else 0
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

        return {
            "total_predictions": total,
            "predictions_with_outcomes": total,
            "accuracy": accuracy,
            "precision": precision,
            "recall": recall,
            "f1": f1,
            "auc_roc": None,  # Would need scores for AUC
            "tp": tp,
            "fp": fp,
            "tn": tn,
            "fn": fn,
        }

    except Exception as e:
        print(f"[PerformanceMonitoring] Query error: {e}")
        return None


def calculate_metrics_from_batch_results(
    trino,
    model_stage: str,
) -> Optional[Dict[str, Any]]:
    """
    Get latest batch evaluation metrics for a model stage.
    """
    try:
        from src.core.config import escape_sql_string
        model_stage_safe = escape_sql_string(model_stage)
        query = f"""
        SELECT
            model_version,
            accuracy,
            precision_score,
            recall,
            f1_score,
            auc_roc,
            total_samples
        FROM {TRINO_CATALOG}.evaluation.batch_results
        WHERE model_stage = '{model_stage_safe}'
        ORDER BY evaluation_timestamp DESC
        LIMIT 1
        """

        result = trino.execute(query)
        if not result:
            return None

        row = result[0]
        return {
            "model_version": row[0],
            "accuracy": row[1] or 0,
            "precision": row[2] or 0,
            "recall": row[3] or 0,
            "f1": row[4] or 0,
            "auc_roc": row[5],
            "total_predictions": row[6] or 0,
        }

    except Exception as e:
        print(f"[PerformanceMonitoring] Batch query error: {e}")
        return None


# =============================================================================
# DAGSTER ASSETS
# =============================================================================

@asset(
    group_name="performance_monitoring",
    description="Compute rolling performance metrics from streaming and batch results",
)
def performance_compute(
    context: AssetExecutionContext,
    trino: TrinoResource,
    config: PerformanceMonitoringConfig,
) -> MaterializeResult:
    """
    Compute performance metrics for all model stages and time windows.

    Combines data from:
    - Streaming evaluation results (real-time predictions with outcomes)
    - Batch evaluation results (scheduled evaluations)
    """
    # Ensure tables exist
    ensure_performance_tables(trino)

    metrics_computed = []
    alerts_triggered = []

    model_stages = ["Production", "Staging"]
    window_map = {"1h": 1, "24h": 24, "7d": 168}

    for stage in model_stages:
        for window in config.windows:
            window_hours = window_map.get(window, 24)

            context.log.info(f"Computing metrics for {stage} / {window}")

            # Try streaming results first
            metrics = calculate_metrics_from_streaming_results(trino, stage, window_hours)

            if metrics is None or metrics.get("total_predictions", 0) < config.min_samples:
                # Fall back to batch results for longer windows
                if window in ["24h", "7d"]:
                    batch_metrics = calculate_metrics_from_batch_results(trino, stage)
                    if batch_metrics:
                        metrics = batch_metrics
                        context.log.info(f"Using batch metrics for {stage}")

            if metrics is None:
                context.log.info(f"No data available for {stage} / {window}")
                continue

            # Check alerts
            alert_triggered = False
            alert_type = None

            if metrics.get("total_predictions", 0) >= config.min_samples:
                if metrics.get("f1", 1.0) < config.f1_threshold:
                    alert_triggered = True
                    alert_type = "f1_low"
                    alerts_triggered.append({
                        "stage": stage,
                        "window": window,
                        "type": alert_type,
                        "f1": metrics.get("f1"),
                    })
                elif metrics.get("accuracy", 1.0) < config.accuracy_threshold:
                    alert_triggered = True
                    alert_type = "accuracy_low"
                    alerts_triggered.append({
                        "stage": stage,
                        "window": window,
                        "type": alert_type,
                        "accuracy": metrics.get("accuracy"),
                    })

            # Store metrics
            metric_id = f"PERF_{uuid.uuid4().hex[:12]}"
            now = datetime.now()

            def esc(s):
                if s is None:
                    return "NULL"
                return f"'{str(s)}'"

            insert_sql = f"""
            INSERT INTO {PERFORMANCE_METRICS_TABLE} VALUES (
                {esc(metric_id)},
                TIMESTAMP '{now.strftime('%Y-%m-%d %H:%M:%S')}',
                {esc(stage)},
                {esc(metrics.get('model_version', 'unknown'))},
                {window_hours},
                {metrics.get('accuracy', 0)},
                {metrics.get('precision', 0)},
                {metrics.get('recall', 0)},
                {metrics.get('f1', 0)},
                {metrics.get('auc_roc') or 'NULL'},
                {metrics.get('tp', 0)},
                {metrics.get('fp', 0)},
                {metrics.get('tn', 0)},
                {metrics.get('fn', 0)},
                {metrics.get('total_predictions', 0)},
                {metrics.get('predictions_with_outcomes', 0)},
                {str(alert_triggered).lower()},
                {esc(alert_type)},
                DATE '{now.strftime('%Y-%m-%d')}'
            )
            """

            try:
                trino.execute(insert_sql)
                metrics_computed.append({
                    "stage": stage,
                    "window": window,
                    "f1": round(metrics.get("f1", 0), 4),
                    "accuracy": round(metrics.get("accuracy", 0), 4),
                    "samples": metrics.get("total_predictions", 0),
                })
            except Exception as e:
                context.log.warning(f"Failed to store metrics: {e}")

    # Log alerts
    for alert in alerts_triggered:
        context.log.warning(f"ALERT: {alert}")

    return MaterializeResult(
        metadata={
            "metrics_computed": MetadataValue.int(len(metrics_computed)),
            "alerts_triggered": MetadataValue.int(len(alerts_triggered)),
            "stages": MetadataValue.json(model_stages),
            "windows": MetadataValue.json(config.windows),
            "metrics_summary": MetadataValue.json(metrics_computed[:10]),
            "alerts": MetadataValue.json(alerts_triggered),
        }
    )


@asset(
    group_name="performance_monitoring",
    deps=["performance_compute"],
    description="Check for performance degradation and update Prometheus metrics",
)
def performance_alert_check(
    context: AssetExecutionContext,
    trino: TrinoResource,
    config: PerformanceMonitoringConfig,
) -> MaterializeResult:
    """
    Check latest performance metrics and update alert status.

    Updates Prometheus metrics for dashboard visibility.
    """
    try:
        from api.metrics import (
            update_performance_metrics,
            set_performance_alert,
            PROMETHEUS_ENABLED,
        )
    except ImportError:
        context.log.warning("Prometheus metrics not available")
        return MaterializeResult(metadata={"status": "skipped"})

    if not PROMETHEUS_ENABLED:
        return MaterializeResult(metadata={"status": "prometheus_disabled"})

    alerts_active = []

    from src.core.config import escape_sql_string
    # Get latest metrics for each stage/window
    for stage in ["Production", "Staging"]:
        stage_safe = escape_sql_string(stage)
        for window in config.windows:
            window_hours = {"1h": 1, "24h": 24, "7d": 168}.get(window, 24)

            query = f"""
            SELECT
                accuracy, precision_score, recall, f1_score, auc_roc,
                true_positives, false_positives, true_negatives, false_negatives,
                alert_triggered, alert_type
            FROM {PERFORMANCE_METRICS_TABLE}
            WHERE model_stage = '{stage_safe}'
              AND window_hours = {window_hours}
            ORDER BY evaluation_timestamp DESC
            LIMIT 1
            """

            try:
                result = trino.execute(query)
                if not result:
                    continue

                row = result[0]
                accuracy = row[0] or 0
                precision = row[1] or 0
                recall = row[2] or 0
                f1 = row[3] or 0
                auc_roc = row[4]
                tp = row[5] or 0
                fp = row[6] or 0
                tn = row[7] or 0
                fn = row[8] or 0
                alert_triggered = row[9]
                alert_type = row[10]

                # Update Prometheus metrics
                update_performance_metrics(
                    model_stage=stage,
                    window=window,
                    accuracy=accuracy,
                    precision=precision,
                    recall=recall,
                    f1=f1,
                    auc_roc=auc_roc,
                    tp=tp,
                    fp=fp,
                    tn=tn,
                    fn=fn,
                )

                # Set alert status
                set_performance_alert(stage, "f1_low", alert_type == "f1_low")
                set_performance_alert(stage, "accuracy_low", alert_type == "accuracy_low")

                if alert_triggered:
                    alerts_active.append({
                        "stage": stage,
                        "window": window,
                        "type": alert_type,
                        "f1": f1,
                        "accuracy": accuracy,
                    })

                context.log.info(f"Updated metrics for {stage}/{window}: F1={f1:.4f}, Acc={accuracy:.4f}")

            except Exception as e:
                context.log.warning(f"Failed to query metrics for {stage}/{window}: {e}")

    return MaterializeResult(
        metadata={
            "alerts_active": MetadataValue.int(len(alerts_active)),
            "alerts_details": MetadataValue.json(alerts_active),
            "status": "success",
        }
    )


@asset(
    group_name="performance_monitoring",
    description="Get performance degradation summary for retrain decisions",
)
def performance_degradation_check(
    context: AssetExecutionContext,
    trino: TrinoResource,
    config: PerformanceMonitoringConfig,
) -> MaterializeResult:
    """
    Check for significant performance degradation that might trigger retraining.

    Returns summary of performance trends and degradation indicators.
    """
    degradation_detected = False
    degradation_details = {}

    for stage in ["Production", "Staging"]:
        # Compare 24h to 7d metrics
        query = f"""
        WITH recent AS (
            SELECT f1_score, accuracy
            FROM {PERFORMANCE_METRICS_TABLE}
            WHERE model_stage = '{stage}'
              AND window_hours = 24
            ORDER BY evaluation_timestamp DESC
            LIMIT 1
        ),
        baseline AS (
            SELECT AVG(f1_score) as avg_f1, AVG(accuracy) as avg_accuracy
            FROM {PERFORMANCE_METRICS_TABLE}
            WHERE model_stage = '{stage}'
              AND window_hours = 168
              AND evaluation_timestamp >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
        )
        SELECT
            r.f1_score as recent_f1,
            r.accuracy as recent_accuracy,
            b.avg_f1 as baseline_f1,
            b.avg_accuracy as baseline_accuracy
        FROM recent r, baseline b
        """

        try:
            result = trino.execute(query)
            if not result:
                continue

            row = result[0]
            recent_f1 = row[0] or 0
            recent_accuracy = row[1] or 0
            baseline_f1 = row[2] or recent_f1
            baseline_accuracy = row[3] or recent_accuracy

            # Check for degradation (>10% drop)
            f1_drop = (baseline_f1 - recent_f1) / baseline_f1 if baseline_f1 > 0 else 0
            acc_drop = (baseline_accuracy - recent_accuracy) / baseline_accuracy if baseline_accuracy > 0 else 0

            if f1_drop > 0.1 or acc_drop > 0.1:
                degradation_detected = True
                degradation_details[stage] = {
                    "recent_f1": recent_f1,
                    "baseline_f1": baseline_f1,
                    "f1_drop_pct": round(f1_drop * 100, 2),
                    "recent_accuracy": recent_accuracy,
                    "baseline_accuracy": baseline_accuracy,
                    "accuracy_drop_pct": round(acc_drop * 100, 2),
                }
                context.log.warning(f"Performance degradation detected for {stage}: F1 drop {f1_drop*100:.1f}%")

        except Exception as e:
            context.log.warning(f"Failed to check degradation for {stage}: {e}")

    return MaterializeResult(
        metadata={
            "degradation_detected": MetadataValue.bool(degradation_detected),
            "degradation_details": MetadataValue.json(degradation_details),
            "threshold_f1": MetadataValue.float(config.f1_threshold),
            "threshold_accuracy": MetadataValue.float(config.accuracy_threshold),
        }
    )


# =============================================================================
# JOBS
# =============================================================================

PERFORMANCE_MONITORING_ASSETS = [
    performance_compute,
    performance_alert_check,
    performance_degradation_check,
]

performance_monitoring_job = define_asset_job(
    name="performance_monitoring_job",
    selection=AssetSelection.assets(*PERFORMANCE_MONITORING_ASSETS),
    description="Compute and check model performance metrics",
)
