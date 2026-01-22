# pipelines/drift_monitoring.py
"""
Comprehensive drift monitoring using Evidently.
Detects feature drift, target drift, and data quality issues.

This module queries REAL data from Trino/Iceberg tables for drift detection.
Falls back to synthetic data only if tables are unavailable.
"""

import os
import uuid
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue
# Evidently 0.7+ API
from evidently import Report, Dataset
from evidently.presets import DataDriftPreset, DataSummaryPreset
import pandas as pd
import json
from datetime import datetime
from pathlib import Path
from src.core.monitoring import (
    feature_drift_gauge,
    dataset_drift_gauge,
    prediction_drift_gauge,
    log_data_quality
)
from src.core.resources import MinIOResource, TrinoResource
from src.core.config import TRINO_CATALOG
import mlflow

# ============================================================================
# CONFIGURATION
# ============================================================================

DRIFT_REPORTS_PATH = os.getenv("DRIFT_REPORTS_PATH", "/tmp/drift_reports")
MODEL_NAME = "fraud_detector"

# Feature columns to monitor for drift (must match gold table schema)
DRIFT_FEATURES = [
    "amount",
    "account_age_days",      # customer tenure
    "tx_hour",               # transaction hour
    "tx_dayofweek",          # day of week
    "quantity",
    "transactions_before",
    "avg_amount_before",
]

# Target column in gold table
TARGET_COLUMN = "final_label"

# Timestamp column for filtering
TIMESTAMP_COLUMN = "event_timestamp"

# Tables
GOLD_TABLE = os.getenv("GOLD_TABLE", f"{TRINO_CATALOG}.gold.fraud_training_data")
DRIFT_REPORTS_TABLE = os.getenv("DRIFT_REPORTS_TABLE", f"{TRINO_CATALOG}.monitoring.drift_reports")

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _get_trino_resource() -> TrinoResource:
    """Get a Trino resource instance."""
    return TrinoResource()


def load_reference_data(trino: TrinoResource = None, days_back: int = 60, days_window: int = 30) -> pd.DataFrame:
    """
    Load reference data from Trino.

    Reference data is historical data from (days_back to days_back-days_window).
    For example: data from 60-30 days ago serves as reference.

    Falls back to synthetic data if Trino query fails.
    """
    if trino is None:
        trino = _get_trino_resource()

    try:
        feature_cols = ", ".join(DRIFT_FEATURES)
        query = f"""
        SELECT {feature_cols}, {TARGET_COLUMN} as is_fraud
        FROM {GOLD_TABLE}
        WHERE {TIMESTAMP_COLUMN} >= DATE_ADD('day', -{days_back}, CURRENT_DATE)
          AND {TIMESTAMP_COLUMN} < DATE_ADD('day', -{days_back - days_window}, CURRENT_DATE)
        LIMIT 10000
        """
        df = trino.execute_query_as_dataframe(query)

        if df is not None and len(df) >= 100:
            print(f"[DriftMonitoring] Loaded {len(df)} reference rows from Trino")
            return df
        else:
            print(f"[DriftMonitoring] Not enough reference data ({len(df) if df is not None else 0} rows), using synthetic")
            return generate_synthetic_reference_data()

    except Exception as e:
        print(f"[DriftMonitoring] Failed to load reference data from Trino: {e}")
        return generate_synthetic_reference_data()


def generate_synthetic_reference_data() -> pd.DataFrame:
    """Generate synthetic reference data for demo/fallback."""
    import numpy as np

    np.random.seed(42)
    n_samples = 5000

    df = pd.DataFrame({
        'amount': np.random.lognormal(5, 2, n_samples),
        'account_age_days': np.random.randint(1, 1000, n_samples),
        'tx_hour': np.random.randint(0, 24, n_samples),
        'tx_dayofweek': np.random.randint(0, 7, n_samples),
        'quantity': np.random.randint(1, 10, n_samples),
        'transactions_before': np.random.randint(0, 100, n_samples),
        'avg_amount_before': np.random.lognormal(4, 1.5, n_samples),
        'is_fraud': np.random.choice([0, 1], n_samples, p=[0.95, 0.05])
    })

    print(f"[DriftMonitoring] Generated {len(df)} synthetic reference rows")
    return df


def get_recent_data(trino: TrinoResource = None, days_back: int = 30, min_rows: int = 50) -> pd.DataFrame:
    """
    Get recent data from Trino for drift comparison.

    Current data is the most recent data. If not enough recent data,
    expands the window to get sufficient samples.

    Falls back to synthetic data if Trino query fails.
    """
    if trino is None:
        trino = _get_trino_resource()

    try:
        feature_cols = ", ".join(DRIFT_FEATURES)

        # First try the specified window
        query = f"""
        SELECT {feature_cols}, {TARGET_COLUMN} as is_fraud
        FROM {GOLD_TABLE}
        WHERE {TIMESTAMP_COLUMN} >= DATE_ADD('day', -{days_back}, CURRENT_DATE)
        LIMIT 5000
        """
        df = trino.execute_query_as_dataframe(query)

        # If not enough data, get the most recent N rows instead
        if df is None or len(df) < min_rows:
            print(f"[DriftMonitoring] Only {len(df) if df is not None else 0} rows in last {days_back} days, fetching most recent {min_rows * 10} rows")
            query = f"""
            SELECT {feature_cols}, {TARGET_COLUMN} as is_fraud
            FROM {GOLD_TABLE}
            ORDER BY {TIMESTAMP_COLUMN} DESC
            LIMIT {min_rows * 10}
            """
            df = trino.execute_query_as_dataframe(query)

        if df is not None and len(df) >= min_rows:
            print(f"[DriftMonitoring] Loaded {len(df)} current rows from Trino")
            return df
        else:
            print(f"[DriftMonitoring] Not enough current data ({len(df) if df is not None else 0} rows), using synthetic")
            return generate_synthetic_current_data()

    except Exception as e:
        print(f"[DriftMonitoring] Failed to load current data from Trino: {e}")
        return generate_synthetic_current_data()


def generate_synthetic_current_data() -> pd.DataFrame:
    """Generate synthetic current data (with potential drift) for demo/fallback."""
    import numpy as np

    np.random.seed(int(datetime.now().timestamp()) % 1000000)
    n_samples = 1000

    # Simulate some drift
    drift_factor = 1.2  # 20% increase in transaction amounts

    df = pd.DataFrame({
        'amount': np.random.lognormal(5, 2, n_samples) * drift_factor,  # DRIFT!
        'account_age_days': np.random.randint(1, 1000, n_samples),
        'tx_hour': np.random.randint(0, 24, n_samples),
        'tx_dayofweek': np.random.randint(0, 7, n_samples),
        'quantity': np.random.randint(1, 10, n_samples),
        'transactions_before': np.random.randint(0, 100, n_samples),
        'avg_amount_before': np.random.lognormal(4, 1.5, n_samples) * 1.1,  # Slight drift
        'is_fraud': np.random.choice([0, 1], n_samples, p=[0.93, 0.07])  # Slight drift
    })

    print(f"[DriftMonitoring] Generated {len(df)} synthetic current rows")
    return df


def store_drift_report(
    trino: TrinoResource,
    drift_share: float,
    n_drifted: int,
    target_drift_score: float,
    drifted_features: list,
    severity: str,
) -> str:
    """Store drift report in Iceberg monitoring table."""
    report_id = f"DRIFT_{uuid.uuid4().hex[:12]}"
    now = datetime.now()

    try:
        # Check if table exists
        if not trino.table_exists(DRIFT_REPORTS_TABLE):
            print(f"[DriftMonitoring] Creating drift reports table: {DRIFT_REPORTS_TABLE}")
            trino.execute_ddl(f"""
                CREATE TABLE IF NOT EXISTS {DRIFT_REPORTS_TABLE} (
                    report_id VARCHAR,
                    report_timestamp TIMESTAMP,
                    drift_share DOUBLE,
                    n_drifted_features INTEGER,
                    target_drift_score DOUBLE,
                    drifted_features VARCHAR,
                    severity VARCHAR,
                    model_name VARCHAR,
                    partition_date DATE
                ) WITH (
                    format = 'PARQUET',
                    partitioning = ARRAY['partition_date']
                )
            """)

        # Insert report with SQL injection prevention
        from src.core.config import escape_sql_string
        drifted_str = ",".join(drifted_features) if drifted_features else ""
        drifted_str_safe = escape_sql_string(drifted_str)
        severity_safe = escape_sql_string(severity)
        model_name_safe = escape_sql_string(MODEL_NAME)
        trino.execute_ddl(f"""
            INSERT INTO {DRIFT_REPORTS_TABLE} VALUES (
                '{report_id}',
                TIMESTAMP '{now.strftime('%Y-%m-%d %H:%M:%S')}',
                {drift_share},
                {n_drifted},
                {target_drift_score},
                '{drifted_str_safe}',
                '{severity_safe}',
                '{model_name_safe}',
                DATE '{now.strftime('%Y-%m-%d')}'
            )
        """)
        print(f"[DriftMonitoring] Stored drift report: {report_id}")

    except Exception as e:
        print(f"[DriftMonitoring] Failed to store drift report: {e}")

    return report_id


# ============================================================================
# DRIFT MONITORING ASSETS
# ============================================================================

@asset(
    group_name="monitoring",
    description="Comprehensive drift monitoring with Evidently (queries real Trino data)"
)
def drift_monitoring_report(context: AssetExecutionContext) -> MaterializeResult:
    """
    Generate comprehensive drift report using Evidently.
    Checks feature drift, target drift, and data quality.

    Queries real data from Trino gold table, falls back to synthetic if unavailable.
    """
    # Get Trino resource
    trino = _get_trino_resource()

    # Load data (tries Trino first, falls back to synthetic)
    reference_data = load_reference_data(trino)
    current_data = get_recent_data(trino)

    context.log.info(f"Reference data: {len(reference_data)} rows")
    context.log.info(f"Current data: {len(current_data)} rows")

    # Convert to Evidently Dataset format (0.7+ API)
    ref_dataset = Dataset.from_pandas(reference_data)
    cur_dataset = Dataset.from_pandas(current_data)

    # Create report with DataDriftPreset only (TargetDriftPreset removed in 0.7+)
    report = Report(metrics=[DataDriftPreset()])

    # Run report and get snapshot
    snapshot = report.run(reference_data=ref_dataset, current_data=cur_dataset)

    # Extract metrics from snapshot (0.7+ API)
    drift_share = 0.0
    n_drifted = 0
    drifted_features = []
    target_drift_score = 0.0

    for key, value in snapshot.metric_results.items():
        d = value.dict()
        name = d.get('display_name', '')

        if 'Count of Drifted' in name:
            # Extract nested values from the new structure
            count_info = d.get('count', {})
            share_info = d.get('share', {})
            if isinstance(count_info, dict):
                n_drifted = int(count_info.get('value', 0))
            if isinstance(share_info, dict):
                drift_share = float(share_info.get('value', 0.0))

        elif 'drift for' in name.lower():
            # Per-feature drift p-value
            feature_name = name.replace('Value drift for ', '').replace('Drift for ', '')
            p_value = d.get('value', 1.0)
            # p-value < 0.05 indicates drift
            if p_value < 0.05:
                drifted_features.append(feature_name)
                feature_drift_gauge.labels(feature=feature_name).set(p_value)
            # Track target drift separately
            if feature_name == 'is_fraud':
                target_drift_score = 1.0 - p_value  # Convert p-value to drift score

    # Determine if dataset has drift
    dataset_drift = drift_share > 0.3

    # Export metrics to Prometheus
    dataset_drift_gauge.labels(model_name=MODEL_NAME).set(1 if dataset_drift else 0)
    prediction_drift_gauge.labels(model_name=MODEL_NAME).set(target_drift_score)

    # Save HTML report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = Path(DRIFT_REPORTS_PATH) / f"drift_report_{timestamp}.html"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot.save_html(str(report_path))

    # Determine drift severity
    if drift_share > 0.5:
        severity = "CRITICAL"
        message = f"CRITICAL: {drift_share:.1%} of features drifted! Immediate action required."
    elif drift_share > 0.3:
        severity = "WARNING"
        message = f"WARNING: {drift_share:.1%} of features drifted. Investigation recommended."
    elif drift_share > 0.15:
        severity = "INFO"
        message = f"INFO: {drift_share:.1%} of features drifted. Monitor closely."
    else:
        severity = "OK"
        message = f"OK: Only {drift_share:.1%} of features drifted."

    # Store drift report in Iceberg
    report_id = store_drift_report(
        trino=trino,
        drift_share=drift_share,
        n_drifted=n_drifted,
        target_drift_score=target_drift_score,
        drifted_features=drifted_features,
        severity=severity,
    )

    # Log to MLflow
    try:
        with mlflow.start_run(run_name=f"drift_monitoring_{timestamp}"):
            mlflow.log_params({
                "reference_size": len(reference_data),
                "current_size": len(current_data),
                "model_name": MODEL_NAME,
                "report_id": report_id,
            })
            mlflow.log_metrics({
                "dataset_drift": 1 if dataset_drift else 0,
                "drift_share": drift_share,
                "n_drifted_features": n_drifted,
                "target_drift_score": target_drift_score
            })
            mlflow.log_artifact(str(report_path))
    except Exception as e:
        context.log.warning(f"Failed to log to MLflow: {e}")

    context.log.info(f"Drift monitoring complete. Report saved to {report_path}")
    context.log.info(f"Dataset drift: {dataset_drift}")
    context.log.info(f"Drift share: {drift_share:.1%}")
    context.log.info(f"Drifted features: {drifted_features}")

    return MaterializeResult(
        metadata={
            "report_id": report_id,
            "dataset_drift": dataset_drift,
            "drift_share": drift_share,
            "n_drifted_features": n_drifted,
            "drifted_features": MetadataValue.text(", ".join(drifted_features) if drifted_features else "None"),
            "target_drift_score": target_drift_score,
            "severity": severity,
            "message": MetadataValue.md(message),
            "report_path": str(report_path),
            "reference_size": len(reference_data),
            "current_size": len(current_data)
        }
    )


@asset(
    group_name="monitoring",
    description="Per-feature drift analysis"
)
def feature_drift_analysis(
    context: AssetExecutionContext,
    drift_monitoring_report: MaterializeResult
) -> MaterializeResult:
    """
    Detailed per-feature drift analysis.
    Identifies which features are drifting and by how much.
    Uses Evidently 0.7+ API with DataDriftPreset.
    """
    trino = _get_trino_resource()

    reference_data = load_reference_data(trino)
    current_data = get_recent_data(trino)

    feature_columns = [col for col in reference_data.columns if col != 'is_fraud']

    # Convert to Evidently Dataset format
    ref_dataset = Dataset.from_pandas(reference_data)
    cur_dataset = Dataset.from_pandas(current_data)

    # Run DataDriftPreset to get per-feature drift
    report = Report(metrics=[DataDriftPreset()])
    snapshot = report.run(reference_data=ref_dataset, current_data=cur_dataset)

    # Extract per-feature drift from snapshot
    drift_results = {}
    for key, value in snapshot.metric_results.items():
        d = value.dict()
        name = d.get('display_name', '')

        if 'drift for' in name.lower():
            # Per-feature drift p-value
            feature_name = name.replace('Value drift for ', '').replace('Drift for ', '')
            p_value = d.get('value', 1.0)

            drift_results[feature_name] = {
                'drift_detected': p_value < 0.05,
                'drift_score': 1.0 - p_value,  # Convert p-value to drift score
                'p_value': p_value,
                'stattest_name': 'ks_test',  # Default in Evidently
            }

            # Log to Prometheus
            feature_drift_gauge.labels(feature=feature_name).set(1.0 - p_value)

    # Sort by drift score (descending)
    sorted_features = sorted(
        drift_results.items(),
        key=lambda x: x[1]['drift_score'],
        reverse=True
    )

    # Create summary table
    summary_lines = ["| Feature | Drift Score | P-Value | Status |",
                     "|---------|-------------|---------|--------|"]

    for feature, result in sorted_features:
        status = "DRIFT" if result['drift_detected'] else "OK"
        summary_lines.append(
            f"| {feature} | {result['drift_score']:.3f} | {result['p_value']:.4f} | {status} |"
        )

    summary_md = "\n".join(summary_lines)

    context.log.info(f"Feature drift analysis complete for {len(drift_results)} features")

    return MaterializeResult(
        metadata={
            "n_features": len(drift_results),
            "n_drifted": sum(1 for r in drift_results.values() if r['drift_detected']),
            "drift_summary": MetadataValue.md(summary_md),
            "top_drifted_feature": sorted_features[0][0] if sorted_features else None,
            "top_drift_score": sorted_features[0][1]['drift_score'] if sorted_features else 0.0,
        }
    )


@asset(
    group_name="monitoring",
    description="Data quality monitoring"
)
def data_quality_monitoring(context: AssetExecutionContext) -> MaterializeResult:
    """
    Monitor data quality: missing values, duplicates, out-of-range values.
    """
    trino = _get_trino_resource()
    current_data = get_recent_data(trino)

    # Check missing values
    missing_counts = current_data.isnull().sum()
    missing_percentages = (missing_counts / len(current_data) * 100).to_dict()

    # Check duplicates
    n_duplicates = current_data.duplicated().sum()
    duplicate_rate = n_duplicates / len(current_data) * 100

    # Check value ranges (example: amount should be positive)
    if 'amount' in current_data.columns:
        negative_amounts = (current_data['amount'] < 0).sum()
        negative_rate = negative_amounts / len(current_data) * 100
    else:
        negative_amounts = 0
        negative_rate = 0.0

    # Log quality metrics
    for feature, null_rate in missing_percentages.items():
        log_data_quality(
            layer="current",
            check_name=f"{feature}_null_check",
            passed=(null_rate < 5.0)  # Fail if >5% nulls
        )

    log_data_quality(
        layer="current",
        check_name="duplicates_check",
        passed=(duplicate_rate < 1.0)  # Fail if >1% duplicates
    )

    log_data_quality(
        layer="current",
        check_name="amount_range",
        passed=(negative_rate == 0.0)  # Fail if any negative
    )

    # Determine overall quality status
    quality_issues = []

    if any(rate > 5.0 for rate in missing_percentages.values()):
        quality_issues.append("High missing value rate")

    if duplicate_rate > 1.0:
        quality_issues.append(f"{duplicate_rate:.1f}% duplicates")

    if negative_rate > 0:
        quality_issues.append(f"{negative_rate:.1f}% negative amounts")

    if quality_issues:
        status = "FAILED"
        message = f"Quality issues detected: {', '.join(quality_issues)}"
    else:
        status = "PASSED"
        message = "All data quality checks passed"

    context.log.info(f"Data quality monitoring: {status}")

    return MaterializeResult(
        metadata={
            "n_rows": len(current_data),
            "n_duplicates": n_duplicates,
            "duplicate_rate": f"{duplicate_rate:.2f}%",
            "missing_values": MetadataValue.json(missing_percentages),
            "negative_amounts": negative_amounts,
            "negative_rate": f"{negative_rate:.2f}%",
            "quality_status": status,
            "message": MetadataValue.md(message),
            "quality_issues": quality_issues if quality_issues else []
        }
    )


# ============================================================================
# MONITORING SUMMARY ASSET
# ============================================================================

@asset(
    group_name="monitoring",
    description="Overall monitoring summary dashboard"
)
def monitoring_summary(
    context: AssetExecutionContext,
    drift_monitoring_report: MaterializeResult,
    feature_drift_analysis: MaterializeResult,
    data_quality_monitoring: MaterializeResult
) -> MaterializeResult:
    """
    Aggregate monitoring summary combining drift and quality checks.
    """

    # Extract metrics from upstream assets
    drift_metadata = drift_monitoring_report.metadata
    feature_metadata = feature_drift_analysis.metadata
    quality_metadata = data_quality_monitoring.metadata

    # Create summary dashboard
    summary = f"""
## Monitoring Summary

**Generated**: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

### Drift Detection
- **Dataset Drift**: {"DETECTED" if drift_metadata.get("dataset_drift") else "OK"}
- **Drift Share**: {drift_metadata.get("drift_share", 0):.1%}
- **Drifted Features**: {drift_metadata.get("n_drifted_features", 0)} / {feature_metadata.get("n_features", 0)}
- **Severity**: {drift_metadata.get("severity", "UNKNOWN")}

### Data Quality
- **Status**: {quality_metadata.get("quality_status", "UNKNOWN")}
- **Rows Analyzed**: {quality_metadata.get("n_rows", 0):,}
- **Duplicates**: {quality_metadata.get("duplicate_rate", "0%")}
- **Missing Values**: Detected in {len(quality_metadata.get("missing_values", {}))} features

### Top Issues
"""

    # Add top drifted features
    if drift_metadata.get("drifted_features"):
        summary += f"- Drifted Features: {drift_metadata.get('drifted_features')}\n"

    # Add quality issues
    if quality_metadata.get("quality_issues"):
        for issue in quality_metadata.get("quality_issues", []):
            summary += f"- {issue}\n"

    # Overall health score (0-100)
    drift_penalty = drift_metadata.get("drift_share", 0) * 50
    quality_penalty = 25 if quality_metadata.get("quality_status") == "FAILED" else 0
    health_score = max(0, 100 - drift_penalty - quality_penalty)

    summary += f"\n### Overall Health Score: {health_score:.0f}/100\n"

    if health_score >= 90:
        summary += "**EXCELLENT** - No issues detected"
    elif health_score >= 70:
        summary += "**GOOD** - Minor issues, monitor closely"
    elif health_score >= 50:
        summary += "**WARNING** - Moderate issues, investigation recommended"
    else:
        summary += "**CRITICAL** - Severe issues, immediate action required"

    context.log.info(f"Monitoring summary generated. Health score: {health_score:.0f}/100")

    return MaterializeResult(
        metadata={
            "health_score": health_score,
            "drift_status": "DETECTED" if drift_metadata.get("dataset_drift") else "OK",
            "quality_status": quality_metadata.get("quality_status", "UNKNOWN"),
            "summary": MetadataValue.md(summary),
            "timestamp": datetime.now().isoformat()
        }
    )
