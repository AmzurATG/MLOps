"""
Model Evaluation Pipeline - Batch and Streaming Evaluation
═══════════════════════════════════════════════════════════════════════════════

ARCHITECTURE:
1. Evaluation data (PRIMITIVES ONLY) inserted into evaluation.requests
2. API /evaluate fetches features from Feast (Redis) and runs inference
3. Results stored in evaluation.batch_results or evaluation.streaming_results

PRIMITIVES (what goes into evaluation.requests):
- transaction_id, customer_id, amount, quantity
- country, device_type, payment_method, category
- actual_label (ground truth for metrics calculation)

FEATURES (fetched by API from Feast):
- customer_behavioral: transactions_before, total_spend_before, etc.
- customer_spending: total_spend_7d, tx_count_7d, etc.

NO TRAINING-SERVING SKEW - API uses same Feast features for eval and inference.
"""

import os
import json
import uuid
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Generator
from dataclasses import dataclass
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from dagster import (
    asset,
    job,
    schedule,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    RunRequest,
    AssetSelection,
    define_asset_job,
    Config,
)

from src.core.resources import TrinoResource
from src.core.config import TRINO_CATALOG


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class EvaluationConfig:
    """Configuration for model evaluation."""
    # Tables - will be updated at initialization
    evaluation_requests_table: str = ""
    batch_results_table: str = ""
    streaming_results_table: str = ""

    # API
    fraud_api_url: str = os.getenv("FRAUD_API_URL", "http://exp-fraud-api:8001")

    # Evaluation settings
    fraud_threshold: float = 0.5

    def __post_init__(self):
        """Initialize table names with current TRINO_CATALOG."""
        if not self.evaluation_requests_table:
            self.evaluation_requests_table = f"{TRINO_CATALOG}.evaluation.requests"
        if not self.batch_results_table:
            self.batch_results_table = f"{TRINO_CATALOG}.evaluation.batch_results"
        if not self.streaming_results_table:
            self.streaming_results_table = f"{TRINO_CATALOG}.evaluation.streaming_results"


EVAL_CONFIG = EvaluationConfig()


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER: HTTP Session with Connection Pooling
# ═══════════════════════════════════════════════════════════════════════════════

def create_api_session(
    pool_connections: int = 10,
    pool_maxsize: int = 20,
    max_retries: int = 3,
    backoff_factor: float = 0.3,
) -> requests.Session:
    """
    Create a requests session with connection pooling and retry logic.

    Args:
        pool_connections: Number of connection pools to cache
        pool_maxsize: Max connections per pool
        max_retries: Number of retries for failed requests
        backoff_factor: Backoff factor for retries

    Returns:
        Configured requests.Session
    """
    session = requests.Session()

    # Configure retry strategy
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )

    # Create adapter with connection pooling
    adapter = HTTPAdapter(
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
        max_retries=retry_strategy,
    )

    # Mount for both http and https
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER: Checkpointing for Long-Running Jobs
# ═══════════════════════════════════════════════════════════════════════════════

CHECKPOINT_DIR = Path("/tmp/eval_checkpoints")


def save_checkpoint(
    evaluation_id: str,
    last_processed_id: str,
    processed_count: int,
    partial_results: Dict[str, Any],
) -> None:
    """
    Save progress checkpoint for resume capability.

    Args:
        evaluation_id: Unique evaluation run identifier
        last_processed_id: Last successfully processed request_id
        processed_count: Total records processed so far
        partial_results: Accumulated results by model
    """
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    checkpoint_file = CHECKPOINT_DIR / f"{evaluation_id}.json"

    # Convert numpy arrays to lists for JSON serialization
    serializable_results = {}
    for model_key, data in partial_results.items():
        serializable_results[model_key] = {
            k: (v if not isinstance(v, (list, np.ndarray)) or not v else
                [int(x) if isinstance(x, (np.integer, bool)) else float(x) if isinstance(x, (np.floating, float)) else x
                 for x in v])
            for k, v in data.items()
        }

    checkpoint_file.write_text(json.dumps({
        "last_processed_id": last_processed_id,
        "processed_count": processed_count,
        "partial_results": serializable_results,
        "timestamp": datetime.now().isoformat(),
    }))


def load_checkpoint(evaluation_id: str) -> Optional[Dict[str, Any]]:
    """
    Load checkpoint if exists.

    Args:
        evaluation_id: Unique evaluation run identifier

    Returns:
        Checkpoint data if exists, None otherwise
    """
    checkpoint_file = CHECKPOINT_DIR / f"{evaluation_id}.json"
    if checkpoint_file.exists():
        return json.loads(checkpoint_file.read_text())
    return None


def clear_checkpoint(evaluation_id: str) -> None:
    """Remove checkpoint file after successful completion."""
    checkpoint_file = CHECKPOINT_DIR / f"{evaluation_id}.json"
    if checkpoint_file.exists():
        checkpoint_file.unlink()


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER: Compute Metrics
# ═══════════════════════════════════════════════════════════════════════════════

def compute_metrics(y_true: List[int], y_pred: List[int], y_prob: List[float]) -> Dict[str, float]:
    """Compute evaluation metrics."""
    from sklearn.metrics import (
        accuracy_score, precision_score, recall_score, f1_score,
        roc_auc_score, confusion_matrix
    )
    
    y_true = np.array(y_true)
    y_pred = np.array(y_pred)
    y_prob = np.array(y_prob)
    
    metrics = {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "f1_score": float(f1_score(y_true, y_pred, zero_division=0)),
    }
    
    # AUC-ROC (needs both classes)
    if len(np.unique(y_true)) > 1:
        metrics["auc_roc"] = float(roc_auc_score(y_true, y_prob))
    else:
        metrics["auc_roc"] = 0.0
    
    # Confusion matrix
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()
    metrics["true_positives"] = int(tp)
    metrics["true_negatives"] = int(tn)
    metrics["false_positives"] = int(fp)
    metrics["false_negatives"] = int(fn)
    
    return metrics


# ═══════════════════════════════════════════════════════════════════════════════
# ASSET: Setup Evaluation Tables (Primitives Only)
# ═══════════════════════════════════════════════════════════════════════════════

class EvaluationSetupConfig(Config):
    """Config for evaluation setup."""
    drop_existing: bool = True


@asset(
    name="mlops_evaluation_setup",
    group_name="evaluation",
    description="Create evaluation tables (requests with PRIMITIVES ONLY, results)",
    required_resource_keys={"trino"},
)
def mlops_evaluation_setup(
    context: AssetExecutionContext,
    config: EvaluationSetupConfig,
) -> MaterializeResult:
    """
    Create evaluation tables:
    1. evaluation.requests - PRIMITIVES ONLY (no pre-computed features)
    2. evaluation.batch_results - Batch evaluation results
    3. evaluation.streaming_results - Streaming evaluation results
    """
    trino: TrinoResource = context.resources.trino

    # Ensure schema exists
    trino.execute_ddl(f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.evaluation")
    
    # =========================================================================
    # 1. EVALUATION REQUESTS TABLE - PRIMITIVES ONLY
    # =========================================================================
    requests_table = EVAL_CONFIG.evaluation_requests_table
    
    if config.drop_existing:
        trino.execute_ddl(f"DROP TABLE IF EXISTS {requests_table}")
    
    create_requests_sql = f"""
    CREATE TABLE IF NOT EXISTS {requests_table} (
        -- Identifiers
        request_id VARCHAR,
        transaction_id VARCHAR,
        customer_id VARCHAR,
        
        -- PRIMITIVES ONLY (features fetched from Feast by API)
        amount DOUBLE,
        quantity INTEGER,
        country VARCHAR,
        device_type VARCHAR,
        payment_method VARCHAR,
        category VARCHAR,
        
        -- Ground truth for evaluation
        actual_label INTEGER,
        
        -- Metadata
        request_timestamp TIMESTAMP(6),
        request_date DATE,
        batch_id VARCHAR,
        source VARCHAR
    ) WITH (
        partitioning = ARRAY['request_date']
    )
    """
    trino.execute_ddl(create_requests_sql)
    context.log.info(f"✓ Created {requests_table} (PRIMITIVES ONLY)")
    
    # =========================================================================
    # 2. BATCH RESULTS TABLE
    # =========================================================================
    batch_results_table = EVAL_CONFIG.batch_results_table
    
    if config.drop_existing:
        trino.execute_ddl(f"DROP TABLE IF EXISTS {batch_results_table}")
    
    create_batch_results_sql = f"""
    CREATE TABLE IF NOT EXISTS {batch_results_table} (
        -- Evaluation run info
        evaluation_id VARCHAR,
        evaluation_timestamp TIMESTAMP(6),
        evaluation_date DATE,
        
        -- Model info
        model_name VARCHAR,
        model_version VARCHAR,
        model_stage VARCHAR,
        run_id VARCHAR,
        
        -- Metrics
        accuracy DOUBLE,
        precision_score DOUBLE,
        recall DOUBLE,
        f1_score DOUBLE,
        auc_roc DOUBLE,
        
        -- Confusion matrix
        true_positives INTEGER,
        true_negatives INTEGER,
        false_positives INTEGER,
        false_negatives INTEGER,
        
        -- Sample info
        total_predictions INTEGER,
        total_frauds INTEGER,
        total_non_frauds INTEGER,
        
        -- Performance
        avg_latency_ms DOUBLE,
        feature_source VARCHAR,
        
        -- Config
        threshold DOUBLE,
        batch_id VARCHAR
    ) WITH (
        partitioning = ARRAY['evaluation_date']
    )
    """
    trino.execute_ddl(create_batch_results_sql)
    context.log.info(f"✓ Created {batch_results_table}")
    
    # =========================================================================
    # 3. STREAMING RESULTS TABLE
    # =========================================================================
    streaming_results_table = EVAL_CONFIG.streaming_results_table
    
    if config.drop_existing:
        trino.execute_ddl(f"DROP TABLE IF EXISTS {streaming_results_table}")
    
    create_streaming_results_sql = f"""
    CREATE TABLE IF NOT EXISTS {streaming_results_table} (
        -- Identifiers
        result_id VARCHAR,
        request_id VARCHAR,
        transaction_id VARCHAR,
        customer_id VARCHAR,
        
        -- Model info
        model_name VARCHAR,
        model_version VARCHAR,
        model_stage VARCHAR,
        run_id VARCHAR,
        
        -- Prediction
        fraud_probability DOUBLE,
        fraud_prediction INTEGER,
        risk_level VARCHAR,
        
        -- Ground truth
        actual_label INTEGER,
        is_correct INTEGER,
        
        -- Performance
        feature_latency_ms DOUBLE,
        inference_latency_ms DOUBLE,
        total_latency_ms DOUBLE,
        feature_source VARCHAR,
        
        -- Timestamps
        request_timestamp TIMESTAMP(6),
        result_timestamp TIMESTAMP(6),
        result_date DATE
    ) WITH (
        partitioning = ARRAY['result_date']
    )
    """
    trino.execute_ddl(create_streaming_results_sql)
    context.log.info(f"✓ Created {streaming_results_table}")
    
    return MaterializeResult(
        metadata={
            "requests_table": requests_table,
            "batch_results_table": batch_results_table,
            "streaming_results_table": streaming_results_table,
            "schema": "PRIMITIVES ONLY - features fetched from Feast by API",
        }
    )


# ═══════════════════════════════════════════════════════════════════════════════
# ASSET: Generate Evaluation Requests (Sample Data)
# ═══════════════════════════════════════════════════════════════════════════════

class GenerateRequestsConfig(Config):
    """Config for generating evaluation requests."""
    count: int = 1000
    fraud_rate: float = 0.1
    batch_id: Optional[str] = None


@asset(
    name="mlops_generate_evaluation_requests",
    group_name="evaluation",
    deps=[
        "mlops_evaluation_setup",          # Evaluation tables exist
        "mlops_gold_table",                # Gold table has customer data
        "materialize_online_features",     # Feast has features for customers
    ],
    description="Generate sample evaluation requests with PRIMITIVES ONLY",
    required_resource_keys={"trino"},
)
def mlops_generate_evaluation_requests(
    context: AssetExecutionContext,
    config: GenerateRequestsConfig,
) -> MaterializeResult:
    """
    Generate sample evaluation requests.
    
    This creates test data with PRIMITIVES ONLY:
    - transaction_id, customer_id
    - amount, quantity, country, device_type, payment_method, category
    - actual_label (simulated ground truth)
    
    Features will be fetched from Feast by the API during evaluation.
    """
    import random
    
    trino: TrinoResource = context.resources.trino
    requests_table = EVAL_CONFIG.evaluation_requests_table
    
    batch_id = config.batch_id or f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    now = datetime.now()
    
    # Get real customer IDs from Gold table to ensure Feast has features
    customer_query = f"""
    SELECT DISTINCT customer_id
    FROM {TRINO_CATALOG}.gold.fraud_training_data
    LIMIT 100
    """
    customer_rows = trino.execute_query(customer_query)
    customer_ids = [row[0] for row in customer_rows] if customer_rows else [f"CUST-{i:06d}" for i in range(10)]
    
    context.log.info(f"Using {len(customer_ids)} customer IDs from Gold table")
    
    # Configuration for random data
    countries = ["US", "UK", "CA", "AU", "DE", "FR", "JP", "BR", "IN", "MX"]
    device_types = ["desktop", "mobile", "tablet"]
    payment_methods = ["credit_card", "debit_card", "wallet", "bank_transfer"]
    categories = ["Electronics", "Clothing", "Home", "Food", "Travel", "Entertainment"]
    
    # Generate requests
    values = []
    for i in range(config.count):
        request_id = str(uuid.uuid4())
        transaction_id = f"EVAL-{now.strftime('%Y%m%d%H%M%S')}-{i:06d}"
        customer_id = random.choice(customer_ids)
        
        # Random primitives
        amount = round(random.uniform(10, 5000), 2)
        quantity = random.randint(1, 5)
        country = random.choice(countries)
        device_type = random.choice(device_types)
        payment_method = random.choice(payment_methods)
        category = random.choice(categories)
        
        # Simulated ground truth
        # Higher fraud probability for: high amount, wallet payment, new customer
        fraud_prob = config.fraud_rate
        if amount > 2000:
            fraud_prob += 0.1
        if payment_method == "wallet":
            fraud_prob += 0.05
        actual_label = 1 if random.random() < fraud_prob else 0
        
        values.append(f"""(
            '{request_id}',
            '{transaction_id}',
            '{customer_id}',
            {amount},
            {quantity},
            '{country}',
            '{device_type}',
            '{payment_method}',
            '{category}',
            {actual_label},
            TIMESTAMP '{now.strftime('%Y-%m-%d %H:%M:%S')}',
            DATE '{now.strftime('%Y-%m-%d')}',
            '{batch_id}',
            'generated'
        )""")
    
    # Insert in batches
    batch_size = 500
    total_inserted = 0
    
    for i in range(0, len(values), batch_size):
        batch_values = values[i:i + batch_size]
        insert_sql = f"""
        INSERT INTO {requests_table} (
            request_id, transaction_id, customer_id,
            amount, quantity, country, device_type, payment_method, category,
            actual_label, request_timestamp, request_date, batch_id, source
        ) VALUES {', '.join(batch_values)}
        """
        trino.execute_ddl(insert_sql)
        total_inserted += len(batch_values)
        context.log.info(f"  Inserted {total_inserted}/{config.count} requests")
    
    # Count fraud/non-fraud
    fraud_count = sum(1 for v in values if "actual_label,\n            1," in v or ", 1," in v.split("actual_label")[0][-10:])
    
    context.log.info(f"✓ Generated {config.count} evaluation requests (batch_id: {batch_id})")
    
    return MaterializeResult(
        metadata={
            "table": requests_table,
            "count": config.count,
            "batch_id": batch_id,
            "fraud_rate": config.fraud_rate,
            "customer_ids_used": len(customer_ids),
            "schema": "PRIMITIVES ONLY",
        }
    )


# ═══════════════════════════════════════════════════════════════════════════════
# ASSET: Batch Evaluation (reads from evaluation.requests, calls API)
# ═══════════════════════════════════════════════════════════════════════════════

class BatchEvaluationConfig(Config):
    """
    Config for batch evaluation with scalability settings.

    All settings can be overridden via environment variables:
    - EVAL_SAMPLE_SIZE: Max records to process (0 = unlimited)
    - EVAL_CHUNK_SIZE: DB fetch chunk size
    - EVAL_API_BATCH_SIZE: Transactions per API call
    - EVAL_PARALLEL_WORKERS: Concurrent API calls
    - EVAL_ENABLE_CHECKPOINTS: Enable resume capability (true/false)
    - EVAL_CHECKPOINT_INTERVAL: Save progress every N records
    - EVAL_USE_LEGACY: Use legacy sequential processing (true/false)
    """
    batch_id: Optional[str] = None  # If None, evaluate all unprocessed
    stages: List[str] = ["Production", "Staging"]

    # Scalability settings for 1M+ records (env var overridable)
    sample_size: int = int(os.getenv("EVAL_SAMPLE_SIZE", "0"))  # 0 = process all
    chunk_size: int = int(os.getenv("EVAL_CHUNK_SIZE", "10000"))  # DB fetch chunk
    api_batch_size: int = int(os.getenv("EVAL_API_BATCH_SIZE", "500"))  # Per API call
    parallel_workers: int = int(os.getenv("EVAL_PARALLEL_WORKERS", "5"))  # Concurrent
    enable_checkpoints: bool = os.getenv("EVAL_ENABLE_CHECKPOINTS", "true").lower() == "true"
    checkpoint_interval: int = int(os.getenv("EVAL_CHECKPOINT_INTERVAL", "10000"))
    use_legacy_processing: bool = os.getenv("EVAL_USE_LEGACY", "false").lower() == "true"


@asset(
    name="mlops_batch_evaluation",
    group_name="evaluation",
    deps=[
        "mlops_evaluation_setup",          # Evaluation tables exist
        "materialize_online_features",     # Feast features in Redis
        "train_with_lineage",              # Model trained and registered
    ],
    description="Batch evaluation: Read PRIMITIVES from evaluation.requests, call API for features + inference",
    required_resource_keys={"trino"},
)
def mlops_batch_evaluation(
    context: AssetExecutionContext,
    config: BatchEvaluationConfig,
) -> MaterializeResult:
    """
    Batch Model Evaluation via API with streaming and parallel processing.

    Supports 1M+ records through:
    - Streaming database queries (memory-efficient)
    - Parallel API batch submission
    - Checkpointing for resume capability

    Flow:
    1. Read PRIMITIVES from evaluation.requests table (streaming)
    2. Process in parallel batches via API /evaluate/batch
    3. API fetches features from Feast (Redis) - NO training-serving skew
    4. Store aggregated metrics in evaluation.batch_results
    """
    trino: TrinoResource = context.resources.trino
    api_url = EVAL_CONFIG.fraud_api_url
    evaluation_id = str(uuid.uuid4())

    context.log.info("=" * 60)
    context.log.info("BATCH MODEL EVALUATION (via API)")
    context.log.info("=" * 60)
    context.log.info(f"API: {api_url}")
    context.log.info(f"Evaluation ID: {evaluation_id}")
    context.log.info(f"Config: chunk_size={config.chunk_size}, api_batch_size={config.api_batch_size}, "
                     f"parallel_workers={config.parallel_workers}")
    context.log.info("Features fetched from Feast by API (no training-serving skew)")

    # 1. Check API health and model status
    try:
        health_resp = requests.get(f"{api_url}/health", timeout=10)
        health_resp.raise_for_status()
        health_data = health_resp.json()
        context.log.info(f"✓ API healthy: {health_data}")

        model_status = health_data.get('components', {}).get('model', 'unknown')
        if model_status == 'not_loaded':
            context.log.error("Model not loaded in API. Check MLflow model stage assignment.")
            raise RuntimeError(
                "Model not loaded. Ensure a model is registered in MLflow with stage='Production' or 'Staging'. "
                "Run: mlflow.transition_model_version_stage(name='fraud_detection_model', version='1', stage='Production')"
            )
    except requests.RequestException as e:
        context.log.error(f"API not reachable: {e}")
        return MaterializeResult(metadata={"status": "api_unreachable", "error": str(e)})

    # 2. Build query - with or without LIMIT based on sample_size
    requests_table = EVAL_CONFIG.evaluation_requests_table

    where_clause = ""
    if config.batch_id:
        where_clause = f"WHERE batch_id = '{config.batch_id}'"

    # Check for checkpoint to resume from
    checkpoint = None
    if config.enable_checkpoints:
        checkpoint = load_checkpoint(evaluation_id)
        if checkpoint:
            context.log.info(f"✓ Resuming from checkpoint: {checkpoint['processed_count']} records already processed")
            if where_clause:
                where_clause += f" AND request_id > '{checkpoint['last_processed_id']}'"
            else:
                where_clause = f"WHERE request_id > '{checkpoint['last_processed_id']}'"

    # Build query with optional LIMIT
    limit_clause = f"LIMIT {config.sample_size}" if config.sample_size > 0 else ""

    query = f"""
    SELECT
        request_id,
        transaction_id,
        customer_id,
        amount,
        quantity,
        country,
        device_type,
        payment_method,
        category,
        actual_label
    FROM {requests_table}
    {where_clause}
    ORDER BY request_id
    {limit_clause}
    """

    columns = [
        "request_id", "transaction_id", "customer_id",
        "amount", "quantity", "country", "device_type", "payment_method", "category",
        "actual_label"
    ]

    # 3. Initialize results - restore from checkpoint if available
    results_by_model: Dict[str, Dict] = {}
    if checkpoint:
        results_by_model = checkpoint.get('partial_results', {})

    # Thread-safe lock for updating results
    results_lock = threading.Lock()

    def build_eval_request(row) -> Dict:
        """Build evaluation request for a single transaction."""
        return {
            "transaction": {
                "transaction_id": row["transaction_id"],
                "customer_id": row["customer_id"],
                "amount": float(row["amount"]),
                "quantity": int(row["quantity"]),
                "country": str(row["country"]) if row["country"] else "US",
                "device_type": str(row["device_type"]) if row["device_type"] else "desktop",
                "payment_method": str(row["payment_method"]) if row["payment_method"] else "credit_card",
                "category": str(row["category"]) if row["category"] else "Other",
            },
            "model_stages": config.stages,
            "run_ids": [],
        }

    def submit_batch(session, batch_data: List[Dict], batch_idx: int) -> tuple:
        """Submit a single batch to the API and return results."""
        batch_request = {"transactions": batch_data}
        try:
            resp = session.post(
                f"{api_url}/evaluate/batch",
                json=batch_request,
                timeout=300,  # 5 min timeout for large batches
            )
            if resp.status_code == 200:
                return batch_idx, resp.json(), None
            else:
                return batch_idx, None, f"HTTP {resp.status_code}: {resp.text[:100]}"
        except Exception as e:
            return batch_idx, None, str(e)

    def process_batch_result(batch_result: Dict, actual_labels: Dict[str, int]):
        """Process a batch result and update results_by_model (thread-safe)."""
        with results_lock:
            for eval_result in batch_result.get("results", []):
                tx_id = eval_result["transaction_id"]
                actual_label = actual_labels.get(tx_id, 0)

                for pred in eval_result.get("predictions", []):
                    model_key = f"{pred['model_stage']}_{pred['model_version']}"

                    if model_key not in results_by_model:
                        results_by_model[model_key] = {
                            "model_name": pred.get("model_name", "fraud_detection_model"),
                            "model_version": pred["model_version"],
                            "model_stage": pred["model_stage"],
                            "run_id": pred.get("run_id"),
                            "y_true": [],
                            "y_pred": [],
                            "y_prob": [],
                            "latencies": [],
                            "feature_source": eval_result.get("feature_source", "unknown"),
                        }

                    results_by_model[model_key]["y_true"].append(actual_label)
                    results_by_model[model_key]["y_pred"].append(1 if pred["is_fraud"] else 0)
                    results_by_model[model_key]["y_prob"].append(pred["fraud_score"])
                    results_by_model[model_key]["latencies"].append(pred.get("inference_latency_ms", 0))

    # Create session with connection pooling
    session = create_api_session(
        pool_connections=config.parallel_workers,
        pool_maxsize=config.parallel_workers * 2,
    )
    context.log.info(f"✓ Created HTTP session with pool_maxsize={config.parallel_workers * 2}")

    # 4. Process data - streaming or legacy mode
    total_processed = checkpoint['processed_count'] if checkpoint else 0
    last_request_id = checkpoint['last_processed_id'] if checkpoint else None
    start_time = datetime.now()

    try:
        if config.use_legacy_processing:
            # Legacy mode: load all into memory (for backward compatibility)
            context.log.info("Using LEGACY processing mode (all-in-memory)")
            rows = trino.execute_query(query)
            if not rows:
                context.log.warning("No evaluation requests found. Run mlops_generate_evaluation_requests first.")
                return MaterializeResult(metadata={"status": "no_data"})

            eval_df = pd.DataFrame(rows, columns=columns)
            context.log.info(f"✓ Loaded {len(eval_df)} evaluation requests")

            # Build actual_labels mapping
            actual_labels = {row["transaction_id"]: int(row["actual_label"]) for _, row in eval_df.iterrows()}

            # Process in batches (sequential for legacy mode)
            for start_idx in range(0, len(eval_df), config.api_batch_size):
                end_idx = min(start_idx + config.api_batch_size, len(eval_df))
                batch_df = eval_df.iloc[start_idx:end_idx]
                batch_data = [build_eval_request(row) for _, row in batch_df.iterrows()]

                _, result, error = submit_batch(session, batch_data, 0)
                if result:
                    process_batch_result(result, actual_labels)
                    total_processed += result.get("success_count", len(batch_df))
                elif error:
                    context.log.warning(f"Batch error: {error}")

                if (start_idx // config.api_batch_size + 1) % 10 == 0:
                    context.log.info(f"  Progress: {total_processed}/{len(eval_df)}")

        else:
            # Streaming mode: process in chunks with parallel batches
            context.log.info("Using STREAMING processing mode (memory-efficient)")

            chunk_num = 0
            for chunk_rows in trino.execute_query_streaming(query, config.chunk_size):
                chunk_num += 1
                chunk_df = pd.DataFrame(chunk_rows, columns=columns)
                chunk_size = len(chunk_df)

                context.log.info(f"  Processing chunk {chunk_num}: {chunk_size} records")

                # Build actual_labels for this chunk
                actual_labels = {row["transaction_id"]: int(row["actual_label"]) for _, row in chunk_df.iterrows()}

                # Split chunk into API batches
                batches = []
                for start_idx in range(0, chunk_size, config.api_batch_size):
                    end_idx = min(start_idx + config.api_batch_size, chunk_size)
                    batch_df = chunk_df.iloc[start_idx:end_idx]
                    batch_data = [build_eval_request(row) for _, row in batch_df.iterrows()]
                    batches.append(batch_data)

                # Process batches in parallel
                with ThreadPoolExecutor(max_workers=config.parallel_workers) as executor:
                    futures = {
                        executor.submit(submit_batch, session, batch_data, idx): idx
                        for idx, batch_data in enumerate(batches)
                    }

                    for future in as_completed(futures):
                        batch_idx, result, error = future.result()
                        if result:
                            process_batch_result(result, actual_labels)
                            total_processed += result.get("success_count", 0)
                        elif error:
                            context.log.warning(f"Batch {batch_idx} error: {error}")

                # Track last processed request_id for checkpointing
                last_request_id = chunk_df.iloc[-1]["request_id"]

                # Save checkpoint periodically
                if config.enable_checkpoints and total_processed % config.checkpoint_interval < chunk_size:
                    save_checkpoint(evaluation_id, last_request_id, total_processed, results_by_model)
                    context.log.info(f"  ✓ Checkpoint saved: {total_processed} records processed")

                elapsed = (datetime.now() - start_time).total_seconds()
                rate = total_processed / elapsed if elapsed > 0 else 0
                context.log.info(f"  Chunk {chunk_num} complete: {total_processed} total, {rate:.1f} records/sec")

        context.log.info(f"✓ Processed {total_processed} transactions via batch API")

    finally:
        session.close()
        context.log.info("✓ Closed HTTP session")

    # 5. Compute and store metrics
    context.log.info(f"DEBUG: Final results_by_model keys: {list(results_by_model.keys())}")
    context.log.info(f"DEBUG: Final results_by_model count: {len(results_by_model)}")
    for k, v in results_by_model.items():
        context.log.info(f"DEBUG:   {k}: {len(v.get('y_true', []))} samples")

    if not results_by_model:
        context.log.error("No successful evaluations")
        return MaterializeResult(metadata={"status": "no_results"})

    now = datetime.now()
    batch_results_table = EVAL_CONFIG.batch_results_table
    all_metrics = []

    for model_key, data in results_by_model.items():
        if len(data["y_true"]) < 10:
            context.log.warning(f"Skipping {model_key}: only {len(data['y_true'])} samples")
            continue

        metrics = compute_metrics(data["y_true"], data["y_pred"], data["y_prob"])

        # Insert results
        insert_sql = f"""
        INSERT INTO {batch_results_table} (
            evaluation_id, evaluation_timestamp, evaluation_date,
            model_name, model_version, model_stage, run_id,
            accuracy, precision_score, recall, f1_score, auc_roc,
            true_positives, true_negatives, false_positives, false_negatives,
            total_predictions, total_frauds, total_non_frauds,
            avg_latency_ms, feature_source, threshold, batch_id
        ) VALUES (
            '{evaluation_id}',
            TIMESTAMP '{now.strftime('%Y-%m-%d %H:%M:%S')}',
            DATE '{now.strftime('%Y-%m-%d')}',
            '{data["model_name"]}',
            '{data["model_version"]}',
            '{data["model_stage"]}',
            '{data["run_id"] or ""}',
            {metrics["accuracy"]},
            {metrics["precision"]},
            {metrics["recall"]},
            {metrics["f1_score"]},
            {metrics["auc_roc"]},
            {metrics["true_positives"]},
            {metrics["true_negatives"]},
            {metrics["false_positives"]},
            {metrics["false_negatives"]},
            {len(data["y_true"])},
            {sum(data["y_true"])},
            {len(data["y_true"]) - sum(data["y_true"])},
            {np.mean(data["latencies"]) if data["latencies"] else 0},
            '{data["feature_source"]}',
            {EVAL_CONFIG.fraud_threshold},
            '{config.batch_id or "all"}'
        )
        """
        trino.execute_ddl(insert_sql)

        all_metrics.append({
            "model": model_key,
            "stage": data["model_stage"],
            "version": data["model_version"],
            **metrics,
            "samples": len(data["y_true"]),
        })

        context.log.info(f"""
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        Model: {data["model_stage"]} v{data["model_version"]}
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        Samples:   {len(data["y_true"])}
        Accuracy:  {metrics["accuracy"]:.4f}
        Precision: {metrics["precision"]:.4f}
        Recall:    {metrics["recall"]:.4f}
        F1 Score:  {metrics["f1_score"]:.4f}
        AUC-ROC:   {metrics["auc_roc"]:.4f}
        Features:  {data["feature_source"]} (fetched from Feast)
        """)

    # Clear checkpoint on successful completion
    if config.enable_checkpoints:
        clear_checkpoint(evaluation_id)
        context.log.info("✓ Cleared checkpoint (evaluation complete)")

    elapsed_total = (datetime.now() - start_time).total_seconds()
    context.log.info(f"✓ Evaluation complete in {elapsed_total:.1f}s. Results stored in {batch_results_table}")

    return MaterializeResult(
        metadata={
            "evaluation_id": evaluation_id,
            "total_requests": total_processed,
            "models_evaluated": len(all_metrics),
            "results_table": batch_results_table,
            "metrics": all_metrics,
            "feature_source": "Feast (via API)",
            "processing_mode": "legacy" if config.use_legacy_processing else "streaming",
            "elapsed_seconds": round(elapsed_total, 2),
            "records_per_second": round(total_processed / elapsed_total, 2) if elapsed_total > 0 else 0,
        }
    )


# ═══════════════════════════════════════════════════════════════════════════════
# ASSET: Evaluation Statistics (aggregated view)
# ═══════════════════════════════════════════════════════════════════════════════

@asset(
    name="mlops_evaluation_statistics",
    group_name="evaluation",
    deps=["mlops_batch_evaluation"],
    description="Compute aggregated evaluation statistics across models",
    required_resource_keys={"trino"},
)
def mlops_evaluation_statistics(
    context: AssetExecutionContext,
) -> MaterializeResult:
    """
    Aggregated statistics from batch evaluation results.
    """
    trino: TrinoResource = context.resources.trino
    batch_results_table = EVAL_CONFIG.batch_results_table
    
    # Get latest results per model
    query = f"""
    SELECT 
        model_stage,
        model_version,
        accuracy,
        precision_score,
        recall,
        f1_score,
        auc_roc,
        total_predictions,
        evaluation_timestamp,
        feature_source
    FROM {batch_results_table}
    WHERE evaluation_date = CURRENT_DATE
    ORDER BY evaluation_timestamp DESC
    """
    
    rows = trino.execute_query(query)
    
    if not rows:
        context.log.warning("No evaluation results found for today")
        return MaterializeResult(metadata={"status": "no_results"})
    
    stats = []
    for row in rows:
        stats.append({
            "model_stage": row[0],
            "model_version": row[1],
            "accuracy": row[2],
            "precision": row[3],
            "recall": row[4],
            "f1_score": row[5],
            "auc_roc": row[6],
            "samples": row[7],
            "timestamp": str(row[8]),
            "feature_source": row[9],
        })
    
    context.log.info(f"Found {len(stats)} evaluation results for today")
    for s in stats:
        context.log.info(f"  {s['model_stage']} v{s['model_version']}: F1={s['f1_score']:.4f}, AUC={s['auc_roc']:.4f}")
    
    return MaterializeResult(
        metadata={
            "results_count": len(stats),
            "statistics": stats,
        }
    )


# ═══════════════════════════════════════════════════════════════════════════════
# JOBS
# ═══════════════════════════════════════════════════════════════════════════════

evaluation_setup_job = define_asset_job(
    name="evaluation_setup_job",
    selection=AssetSelection.assets("mlops_evaluation_setup"),
    description="Create evaluation tables (primitives only schema)",
)

generate_evaluation_data_job = define_asset_job(
    name="generate_evaluation_data_job",
    selection=AssetSelection.assets("mlops_generate_evaluation_requests"),
    description="Generate sample evaluation requests (primitives only)",
)

batch_evaluation_job = define_asset_job(
    name="batch_evaluation_job",
    selection=AssetSelection.assets("mlops_batch_evaluation"),
    description="Run batch evaluation via API (features from Feast)",
)

evaluation_statistics_job = define_asset_job(
    name="evaluation_statistics_job",
    selection=AssetSelection.assets("mlops_evaluation_statistics"),
    description="Compute evaluation statistics",
)


# ═══════════════════════════════════════════════════════════════════════════════
# SCHEDULES
# ═══════════════════════════════════════════════════════════════════════════════

@schedule(
    job=batch_evaluation_job,
    cron_schedule="0 6 * * *",  # Daily at 6 AM
)
def daily_batch_evaluation_schedule(context):
    """Run batch evaluation daily."""
    return RunRequest(
        run_key=f"daily_eval_{datetime.now().strftime('%Y%m%d')}",
        tags={"schedule": "daily_batch_evaluation"},
    )


# ═══════════════════════════════════════════════════════════════════════════════
# EXPORTS
# ═══════════════════════════════════════════════════════════════════════════════

EVALUATION_ASSETS = [
    mlops_evaluation_setup,
    mlops_generate_evaluation_requests,
    mlops_batch_evaluation,
    mlops_evaluation_statistics,
]

EVALUATION_JOBS = [
    evaluation_setup_job,
    generate_evaluation_data_job,
    batch_evaluation_job,
    evaluation_statistics_job,
]

EVALUATION_SCHEDULES = [
    daily_batch_evaluation_schedule,
]