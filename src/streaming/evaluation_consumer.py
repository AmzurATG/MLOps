#!/usr/bin/env python3
"""
Streaming Evaluation Consumer
═══════════════════════════════════════════════════════════════════════════════

Consumes CDC events from Kafka (via Debezium) and evaluates multiple models
by calling the Fraud Detection API.

ARCHITECTURE (PRIMITIVES ONLY):
    evaluation.requests table (PRIMITIVES ONLY)
        → Debezium CDC 
            → Kafka (fraud.demo.evaluation_requests)
                → This Consumer 
                    → Fraud API /evaluate
                        - API fetches features from Feast (Redis)
                        - API runs inference with Production + Staging models
                            → Results to CLI + Iceberg

PRIMITIVES (what flows through this consumer):
    - transaction_id, customer_id
    - amount, quantity, country, device_type, payment_method, category
    - actual_label (ground truth for metrics)

FEATURES (fetched by API from Feast - NOT in CDC stream):
    - customer_behavioral: transactions_before, total_spend_before, etc.
    - customer_spending: total_spend_7d, tx_count_7d, etc.

Usage:
    python evaluation_consumer.py [options]
    
    Options:
        --api-url http://exp-fraud-api:8001  API URL
        --stages Production,Staging      Models to evaluate by stage
        --run-ids abc123,def456          Specific MLflow run IDs
        --output-cli                     Show live results in CLI
        --output-iceberg                 Store results to Iceberg

Environment Variables:
    FRAUD_API_URL           Fraud API URL (default: http://exp-fraud-api:8001)
    KAFKA_BOOTSTRAP_SERVERS Kafka broker (default: exp-kafka:9092)
    KAFKA_TOPIC             Topic to consume (default: fraud.demo.evaluation_requests)
    KAFKA_GROUP_ID          Consumer group (default: model-evaluator)
"""

import os
import sys
import json
import signal
import time
import uuid
import logging
import argparse
import threading
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from collections import defaultdict

# Performance tracking imports
try:
    from api.performance_tracker import (
        get_performance_tracker,
        record_prediction,
        record_outcome,
    )
    PERFORMANCE_TRACKER_AVAILABLE = True
except ImportError:
    PERFORMANCE_TRACKER_AVAILABLE = False
    print("[EvaluationConsumer] Performance tracker not available")

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class Config:
    # API
    api_url: str = os.getenv("FRAUD_API_URL", "http://exp-fraud-api:8001")
    api_timeout: int = 30
    
    # Kafka
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "exp-kafka:9092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "fraud.demo.evaluation_requests")
    kafka_group_id: str = os.getenv("KAFKA_GROUP_ID", "model-evaluator")
    
    # Trino / Iceberg
    trino_host: str = os.getenv("TRINO_HOST", "trino")
    trino_port: int = int(os.getenv("TRINO_PORT", "8080"))
    trino_user: str = os.getenv("TRINO_USER", "trino")
    trino_catalog: str = os.getenv("TRINO_CATALOG", "iceberg_dev")
    results_table: str = os.getenv("RESULTS_TABLE", "iceberg_dev.evaluation.streaming_results")
    
    # Evaluation
    stages: List[str] = field(default_factory=lambda: ["Production", "Staging"])
    run_ids: List[str] = field(default_factory=list)
    
    # Output
    output_cli: bool = True
    output_iceberg: bool = True
    iceberg_batch_size: int = 100
    iceberg_flush_interval: int = 10
    
    verbose: bool = False


# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# API CLIENT
# ═══════════════════════════════════════════════════════════════════════════════

class FraudAPIClient:
    """Client for the Fraud Detection API with connection pooling and retry logic."""

    def __init__(self, config: Config):
        self.config = config
        self.session = self._create_session()
        self._check_health()

    def _create_session(self) -> requests.Session:
        """Create session with connection pooling and retry strategy."""
        session = requests.Session()

        # Retry strategy for resilience
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )

        # HTTPAdapter with connection pooling
        adapter = HTTPAdapter(
            pool_connections=10,  # Number of connection pools
            pool_maxsize=20,      # Max connections per pool
            max_retries=retry_strategy,
        )

        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session
    
    def _check_health(self):
        """Check API health."""
        try:
            resp = self.session.get(
                f"{self.config.api_url}/health",
                timeout=10
            )
            if resp.status_code == 200:
                logger.info(f"✓ API connected: {self.config.api_url}")
            else:
                logger.warning(f"API returned {resp.status_code}")
        except Exception as e:
            logger.error(f"✗ API not reachable: {e}")
            raise
    
    def evaluate(
        self,
        transaction: Dict[str, Any],
        model_stages: List[str],
        run_ids: List[str] = None,
    ) -> Dict[str, Any]:
        """
        Call API /evaluate endpoint for multi-model comparison.
        """
        request_body = {
            "transaction": transaction,
            "model_stages": model_stages,
            "run_ids": run_ids or [],
        }
        
        try:
            resp = self.session.post(
                f"{self.config.api_url}/evaluate",
                json=request_body,
                timeout=self.config.api_timeout,
            )
            
            if resp.status_code == 200:
                return resp.json()
            else:
                logger.warning(f"API error: {resp.status_code} - {resp.text[:100]}")
                return None
                
        except Exception as e:
            logger.error(f"API call failed: {e}")
            return None

    def close(self):
        """Close the session and release connections."""
        if self.session:
            self.session.close()
            logger.debug("API client session closed")


# ═══════════════════════════════════════════════════════════════════════════════
# ICEBERG SINK
# ═══════════════════════════════════════════════════════════════════════════════

class IcebergSink:
    """Batched writes to Iceberg for streaming results."""
    
    def __init__(self, config: Config):
        self.config = config
        self.buffer = []
        self.buffer_lock = threading.Lock()
        self.last_flush = datetime.now()
        self._connection = None
    
    def _get_connection(self):
        if self._connection is None:
            from trino.dbapi import connect
            self._connection = connect(
                host=self.config.trino_host,
                port=self.config.trino_port,
                user=self.config.trino_user,
                catalog=self.config.trino_catalog,
            )
        return self._connection
    
    def _format_timestamp(self, ts) -> str:
        """Convert timestamp to Trino-compatible format (YYYY-MM-DD HH:MM:SS)."""
        if ts is None:
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(ts, datetime):
            return ts.strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(ts, str):
            # Handle ISO format: 2025-12-07T18:41:24Z -> 2025-12-07 18:41:24
            ts = ts.replace('T', ' ').replace('Z', '').split('.')[0]
            return ts
        return str(ts)
    
    def add(self, result: Dict[str, Any]):
        """Add result to buffer."""
        with self.buffer_lock:
            self.buffer.append(result)
            
            if len(self.buffer) >= self.config.iceberg_batch_size:
                self._flush()
    
    def _flush(self):
        """Flush buffer to Iceberg."""
        if not self.buffer:
            return
        
        with self.buffer_lock:
            records = self.buffer.copy()
            self.buffer = []
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            for r in records:
                def esc(s):
                    if s is None:
                        return "NULL"
                    return f"'{str(s).replace(chr(39), chr(39)+chr(39))}'"
                
                sql = f"""
                INSERT INTO {self.config.results_table} VALUES (
                    {esc(r.get('result_id'))},
                    {esc(r.get('request_id'))},
                    {esc(r.get('transaction_id'))},
                    {esc(r.get('customer_id'))},
                    {esc(r.get('model_name'))},
                    {esc(r.get('model_version'))},
                    {esc(r.get('model_stage'))},
                    {esc(r.get('run_id'))},
                    CAST({r.get('fraud_probability', 0)} AS DOUBLE),
                    {r.get('fraud_prediction', 0)},
                    {esc(r.get('risk_level'))},
                    {r.get('actual_label') if r.get('actual_label') is not None else 'NULL'},
                    {r.get('is_correct') if r.get('is_correct') is not None else 'NULL'},
                    CAST({r.get('feature_latency_ms', 0)} AS DOUBLE),
                    CAST({r.get('inference_latency_ms', 0)} AS DOUBLE),
                    CAST({r.get('total_latency_ms', 0)} AS DOUBLE),
                    {esc(r.get('feature_source'))},
                    TIMESTAMP '{self._format_timestamp(r.get('request_timestamp'))}',
                    TIMESTAMP '{self._format_timestamp(r.get('result_timestamp'))}',
                    DATE '{datetime.now().strftime('%Y-%m-%d')}'
                )
                """
                cursor.execute(sql)
            
            self.last_flush = datetime.now()
            logger.debug(f"Flushed {len(records)} results to Iceberg")
            
        except Exception as e:
            logger.error(f"Iceberg flush failed: {e}")
            with self.buffer_lock:
                self.buffer = records + self.buffer
    
    def should_flush(self) -> bool:
        elapsed = (datetime.now() - self.last_flush).total_seconds()
        return elapsed >= self.config.iceberg_flush_interval and len(self.buffer) > 0
    
    def flush(self):
        self._flush()


# ═══════════════════════════════════════════════════════════════════════════════
# CLI DISPLAY
# ═══════════════════════════════════════════════════════════════════════════════

class CLIDisplay:
    """Rich CLI display for live evaluation results."""
    
    def __init__(self, config: Config):
        self.config = config
        self.results_buffer = []
        self.max_display = 10
        self.stats = defaultdict(lambda: {"count": 0, "correct": 0, "latency_sum": 0})
    
    def add_results(
        self,
        transaction_id: str,
        amount: float,
        actual_label: Optional[int],
        predictions: List[Dict[str, Any]],
        feature_latency_ms: float,
        total_latency_ms: float,
    ):
        """Add results and refresh display."""
        
        row = {
            "transaction_id": transaction_id,
            "amount": amount,
            "actual_label": actual_label,
            "predictions": predictions,
            "feature_latency_ms": feature_latency_ms,
            "total_latency_ms": total_latency_ms,
            "timestamp": datetime.now(),
        }
        
        self.results_buffer.append(row)
        if len(self.results_buffer) > self.max_display:
            self.results_buffer.pop(0)
        
        # Update stats
        for pred in predictions:
            key = pred.get("model_stage", "unknown")
            self.stats[key]["count"] += 1
            self.stats[key]["latency_sum"] += pred.get("inference_latency_ms", 0)
            if actual_label is not None and pred.get("fraud_score", -1) >= 0:
                if pred.get("is_fraud") == (actual_label == 1):
                    self.stats[key]["correct"] += 1
        
        self._render()
    
    def _render(self):
        """Render the display."""
        print("\033[2J\033[H", end="")
        
        print("=" * 100)
        print("  STREAMING MODEL EVALUATION - LIVE VIEW (via API)")
        print("=" * 100)
        print()
        
        # Stats
        print("MODEL STATISTICS:")
        print("-" * 60)
        for model_stage, stats in sorted(self.stats.items()):
            count = stats["count"]
            correct = stats["correct"]
            acc = (correct / count * 100) if count > 0 else 0
            avg_lat = (stats["latency_sum"] / count) if count > 0 else 0
            print(f"  {model_stage:<12} | Predictions: {count:>6} | Accuracy: {acc:>6.2f}% | Avg Latency: {avg_lat:>6.2f}ms")
        print()
        
        # Results table
        models = list(set(p.get("model_stage", "?") for row in self.results_buffer for p in row["predictions"]))
        models = sorted(models)
        
        header = f"{'TXN_ID':<16} {'Amount':>10} {'Label':>6}"
        for m in models:
            header += f" | {m[:8]:>8}"
        header += f" {'Match':>6} {'Latency':>10}"
        
        print("RECENT PREDICTIONS:")
        print("-" * len(header))
        print(header)
        print("-" * len(header))
        
        for row in self.results_buffer[-10:]:
            txn_id = str(row["transaction_id"])[:16]
            amount = f"${row['amount']:.2f}" if row['amount'] else "N/A"
            label = str(row['actual_label']) if row['actual_label'] is not None else "?"
            
            line = f"{txn_id:<16} {amount:>10} {label:>6}"
            
            preds_by_model = {p.get("model_stage"): p for p in row["predictions"]}
            
            all_match = True
            for m in models:
                pred = preds_by_model.get(m, {})
                proba = pred.get("fraud_score", -1)
                if proba >= 0:
                    line += f" | {proba:>8.4f}"
                    if row['actual_label'] is not None:
                        if pred.get("is_fraud") != (row['actual_label'] == 1):
                            all_match = False
                else:
                    line += f" | {'ERR':>8}"
            
            match_str = "✓" if all_match else "✗"
            line += f" {match_str:>6} {row['total_latency_ms']:>10.2f}ms"
            
            print(line)
        
        print("-" * len(header))
        print()
        print(f"Press Ctrl+C to stop | Buffer: {len(self.results_buffer)} rows")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN CONSUMER
# ═══════════════════════════════════════════════════════════════════════════════

class EvaluationConsumer:
    """Main Kafka consumer for streaming evaluation via API."""
    
    def __init__(self, config: Config):
        self.config = config
        self.running = False
        
        # Components
        self.api_client = FraudAPIClient(config)
        self.iceberg_sink = IcebergSink(config) if config.output_iceberg else None
        self.cli_display = CLIDisplay(config) if config.output_cli else None
        
        # Stats
        self.messages_processed = 0
        self.start_time = None
        
        self.consumer = None
    
    def _create_consumer(self):
        """Create Kafka consumer."""
        from kafka import KafkaConsumer
        
        max_retries = 10
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                consumer = KafkaConsumer(
                    self.config.kafka_topic,
                    bootstrap_servers=self.config.kafka_bootstrap_servers,
                    group_id=self.config.kafka_group_id,
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    consumer_timeout_ms=1000,
                )
                logger.info(f"✓ Kafka consumer connected: {self.config.kafka_topic}")
                return consumer
                
            except Exception as e:
                logger.warning(f"Kafka connection attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise
    
    def _parse_message(self, message) -> Optional[Dict[str, Any]]:
        """Parse Debezium CDC message."""
        try:
            value = message.value
            
            if 'payload' in value:
                payload = value['payload']
                op = payload.get('op', 'c')
                
                if op == 'd':
                    return None
                
                record = payload.get('after', {})
            else:
                record = value
            
            if not record:
                return None
            
            return record
            
        except Exception as e:
            logger.warning(f"Failed to parse message: {e}")
            return None
    
    def _process_message(self, message):
        """Process a single message by calling the API."""
        record = self._parse_message(message)
        
        if not record:
            return
        
        request_id = record.get('request_id', str(uuid.uuid4()))
        transaction_id = record.get('transaction_id')
        customer_id = record.get('customer_id')
        
        if not customer_id or not transaction_id:
            logger.warning(f"Message missing customer_id or transaction_id")
            return
        
        # Build transaction for API (PRIMITIVES ONLY)
        # Features are fetched from Feast by the API
        transaction = {
            "transaction_id": transaction_id,
            "customer_id": customer_id,
            "amount": float(record.get('amount', 0) or 0),
            "quantity": int(record.get('quantity', 1) or 1),
            "country": record.get('country', 'US') or 'US',
            "device_type": record.get('device_type', 'desktop') or 'desktop',
            "payment_method": record.get('payment_method', 'credit_card') or 'credit_card',
            "category": record.get('category', 'Other') or 'Other',
        }
        
        # Call API
        result = self.api_client.evaluate(
            transaction=transaction,
            model_stages=self.config.stages,
            run_ids=self.config.run_ids,
        )
        
        if not result:
            return
        
        # Get actual label
        actual_label = record.get('actual_label')
        if actual_label is not None:
            actual_label = int(actual_label)
        
        # Store results
        for pred in result.get("predictions", []):
            is_correct = None
            if actual_label is not None and pred.get("fraud_score", -1) >= 0:
                is_correct = 1 if pred.get("is_fraud") == (actual_label == 1) else 0
            
            store_result = {
                "result_id": f"RES_{uuid.uuid4().hex[:12]}",
                "request_id": request_id,
                "transaction_id": transaction_id,
                "customer_id": customer_id,
                "model_name": pred.get("model_name", ""),
                "model_version": pred.get("model_version", ""),
                "model_stage": pred.get("model_stage", ""),
                "run_id": pred.get("run_id"),
                "fraud_probability": pred.get("fraud_score", 0),
                "fraud_prediction": 1 if pred.get("is_fraud") else 0,
                "risk_level": pred.get("risk_level", ""),
                "actual_label": actual_label,
                "is_correct": is_correct,
                "feature_latency_ms": result.get("feature_latency_ms", 0),
                "inference_latency_ms": pred.get("inference_latency_ms", 0),
                "total_latency_ms": result.get("total_latency_ms", 0),
                "feature_source": result.get("feature_source", ""),
                "request_timestamp": record.get('request_timestamp', datetime.now().isoformat()),
                "result_timestamp": result.get("timestamp", datetime.now().isoformat()),
            }
            
            if self.iceberg_sink:
                self.iceberg_sink.add(store_result)
        
        # Update CLI
        if self.cli_display:
            self.cli_display.add_results(
                transaction_id=transaction_id,
                amount=transaction["amount"],
                actual_label=actual_label,
                predictions=result.get("predictions", []),
                feature_latency_ms=result.get("feature_latency_ms", 0),
                total_latency_ms=result.get("total_latency_ms", 0),
            )

        # Track predictions with performance tracker
        if PERFORMANCE_TRACKER_AVAILABLE:
            for pred in result.get("predictions", []):
                if pred.get("fraud_score", -1) >= 0:  # Skip errors
                    try:
                        # Record the prediction
                        record_prediction(
                            model_stage=pred.get("model_stage", "unknown"),
                            transaction_id=transaction_id,
                            fraud_score=pred.get("fraud_score", 0),
                            predicted_fraud=pred.get("is_fraud", False),
                        )

                        # If we have actual label, record the outcome immediately
                        if actual_label is not None:
                            record_outcome(
                                transaction_id=transaction_id,
                                actual_fraud=(actual_label == 1),
                            )
                    except Exception as e:
                        logger.debug(f"Performance tracking failed: {e}")

        self.messages_processed += 1
    
    def run(self):
        """Main consumer loop."""
        self.running = True
        self.start_time = datetime.now()
        
        self.consumer = self._create_consumer()
        
        logger.info("=" * 60)
        logger.info("STREAMING EVALUATION CONSUMER STARTED (via API)")
        logger.info(f"  API: {self.config.api_url}")
        logger.info(f"  Topic: {self.config.kafka_topic}")
        logger.info(f"  Stages: {self.config.stages}")
        logger.info(f"  Run IDs: {self.config.run_ids}")
        logger.info("=" * 60)
        
        try:
            while self.running:
                for message in self.consumer:
                    if not self.running:
                        break
                    self._process_message(message)
                
                if self.iceberg_sink and self.iceberg_sink.should_flush():
                    self.iceberg_sink.flush()
                    
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Graceful shutdown."""
        self.running = False

        logger.info("Shutting down...")

        if self.iceberg_sink:
            self.iceberg_sink.flush()

        if self.consumer:
            self.consumer.close()

        if self.api_client:
            self.api_client.close()

        elapsed = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        rate = self.messages_processed / elapsed if elapsed > 0 else 0

        logger.info(f"✓ Shutdown complete. Processed {self.messages_processed} messages ({rate:.1f}/sec)")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Streaming Model Evaluation via API")
    parser.add_argument("--api-url", default="http://exp-fraud-api:8001", help="Fraud API URL")
    parser.add_argument("--stages", default="Production,Staging", help="Model stages")
    parser.add_argument("--run-ids", default="", help="Specific MLflow run IDs")
    parser.add_argument("--output-cli", action="store_true", default=True, help="Show CLI")
    parser.add_argument("--no-cli", action="store_true", help="Disable CLI")
    parser.add_argument("--output-iceberg", action="store_true", default=True, help="Store to Iceberg")
    parser.add_argument("--no-iceberg", action="store_true", help="Disable Iceberg")
    parser.add_argument("--batch-size", type=int, default=100, help="Iceberg batch size")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")
    
    args = parser.parse_args()
    
    config = Config()
    config.api_url = args.api_url
    config.stages = [s.strip() for s in args.stages.split(",") if s.strip()]
    config.run_ids = [r.strip() for r in args.run_ids.split(",") if r.strip()]
    config.output_cli = args.output_cli and not args.no_cli
    config.output_iceberg = args.output_iceberg and not args.no_iceberg
    config.iceberg_batch_size = args.batch_size
    config.verbose = args.verbose
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    consumer = EvaluationConsumer(config)
    
    def signal_handler(sig, frame):
        logger.info("Signal received, shutting down...")
        consumer.shutdown()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    consumer.run()


if __name__ == "__main__":
    main()


# ═══════════════════════════════════════════════════════════════════════════════
# FEAST FEATURE RETRIEVER
# ═══════════════════════════════════════════════════════════════════════════════