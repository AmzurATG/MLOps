"""
Real-time Performance Tracker
═══════════════════════════════════════════════════════════════════════════════

Tracks model performance in real-time by comparing predictions to actual outcomes.
Maintains rolling windows (1h, 24h, 7d) for performance metrics.

Usage:
    from src.api.performance_tracker import PerformanceTracker, get_performance_tracker

    tracker = get_performance_tracker()

    # Record a prediction
    tracker.record_prediction(
        model_stage="Production",
        transaction_id="TXN123",
        fraud_score=0.85,
        predicted_fraud=True,
    )

    # Later, when outcome is known
    tracker.record_outcome(
        transaction_id="TXN123",
        actual_fraud=True,
    )

    # Get current metrics
    metrics = tracker.get_metrics("Production", window="1h")
"""

import os
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import defaultdict
import json

try:
    from sklearn.metrics import (
        accuracy_score,
        precision_score,
        recall_score,
        f1_score,
        roc_auc_score,
        confusion_matrix,
    )
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

from src.api.metrics import (
    update_performance_metrics,
    set_performance_alert,
    track_prediction_outcome,
    PROMETHEUS_ENABLED,
)


@dataclass
class PredictionRecord:
    """A single prediction with optional outcome."""
    transaction_id: str
    model_stage: str
    fraud_score: float
    predicted_fraud: bool
    actual_fraud: Optional[bool] = None
    prediction_time: datetime = field(default_factory=datetime.now)
    outcome_time: Optional[datetime] = None


@dataclass
class PerformanceMetrics:
    """Performance metrics for a model."""
    model_stage: str
    window: str  # "1h", "24h", "7d"
    accuracy: float = 0.0
    precision: float = 0.0
    recall: float = 0.0
    f1: float = 0.0
    auc_roc: Optional[float] = None
    true_positives: int = 0
    false_positives: int = 0
    true_negatives: int = 0
    false_negatives: int = 0
    total_predictions: int = 0
    predictions_with_outcomes: int = 0
    calculated_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict:
        return {
            "model_stage": self.model_stage,
            "window": self.window,
            "accuracy": round(self.accuracy, 4),
            "precision": round(self.precision, 4),
            "recall": round(self.recall, 4),
            "f1": round(self.f1, 4),
            "auc_roc": round(self.auc_roc, 4) if self.auc_roc else None,
            "true_positives": self.true_positives,
            "false_positives": self.false_positives,
            "true_negatives": self.true_negatives,
            "false_negatives": self.false_negatives,
            "total_predictions": self.total_predictions,
            "predictions_with_outcomes": self.predictions_with_outcomes,
            "calculated_at": self.calculated_at.isoformat(),
        }


class PerformanceTracker:
    """
    Thread-safe performance tracker with rolling windows.

    Stores predictions in memory and calculates metrics when outcomes arrive.
    Supports multiple model stages (Production, Staging) and time windows.
    """

    # Time windows in hours
    WINDOWS = {
        "1h": 1,
        "24h": 24,
        "7d": 24 * 7,
    }

    # Alert thresholds
    F1_ALERT_THRESHOLD = float(os.getenv("PERFORMANCE_F1_THRESHOLD", "0.7"))
    ACCURACY_ALERT_THRESHOLD = float(os.getenv("PERFORMANCE_ACCURACY_THRESHOLD", "0.8"))
    MIN_SAMPLES_FOR_ALERT = int(os.getenv("PERFORMANCE_MIN_SAMPLES", "50"))

    def __init__(self, max_predictions: int = 100000, cleanup_interval: int = 3600):
        """
        Initialize tracker.

        Args:
            max_predictions: Maximum predictions to keep in memory
            cleanup_interval: Cleanup interval in seconds (default 1 hour)
        """
        self.max_predictions = max_predictions
        self.cleanup_interval = cleanup_interval

        # Thread-safe storage
        self._lock = threading.RLock()
        self._predictions: Dict[str, PredictionRecord] = {}  # transaction_id -> record
        self._predictions_by_stage: Dict[str, List[str]] = defaultdict(list)  # stage -> [tx_ids]

        # Cache for metrics
        self._metrics_cache: Dict[str, PerformanceMetrics] = {}
        self._cache_ttl = 60  # seconds
        self._last_cache_update: Dict[str, float] = {}

        # Start cleanup thread
        self._cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self._cleanup_thread.start()

        print(f"[PerformanceTracker] Initialized with thresholds: F1={self.F1_ALERT_THRESHOLD}, Accuracy={self.ACCURACY_ALERT_THRESHOLD}")

    def record_prediction(
        self,
        model_stage: str,
        transaction_id: str,
        fraud_score: float,
        predicted_fraud: bool,
    ):
        """
        Record a new prediction.

        Args:
            model_stage: Model stage (Production, Staging)
            transaction_id: Unique transaction ID
            fraud_score: Fraud probability score
            predicted_fraud: Whether model predicted fraud
        """
        with self._lock:
            record = PredictionRecord(
                transaction_id=transaction_id,
                model_stage=model_stage,
                fraud_score=fraud_score,
                predicted_fraud=predicted_fraud,
            )

            self._predictions[transaction_id] = record
            self._predictions_by_stage[model_stage].append(transaction_id)

    def record_outcome(
        self,
        transaction_id: str,
        actual_fraud: bool,
    ) -> bool:
        """
        Record the actual outcome for a prediction.

        Args:
            transaction_id: Transaction ID to update
            actual_fraud: Whether transaction was actually fraud

        Returns:
            True if prediction was found and updated
        """
        with self._lock:
            if transaction_id not in self._predictions:
                return False

            record = self._predictions[transaction_id]
            record.actual_fraud = actual_fraud
            record.outcome_time = datetime.now()

            # Track in Prometheus
            track_prediction_outcome(
                model_stage=record.model_stage,
                predicted=record.predicted_fraud,
                actual=actual_fraud,
            )

            # Invalidate cache
            self._invalidate_cache(record.model_stage)

            return True

    def get_metrics(
        self,
        model_stage: str,
        window: str = "24h",
        force_refresh: bool = False,
    ) -> PerformanceMetrics:
        """
        Get performance metrics for a model stage and time window.

        Args:
            model_stage: Model stage (Production, Staging)
            window: Time window ("1h", "24h", "7d")
            force_refresh: Force recalculation even if cached

        Returns:
            PerformanceMetrics object
        """
        cache_key = f"{model_stage}_{window}"

        # Check cache
        if not force_refresh:
            with self._lock:
                if cache_key in self._metrics_cache:
                    last_update = self._last_cache_update.get(cache_key, 0)
                    if time.time() - last_update < self._cache_ttl:
                        return self._metrics_cache[cache_key]

        # Calculate fresh metrics
        metrics = self._calculate_metrics(model_stage, window)

        # Update cache
        with self._lock:
            self._metrics_cache[cache_key] = metrics
            self._last_cache_update[cache_key] = time.time()

        # Update Prometheus
        update_performance_metrics(
            model_stage=model_stage,
            window=window,
            accuracy=metrics.accuracy,
            precision=metrics.precision,
            recall=metrics.recall,
            f1=metrics.f1,
            auc_roc=metrics.auc_roc,
            tp=metrics.true_positives,
            fp=metrics.false_positives,
            tn=metrics.true_negatives,
            fn=metrics.false_negatives,
        )

        # Check alerts
        self._check_alerts(metrics)

        return metrics

    def _calculate_metrics(
        self,
        model_stage: str,
        window: str,
    ) -> PerformanceMetrics:
        """Calculate metrics for a model stage and time window."""
        cutoff = datetime.now() - timedelta(hours=self.WINDOWS.get(window, 24))

        y_true = []
        y_pred = []
        y_scores = []

        with self._lock:
            tx_ids = self._predictions_by_stage.get(model_stage, [])
            total_predictions = 0

            for tx_id in tx_ids:
                record = self._predictions.get(tx_id)
                if record is None:
                    continue

                # Check time window
                if record.prediction_time < cutoff:
                    continue

                total_predictions += 1

                # Only include predictions with outcomes
                if record.actual_fraud is not None:
                    y_true.append(1 if record.actual_fraud else 0)
                    y_pred.append(1 if record.predicted_fraud else 0)
                    y_scores.append(record.fraud_score)

        metrics = PerformanceMetrics(
            model_stage=model_stage,
            window=window,
            total_predictions=total_predictions,
            predictions_with_outcomes=len(y_true),
        )

        if len(y_true) < 2:
            return metrics

        if not SKLEARN_AVAILABLE:
            # Manual calculation without sklearn
            tp = sum(1 for yt, yp in zip(y_true, y_pred) if yt == 1 and yp == 1)
            fp = sum(1 for yt, yp in zip(y_true, y_pred) if yt == 0 and yp == 1)
            tn = sum(1 for yt, yp in zip(y_true, y_pred) if yt == 0 and yp == 0)
            fn = sum(1 for yt, yp in zip(y_true, y_pred) if yt == 1 and yp == 0)

            metrics.true_positives = tp
            metrics.false_positives = fp
            metrics.true_negatives = tn
            metrics.false_negatives = fn

            total = tp + fp + tn + fn
            metrics.accuracy = (tp + tn) / total if total > 0 else 0
            metrics.precision = tp / (tp + fp) if (tp + fp) > 0 else 0
            metrics.recall = tp / (tp + fn) if (tp + fn) > 0 else 0

            if metrics.precision + metrics.recall > 0:
                metrics.f1 = 2 * (metrics.precision * metrics.recall) / (metrics.precision + metrics.recall)

            return metrics

        # Calculate with sklearn
        try:
            metrics.accuracy = accuracy_score(y_true, y_pred)
            metrics.precision = precision_score(y_true, y_pred, zero_division=0)
            metrics.recall = recall_score(y_true, y_pred, zero_division=0)
            metrics.f1 = f1_score(y_true, y_pred, zero_division=0)

            # AUC-ROC needs both classes
            if len(set(y_true)) > 1:
                metrics.auc_roc = roc_auc_score(y_true, y_scores)

            # Confusion matrix
            cm = confusion_matrix(y_true, y_pred)
            if cm.shape == (2, 2):
                metrics.true_negatives = int(cm[0, 0])
                metrics.false_positives = int(cm[0, 1])
                metrics.false_negatives = int(cm[1, 0])
                metrics.true_positives = int(cm[1, 1])

        except Exception as e:
            print(f"[PerformanceTracker] Metric calculation error: {e}")

        return metrics

    def _check_alerts(self, metrics: PerformanceMetrics):
        """Check if performance alerts should be triggered."""
        if metrics.predictions_with_outcomes < self.MIN_SAMPLES_FOR_ALERT:
            # Not enough data for alerts
            set_performance_alert(metrics.model_stage, "f1_low", False)
            set_performance_alert(metrics.model_stage, "accuracy_low", False)
            return

        # F1 alert
        f1_alert = metrics.f1 < self.F1_ALERT_THRESHOLD
        set_performance_alert(metrics.model_stage, "f1_low", f1_alert)

        # Accuracy alert
        accuracy_alert = metrics.accuracy < self.ACCURACY_ALERT_THRESHOLD
        set_performance_alert(metrics.model_stage, "accuracy_low", accuracy_alert)

        if f1_alert or accuracy_alert:
            print(f"[PerformanceTracker] ALERT: {metrics.model_stage} - F1={metrics.f1:.3f}, Accuracy={metrics.accuracy:.3f}")

    def _invalidate_cache(self, model_stage: str):
        """Invalidate cache for a model stage."""
        for window in self.WINDOWS:
            cache_key = f"{model_stage}_{window}"
            self._last_cache_update[cache_key] = 0

    def _cleanup_loop(self):
        """Background cleanup of old predictions."""
        while True:
            time.sleep(self.cleanup_interval)
            self._cleanup_old_predictions()

    def _cleanup_old_predictions(self):
        """Remove predictions older than 7 days."""
        cutoff = datetime.now() - timedelta(days=7)
        removed = 0

        with self._lock:
            to_remove = []

            for tx_id, record in self._predictions.items():
                if record.prediction_time < cutoff:
                    to_remove.append(tx_id)

            for tx_id in to_remove:
                record = self._predictions.pop(tx_id, None)
                if record:
                    try:
                        self._predictions_by_stage[record.model_stage].remove(tx_id)
                    except ValueError:
                        pass
                    removed += 1

            # Also enforce max predictions limit
            if len(self._predictions) > self.max_predictions:
                # Remove oldest predictions
                sorted_predictions = sorted(
                    self._predictions.items(),
                    key=lambda x: x[1].prediction_time
                )
                excess = len(self._predictions) - self.max_predictions
                for tx_id, record in sorted_predictions[:excess]:
                    del self._predictions[tx_id]
                    try:
                        self._predictions_by_stage[record.model_stage].remove(tx_id)
                    except ValueError:
                        pass
                    removed += 1

        if removed > 0:
            print(f"[PerformanceTracker] Cleaned up {removed} old predictions")

    def get_all_metrics(self) -> Dict[str, Dict[str, PerformanceMetrics]]:
        """Get metrics for all stages and windows."""
        result = {}

        with self._lock:
            stages = list(self._predictions_by_stage.keys())

        for stage in stages:
            result[stage] = {}
            for window in self.WINDOWS:
                result[stage][window] = self.get_metrics(stage, window)

        return result

    def get_summary(self) -> Dict:
        """Get summary of tracker state."""
        with self._lock:
            total_predictions = len(self._predictions)
            predictions_with_outcomes = sum(
                1 for r in self._predictions.values() if r.actual_fraud is not None
            )
            stages = {
                stage: len(ids) for stage, ids in self._predictions_by_stage.items()
            }

        return {
            "total_predictions": total_predictions,
            "predictions_with_outcomes": predictions_with_outcomes,
            "outcome_rate": predictions_with_outcomes / total_predictions if total_predictions > 0 else 0,
            "stages": stages,
            "thresholds": {
                "f1": self.F1_ALERT_THRESHOLD,
                "accuracy": self.ACCURACY_ALERT_THRESHOLD,
                "min_samples": self.MIN_SAMPLES_FOR_ALERT,
            },
        }


# Global singleton
_tracker_instance: Optional[PerformanceTracker] = None
_tracker_lock = threading.Lock()


def get_performance_tracker() -> PerformanceTracker:
    """Get or create the global performance tracker instance."""
    global _tracker_instance

    if _tracker_instance is None:
        with _tracker_lock:
            if _tracker_instance is None:
                _tracker_instance = PerformanceTracker()

    return _tracker_instance


# Convenience functions
def record_prediction(
    model_stage: str,
    transaction_id: str,
    fraud_score: float,
    predicted_fraud: bool,
):
    """Record a prediction."""
    get_performance_tracker().record_prediction(
        model_stage=model_stage,
        transaction_id=transaction_id,
        fraud_score=fraud_score,
        predicted_fraud=predicted_fraud,
    )


def record_outcome(transaction_id: str, actual_fraud: bool) -> bool:
    """Record an outcome."""
    return get_performance_tracker().record_outcome(
        transaction_id=transaction_id,
        actual_fraud=actual_fraud,
    )


def get_metrics(model_stage: str, window: str = "24h") -> PerformanceMetrics:
    """Get performance metrics."""
    return get_performance_tracker().get_metrics(model_stage, window)
