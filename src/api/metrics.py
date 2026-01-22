"""
Prometheus Metrics for Fraud Detection API
===========================================

Simplified metrics module using a registry pattern.
Metric names match Grafana dashboards in /monitoring/grafana/dashboards/
"""

import os
import time
from functools import wraps
from typing import Callable, Dict, Any, List, Optional

try:
    from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST, REGISTRY
    PROMETHEUS_ENABLED = os.getenv("PROMETHEUS_METRICS", "true").lower() == "true"
except ImportError:
    PROMETHEUS_ENABLED = False
    REGISTRY = None


# =============================================================================
# METRIC FACTORY
# =============================================================================

def _get_or_create(metric_class, name: str, desc: str, labels: List[str] = None, **kwargs):
    """Get existing metric or create new one."""
    if not PROMETHEUS_ENABLED:
        return None
    try:
        return metric_class(name, desc, labels or [], **kwargs) if labels else metric_class(name, desc, **kwargs)
    except ValueError:
        return REGISTRY._names_to_collectors.get(name) if REGISTRY else None


# =============================================================================
# METRIC DEFINITIONS (Data-Driven)
# =============================================================================

METRIC_DEFS = {
    # Fraud API Prediction
    "prediction_counter": ("counter", "fraud_predictions_total", "Total predictions", ["status", "risk_level", "variant", "is_fraud"]),
    "prediction_latency": ("histogram", "fraud_prediction_latency_seconds", "Prediction latency", ["variant"], [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]),
    "fraud_score": ("histogram", "fraud_score_distribution", "Fraud score distribution", ["variant"], [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]),
    "ab_traffic": ("counter", "ab_traffic_total", "A/B test traffic", ["experiment", "variant"]),
    "feature_drift": ("gauge", "feature_drift_psi", "Feature drift PSI", ["feature"]),
    "data_quality": ("gauge", "data_quality_passed", "Data quality check passed", ["layer"]),
    "model_info": ("gauge", "model_info", "Model version info", ["model_name", "version", "stage"]),

    # Batch Evaluation
    "eval_f1": ("gauge", "model_evaluation_f1_score", "Model F1 score", ["model_stage", "model_version"]),
    "eval_auc": ("gauge", "model_evaluation_auc_roc", "Model AUC-ROC", ["model_stage", "model_version"]),
    "eval_precision": ("gauge", "model_evaluation_precision", "Model precision", ["model_stage", "model_version"]),
    "eval_recall": ("gauge", "model_evaluation_recall", "Model recall", ["model_stage", "model_version"]),
    "eval_accuracy": ("gauge", "model_evaluation_accuracy", "Model accuracy", ["model_stage", "model_version"]),

    # Streaming Evaluation
    "stream_predictions": ("counter", "streaming_evaluation_predictions_total", "Streaming predictions", ["model_stage", "is_correct"]),
    "stream_latency": ("histogram", "streaming_evaluation_latency_seconds", "Streaming latency", ["model_stage"], [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]),
    "stream_accuracy": ("gauge", "streaming_evaluation_accuracy", "Streaming accuracy", ["model_stage"]),
    "stream_agreement": ("counter", "streaming_evaluation_model_agreement_total", "Model agreement count", []),
    "stream_disagreement": ("counter", "streaming_evaluation_model_disagreement_total", "Model disagreement count", []),

    # Feast
    "feast_latency": ("histogram", "feast_feature_retrieval_latency_seconds", "Feast latency", ["source"], [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25]),
    "feast_entities": ("gauge", "feast_online_store_entities_total", "Feast entities count", []),
    "feast_success_rate": ("gauge", "feast_feature_retrieval_success_rate", "Feast success rate", []),

    # Performance Monitoring
    "perf_f1": ("gauge", "model_performance_f1", "Rolling F1", ["model_stage", "window"]),
    "perf_accuracy": ("gauge", "model_performance_accuracy", "Rolling accuracy", ["model_stage", "window"]),
    "perf_precision": ("gauge", "model_performance_precision", "Rolling precision", ["model_stage", "window"]),
    "perf_recall": ("gauge", "model_performance_recall", "Rolling recall", ["model_stage", "window"]),
    "perf_auc": ("gauge", "model_performance_auc_roc", "Rolling AUC-ROC", ["model_stage", "window"]),
    "perf_tp": ("gauge", "model_performance_true_positives", "True positives", ["model_stage", "window"]),
    "perf_fp": ("gauge", "model_performance_false_positives", "False positives", ["model_stage", "window"]),
    "perf_tn": ("gauge", "model_performance_true_negatives", "True negatives", ["model_stage", "window"]),
    "perf_fn": ("gauge", "model_performance_false_negatives", "False negatives", ["model_stage", "window"]),
    "perf_alert": ("gauge", "model_performance_alert_active", "Performance alert active", ["model_stage", "alert_type"]),
    "predictions_with_outcomes": ("counter", "model_predictions_with_outcomes_total", "Predictions with outcomes", ["model_stage", "outcome"]),

    # Shadow Mode
    "shadow_enabled": ("gauge", "shadow_mode_enabled", "Shadow mode enabled", []),
    "shadow_predictions": ("counter", "shadow_predictions_total", "Shadow predictions", ["agreement"]),
    "shadow_agreement_rate": ("gauge", "shadow_mode_agreement_rate", "Shadow agreement rate", []),
    "shadow_score_diff": ("histogram", "shadow_score_difference", "Shadow score difference", [], [0.01, 0.02, 0.05, 0.1, 0.15, 0.2, 0.3, 0.5]),

    # Auto-Retrain
    "retrain_triggered": ("gauge", "auto_retrain_last_triggered_timestamp", "Last retrain timestamp", []),
    "retrain_cooldown": ("gauge", "auto_retrain_cooldown_remaining_hours", "Retrain cooldown hours", []),
    "retrain_triggers": ("counter", "auto_retrain_triggers_total", "Retrain triggers", ["trigger_reason"]),
}


# =============================================================================
# METRIC REGISTRY
# =============================================================================

class MetricRegistry:
    """Central registry for all metrics."""

    def __init__(self):
        self._metrics: Dict[str, Any] = {}
        self._init_metrics()

    def _init_metrics(self):
        """Initialize all metrics from definitions."""
        for key, defn in METRIC_DEFS.items():
            metric_type, name, desc, labels = defn[0], defn[1], defn[2], defn[3]
            buckets = defn[4] if len(defn) > 4 else None

            if metric_type == "counter":
                self._metrics[key] = _get_or_create(Counter, name, desc, labels)
            elif metric_type == "gauge":
                self._metrics[key] = _get_or_create(Gauge, name, desc, labels)
            elif metric_type == "histogram":
                self._metrics[key] = _get_or_create(Histogram, name, desc, labels, buckets=buckets) if buckets else _get_or_create(Histogram, name, desc, labels)

    def get(self, key: str):
        """Get metric by key, returns None if not available."""
        return self._metrics.get(key)

    def inc(self, key: str, **labels):
        """Increment counter."""
        m = self.get(key)
        if m:
            m.labels(**labels).inc() if labels else m.inc()

    def observe(self, key: str, value: float, **labels):
        """Observe histogram value."""
        m = self.get(key)
        if m:
            m.labels(**labels).observe(value) if labels else m.observe(value)

    def set(self, key: str, value: float, **labels):
        """Set gauge value."""
        m = self.get(key)
        if m:
            m.labels(**labels).set(value) if labels else m.set(value)


# Singleton
_registry = MetricRegistry() if PROMETHEUS_ENABLED else None

# Rolling stats for success rate calculations
_feast_stats = {"success": 0, "total": 0}
_streaming_stats = {"Production": {"correct": 0, "total": 0}, "Staging": {"correct": 0, "total": 0}}


# =============================================================================
# PUBLIC API
# =============================================================================

def track_prediction(status: str, risk_level: str, variant: str, is_fraud: bool, fraud_score: float, latency: float):
    """Track prediction metrics."""
    if not _registry:
        return
    _registry.inc("prediction_counter", status=status, risk_level=risk_level, variant=variant, is_fraud=str(is_fraud).lower())
    _registry.observe("prediction_latency", latency, variant=variant)
    _registry.observe("fraud_score", fraud_score, variant=variant)


def track_feature_fetch(source: str, latency: float, status: str = "success"):
    """Track Feast feature fetching."""
    if not _registry:
        return
    _registry.observe("feast_latency", latency, source=source)
    _feast_stats["total"] += 1
    if status == "success":
        _feast_stats["success"] += 1
    if _feast_stats["total"] > 0:
        _registry.set("feast_success_rate", _feast_stats["success"] / _feast_stats["total"])


def track_ab_traffic(experiment: str, variant: str):
    """Track A/B test traffic."""
    if _registry:
        _registry.inc("ab_traffic", experiment=experiment, variant=variant)


def update_batch_evaluation_metrics(results: list):
    """Update batch evaluation metrics."""
    if not _registry:
        return
    for r in results:
        stage, version = r.get("model_stage", "unknown"), str(r.get("model_version", "unknown"))
        _registry.set("eval_f1", float(r.get("f1_score") or 0), model_stage=stage, model_version=version)
        _registry.set("eval_auc", float(r.get("auc_roc") or 0), model_stage=stage, model_version=version)
        _registry.set("eval_precision", float(r.get("precision_score") or 0), model_stage=stage, model_version=version)
        _registry.set("eval_recall", float(r.get("recall") or 0), model_stage=stage, model_version=version)
        _registry.set("eval_accuracy", float(r.get("accuracy") or 0), model_stage=stage, model_version=version)


def track_streaming_prediction(model_stage: str, is_correct: bool, latency: float,
                               production_prediction: bool = None, staging_prediction: bool = None):
    """Track streaming evaluation prediction."""
    if not _registry:
        return
    _registry.inc("stream_predictions", model_stage=model_stage, is_correct=str(is_correct).lower())
    _registry.observe("stream_latency", latency, model_stage=model_stage)

    _streaming_stats[model_stage]["total"] += 1
    if is_correct:
        _streaming_stats[model_stage]["correct"] += 1
    if _streaming_stats[model_stage]["total"] > 0:
        acc = _streaming_stats[model_stage]["correct"] / _streaming_stats[model_stage]["total"]
        _registry.set("stream_accuracy", acc, model_stage=model_stage)

    if production_prediction is not None and staging_prediction is not None:
        if production_prediction == staging_prediction:
            _registry.inc("stream_agreement")
        else:
            _registry.inc("stream_disagreement")


def update_performance_metrics(model_stage: str, window: str, accuracy: float, precision: float,
                               recall: float, f1: float, auc_roc: float = None,
                               tp: int = 0, fp: int = 0, tn: int = 0, fn: int = 0):
    """Update rolling performance metrics."""
    if not _registry:
        return
    labels = {"model_stage": model_stage, "window": window}
    _registry.set("perf_accuracy", accuracy, **labels)
    _registry.set("perf_precision", precision, **labels)
    _registry.set("perf_recall", recall, **labels)
    _registry.set("perf_f1", f1, **labels)
    if auc_roc is not None:
        _registry.set("perf_auc", auc_roc, **labels)
    _registry.set("perf_tp", tp, **labels)
    _registry.set("perf_fp", fp, **labels)
    _registry.set("perf_tn", tn, **labels)
    _registry.set("perf_fn", fn, **labels)


def set_performance_alert(model_stage: str, alert_type: str, active: bool):
    """Set performance alert status."""
    if _registry:
        _registry.set("perf_alert", 1 if active else 0, model_stage=model_stage, alert_type=alert_type)


def track_prediction_outcome(model_stage: str, predicted: bool, actual: bool):
    """Track prediction with actual outcome."""
    if not _registry:
        return
    if predicted == actual:
        outcome = "correct"
    elif predicted and not actual:
        outcome = "false_positive"
    else:
        outcome = "false_negative"
    _registry.inc("predictions_with_outcomes", model_stage=model_stage, outcome=outcome)


def track_shadow_prediction(agreement: bool, prod_score: float, shadow_score: float):
    """Track shadow mode prediction."""
    if not _registry:
        return
    _registry.inc("shadow_predictions", agreement="agree" if agreement else "disagree")
    _registry.observe("shadow_score_diff", abs(prod_score - shadow_score))


def update_shadow_agreement_rate(rate: float):
    if _registry:
        _registry.set("shadow_agreement_rate", rate)


def set_shadow_mode_enabled(enabled: bool):
    if _registry:
        _registry.set("shadow_enabled", 1 if enabled else 0)


def track_auto_retrain_trigger(trigger_reason: str):
    if not _registry:
        return
    _registry.inc("retrain_triggers", trigger_reason=trigger_reason)
    _registry.set("retrain_triggered", time.time())


def update_retrain_cooldown(hours_remaining: float):
    if _registry:
        _registry.set("retrain_cooldown", hours_remaining)


def update_feast_entities_count(count: int):
    if _registry:
        _registry.set("feast_entities", count)


def get_metrics_response():
    """Generate Prometheus metrics response."""
    if not PROMETHEUS_ENABLED:
        return "# Prometheus metrics disabled", "text/plain"
    return generate_latest(), CONTENT_TYPE_LATEST


def timed(metric_name: str = "default"):
    """Decorator to time function execution."""
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            try:
                return func(*args, **kwargs)
            finally:
                if _registry:
                    _registry.observe("feast_latency", time.time() - start, source=metric_name)
        return wrapper
    return decorator


