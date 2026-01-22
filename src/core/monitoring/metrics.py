"""
Core Monitoring Metrics
=======================

Unified Prometheus metrics utilities for MLOps, CVOps, and LLMOps.

This module provides:
1. Safe metric creation helpers (avoid duplicate registration errors)
2. Standard metric decorators
3. Common metric definitions
4. MetricRegistry class for domain-specific metrics

Usage:
    from src.core.monitoring import (
        get_or_create_counter,
        get_or_create_histogram,
        track_execution,
        MetricRegistry,
    )
"""

import os
import time
from functools import wraps
from typing import Callable, Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)

try:
    from prometheus_client import Counter, Histogram, Gauge, Summary, generate_latest, CONTENT_TYPE_LATEST, REGISTRY
    PROMETHEUS_ENABLED = os.getenv("PROMETHEUS_METRICS", "true").lower() == "true"
except ImportError:
    PROMETHEUS_ENABLED = False
    REGISTRY = None
    Counter = None
    Histogram = None
    Gauge = None
    Summary = None


# =============================================================================
# SAFE METRIC CREATION HELPERS
# =============================================================================

def get_or_create_counter(
    name: str,
    description: str,
    labelnames: List[str],
) -> Optional[Counter]:
    """
    Get existing counter or create new one.
    Safely handles duplicate registration errors from Prometheus.
    """
    if not PROMETHEUS_ENABLED or Counter is None:
        return None
    try:
        return Counter(name, description, labelnames)
    except ValueError:
        return REGISTRY._names_to_collectors.get(name) if REGISTRY else None


def get_or_create_histogram(
    name: str,
    description: str,
    labelnames: List[str],
    buckets: Optional[List[float]] = None,
) -> Optional[Histogram]:
    """
    Get existing histogram or create new one.
    Safely handles duplicate registration errors from Prometheus.
    """
    if not PROMETHEUS_ENABLED or Histogram is None:
        return None
    try:
        if buckets:
            return Histogram(name, description, labelnames, buckets=buckets)
        return Histogram(name, description, labelnames)
    except ValueError:
        return REGISTRY._names_to_collectors.get(name) if REGISTRY else None


def get_or_create_gauge(
    name: str,
    description: str,
    labelnames: List[str],
) -> Optional[Gauge]:
    """
    Get existing gauge or create new one.
    Safely handles duplicate registration errors from Prometheus.
    """
    if not PROMETHEUS_ENABLED or Gauge is None:
        return None
    try:
        return Gauge(name, description, labelnames)
    except ValueError:
        return REGISTRY._names_to_collectors.get(name) if REGISTRY else None


def get_or_create_summary(
    name: str,
    description: str,
    labelnames: List[str],
) -> Optional[Summary]:
    """
    Get existing summary or create new one.
    Safely handles duplicate registration errors from Prometheus.
    """
    if not PROMETHEUS_ENABLED or Summary is None:
        return None
    try:
        return Summary(name, description, labelnames)
    except ValueError:
        return REGISTRY._names_to_collectors.get(name) if REGISTRY else None


def get_or_create_metric(metric_class, name: str, desc: str, labels: List[str] = None, **kwargs):
    """
    Generic helper to get existing metric or create new one.
    """
    if not PROMETHEUS_ENABLED or metric_class is None:
        return None
    try:
        return metric_class(name, desc, labels or [], **kwargs) if labels else metric_class(name, desc, **kwargs)
    except ValueError:
        return REGISTRY._names_to_collectors.get(name) if REGISTRY else None


# =============================================================================
# STANDARD BUCKETS
# =============================================================================

# Latency buckets (in seconds)
LATENCY_BUCKETS_FAST = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
LATENCY_BUCKETS_MEDIUM = [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
LATENCY_BUCKETS_SLOW = [0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0]

# Score/probability buckets
PROBABILITY_BUCKETS = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]

# Size buckets (in bytes)
SIZE_BUCKETS_SMALL = [1e3, 5e3, 1e4, 5e4, 1e5, 5e5, 1e6]  # 1KB to 1MB
SIZE_BUCKETS_LARGE = [1e5, 5e5, 1e6, 5e6, 1e7, 5e7, 1e8]  # 100KB to 100MB

# Count buckets
COUNT_BUCKETS = [0, 1, 2, 5, 10, 20, 50, 100, 200, 500]


# =============================================================================
# DECORATORS
# =============================================================================

def track_execution(
    counter: Optional[Counter] = None,
    histogram: Optional[Histogram] = None,
    labels: Optional[Dict[str, str]] = None,
):
    """
    Decorator to track function execution.
    Records execution count (via counter) and duration (via histogram).

    Usage:
        @track_execution(counter=my_counter, histogram=my_histogram, labels={"operation": "process"})
        def my_function():
            ...
    """
    labels = labels or {}

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            status = "success"
            try:
                result = func(*args, **kwargs)
                return result
            except Exception:
                status = "error"
                raise
            finally:
                duration = time.time() - start_time
                label_values = {**labels, "status": status}

                if counter is not None:
                    try:
                        counter.labels(**label_values).inc()
                    except Exception as e:
                        logger.debug(f"Failed to increment counter: {e}")

                if histogram is not None:
                    try:
                        hist_labels = {k: v for k, v in label_values.items() if k != "status"}
                        histogram.labels(**hist_labels).observe(duration)
                    except Exception as e:
                        logger.debug(f"Failed to observe histogram: {e}")

        return wrapper
    return decorator


def track_async_execution(
    counter: Optional[Counter] = None,
    histogram: Optional[Histogram] = None,
    labels: Optional[Dict[str, str]] = None,
):
    """Async version of track_execution decorator."""
    labels = labels or {}

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            status = "success"
            try:
                result = await func(*args, **kwargs)
                return result
            except Exception:
                status = "error"
                raise
            finally:
                duration = time.time() - start_time
                label_values = {**labels, "status": status}

                if counter is not None:
                    try:
                        counter.labels(**label_values).inc()
                    except Exception:
                        pass

                if histogram is not None:
                    try:
                        hist_labels = {k: v for k, v in label_values.items() if k != "status"}
                        histogram.labels(**hist_labels).observe(duration)
                    except Exception:
                        pass

        return wrapper
    return decorator


def timed(metric_key: str, registry: "MetricRegistry" = None, **default_labels):
    """
    Decorator to time function execution.

    Usage:
        @timed("my_latency", registry=my_registry, endpoint="api")
        def my_function():
            ...
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            try:
                return func(*args, **kwargs)
            finally:
                if registry:
                    registry.observe(metric_key, time.time() - start, **default_labels)
        return wrapper
    return decorator


# =============================================================================
# COMMON METRICS (Shared across all domains)
# =============================================================================

# Pipeline Execution Metrics
pipeline_execution_counter = get_or_create_counter(
    'pipeline_executions_total',
    'Total number of pipeline executions',
    ['pipeline', 'stage', 'status']
)

pipeline_execution_duration = get_or_create_histogram(
    'pipeline_execution_duration_seconds',
    'Pipeline execution duration in seconds',
    ['pipeline', 'stage'],
    buckets=LATENCY_BUCKETS_SLOW
)

# Data Quality Metrics
data_quality_check = get_or_create_counter(
    'data_quality_checks_total',
    'Total data quality checks',
    ['check_type', 'layer', 'status']
)

data_quality_score = get_or_create_gauge(
    'data_quality_score',
    'Current data quality score (0-1)',
    ['layer', 'table']
)

row_count_gauge = get_or_create_gauge(
    'table_row_count',
    'Current row count in table',
    ['layer', 'table']
)

# Feature Store Metrics
feature_freshness_seconds = get_or_create_gauge(
    'feature_freshness_seconds',
    'Time since last feature update',
    ['feature_view']
)

feature_null_rate = get_or_create_gauge(
    'feature_null_rate',
    'Percentage of null values in feature',
    ['feature_name', 'layer']
)

# Drift Metrics
feature_drift_psi = get_or_create_gauge(
    'feature_drift_psi',
    'Population Stability Index for feature',
    ['feature']
)

dataset_drift_detected = get_or_create_gauge(
    'dataset_drift_detected',
    'Whether dataset drift is detected (0 or 1)',
    ['model_name']
)

# Error Metrics
error_counter = get_or_create_counter(
    'errors_total',
    'Total errors',
    ['domain', 'component', 'error_type']
)


# =============================================================================
# METRIC REGISTRY CLASS
# =============================================================================

class MetricRegistry:
    """
    Base registry for Prometheus metrics.
    Provides a unified interface for metric operations.

    Usage:
        registry = MetricRegistry()
        registry.register("requests", "counter", "app_requests_total", "Total requests", ["status"])
        registry.inc("requests", status="success")
    """

    def __init__(self):
        self._metrics: Dict[str, Any] = {}

    def register(
        self,
        key: str,
        metric_type: str,
        name: str,
        description: str,
        labels: List[str] = None,
        buckets: List[float] = None
    ):
        """Register a new metric."""
        if not PROMETHEUS_ENABLED:
            return

        if metric_type == "counter":
            self._metrics[key] = get_or_create_counter(name, description, labels or [])
        elif metric_type == "gauge":
            self._metrics[key] = get_or_create_gauge(name, description, labels or [])
        elif metric_type == "histogram":
            self._metrics[key] = get_or_create_histogram(name, description, labels or [], buckets=buckets)
        elif metric_type == "summary":
            self._metrics[key] = get_or_create_summary(name, description, labels or [])

    def get(self, key: str):
        """Get metric by key, returns None if not available."""
        return self._metrics.get(key)

    def inc(self, key: str, value: float = 1, **labels):
        """Increment counter."""
        m = self.get(key)
        if m:
            if labels:
                m.labels(**labels).inc(value)
            else:
                m.inc(value)

    def observe(self, key: str, value: float, **labels):
        """Observe histogram value."""
        m = self.get(key)
        if m:
            if labels:
                m.labels(**labels).observe(value)
            else:
                m.observe(value)

    def set(self, key: str, value: float, **labels):
        """Set gauge value."""
        m = self.get(key)
        if m:
            if labels:
                m.labels(**labels).set(value)
            else:
                m.set(value)


# =============================================================================
# MIXIN CLASS FOR MONITORING
# =============================================================================

class MonitoringMixin:
    """
    Mixin class providing monitoring capabilities.

    Usage:
        class MyProcessor(MonitoringMixin):
            def __init__(self):
                super().__init__()
                self._init_metrics("my_processor")

            def process(self, data):
                with self.track_operation("process"):
                    ...
    """

    _metrics_initialized: bool = False
    _component_name: str = "unknown"

    def _init_metrics(self, component_name: str):
        """Initialize component-specific metrics."""
        self._component_name = component_name
        self._operation_counter = get_or_create_counter(
            f'{component_name}_operations_total',
            f'Total operations for {component_name}',
            ['operation', 'status']
        )
        self._operation_duration = get_or_create_histogram(
            f'{component_name}_operation_duration_seconds',
            f'Operation duration for {component_name}',
            ['operation'],
            buckets=LATENCY_BUCKETS_MEDIUM
        )
        self._metrics_initialized = True

    def track_operation(self, operation: str):
        """Context manager to track an operation."""
        return OperationTracker(
            self._operation_counter,
            self._operation_duration,
            operation,
        )

    def record_error(self, error_type: str, error: Exception):
        """Record an error occurrence."""
        if error_counter:
            error_counter.labels(
                domain=self._component_name,
                component=self._component_name,
                error_type=error_type,
            ).inc()


class OperationTracker:
    """Context manager for tracking operation metrics."""

    def __init__(
        self,
        counter: Optional[Counter],
        histogram: Optional[Histogram],
        operation: str,
    ):
        self.counter = counter
        self.histogram = histogram
        self.operation = operation
        self.start_time = None
        self.status = "success"

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time

        if exc_type is not None:
            self.status = "error"

        if self.counter:
            try:
                self.counter.labels(operation=self.operation, status=self.status).inc()
            except Exception:
                pass

        if self.histogram:
            try:
                self.histogram.labels(operation=self.operation).observe(duration)
            except Exception:
                pass

        return False


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def safe_observe(histogram: Optional[Histogram], value: float, **labels):
    """Safely observe a histogram value."""
    if histogram is not None:
        try:
            histogram.labels(**labels).observe(value)
        except Exception as e:
            logger.debug(f"Failed to observe histogram: {e}")


def safe_increment(counter: Optional[Counter], amount: int = 1, **labels):
    """Safely increment a counter."""
    if counter is not None:
        try:
            counter.labels(**labels).inc(amount)
        except Exception as e:
            logger.debug(f"Failed to increment counter: {e}")


def safe_set_gauge(gauge: Optional[Gauge], value: float, **labels):
    """Safely set a gauge value."""
    if gauge is not None:
        try:
            gauge.labels(**labels).set(value)
        except Exception as e:
            logger.debug(f"Failed to set gauge: {e}")


def get_metrics_response():
    """Generate Prometheus metrics response."""
    if not PROMETHEUS_ENABLED:
        return "# Prometheus metrics disabled", "text/plain"
    return generate_latest(), CONTENT_TYPE_LATEST
