"""
Prometheus Metrics Recorder
============================

Records serving metrics to Prometheus.

Wraps the existing api/metrics.py module for clean integration
with the serving layer.
"""

import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# Import existing metrics if available
try:
    from src.api.metrics import (
        track_prediction as _track_prediction,
        track_feature_fetch as _track_feature_fetch,
        track_ab_traffic as _track_ab_traffic,
        track_streaming_prediction as _track_streaming_prediction,
        PROMETHEUS_ENABLED,
    )
    METRICS_AVAILABLE = True
except ImportError:
    PROMETHEUS_ENABLED = False
    METRICS_AVAILABLE = False
    logger.warning("Prometheus metrics not available")


class MetricsRecorder:
    """
    Records serving metrics to Prometheus.

    Wraps the existing metrics module and provides a cleaner interface
    for the serving layer.
    """

    def __init__(self, enabled: bool = True):
        self.enabled = enabled and METRICS_AVAILABLE

    def record_prediction(
        self,
        status: str,
        risk_level: str,
        variant: str,
        is_fraud: bool,
        fraud_score: float,
        latency_seconds: float,
    ):
        """Record a prediction event."""
        if not self.enabled:
            return

        try:
            _track_prediction(
                status=status,
                risk_level=risk_level,
                variant=variant,
                is_fraud=is_fraud,
                fraud_score=fraud_score,
                latency=latency_seconds,
            )
        except Exception as e:
            logger.warning(f"Failed to record prediction metric: {e}")

    def record_feature_fetch(
        self,
        status: str,
        source: str,
        latency_seconds: float,
    ):
        """Record a feature fetch event."""
        if not self.enabled:
            return

        try:
            _track_feature_fetch(
                status=status,
                source=source,
                latency=latency_seconds,
            )
        except Exception as e:
            logger.warning(f"Failed to record feature fetch metric: {e}")

    def record_ab_traffic(
        self,
        experiment: str,
        variant: str,
    ):
        """Record A/B test traffic."""
        if not self.enabled:
            return

        try:
            _track_ab_traffic(
                experiment=experiment,
                variant=variant,
            )
        except Exception as e:
            logger.warning(f"Failed to record A/B traffic metric: {e}")

    def record_streaming_prediction(
        self,
        model_stage: str,
        is_correct: bool,
        latency_seconds: float,
        production_prediction: bool,
        staging_prediction: Optional[bool] = None,
    ):
        """Record a streaming-enhanced prediction."""
        if not self.enabled:
            return

        try:
            _track_streaming_prediction(
                model_stage=model_stage,
                is_correct=is_correct,
                latency=latency_seconds,
                production_prediction=production_prediction,
                staging_prediction=staging_prediction,
            )
        except Exception as e:
            logger.warning(f"Failed to record streaming prediction metric: {e}")
