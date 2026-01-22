"""
Telemetry Observer
==================

Unified telemetry interface for the serving layer.

Combines:
- Prometheus metrics (for dashboards and alerting)
- Performance tracking (for model monitoring)
- Logging (for debugging and audit)
"""

import logging
from typing import Dict, Any, Optional

from src.api.serving.telemetry.metrics import MetricsRecorder
from src.api.serving.telemetry.tracker import PerformanceTracker

logger = logging.getLogger(__name__)


class TelemetryObserver:
    """
    Unified telemetry observer.

    Provides a single interface for recording all types of telemetry
    during model serving.
    """

    def __init__(
        self,
        metrics_enabled: bool = True,
        tracking_enabled: bool = True,
        logging_enabled: bool = True,
    ):
        self.metrics = MetricsRecorder(enabled=metrics_enabled)
        self.tracker = PerformanceTracker(enabled=tracking_enabled)
        self.logging_enabled = logging_enabled

    def record_prediction(
        self,
        transaction_id: str,
        customer_id: str,
        fraud_score: float,
        is_fraud: bool,
        risk_level: str,
        model_stage: str,
        model_version: str,
        latency_ms: float,
        feature_source: str,
        streaming_available: bool = False,
        triggered_rules: Optional[list] = None,
        features: Optional[Dict[str, Any]] = None,
    ):
        """
        Record a prediction event across all telemetry systems.

        Args:
            transaction_id: Transaction identifier
            customer_id: Customer identifier
            fraud_score: Predicted fraud probability
            is_fraud: Binary fraud prediction
            risk_level: Risk level string
            model_stage: Model stage used
            model_version: Model version used
            latency_ms: Total latency in milliseconds
            feature_source: Source of features (feast, streaming, etc.)
            streaming_available: Whether streaming features were available
            triggered_rules: List of streaming rules that fired
            features: Optional feature values for tracking
        """
        # Log
        if self.logging_enabled:
            logger.info(
                f"Prediction: tx={transaction_id} customer={customer_id} "
                f"score={fraud_score:.4f} fraud={is_fraud} risk={risk_level} "
                f"model={model_stage} latency={latency_ms:.1f}ms "
                f"streaming={streaming_available}"
            )

        # Prometheus metrics
        self.metrics.record_prediction(
            status="success",
            risk_level=risk_level,
            variant=model_stage,
            is_fraud=is_fraud,
            fraud_score=fraud_score,
            latency_seconds=latency_ms / 1000,
        )

        # Feature fetch metrics
        self.metrics.record_feature_fetch(
            status="success",
            source=feature_source,
            latency_seconds=latency_ms / 1000,
        )

        # Streaming metrics
        if streaming_available and triggered_rules:
            self.metrics.record_streaming_prediction(
                model_stage=model_stage,
                is_correct=True,  # Would need actual label
                latency_seconds=latency_ms / 1000,
                production_prediction=is_fraud,
            )

        # Performance tracking
        self.tracker.record_prediction(
            transaction_id=transaction_id,
            customer_id=customer_id,
            fraud_score=fraud_score,
            is_fraud=is_fraud,
            model_version=model_version,
            features=features,
            metadata={
                "model_stage": model_stage,
                "feature_source": feature_source,
                "streaming_available": streaming_available,
                "triggered_rules": triggered_rules or [],
            },
        )

    def record_error(
        self,
        transaction_id: str,
        error: str,
        error_type: str,
        latency_ms: float,
    ):
        """
        Record a prediction error.

        Args:
            transaction_id: Transaction identifier
            error: Error message
            error_type: Type of error
            latency_ms: Latency before error
        """
        if self.logging_enabled:
            logger.error(
                f"Prediction error: tx={transaction_id} type={error_type} "
                f"error={error} latency={latency_ms:.1f}ms"
            )

        self.metrics.record_prediction(
            status="error",
            risk_level="unknown",
            variant="unknown",
            is_fraud=False,
            fraud_score=0.0,
            latency_seconds=latency_ms / 1000,
        )

    def record_outcome(
        self,
        transaction_id: str,
        actual_fraud: bool,
        outcome_source: str = "feedback",
    ):
        """
        Record actual outcome for a prediction.

        Args:
            transaction_id: Transaction identifier
            actual_fraud: Actual fraud label
            outcome_source: Source of the outcome
        """
        if self.logging_enabled:
            logger.info(
                f"Outcome: tx={transaction_id} actual_fraud={actual_fraud} "
                f"source={outcome_source}"
            )

        self.tracker.record_outcome(
            transaction_id=transaction_id,
            actual_fraud=actual_fraud,
            outcome_source=outcome_source,
        )

    def record_ab_traffic(
        self,
        experiment: str,
        variant: str,
    ):
        """Record A/B test traffic assignment."""
        self.metrics.record_ab_traffic(
            experiment=experiment,
            variant=variant,
        )

    def get_stats(self) -> Dict[str, Any]:
        """Get telemetry statistics."""
        return {
            "metrics_enabled": self.metrics.enabled,
            "tracking_enabled": self.tracker.enabled,
            "tracker_stats": self.tracker.get_stats(),
        }
