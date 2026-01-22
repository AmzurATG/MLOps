"""
Performance Tracker
===================

Tracks prediction performance for monitoring and analysis.

Wraps the existing api/performance_tracker.py module.
"""

import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# Import existing tracker if available
try:
    from src.api.performance_tracker import (
        get_performance_tracker,
        record_prediction as _record_prediction,
        record_outcome as _record_outcome,
    )
    TRACKER_AVAILABLE = True
except ImportError:
    TRACKER_AVAILABLE = False
    logger.warning("Performance tracker not available")


class PerformanceTracker:
    """
    Tracks prediction performance.

    Records predictions and outcomes for later analysis
    of model performance and drift detection.
    """

    def __init__(self, enabled: bool = True):
        self.enabled = enabled and TRACKER_AVAILABLE
        self._tracker = None

    @property
    def tracker(self):
        """Lazy initialize tracker."""
        if self._tracker is None and self.enabled:
            try:
                self._tracker = get_performance_tracker()
            except Exception as e:
                logger.warning(f"Failed to initialize tracker: {e}")
                self.enabled = False
        return self._tracker

    def record_prediction(
        self,
        transaction_id: str,
        customer_id: str,
        fraud_score: float,
        is_fraud: bool,
        model_version: str,
        features: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """
        Record a prediction for tracking.

        Args:
            transaction_id: Transaction identifier
            customer_id: Customer identifier
            fraud_score: Predicted fraud probability
            is_fraud: Binary fraud prediction
            model_version: Model version used
            features: Optional feature values
            metadata: Optional additional metadata
        """
        if not self.enabled:
            return

        try:
            _record_prediction(
                transaction_id=transaction_id,
                customer_id=customer_id,
                fraud_score=fraud_score,
                is_fraud=is_fraud,
                model_version=model_version,
                features=features,
                metadata=metadata,
            )
        except Exception as e:
            logger.warning(f"Failed to record prediction: {e}")

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
            outcome_source: Source of the outcome (feedback, chargeback, etc.)
        """
        if not self.enabled:
            return

        try:
            _record_outcome(
                transaction_id=transaction_id,
                actual_fraud=actual_fraud,
                outcome_source=outcome_source,
            )
        except Exception as e:
            logger.warning(f"Failed to record outcome: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """Get tracker statistics."""
        if not self.enabled or not self.tracker:
            return {"status": "unavailable"}

        try:
            return self.tracker.get_stats()
        except Exception as e:
            return {"status": "error", "error": str(e)}
