"""
Shadow Mode Deployment
═══════════════════════════════════════════════════════════════════════════════

Enables parallel model comparison by running a "shadow" model alongside production.
Shadow model predictions are logged but never returned to users.

Features:
- Run production and shadow models in parallel
- Log all shadow predictions for analysis
- Track agreement rate between models
- Alert on significant disagreements
- Store comparison data for later analysis

Usage:
    from src.api.shadow_mode import ShadowModePredictor, get_shadow_predictor

    predictor = get_shadow_predictor()

    # Run prediction with shadow model
    result = predictor.predict_with_shadow(
        transaction=transaction_data,
        features=features_dict,
    )

    # result contains:
    # - production_result: The actual prediction to return
    # - shadow_result: Shadow model prediction (logged, not returned)
    # - agreement: Whether models agree
"""

import os
import time
import threading
from datetime import datetime
from typing import Dict, Any, Optional, Tuple, List
from dataclasses import dataclass, field
from collections import deque

from src.api.metrics import (
    track_shadow_prediction,
    update_shadow_agreement_rate,
    set_shadow_mode_enabled,
    PROMETHEUS_ENABLED,
)


@dataclass
class ShadowPrediction:
    """Result from shadow mode prediction."""
    transaction_id: str
    production_score: float
    production_prediction: bool
    shadow_score: float
    shadow_prediction: bool
    agreement: bool
    score_difference: float
    production_latency_ms: float
    shadow_latency_ms: float
    timestamp: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "transaction_id": self.transaction_id,
            "production_score": round(self.production_score, 4),
            "production_prediction": self.production_prediction,
            "shadow_score": round(self.shadow_score, 4),
            "shadow_prediction": self.shadow_prediction,
            "agreement": self.agreement,
            "score_difference": round(self.score_difference, 4),
            "production_latency_ms": round(self.production_latency_ms, 2),
            "shadow_latency_ms": round(self.shadow_latency_ms, 2),
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class ShadowModeStats:
    """Statistics for shadow mode."""
    total_predictions: int = 0
    agreements: int = 0
    disagreements: int = 0
    agreement_rate: float = 0.0
    avg_score_difference: float = 0.0
    max_score_difference: float = 0.0
    production_avg_latency_ms: float = 0.0
    shadow_avg_latency_ms: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_predictions": self.total_predictions,
            "agreements": self.agreements,
            "disagreements": self.disagreements,
            "agreement_rate": round(self.agreement_rate, 4),
            "avg_score_difference": round(self.avg_score_difference, 4),
            "max_score_difference": round(self.max_score_difference, 4),
            "production_avg_latency_ms": round(self.production_avg_latency_ms, 2),
            "shadow_avg_latency_ms": round(self.shadow_avg_latency_ms, 2),
        }


class ShadowModePredictor:
    """
    Manages shadow mode predictions.

    Runs production and shadow models in parallel, logs comparisons,
    and only returns production results.
    """

    # Configuration from environment
    SHADOW_STAGE = os.getenv("SHADOW_MODE_STAGE", "Staging")
    LOG_ALL = os.getenv("SHADOW_LOG_ALL", "true").lower() in ("true", "1", "yes")
    LOG_DISAGREEMENTS_ONLY = os.getenv("SHADOW_LOG_DISAGREEMENTS", "false").lower() in ("true", "1", "yes")
    AGREEMENT_THRESHOLD = float(os.getenv("SHADOW_AGREEMENT_THRESHOLD", "0.9"))
    SCORE_DIFF_THRESHOLD = float(os.getenv("SHADOW_SCORE_DIFF_THRESHOLD", "0.2"))
    FRAUD_THRESHOLD = float(os.getenv("FRAUD_THRESHOLD", "0.5"))

    def __init__(
        self,
        enabled: bool = None,
        buffer_size: int = 1000,
    ):
        """
        Initialize shadow mode predictor.

        Args:
            enabled: Override for shadow mode enabled status
            buffer_size: Size of rolling buffer for stats calculation
        """
        if enabled is not None:
            self._enabled = enabled
        else:
            self._enabled = os.getenv("SHADOW_MODE_ENABLED", "false").lower() in ("true", "1", "yes")

        self._buffer_size = buffer_size
        self._predictions_buffer: deque = deque(maxlen=buffer_size)
        self._lock = threading.RLock()

        # Stats counters
        self._total_predictions = 0
        self._agreements = 0
        self._disagreements = 0
        self._total_score_diff = 0.0
        self._max_score_diff = 0.0
        self._total_prod_latency = 0.0
        self._total_shadow_latency = 0.0

        # Callbacks for model loading (set by fraud_api.py)
        self._load_model_fn = None
        self._run_inference_fn = None

        # Update Prometheus on init
        set_shadow_mode_enabled(self._enabled)

        print(f"[ShadowMode] Initialized: enabled={self._enabled}, shadow_stage={self.SHADOW_STAGE}")

    @property
    def enabled(self) -> bool:
        return self._enabled

    def enable(self):
        """Enable shadow mode."""
        self._enabled = True
        set_shadow_mode_enabled(True)
        print("[ShadowMode] Enabled")

    def disable(self):
        """Disable shadow mode."""
        self._enabled = False
        set_shadow_mode_enabled(False)
        print("[ShadowMode] Disabled")

    def set_model_functions(
        self,
        load_model_fn,
        run_inference_fn,
    ):
        """Set the model loading and inference functions."""
        self._load_model_fn = load_model_fn
        self._run_inference_fn = run_inference_fn

    def predict_with_shadow(
        self,
        transaction_id: str,
        request_features: Dict[str, Any],
        feast_features: Dict[str, Any],
        production_model: Any = None,
        production_contract: Any = None,
        production_transformer: Any = None,
        production_info: Dict = None,
    ) -> Tuple[float, List[str], Optional[ShadowPrediction]]:
        """
        Run prediction with both production and shadow models.

        Args:
            transaction_id: Unique transaction ID
            request_features: Features from the request
            feast_features: Features from Feast
            production_model: Pre-loaded production model (optional)
            production_contract: Production feature contract (optional)
            production_transformer: Production transformer (optional)
            production_info: Production model info (optional)

        Returns:
            (production_score, warnings, shadow_prediction)
            - production_score: The score to return to caller
            - warnings: Any warnings from inference
            - shadow_prediction: Shadow comparison data (or None if disabled)
        """
        warnings = []

        if not self._enabled:
            # Shadow mode disabled - just return production prediction
            if production_model and self._run_inference_fn:
                score, inf_warnings = self._run_inference_fn(
                    production_model,
                    production_contract,
                    production_transformer,
                    request_features,
                    feast_features,
                )
                return score, inf_warnings, None
            return 0.0, ["No model provided"], None

        # Run production inference
        prod_start = time.time()
        prod_score = 0.0
        try:
            if production_model and self._run_inference_fn:
                prod_score, prod_warnings = self._run_inference_fn(
                    production_model,
                    production_contract,
                    production_transformer,
                    request_features,
                    feast_features,
                )
                warnings.extend(prod_warnings)
        except Exception as e:
            warnings.append(f"Production inference error: {e}")
        prod_latency = (time.time() - prod_start) * 1000

        # Run shadow inference
        shadow_start = time.time()
        shadow_score = -1.0
        try:
            if self._load_model_fn and self._run_inference_fn:
                shadow_model, shadow_contract, shadow_transformer, shadow_info = self._load_model_fn(
                    model_stage=self.SHADOW_STAGE
                )
                shadow_score, _ = self._run_inference_fn(
                    shadow_model,
                    shadow_contract,
                    shadow_transformer,
                    request_features,
                    feast_features,
                )
        except Exception as e:
            # Shadow errors don't fail the request
            print(f"[ShadowMode] Shadow inference error: {e}")
        shadow_latency = (time.time() - shadow_start) * 1000

        # Compare predictions
        if shadow_score >= 0:
            prod_prediction = prod_score >= self.FRAUD_THRESHOLD
            shadow_prediction = shadow_score >= self.FRAUD_THRESHOLD
            agreement = prod_prediction == shadow_prediction
            score_diff = abs(prod_score - shadow_score)

            shadow_result = ShadowPrediction(
                transaction_id=transaction_id,
                production_score=prod_score,
                production_prediction=prod_prediction,
                shadow_score=shadow_score,
                shadow_prediction=shadow_prediction,
                agreement=agreement,
                score_difference=score_diff,
                production_latency_ms=prod_latency,
                shadow_latency_ms=shadow_latency,
            )

            # Record the comparison
            self._record_comparison(shadow_result)

            return prod_score, warnings, shadow_result

        return prod_score, warnings, None

    def _record_comparison(self, result: ShadowPrediction):
        """Record a shadow comparison for stats and logging."""
        with self._lock:
            self._predictions_buffer.append(result)
            self._total_predictions += 1
            self._total_score_diff += result.score_difference
            self._max_score_diff = max(self._max_score_diff, result.score_difference)
            self._total_prod_latency += result.production_latency_ms
            self._total_shadow_latency += result.shadow_latency_ms

            if result.agreement:
                self._agreements += 1
            else:
                self._disagreements += 1

            # Update agreement rate
            agreement_rate = self._agreements / self._total_predictions if self._total_predictions > 0 else 0

        # Track in Prometheus
        track_shadow_prediction(
            agreement=result.agreement,
            prod_score=result.production_score,
            shadow_score=result.shadow_score,
        )
        update_shadow_agreement_rate(agreement_rate)

        # Log if needed
        should_log = self.LOG_ALL or (self.LOG_DISAGREEMENTS_ONLY and not result.agreement)
        if should_log:
            status = "AGREE" if result.agreement else "DISAGREE"
            print(f"[ShadowMode] {status}: txn={result.transaction_id}, "
                  f"prod={result.production_score:.4f}, shadow={result.shadow_score:.4f}, "
                  f"diff={result.score_difference:.4f}")

        # Alert on significant disagreement
        if not result.agreement and result.score_difference > self.SCORE_DIFF_THRESHOLD:
            print(f"[ShadowMode] ALERT: Large disagreement for {result.transaction_id}: "
                  f"score_diff={result.score_difference:.4f}")

    def get_stats(self) -> ShadowModeStats:
        """Get current shadow mode statistics."""
        with self._lock:
            total = self._total_predictions
            if total == 0:
                return ShadowModeStats()

            return ShadowModeStats(
                total_predictions=total,
                agreements=self._agreements,
                disagreements=self._disagreements,
                agreement_rate=self._agreements / total,
                avg_score_difference=self._total_score_diff / total,
                max_score_difference=self._max_score_diff,
                production_avg_latency_ms=self._total_prod_latency / total,
                shadow_avg_latency_ms=self._total_shadow_latency / total,
            )

    def get_recent_predictions(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent shadow predictions."""
        with self._lock:
            recent = list(self._predictions_buffer)[-limit:]
            return [p.to_dict() for p in recent]

    def get_disagreements(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent disagreements."""
        with self._lock:
            disagreements = [p for p in self._predictions_buffer if not p.agreement]
            return [p.to_dict() for p in disagreements[-limit:]]

    def reset_stats(self):
        """Reset all statistics."""
        with self._lock:
            self._predictions_buffer.clear()
            self._total_predictions = 0
            self._agreements = 0
            self._disagreements = 0
            self._total_score_diff = 0.0
            self._max_score_diff = 0.0
            self._total_prod_latency = 0.0
            self._total_shadow_latency = 0.0

    def get_status(self) -> Dict[str, Any]:
        """Get shadow mode status."""
        stats = self.get_stats()
        return {
            "enabled": self._enabled,
            "shadow_stage": self.SHADOW_STAGE,
            "fraud_threshold": self.FRAUD_THRESHOLD,
            "agreement_alert_threshold": self.AGREEMENT_THRESHOLD,
            "score_diff_alert_threshold": self.SCORE_DIFF_THRESHOLD,
            "stats": stats.to_dict(),
            "alert_active": stats.agreement_rate < self.AGREEMENT_THRESHOLD if stats.total_predictions >= 50 else False,
        }


# Global singleton
_shadow_predictor: Optional[ShadowModePredictor] = None
_shadow_lock = threading.Lock()


def get_shadow_predictor() -> ShadowModePredictor:
    """Get or create the global shadow predictor instance."""
    global _shadow_predictor

    if _shadow_predictor is None:
        with _shadow_lock:
            if _shadow_predictor is None:
                _shadow_predictor = ShadowModePredictor()

    return _shadow_predictor


def is_shadow_mode_enabled() -> bool:
    """Check if shadow mode is enabled."""
    return get_shadow_predictor().enabled


def enable_shadow_mode():
    """Enable shadow mode."""
    get_shadow_predictor().enable()


def disable_shadow_mode():
    """Disable shadow mode."""
    get_shadow_predictor().disable()
