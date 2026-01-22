"""
Score Adjustments
=================

Applies business rules and adjustments to ML scores.

Adjustments include:
- Streaming velocity rules (from ksqlDB/Redis)
- Risk boosts based on real-time signals
- Custom business rules

The adjustments bridge the training-serving gap by incorporating
real-time signals that weren't available during training.
"""

import os
import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class AdjustmentResult:
    """Result of applying adjustments to ML score."""
    original_score: float
    final_score: float
    total_boost: float = 0.0
    triggered_rules: List[str] = field(default_factory=list)
    streaming_features_used: Dict[str, Any] = field(default_factory=dict)
    streaming_available: bool = False
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "original_score": self.original_score,
            "final_score": self.final_score,
            "total_boost": self.total_boost,
            "triggered_rules": self.triggered_rules,
            "streaming_features_used": self.streaming_features_used,
            "streaming_available": self.streaming_available,
            "timestamp": self.timestamp,
        }


@dataclass
class VelocityThresholds:
    """Thresholds for velocity-based rules."""
    high_5min: int = 5
    medium_5min: int = 3
    high_1h: int = 20
    medium_1h: int = 10
    high_24h: int = 50
    medium_24h: int = 30

    @classmethod
    def from_env(cls) -> "VelocityThresholds":
        """Load thresholds from environment."""
        return cls(
            high_5min=int(os.getenv("VELOCITY_HIGH_5MIN", "5")),
            medium_5min=int(os.getenv("VELOCITY_MEDIUM_5MIN", "3")),
            high_1h=int(os.getenv("VELOCITY_HIGH_1H", "20")),
            medium_1h=int(os.getenv("VELOCITY_MEDIUM_1H", "10")),
            high_24h=int(os.getenv("VELOCITY_HIGH_24H", "50")),
            medium_24h=int(os.getenv("VELOCITY_MEDIUM_24H", "30")),
        )


class ScoreAdjuster:
    """
    Applies business rules to adjust ML scores.

    Uses streaming features from ksqlDB/Redis to detect real-time
    risk patterns and boost scores accordingly.

    Rules are fail-safe: they only BOOST scores, never lower them.
    """

    def __init__(
        self,
        thresholds: Optional[VelocityThresholds] = None,
        max_boost: float = 0.5,
    ):
        self.thresholds = thresholds or VelocityThresholds.from_env()
        self.max_boost = max_boost

    def _safe_int(self, value: Any, default: int = 0) -> int:
        """Safely convert a value to int."""
        if value is None:
            return default
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        """Safely convert a value to float."""
        if value is None:
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    def _is_truthy(self, value: Any) -> bool:
        """Check if value represents True."""
        return value in ('1', 1, True, 'true', 'True')

    def apply(
        self,
        ml_score: float,
        streaming_features: Dict[str, Any],
    ) -> AdjustmentResult:
        """
        Apply streaming rules to adjust ML score.

        Args:
            ml_score: Original ML model probability (0.0 - 1.0)
            streaming_features: Dict of streaming features from Redis

        Returns:
            AdjustmentResult with final_score and triggered_rules

        Rule Categories:
            1. Velocity rules (transaction frequency in time windows)
            2. Multi-entity rules (multiple countries, devices, IPs)
            3. Amount anomaly rules (current amount vs historical average)
            4. ksqlDB computed risk scores
        """
        if not streaming_features:
            return AdjustmentResult(
                original_score=ml_score,
                final_score=ml_score,
                streaming_available=False,
            )

        final_score = ml_score
        total_boost = 0.0
        triggered_rules = []
        th = self.thresholds

        # Extract streaming features
        tx_5min = self._safe_int(streaming_features.get('tx_count_5min'))
        tx_1h = self._safe_int(streaming_features.get('tx_count_1h'))
        tx_24h = self._safe_int(streaming_features.get('tx_count_24h'))
        velocity_score = self._safe_float(streaming_features.get('velocity_score'))

        multi_country = self._is_truthy(streaming_features.get('multi_country_flag'))
        multi_device = self._is_truthy(streaming_features.get('multi_device_flag'))
        high_velocity = self._is_truthy(streaming_features.get('high_velocity_flag'))
        amount_anomaly = self._is_truthy(streaming_features.get('amount_anomaly_flag'))

        # Rule 1: 5-minute velocity
        if tx_5min >= th.high_5min:
            boost = 0.3
            triggered_rules.append(f"high_velocity_5min: {tx_5min} tx in 5min")
            total_boost = max(total_boost, boost)
        elif tx_5min >= th.medium_5min:
            boost = 0.15
            triggered_rules.append(f"medium_velocity_5min: {tx_5min} tx in 5min")
            total_boost = max(total_boost, boost)

        # Rule 2: 1-hour velocity
        if tx_1h >= th.high_1h:
            boost = 0.2
            triggered_rules.append(f"high_velocity_1h: {tx_1h} tx in 1h")
            total_boost = max(total_boost, boost)
        elif tx_1h >= th.medium_1h:
            boost = 0.1
            triggered_rules.append(f"medium_velocity_1h: {tx_1h} tx in 1h")
            total_boost = max(total_boost, boost)

        # Rule 3: Multi-country (impossible travel)
        if multi_country:
            boost = 0.25
            triggered_rules.append("multi_country: transactions from multiple countries")
            total_boost = max(total_boost, boost)

        # Rule 4: Multi-device (account compromise indicator)
        if multi_device:
            boost = 0.15
            triggered_rules.append("multi_device: multiple devices in short window")
            total_boost = max(total_boost, boost)

        # Rule 5: ksqlDB velocity score
        if velocity_score >= 0.8:
            boost = 0.25
            triggered_rules.append(f"ksqldb_high_risk: velocity_score={velocity_score:.2f}")
            total_boost = max(total_boost, boost)
        elif velocity_score >= 0.5:
            boost = 0.1
            triggered_rules.append(f"ksqldb_medium_risk: velocity_score={velocity_score:.2f}")
            total_boost = max(total_boost, boost)

        # Rule 6: Amount anomaly
        if amount_anomaly:
            boost = 0.2
            triggered_rules.append("amount_anomaly: amount > 3x 24h average")
            total_boost = max(total_boost, boost)

        # Apply boost (capped at max_boost)
        actual_boost = min(total_boost, self.max_boost)
        final_score = min(ml_score + actual_boost, 1.0)

        return AdjustmentResult(
            original_score=ml_score,
            final_score=final_score,
            total_boost=actual_boost,
            triggered_rules=triggered_rules,
            streaming_features_used={
                "tx_count_5min": tx_5min,
                "tx_count_1h": tx_1h,
                "tx_count_24h": tx_24h,
                "velocity_score": velocity_score,
                "multi_country_flag": multi_country,
                "multi_device_flag": multi_device,
                "high_velocity_flag": high_velocity,
                "amount_anomaly_flag": amount_anomaly,
            },
            streaming_available=True,
        )
