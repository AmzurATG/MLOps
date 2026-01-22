"""
Streaming Feature Rule Engine
=============================

Applies business rules based on real-time streaming features to boost ML scores.
Rules are data-driven for easy maintenance and configuration.
"""

import os
import time
import logging
from typing import Dict, Any, List, Optional, Tuple, Callable
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

try:
    from pipelines.settings import settings
    THRESHOLDS = settings.mlops.velocity_thresholds
except ImportError:
    THRESHOLDS = {
        "high_5min": int(os.getenv("VELOCITY_HIGH_5MIN", "5")),
        "medium_5min": int(os.getenv("VELOCITY_MEDIUM_5MIN", "3")),
        "high_1h": int(os.getenv("VELOCITY_HIGH_1H", "20")),
        "medium_1h": int(os.getenv("VELOCITY_MEDIUM_1H", "10")),
        "high_24h": int(os.getenv("VELOCITY_HIGH_24H", "50")),
        "medium_24h": int(os.getenv("VELOCITY_MEDIUM_24H", "30")),
    }

REDIS_STREAMING_PREFIX = os.getenv("REDIS_STREAMING_PREFIX", "feast:streaming:")


# =============================================================================
# RESULT DATACLASS
# =============================================================================

@dataclass
class StreamingRuleResult:
    """Result of applying streaming rules to ML score."""
    ml_score: float
    final_score: float
    streaming_boost: float
    triggered_rules: List[str] = field(default_factory=list)
    streaming_features_used: Dict[str, Any] = field(default_factory=dict)
    streaming_available: bool = True
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ml_score": self.ml_score,
            "final_score": self.final_score,
            "streaming_boost": self.streaming_boost,
            "triggered_rules": self.triggered_rules,
            "streaming_features_used": self.streaming_features_used,
            "streaming_available": self.streaming_available,
            "timestamp": self.timestamp,
        }


# =============================================================================
# HELPERS
# =============================================================================

def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value) if value is not None else default
    except (ValueError, TypeError):
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except (ValueError, TypeError):
        return default


def _is_truthy(value: Any) -> bool:
    """Check if value represents True (handles Redis string '1', 'true', etc)."""
    return value in ('1', 1, True, 'true', 'True')


# =============================================================================
# RULE DEFINITIONS (Data-Driven)
# =============================================================================

@dataclass
class Rule:
    """A single streaming rule definition."""
    name: str
    condition: Callable[[Dict[str, Any], Dict[str, int]], bool]
    action: str  # "max" or "boost"
    value: float
    message: Callable[[Dict[str, Any], Dict[str, int]], str]


def _build_rules() -> List[Rule]:
    """Build the list of streaming rules."""
    return [
        # Velocity Rules (5-minute window)
        Rule(
            name="HIGH_VELOCITY_5MIN",
            condition=lambda f, th: f["tx_5min"] > th.get("high_5min", 5),
            action="max",
            value=0.95,
            message=lambda f, th: f"HIGH_VELOCITY_5MIN: {f['tx_5min']} txns in 5min (>{th.get('high_5min', 5)})"
        ),
        Rule(
            name="MEDIUM_VELOCITY_5MIN",
            condition=lambda f, th: th.get("medium_5min", 3) < f["tx_5min"] <= th.get("high_5min", 5),
            action="boost",
            value=0.20,
            message=lambda f, th: f"MEDIUM_VELOCITY_5MIN: {f['tx_5min']} txns in 5min (>{th.get('medium_5min', 3)})"
        ),

        # Velocity Rules (1-hour window)
        Rule(
            name="HIGH_VELOCITY_1H",
            condition=lambda f, th: f["tx_1h"] > th.get("high_1h", 20),
            action="max",
            value=0.85,
            message=lambda f, th: f"HIGH_VELOCITY_1H: {f['tx_1h']} txns in 1h (>{th.get('high_1h', 20)})"
        ),
        Rule(
            name="MEDIUM_VELOCITY_1H",
            condition=lambda f, th: th.get("medium_1h", 10) < f["tx_1h"] <= th.get("high_1h", 20),
            action="boost",
            value=0.15,
            message=lambda f, th: f"MEDIUM_VELOCITY_1H: {f['tx_1h']} txns in 1h (>{th.get('medium_1h', 10)})"
        ),

        # Velocity Rules (24-hour window)
        Rule(
            name="HIGH_VELOCITY_24H",
            condition=lambda f, th: f["tx_24h"] > th.get("high_24h", 50),
            action="max",
            value=0.75,
            message=lambda f, th: f"HIGH_VELOCITY_24H: {f['tx_24h']} txns in 24h (>{th.get('high_24h', 50)})"
        ),
        Rule(
            name="MEDIUM_VELOCITY_24H",
            condition=lambda f, th: th.get("medium_24h", 30) < f["tx_24h"] <= th.get("high_24h", 50),
            action="boost",
            value=0.10,
            message=lambda f, th: f"MEDIUM_VELOCITY_24H: {f['tx_24h']} txns in 24h (>{th.get('medium_24h', 30)})"
        ),

        # Multi-entity rules
        Rule(
            name="MULTI_COUNTRY_5MIN",
            condition=lambda f, th: f["multi_country"],
            action="boost",
            value=0.30,
            message=lambda f, th: "MULTI_COUNTRY_5MIN: Multiple countries in 5-minute window"
        ),
        Rule(
            name="MULTI_DEVICE_5MIN",
            condition=lambda f, th: f["multi_device"],
            action="boost",
            value=0.25,
            message=lambda f, th: "MULTI_DEVICE_5MIN: Multiple devices in 5-minute window"
        ),

        # Amount anomaly
        Rule(
            name="AMOUNT_ANOMALY",
            condition=lambda f, th: f["amount_anomaly"],
            action="boost",
            value=0.15,
            message=lambda f, th: "AMOUNT_ANOMALY: Transaction amount > 3x customer's 24h average"
        ),

        # Compound rules
        Rule(
            name="ADDRESS_NIGHT_COMPOUND",
            condition=lambda f, th: f["address_mismatch"] and f["is_night"],
            action="boost",
            value=0.10,
            message=lambda f, th: "ADDRESS_NIGHT_COMPOUND: Address mismatch during night hours"
        ),
        Rule(
            name="RISKY_PAYMENT_VELOCITY",
            condition=lambda f, th: f["risky_payment"] and f["tx_5min"] > 2,
            action="boost",
            value=0.10,
            message=lambda f, th: "RISKY_PAYMENT_VELOCITY: Risky payment with elevated velocity"
        ),
    ]


RULES = _build_rules()


# =============================================================================
# MAIN RULE ENGINE
# =============================================================================

def apply_streaming_rules(
    ml_score: float,
    streaming_features: Dict[str, Any],
    thresholds: Optional[Dict[str, int]] = None,
) -> StreamingRuleResult:
    """
    Apply business rules based on streaming features to adjust ML score.

    Rules only BOOST scores (fail-safe approach - never reduce risk).
    """
    if not streaming_features:
        return StreamingRuleResult(
            ml_score=ml_score,
            final_score=ml_score,
            streaming_boost=0.0,
            streaming_available=False,
        )

    th = thresholds or THRESHOLDS

    # Extract and normalize features
    features = {
        "tx_5min": _safe_int(streaming_features.get('tx_count_5min')),
        "tx_1h": _safe_int(streaming_features.get('tx_count_1h')),
        "tx_24h": _safe_int(streaming_features.get('tx_count_24h')),
        "velocity_score": _safe_float(streaming_features.get('velocity_score')),
        "multi_country": _is_truthy(streaming_features.get('multi_country_flag')),
        "multi_device": _is_truthy(streaming_features.get('multi_device_flag')),
        "high_velocity": _is_truthy(streaming_features.get('high_velocity_flag')),
        "amount_anomaly": _is_truthy(streaming_features.get('amount_anomaly_flag')),
        "address_mismatch": _is_truthy(streaming_features.get('address_mismatch')),
        "is_night": _is_truthy(streaming_features.get('is_night')),
        "risky_payment": _is_truthy(streaming_features.get('risky_payment')),
    }

    final_score = ml_score
    triggered_rules = []

    # Apply each rule
    for rule in RULES:
        try:
            if rule.condition(features, th):
                if rule.action == "max":
                    final_score = max(final_score, rule.value)
                else:  # boost
                    final_score = min(final_score + rule.value, 1.0)
                triggered_rules.append(rule.message(features, th))
        except Exception as e:
            logger.debug(f"Rule {rule.name} failed: {e}")

    # Handle ksqlDB velocity score (special case)
    velocity_score = features["velocity_score"]
    if velocity_score > 0.8:
        final_score = max(final_score, velocity_score)
        triggered_rules.append(f"KSQLDB_VELOCITY_SCORE: {velocity_score:.2f}")
    elif velocity_score > 0.5:
        final_score = min(final_score + velocity_score * 0.2, 1.0)
        triggered_rules.append(f"KSQLDB_VELOCITY_BOOST: {velocity_score:.2f}")

    # High velocity composite (if not already triggered by specific rules)
    if features["high_velocity"] and not any("VELOCITY" in r for r in triggered_rules):
        final_score = min(final_score + 0.2, 1.0)
        triggered_rules.append("HIGH_VELOCITY_COMPOSITE: ksqlDB flagged high velocity")

    final_score = min(final_score, 1.0)

    return StreamingRuleResult(
        ml_score=ml_score,
        final_score=final_score,
        streaming_boost=final_score - ml_score,
        triggered_rules=triggered_rules,
        streaming_features_used=features,
        streaming_available=True,
    )


# =============================================================================
# REDIS FETCHING
# =============================================================================

def fetch_streaming_features(
    customer_id: str,
    redis_client: Any = None,
) -> Tuple[Dict[str, Any], float, bool]:
    """Fetch streaming features from Redis for a customer."""
    start = time.time()

    try:
        if redis_client is None:
            import redis
            redis_client = redis.Redis(
                host=os.getenv("REDIS_HOST", "exp-redis"),
                port=int(os.getenv("REDIS_PORT", "6379")),
                decode_responses=True,
            )

        features = redis_client.hgetall(f"{REDIS_STREAMING_PREFIX}{customer_id}")
        latency_ms = (time.time() - start) * 1000

        return features, latency_ms, bool(features)

    except Exception as e:
        logger.warning(f"Failed to fetch streaming features for {customer_id}: {e}")
        return {}, (time.time() - start) * 1000, False


# =============================================================================
# EXPLANATION
# =============================================================================

def get_risk_explanation(result: StreamingRuleResult) -> str:
    """Generate a human-readable explanation of the risk assessment."""
    if not result.streaming_available:
        return "Real-time velocity data not available. Using ML score only."

    if not result.triggered_rules:
        return "No real-time risk patterns detected. Using ML score."

    high_severity = [r for r in result.triggered_rules if "HIGH" in r or "MULTI_COUNTRY" in r]
    medium_severity = [r for r in result.triggered_rules if "MEDIUM" in r or "BOOST" in r]

    parts = []
    if result.streaming_boost > 0.3:
        parts.append(f"CRITICAL: Risk boosted by {result.streaming_boost:.1%}.")
    elif result.streaming_boost > 0.15:
        parts.append(f"WARNING: Risk increased by {result.streaming_boost:.1%}.")
    elif result.streaming_boost > 0:
        parts.append(f"NOTE: Risk slightly increased by {result.streaming_boost:.1%}.")

    if high_severity:
        parts.append(f"High-risk: {', '.join(r.split(':')[0] for r in high_severity)}")
    if medium_severity:
        parts.append(f"Elevated: {', '.join(r.split(':')[0] for r in medium_severity)}")

    return " ".join(parts) or "Risk assessment complete."


__all__ = [
    "apply_streaming_rules",
    "fetch_streaming_features",
    "get_risk_explanation",
    "StreamingRuleResult",
    "THRESHOLDS",
]
