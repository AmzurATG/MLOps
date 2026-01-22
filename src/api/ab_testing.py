"""
A/B Testing Infrastructure for Model Comparison
═══════════════════════════════════════════════════════════════════════════════

Provides A/B testing capabilities for safe model rollouts.

Features:
- Consistent user-to-variant assignment (sticky sessions)
- Traffic split configuration
- Experiment metrics tracking
- Statistical analysis for model comparison
- Redis-backed state for distributed deployment

Usage:
    router = ABTestingRouter(redis_client)
    router.create_experiment("model_v2_test", control="v1", treatment="v2", traffic_split=0.1)
    variant, model_uri = router.get_variant("model_v2_test", customer_id)
"""

import os
import json
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
import random

logger = logging.getLogger(__name__)

# Try to import Redis
try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    logger.warning("[A/B Testing] Redis not available - using in-memory storage")


# ═══════════════════════════════════════════════════════════════════════════════
# DATA CLASSES
# ═══════════════════════════════════════════════════════════════════════════════

class ExperimentStatus(str, Enum):
    """Experiment status."""
    DRAFT = "draft"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"


@dataclass
class Experiment:
    """A/B test experiment configuration."""
    name: str
    description: str
    control_model_version: str  # MLflow model version for control
    treatment_model_version: str  # MLflow model version for treatment
    traffic_split: float  # 0.0-1.0, fraction to treatment
    status: ExperimentStatus = ExperimentStatus.DRAFT
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    model_name: str = "fraud_detection_model"
    
    def is_active(self) -> bool:
        """Check if experiment is currently running."""
        return self.status == ExperimentStatus.RUNNING
    
    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "description": self.description,
            "control_model_version": self.control_model_version,
            "treatment_model_version": self.treatment_model_version,
            "traffic_split": self.traffic_split,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "model_name": self.model_name,
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> "Experiment":
        return cls(
            name=data["name"],
            description=data.get("description", ""),
            control_model_version=data["control_model_version"],
            treatment_model_version=data["treatment_model_version"],
            traffic_split=float(data["traffic_split"]),
            status=ExperimentStatus(data.get("status", "draft")),
            created_at=datetime.fromisoformat(data["created_at"]) if data.get("created_at") else datetime.now(),
            started_at=datetime.fromisoformat(data["started_at"]) if data.get("started_at") else None,
            ended_at=datetime.fromisoformat(data["ended_at"]) if data.get("ended_at") else None,
            model_name=data.get("model_name", "fraud_detection_model"),
        )


@dataclass
class ExperimentResult:
    """Single prediction result for an experiment."""
    experiment_name: str
    variant: str  # "control" or "treatment"
    customer_id: str
    transaction_id: str
    prediction: float
    actual_label: Optional[int] = None
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict:
        return {
            "experiment_name": self.experiment_name,
            "variant": self.variant,
            "customer_id": self.customer_id,
            "transaction_id": self.transaction_id,
            "prediction": self.prediction,
            "actual_label": self.actual_label,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class ExperimentStats:
    """Statistics for an experiment."""
    experiment_name: str
    control_count: int
    treatment_count: int
    total_count: int
    actual_traffic_split: float
    control_avg_prediction: float
    treatment_avg_prediction: float
    control_fraud_rate: float
    treatment_fraud_rate: float
    started_at: Optional[str]
    status: str
    
    def to_dict(self) -> Dict:
        return asdict(self)


# ═══════════════════════════════════════════════════════════════════════════════
# A/B TESTING ROUTER
# ═══════════════════════════════════════════════════════════════════════════════

class ABTestingRouter:
    """
    Route requests to control or treatment models based on experiment configuration.
    
    Uses consistent hashing for sticky sessions (same user always gets same variant).
    """
    
    def __init__(
        self,
        redis_host: str = None,
        redis_port: int = 6379,
        redis_db: int = 1,  # Use DB 1 for A/B testing (0 is for Feast)
        model_name: str = "fraud_detection_model",
    ):
        self.model_name = model_name
        self._experiments: Dict[str, Experiment] = {}
        self._results: Dict[str, List[ExperimentResult]] = {}
        self._redis = None
        
        # Try to connect to Redis
        if REDIS_AVAILABLE:
            try:
                host = redis_host or os.getenv("REDIS_HOST", "exp-redis")
                self._redis = redis.Redis(
                    host=host,
                    port=redis_port,
                    db=redis_db,
                    decode_responses=True
                )
                self._redis.ping()
                logger.info(f"[A/B Testing] Connected to Redis: {host}:{redis_port}")
            except Exception as e:
                logger.warning(f"[A/B Testing] Redis not available: {e}")
                self._redis = None
    
    # ─────────────────────────────────────────────────────────────────────────
    # EXPERIMENT MANAGEMENT
    # ─────────────────────────────────────────────────────────────────────────
    
    def create_experiment(
        self,
        name: str,
        control_version: str,
        treatment_version: str,
        traffic_split: float = 0.1,
        description: str = "",
    ) -> Experiment:
        """
        Create a new A/B test experiment.
        
        Args:
            name: Unique experiment name
            control_version: MLflow model version for control group
            treatment_version: MLflow model version for treatment group
            traffic_split: Fraction of traffic to treatment (0.0-1.0)
            description: Human-readable description
        
        Returns:
            Created Experiment object
        """
        if traffic_split < 0 or traffic_split > 1:
            raise ValueError("traffic_split must be between 0 and 1")
        
        experiment = Experiment(
            name=name,
            description=description,
            control_model_version=control_version,
            treatment_model_version=treatment_version,
            traffic_split=traffic_split,
            model_name=self.model_name,
        )
        
        # Store in memory
        self._experiments[name] = experiment
        
        # Store in Redis if available
        if self._redis:
            self._redis.hset(
                f"ab:experiment:{name}",
                mapping={
                    "data": json.dumps(experiment.to_dict()),
                    "created_at": datetime.now().isoformat(),
                }
            )
        
        logger.info(f"[A/B Testing] Created experiment: {name} ({traffic_split:.0%} to treatment)")
        return experiment
    
    def start_experiment(self, name: str) -> Experiment:
        """Start a paused or draft experiment."""
        experiment = self.get_experiment(name)
        if not experiment:
            raise ValueError(f"Experiment not found: {name}")
        
        experiment.status = ExperimentStatus.RUNNING
        experiment.started_at = datetime.now()
        
        self._experiments[name] = experiment
        if self._redis:
            self._redis.hset(f"ab:experiment:{name}", "data", json.dumps(experiment.to_dict()))
        
        logger.info(f"[A/B Testing] Started experiment: {name}")
        return experiment
    
    def stop_experiment(self, name: str) -> Experiment:
        """Stop a running experiment."""
        experiment = self.get_experiment(name)
        if not experiment:
            raise ValueError(f"Experiment not found: {name}")
        
        experiment.status = ExperimentStatus.COMPLETED
        experiment.ended_at = datetime.now()
        
        self._experiments[name] = experiment
        if self._redis:
            self._redis.hset(f"ab:experiment:{name}", "data", json.dumps(experiment.to_dict()))
        
        logger.info(f"[A/B Testing] Stopped experiment: {name}")
        return experiment
    
    def get_experiment(self, name: str) -> Optional[Experiment]:
        """Get experiment by name."""
        # Check memory first
        if name in self._experiments:
            return self._experiments[name]
        
        # Try Redis
        if self._redis:
            try:
                data = self._redis.hget(f"ab:experiment:{name}", "data")
                if data:
                    experiment = Experiment.from_dict(json.loads(data))
                    self._experiments[name] = experiment
                    return experiment
            except Exception as e:
                logger.warning(f"[A/B Testing] Error loading experiment from Redis: {e}")
        
        return None
    
    def list_experiments(self) -> List[Experiment]:
        """List all experiments."""
        experiments = list(self._experiments.values())
        
        # Also check Redis for any experiments not in memory
        if self._redis:
            try:
                keys = self._redis.keys("ab:experiment:*")
                for key in keys:
                    name = key.replace("ab:experiment:", "")
                    if name not in self._experiments:
                        experiment = self.get_experiment(name)
                        if experiment:
                            experiments.append(experiment)
            except Exception as e:
                logger.warning(f"[A/B Testing] Error listing experiments from Redis: {e}")
        
        return experiments
    
    def delete_experiment(self, name: str) -> bool:
        """Delete an experiment."""
        if name in self._experiments:
            del self._experiments[name]
        
        if self._redis:
            self._redis.delete(f"ab:experiment:{name}")
            self._redis.delete(f"ab:counts:{name}")
            self._redis.delete(f"ab:results:{name}")
        
        if name in self._results:
            del self._results[name]
        
        logger.info(f"[A/B Testing] Deleted experiment: {name}")
        return True
    
    # ─────────────────────────────────────────────────────────────────────────
    # VARIANT ASSIGNMENT
    # ─────────────────────────────────────────────────────────────────────────
    
    def get_variant(
        self,
        experiment_name: str,
        customer_id: str,
        sticky: bool = True,
    ) -> Tuple[str, str]:
        """
        Determine which variant a customer should see.
        
        Args:
            experiment_name: Name of the experiment
            customer_id: Customer ID for consistent assignment
            sticky: If True, same customer always gets same variant
        
        Returns:
            Tuple of (variant_name, model_version)
        """
        experiment = self.get_experiment(experiment_name)
        
        if not experiment or not experiment.is_active():
            # No active experiment - return control (Production)
            version = experiment.control_model_version if experiment else "Production"
            return ("control", version)
        
        # Determine variant
        if sticky:
            # Deterministic assignment based on customer_id (consistent hashing)
            hash_input = f"{experiment_name}:{customer_id}"
            hash_value = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
            bucket = (hash_value % 10000) / 10000.0
        else:
            bucket = random.random()
        
        if bucket < experiment.traffic_split:
            variant = "treatment"
            model_version = experiment.treatment_model_version
        else:
            variant = "control"
            model_version = experiment.control_model_version
        
        # Track assignment
        self._track_assignment(experiment_name, variant)
        
        return (variant, model_version)
    
    def get_model_uri(self, variant: str, model_version: str) -> str:
        """Get MLflow model URI for a variant."""
        # Handle special cases
        if model_version in ["Production", "Staging", "Archived"]:
            return f"models:/{self.model_name}/{model_version}"
        
        # Assume it's a version number
        return f"models:/{self.model_name}/{model_version}"
    
    def _track_assignment(self, experiment_name: str, variant: str):
        """Track variant assignment."""
        if self._redis:
            self._redis.hincrby(f"ab:counts:{experiment_name}", variant, 1)
            self._redis.hincrby(f"ab:counts:{experiment_name}", "total", 1)
    
    # ─────────────────────────────────────────────────────────────────────────
    # RESULT RECORDING
    # ─────────────────────────────────────────────────────────────────────────
    
    def record_prediction(
        self,
        experiment_name: str,
        variant: str,
        customer_id: str,
        transaction_id: str,
        prediction: float,
        actual_label: Optional[int] = None,
    ):
        """Record a prediction result for analysis."""
        result = ExperimentResult(
            experiment_name=experiment_name,
            variant=variant,
            customer_id=customer_id,
            transaction_id=transaction_id,
            prediction=prediction,
            actual_label=actual_label,
        )
        
        # Store in memory (limited buffer)
        if experiment_name not in self._results:
            self._results[experiment_name] = []
        
        self._results[experiment_name].append(result)
        
        # Keep only last 10000 results in memory
        if len(self._results[experiment_name]) > 10000:
            self._results[experiment_name] = self._results[experiment_name][-10000:]
        
        # Store in Redis for persistence
        if self._redis:
            self._redis.lpush(
                f"ab:results:{experiment_name}",
                json.dumps(result.to_dict())
            )
            # Keep only last 100000 in Redis
            self._redis.ltrim(f"ab:results:{experiment_name}", 0, 99999)
            
            # Update aggregates
            self._redis.hincrbyfloat(f"ab:agg:{experiment_name}:{variant}", "sum_predictions", prediction)
            self._redis.hincrby(f"ab:agg:{experiment_name}:{variant}", "count", 1)
            
            if prediction >= 0.5:
                self._redis.hincrby(f"ab:agg:{experiment_name}:{variant}", "fraud_count", 1)
    
    # ─────────────────────────────────────────────────────────────────────────
    # STATISTICS & ANALYSIS
    # ─────────────────────────────────────────────────────────────────────────
    
    def get_experiment_stats(self, experiment_name: str) -> ExperimentStats:
        """Get current statistics for an experiment."""
        experiment = self.get_experiment(experiment_name)
        
        control_count = 0
        treatment_count = 0
        control_sum = 0.0
        treatment_sum = 0.0
        control_fraud = 0
        treatment_fraud = 0
        
        # Get counts from Redis
        if self._redis:
            counts = self._redis.hgetall(f"ab:counts:{experiment_name}") or {}
            control_count = int(counts.get("control", 0))
            treatment_count = int(counts.get("treatment", 0))
            
            # Get aggregates
            control_agg = self._redis.hgetall(f"ab:agg:{experiment_name}:control") or {}
            treatment_agg = self._redis.hgetall(f"ab:agg:{experiment_name}:treatment") or {}
            
            control_sum = float(control_agg.get("sum_predictions", 0))
            treatment_sum = float(treatment_agg.get("sum_predictions", 0))
            control_fraud = int(control_agg.get("fraud_count", 0))
            treatment_fraud = int(treatment_agg.get("fraud_count", 0))
        else:
            # Calculate from in-memory results
            results = self._results.get(experiment_name, [])
            for r in results:
                if r.variant == "control":
                    control_count += 1
                    control_sum += r.prediction
                    if r.prediction >= 0.5:
                        control_fraud += 1
                else:
                    treatment_count += 1
                    treatment_sum += r.prediction
                    if r.prediction >= 0.5:
                        treatment_fraud += 1
        
        total_count = control_count + treatment_count
        actual_split = treatment_count / total_count if total_count > 0 else 0.0
        
        return ExperimentStats(
            experiment_name=experiment_name,
            control_count=control_count,
            treatment_count=treatment_count,
            total_count=total_count,
            actual_traffic_split=round(actual_split, 4),
            control_avg_prediction=round(control_sum / control_count, 4) if control_count > 0 else 0.0,
            treatment_avg_prediction=round(treatment_sum / treatment_count, 4) if treatment_count > 0 else 0.0,
            control_fraud_rate=round(control_fraud / control_count, 4) if control_count > 0 else 0.0,
            treatment_fraud_rate=round(treatment_fraud / treatment_count, 4) if treatment_count > 0 else 0.0,
            started_at=experiment.started_at.isoformat() if experiment and experiment.started_at else None,
            status=experiment.status.value if experiment else "unknown",
        )
    
    def compare_variants(
        self,
        experiment_name: str,
        metric: str = "fraud_rate",
        confidence_level: float = 0.95,
    ) -> Dict[str, Any]:
        """
        Statistical comparison of control vs treatment.
        
        Returns p-value and confidence interval for the difference.
        """
        stats = self.get_experiment_stats(experiment_name)
        
        if stats.total_count < 100:
            return {
                "status": "insufficient_data",
                "message": f"Need at least 100 samples, have {stats.total_count}",
                "control_count": stats.control_count,
                "treatment_count": stats.treatment_count,
            }
        
        # Simple comparison (for demo - production would use proper statistical tests)
        if metric == "fraud_rate":
            control_rate = stats.control_fraud_rate
            treatment_rate = stats.treatment_fraud_rate
        else:
            control_rate = stats.control_avg_prediction
            treatment_rate = stats.treatment_avg_prediction
        
        difference = treatment_rate - control_rate
        relative_change = difference / control_rate if control_rate > 0 else 0
        
        # Simplified significance test (for demo)
        # In production, use scipy.stats for proper hypothesis testing
        min_samples = min(stats.control_count, stats.treatment_count)
        is_significant = abs(difference) > 0.02 and min_samples >= 500
        
        return {
            "status": "complete",
            "metric": metric,
            "control_value": control_rate,
            "treatment_value": treatment_rate,
            "absolute_difference": round(difference, 4),
            "relative_change": round(relative_change, 4),
            "is_significant": is_significant,
            "confidence_level": confidence_level,
            "recommendation": self._get_recommendation(difference, is_significant, min_samples),
            "control_count": stats.control_count,
            "treatment_count": stats.treatment_count,
        }
    
    def _get_recommendation(
        self,
        difference: float,
        is_significant: bool,
        sample_size: int,
    ) -> str:
        """Generate recommendation based on results."""
        if sample_size < 500:
            return "Continue experiment - need more data for reliable conclusions"
        
        if not is_significant:
            return "No significant difference detected - consider continuing or concluding with no change"
        
        if difference > 0:
            return "Treatment shows higher fraud rate - investigate before promoting"
        else:
            return "Treatment shows lower fraud rate - consider promoting to production"


# ═══════════════════════════════════════════════════════════════════════════════
# SINGLETON INSTANCE
# ═══════════════════════════════════════════════════════════════════════════════

_ab_router: Optional[ABTestingRouter] = None


def get_ab_router() -> ABTestingRouter:
    """Get or create the singleton A/B testing router."""
    global _ab_router
    if _ab_router is None:
        _ab_router = ABTestingRouter()
    return _ab_router


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE EXPORTS
# ═══════════════════════════════════════════════════════════════════════════════

__all__ = [
    "ABTestingRouter",
    "Experiment",
    "ExperimentStatus",
    "ExperimentResult",
    "ExperimentStats",
    "get_ab_router",
    "REDIS_AVAILABLE",
]