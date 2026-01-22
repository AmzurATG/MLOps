"""
Model Selector
==============

Selects which model to use based on:
- A/B testing experiments
- Shadow mode configuration
- Explicit stage/run_id

This module provides the selection logic, not the model loading.
"""

import hashlib
import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from enum import Enum

from src.api.serving.models.model_loader import ModelLoader, LoadedModel

logger = logging.getLogger(__name__)


class SelectionStrategy(Enum):
    """Model selection strategies."""
    EXPLICIT = "explicit"          # Use explicitly specified model
    AB_TEST = "ab_test"           # Use A/B test assignment
    SHADOW = "shadow"             # Production + shadow model
    DEFAULT = "default"           # Use default (Production)


@dataclass
class SelectionContext:
    """Context for model selection."""
    customer_id: str
    transaction_id: Optional[str] = None

    # Explicit selection
    model_stage: Optional[str] = None
    run_id: Optional[str] = None

    # A/B testing
    experiment_name: Optional[str] = None

    # Shadow mode
    enable_shadow: bool = False


@dataclass
class SelectionResult:
    """Result of model selection."""
    primary_model: LoadedModel
    shadow_model: Optional[LoadedModel] = None
    strategy: SelectionStrategy = SelectionStrategy.DEFAULT
    experiment_variant: Optional[str] = None
    ab_bucket: Optional[int] = None

    @property
    def is_shadow_enabled(self) -> bool:
        return self.shadow_model is not None


class ModelSelector:
    """
    Selects models for inference based on context.

    Supports:
    - Explicit model selection (by stage or run_id)
    - A/B testing (consistent bucketing by customer_id)
    - Shadow mode (run two models, return one)
    """

    def __init__(
        self,
        model_loader: ModelLoader,
        ab_experiments: Optional[Dict[str, Dict[str, Any]]] = None,
        shadow_stage: str = "Staging",
    ):
        self.model_loader = model_loader
        self.ab_experiments = ab_experiments or {}
        self.shadow_stage = shadow_stage

    def _hash_to_bucket(self, value: str, num_buckets: int = 100) -> int:
        """Hash a value to a bucket number for consistent assignment."""
        hash_bytes = hashlib.md5(value.encode()).digest()
        hash_int = int.from_bytes(hash_bytes[:4], byteorder='big')
        return hash_int % num_buckets

    def _get_ab_variant(
        self,
        customer_id: str,
        experiment_name: str,
    ) -> Optional[str]:
        """Get A/B test variant for customer."""
        experiment = self.ab_experiments.get(experiment_name)
        if not experiment:
            return None

        if not experiment.get("active", False):
            return None

        variants = experiment.get("variants", {})
        if not variants:
            return None

        bucket = self._hash_to_bucket(customer_id)

        # Assign based on traffic split
        cumulative = 0
        for variant_name, config in variants.items():
            cumulative += config.get("traffic_percent", 0)
            if bucket < cumulative:
                return variant_name

        return None

    def select(self, context: SelectionContext) -> SelectionResult:
        """
        Select model(s) based on context.

        Args:
            context: Selection context with customer info and preferences

        Returns:
            SelectionResult with primary (and optionally shadow) model
        """
        # 1. Explicit selection takes precedence
        if context.run_id:
            model = self.model_loader.load(run_id=context.run_id)
            return SelectionResult(
                primary_model=model,
                strategy=SelectionStrategy.EXPLICIT,
            )

        if context.model_stage:
            model = self.model_loader.load(model_stage=context.model_stage)
            return SelectionResult(
                primary_model=model,
                strategy=SelectionStrategy.EXPLICIT,
            )

        # 2. A/B testing
        if context.experiment_name:
            variant = self._get_ab_variant(
                context.customer_id,
                context.experiment_name,
            )
            if variant:
                experiment = self.ab_experiments.get(context.experiment_name, {})
                variant_config = experiment.get("variants", {}).get(variant, {})
                model_stage = variant_config.get("model_stage", "Production")

                model = self.model_loader.load(model_stage=model_stage)
                return SelectionResult(
                    primary_model=model,
                    strategy=SelectionStrategy.AB_TEST,
                    experiment_variant=variant,
                    ab_bucket=self._hash_to_bucket(context.customer_id),
                )

        # 3. Shadow mode
        if context.enable_shadow:
            try:
                primary = self.model_loader.load(model_stage="Production")
                shadow = self.model_loader.load(model_stage=self.shadow_stage)
                return SelectionResult(
                    primary_model=primary,
                    shadow_model=shadow,
                    strategy=SelectionStrategy.SHADOW,
                )
            except Exception as e:
                logger.warning(f"Shadow model load failed: {e}")
                # Fall through to default

        # 4. Default: Production model
        model = self.model_loader.load(model_stage="Production")
        return SelectionResult(
            primary_model=model,
            strategy=SelectionStrategy.DEFAULT,
        )

    def register_experiment(
        self,
        name: str,
        variants: Dict[str, Dict[str, Any]],
        active: bool = False,
    ):
        """Register an A/B experiment."""
        self.ab_experiments[name] = {
            "name": name,
            "variants": variants,
            "active": active,
        }

    def list_experiments(self) -> List[Dict[str, Any]]:
        """List registered experiments."""
        return list(self.ab_experiments.values())

    def set_experiment_active(self, name: str, active: bool):
        """Activate or deactivate an experiment."""
        if name in self.ab_experiments:
            self.ab_experiments[name]["active"] = active
