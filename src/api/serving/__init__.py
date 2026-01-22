"""
Online Serving Layer
====================

Modular online feature serving and scoring infrastructure.

Submodules:
    features: Feature retrieval (Feast + Redis streaming)
    models: Model loading and selection (MLflow)
    scoring: Scoring pipeline with adjustments
    telemetry: Metrics and performance tracking

Usage:
    from src.api.serving import ScoringPipeline, RequestContext

    pipeline = ScoringPipeline()
    result = await pipeline.score(
        customer_id="cust_123",
        transaction_id="tx_456",
        request_features={"amount": 100.0, ...},
        context=RequestContext(apply_streaming_rules=True)
    )
"""

from src.api.serving.scoring.score_pipeline import (
    ScoringPipeline,
    ScoringResult,
    RequestContext,
)

from src.api.serving.features.feature_service import FeatureService
from src.api.serving.models.model_loader import ModelLoader
from src.api.serving.telemetry.observer import TelemetryObserver

__all__ = [
    "ScoringPipeline",
    "ScoringResult",
    "RequestContext",
    "FeatureService",
    "ModelLoader",
    "TelemetryObserver",
]
