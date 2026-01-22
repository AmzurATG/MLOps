"""
Scoring Pipeline
================

Orchestrates the full scoring flow:
1. Feature retrieval
2. Model selection
3. Inference
4. Score adjustments (streaming rules)
5. Telemetry recording

Usage:
    from src.api.serving.scoring import ScoringPipeline, RequestContext

    pipeline = ScoringPipeline()
    result = await pipeline.score(
        customer_id="cust_123",
        transaction_id="tx_456",
        request_features={"amount": 100.0, ...},
    )
"""

from src.api.serving.scoring.score_pipeline import (
    ScoringPipeline,
    ScoringResult,
    RequestContext,
)
from src.api.serving.scoring.adjustments import (
    ScoreAdjuster,
    AdjustmentResult,
)

__all__ = [
    "ScoringPipeline",
    "ScoringResult",
    "RequestContext",
    "ScoreAdjuster",
    "AdjustmentResult",
]
