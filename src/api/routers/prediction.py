"""
Prediction Router
=================

Handles fraud prediction endpoints:
- POST /predict - Single transaction prediction
- POST /predict/batch - Batch prediction
- POST /evaluate - Multi-model evaluation

Uses the ScoringPipeline from src.api.serving for inference.
"""

import asyncio
import time
from datetime import datetime
from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel, Field

from src.api.errors import (
    APIError,
    ModelNotFoundError,
    InferenceError,
    FeatureStoreError,
)
from src.core.api.models import (
    TransactionRequest,
    PredictionResponse,
    BatchRequest,
    BatchResponse,
)

router = APIRouter(prefix="/predict", tags=["Prediction"])


# =============================================================================
# LAZY INITIALIZATION
# =============================================================================

_scoring_pipeline = None


def get_scoring_pipeline():
    """Lazy initialize scoring pipeline."""
    global _scoring_pipeline
    if _scoring_pipeline is None:
        try:
            from src.api.serving import ScoringPipeline
            _scoring_pipeline = ScoringPipeline()
        except Exception as e:
            raise FeatureStoreError(f"Failed to initialize scoring pipeline: {e}")
    return _scoring_pipeline


# =============================================================================
# ENDPOINTS
# =============================================================================

@router.post("", response_model=PredictionResponse)
async def predict(
    transaction: TransactionRequest,
    model_stage: Optional[str] = Query(None, description="Model stage: Production, Staging, etc."),
    run_id: Optional[str] = Query(None, description="Specific MLflow run ID"),
    streaming: bool = Query(True, description="Apply streaming velocity rules"),
):
    """
    Single transaction fraud prediction.

    By default uses Production model. Specify model_stage or run_id to use different model.

    Streaming Rules:
    - When streaming=true (default), real-time velocity features from ksqlDB/Redis
      are used to boost the ML score based on business rules
    - When streaming=false, only the ML model score is returned
    """
    start_time = time.time()

    try:
        from src.api.serving import RequestContext

        pipeline = get_scoring_pipeline()

        context = RequestContext(
            model_stage=model_stage,
            run_id=run_id,
            include_streaming=streaming,
            apply_streaming_rules=streaming,
        )

        result = pipeline.score(
            customer_id=transaction.customer_id,
            transaction_id=transaction.transaction_id,
            request_features=transaction.dict(),
            context=context,
        )

        total_latency_ms = (time.time() - start_time) * 1000

        # Build streaming features dict for response
        streaming_feats = None
        # DEBUG: Log streaming features status
        print(f"DEBUG: streaming_available={result.streaming_available}, streaming_features type={type(result.streaming_features)}, has_data={bool(result.streaming_features) if result.streaming_features else False}")
        if result.streaming_available and result.streaming_features:
            streaming_feats = {
                "tx_count_5min": result.streaming_features.get("tx_count_5min"),
                "tx_count_1h": result.streaming_features.get("tx_count_1h"),
                "tx_count_24h": result.streaming_features.get("tx_count_24h"),
                "velocity_score": result.streaming_features.get("velocity_score"),
                "high_velocity_flag": result.streaming_features.get("high_velocity_flag"),
                "address_mismatch": result.streaming_features.get("address_mismatch"),
                "is_night": result.streaming_features.get("is_night"),
                "risky_payment": result.streaming_features.get("risky_payment"),
            }

        return PredictionResponse(
            transaction_id=result.transaction_id,
            fraud_score=round(result.fraud_score, 4),
            is_fraud=result.is_fraud,
            risk_level=result.risk_level,
            model_name=result.model_name,
            model_version=result.model_version,
            model_stage=result.model_stage,
            run_id=result.run_id,
            contract_version=result.contract_version,
            schema_version=result.schema_version,  # SCHEMA EVOLUTION
            features_source=result.feature_source,
            feature_latency_ms=round(result.feature_latency_ms, 2),
            inference_latency_ms=round(result.inference_latency_ms, 2),
            total_latency_ms=round(total_latency_ms, 2),
            timestamp=datetime.now().isoformat(),
            warnings=result.warnings,
            ml_score=round(result.ml_score, 4) if result.ml_score is not None else None,
            streaming_boost=round(result.streaming_boost, 4) if result.streaming_boost is not None else 0.0,
            triggered_rules=result.triggered_rules or [],
            streaming_features_available=result.streaming_available,
            streaming_features=streaming_feats,
        )

    except APIError:
        raise
    except Exception as e:
        raise InferenceError(f"Prediction failed: {str(e)}")


@router.post("/batch", response_model=BatchResponse)
async def predict_batch(request: BatchRequest):
    """
    Batch prediction for multiple transactions.

    OPTIMIZED: Uses asyncio.gather for parallel processing,
    significantly improving throughput for large batches.
    """
    start_time = time.time()
    pipeline = get_scoring_pipeline()

    async def score_single(tx: TransactionRequest) -> Optional[PredictionResponse]:
        """Score a single transaction in thread pool."""
        try:
            from src.api.serving import RequestContext

            context = RequestContext(
                include_streaming=True,
                apply_streaming_rules=True,
            )

            # Run synchronous scoring in thread pool to not block event loop
            result = await asyncio.to_thread(
                pipeline.score,
                tx.customer_id,
                tx.transaction_id,
                tx.dict(),
                context,
            )

            return PredictionResponse(
                transaction_id=result.transaction_id,
                fraud_score=round(result.fraud_score, 4),
                is_fraud=result.is_fraud,
                risk_level=result.risk_level,
                model_name=result.model_name,
                model_version=result.model_version,
                model_stage=result.model_stage,
                run_id=result.run_id,
                contract_version=result.contract_version,
                schema_version=result.schema_version,  # SCHEMA EVOLUTION
                features_source=result.feature_source,
                feature_latency_ms=round(result.feature_latency_ms, 2),
                inference_latency_ms=round(result.inference_latency_ms, 2),
                total_latency_ms=round(result.total_latency_ms, 2),
                timestamp=datetime.now().isoformat(),
                warnings=result.warnings,
                ml_score=round(result.ml_score, 4) if result.ml_score else None,
                streaming_boost=round(result.streaming_boost, 4) if result.streaming_boost else None,
                triggered_rules=result.triggered_rules,
                streaming_features_available=result.streaming_available,
            )
        except Exception:
            return None

    # Process all transactions in parallel
    tasks = [score_single(tx) for tx in request.transactions]
    all_results = await asyncio.gather(*tasks)

    # Filter out failed predictions
    predictions = [r for r in all_results if r is not None]
    error_count = len(all_results) - len(predictions)

    total_latency_ms = (time.time() - start_time) * 1000

    return BatchResponse(
        predictions=predictions,
        total_count=len(request.transactions),
        success_count=len(predictions),
        error_count=error_count,
        total_latency_ms=round(total_latency_ms, 2),
        timestamp=datetime.now().isoformat(),
    )
