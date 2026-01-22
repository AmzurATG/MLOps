"""
Fraud Detection API - Production Grade
================================================================================

A simplified, modular fraud detection API using the serving layer.

SOLID Principles Applied:
- Dependency Inversion: Uses FastAPI Depends() for DI
- Single Responsibility: Endpoints only handle HTTP concerns
- Open/Closed: Feature sources can be extended without modification

Endpoints:
  POST /predict              - Single prediction
  POST /predict/batch        - Batch prediction
  POST /evaluate             - Multi-model evaluation
  GET  /models               - List available models
  GET  /features/{customer_id} - Get customer features
  GET  /health               - Health check
  GET  /metrics              - Prometheus metrics

  # Explainability
  POST /explain              - Get explanation with prediction

  # A/B Testing
  GET  /experiments          - List experiments
  POST /experiments          - Create experiment
  POST /experiments/{name}/start - Start experiment
  POST /experiments/{name}/stop  - Stop experiment
  GET  /experiments/{name}/stats - Get experiment statistics

  # Shadow Mode
  GET  /shadow/status        - Shadow mode status
  POST /shadow/enable        - Enable shadow mode
  POST /shadow/disable       - Disable shadow mode

  # Streaming
  GET  /streaming/status     - Streaming rules status
  GET  /streaming/features/{customer_id} - Get streaming features
"""

import asyncio
import time
from datetime import datetime
from typing import Dict, List, Optional, Any

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.responses import Response
from pydantic import BaseModel, Field

from src.api.settings import api_settings, APISettings
from src.core.api.models import TransactionRequest, PredictionResponse
from src.api.dependencies import (
    get_settings,
    get_scoring_pipeline,
    get_feature_service,
    get_ab_router,
    get_shadow_predictor,
    get_explainer,
)

app = FastAPI(
    title="Fraud Detection API",
    version="5.0.0",
    description="Production fraud detection with modular architecture",
)

# =============================================================================
# REQUEST/RESPONSE MODELS (TransactionRequest, PredictionResponse imported from src.core.api.models)
# =============================================================================

class EvaluationRequest(BaseModel):
    """Request for multi-model evaluation."""
    transaction: TransactionRequest
    model_stages: List[str] = Field(default=["Production", "Staging"], description="Model stages to evaluate")
    run_ids: List[str] = Field(default=[], description="Specific run IDs to evaluate")


class ModelPrediction(BaseModel):
    """Single model prediction in evaluation."""
    model_name: str
    model_version: str
    model_stage: str
    run_id: Optional[str] = None
    fraud_score: float
    is_fraud: bool
    risk_level: str
    inference_latency_ms: float


class EvaluationResponse(BaseModel):
    """Multi-model evaluation response."""
    transaction_id: str
    predictions: List[ModelPrediction]
    feature_source: str
    feature_latency_ms: float
    total_latency_ms: float
    timestamp: str


class BatchEvaluationRequest(BaseModel):
    """Batch evaluation request."""
    transactions: List[EvaluationRequest] = Field(..., description="List of evaluation requests")


class BatchEvaluationResponse(BaseModel):
    """Batch evaluation response."""
    results: List[EvaluationResponse]
    total_count: int
    success_count: int
    error_count: int
    total_latency_ms: float
    avg_latency_per_tx_ms: float
    timestamp: str


# =============================================================================
# TYPE ALIASES FOR DEPENDENCIES
# =============================================================================

# Type aliases for better documentation
from src.api.serving import ScoringPipeline
from src.api.serving.features import FeatureService


# =============================================================================
# CORE ENDPOINTS
# =============================================================================

@app.get("/")
def root():
    """API info."""
    return {
        "name": "Fraud Detection API",
        "version": "5.0.0",
        "description": "Production fraud detection with modular architecture",
    }


@app.get("/health")
def health(pipeline: ScoringPipeline = Depends(get_scoring_pipeline)):
    """Health check."""
    status = {"status": "healthy", "timestamp": datetime.now().isoformat()}
    try:
        health_info = pipeline.health_check()
        status["components"] = health_info
    except Exception as e:
        status["status"] = "degraded"
        status["error"] = str(e)[:100]
    return status


@app.post("/predict", response_model=PredictionResponse)
def predict(
    transaction: TransactionRequest,
    model_stage: Optional[str] = Query(None, description="Model stage"),
    run_id: Optional[str] = Query(None, description="MLflow run ID"),
    streaming: bool = Query(True, alias="streaming", description="Apply streaming rules"),
    pipeline: ScoringPipeline = Depends(get_scoring_pipeline),
):
    """Single transaction fraud prediction."""
    try:
        from src.api.serving import RequestContext

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

        # Build streaming features dict for response
        streaming_feats = None
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
            schema_version=result.schema_version,
            features_source=result.feature_source,
            feature_latency_ms=round(result.feature_latency_ms, 2),
            inference_latency_ms=round(result.inference_latency_ms, 2),
            total_latency_ms=round(result.total_latency_ms, 2),
            timestamp=datetime.now().isoformat(),
            warnings=result.warnings,
            ml_score=round(result.ml_score, 4) if result.ml_score is not None else None,
            streaming_boost=round(result.streaming_boost, 4) if result.streaming_boost is not None else 0.0,
            triggered_rules=result.triggered_rules or [],
            streaming_features_available=result.streaming_available,
            streaming_features=streaming_feats,
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")


@app.post("/predict/batch", response_model=List[PredictionResponse])
async def predict_batch(
    transactions: List[TransactionRequest],
    pipeline: ScoringPipeline = Depends(get_scoring_pipeline),
    settings: APISettings = Depends(get_settings),
):
    """
    Batch prediction with parallel processing.

    OPTIMIZED: Uses asyncio.gather to process transactions in parallel,
    significantly improving throughput for large batches.
    """
    if len(transactions) > settings.max_batch_size:
        raise HTTPException(
            status_code=400,
            detail=f"Max {settings.max_batch_size} transactions per batch"
        )

    async def predict_single(tx: TransactionRequest) -> PredictionResponse:
        """Process single transaction in thread pool."""
        try:
            # Run synchronous predict in thread pool to not block event loop
            return await asyncio.to_thread(
                predict,
                tx,
                None,  # model_stage
                None,  # run_id
                True,  # streaming
                pipeline,
            )
        except Exception as e:
            return PredictionResponse(
                transaction_id=tx.transaction_id,
                fraud_score=0.0,
                is_fraud=False,
                risk_level="ERROR",
                model_name=settings.model_name,
                model_version="error",
                model_stage="error",
                features_source="error",
                feature_latency_ms=0,
                inference_latency_ms=0,
                total_latency_ms=0,
                timestamp=datetime.now().isoformat(),
                warnings=[str(e)],
            )

    # Process all transactions in parallel
    tasks = [predict_single(tx) for tx in transactions]
    results = await asyncio.gather(*tasks)

    return list(results)


# =============================================================================
# EVALUATE ENDPOINTS (Multi-model comparison)
# =============================================================================

@app.post("/evaluate", response_model=EvaluationResponse)
def evaluate(
    request: EvaluationRequest,
    pipeline: ScoringPipeline = Depends(get_scoring_pipeline),
    feature_service: FeatureService = Depends(get_feature_service),
):
    """
    Multi-model evaluation for a single transaction.

    Evaluates the transaction against multiple model stages/versions
    and returns predictions from each for comparison.
    """
    start_time = time.time()
    tx = request.transaction
    predictions = []

    try:
        from src.api.serving import RequestContext

        # Fetch features once (shared across all model evaluations)
        feature_vector = feature_service.get_features(
            customer_id=tx.customer_id,
            transaction_id=tx.transaction_id,
            include_streaming=True,
        )
        feature_latency_ms = feature_vector.total_latency_ms

        # Evaluate each model stage
        for stage in request.model_stages:
            try:
                context = RequestContext(
                    model_stage=stage,
                    include_streaming=True,
                    apply_streaming_rules=False,  # Raw ML score for evaluation
                )

                result = pipeline.score(
                    customer_id=tx.customer_id,
                    transaction_id=tx.transaction_id,
                    request_features=tx.dict(),
                    context=context,
                )

                predictions.append(ModelPrediction(
                    model_name=result.model_name,
                    model_version=result.model_version,
                    model_stage=result.model_stage,
                    run_id=result.run_id,
                    fraud_score=round(result.fraud_score, 4),
                    is_fraud=result.is_fraud,
                    risk_level=result.risk_level,
                    inference_latency_ms=round(result.inference_latency_ms, 2),
                ))
            except Exception as e:
                print(f"[Evaluate] Model {stage} failed: {e}", flush=True)

        # Evaluate specific run IDs
        for run_id in request.run_ids:
            try:
                context = RequestContext(
                    run_id=run_id,
                    include_streaming=True,
                    apply_streaming_rules=False,
                )

                result = pipeline.score(
                    customer_id=tx.customer_id,
                    transaction_id=tx.transaction_id,
                    request_features=tx.dict(),
                    context=context,
                )

                predictions.append(ModelPrediction(
                    model_name=result.model_name,
                    model_version=result.model_version,
                    model_stage=result.model_stage,
                    run_id=result.run_id,
                    fraud_score=round(result.fraud_score, 4),
                    is_fraud=result.is_fraud,
                    risk_level=result.risk_level,
                    inference_latency_ms=round(result.inference_latency_ms, 2),
                ))
            except Exception as e:
                print(f"[Evaluate] Run {run_id} failed: {e}", flush=True)

        total_latency_ms = (time.time() - start_time) * 1000

        return EvaluationResponse(
            transaction_id=tx.transaction_id,
            predictions=predictions,
            feature_source=feature_vector.source,
            feature_latency_ms=round(feature_latency_ms, 2),
            total_latency_ms=round(total_latency_ms, 2),
            timestamp=datetime.now().isoformat(),
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Evaluation failed: {str(e)}")


@app.post("/evaluate/batch", response_model=BatchEvaluationResponse)
async def evaluate_batch(
    request: BatchEvaluationRequest,
    pipeline: ScoringPipeline = Depends(get_scoring_pipeline),
    feature_service: FeatureService = Depends(get_feature_service),
    settings: APISettings = Depends(get_settings),
):
    """
    Batch multi-model evaluation for multiple transactions.

    OPTIMIZED: Uses asyncio.gather for parallel transaction processing.
    Significantly faster than sequential /evaluate calls.
    """
    start_time = time.time()

    if len(request.transactions) > settings.max_eval_batch_size:
        raise HTTPException(
            status_code=400,
            detail=f"Max {settings.max_eval_batch_size} transactions per batch"
        )

    try:
        from src.api.serving import RequestContext

        async def evaluate_single(eval_req: EvaluationRequest) -> Optional[EvaluationResponse]:
            """Evaluate single transaction in thread pool."""
            tx = eval_req.transaction
            tx_start = time.time()
            predictions = []

            try:
                # Run synchronous feature fetch in thread pool
                feature_vector = await asyncio.to_thread(
                    feature_service.get_features,
                    tx.customer_id,
                    tx.transaction_id,
                    True,  # include_streaming
                )

                # Evaluate each model stage
                for stage in eval_req.model_stages:
                    try:
                        context = RequestContext(
                            model_stage=stage,
                            include_streaming=True,
                            apply_streaming_rules=False,
                        )

                        result = await asyncio.to_thread(
                            pipeline.score,
                            tx.customer_id,
                            tx.transaction_id,
                            tx.dict(),
                            context,
                        )

                        predictions.append(ModelPrediction(
                            model_name=result.model_name,
                            model_version=result.model_version,
                            model_stage=result.model_stage,
                            run_id=result.run_id,
                            fraud_score=round(result.fraud_score, 4),
                            is_fraud=result.is_fraud,
                            risk_level=result.risk_level,
                            inference_latency_ms=round(result.inference_latency_ms, 2),
                        ))
                    except Exception:
                        pass

                # Evaluate specific run IDs
                for run_id in eval_req.run_ids:
                    try:
                        context = RequestContext(
                            run_id=run_id,
                            include_streaming=True,
                            apply_streaming_rules=False,
                        )

                        result = await asyncio.to_thread(
                            pipeline.score,
                            tx.customer_id,
                            tx.transaction_id,
                            tx.dict(),
                            context,
                        )

                        predictions.append(ModelPrediction(
                            model_name=result.model_name,
                            model_version=result.model_version,
                            model_stage=result.model_stage,
                            run_id=result.run_id,
                            fraud_score=round(result.fraud_score, 4),
                            is_fraud=result.is_fraud,
                            risk_level=result.risk_level,
                            inference_latency_ms=round(result.inference_latency_ms, 2),
                        ))
                    except Exception:
                        pass

                tx_latency_ms = (time.time() - tx_start) * 1000

                return EvaluationResponse(
                    transaction_id=tx.transaction_id,
                    predictions=predictions,
                    feature_source=feature_vector.source,
                    feature_latency_ms=round(feature_vector.total_latency_ms, 2),
                    total_latency_ms=round(tx_latency_ms, 2),
                    timestamp=datetime.now().isoformat(),
                )

            except Exception as e:
                print(f"[Evaluate Batch] Transaction {tx.transaction_id} failed: {e}", flush=True)
                return None

        # Process all transactions in parallel
        tasks = [evaluate_single(eval_req) for eval_req in request.transactions]
        all_results = await asyncio.gather(*tasks)

        # Filter out failed evaluations
        results = [r for r in all_results if r is not None]
        errors = len(all_results) - len(results)

        total_latency_ms = (time.time() - start_time) * 1000

        return BatchEvaluationResponse(
            results=results,
            total_count=len(request.transactions),
            success_count=len(results),
            error_count=errors,
            total_latency_ms=round(total_latency_ms, 2),
            avg_latency_per_tx_ms=round(total_latency_ms / len(request.transactions), 2) if request.transactions else 0,
            timestamp=datetime.now().isoformat(),
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Batch evaluation failed: {str(e)}")


# =============================================================================
# FEATURES ENDPOINT
# =============================================================================

@app.get("/features/{customer_id}")
def get_features(
    customer_id: str,
    transaction_id: Optional[str] = Query(None),
    include_streaming: bool = Query(True),
    feature_service: FeatureService = Depends(get_feature_service),
):
    """Get customer features from Feast and Redis."""
    try:
        features = feature_service.get_features(
            customer_id=customer_id,
            transaction_id=transaction_id,
            include_streaming=include_streaming,
        )
        return {
            "customer_id": customer_id,
            "features": features.merged,
            "feature_count": len(features.merged),
            "source": features.source,
            "streaming_available": features.streaming_available,
            "latency_ms": round(features.total_latency_ms, 2),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# STREAMING ENDPOINTS
# =============================================================================

@app.get("/streaming/status")
def streaming_status(settings: APISettings = Depends(get_settings)):
    """Get streaming rules status."""
    try:
        from src.api.streaming_rules import THRESHOLDS
        import redis
        r = redis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            decode_responses=True,
        )
        r.ping()
        return {"available": True, "redis_connected": True, "thresholds": THRESHOLDS}
    except Exception as e:
        return {"available": False, "error": str(e)[:100]}


@app.get("/streaming/features/{customer_id}")
async def get_streaming_features(customer_id: str):
    """
    Get streaming features for a customer.

    OPTIMIZED: Uses async Redis client for non-blocking I/O.
    """
    try:
        from src.api.serving.features.redis_client import AsyncStreamingRedisClient

        # Use async client for non-blocking Redis access
        client = AsyncStreamingRedisClient()
        features, latency_ms, success = await client.get_streaming_features(customer_id)

        return {
            "customer_id": customer_id,
            "features": features,
            "found": success,
            "latency_ms": round(latency_ms, 2),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# A/B TESTING ENDPOINTS
# =============================================================================

@app.get("/experiments")
def list_experiments(router=Depends(get_ab_router)):
    """List all A/B experiments."""
    if not router:
        raise HTTPException(status_code=503, detail="A/B testing not available")
    return [e.to_dict() for e in router.list_experiments()]


@app.post("/experiments")
def create_experiment(
    name: str,
    control_version: str,
    treatment_version: str,
    traffic_split: float = 0.1,
    description: str = "",
    router=Depends(get_ab_router),
):
    """Create a new A/B experiment."""
    if not router:
        raise HTTPException(status_code=503, detail="A/B testing not available")

    try:
        experiment = router.create_experiment(
            name=name,
            control_version=control_version,
            treatment_version=treatment_version,
            traffic_split=traffic_split,
            description=description,
        )
        return experiment.to_dict()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/experiments/{name}/start")
def start_experiment(name: str, router=Depends(get_ab_router)):
    """Start an experiment."""
    if not router:
        raise HTTPException(status_code=503, detail="A/B testing not available")
    try:
        return router.start_experiment(name).to_dict()
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.post("/experiments/{name}/stop")
def stop_experiment(name: str, router=Depends(get_ab_router)):
    """Stop an experiment."""
    if not router:
        raise HTTPException(status_code=503, detail="A/B testing not available")
    try:
        return router.stop_experiment(name).to_dict()
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/experiments/{name}/stats")
def get_experiment_stats(name: str, router=Depends(get_ab_router)):
    """Get experiment statistics."""
    if not router:
        raise HTTPException(status_code=503, detail="A/B testing not available")

    experiment = router.get_experiment(name)
    if not experiment:
        raise HTTPException(status_code=404, detail=f"Experiment not found: {name}")

    return router.get_experiment_stats(name).to_dict()


# =============================================================================
# SHADOW MODE ENDPOINTS
# =============================================================================

@app.get("/shadow/status")
def shadow_status(predictor=Depends(get_shadow_predictor)):
    """Get shadow mode status."""
    if not predictor:
        return {"available": False}
    return predictor.get_status()


@app.post("/shadow/enable")
def enable_shadow(predictor=Depends(get_shadow_predictor)):
    """Enable shadow mode."""
    if not predictor:
        raise HTTPException(status_code=503, detail="Shadow mode not available")
    predictor.enable()
    return {"status": "enabled"}


@app.post("/shadow/disable")
def disable_shadow(predictor=Depends(get_shadow_predictor)):
    """Disable shadow mode."""
    if not predictor:
        raise HTTPException(status_code=503, detail="Shadow mode not available")
    predictor.disable()
    return {"status": "disabled"}


@app.get("/shadow/stats")
def shadow_stats(predictor=Depends(get_shadow_predictor)):
    """Get shadow mode statistics."""
    if not predictor:
        raise HTTPException(status_code=503, detail="Shadow mode not available")
    return predictor.get_stats().to_dict()


# =============================================================================
# EXPLAINABILITY ENDPOINTS
# =============================================================================

@app.post("/explain")
def explain(
    transaction: TransactionRequest,
    top_k: int = Query(5, ge=1, le=10),
    pipeline: ScoringPipeline = Depends(get_scoring_pipeline),
):
    """Get prediction explanation with SHAP values."""
    try:
        from src.api.explainability import FraudExplainer, SHAP_AVAILABLE
        if not SHAP_AVAILABLE:
            raise HTTPException(status_code=503, detail="SHAP not available")

        # Get prediction first
        from src.api.serving import RequestContext
        context = RequestContext(include_features=True)

        result = pipeline.score(
            customer_id=transaction.customer_id,
            transaction_id=transaction.transaction_id,
            request_features=transaction.dict(),
            context=context,
        )

        # Get or create explainer using DI helper
        explainer = get_explainer(result.model_version)
        if not explainer:
            raise HTTPException(status_code=503, detail="Explainer not available")

        # Build features for explanation
        import pandas as pd
        features_df = pd.DataFrame([result.features]) if result.features else None
        if features_df is None:
            return {
                "transaction_id": transaction.transaction_id,
                "prediction": result.fraud_score,
                "risk_level": result.risk_level,
                "explanation": "Features not available for explanation",
            }

        explanation = explainer.explain_prediction(
            features=features_df,
            transaction_id=transaction.transaction_id,
            top_k=top_k,
        )

        return {
            "transaction_id": explanation.transaction_id,
            "prediction": float(explanation.prediction),
            "risk_level": result.risk_level,
            "base_value": float(explanation.base_value),
            "risk_factors": explanation.risk_factors,
            "protective_factors": explanation.protective_factors,
            "top_features": [
                {
                    "feature": f.feature,
                    "value": float(f.value) if f.value else None,
                    "contribution": float(f.contribution),
                    "direction": f.direction,
                }
                for f in explanation.top_features
            ],
            "explanation_text": explanation.explanation_text,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Explanation failed: {str(e)}")


# =============================================================================
# METRICS ENDPOINT
# =============================================================================

@app.get("/metrics")
def metrics():
    """Prometheus metrics endpoint."""
    try:
        from src.api.metrics import get_metrics_response
        content, content_type = get_metrics_response()
        return Response(content=content, media_type=content_type)
    except Exception as e:
        return Response(content=f"# Error: {e}", media_type="text/plain")


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=api_settings.api_host, port=api_settings.api_port)
