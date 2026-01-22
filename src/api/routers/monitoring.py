"""
Monitoring Router
=================

Handles health and monitoring endpoints:
- GET /health - Health check
- GET /metrics - Prometheus metrics
- GET /ready - Readiness probe

Provides observability for the fraud detection API.
"""

import os
from typing import Dict, Any, Optional
from datetime import datetime

from fastapi import APIRouter, Response
from pydantic import BaseModel

router = APIRouter(tags=["Monitoring"])


# =============================================================================
# RESPONSE MODELS
# =============================================================================

class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    components: Dict[str, str]
    cached_models: int
    version: str
    timestamp: str


class ReadinessResponse(BaseModel):
    """Readiness check response."""
    ready: bool
    checks: Dict[str, bool]
    timestamp: str


# =============================================================================
# LAZY INITIALIZATION
# =============================================================================

_feast_store = None
_mlflow_client = None
_model_cache_count = 0


def _check_feast():
    """Check Feast availability."""
    global _feast_store
    try:
        from feast import FeatureStore
        feast_path = os.getenv("FEAST_REPO_PATH", "/app/feast_repo")
        if _feast_store is None:
            _feast_store = FeatureStore(repo_path=feast_path)
        return "ok"
    except Exception as e:
        return f"error: {str(e)[:50]}"


def _check_mlflow():
    """Check MLflow availability."""
    global _mlflow_client
    try:
        import mlflow
        uri = os.getenv("MLFLOW_TRACKING_URI", "http://exp-mlflow:5000")
        mlflow.set_tracking_uri(uri)
        if _mlflow_client is None:
            _mlflow_client = mlflow.tracking.MlflowClient()
        return "ok"
    except Exception as e:
        return f"error: {str(e)[:50]}"


def _check_redis():
    """Check Redis availability."""
    try:
        from src.api.serving.features.redis_client import StreamingRedisClient
        client = StreamingRedisClient()
        if client.is_available():
            return "ok"
        return "error: connection failed"
    except Exception as e:
        return f"error: {str(e)[:50]}"


def _check_model():
    """Check if model is loadable."""
    try:
        from src.api.serving.models.model_loader import ModelLoader
        loader = ModelLoader()
        loader.load(model_stage="Production")
        return "ok"
    except Exception as e:
        return f"error: {str(e)[:50]}"


def _get_cached_model_count():
    """Get count of cached models."""
    try:
        from src.api.serving.models.model_loader import ModelLoader
        loader = ModelLoader()
        return loader.cached_count
    except Exception:
        return 0


# =============================================================================
# ENDPOINTS
# =============================================================================

@router.get("/", include_in_schema=False)
async def root():
    """API info."""
    return {
        "name": "Fraud Detection API",
        "version": "5.0.0",
        "description": "Production fraud detection with modular routers",
        "endpoints": {
            "prediction": "/predict",
            "streaming": "/streaming",
            "health": "/health",
            "metrics": "/metrics",
        },
    }


@router.get("/health", response_model=HealthResponse)
async def health():
    """
    Health check with component status.

    Returns status of all dependent services:
    - Feast (feature store)
    - MLflow (model registry)
    - Redis (streaming features)
    - Model (production model loaded)
    """
    components = {}
    status = "healthy"

    # Check each component
    components["feast"] = _check_feast()
    if "error" in components["feast"]:
        status = "degraded"

    components["mlflow"] = _check_mlflow()
    if "error" in components["mlflow"]:
        status = "degraded"

    components["redis"] = _check_redis()
    if "error" in components["redis"]:
        status = "degraded"

    components["model"] = _check_model()
    if "error" in components["model"]:
        status = "degraded"

    return HealthResponse(
        status=status,
        components=components,
        cached_models=_get_cached_model_count(),
        version="5.0.0",
        timestamp=datetime.now().isoformat(),
    )


@router.get("/ready", response_model=ReadinessResponse)
async def ready():
    """
    Readiness probe for Kubernetes.

    Returns whether the service is ready to accept requests.
    """
    checks = {
        "feast": _check_feast() == "ok",
        "mlflow": _check_mlflow() == "ok",
        "model": _check_model() == "ok",
    }

    all_ready = all(checks.values())

    return ReadinessResponse(
        ready=all_ready,
        checks=checks,
        timestamp=datetime.now().isoformat(),
    )


@router.get("/live")
async def live():
    """
    Liveness probe for Kubernetes.

    Simple check that the service is running.
    """
    return {"alive": True, "timestamp": datetime.now().isoformat()}


@router.get("/metrics")
async def metrics():
    """
    Prometheus metrics endpoint.

    Returns metrics in Prometheus text format.
    """
    try:
        from src.api.metrics import get_metrics_response
        return get_metrics_response()
    except ImportError:
        # Fallback if metrics module not available
        from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
        return Response(
            content=generate_latest(),
            media_type=CONTENT_TYPE_LATEST,
        )
    except Exception as e:
        return Response(
            content=f"# Error generating metrics: {e}",
            media_type="text/plain",
        )


@router.get("/info")
async def info():
    """
    Get API information and configuration.
    """
    try:
        from pipelines.settings import settings

        return {
            "api": {
                "name": "Fraud Detection API",
                "version": "5.0.0",
            },
            "config": {
                "mlflow_uri": settings.infra.mlflow_tracking_uri,
                "feast_repo": settings.mlops.feast_repo_path,
                "redis_host": settings.infra.redis_host,
                "streaming_enabled": settings.mlops.streaming_enabled,
            },
            "timestamp": datetime.now().isoformat(),
        }
    except ImportError:
        return {
            "api": {
                "name": "Fraud Detection API",
                "version": "5.0.0",
            },
            "config": "settings not available",
            "timestamp": datetime.now().isoformat(),
        }
