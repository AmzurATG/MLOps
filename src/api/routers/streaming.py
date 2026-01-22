"""
Streaming Router
================

Handles streaming features endpoints:
- GET /streaming/status - Streaming rules status
- GET /streaming/features/{customer_id} - Get streaming features from Redis
- POST /streaming/test-rules - Test streaming rules

Provides access to real-time velocity features computed by ksqlDB.
"""

import os
from typing import Dict, Any, Optional, List
from datetime import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

router = APIRouter(prefix="/streaming", tags=["Streaming"])


# =============================================================================
# RESPONSE MODELS
# =============================================================================

class StreamingStatus(BaseModel):
    """Streaming rules status."""
    enabled: bool
    redis_connected: bool
    redis_host: str
    redis_port: int
    prefix: str
    velocity_thresholds: Dict[str, int]
    timestamp: str


class StreamingFeatures(BaseModel):
    """Streaming features response."""
    customer_id: str
    features: Dict[str, Any]
    found: bool
    ttl_seconds: int
    timestamp: str


class TestRulesRequest(BaseModel):
    """Test streaming rules request."""
    customer_id: str
    ml_score: float = Field(0.5, ge=0.0, le=1.0)


class TestRulesResponse(BaseModel):
    """Test streaming rules response."""
    customer_id: str
    ml_score: float
    final_score: float
    streaming_boost: float
    triggered_rules: List[str]
    streaming_features: Dict[str, Any]
    streaming_available: bool
    timestamp: str


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def _get_streaming_settings():
    """Get streaming settings from unified config."""
    try:
        from src.core.config import settings
        return settings.mlops
    except ImportError:
        return None


def _get_redis_client():
    """Get Redis client for streaming features."""
    try:
        from src.api.serving.features.redis_client import StreamingRedisClient
        return StreamingRedisClient()
    except ImportError:
        return None


def _get_score_adjuster():
    """Get score adjuster for testing rules."""
    try:
        from src.api.serving.scoring.adjustments import ScoreAdjuster
        return ScoreAdjuster()
    except ImportError:
        return None


# =============================================================================
# ENDPOINTS
# =============================================================================

@router.get("/status", response_model=StreamingStatus)
async def streaming_status():
    """
    Get streaming rules status.

    Returns configuration and connection status for streaming features.
    """
    settings = _get_streaming_settings()
    redis_client = _get_redis_client()

    redis_connected = False
    redis_host = os.getenv("REDIS_HOST", "exp-redis")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))

    if redis_client:
        redis_connected = redis_client.is_available()
        redis_host = redis_client.config.host
        redis_port = redis_client.config.port

    velocity_thresholds = {}
    if settings:
        velocity_thresholds = settings.velocity_thresholds
    else:
        velocity_thresholds = {
            "high_5min": 5,
            "medium_5min": 3,
            "high_1h": 20,
            "medium_1h": 10,
            "high_24h": 50,
        }

    return StreamingStatus(
        enabled=settings.streaming_enabled if settings else True,
        redis_connected=redis_connected,
        redis_host=redis_host,
        redis_port=redis_port,
        prefix=settings.streaming_redis_prefix if settings else "feast:streaming:",
        velocity_thresholds=velocity_thresholds,
        timestamp=datetime.now().isoformat(),
    )


@router.get("/features/{customer_id}", response_model=StreamingFeatures)
async def get_streaming_features(customer_id: str):
    """
    Get streaming features for a customer from Redis.

    These are real-time velocity features computed by ksqlDB.
    """
    redis_client = _get_redis_client()

    if not redis_client:
        raise HTTPException(
            status_code=503,
            detail="Redis client not available"
        )

    features, latency_ms, success = redis_client.get_streaming_features(customer_id)
    ttl = redis_client.get_ttl(customer_id) if success else -1

    return StreamingFeatures(
        customer_id=customer_id,
        features=features,
        found=bool(features),
        ttl_seconds=ttl,
        timestamp=datetime.now().isoformat(),
    )


@router.post("/test-rules", response_model=TestRulesResponse)
async def test_streaming_rules(request: TestRulesRequest):
    """
    Test streaming rules for a customer.

    Fetches streaming features and applies rules to see what would trigger.
    Useful for debugging and understanding rule behavior.
    """
    redis_client = _get_redis_client()
    adjuster = _get_score_adjuster()

    if not redis_client:
        raise HTTPException(
            status_code=503,
            detail="Redis client not available"
        )

    if not adjuster:
        raise HTTPException(
            status_code=503,
            detail="Score adjuster not available"
        )

    # Get streaming features
    features, latency_ms, success = redis_client.get_streaming_features(request.customer_id)

    # Apply rules
    result = adjuster.apply(request.ml_score, features)

    return TestRulesResponse(
        customer_id=request.customer_id,
        ml_score=request.ml_score,
        final_score=result.final_score,
        streaming_boost=result.total_boost,
        triggered_rules=result.triggered_rules,
        streaming_features=result.streaming_features_used,
        streaming_available=result.streaming_available,
        timestamp=datetime.now().isoformat(),
    )


@router.get("/redis/stats")
async def redis_stats():
    """
    Get Redis connection and memory stats.
    """
    redis_client = _get_redis_client()

    if not redis_client:
        return {"status": "unavailable", "error": "Redis client not configured"}

    return redis_client.get_stats()
