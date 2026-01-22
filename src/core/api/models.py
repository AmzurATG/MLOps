"""
Shared API Models
=================

Pydantic models shared across all domain APIs (MLOps, CVOps, etc.)

Usage:
    from src.core.api.models import TransactionRequest, PredictionResponse
"""

from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


# =============================================================================
# FRAUD DETECTION MODELS
# =============================================================================

class TransactionRequest(BaseModel):
    """Input transaction for fraud prediction."""
    transaction_id: str = Field(..., description="Unique transaction ID")
    customer_id: str = Field(..., description="Customer ID for feature lookup")
    amount: float = Field(..., ge=0, description="Transaction amount")
    quantity: int = Field(1, ge=1, description="Item quantity")
    country: str = Field("US", description="Transaction country")
    device_type: str = Field("desktop", description="Device type")
    payment_method: str = Field("credit_card", description="Payment method")
    category: str = Field("Electronics", description="Product category")
    account_age_days: int = Field(0, ge=0, description="Account age in days")
    tx_hour: int = Field(12, ge=0, le=23, description="Transaction hour")
    tx_dayofweek: int = Field(1, ge=0, le=6, description="Day of week")
    ip_address: str = Field("0.0.0.0", description="IP address")
    shipping_country: Optional[str] = None
    billing_country: Optional[str] = None


class PredictionResponse(BaseModel):
    """Fraud prediction response."""
    transaction_id: str
    fraud_score: float
    is_fraud: bool
    risk_level: str
    model_name: str
    model_version: str
    model_stage: str
    run_id: Optional[str] = None
    contract_version: Optional[str] = None
    schema_version: Optional[str] = None
    features_source: str
    feature_latency_ms: float
    inference_latency_ms: float
    total_latency_ms: float
    timestamp: str
    warnings: List[str] = []
    # Streaming rule engine fields
    ml_score: Optional[float] = None
    streaming_boost: Optional[float] = None
    triggered_rules: Optional[List[str]] = None
    streaming_features_available: Optional[bool] = None
    streaming_features: Optional[Dict[str, Any]] = None  # Real-time velocity features


class BatchRequest(BaseModel):
    """Batch prediction request."""
    transactions: List[TransactionRequest]


class BatchResponse(BaseModel):
    """Batch prediction response."""
    predictions: List[PredictionResponse]
    total_count: int
    success_count: int
    error_count: int
    total_latency_ms: float
    timestamp: Optional[str] = None


class EvaluationRequest(BaseModel):
    """Request for multi-model evaluation."""
    transaction: TransactionRequest
    model_stages: List[str] = Field(
        default=["Production", "Staging"],
        description="Model stages to evaluate"
    )


class EvaluationResult(BaseModel):
    """Result from a single model evaluation."""
    model_stage: str
    fraud_score: float
    is_fraud: bool
    risk_level: str
    model_name: str
    model_version: str
    inference_latency_ms: float


class EvaluationResponse(BaseModel):
    """Multi-model evaluation response."""
    transaction_id: str
    results: List[EvaluationResult]
    agreement: bool
    score_difference: float
    feature_latency_ms: float
    total_latency_ms: float


# =============================================================================
# HEALTH CHECK MODELS
# =============================================================================

class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    service: str
    timestamp: str
    model_loaded: Optional[bool] = None
    feature_store_connected: Optional[bool] = None
    redis_connected: Optional[bool] = None
    details: Optional[dict] = None
