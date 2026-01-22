"""
Core API Infrastructure
=======================

Shared API utilities used across all domain APIs.

Provides:
- Error handling (errors.py)
- Shared models (models.py)
- Common dependencies (dependencies.py)

Usage:
    from src.core.api import APIError, NotFoundError
    from src.core.api import TransactionRequest, PredictionResponse
    from src.core.api.dependencies import get_settings
"""

from src.core.api.errors import (
    APIError,
    NotFoundError,
    ValidationError,
    ServiceUnavailableError,
    ModelNotFoundError,
    FeatureStoreError,
    InferenceError,
    StreamingError,
    ErrorResponse,
    api_exception_handler,
    generic_exception_handler,
)

from src.core.api.models import (
    TransactionRequest,
    PredictionResponse,
    BatchRequest,
    BatchResponse,
    EvaluationRequest,
    EvaluationResult,
    EvaluationResponse,
    HealthResponse,
)

__all__ = [
    # Errors
    "APIError",
    "NotFoundError",
    "ValidationError",
    "ServiceUnavailableError",
    "ModelNotFoundError",
    "FeatureStoreError",
    "InferenceError",
    "StreamingError",
    "ErrorResponse",
    "api_exception_handler",
    "generic_exception_handler",
    # Models
    "TransactionRequest",
    "PredictionResponse",
    "BatchRequest",
    "BatchResponse",
    "EvaluationRequest",
    "EvaluationResult",
    "EvaluationResponse",
    "HealthResponse",
]
