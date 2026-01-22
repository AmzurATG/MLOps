"""
Standardized API Error Handling
================================

Provides consistent error responses across all API endpoints.

Usage:
    from src.api.errors import (
        APIError,
        NotFoundError,
        ValidationError,
        ServiceUnavailableError,
        api_exception_handler,
    )

    # Raise custom errors
    raise NotFoundError("Customer not found", customer_id="cust_123")

    # Register handler with FastAPI
    app.add_exception_handler(APIError, api_exception_handler)
"""

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, Dict, Any
from datetime import datetime
import logging
import traceback

logger = logging.getLogger(__name__)


# =============================================================================
# ERROR RESPONSE MODELS
# =============================================================================

class ErrorResponse(BaseModel):
    """Standard error response model."""
    error: str
    error_code: str
    message: str
    details: Optional[Dict[str, Any]] = None
    timestamp: str
    request_id: Optional[str] = None


class ValidationErrorResponse(ErrorResponse):
    """Validation error with field-level details."""
    validation_errors: Optional[list] = None


# =============================================================================
# CUSTOM EXCEPTIONS
# =============================================================================

class APIError(Exception):
    """Base API error class."""

    def __init__(
        self,
        message: str,
        error_code: str = "INTERNAL_ERROR",
        status_code: int = 500,
        details: Optional[Dict[str, Any]] = None,
    ):
        self.message = message
        self.error_code = error_code
        self.status_code = status_code
        self.details = details or {}
        super().__init__(message)

    def to_response(self, request_id: Optional[str] = None) -> ErrorResponse:
        return ErrorResponse(
            error=self.__class__.__name__,
            error_code=self.error_code,
            message=self.message,
            details=self.details,
            timestamp=datetime.now().isoformat(),
            request_id=request_id,
        )


class NotFoundError(APIError):
    """Resource not found error."""

    def __init__(self, message: str = "Resource not found", **details):
        super().__init__(
            message=message,
            error_code="NOT_FOUND",
            status_code=404,
            details=details,
        )


class ValidationError(APIError):
    """Request validation error."""

    def __init__(self, message: str = "Validation failed", **details):
        super().__init__(
            message=message,
            error_code="VALIDATION_ERROR",
            status_code=400,
            details=details,
        )


class ServiceUnavailableError(APIError):
    """Service unavailable error."""

    def __init__(self, message: str = "Service unavailable", service: str = None, **details):
        if service:
            details["service"] = service
        super().__init__(
            message=message,
            error_code="SERVICE_UNAVAILABLE",
            status_code=503,
            details=details,
        )


class ModelNotFoundError(APIError):
    """Model not found in registry."""

    def __init__(self, message: str = "Model not found", model_name: str = None, stage: str = None, **details):
        if model_name:
            details["model_name"] = model_name
        if stage:
            details["stage"] = stage
        super().__init__(
            message=message,
            error_code="MODEL_NOT_FOUND",
            status_code=404,
            details=details,
        )


class FeatureStoreError(APIError):
    """Feature store access error."""

    def __init__(self, message: str = "Feature store error", **details):
        super().__init__(
            message=message,
            error_code="FEATURE_STORE_ERROR",
            status_code=503,
            details=details,
        )


class InferenceError(APIError):
    """Model inference error."""

    def __init__(self, message: str = "Inference failed", **details):
        super().__init__(
            message=message,
            error_code="INFERENCE_ERROR",
            status_code=500,
            details=details,
        )


class StreamingError(APIError):
    """Streaming features error."""

    def __init__(self, message: str = "Streaming error", **details):
        super().__init__(
            message=message,
            error_code="STREAMING_ERROR",
            status_code=503,
            details=details,
        )


# =============================================================================
# EXCEPTION HANDLERS
# =============================================================================

async def api_exception_handler(request: Request, exc: APIError) -> JSONResponse:
    """
    Handle APIError exceptions.

    Register with FastAPI:
        app.add_exception_handler(APIError, api_exception_handler)
    """
    request_id = getattr(request.state, "request_id", None)

    logger.warning(
        f"API Error: {exc.error_code} - {exc.message}",
        extra={
            "error_code": exc.error_code,
            "status_code": exc.status_code,
            "request_id": request_id,
            "details": exc.details,
        },
    )

    return JSONResponse(
        status_code=exc.status_code,
        content=exc.to_response(request_id).dict(),
    )


async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """
    Handle unexpected exceptions.

    Register with FastAPI:
        app.add_exception_handler(Exception, generic_exception_handler)
    """
    request_id = getattr(request.state, "request_id", None)

    logger.error(
        f"Unexpected error: {str(exc)}",
        extra={
            "request_id": request_id,
            "traceback": traceback.format_exc(),
        },
    )

    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            error="InternalServerError",
            error_code="INTERNAL_ERROR",
            message="An unexpected error occurred",
            details={"error": str(exc)} if logger.isEnabledFor(logging.DEBUG) else None,
            timestamp=datetime.now().isoformat(),
            request_id=request_id,
        ).dict(),
    )


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def handle_http_exception(exc: HTTPException) -> APIError:
    """Convert FastAPI HTTPException to APIError."""
    error_code_map = {
        400: "BAD_REQUEST",
        401: "UNAUTHORIZED",
        403: "FORBIDDEN",
        404: "NOT_FOUND",
        422: "VALIDATION_ERROR",
        500: "INTERNAL_ERROR",
        503: "SERVICE_UNAVAILABLE",
    }

    return APIError(
        message=str(exc.detail),
        error_code=error_code_map.get(exc.status_code, "UNKNOWN_ERROR"),
        status_code=exc.status_code,
    )


def wrap_exception(exc: Exception, message: str = None) -> APIError:
    """Wrap a generic exception as an APIError."""
    return APIError(
        message=message or str(exc),
        error_code="INTERNAL_ERROR",
        status_code=500,
        details={"original_error": type(exc).__name__},
    )
