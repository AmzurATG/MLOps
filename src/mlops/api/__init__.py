"""
MLOps API Module
================

FastAPI endpoints for fraud detection inference.

Provides:
- POST /predict: Single prediction
- POST /batch: Batch predictions
- GET /health: Health check

Usage:
    from src.mlops.api import app

    # Run with uvicorn
    uvicorn src.mlops.api.main:app --host 0.0.0.0 --port 8001
"""

from src.mlops.api.main import app

__all__ = ["app"]
