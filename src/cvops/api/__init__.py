"""
CVOps API Module
================

FastAPI endpoints for object detection inference.

Provides:
- POST /detect: Object detection on images
- GET /health: Health check

Usage:
    from src.cvops.api import app

    # Run with uvicorn
    uvicorn src.cvops.api.main:app --host 0.0.0.0 --port 8002
"""

from src.cvops.api.main import app

__all__ = ["app"]
