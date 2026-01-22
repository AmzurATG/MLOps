"""
API Routers
===========

Modular FastAPI routers for the Fraud Detection API.

Routers:
- prediction: /predict endpoints
- streaming: /streaming/* endpoints
- monitoring: /health, /metrics endpoints
- models: /models endpoints

Usage:
    from src.api.routers import prediction_router, streaming_router

    app.include_router(prediction_router)
    app.include_router(streaming_router)
"""

from src.api.routers.prediction import router as prediction_router
from src.api.routers.streaming import router as streaming_router
from src.api.routers.monitoring import router as monitoring_router

__all__ = [
    "prediction_router",
    "streaming_router",
    "monitoring_router",
]
