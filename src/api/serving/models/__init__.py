"""
Model Loading and Selection
============================

Provides model loading from MLflow and selection logic for:
- Production/Staging models
- A/B testing variants
- Shadow mode comparisons

Usage:
    from src.api.serving.models import ModelLoader, ModelSelector

    loader = ModelLoader()
    model, info = loader.load(model_stage="Production")
"""

from src.api.serving.models.model_loader import ModelLoader, ModelInfo, LoadedModel
from src.api.serving.models.model_selector import ModelSelector, SelectionContext

__all__ = [
    "ModelLoader",
    "ModelInfo",
    "LoadedModel",
    "ModelSelector",
    "SelectionContext",
]
