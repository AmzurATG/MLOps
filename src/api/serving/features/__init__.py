"""
Feature Retrieval Layer
=======================

Provides unified access to features from multiple sources:
- Feast (batch features from online store)
- Redis (real-time streaming features)

SOLID Principles:
- FeatureSource protocol enables adding new sources without modifying FeatureService
- Dependency injection via constructor allows testing with mocks

Usage:
    from src.api.serving.features import FeatureService

    service = FeatureService()
    features = service.get_features("cust_123", "tx_456")

    # With custom sources (Open/Closed Principle):
    from src.api.serving.features import FeatureSource
    custom_source = MyCustomSource()
    service = FeatureService(additional_sources=[custom_source])
"""

from src.api.serving.features.protocols import (
    FeatureSource,
    FeatureResult,
    BaseFeatureSource,
)
from src.api.serving.features.feast_client import FeastClient, FeastConfig
from src.api.serving.features.redis_client import StreamingRedisClient, StreamingRedisConfig
from src.api.serving.features.feature_service import FeatureService, FeatureVector

__all__ = [
    # Protocols (for extension)
    "FeatureSource",
    "FeatureResult",
    "BaseFeatureSource",
    # Concrete implementations
    "FeastClient",
    "FeastConfig",
    "StreamingRedisClient",
    "StreamingRedisConfig",
    # Service layer
    "FeatureService",
    "FeatureVector",
]
