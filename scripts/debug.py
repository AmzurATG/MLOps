"""
Debug Helper Module
===================

Quick access to all common classes for interactive debugging and development.

Usage:
    # In Python shell or notebook:
    from debug import *

    # Now you have access to:
    # - FeatureService, ModelLoader, ScoringPipeline (API)
    # - FeatureContract, FeatureTransformer, SchemaValidator (Pipelines)
    # - FraudStreamingConsumer, SchemaRegistryClient (Streaming)

Example:
    >>> from debug import *
    >>> client = SchemaRegistryClient("http://localhost:8081")
    >>> client.is_available()
    True
    >>> contract = FeatureContract.from_json("contracts/fraud_v1.json")
    >>> print(contract.version)
    "1.0.0"

Note: Some classes require optional dependencies.
Check __all__ to see what's available in your environment.
"""

import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)

__all__ = []

# =============================================================================
# API MODULE EXPORTS
# =============================================================================

try:
    # Features
    from src.api.serving.features.feature_service import FeatureService, FeatureVector
    from src.api.serving.features.feast_client import FeastClient, FeastConfig
    from src.api.serving.features.redis_client import StreamingRedisClient, StreamingRedisConfig
    # Models
    from src.api.serving.models.model_loader import (
        ModelLoader, LoadedModel, SchemaCompatibilityError,
        MissingFeaturesError, IncompatibleContractError,
    )
    from src.api.serving.models.model_selector import (
        ModelSelector, SelectionContext, SelectionResult, SelectionStrategy,
    )
    # Scoring
    from src.api.serving.scoring.score_pipeline import ScoringPipeline, ScoringResult, RequestContext
    from src.api.serving.scoring.adjustments import ScoreAdjuster, AdjustmentResult
    # Settings
    from src.api.settings import api_settings, APISettings
    # Telemetry
    from src.api.serving.telemetry.tracker import PerformanceTracker
    from src.api.serving.telemetry.observer import TelemetryObserver
    __all__.extend([
        # API - Features
        "FeatureService", "FeatureVector", "FeastClient", "FeastConfig",
        "StreamingRedisClient", "StreamingRedisConfig",
        # API - Models
        "ModelLoader", "LoadedModel", "ModelSelector",
        "SelectionContext", "SelectionResult", "SelectionStrategy",
        "SchemaCompatibilityError", "MissingFeaturesError", "IncompatibleContractError",
        # API - Scoring
        "ScoringPipeline", "ScoringResult", "RequestContext",
        "ScoreAdjuster", "AdjustmentResult",
        # API - Settings
        "api_settings", "APISettings",
        # API - Telemetry
        "PerformanceTracker", "TelemetryObserver",
    ])
    # Aliases
    Scorer = ScoringPipeline
    Loader = ModelLoader
    Selector = ModelSelector
    __all__.extend(["Scorer", "Loader", "Selector"])
except ImportError as e:
    print(f"Warning: API module not fully available: {e}")

# =============================================================================
# PIPELINES MODULE EXPORTS
# =============================================================================

try:
    # Feature Transformer
    from src.mlops.pipelines.feature_transformer import (
        FeatureContract, FeatureTransformer, SchemaValidator,
        SchemaValidationResult, DeprecationInfo,
    )
    # Settings
    from src.core.config import settings as pipeline_settings, InfraSettings, MLOpsSettings
    # Resources
    from src.core.resources import LakeFSResource, TrinoResource, MinIOResource, FeastResource
    __all__.extend([
        # Pipelines
        "FeatureContract", "FeatureTransformer", "SchemaValidator",
        "SchemaValidationResult", "DeprecationInfo",
        "pipeline_settings", "InfraSettings", "MLOpsSettings",
        "LakeFSResource", "TrinoResource", "MinIOResource", "FeastResource",
    ])
    # Aliases
    Contract = FeatureContract
    Transformer = FeatureTransformer
    __all__.extend(["Contract", "Transformer"])
except ImportError as e:
    print(f"Warning: Pipelines module not fully available: {e}")

# =============================================================================
# STREAMING MODULE EXPORTS
# =============================================================================

try:
    from src.streaming import SchemaRegistryClient, SchemaInfo
    __all__.extend(["SchemaRegistryClient", "SchemaInfo"])
    Registry = SchemaRegistryClient
    __all__.append("Registry")
except ImportError:
    pass

try:
    from src.streaming import (
        FraudStreamingConsumer,
        Config as StreamingConfig,
        FieldDefinition,
        TYPED_FEATURE_MAPPING,
        STREAMING_SCHEMA_VERSION,
    )
    __all__.extend([
        "FraudStreamingConsumer", "StreamingConfig", "FieldDefinition",
        "TYPED_FEATURE_MAPPING", "STREAMING_SCHEMA_VERSION",
    ])
    Consumer = FraudStreamingConsumer
    __all__.append("Consumer")
except ImportError:
    pass

try:
    from src.streaming import (
        SchemaRegistryError,
        SchemaNotFoundError,
        SchemaIncompatibleError,
        load_avro_schema,
        register_fraud_transaction_schema,
    )
    __all__.extend([
        "SchemaRegistryError", "SchemaNotFoundError", "SchemaIncompatibleError",
        "load_avro_schema", "register_fraud_transaction_schema",
    ])
except ImportError:
    pass
