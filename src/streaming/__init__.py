"""
Streaming Module
================

This module provides imports for streaming components.

Usage:
    from src.streaming import FraudStreamingConsumer, Config
    from src.streaming import SchemaRegistryClient, SchemaInfo

Domain-specific access:
    from src.mlops.streaming import FraudStreamingConsumer
    from src.cvops.streaming import run_consumer

Note: Some imports require optional dependencies (redis, kafka-python).
If these are not installed, those classes will not be available.
"""

__all__ = []

# =============================================================================
# SCHEMA REGISTRY (shared utility)
# =============================================================================

try:
    from src.streaming.schema_registry_client import (
        SchemaRegistryClient,
        SchemaInfo,
        SchemaRegistryError,
        SchemaNotFoundError,
        SchemaIncompatibleError,
        load_avro_schema,
        register_fraud_transaction_schema,
    )
    __all__.extend([
        "SchemaRegistryClient",
        "SchemaInfo",
        "SchemaRegistryError",
        "SchemaNotFoundError",
        "SchemaIncompatibleError",
        "load_avro_schema",
        "register_fraud_transaction_schema",
    ])
except ImportError:
    pass

# =============================================================================
# MLOPS STREAMING (fraud detection)
# =============================================================================

try:
    from src.mlops.streaming.consumer import (
        FraudStreamingConsumer,
        Config,
        FieldDefinition,
        TYPED_FEATURE_MAPPING,
        STREAMING_SCHEMA_VERSION,
    )
    __all__.extend([
        "FraudStreamingConsumer",
        "Config",
        "FieldDefinition",
        "TYPED_FEATURE_MAPPING",
        "STREAMING_SCHEMA_VERSION",
    ])
except ImportError:
    pass

# =============================================================================
# CVOPS STREAMING (computer vision)
# =============================================================================

try:
    from src.cvops.streaming.ingest_consumer import (
        CVIngestConsumer,
        run_consumer as cv_run_consumer,
    )
    __all__.extend([
        "CVIngestConsumer",
        "cv_run_consumer",
    ])
except ImportError:
    pass

# =============================================================================
# EVALUATION CONSUMER
# =============================================================================

try:
    from src.streaming.evaluation_consumer import (
        EvaluationConsumer,
    )
    __all__.extend([
        "EvaluationConsumer",
    ])
except ImportError:
    pass
