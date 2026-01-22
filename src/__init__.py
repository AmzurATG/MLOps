"""
Fraud Detection MLOps Platform - Source Package
================================================

This package contains all source code for the platform:
- api: REST API for model serving
- pipelines: Dagster pipeline definitions
- streaming: Kafka streaming components
- core: Shared utilities (monitoring, lineage, resources)
- mlops: MLOps domain (fraud detection)
- cvops: CVOps domain (computer vision)
- llmops: LLMOps domain (LLM operations)

Quick Imports:
    from src.api.serving import FeatureService, ModelLoader
    from src.core.monitoring import get_or_create_counter, track_execution
    from src.core.lineage import UnifiedLineageTracker
"""

# Lazy imports - only import when accessed to avoid triggering
# unnecessary dependencies during test collection
__all__ = ["api", "pipelines", "streaming", "core", "mlops", "cvops", "llmops"]


def __getattr__(name):
    """Lazy module loading to avoid import side effects."""
    if name == "api":
        from src import api
        return api
    elif name == "pipelines":
        from src import pipelines
        return pipelines
    elif name == "streaming":
        from src import streaming
        return streaming
    elif name == "core":
        from src import core
        return core
    elif name == "mlops":
        from src import mlops
        return mlops
    elif name == "cvops":
        from src import cvops
        return cvops
    elif name == "llmops":
        from src import llmops
        return llmops
    raise AttributeError(f"module 'src' has no attribute {name!r}")
