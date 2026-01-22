"""
FastAPI Dependency Injection
============================

Provides dependency injection for API components following SOLID principles.

SOLID Principles:
- Dependency Inversion: Endpoints depend on abstractions (functions)
- Single Responsibility: Each dependency has one purpose
- Open/Closed: Easy to extend with new dependencies

Usage:
    from fastapi import Depends
    from src.api.dependencies import get_scoring_pipeline, get_feature_service

    @app.post("/predict")
    async def predict(
        pipeline: ScoringPipeline = Depends(get_scoring_pipeline),
    ):
        ...

Benefits over global singletons:
- Thread-safe initialization
- Easy to mock in tests
- Explicit dependencies
- Proper lifecycle management
"""

import logging
from typing import Optional, Generator
from functools import lru_cache

from src.api.settings import api_settings, APISettings

logger = logging.getLogger(__name__)


# =============================================================================
# Settings Dependency
# =============================================================================

def get_settings() -> APISettings:
    """
    Get API settings.

    Can be overridden in tests:
        app.dependency_overrides[get_settings] = lambda: MockSettings()
    """
    return api_settings


# =============================================================================
# Feature Service Dependency
# =============================================================================

@lru_cache(maxsize=1)
def _create_feature_service():
    """Create and cache FeatureService instance."""
    from src.api.serving.features import FeatureService, FeastConfig, StreamingRedisConfig

    settings = api_settings

    feast_config = FeastConfig(repo_path=settings.feast_repo_path)
    redis_config = StreamingRedisConfig(
        host=settings.redis_host,
        port=settings.redis_port,
        prefix=settings.redis_streaming_prefix,
        pool_max_connections=settings.redis_pool_max_connections,
        pool_timeout=settings.redis_pool_timeout,
    )

    service = FeatureService(
        feast_config=feast_config,
        redis_config=redis_config,
    )
    logger.info("[DI] FeatureService initialized")
    return service


def get_feature_service():
    """
    Dependency for FeatureService.

    Returns cached instance with thread-safe initialization.
    """
    return _create_feature_service()


# =============================================================================
# Model Loader Dependency
# =============================================================================

@lru_cache(maxsize=1)
def _create_model_loader():
    """Create and cache ModelLoader instance."""
    from src.api.serving.models.model_loader import ModelLoader, ModelLoaderConfig

    settings = api_settings

    config = ModelLoaderConfig(
        tracking_uri=settings.mlflow_tracking_uri,
        model_name=settings.model_name,
        default_stage=settings.default_model_stage,
    )

    loader = ModelLoader(config=config)
    logger.info("[DI] ModelLoader initialized")
    return loader


def get_model_loader():
    """
    Dependency for ModelLoader.

    Returns cached instance with thread-safe initialization.
    """
    return _create_model_loader()


# =============================================================================
# Scoring Pipeline Dependency
# =============================================================================

@lru_cache(maxsize=1)
def _create_scoring_pipeline():
    """Create and cache ScoringPipeline instance."""
    from src.api.serving import ScoringPipeline

    # Get dependencies
    feature_service = get_feature_service()
    model_loader = get_model_loader()

    pipeline = ScoringPipeline(
        feature_service=feature_service,
        model_loader=model_loader,
    )
    logger.info("[DI] ScoringPipeline initialized")
    return pipeline


def get_scoring_pipeline():
    """
    Dependency for ScoringPipeline.

    Returns cached instance with proper dependency chain.
    """
    return _create_scoring_pipeline()


# =============================================================================
# A/B Testing Dependency
# =============================================================================

@lru_cache(maxsize=1)
def _create_ab_router():
    """Create and cache ABTestingRouter instance."""
    try:
        from src.api.ab_testing import ABTestingRouter
        router = ABTestingRouter()
        logger.info("[DI] ABTestingRouter initialized")
        return router
    except ImportError as e:
        logger.warning(f"[DI] ABTestingRouter not available: {e}")
        return None


def get_ab_router():
    """
    Dependency for ABTestingRouter.

    Returns None if A/B testing is not available.
    """
    if not api_settings.enable_ab_testing:
        return None
    return _create_ab_router()


# =============================================================================
# Shadow Mode Dependency
# =============================================================================

@lru_cache(maxsize=1)
def _create_shadow_predictor():
    """Create and cache ShadowModePredictor instance."""
    try:
        from src.api.shadow_mode import ShadowModePredictor
        predictor = ShadowModePredictor()
        logger.info("[DI] ShadowModePredictor initialized")
        return predictor
    except ImportError as e:
        logger.warning(f"[DI] ShadowModePredictor not available: {e}")
        return None


def get_shadow_predictor():
    """
    Dependency for ShadowModePredictor.

    Returns None if shadow mode is not available.
    """
    if not api_settings.enable_shadow_mode:
        return None
    return _create_shadow_predictor()


# =============================================================================
# Explainability Dependency
# =============================================================================

_explainer_cache = {}


def get_explainer(model_version: str):
    """
    Get or create explainer for a specific model version.

    Args:
        model_version: Model version string for cache key

    Returns:
        FraudExplainer instance or None if not available
    """
    if not api_settings.enable_explainability:
        return None

    cache_key = f"explainer_{model_version}"
    if cache_key in _explainer_cache:
        return _explainer_cache[cache_key]

    try:
        from src.api.explainability import FraudExplainer, SHAP_AVAILABLE
        if not SHAP_AVAILABLE:
            return None

        from src.api.serving.models.model_loader import ModelLoader
        loader = get_model_loader()
        model = loader.load(model_stage="Production")

        explainer = FraudExplainer(model.model)
        _explainer_cache[cache_key] = explainer
        logger.info(f"[DI] FraudExplainer created for {model_version}")
        return explainer
    except Exception as e:
        logger.warning(f"[DI] FraudExplainer not available: {e}")
        return None


# =============================================================================
# Cleanup Utilities
# =============================================================================

def clear_dependency_cache():
    """
    Clear all cached dependencies.

    Useful for testing or when settings change.
    """
    _create_feature_service.cache_clear()
    _create_model_loader.cache_clear()
    _create_scoring_pipeline.cache_clear()
    _create_ab_router.cache_clear()
    _create_shadow_predictor.cache_clear()
    _explainer_cache.clear()
    logger.info("[DI] All dependency caches cleared")
