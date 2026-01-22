"""
API Configuration Settings
==========================

Centralized, type-safe configuration for the Fraud Detection API.
Follows the same pattern as pipelines/settings.py for consistency.

SOLID Principles:
- Single Responsibility: Configuration only
- Dependency Inversion: Settings can be injected/overridden

Usage:
    from src.api.settings import api_settings

    threshold = api_settings.fraud_threshold
    model_name = api_settings.model_name
"""

import os
from typing import Optional, List
from functools import lru_cache

try:
    from pydantic_settings import BaseSettings, SettingsConfigDict
except ImportError:
    from pydantic import BaseSettings
    SettingsConfigDict = None


class APISettings(BaseSettings):
    """
    API-specific configuration.

    All settings can be overridden via environment variables.
    """

    # Model settings
    fraud_threshold: float = 0.5
    model_name: str = "fraud_detection_model"
    default_model_stage: str = "Production"

    # Feature settings
    feast_repo_path: str = "/app/feast_repo"
    include_streaming_by_default: bool = True

    # Redis settings (streaming features)
    redis_host: str = "exp-redis"
    redis_port: int = 6379
    redis_streaming_prefix: str = "feast:streaming:"
    redis_pool_max_connections: int = 20
    redis_pool_timeout: int = 5

    # MLflow settings
    mlflow_tracking_uri: str = "http://exp-mlflow:5000"

    # API settings
    api_host: str = "0.0.0.0"
    api_port: int = 8001
    max_batch_size: int = 1000
    max_eval_batch_size: int = 2000

    # Feature flags
    enable_ab_testing: bool = True
    enable_shadow_mode: bool = True
    enable_explainability: bool = True

    # Risk thresholds
    risk_high_threshold: float = 0.8
    risk_medium_threshold: float = 0.5

    if SettingsConfigDict:
        model_config = SettingsConfigDict(
            env_file=".env",
            env_prefix="API_",
            extra="ignore",
            protected_namespaces=('settings_',),
        )
    else:
        class Config:
            env_file = ".env"
            env_prefix = "API_"
            extra = "ignore"

    def get_risk_level(self, score: float) -> str:
        """Determine risk level from fraud score."""
        if score >= self.risk_high_threshold:
            return "HIGH"
        elif score >= self.risk_medium_threshold:
            return "MEDIUM"
        return "LOW"


@lru_cache(maxsize=1)
def get_api_settings() -> APISettings:
    """
    Get cached API settings instance.

    Returns:
        APISettings singleton
    """
    return APISettings()


# Convenience singleton
api_settings = get_api_settings()
