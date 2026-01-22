"""
Core Configuration Module
=========================

Centralized, type-safe configuration using Pydantic Settings.
All environment variables are loaded once at startup and validated.

Usage:
    from src.core.config import settings

    # Access infrastructure settings
    catalog = settings.infra.trino_catalog

    # Access domain-specific settings
    experiment = settings.mlops.mlflow_experiment
"""

from src.core.config.settings import (
    Settings,
    settings,
    InfraSettings,
    MLOpsSettings,
    CVOpsSettings,
    LLMOpsSettings,
    get_catalog,
    table_name,
    TRINO_CATALOG,
    escape_sql_string,
    validate_identifier,
    escape_sql_list,
)
from src.core.config.domain_configs import (
    LLMOPS_CONFIG,
    FRAUD_FEATURES,
    RETRAIN_CONFIG,
    SHADOW_MODE_CONFIG,
    PERFORMANCE_CONFIG,
    ICEBERG_SNAPSHOT_CONFIG,
    KSQLDB_CONFIG,
)

__all__ = [
    # Pydantic settings
    "Settings",
    "settings",
    "InfraSettings",
    "MLOpsSettings",
    "CVOpsSettings",
    "LLMOpsSettings",
    "get_catalog",
    "table_name",
    "TRINO_CATALOG",
    # SQL utilities
    "escape_sql_string",
    "validate_identifier",
    "escape_sql_list",
    # Domain config dicts (prefer settings.mlops, settings.cvops for new code)
    "LLMOPS_CONFIG",
    "FRAUD_FEATURES",
    "RETRAIN_CONFIG",
    "SHADOW_MODE_CONFIG",
    "PERFORMANCE_CONFIG",
    "ICEBERG_SNAPSHOT_CONFIG",
    "KSQLDB_CONFIG",
]
