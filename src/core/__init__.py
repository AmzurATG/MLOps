"""
Core Infrastructure Module
==========================

Shared infrastructure components used across all *ops domains (MLOps, CVOps, LLMOps).

This module provides:
- Configuration management (config/)
- Resource connections (resources/)
- API utilities (api/)
- Monitoring helpers (monitoring/)
- Lineage tracking (lineage/)

Usage:
    from src.core.config import settings, InfraSettings
    from src.core.resources import TrinoResource, LakeFSResource
    from src.core.api import APIError, get_settings
"""

from src.core.config import settings, InfraSettings, MLOpsSettings, CVOpsSettings, LLMOpsSettings
from src.core.resources import (
    TrinoResource,
    LakeFSResource,
    MinIOResource,
    FeastResource,
    get_trino_client,
    get_lakefs_client,
)
from src.core.api import APIError, NotFoundError, ValidationError

__all__ = [
    # Config
    "settings",
    "InfraSettings",
    "MLOpsSettings",
    "CVOpsSettings",
    "LLMOpsSettings",
    # Resources
    "TrinoResource",
    "LakeFSResource",
    "MinIOResource",
    "FeastResource",
    "get_trino_client",
    "get_lakefs_client",
    # API
    "APIError",
    "NotFoundError",
    "ValidationError",
]
