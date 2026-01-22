"""
Webhook Configuration
=====================

Centralized configuration for the unified webhook service.
"""

import os
from typing import Optional
from functools import lru_cache

try:
    from pydantic_settings import BaseSettings, SettingsConfigDict
except ImportError:
    from pydantic import BaseSettings
    SettingsConfigDict = None


class WebhookConfig(BaseSettings):
    """Webhook service configuration."""

    # Server
    host: str = "0.0.0.0"
    port: int = 5000

    # Trino (reads from env vars with defaults)
    trino_host: str = os.getenv("TRINO_HOST", "exp-trino")
    trino_port: int = int(os.getenv("TRINO_PORT", "8080"))
    trino_user: str = os.getenv("TRINO_USER", "trino")
    trino_catalog: str = os.getenv("TRINO_CATALOG", "iceberg_dev")

    # Schemas for event storage
    mlops_schema: str = "metadata"
    cvops_schema: str = "cv"
    events_table: str = "lakefs_webhook_events"

    # MLOps repository config
    mlops_repos: str = "warehouse,bronze"

    # CVOps repository config
    cvops_repo: str = "cv-data"
    cvops_branch: str = "dev-cvops"

    # Signature verification
    mlops_webhook_secret: str = ""
    cvops_webhook_secret: str = ""
    verify_signature: bool = False

    # Label Studio (for downstream sync)
    labelstudio_url: str = "http://exp-label-studio:8080"
    labelstudio_token: Optional[str] = None
    labelstudio_project_id: int = 1

    if SettingsConfigDict:
        model_config = SettingsConfigDict(
            env_file=".env",
            env_prefix="WEBHOOK_",
            extra="ignore",
        )
    else:
        class Config:
            env_file = ".env"
            env_prefix = "WEBHOOK_"
            extra = "ignore"

    @property
    def mlops_repos_list(self):
        """Get MLOps repos as a list."""
        return [r.strip() for r in self.mlops_repos.split(",")]

    def get_schema_for_domain(self, domain: str) -> str:
        """Get the schema for a given domain."""
        if domain == "cvops":
            return self.cvops_schema
        return self.mlops_schema

    def get_events_table(self, domain: str) -> str:
        """Get the fully qualified events table name for a domain."""
        schema = self.get_schema_for_domain(domain)
        return f"{self.trino_catalog}.{schema}.{self.events_table}"


@lru_cache()
def get_config() -> WebhookConfig:
    """Get cached webhook configuration."""
    return WebhookConfig()
