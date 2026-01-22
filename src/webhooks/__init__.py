"""
Shared Webhook Service
======================

Consolidated webhook handling for LakeFS events across all domains
(MLOps, CVOps, LLMOps).

Usage:
    # Run with uvicorn
    uvicorn webhooks.main:app --host 0.0.0.0 --port 5000

    # Or via Python
    python -m webhooks.main
"""

from src.webhooks.config import WebhookConfig, get_config
from src.webhooks.models import LakeFSEvent, WebhookEventRecord
from src.webhooks.storage import WebhookStorage

__all__ = [
    "WebhookConfig",
    "get_config",
    "LakeFSEvent",
    "WebhookEventRecord",
    "WebhookStorage",
]
