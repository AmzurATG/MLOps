"""
Webhook Handlers
================

Domain-specific handlers for processing LakeFS webhook events.
"""

from src.webhooks.handlers.base import BaseWebhookHandler
from src.webhooks.handlers.mlops import MLOpsWebhookHandler
from src.webhooks.handlers.cvops import CVOpsWebhookHandler

__all__ = [
    "BaseWebhookHandler",
    "MLOpsWebhookHandler",
    "CVOpsWebhookHandler",
]


def get_handler(domain: str) -> BaseWebhookHandler:
    """
    Get the appropriate handler for a domain.

    Args:
        domain: Domain identifier (mlops, cvops, llmops)

    Returns:
        Handler instance for the domain
    """
    handlers = {
        "mlops": MLOpsWebhookHandler,
        "cvops": CVOpsWebhookHandler,
    }

    handler_class = handlers.get(domain, BaseWebhookHandler)
    return handler_class()
