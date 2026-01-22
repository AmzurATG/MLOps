"""
Base Webhook Handler
====================

Abstract base class for domain-specific webhook handlers.
"""

import hmac
import hashlib
from abc import ABC
from typing import Dict, Any, Optional, Tuple

from src.webhooks.config import WebhookConfig, get_config
from src.webhooks.models import LakeFSEvent, WebhookResponse
from src.webhooks.storage import WebhookStorage


class BaseWebhookHandler(ABC):
    """
    Base class for webhook handlers.

    Subclasses implement domain-specific processing logic.
    """

    domain: str = "generic"

    def __init__(self, config: WebhookConfig = None):
        self.config = config or get_config()
        self.storage = WebhookStorage(self.config)

    def get_webhook_secret(self) -> str:
        """Get the webhook secret for this domain."""
        return ""

    def verify_signature(
        self, payload: bytes, signature: str
    ) -> bool:
        """
        Verify webhook signature.

        Returns True if verification passes or is disabled.
        """
        if not self.config.verify_signature:
            return True

        secret = self.get_webhook_secret()
        if not secret:
            return True

        if not signature:
            print(f"Warning: No signature provided for {self.domain} webhook")
            return False

        expected = hmac.new(
            secret.encode(),
            payload,
            hashlib.sha256
        ).hexdigest()

        return hmac.compare_digest(signature, expected)

    def should_process(self, event: LakeFSEvent) -> Tuple[bool, str]:
        """
        Check if this handler should process the event.

        Returns:
            (should_process, reason)
        """
        return True, "accepted"

    def extract_metadata(self, event: LakeFSEvent) -> Dict[str, Any]:
        """
        Extract domain-specific metadata from the event.

        Subclasses can override to add custom metadata.
        """
        return {}

    def post_process(
        self, event: LakeFSEvent, event_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Perform any post-processing after storing the event.

        Subclasses can override to trigger downstream actions.

        Returns:
            Optional result dict from post-processing
        """
        return None

    async def process(
        self, event: LakeFSEvent, raw_payload: bytes = None, signature: str = None
    ) -> WebhookResponse:
        """
        Main processing method for webhook events.

        Args:
            event: Parsed LakeFS event
            raw_payload: Raw request payload for signature verification
            signature: Request signature header

        Returns:
            WebhookResponse with processing result
        """
        # Verify signature if enabled
        if raw_payload and not self.verify_signature(raw_payload, signature):
            return WebhookResponse(
                status="rejected",
                error="Invalid signature",
            )

        # Check if we should process this event
        should_process, reason = self.should_process(event)
        if not should_process:
            return WebhookResponse(
                status="skipped",
                message=reason,
                domain=self.domain,
                repository=event.repository_id,
                branch=event.branch_id,
            )

        try:
            # Extract domain-specific metadata
            metadata = self.extract_metadata(event)

            # Store the event
            event_id = self.storage.store_event(
                event=event,
                domain=self.domain,
                metadata=metadata,
            )

            # Perform post-processing
            post_result = self.post_process(event, event_id)

            return WebhookResponse(
                status="accepted",
                event_id=event_id,
                message="Event queued for processing",
                domain=self.domain,
                repository=event.repository_id,
                branch=event.branch_id,
                sync_triggered=post_result is not None,
                sync_result=post_result,
            )

        except Exception as e:
            print(f"Error processing {self.domain} webhook: {e}")
            return WebhookResponse(
                status="error",
                error=str(e),
                domain=self.domain,
            )
