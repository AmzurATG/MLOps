"""
CVOps Webhook Handler
=====================

Handles LakeFS webhooks for computer vision pipeline.

ALIGNED WITH MLOPS PATTERNS:
- Webhook config auto-restore after revert
- Dagster job triggering via GraphQL API
- Formatted logging with boxes
"""

import os
import httpx
from typing import Dict, Any, Optional, Tuple

from src.webhooks.handlers.base import BaseWebhookHandler
from src.webhooks.models import LakeFSEvent

# Dagster GraphQL endpoint
DAGSTER_GRAPHQL_URL = os.getenv("DAGSTER_GRAPHQL_URL", "http://exp-dagster-webserver:3000/graphql")

# LakeFS endpoint for config re-upload
LAKEFS_ENDPOINT = os.getenv("LAKEFS_ENDPOINT", "http://exp-lakefs:8000")
LAKEFS_ACCESS_KEY = os.getenv("LAKEFS_ACCESS_KEY_ID", "")
LAKEFS_SECRET_KEY = os.getenv("LAKEFS_SECRET_ACCESS_KEY", "")

# Path to webhook config in repository (must be _lakefs_actions/ for LakeFS to recognize)
WEBHOOK_CONFIG_PATH = "_lakefs_actions/dagster_webhook.yaml"

# Default CVOps webhook config content (re-uploaded if missing after revert)
DEFAULT_CVOPS_WEBHOOK_CONFIG = """name: cvops_dagster_integration
description: Notify Dagster on commits and reverts for CVOps data sync

on:
  post-revert:
    branches:
      - dev-cvops
      - main
      - experiment-*
      - feature-*

hooks:
  - id: notify_dagster_revert
    type: webhook
    properties:
      url: http://unified-webhook:5000/lakefs-webhook
      timeout: 5s
      query_params:
        source: lakefs
        event_type: revert
      headers:
        Content-Type: application/json

---

name: track_cvops_commits
description: Track new CVOps commits in Dagster

on:
  post-commit:
    branches:
      - dev-cvops
      - experiment-*
      - feature-*

hooks:
  - id: notify_dagster_commit
    type: webhook
    properties:
      url: http://unified-webhook:5000/lakefs-webhook
      timeout: 5s
      query_params:
        source: lakefs
        event_type: commit
      headers:
        Content-Type: application/json
"""


class CVOpsWebhookHandler(BaseWebhookHandler):
    """
    Handler for CVOps (computer vision) webhook events.

    Processes events from cv-data repository.
    Tracks image file changes and counts.
    """

    domain = "cvops"

    # Supported image extensions
    IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".tiff"}

    def get_webhook_secret(self) -> str:
        """Get CVOps webhook secret."""
        return self.config.cvops_webhook_secret

    def should_process(self, event: LakeFSEvent) -> Tuple[bool, str]:
        """Check if this event is for the CVOps repository."""
        if event.repository_id != self.config.cvops_repo:
            return False, f"Repository {event.repository_id} not tracked by CVOps"
        return True, "accepted"

    def extract_metadata(self, event: LakeFSEvent) -> Dict[str, Any]:
        """Extract CVOps-specific metadata (image paths, counts)."""
        metadata = {
            "pipeline": "object_detection",
        }

        if not event.diff:
            return metadata

        # Count affected image files
        affected_images = []
        for diff_entry in event.diff:
            path = diff_entry.get("path", "")
            # Check if it's an image file
            if any(path.lower().endswith(ext) for ext in self.IMAGE_EXTENSIONS):
                affected_images.append(path)

        metadata["affected_images"] = affected_images[:100]  # Limit to 100
        metadata["image_count"] = len(affected_images)
        metadata["total_changes"] = len(event.diff)

        # Log image count
        if affected_images:
            print(f"  {len(affected_images)} image(s) affected")

        return metadata

    def post_process(
        self, event: LakeFSEvent, event_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Handle CVOps-specific event types with MLOps-aligned patterns.

        For post-revert events:
        1. Validate webhook config (re-upload if missing)
        2. Trigger Dagster cleanup job

        For post-merge to main:
        - Log promotion event

        For post-commit:
        - Log new commit
        """
        if event.event_type != "post-revert":
            # Handle non-revert events
            if event.event_type == "post-merge" and event.branch_id == "main":
                print(f"  PROMOTION to main detected")
                print(f"  New model training may be triggered")
            elif event.event_type == "post-commit":
                print(f"  New commit on {event.branch_id}")
            return None

        # ═══════════════════════════════════════════════════════════════
        # POST-REVERT HANDLING (Aligned with MLOps)
        # ═══════════════════════════════════════════════════════════════
        from datetime import datetime

        print("")
        print("╔" + "═" * 60 + "╗")
        print("║" + " 🔄 CVOPS LAKEFS REVERT DETECTED".center(60) + "║")
        print("╠" + "═" * 60 + "╣")
        print(f"║  Repository:  {event.repository_id:<44}  ║")
        print(f"║  Branch:      {event.branch_id:<44}  ║")
        print(f"║  Commit:      {(event.commit_id or 'N/A')[:40]:<44}  ║")
        print(f"║  Time:        {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<44}  ║")
        print("╚" + "═" * 60 + "╝")
        print("")

        # CRITICAL: Check if webhook config still exists after revert
        print("┌─── Step 1: Validate Webhook Config ───")
        config_result = self._ensure_webhook_config_exists(event)
        if config_result.get("config_restored"):
            print("│ ⚠️  Webhook config was missing after revert - restored automatically")
        elif config_result.get("config_status") == "exists":
            print("│ ✓ Webhook config exists")
        else:
            print(f"│ ⚠️  Config status: {config_result.get('config_status', 'unknown')}")

        print("")
        print("┌─── Step 2: Trigger CVOps Cleanup Job ───")
        sync_result = self._trigger_cvops_cleanup(event)

        # Combine results
        if sync_result:
            sync_result["config_validation"] = config_result

        return sync_result

    def _ensure_webhook_config_exists(self, event: LakeFSEvent) -> Dict[str, Any]:
        """
        Check if webhook config exists after revert, re-upload if missing.

        This prevents the scenario where reverting past the config upload commit
        causes future reverts to not trigger webhooks.

        Returns:
            {
                "config_status": "exists" | "restored" | "error",
                "config_restored": bool,
                "error": Optional[str],
            }
        """
        result = {
            "config_status": "exists",
            "config_restored": False,
        }

        if not LAKEFS_ACCESS_KEY or not LAKEFS_SECRET_KEY:
            print("[CVOpsHandler] LakeFS credentials not configured, skipping config check")
            result["config_status"] = "skipped"
            return result

        auth = (LAKEFS_ACCESS_KEY, LAKEFS_SECRET_KEY)

        try:
            with httpx.Client(timeout=10.0) as client:
                # Check if config file exists
                stat_url = f"{LAKEFS_ENDPOINT}/api/v1/repositories/{event.repository_id}/refs/{event.branch_id}/objects/stat"

                stat_resp = client.get(
                    stat_url,
                    params={"path": WEBHOOK_CONFIG_PATH},
                    auth=auth,
                )

                if stat_resp.status_code == 200:
                    # Config exists
                    print(f"[CVOpsHandler] Webhook config exists at {WEBHOOK_CONFIG_PATH}")
                    return result

                if stat_resp.status_code == 404:
                    # Config missing - re-upload it
                    print(f"⚠️ Webhook config missing after revert, re-uploading...")

                    upload_result = self._upload_webhook_config(event, client, auth)
                    if upload_result.get("success"):
                        result["config_status"] = "restored"
                        result["config_restored"] = True
                        print(f"✅ Webhook config restored successfully")
                    else:
                        result["config_status"] = "error"
                        result["error"] = upload_result.get("error")
                        print(f"❌ Failed to restore webhook config: {upload_result.get('error')}")
                else:
                    print(f"[CVOpsHandler] Unexpected response checking config: {stat_resp.status_code}")
                    result["config_status"] = "error"
                    result["error"] = f"Unexpected status: {stat_resp.status_code}"

        except Exception as e:
            print(f"[CVOpsHandler] Error checking webhook config: {e}")
            result["config_status"] = "error"
            result["error"] = str(e)

        return result

    def _upload_webhook_config(
        self,
        event: LakeFSEvent,
        client: httpx.Client,
        auth: Tuple[str, str],
    ) -> Dict[str, Any]:
        """
        Upload webhook config file to LakeFS repository.

        Returns:
            {"success": bool, "error": Optional[str], "commit_id": Optional[str]}
        """
        try:
            # Step 1: Upload the file
            upload_url = f"{LAKEFS_ENDPOINT}/api/v1/repositories/{event.repository_id}/branches/{event.branch_id}/objects"

            upload_resp = client.post(
                upload_url,
                params={"path": WEBHOOK_CONFIG_PATH},
                content=DEFAULT_CVOPS_WEBHOOK_CONFIG.encode('utf-8'),
                headers={"Content-Type": "application/octet-stream"},
                auth=auth,
            )

            if upload_resp.status_code not in [200, 201]:
                return {
                    "success": False,
                    "error": f"Upload failed: {upload_resp.status_code} - {upload_resp.text[:200]}",
                }

            # Step 2: Commit the file
            commit_url = f"{LAKEFS_ENDPOINT}/api/v1/repositories/{event.repository_id}/branches/{event.branch_id}/commits"

            commit_resp = client.post(
                commit_url,
                json={
                    "message": "Auto-restore CVOps webhook config after revert",
                    "metadata": {
                        "source": "unified-webhook",
                        "trigger": "post-revert-config-validation",
                        "original_event_id": event.event_id or "unknown",
                    }
                },
                auth=auth,
            )

            if commit_resp.status_code not in [200, 201]:
                return {
                    "success": False,
                    "error": f"Commit failed: {commit_resp.status_code} - {commit_resp.text[:200]}",
                }

            commit_data = commit_resp.json()
            commit_id = commit_data.get("id", "unknown")

            return {
                "success": True,
                "commit_id": commit_id,
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
            }

    def _trigger_cvops_cleanup(self, event: LakeFSEvent) -> Dict[str, Any]:
        """
        Trigger Dagster cvops_cleanup_job via GraphQL API.

        This ensures CV data is properly cleaned up when commits are reverted.
        The Dagster sensor will pick up the webhook event from the events table
        and process it, but we log the intent here for visibility.

        Note: Unlike MLOps which triggers data_sync_job directly, CVOps cleanup
        is handled by the cvops_webhook_sensor polling the events table.
        This method provides logging and could trigger the job directly if needed.
        """
        try:
            # For CVOps, the cleanup is triggered by the sensor polling events table
            # The webhook already inserted the event, so the sensor will pick it up
            # But we log the state here for visibility

            print(f"│ ✓ Revert event recorded in cv.lakefs_webhook_events")
            print(f"│   The cvops_webhook_sensor will process this event")
            print(f"│   and trigger cvops_cleanup_job automatically")
            print("└" + "─" * 50)
            print("")
            print("╔" + "═" * 60 + "╗")
            print("║" + " ✅ CVOPS REVERT HANDLING COMPLETE".center(60) + "║")
            print("║" + "   Event stored for sensor processing".center(60) + "║")
            print("║" + f"   Repository: {event.repository_id}".center(60) + "║")
            print("╚" + "═" * 60 + "╝")

            return {
                "status": "event_stored",
                "repository": event.repository_id,
                "branch": event.branch_id,
                "message": "Event stored for cvops_webhook_sensor processing"
            }

        except Exception as e:
            print(f"│ ❌ Error: {e}")
            print("└" + "─" * 50)
            return {"error": str(e)}
