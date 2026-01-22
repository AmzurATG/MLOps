"""
MLOps Webhook Handler
=====================

Handles LakeFS webhooks for fraud detection MLOps pipeline.
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

# Default webhook config content (re-uploaded if missing after revert)
DEFAULT_WEBHOOK_CONFIG = """name: dagster_integration
description: Notify Dagster on commits and reverts for data sync

on:
  post-revert:
    branches:
      - dev
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

name: track_commits
description: Track new commits in Dagster

on:
  post-commit:
    branches:
      - dev
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


class MLOpsWebhookHandler(BaseWebhookHandler):
    """
    Handler for MLOps (fraud detection) webhook events.

    Processes events from warehouse and bronze repositories.
    Triggers downstream sync on post-revert events.
    """

    domain = "mlops"

    def get_webhook_secret(self) -> str:
        """Get MLOps webhook secret."""
        return self.config.mlops_webhook_secret

    def should_process(self, event: LakeFSEvent) -> Tuple[bool, str]:
        """Check if this event is for an MLOps repository."""
        if event.repository_id not in self.config.mlops_repos_list:
            return False, f"Repository {event.repository_id} not tracked by MLOps"
        return True, "accepted"

    def extract_metadata(self, event: LakeFSEvent) -> Dict[str, Any]:
        """Extract MLOps-specific metadata."""
        metadata = {
            "pipeline": "fraud_detection",
        }

        # Count affected tables from diff
        if event.diff:
            table_paths = set()
            for diff_entry in event.diff:
                path = diff_entry.get("path", "")
                # Extract table name from path if it looks like a data path
                if "/" in path:
                    table_paths.add(path.split("/")[0])
            metadata["affected_tables"] = list(table_paths)
            metadata["table_count"] = len(table_paths)

        return metadata

    def post_process(
        self, event: LakeFSEvent, event_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Trigger downstream sync for post-revert events.

        When a revert happens on warehouse or bronze, we need to sync:
        - Gold table
        - Training data
        - Label Studio tasks

        Also validates webhook config exists after revert (re-uploads if missing).
        """
        if event.event_type != "post-revert":
            return None

        if event.repository_id not in ["warehouse", "bronze"]:
            return None

        from datetime import datetime

        print("")
        print("╔" + "═" * 60 + "╗")
        print("║" + " 🔄 LAKEFS REVERT DETECTED".center(60) + "║")
        print("╠" + "═" * 60 + "╣")
        print(f"║  Repository:  {event.repository_id:<44}  ║")
        print(f"║  Branch:      {event.branch_id:<44}  ║")
        print(f"║  Commit:      {(event.commit_id or 'N/A')[:40]:<44}  ║")
        print(f"║  Time:        {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<44}  ║")
        print("╚" + "═" * 60 + "╝")
        print("")

        # CRITICAL: Check if webhook config still exists after revert
        # If config was reverted away, re-upload it to maintain webhook functionality
        print("┌─── Step 1: Validate Webhook Config ───")
        config_result = self._ensure_webhook_config_exists(event)
        if config_result.get("config_restored"):
            print("│ ⚠️  Webhook config was missing after revert - restored automatically")
        elif config_result.get("config_status") == "exists":
            print("│ ✓ Webhook config exists")
        else:
            print(f"│ ⚠️  Config status: {config_result.get('config_status', 'unknown')}")

        # Step 2: Mark reverted commit in lakefs_commits table
        print("")
        print("┌─── Step 2: Mark Reverted Commit ───")
        reverted_commit = self._extract_reverted_commit(event)
        if reverted_commit:
            mark_result = self._mark_commit_as_reverted(reverted_commit, event)
            if mark_result.get("success"):
                print(f"│ ✓ Marked commit {reverted_commit[:12]}... as reverted")
            else:
                print(f"│ ⚠️  Could not mark commit: {mark_result.get('error', 'unknown')}")
        else:
            print("│ ⚠️  Could not extract reverted commit ID from event")

        print("")
        print("┌─── Step 3: Trigger Downstream Sync ───")
        sync_result = self._trigger_downstream_sync()

        # Combine results
        if sync_result:
            sync_result["config_validation"] = config_result
            sync_result["reverted_commit"] = reverted_commit

        return sync_result

    def _extract_reverted_commit(self, event: LakeFSEvent) -> Optional[str]:
        """Extract the reverted commit ID from event message."""
        import re
        if event.commit_message:
            # Look for "Revert <64-char-hex>" pattern
            match = re.search(r"Revert\s+([a-f0-9]{64})", event.commit_message)
            if match:
                return match.group(1)
        # Fallback: use source_ref if different from commit_id
        if event.source_ref and event.source_ref != event.commit_id:
            return event.source_ref
        return None

    def _mark_commit_as_reverted(
        self, reverted_commit: str, event: LakeFSEvent
    ) -> Dict[str, Any]:
        """Mark the reverted commit in lakefs_commits table."""
        try:
            from trino.dbapi import connect
            from src.core.config import TRINO_CATALOG, escape_sql_string

            conn = connect(
                host=os.getenv("TRINO_HOST", "exp-trino"),
                port=int(os.getenv("TRINO_PORT", "8080")),
                user=os.getenv("TRINO_USER", "trino"),
                catalog=TRINO_CATALOG,
            )
            cursor = conn.cursor()

            commit_safe = escape_sql_string(reverted_commit)
            message_safe = escape_sql_string((event.commit_message or "")[:200])

            cursor.execute(f"""
                UPDATE {TRINO_CATALOG}.metadata.lakefs_commits
                SET status = 'reverted',
                    reverted_at = CURRENT_TIMESTAMP,
                    revert_reason = '{message_safe}',
                    last_updated = CURRENT_TIMESTAMP
                WHERE commit_id = '{commit_safe}'
            """)

            cursor.close()
            conn.close()
            return {"success": True}

        except Exception as e:
            return {"success": False, "error": str(e)}

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
            print("[MLOpsHandler] LakeFS credentials not configured, skipping config check")
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
                    print(f"[MLOpsHandler] Webhook config exists at {WEBHOOK_CONFIG_PATH}")
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
                    print(f"[MLOpsHandler] Unexpected response checking config: {stat_resp.status_code}")
                    result["config_status"] = "error"
                    result["error"] = f"Unexpected status: {stat_resp.status_code}"

        except Exception as e:
            print(f"[MLOpsHandler] Error checking webhook config: {e}")
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
                content=DEFAULT_WEBHOOK_CONFIG.encode('utf-8'),
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
                    "message": "Auto-restore webhook config after revert",
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

    def _trigger_downstream_sync(self) -> Dict[str, Any]:
        """
        Trigger Dagster data_sync_job via GraphQL API.

        This ensures Label Studio, Gold, and Training Data stay in sync
        with Silver after records are reverted in LakeFS.

        Uses data_sync_job which compares actual data between tables
        and doesn't require any run config parameters.
        """
        try:
            mutation = """
            mutation LaunchRun($jobName: String!) {
                launchRun(executionParams: {
                    selector: {
                        jobName: $jobName,
                        repositoryName: "__repository__",
                        repositoryLocationName: "unified_mlops_platform"
                    }
                }) {
                    __typename
                    ... on LaunchRunSuccess {
                        run { runId }
                    }
                    ... on PythonError {
                        message
                        stack
                    }
                    ... on RunConfigValidationInvalid {
                        errors { message reason }
                    }
                    ... on InvalidSubsetError {
                        message
                    }
                }
            }
            """

            with httpx.Client(timeout=30.0) as client:
                response = client.post(
                    DAGSTER_GRAPHQL_URL,
                    json={
                        "query": mutation,
                        "variables": {"jobName": "data_sync_job"}
                    }
                )

                if response.status_code != 200:
                    print(f"│ ❌ Dagster API error: {response.status_code}")
                    return {"error": f"Dagster API returned {response.status_code}"}

                result = response.json()

                # Check for GraphQL errors
                if "errors" in result:
                    error_msg = result["errors"][0].get("message", "Unknown error")
                    print(f"│ ❌ Dagster GraphQL error: {error_msg}")
                    return {"error": error_msg}

                # Check launch result
                launch_result = result.get("data", {}).get("launchRun", {})
                typename = launch_result.get("__typename", "")

                if typename == "LaunchRunSuccess":
                    run_id = launch_result["run"]["runId"]
                    print(f"│ ✓ Triggered data_sync_job successfully")
                    print(f"│   Run ID: {run_id}")
                    print(f"│   Dagster UI: http://localhost:13000/runs/{run_id}")
                    print("└" + "─" * 50)
                    print("")
                    print("╔" + "═" * 60 + "╗")
                    print("║" + " ✅ REVERT HANDLING COMPLETE".center(60) + "║")
                    print("║" + f"   Dagster job triggered: data_sync_job".center(60) + "║")
                    print("║" + f"   Run ID: {run_id[:20]}...".center(60) + "║")
                    print("╚" + "═" * 60 + "╝")
                    return {"run_id": run_id, "status": "triggered"}
                elif typename == "RunConfigValidationInvalid":
                    errors = launch_result.get("errors", [])
                    error_msgs = [e.get("message", "Unknown error") for e in errors]
                    print(f"│ ❌ Dagster config error: {error_msgs}")
                    return {"error": f"Config validation failed: {error_msgs}"}
                elif "message" in launch_result:
                    print(f"│ ❌ Dagster launch error: {launch_result['message']}")
                    return {"error": launch_result["message"]}
                else:
                    print(f"│ ⚠️  Unexpected launch result: {launch_result}")
                    return {"error": f"Unexpected response type: {typename}"}

        except httpx.ConnectError as e:
            print(f"Cannot connect to Dagster: {e}")
            return {"error": f"Cannot connect to Dagster at {DAGSTER_GRAPHQL_URL}"}

        except Exception as e:
            print(f"Downstream sync failed: {e}")
            return {"error": str(e)}
