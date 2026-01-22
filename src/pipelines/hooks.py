"""
Alerting Hooks for Pipeline Monitoring
Slack/webhook notifications on job success/failure.
"""
import os
import requests
from dagster import HookContext, success_hook, failure_hook


def _send_slack(webhook_url: str, message: dict):
    """Send message to Slack."""
    try:
        requests.post(webhook_url, json=message, timeout=10)
    except Exception as e:
        print(f"Slack notification failed: {e}")


@success_hook
def notify_on_success(context: HookContext):
    """Notify on job success."""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        return
    
    message = {
        "text": f"✅ *Job Succeeded*\n*Job:* `{context.job_name}`\n*Run:* `{context.run_id[:8]}...`"
    }
    _send_slack(webhook_url, message)


@failure_hook
def notify_on_failure(context: HookContext):
    """Notify on job failure."""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        return
    
    dagster_url = os.getenv("DAGSTER_UI_URL", "http://localhost:3000")
    message = {
        "text": (
            f"❌ *Job Failed*\n"
            f"*Job:* `{context.job_name}`\n"
            f"*Run:* `{context.run_id[:8]}...`\n"
            f"<{dagster_url}/runs/{context.run_id}|View Details>"
        )
    }
    _send_slack(webhook_url, message)


@failure_hook
def alert_critical_failure(context: HookContext):
    """High-priority alert for production failures."""
    # Check if critical job
    is_critical = "promote" in context.job_name.lower() or "production" in context.job_name.lower()
    if not is_critical:
        return
    
    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        return
    
    dagster_url = os.getenv("DAGSTER_UI_URL", "http://localhost:3000")
    message = {
        "text": (
            f"🚨 *CRITICAL JOB FAILED* 🚨\n<!channel>\n"
            f"*Job:* `{context.job_name}`\n"
            f"*Run:* `{context.run_id[:8]}...`\n"
            f"<{dagster_url}/runs/{context.run_id}|VIEW IMMEDIATELY>"
        )
    }
    _send_slack(webhook_url, message)


# Collect hooks
SUCCESS_HOOKS = [notify_on_success]
FAILURE_HOOKS = [notify_on_failure, alert_critical_failure]
