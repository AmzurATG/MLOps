"""
Unified Webhook Server
======================

Single FastAPI application for handling LakeFS webhooks across all domains.

Replaces:
- pipelines/webhook_server.py (MLOps, Flask, port 5000)
- api/cvops_webhook_server.py (CVOps, Flask, port 5001)

Usage:
    uvicorn api.webhooks.main:app --host 0.0.0.0 --port 5000
"""

from datetime import datetime
from typing import Optional

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

from src.webhooks.config import get_config
from src.webhooks.models import LakeFSEvent, WebhookResponse, HealthResponse
from src.webhooks.storage import WebhookStorage
from src.webhooks.handlers import get_handler, MLOpsWebhookHandler, CVOpsWebhookHandler


# Create FastAPI app
app = FastAPI(
    title="Unified LakeFS Webhook Service",
    description="Consolidated webhook handling for MLOps, CVOps, and LLMOps",
    version="2.0.0",
)


# =============================================================================
# HEALTH CHECK
# =============================================================================

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    config = get_config()
    return HealthResponse(
        status="healthy",
        service="unified-lakefs-webhook",
        timestamp=datetime.now().isoformat(),
        domains=["mlops", "cvops"],
        signature_verification="enabled" if config.verify_signature else "disabled",
    )


# =============================================================================
# DOMAIN-ROUTED WEBHOOK ENDPOINTS
# =============================================================================

@app.post("/webhook/{domain}", response_model=WebhookResponse)
async def handle_webhook(domain: str, request: Request):
    """
    Unified webhook endpoint with domain routing.

    Args:
        domain: Domain identifier (mlops, cvops, llmops)
    """
    # Validate domain
    valid_domains = {"mlops", "cvops", "llmops"}
    if domain not in valid_domains:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid domain: {domain}. Valid domains: {valid_domains}"
        )

    # Get raw payload for signature verification
    raw_payload = await request.body()
    signature = request.headers.get("X-LakeFS-Signature", "")

    # Parse event
    try:
        event_data = await request.json()
        event = LakeFSEvent(**event_data)
    except Exception as e:
        return WebhookResponse(
            status="rejected",
            error=f"Invalid JSON payload: {e}",
        )

    # Get handler and process
    handler = get_handler(domain)
    return await handler.process(event, raw_payload, signature)


# =============================================================================
# EVENT MANAGEMENT ENDPOINTS
# =============================================================================

@app.get("/pending-events/{domain}")
async def get_pending_events(domain: str, limit: int = 100):
    """Get pending webhook events for a domain."""
    config = get_config()
    storage = WebhookStorage(config)

    try:
        events = storage.get_pending_events(domain, limit)
        return {
            "status": "ok",
            "domain": domain,
            "count": len(events),
            "events": events,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/mark-processed/{domain}/{event_id}")
async def mark_event_processed(
    domain: str,
    event_id: int,
    status: str = "processed",
    error_message: Optional[str] = None,
):
    """Mark a webhook event as processed."""
    config = get_config()
    storage = WebhookStorage(config)

    try:
        storage.mark_processed(domain, event_id, status, error_message)
        return {
            "status": "ok",
            "domain": domain,
            "event_id": event_id,
            "new_status": status,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    import uvicorn

    config = get_config()

    print("Starting Unified LakeFS Webhook Server")
    print(f"  Trino: {config.trino_host}:{config.trino_port}")
    print(f"  Catalog: {config.trino_catalog}")
    print(f"  MLOps repos: {config.mlops_repos}")
    print(f"  CVOps repo: {config.cvops_repo}")
    print(f"  Signature verification: {'enabled' if config.verify_signature else 'disabled'}")
    print("")
    print("Endpoints:")
    print("  POST /webhook/{domain}  - Domain-routed webhook (mlops, cvops, llmops)")
    print("  GET  /health            - Health check")
    print("  GET  /pending-events/{domain}")
    print("  POST /mark-processed/{domain}/{event_id}")
    print("")

    uvicorn.run(app, host=config.host, port=config.port)
