"""
Webhook Data Models
===================

Pydantic models for LakeFS webhook events.
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


class LakeFSEvent(BaseModel):
    """LakeFS webhook event payload."""

    event_id: Optional[str] = Field(default=None, alias="event_id")
    event_type: str = Field(default="unknown", alias="event_type")
    repository_id: str = Field(default="", alias="repository_id")
    branch_id: str = Field(default="", alias="branch_id")
    commit_id: Optional[str] = Field(default=None, alias="commit_id")
    commit_message: Optional[str] = Field(default=None, alias="commit_message")
    source_ref: Optional[str] = Field(default=None, alias="source_ref")
    committer: Optional[str] = Field(default=None, alias="committer")
    diff: Optional[List[Dict[str, Any]]] = Field(default=None, alias="diff")

    class Config:
        populate_by_name = True
        extra = "allow"


class WebhookEventRecord(BaseModel):
    """Database record for a webhook event."""

    id: int
    event_id: str
    event_type: str
    domain: str  # mlops, cvops, llmops
    repository: str
    branch: str
    commit_id: str
    reverted_commit: Optional[str] = None
    source_ref: Optional[str] = None
    received_at: datetime
    processed_at: Optional[datetime] = None
    status: str = "pending"
    error_message: Optional[str] = None
    retry_count: int = 0
    metadata: Optional[Dict[str, Any]] = None


class WebhookResponse(BaseModel):
    """Standard webhook response."""

    status: str
    event_id: Optional[int] = None
    message: Optional[str] = None
    error: Optional[str] = None
    domain: Optional[str] = None
    repository: Optional[str] = None
    branch: Optional[str] = None
    sync_triggered: bool = False
    sync_result: Optional[Dict[str, Any]] = None


class HealthResponse(BaseModel):
    """Health check response."""

    status: str
    service: str
    timestamp: str
    domains: List[str]
    signature_verification: str
