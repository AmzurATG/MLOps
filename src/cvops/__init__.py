"""
CVOps Module - Computer Vision Domain
======================================

This module contains all computer vision-specific code:
- Dagster pipelines (pipelines/)
- FastAPI endpoints (api/)
- Kafka consumers (streaming/)

Pipeline Flow:
    Image Ingest → Manifest → Detection → Annotation → Gold → Training

Usage:
    # Import pipeline definitions
    from cvops.pipelines import defs

    # Import API app
    from cvops.api import app
"""

__all__ = []
