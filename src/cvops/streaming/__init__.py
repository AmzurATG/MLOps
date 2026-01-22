"""
CVOps Streaming Module
======================

Kafka consumers for image metadata ingestion.

Components:
- CVIngestConsumer: Consumes image metadata and validates/stores in LakeFS

Usage:
    from cvops.streaming import run_consumer

    run_consumer()
"""

from cvops.streaming.ingest_consumer import run_consumer

__all__ = ["run_consumer"]
