"""
LLMOps Pipelines Module
=======================

Dagster pipelines for LLM/RAG data processing (stub).

Planned Pipeline Flow:
    Corpus Ingest → Chunking → Embedding → Vector Store → RAG Inference

Usage:
    from src.llmops.pipelines import LLMOPS_ASSETS, LLMOPS_JOBS
"""

from src.llmops.pipelines.definitions import (
    llmops_init_lakefs,
    llmops_ingest_corpus,
    llmops_generate_embeddings,
    llmops_index_vectors,
    llmops_create_finetuning_data,
    llmops_full_job,
    LLMOPS_ASSETS,
    LLMOPS_JOBS,
)

__all__ = [
    # Assets
    "llmops_init_lakefs",
    "llmops_ingest_corpus",
    "llmops_generate_embeddings",
    "llmops_index_vectors",
    "llmops_create_finetuning_data",
    # Jobs
    "llmops_full_job",
    # Aggregated lists
    "LLMOPS_ASSETS",
    "LLMOPS_JOBS",
]
