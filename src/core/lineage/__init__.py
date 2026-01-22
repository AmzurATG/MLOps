"""
Core Lineage Module
===================

Shared lineage tracking patterns for all domains (MLOps, CVOps, LLMOps).

Provides:
- LineageInfo dataclass for pipeline lineage
- collect_lineage() for comprehensive lineage collection
- UnifiedLineageTracker for MLflow integration

Usage:
    from src.core.lineage import LineageInfo, collect_lineage, UnifiedLineageTracker
"""

from src.core.lineage.tracker import (
    LineageInfo,
    collect_lineage,
    collect_mlops_lineage,
    collect_cvops_lineage,
    log_lineage_to_mlflow,
    log_lineage_to_iceberg,
)
from src.core.lineage.mlflow_lineage import (
    UnifiedLineageTracker,
    get_iceberg_snapshot,
    get_table_snapshot_count,
    generate_data_version,
    get_data_version_from_table,
    create_mlops_tracker,
    create_cvops_tracker,
    create_llmops_tracker,
    STAGE_BRONZE,
    STAGE_SILVER,
    STAGE_GOLD,
    STAGE_ANNOTATION,
    STAGE_TRAINING,
    STAGE_INFERENCE,
)

__all__ = [
    # From tracker
    "LineageInfo",
    "collect_lineage",
    "collect_mlops_lineage",
    "collect_cvops_lineage",
    "log_lineage_to_mlflow",
    "log_lineage_to_iceberg",
    # From mlflow_lineage
    "UnifiedLineageTracker",
    "get_iceberg_snapshot",
    "get_table_snapshot_count",
    "generate_data_version",
    "get_data_version_from_table",
    "create_mlops_tracker",
    "create_cvops_tracker",
    "create_llmops_tracker",
    # Constants
    "STAGE_BRONZE",
    "STAGE_SILVER",
    "STAGE_GOLD",
    "STAGE_ANNOTATION",
    "STAGE_TRAINING",
    "STAGE_INFERENCE",
]
