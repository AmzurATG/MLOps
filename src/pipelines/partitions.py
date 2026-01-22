"""
Partitions for Backfill Support
Simple date-based partitions for MLOps/CVOps pipelines.
"""
import os
from datetime import datetime, timedelta
from dagster import DailyPartitionsDefinition


# Partition start date: configurable via env, default to 6 months ago
_default_start = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
PARTITION_START_DATE = os.getenv("PARTITION_START_DATE", _default_start)

# Daily partitions - use for incremental processing
daily_partitions = DailyPartitionsDefinition(
    start_date=PARTITION_START_DATE,
    timezone="UTC",
    end_offset=0,  # Include today
)


def get_partition_date(partition_key: str) -> datetime:
    """Convert partition key to datetime."""
    return datetime.strptime(partition_key, "%Y-%m-%d")


def get_current_partition() -> str:
    """Get today's partition key."""
    return datetime.utcnow().strftime("%Y-%m-%d")
