"""
Iceberg Snapshot Change Sensor
===============================================================================

Monitors Iceberg tables for rollback events by polling the $snapshots metadata
table. Mirrors the LakeFS webhook pattern for automated rollback propagation.

Detection triggers:
1. Snapshot ID decreased (rollback_to_snapshot executed)
2. Snapshot timestamp regressed
3. Parent chain broken

On detection:
- Logs warning with details
- Yields RunRequest to trigger rollback_sync_job
- Updates cursor table
- Stores event for audit trail

Usage:
    The sensor is automatically registered with Dagster and polls every 30s.
    Configure via environment variables:
    - ICEBERG_SNAPSHOT_SENSOR_ENABLED: Enable/disable sensor (default: true)
    - ICEBERG_SNAPSHOT_POLL_INTERVAL: Poll interval in seconds (default: 30)
    - ICEBERG_MONITORED_TABLES: Comma-separated table list
"""

import os
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any

from dagster import (
    sensor,
    SensorEvaluationContext,
    RunRequest,
    SkipReason,
    DefaultSensorStatus,
)

from src.core.config import TRINO_CATALOG


# =============================================================================
# CONFIGURATION
# =============================================================================

# Tables to monitor for rollback events
MONITORED_TABLES = [
    f"{TRINO_CATALOG}.gold.fraud_training_data",
    f"{TRINO_CATALOG}.silver.fraud_transactions",
]

# Override from environment if specified
_env_tables = os.getenv("ICEBERG_MONITORED_TABLES", "")
if _env_tables:
    MONITORED_TABLES = [t.strip() for t in _env_tables.split(",") if t.strip()]

# Table names for tracking
CURSOR_TABLE = f"{TRINO_CATALOG}.metadata.snapshot_cursor"
EVENT_TABLE = f"{TRINO_CATALOG}.metadata.iceberg_rollback_events"

# Sensor configuration
SENSOR_ENABLED = os.getenv("ICEBERG_SNAPSHOT_SENSOR_ENABLED", "true").lower() == "true"
POLL_INTERVAL = int(os.getenv("ICEBERG_SNAPSHOT_POLL_INTERVAL", "30"))


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def _parse_timestamp_to_epoch(ts) -> Optional[float]:
    """
    Robustly parse various timestamp formats to epoch seconds.

    Handles:
    - datetime objects (with or without timezone)
    - ISO 8601 strings
    - Trino timestamp strings (with/without UTC suffix)
    - Various date formats

    Returns:
        Epoch timestamp in seconds, or None if parsing fails
    """
    if ts is None:
        return None

    try:
        # If it's already a datetime object
        if hasattr(ts, 'timestamp'):
            # Handle timezone-aware datetimes
            dt = ts
            if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
                return dt.timestamp()
            else:
                # Assume UTC for naive datetimes
                return dt.timestamp()

        # Convert to string for parsing
        ts_str = str(ts).strip()

        # Try dateutil parser first (most flexible)
        try:
            from dateutil import parser as dateutil_parser
            dt = dateutil_parser.parse(ts_str)
            return dt.timestamp()
        except ImportError:
            pass  # dateutil not available, fall through to manual parsing

        # Manual parsing fallbacks
        # Handle common formats from Trino/Iceberg

        # Remove UTC suffix variations
        ts_str = ts_str.replace(' UTC', '+00:00')
        ts_str = ts_str.replace('Z', '+00:00')

        # Handle space separator (Trino format) vs T separator (ISO)
        if ' ' in ts_str and 'T' not in ts_str:
            # Check if it's a date-only format
            if ':' in ts_str:
                ts_str = ts_str.replace(' ', 'T', 1)

        # Try ISO format with timezone
        try:
            dt = datetime.fromisoformat(ts_str)
            return dt.timestamp()
        except ValueError:
            pass

        # Try without microseconds
        for fmt in [
            '%Y-%m-%dT%H:%M:%S%z',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d',
        ]:
            try:
                dt = datetime.strptime(ts_str.split('.')[0].split('+')[0], fmt)
                return dt.timestamp()
            except ValueError:
                continue

        print(f"[IcebergSensor] Could not parse timestamp: {ts}")
        return None

    except Exception as e:
        print(f"[IcebergSensor] Timestamp parsing error for {ts}: {e}")
        return None


def ensure_cursor_table(trino) -> bool:
    """
    Create cursor table if it doesn't exist.

    Returns True if table is ready, False if creation failed.
    """
    try:
        if not trino.table_exists(CURSOR_TABLE):
            print(f"[IcebergSensor] Creating cursor table: {CURSOR_TABLE}")
            trino.execute_ddl(f"""
                CREATE TABLE IF NOT EXISTS {CURSOR_TABLE} (
                    table_name VARCHAR,
                    last_snapshot_id BIGINT,
                    last_snapshot_timestamp TIMESTAMP,
                    checked_at TIMESTAMP
                ) WITH (
                    format = 'PARQUET'
                )
            """)
        return True
    except Exception as e:
        print(f"[IcebergSensor] Failed to create cursor table: {e}")
        return False


def ensure_event_table(trino) -> bool:
    """
    Create event table if it doesn't exist.

    Returns True if table is ready, False if creation failed.
    """
    try:
        if not trino.table_exists(EVENT_TABLE):
            print(f"[IcebergSensor] Creating event table: {EVENT_TABLE}")
            trino.execute_ddl(f"""
                CREATE TABLE IF NOT EXISTS {EVENT_TABLE} (
                    event_id VARCHAR,
                    table_name VARCHAR,
                    previous_snapshot_id BIGINT,
                    current_snapshot_id BIGINT,
                    detected_at TIMESTAMP,
                    status VARCHAR,
                    dagster_run_id VARCHAR,
                    partition_date DATE
                ) WITH (
                    format = 'PARQUET',
                    partitioning = ARRAY['partition_date']
                )
            """)
        return True
    except Exception as e:
        print(f"[IcebergSensor] Failed to create event table: {e}")
        return False


def get_current_snapshot(trino, table: str) -> Optional[Dict[str, Any]]:
    """
    Query current snapshot from Iceberg metadata table.

    Uses $refs table to get the actual current snapshot (not just most recent by time).
    This correctly handles rollbacks where the current snapshot is an older one.

    Returns dict with snapshot_id, committed_at, parent_id or None.
    """
    try:
        parts = table.split(".")
        if len(parts) != 3:
            return None

        catalog, schema, tbl = parts

        # First get the current snapshot ID from $refs (this is the actual current state)
        refs_query = f"""
        SELECT snapshot_id
        FROM {catalog}.{schema}."{tbl}$refs"
        WHERE name = 'main'
        """
        refs_result = trino.execute_query(refs_query)
        if not refs_result or not refs_result[0][0]:
            return None

        current_snapshot_id = int(refs_result[0][0])

        # Now get the snapshot details
        snapshot_query = f"""
        SELECT snapshot_id, committed_at, parent_id
        FROM {catalog}.{schema}."{tbl}$snapshots"
        WHERE snapshot_id = {current_snapshot_id}
        """
        result = trino.execute_query(snapshot_query)
        if result and result[0][0]:
            return {
                "snapshot_id": int(result[0][0]),
                "committed_at": result[0][1],
                "parent_id": result[0][2],
            }
    except Exception as e:
        print(f"[IcebergSensor] Failed to get snapshot for {table}: {e}")

    return None


def get_cursor(trino, table: str) -> Optional[Dict[str, Any]]:
    """
    Get last known snapshot from cursor table.

    Returns dict with snapshot_id, timestamp or None.
    """
    try:
        from src.core.config import escape_sql_string
        table_safe = escape_sql_string(table)
        query = f"""
        SELECT last_snapshot_id, last_snapshot_timestamp
        FROM {CURSOR_TABLE}
        WHERE table_name = '{table_safe}'
        """
        result = trino.execute_query(query)
        if result and result[0][0]:
            return {
                "snapshot_id": int(result[0][0]),
                "timestamp": result[0][1],
            }
    except Exception as e:
        # Table might not exist yet or no entry for this table
        pass

    return None


def update_cursor(trino, table: str, snapshot: Dict[str, Any]):
    """
    Update cursor with current snapshot using MERGE (upsert).
    """
    try:
        from src.core.config import escape_sql_string
        table_safe = escape_sql_string(table)
        committed_at = snapshot["committed_at"]
        # Format timestamp if it's a datetime object
        if hasattr(committed_at, "strftime"):
            committed_at = committed_at.strftime("%Y-%m-%d %H:%M:%S")

        # Try to update existing row first
        update_query = f"""
        UPDATE {CURSOR_TABLE}
        SET last_snapshot_id = {snapshot['snapshot_id']},
            last_snapshot_timestamp = TIMESTAMP '{committed_at}',
            checked_at = CURRENT_TIMESTAMP
        WHERE table_name = '{table_safe}'
        """

        # Check if row exists
        check_query = f"SELECT 1 FROM {CURSOR_TABLE} WHERE table_name = '{table_safe}'"
        result = trino.execute_query(check_query)

        if result:
            trino.execute_ddl(update_query)
        else:
            # Insert new row
            insert_query = f"""
            INSERT INTO {CURSOR_TABLE} VALUES (
                '{table_safe}',
                {snapshot['snapshot_id']},
                TIMESTAMP '{committed_at}',
                CURRENT_TIMESTAMP
            )
            """
            trino.execute_ddl(insert_query)

    except Exception as e:
        print(f"[IcebergSensor] Failed to update cursor for {table}: {e}")


def record_rollback_event(
    trino,
    table: str,
    previous_snapshot: int,
    current_snapshot: int,
    run_key: str,
) -> str:
    """
    Record a rollback detection event in the event table.

    Returns the event_id.
    """
    event_id = f"ROLLBACK_{uuid.uuid4().hex[:12]}"
    now = datetime.now()

    try:
        ensure_event_table(trino)

        query = f"""
        INSERT INTO {EVENT_TABLE} VALUES (
            '{event_id}',
            '{table}',
            {previous_snapshot},
            {current_snapshot},
            TIMESTAMP '{now.strftime('%Y-%m-%d %H:%M:%S')}',
            'detected',
            '{run_key}',
            DATE '{now.strftime('%Y-%m-%d')}'
        )
        """
        trino.execute_ddl(query)
        print(f"[IcebergSensor] Recorded event: {event_id}")

    except Exception as e:
        print(f"[IcebergSensor] Failed to record event: {e}")

    return event_id


# =============================================================================
# DAGSTER SENSOR
# =============================================================================

@sensor(
    name="iceberg_snapshot_sensor",
    job_name="rollback_sync_job",  # Target job to run when rollback detected
    minimum_interval_seconds=POLL_INTERVAL,
    default_status=DefaultSensorStatus.RUNNING,  # Auto-start to detect rollbacks
    description="Monitors Iceberg tables for rollback events by polling $snapshots metadata",
)
def iceberg_snapshot_sensor(context: SensorEvaluationContext):
    """
    Poll Iceberg $snapshots table to detect rollbacks.

    Detection Logic:
    1. Query current snapshot_id from $snapshots table
    2. Compare with last known snapshot_id from cursor table
    3. If current < last (regression), a rollback occurred
    4. Trigger rollback_sync_job to propagate changes

    The sensor runs every 30 seconds (configurable) and checks all
    monitored tables for snapshot changes.
    """
    # Check if sensor is enabled
    if not SENSOR_ENABLED:
        return SkipReason("Iceberg snapshot sensor is disabled via ICEBERG_SNAPSHOT_SENSOR_ENABLED=false")

    # Get Trino connection
    try:
        from src.core.resources import TrinoResource
        trino = TrinoResource()
    except Exception as e:
        return SkipReason(f"Trino not available: {e}")

    # Ensure cursor table exists
    if not ensure_cursor_table(trino):
        context.log.warning("Could not ensure cursor table exists")

    rollbacks_detected = 0
    tables_checked = 0

    for table in MONITORED_TABLES:
        try:
            tables_checked += 1

            # Get current snapshot
            current = get_current_snapshot(trino, table)
            if not current:
                context.log.debug(f"No snapshot found for {table}")
                continue

            # Get last known snapshot from cursor
            cursor = get_cursor(trino, table)

            # First time seeing this table - just record and continue
            if not cursor:
                update_cursor(trino, table, current)
                context.log.info(
                    f"Initialized cursor for {table}: snapshot_id={current['snapshot_id']}"
                )
                continue

            # Check for rollback:
            # 1. Snapshot ID changed (new snapshot created)
            # 2. Current snapshot's committed_at is OLDER than the cursor timestamp
            #    (This indicates data was rolled back to an earlier point in time)
            # 3. Or snapshot ID changed but not a simple append (parent chain broken)

            is_rollback = False
            rollback_reason = ""

            if current["snapshot_id"] != cursor["snapshot_id"]:
                # Snapshot changed - check if it's a rollback
                current_ts = current.get("committed_at")
                cursor_ts = cursor.get("timestamp")

                # If current snapshot was committed at a time OLDER than cursor
                # it means we rolled back to an older snapshot
                if current_ts and cursor_ts:
                    # Robust timestamp parsing using helper function
                    current_epoch = _parse_timestamp_to_epoch(current_ts)
                    cursor_epoch = _parse_timestamp_to_epoch(cursor_ts)

                    # If current snapshot is older than what we last saw, it's a rollback
                    if current_epoch is not None and cursor_epoch is not None:
                        if current_epoch < cursor_epoch:
                            is_rollback = True
                            rollback_reason = f"Snapshot timestamp regressed from {cursor_ts} to {current_ts}"

                # Also check if parent chain is broken (snapshot jumped to non-child)
                if current.get("parent_id") and current["parent_id"] != cursor["snapshot_id"]:
                    # Parent is not our last known snapshot - possible rollback
                    is_rollback = True
                    rollback_reason = f"Parent chain broken: expected parent {cursor['snapshot_id']}, got {current.get('parent_id')}"

            if is_rollback:
                rollbacks_detected += 1
                table_short = table.split(".")[-1] if "." in table else table

                context.log.warning("")
                context.log.warning("╔" + "═" * 60 + "╗")
                context.log.warning("║" + " 🚨 ICEBERG ROLLBACK DETECTED".center(60) + "║")
                context.log.warning("╠" + "═" * 60 + "╣")
                context.log.warning(f"║  Table:             {table_short:<36}  ║")
                context.log.warning(f"║  Previous Snapshot: {cursor['snapshot_id']:<36}  ║")
                context.log.warning(f"║  Current Snapshot:  {current['snapshot_id']:<36}  ║")
                context.log.warning(f"║  Reason:            {rollback_reason[:36]:<36}  ║")
                context.log.warning("╚" + "═" * 60 + "╝")
                context.log.warning("")

                # IDEMPOTENCY CHECK: Skip if cleanup already in progress or recently completed
                try:
                    from src.mlops.pipelines.rollback_sync import should_skip_cleanup
                    should_skip, skip_reason = should_skip_cleanup(
                        trino, "iceberg_snapshot_sensor", table
                    )
                    if should_skip:
                        context.log.info(f"Skipping {table} - {skip_reason}")
                        # Still update cursor to prevent repeated detection
                        update_cursor(trino, table, current)
                        continue
                except ImportError:
                    pass  # Function not available, proceed with cleanup

                # Generate unique run key
                run_key = f"iceberg_rollback_{table.replace('.', '_')}_{current['snapshot_id']}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

                # Record event for audit trail
                event_id = record_rollback_event(
                    trino=trino,
                    table=table,
                    previous_snapshot=cursor["snapshot_id"],
                    current_snapshot=current["snapshot_id"],
                    run_key=run_key,
                )

                # Trigger rollback sync job
                yield RunRequest(
                    run_key=run_key,
                    run_config={
                        "ops": {
                            "sync_rollback_op": {
                                "config": {
                                    "table": table,
                                    "target_snapshot_id": current["snapshot_id"],
                                    "dry_run": False,
                                }
                            }
                        }
                    },
                    tags={
                        "event_type": "iceberg_rollback",
                        "event_id": event_id,
                        "table": table,
                        "previous_snapshot": str(cursor["snapshot_id"]),
                        "current_snapshot": str(current["snapshot_id"]),
                        "trigger_source": "iceberg_snapshot_sensor",
                    },
                )

                # SELF-HEALING: Check if snapshot contains reverted data and clean it
                # This is Layer 2 (Reactive) protection against rollbacks that restore
                # data from reverted LakeFS commits
                try:
                    from src.mlops.pipelines.rollback_sync import cleanup_reverted_commits
                    cleanup_result = cleanup_reverted_commits(trino, table)
                    if cleanup_result["cleaned_count"] > 0:
                        context.log.warning("┌─── SELF-HEALING ACTIVATED ───")
                        context.log.warning(f"│ ✓ Removed {cleanup_result['cleaned_count']} records from reverted commits")
                        for commit in cleanup_result['commits_cleaned'][:3]:  # Show first 3
                            context.log.warning(f"│   • Commit: {commit[:12]}...")
                        context.log.warning("└───────────────────────────────")
                except Exception as e:
                    context.log.error(f"Self-healing cleanup failed: {e}")

            # Update cursor with current snapshot
            update_cursor(trino, table, current)

        except Exception as e:
            context.log.error(f"Error checking {table}: {e}")

    # Return skip reason if no rollbacks detected
    if rollbacks_detected == 0:
        return SkipReason(
            f"No rollbacks detected in {tables_checked} monitored tables"
        )


# =============================================================================
# EXPORTS
# =============================================================================

ICEBERG_SNAPSHOT_SENSORS = [iceberg_snapshot_sensor]

__all__ = [
    "iceberg_snapshot_sensor",
    "ICEBERG_SNAPSHOT_SENSORS",
    "MONITORED_TABLES",
    "CURSOR_TABLE",
    "EVENT_TABLE",
]
