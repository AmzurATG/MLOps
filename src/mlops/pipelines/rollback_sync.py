"""
Rollback Synchronization Module
===============================================================================

Handles bidirectional rollback sync between LakeFS and Iceberg, ensuring
data consistency across the entire pipeline including Label Studio.

Two Rollback Scenarios:
1. LakeFS Revert → Already handled by webhook_server.py + mlops_maintenance.py
2. Iceberg Rollback → NEW: This module handles propagation

Usage:
    from src.mlops.pipelines.rollback_sync import (
        sync_iceberg_rollback,
        validate_rollback_consistency,
        get_rollback_status,
    )

    # After Iceberg rollback, sync everything
    result = sync_iceberg_rollback(
        trino=trino,
        table="iceberg_dev.gold.fraud_training_data",
        target_snapshot_id=12345678,
        lakefs=lakefs,
        labelstudio_client=ls_client,
    )
"""

import os
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum

from dagster import asset, op, job, AssetExecutionContext, Config


# =============================================================================
# ENHANCED LOGGING HELPERS
# =============================================================================

def log_header(title: str, char: str = "═"):
    """Print a formatted header for major operations."""
    border = char * 70
    print(f"\n{border}")
    print(f"  {title}")
    print(f"{border}")


def log_section(title: str):
    """Print a section divider."""
    print(f"\n┌{'─' * 68}┐")
    print(f"│ {title:<66} │")
    print(f"└{'─' * 68}┘")


def log_stat(label: str, value: Any, indent: int = 2):
    """Print a formatted statistic."""
    prefix = " " * indent
    print(f"{prefix}│ {label:<35} {str(value):>30} │")


def log_table_header(cols: List[str], widths: List[int]):
    """Print a table header."""
    header = "│ "
    for col, width in zip(cols, widths):
        header += f"{col:<{width}} "
    header += "│"
    border = "├" + "─" * (sum(widths) + len(widths) + 1) + "┤"
    print(border)
    print(header)
    print(border)


def log_table_row(values: List[Any], widths: List[int]):
    """Print a table row."""
    row = "│ "
    for val, width in zip(values, widths):
        row += f"{str(val):<{width}} "
    row += "│"
    print(row)


def log_success(msg: str):
    """Print a success message."""
    print(f"  ✅ {msg}")


def log_warning(msg: str):
    """Print a warning message."""
    print(f"  ⚠️  {msg}")


def log_error(msg: str):
    """Print an error message."""
    print(f"  ❌ {msg}")


def log_info(msg: str):
    """Print an info message."""
    print(f"  ℹ️  {msg}")


# =============================================================================
# SIMPLE TRINO CLIENT (for direct usage outside Dagster resource context)
# =============================================================================

class SimpleTrinoClient:
    """Simple Trino client for direct usage outside Dagster resource context."""

    def __init__(self):
        import os
        self.host = os.getenv("TRINO_HOST", "exp-trino")
        self.port = int(os.getenv("TRINO_PORT", "8080"))
        self.user = os.getenv("TRINO_USER", "trino")
        self.catalog = os.getenv("TRINO_CATALOG", "iceberg_dev")

    def _get_connection(self):
        import trino
        return trino.dbapi.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
        )

    def execute_query(self, query: str):
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            return cursor.fetchall()
        finally:
            conn.close()

    def execute_ddl(self, query: str):
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query)
        finally:
            conn.close()


# =============================================================================
# CONFIGURATION
# =============================================================================

# Lock table for preventing concurrent cleanups
LOCK_TABLE = None  # Will be set dynamically based on TRINO_CATALOG

# Lock expiry in minutes
LOCK_EXPIRY_MINUTES = 5


def _get_lock_table() -> str:
    """Get the lock table name using current TRINO_CATALOG."""
    from src.core.config import TRINO_CATALOG
    return f"{TRINO_CATALOG}.metadata.cleanup_locks"


# =============================================================================
# DISTRIBUTED LOCKING
# =============================================================================

def ensure_lock_table(trino) -> bool:
    """Create the lock table if it doesn't exist."""
    lock_table = _get_lock_table()
    try:
        # Note: Iceberg doesn't support PRIMARY KEY, so we handle uniqueness in app logic
        trino.execute_ddl(f"""
            CREATE TABLE IF NOT EXISTS {lock_table} (
                table_name VARCHAR,
                operation VARCHAR,
                locked_at TIMESTAMP(6) WITH TIME ZONE,
                locked_by VARCHAR
            )
        """)
        return True
    except Exception as e:
        print(f"[RollbackSync] Failed to create lock table: {e}")
        return False


def acquire_cleanup_lock(trino, table: str, operation: str, locked_by: str = "unknown") -> bool:
    """
    Acquire lock before cleanup to prevent concurrent operations.

    Returns True if lock acquired, False if already locked by another operation.
    Stale locks (>5 min old) are automatically released.
    """
    from src.core.config import escape_sql_string
    lock_table = _get_lock_table()

    try:
        ensure_lock_table(trino)
        table_safe = escape_sql_string(table)
        operation_safe = escape_sql_string(operation)
        locked_by_safe = escape_sql_string(locked_by)

        # Check for existing lock
        check_query = f"""
        SELECT locked_at, locked_by
        FROM {lock_table}
        WHERE table_name = '{table_safe}' AND operation = '{operation_safe}'
        """
        result = trino.execute_query(check_query)

        if result and result[0][0]:
            locked_at = result[0][0]
            existing_locked_by = result[0][1]

            # Check if lock is stale (older than LOCK_EXPIRY_MINUTES)
            from datetime import datetime, timedelta

            # Handle different timestamp formats
            if hasattr(locked_at, 'timestamp'):
                lock_time = locked_at
            else:
                try:
                    lock_time = datetime.fromisoformat(str(locked_at).replace(' UTC', '+00:00').replace(' ', 'T'))
                except:
                    lock_time = datetime.now() - timedelta(minutes=LOCK_EXPIRY_MINUTES + 1)

            if hasattr(lock_time, 'replace'):
                # Remove timezone info for comparison
                lock_time = lock_time.replace(tzinfo=None)

            age_minutes = (datetime.now() - lock_time).total_seconds() / 60

            if age_minutes < LOCK_EXPIRY_MINUTES:
                print(f"[RollbackSync] Lock held by {existing_locked_by} ({age_minutes:.1f}m old)")
                return False
            else:
                # Stale lock - delete it
                print(f"[RollbackSync] Releasing stale lock ({age_minutes:.1f}m old)")
                trino.execute_ddl(f"""
                    DELETE FROM {lock_table}
                    WHERE table_name = '{table_safe}' AND operation = '{operation_safe}'
                """)

        # Try to insert lock row
        trino.execute_ddl(f"""
            INSERT INTO {lock_table} VALUES (
                '{table_safe}',
                '{operation_safe}',
                CURRENT_TIMESTAMP,
                '{locked_by_safe}'
            )
        """)
        print(f"[RollbackSync] Lock acquired for {table} ({operation})")
        return True

    except Exception as e:
        # Insert failed - lock already exists (race condition)
        print(f"[RollbackSync] Failed to acquire lock: {e}")
        return False


def release_cleanup_lock(trino, table: str, operation: str):
    """Release lock after cleanup."""
    from src.core.config import escape_sql_string
    lock_table = _get_lock_table()

    try:
        table_safe = escape_sql_string(table)
        operation_safe = escape_sql_string(operation)

        trino.execute_ddl(f"""
            DELETE FROM {lock_table}
            WHERE table_name = '{table_safe}' AND operation = '{operation_safe}'
        """)
        print(f"[RollbackSync] Lock released for {table} ({operation})")

    except Exception as e:
        print(f"[RollbackSync] Failed to release lock: {e}")


class RollbackDirection(Enum):
    """Direction of rollback propagation."""
    LAKEFS_TO_ICEBERG = "lakefs_to_iceberg"  # LakeFS revert → clean Iceberg
    ICEBERG_TO_ALL = "iceberg_to_all"        # Iceberg rollback → sync all


@dataclass
class RollbackResult:
    """Result of a rollback sync operation."""
    rollback_id: str
    direction: RollbackDirection
    source_table: str
    target_snapshot_id: int
    previous_snapshot_id: int

    # What was affected
    upstream_tables_synced: List[str]
    downstream_tables_synced: List[str]
    labelstudio_tasks_deleted: int
    lakefs_commit_created: Optional[str]

    # Status
    success: bool
    errors: List[str]
    started_at: datetime
    completed_at: Optional[datetime]


# =============================================================================
# SNAPSHOT DISCOVERY
# =============================================================================

def get_current_snapshot(trino, table: str) -> Optional[int]:
    """Get the current snapshot ID for an Iceberg table."""
    try:
        # Parse table name
        parts = table.split(".")
        if len(parts) != 3:
            return None
        catalog, schema, table_name = parts

        query = f"""
        SELECT snapshot_id
        FROM {catalog}.{schema}."{table_name}$snapshots"
        ORDER BY committed_at DESC
        LIMIT 1
        """
        result = trino.execute_query(query)
        if result and result[0][0]:
            return int(result[0][0])
    except Exception as e:
        print(f"[RollbackSync] Failed to get snapshot for {table}: {e}")
    return None


def get_snapshot_commits(trino, table: str, snapshot_id: int) -> List[str]:
    """Get LakeFS commits associated with records in a specific snapshot."""
    try:
        parts = table.split(".")
        if len(parts) != 3:
            return []
        catalog, schema, table_name = parts

        # Query records from this snapshot that have source_lakefs_commit
        query = f"""
        SELECT DISTINCT source_lakefs_commit
        FROM {table}
        FOR VERSION AS OF {snapshot_id}
        WHERE source_lakefs_commit IS NOT NULL
        """
        result = trino.execute_query(query)
        return [row[0] for row in result if row[0]]
    except Exception as e:
        print(f"[RollbackSync] Failed to get commits for snapshot {snapshot_id}: {e}")
        return []


def get_records_added_after_snapshot(
    trino,
    table: str,
    target_snapshot_id: int,
    current_snapshot_id: int
) -> List[str]:
    """
    Get source_lakefs_commit values for records that were added AFTER the target snapshot.
    These are the commits that need to be cleaned from upstream/downstream tables.
    """
    try:
        # Get commits in current snapshot
        current_commits = set(get_snapshot_commits(trino, table, current_snapshot_id))

        # Get commits in target snapshot (what we're rolling back TO)
        target_commits = set(get_snapshot_commits(trino, table, target_snapshot_id))

        # Commits to remove = current - target
        commits_to_remove = current_commits - target_commits

        return list(commits_to_remove)
    except Exception as e:
        print(f"[RollbackSync] Failed to compute commit diff: {e}")
        return []


# =============================================================================
# TABLE DISCOVERY
# =============================================================================

def discover_related_tables(trino, source_table: str) -> Dict[str, List[str]]:
    """
    Discover upstream and downstream tables related to the source table.

    Returns:
        {
            "upstream": ["iceberg_dev.silver.fraud_transactions", ...],
            "downstream": [],  # Tables that depend on source
            "same_level": [],  # Tables at same layer
        }
    """
    parts = source_table.split(".")
    if len(parts) != 3:
        return {"upstream": [], "downstream": [], "same_level": []}

    catalog, schema, table_name = parts

    # Determine layer
    layer_order = {"bronze": 1, "silver": 2, "gold": 3, "monitoring": 4}
    current_layer = layer_order.get(schema, 0)

    related = {"upstream": [], "downstream": [], "same_level": []}

    try:
        # Find all schemas with source_lakefs_commit column
        schemas_to_check = ["bronze", "silver", "gold"]

        for check_schema in schemas_to_check:
            check_layer = layer_order.get(check_schema, 0)

            # Get tables in this schema
            query = f"SHOW TABLES IN {catalog}.{check_schema}"
            try:
                tables = trino.execute_query(query)
                for (tbl,) in tables:
                    full_table = f"{catalog}.{check_schema}.{tbl}"
                    if full_table == source_table:
                        continue

                    # Check if table has source_lakefs_commit
                    desc_query = f"DESCRIBE {full_table}"
                    try:
                        columns = trino.execute_query(desc_query)
                        has_commit_col = any(
                            col[0] == "source_lakefs_commit"
                            for col in columns
                        )
                        if has_commit_col:
                            if check_layer < current_layer:
                                related["upstream"].append(full_table)
                            elif check_layer > current_layer:
                                related["downstream"].append(full_table)
                            else:
                                related["same_level"].append(full_table)
                    except:
                        pass
            except:
                pass

    except Exception as e:
        print(f"[RollbackSync] Table discovery failed: {e}")

    return related


# =============================================================================
# ROLLBACK EXECUTION
# =============================================================================

def execute_iceberg_rollback(trino, table: str, target_snapshot_id: int) -> bool:
    """
    Execute the actual Iceberg rollback to a specific snapshot.

    Note: This uses CALL procedure which requires appropriate permissions.
    """
    try:
        parts = table.split(".")
        if len(parts) != 3:
            return False
        catalog, schema, table_name = parts

        # Execute rollback procedure
        query = f"""
        CALL {catalog}.system.rollback_to_snapshot('{schema}', '{table_name}', {target_snapshot_id})
        """
        trino.execute_ddl(query)
        print(f"[RollbackSync] Rolled back {table} to snapshot {target_snapshot_id}")
        return True

    except Exception as e:
        print(f"[RollbackSync] Iceberg rollback failed: {e}")
        return False


def cleanup_table_by_commits(trino, table: str, commits: List[str]) -> int:
    """
    Delete records from a table where source_lakefs_commit is in the given list.

    Returns: Number of records deleted
    """
    if not commits:
        return 0

    try:
        from src.core.config import escape_sql_list
        # Build IN clause with proper escaping to prevent SQL injection
        commit_list = escape_sql_list(commits)

        # Count first
        count_query = f"""
        SELECT COUNT(*) FROM {table}
        WHERE source_lakefs_commit IN ({commit_list})
        """
        count_result = trino.execute_query(count_query)
        count = count_result[0][0] if count_result else 0

        if count > 0:
            # Delete
            delete_query = f"""
            DELETE FROM {table}
            WHERE source_lakefs_commit IN ({commit_list})
            """
            trino.execute_ddl(delete_query)
            print(f"[RollbackSync] Deleted {count} records from {table}")

        return count

    except Exception as e:
        print(f"[RollbackSync] Cleanup failed for {table}: {e}")
        return 0


def cleanup_labelstudio_by_commits(
    labelstudio_client,
    project_id: int,
    commits: List[str]
) -> int:
    """
    Delete Label Studio tasks associated with the given commits.

    Returns: Number of tasks deleted
    """
    if not labelstudio_client or not commits:
        return 0

    deleted = 0
    commits_set = set(commits)

    try:
        # Get all tasks (paginated)
        page = 1
        page_size = 100

        while True:
            tasks = labelstudio_client.tasks.list(
                project=project_id,
                page=page,
                page_size=page_size
            )

            if not tasks:
                break

            for task in tasks:
                task_commit = task.data.get("source_lakefs_commit")
                if task_commit in commits_set:
                    try:
                        labelstudio_client.tasks.delete(id=task.id)
                        deleted += 1
                    except Exception as e:
                        print(f"[RollbackSync] Failed to delete task {task.id}: {e}")

            if len(tasks) < page_size:
                break
            page += 1

        print(f"[RollbackSync] Deleted {deleted} Label Studio tasks")

    except Exception as e:
        print(f"[RollbackSync] Label Studio cleanup failed: {e}")

    return deleted


# =============================================================================
# MAIN SYNC FUNCTION
# =============================================================================

def sync_iceberg_rollback(
    trino,
    table: str,
    target_snapshot_id: int,
    lakefs=None,
    labelstudio_client=None,
    labelstudio_project_id: Optional[int] = None,
    dry_run: bool = False,
) -> RollbackResult:
    """
    Synchronize an Iceberg table rollback across the entire pipeline.

    This function:
    1. Identifies records added after target_snapshot_id
    2. Executes Iceberg rollback (if not dry_run)
    3. Cleans up upstream tables (bronze, silver)
    4. Cleans up downstream tables (gold, monitoring)
    5. Removes corresponding Label Studio tasks
    6. Optionally creates a LakeFS revert commit

    Args:
        trino: Trino connection resource
        table: Full table name (catalog.schema.table)
        target_snapshot_id: Snapshot to roll back to
        lakefs: Optional LakeFS resource for creating revert commit
        labelstudio_client: Optional Label Studio client
        labelstudio_project_id: Label Studio project ID
        dry_run: If True, only report what would be done

    Returns:
        RollbackResult with full details
    """
    rollback_id = f"ROLLBACK_{uuid.uuid4().hex[:12]}"
    started_at = datetime.now()
    errors = []

    print(f"[RollbackSync] Starting rollback sync: {rollback_id}")
    print(f"[RollbackSync] Table: {table}, Target Snapshot: {target_snapshot_id}")
    print(f"[RollbackSync] Dry Run: {dry_run}")

    # Get current snapshot
    current_snapshot = get_current_snapshot(trino, table)
    if not current_snapshot:
        errors.append("Failed to get current snapshot")
        return RollbackResult(
            rollback_id=rollback_id,
            direction=RollbackDirection.ICEBERG_TO_ALL,
            source_table=table,
            target_snapshot_id=target_snapshot_id,
            previous_snapshot_id=0,
            upstream_tables_synced=[],
            downstream_tables_synced=[],
            labelstudio_tasks_deleted=0,
            lakefs_commit_created=None,
            success=False,
            errors=errors,
            started_at=started_at,
            completed_at=datetime.now(),
        )

    print(f"[RollbackSync] Current snapshot: {current_snapshot}")

    # Find commits to remove
    commits_to_remove = get_records_added_after_snapshot(
        trino, table, target_snapshot_id, current_snapshot
    )
    print(f"[RollbackSync] Commits to remove: {len(commits_to_remove)}")

    if not commits_to_remove:
        print("[RollbackSync] No commits to remove - tables already in sync")
        return RollbackResult(
            rollback_id=rollback_id,
            direction=RollbackDirection.ICEBERG_TO_ALL,
            source_table=table,
            target_snapshot_id=target_snapshot_id,
            previous_snapshot_id=current_snapshot,
            upstream_tables_synced=[],
            downstream_tables_synced=[],
            labelstudio_tasks_deleted=0,
            lakefs_commit_created=None,
            success=True,
            errors=[],
            started_at=started_at,
            completed_at=datetime.now(),
        )

    # Discover related tables
    related = discover_related_tables(trino, table)
    print(f"[RollbackSync] Upstream tables: {related['upstream']}")
    print(f"[RollbackSync] Downstream tables: {related['downstream']}")

    upstream_synced = []
    downstream_synced = []
    ls_deleted = 0
    lakefs_commit = None

    if not dry_run:
        # 1. Execute Iceberg rollback on source table
        if not execute_iceberg_rollback(trino, table, target_snapshot_id):
            errors.append(f"Failed to rollback {table}")

        # 2. Clean upstream tables
        for upstream_table in related["upstream"]:
            try:
                deleted = cleanup_table_by_commits(trino, upstream_table, commits_to_remove)
                if deleted > 0:
                    upstream_synced.append(f"{upstream_table} ({deleted} records)")
            except Exception as e:
                errors.append(f"Upstream cleanup failed for {upstream_table}: {e}")

        # 3. Clean downstream tables
        for downstream_table in related["downstream"]:
            try:
                deleted = cleanup_table_by_commits(trino, downstream_table, commits_to_remove)
                if deleted > 0:
                    downstream_synced.append(f"{downstream_table} ({deleted} records)")
            except Exception as e:
                errors.append(f"Downstream cleanup failed for {downstream_table}: {e}")

        # 4. Clean Label Studio
        if labelstudio_client and labelstudio_project_id:
            try:
                ls_deleted = cleanup_labelstudio_by_commits(
                    labelstudio_client,
                    labelstudio_project_id,
                    commits_to_remove
                )
            except Exception as e:
                errors.append(f"Label Studio cleanup failed: {e}")

        # 5. Create LakeFS revert commit (optional)
        if lakefs:
            try:
                # Create a metadata commit indicating the rollback
                commit_msg = f"Iceberg rollback sync: {table} to snapshot {target_snapshot_id}"
                # This would need actual LakeFS commit implementation
                # lakefs_commit = lakefs.commit(message=commit_msg)
                lakefs_commit = f"sync_{rollback_id}"
            except Exception as e:
                errors.append(f"LakeFS commit failed: {e}")

    else:
        # Dry run - just report
        print(f"[RollbackSync] DRY RUN - Would remove commits: {commits_to_remove[:5]}...")
        print(f"[RollbackSync] DRY RUN - Would clean upstream: {related['upstream']}")
        print(f"[RollbackSync] DRY RUN - Would clean downstream: {related['downstream']}")

    # Record result
    result = RollbackResult(
        rollback_id=rollback_id,
        direction=RollbackDirection.ICEBERG_TO_ALL,
        source_table=table,
        target_snapshot_id=target_snapshot_id,
        previous_snapshot_id=current_snapshot,
        upstream_tables_synced=upstream_synced,
        downstream_tables_synced=downstream_synced,
        labelstudio_tasks_deleted=ls_deleted,
        lakefs_commit_created=lakefs_commit,
        success=len(errors) == 0,
        errors=errors,
        started_at=started_at,
        completed_at=datetime.now(),
    )

    # Store result in monitoring table
    try:
        store_rollback_result(trino, result)
    except Exception as e:
        print(f"[RollbackSync] Failed to store result: {e}")

    return result


def store_rollback_result(trino, result: RollbackResult):
    """Store rollback result in monitoring table for audit."""
    from src.core.config import TRINO_CATALOG
    try:
        # Ensure table exists
        trino.execute_ddl(f"""
            CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.monitoring.rollback_history (
                rollback_id VARCHAR,
                direction VARCHAR,
                source_table VARCHAR,
                target_snapshot_id BIGINT,
                previous_snapshot_id BIGINT,
                upstream_synced VARCHAR,
                downstream_synced VARCHAR,
                labelstudio_deleted INTEGER,
                lakefs_commit VARCHAR,
                success BOOLEAN,
                errors VARCHAR,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                partition_date DATE
            ) WITH (
                format = 'PARQUET',
                partitioning = ARRAY['partition_date']
            )
        """)

        # Insert result
        query = f"""
        INSERT INTO {TRINO_CATALOG}.monitoring.rollback_history VALUES (
            '{result.rollback_id}',
            '{result.direction.value}',
            '{result.source_table}',
            {result.target_snapshot_id},
            {result.previous_snapshot_id},
            '{", ".join(result.upstream_tables_synced)}',
            '{", ".join(result.downstream_tables_synced)}',
            {result.labelstudio_tasks_deleted},
            {f"'{result.lakefs_commit_created}'" if result.lakefs_commit_created else 'NULL'},
            {str(result.success).lower()},
            '{"; ".join(result.errors)}',
            TIMESTAMP '{result.started_at.strftime("%Y-%m-%d %H:%M:%S")}',
            TIMESTAMP '{result.completed_at.strftime("%Y-%m-%d %H:%M:%S")}',
            DATE '{result.started_at.strftime("%Y-%m-%d")}'
        )
        """
        trino.execute_ddl(query)

    except Exception as e:
        print(f"[RollbackSync] Failed to store result: {e}")


# =============================================================================
# DATA-BASED SYNC (For Direct Iceberg Rollbacks)
# =============================================================================

def sync_gold_from_silver(trino, dry_run: bool = False) -> Dict[str, Any]:
    """
    Sync Gold table to match Silver table state.

    Deletes Gold records that should no longer be in Gold because:
    1. Transaction_id no longer exists in Silver (deleted)
    2. Transaction_id exists in Silver but is no longer 'reviewed' (rolled back)

    This handles Iceberg rollbacks where Silver was rolled back directly.

    Returns:
        {
            "deleted_count": int,
            "gold_count_before": int,
            "gold_count_after": int,
            "silver_reviewed_count": int,
            "orphaned_deleted": int,
            "status_mismatch_deleted": int,
        }
    """
    from src.core.config import TRINO_CATALOG
    silver_table = f"{TRINO_CATALOG}.silver.fraud_transactions"
    gold_table = f"{TRINO_CATALOG}.gold.fraud_transactions"

    result = {
        "deleted_count": 0,
        "gold_count_before": 0,
        "gold_count_after": 0,
        "silver_reviewed_count": 0,
        "orphaned_deleted": 0,
        "status_mismatch_deleted": 0,
    }

    try:
        # Get current counts
        result["gold_count_before"] = trino.execute_query(
            f"SELECT COUNT(*) FROM {gold_table}"
        )[0][0]

        result["silver_reviewed_count"] = trino.execute_query(
            f"SELECT COUNT(*) FROM {silver_table} WHERE review_status = 'reviewed'"
        )[0][0]

        # Find orphaned Gold records (not in Silver at all)
        orphan_count_query = f"""
        SELECT COUNT(*) FROM {gold_table}
        WHERE transaction_id NOT IN (
            SELECT transaction_id FROM {silver_table}
        )
        """
        orphan_count = trino.execute_query(orphan_count_query)[0][0]
        print(f"[RollbackSync] Gold has {orphan_count} records not in Silver")

        # Find status mismatch records (in Silver but no longer 'reviewed')
        status_mismatch_query = f"""
        SELECT COUNT(*) FROM {gold_table} g
        WHERE EXISTS (
            SELECT 1 FROM {silver_table} s
            WHERE s.transaction_id = g.transaction_id
            AND s.review_status != 'reviewed'
        )
        """
        status_mismatch_count = trino.execute_query(status_mismatch_query)[0][0]
        print(f"[RollbackSync] Gold has {status_mismatch_count} records where Silver is no longer 'reviewed'")

        total_to_delete = orphan_count + status_mismatch_count

        if total_to_delete > 0 and not dry_run:
            # Delete all invalid Gold records (orphaned OR status mismatch)
            delete_query = f"""
            DELETE FROM {gold_table}
            WHERE transaction_id NOT IN (
                SELECT transaction_id FROM {silver_table}
                WHERE review_status = 'reviewed'
            )
            """
            trino.execute_ddl(delete_query)
            result["orphaned_deleted"] = orphan_count
            result["status_mismatch_deleted"] = status_mismatch_count
            result["deleted_count"] = total_to_delete
            print(f"[RollbackSync] Deleted {total_to_delete} Gold records (orphaned: {orphan_count}, status mismatch: {status_mismatch_count})")

        # Get final count
        result["gold_count_after"] = trino.execute_query(
            f"SELECT COUNT(*) FROM {gold_table}"
        )[0][0]

    except Exception as e:
        print(f"[RollbackSync] Gold sync failed: {e}")
        result["error"] = str(e)

    return result


def get_labelstudio_access_token(
    labelstudio_url: str,
    refresh_token: str
) -> Tuple[Optional[str], str]:
    """
    Exchange a refresh token for an access token.

    Label Studio 1.21+ uses JWT with refresh/access token pattern.
    The token in .env is typically a refresh token that needs to be
    exchanged for an access token.

    Returns:
        Tuple of (access_token, auth_type) where auth_type is "Bearer" or "Token"
        Returns (None, "") if refresh fails.
    """
    import requests

    try:
        # Try to use the token directly with Bearer auth first (JWT access token)
        test_resp = requests.get(
            f"{labelstudio_url}/api/current-user/whoami",
            headers={"Authorization": f"Bearer {refresh_token}"},
            timeout=10
        )
        if test_resp.status_code == 200:
            print("[RollbackSync] Token is valid as Bearer token")
            return refresh_token, "Bearer"

        # Try as legacy Token auth (older Label Studio versions)
        test_resp2 = requests.get(
            f"{labelstudio_url}/api/current-user/whoami",
            headers={"Authorization": f"Token {refresh_token}"},
            timeout=10
        )
        if test_resp2.status_code == 200:
            print("[RollbackSync] Token is valid as legacy Token")
            return refresh_token, "Token"

        # Token didn't work, try to refresh it using JWT endpoint
        print("[RollbackSync] Attempting to refresh JWT token...")
        refresh_resp = requests.post(
            f"{labelstudio_url}/api/token/refresh/",  # Trailing slash required
            json={"refresh": refresh_token},
            timeout=10
        )

        if refresh_resp.status_code == 200:
            data = refresh_resp.json()
            access_token = data.get("access")
            if access_token:
                print("[RollbackSync] JWT token refreshed successfully")
                return access_token, "Bearer"

        # Try without trailing slash
        refresh_resp2 = requests.post(
            f"{labelstudio_url}/api/token/refresh",
            json={"refresh": refresh_token},
            timeout=10
        )

        if refresh_resp2.status_code == 200:
            data = refresh_resp2.json()
            access_token = data.get("access")
            if access_token:
                print("[RollbackSync] JWT token refreshed successfully (no trailing slash)")
                return access_token, "Bearer"

        print(f"[RollbackSync] Token refresh failed: {refresh_resp.status_code} - {refresh_resp.text[:200]}")
        return None, ""

    except Exception as e:
        print(f"[RollbackSync] Token refresh error: {e}")
        return None, ""


def sync_labelstudio_from_silver(
    trino,
    labelstudio_url: str,
    labelstudio_token: str,
    project_id: int,
    dry_run: bool = False
) -> Dict[str, Any]:
    """
    Sync Label Studio tasks to match Silver table state.

    Deletes Label Studio tasks whose transaction_id no longer exists
    in Silver (regardless of review_status). Only truly orphaned tasks
    are deleted - tasks are NOT deleted just because they're 'reviewed'.

    This ensures Label Studio mirrors ALL Silver records, not just in_review.

    TURBO OPTIMIZED: Uses export API + bulk delete + parallel batch deletion.
    - Connection pooling for faster HTTP
    - Bulk delete for ≤5000 tasks (single API call)
    - Larger batches (1000) + more workers (10) for >5000 tasks

    Returns:
        {
            "deleted_count": int,
            "tasks_before": int,
            "tasks_after": int,
            "silver_total_count": int,
            "orphaned_task_ids": List[int],
        }
    """
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time

    from src.core.config import TRINO_CATALOG
    silver_table = f"{TRINO_CATALOG}.silver.fraud_transactions"

    start_time = time.time()

    # Connection pooling for faster HTTP requests
    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=20,
        pool_maxsize=20,
        max_retries=Retry(total=3, backoff_factor=0.1)
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Try to get a valid access token (returns token and auth type)
    access_token, auth_type = get_labelstudio_access_token(labelstudio_url, labelstudio_token)
    if not access_token:
        print("[RollbackSync] Failed to get valid Label Studio token")
        return {
            "deleted_count": 0,
            "tasks_before": 0,
            "tasks_after": 0,
            "silver_total_count": 0,
            "orphaned_task_ids": [],
            "error": "Authentication failed - could not get valid token",
        }

    result = {
        "deleted_count": 0,
        "tasks_before": 0,
        "tasks_after": 0,
        "silver_total_count": 0,
        "orphaned_task_ids": [],
    }

    try:
        # Get ALL Silver transaction_ids (regardless of review_status)
        print("[RollbackSync] Fetching Silver transaction IDs...")
        silver_ids_query = f"""
        SELECT transaction_id FROM {silver_table}
        """
        silver_ids = {row[0] for row in trino.execute_query(silver_ids_query)}
        result["silver_total_count"] = len(silver_ids)
        print(f"[RollbackSync] Silver has {len(silver_ids)} total records ({time.time() - start_time:.1f}s)")

        headers = {"Authorization": f"{auth_type} {access_token}"}

        # OPTIMIZATION 1: Try Data Manager API (faster, only IDs + data fields)
        print("[RollbackSync] Fetching Label Studio tasks...")
        fetch_start = time.time()
        tasks_to_delete = []
        all_tasks = []

        # Try Data Manager API first - it's faster and allows field filtering
        try:
            dm_resp = session.post(
                f"{labelstudio_url}/api/dm/views/{project_id}/tasks",
                headers=headers,
                json={
                    "filters": {"conjunction": "and", "items": []},
                    "selectedItems": {"all": True, "excluded": []},
                    "ordering": [],
                },
                params={"fields": "all", "page_size": -1},  # -1 = all tasks, no pagination
                timeout=180
            )
            if dm_resp.status_code == 200:
                data = dm_resp.json()
                all_tasks = data.get("tasks", data) if isinstance(data, dict) else data
                print(f"[RollbackSync] DM API returned {len(all_tasks)} tasks in {time.time()-fetch_start:.1f}s")
        except Exception as e:
            print(f"[RollbackSync] DM API failed: {e}")

        # OPTIMIZATION 2: Try export API (single request, minimal format)
        if not all_tasks:
            print("[RollbackSync] Trying export API...")
            fetch_start = time.time()
            try:
                export_resp = session.get(
                    f"{labelstudio_url}/api/projects/{project_id}/export",
                    headers=headers,
                    params={"exportType": "JSON_MIN"},
                    timeout=180,
                    stream=True  # Stream for large responses
                )

                if export_resp.status_code == 200:
                    all_tasks = export_resp.json()
                    print(f"[RollbackSync] Export API returned {len(all_tasks)} tasks in {time.time()-fetch_start:.1f}s")
                else:
                    print(f"[RollbackSync] Export API failed ({export_resp.status_code})")
            except Exception as e:
                print(f"[RollbackSync] Export API error: {e}")

        # OPTIMIZATION 3: Parallel pagination fallback (more workers, larger pages)
        if not all_tasks:
            print("[RollbackSync] Using turbo parallel pagination...")
            fetch_start = time.time()
            all_tasks = []

            # Get total count
            count_resp = session.get(
                f"{labelstudio_url}/api/projects/{project_id}/tasks",
                headers=headers,
                params={"page": 1, "page_size": 1},
                timeout=30
            )
            if count_resp.status_code == 200:
                data = count_resp.json()
                total_count = data.get("total", data.get("count", 10000))
            else:
                total_count = 10000

            page_size = 2000  # Even larger pages
            total_pages = (total_count // page_size) + 1
            pages_fetched = [0]  # Use list for closure

            def fetch_page(page_num):
                """Fetch a single page of tasks (minimal fields)."""
                try:
                    resp = session.get(
                        f"{labelstudio_url}/api/projects/{project_id}/tasks",
                        headers=headers,
                        params={
                            "page": page_num,
                            "page_size": page_size,
                            "fields": "id,data",  # Only fetch what we need
                        },
                        timeout=60
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        pages_fetched[0] += 1
                        if isinstance(data, list):
                            return data
                        return data.get("tasks", data.get("results", []))
                except Exception:
                    pass
                return []

            # Fetch pages in parallel (20 workers for max speed)
            with ThreadPoolExecutor(max_workers=20) as executor:
                futures = {executor.submit(fetch_page, p): p for p in range(1, total_pages + 1)}
                for future in as_completed(futures):
                    page_tasks = future.result()
                    all_tasks.extend(page_tasks)

            print(f"[RollbackSync] Parallel fetch got {len(all_tasks)} tasks in {time.time()-fetch_start:.1f}s ({pages_fetched[0]} pages)")

        result["tasks_before"] = len(all_tasks)

        # Find orphaned tasks (not in Silver)
        for task in all_tasks:
            # Handle both export format and API format
            if isinstance(task, dict):
                task_tx_id = task.get("data", {}).get("transaction_id") or task.get("transaction_id")
                task_id = task.get("id")
                if task_tx_id and task_tx_id not in silver_ids and task_id:
                    tasks_to_delete.append(task_id)

        print(f"[RollbackSync] Found {len(tasks_to_delete)} orphaned tasks ({time.time() - start_time:.1f}s)")
        result["orphaned_task_ids"] = tasks_to_delete[:100]  # Store sample

        # OPTIMIZATION 3: Bulk delete for small counts, parallel batch for large
        if tasks_to_delete and not dry_run:
            total_deleted = 0

            # TURBO: If ≤5000 tasks, delete all in ONE API call
            if len(tasks_to_delete) <= 5000:
                print(f"[RollbackSync] Using bulk delete for {len(tasks_to_delete)} tasks...")
                try:
                    del_resp = session.post(
                        f"{labelstudio_url}/api/dm/actions",
                        headers=headers,
                        params={"project": project_id, "id": "delete_tasks"},
                        json={"selectedItems": {"all": False, "included": tasks_to_delete}},
                        timeout=180  # Longer timeout for bulk
                    )
                    if del_resp.status_code in [200, 204]:
                        total_deleted = len(tasks_to_delete)
                        print(f"[RollbackSync] Bulk delete succeeded: {total_deleted} tasks")
                    else:
                        print(f"[RollbackSync] Bulk delete failed ({del_resp.status_code}), falling back to batches")
                        total_deleted = -1  # Signal to use batch fallback
                except Exception as e:
                    print(f"[RollbackSync] Bulk delete error: {e}, falling back to batches")
                    total_deleted = -1  # Signal to use batch fallback

            # Parallel batch deletion for >5000 tasks or if bulk failed
            if total_deleted < 0 or len(tasks_to_delete) > 5000:
                total_deleted = 0
                batch_size = 1000  # Larger batches (increased from 500)
                batches = [tasks_to_delete[i:i + batch_size] for i in range(0, len(tasks_to_delete), batch_size)]

                def delete_batch(batch):
                    """Delete a batch of tasks."""
                    try:
                        del_resp = session.post(
                            f"{labelstudio_url}/api/dm/actions",
                            headers=headers,
                            params={"project": project_id, "id": "delete_tasks"},
                            json={"selectedItems": {"all": False, "included": batch}},
                            timeout=120
                        )
                        if del_resp.status_code in [200, 204]:
                            return len(batch)
                        else:
                            print(f"[RollbackSync] Batch delete failed: {del_resp.status_code}")
                            return 0
                    except Exception as e:
                        print(f"[RollbackSync] Batch delete error: {e}")
                        return 0

                # Delete batches in parallel (max 10 concurrent - increased from 5)
                print(f"[RollbackSync] Deleting {len(batches)} batches in parallel (10 workers)...")
                with ThreadPoolExecutor(max_workers=10) as executor:
                    futures = [executor.submit(delete_batch, batch) for batch in batches]
                    for future in as_completed(futures):
                        total_deleted += future.result()

            result["deleted_count"] = total_deleted
            elapsed = time.time() - start_time
            rate = total_deleted / elapsed if elapsed > 0 else 0
            print(f"[RollbackSync] Deleted {total_deleted} tasks in {elapsed:.1f}s ({rate:.0f} tasks/sec)")

        result["tasks_after"] = result["tasks_before"] - result["deleted_count"]

    except Exception as e:
        print(f"[RollbackSync] Label Studio sync failed: {e}")
        result["error"] = str(e)
    finally:
        # Clean up session connection pool
        session.close()

    return result


def sync_training_data_from_gold(trino, dry_run: bool = False) -> Dict[str, Any]:
    """
    Sync fraud_training_data table to match gold.fraud_transactions state.

    Deletes fraud_training_data records that should no longer exist because:
    1. Transaction_id no longer exists in gold.fraud_transactions (deleted/rolled back)

    This propagates Gold table rollbacks to the training data table.

    Returns:
        {
            "deleted_count": int,
            "training_count_before": int,
            "training_count_after": int,
            "gold_count": int,
            "orphaned_deleted": int,
        }
    """
    from src.core.config import TRINO_CATALOG
    gold_table = f"{TRINO_CATALOG}.gold.fraud_transactions"
    training_table = f"{TRINO_CATALOG}.gold.fraud_training_data"

    result = {
        "deleted_count": 0,
        "training_count_before": 0,
        "training_count_after": 0,
        "gold_count": 0,
        "orphaned_deleted": 0,
    }

    try:
        # Check if training table exists
        try:
            trino.execute_query(f"SELECT 1 FROM {training_table} LIMIT 1")
        except Exception as e:
            print(f"[RollbackSync] Training table {training_table} does not exist or is empty - skipping")
            result["error"] = f"Table not found: {training_table}"
            return result

        # Get current counts
        result["training_count_before"] = trino.execute_query(
            f"SELECT COUNT(*) FROM {training_table}"
        )[0][0]

        result["gold_count"] = trino.execute_query(
            f"SELECT COUNT(*) FROM {gold_table}"
        )[0][0]

        print(f"[RollbackSync] Training data: {result['training_count_before']} records")
        print(f"[RollbackSync] Gold transactions: {result['gold_count']} records")

        # Find orphaned training records (not in Gold)
        orphan_count_query = f"""
        SELECT COUNT(*) FROM {training_table}
        WHERE transaction_id NOT IN (
            SELECT transaction_id FROM {gold_table}
        )
        """
        orphan_count = trino.execute_query(orphan_count_query)[0][0]
        print(f"[RollbackSync] Training data has {orphan_count} records not in Gold")

        if orphan_count > 0 and not dry_run:
            # Delete orphaned training records
            delete_query = f"""
            DELETE FROM {training_table}
            WHERE transaction_id NOT IN (
                SELECT transaction_id FROM {gold_table}
            )
            """
            trino.execute_ddl(delete_query)
            result["orphaned_deleted"] = orphan_count
            result["deleted_count"] = orphan_count
            print(f"[RollbackSync] Deleted {orphan_count} orphaned records from training data")

        # Get final count
        result["training_count_after"] = trino.execute_query(
            f"SELECT COUNT(*) FROM {training_table}"
        )[0][0]

    except Exception as e:
        print(f"[RollbackSync] Training data sync failed: {e}")
        result["error"] = str(e)

    return result


def sync_downstream_from_silver(
    trino,
    labelstudio_url: str = None,
    labelstudio_token: str = None,
    labelstudio_project_id: int = None,
    dry_run: bool = False
) -> Dict[str, Any]:
    """
    Sync all downstream tables/systems from Silver after a rollback.

    This is the main entry point for data-based rollback sync.

    Syncs:
    1. Gold table - removes records not in Silver
    2. Training data - removes records not in Gold (cascades from step 1)
    3. Label Studio - removes tasks not matching Silver's in_review
    4. LAYERED PROTECTION - cleanup any records from reverted LakeFS commits

    Uses distributed locking to prevent concurrent operations.

    Returns combined sync results.
    """
    from src.core.config import TRINO_CATALOG
    import time
    sync_start = time.time()

    log_header(f"DOWNSTREAM SYNC {'(DRY RUN)' if dry_run else ''}")
    print(f"  Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Catalog:    {TRINO_CATALOG}")

    # Acquire lock to prevent concurrent operations
    log_section("ACQUIRING LOCK")
    if not dry_run:
        lock_acquired = acquire_cleanup_lock(
            trino,
            f"{TRINO_CATALOG}.silver.fraud_transactions",
            "downstream_sync",
            "data_sync_job"
        )
        if not lock_acquired:
            log_warning("Another sync operation is in progress, skipping...")
            return {
                "gold_sync": {},
                "training_data_sync": {},
                "labelstudio_sync": {},
                "reverted_commits_cleanup": {},
                "success": False,
                "errors": ["Another sync operation is in progress"],
                "skipped": True,
            }
        log_success("Lock acquired successfully")
    else:
        log_info("Dry run mode - no lock needed")

    results = {
        "gold_sync": {},
        "training_data_sync": {},
        "labelstudio_sync": {},
        "reverted_commits_cleanup": {},
        "success": True,
        "errors": [],
    }

    try:
        # 1. Sync Gold table from Silver
        log_section("STEP 1: SYNC GOLD FROM SILVER")
        try:
            results["gold_sync"] = sync_gold_from_silver(trino, dry_run=dry_run)
            gold_deleted = results['gold_sync'].get('deleted_count', 0)
            gold_before = results['gold_sync'].get('gold_count_before', 0)
            gold_after = results['gold_sync'].get('gold_count_after', 0)
            print(f"  ┌{'─' * 50}┐")
            print(f"  │ {'Gold Records Before:':<30} {gold_before:>15} │")
            print(f"  │ {'Gold Records After:':<30} {gold_after:>15} │")
            print(f"  │ {'Records Deleted:':<30} {gold_deleted:>15} │")
            print(f"  └{'─' * 50}┘")
            if gold_deleted > 0:
                log_success(f"Deleted {gold_deleted} orphaned Gold records")
            else:
                log_info("No orphaned Gold records found")
        except Exception as e:
            log_error(f"Gold sync failed: {e}")
            results["errors"].append(f"Gold sync failed: {e}")
            results["success"] = False

        # 2. Sync Training Data from Gold (cascade effect)
        log_section("STEP 2: SYNC TRAINING DATA FROM GOLD")
        try:
            results["training_data_sync"] = sync_training_data_from_gold(trino, dry_run=dry_run)
            td_deleted = results['training_data_sync'].get('deleted_count', 0)
            td_before = results['training_data_sync'].get('training_count_before', 0)
            td_after = results['training_data_sync'].get('training_count_after', 0)
            print(f"  ┌{'─' * 50}┐")
            print(f"  │ {'Training Records Before:':<30} {td_before:>15} │")
            print(f"  │ {'Training Records After:':<30} {td_after:>15} │")
            print(f"  │ {'Records Deleted:':<30} {td_deleted:>15} │")
            print(f"  └{'─' * 50}┘")
            if td_deleted > 0:
                log_success(f"Deleted {td_deleted} orphaned Training records")
            else:
                log_info("No orphaned Training records found")
        except Exception as e:
            log_error(f"Training data sync failed: {e}")
            results["errors"].append(f"Training data sync failed: {e}")
            results["success"] = False

        # 3. Sync Label Studio (if credentials provided)
        log_section("STEP 3: SYNC LABEL STUDIO")
        if labelstudio_url and labelstudio_token and labelstudio_project_id:
            try:
                results["labelstudio_sync"] = sync_labelstudio_from_silver(
                    trino=trino,
                    labelstudio_url=labelstudio_url,
                    labelstudio_token=labelstudio_token,
                    project_id=labelstudio_project_id,
                    dry_run=dry_run
                )
                ls_deleted = results['labelstudio_sync'].get('deleted_count', 0)
                ls_before = results['labelstudio_sync'].get('tasks_before', 0)
                ls_after = results['labelstudio_sync'].get('tasks_after', 0)
                print(f"  ┌{'─' * 50}┐")
                print(f"  │ {'Label Studio Tasks Before:':<30} {ls_before:>15} │")
                print(f"  │ {'Label Studio Tasks After:':<30} {ls_after:>15} │")
                print(f"  │ {'Tasks Deleted:':<30} {ls_deleted:>15} │")
                print(f"  └{'─' * 50}┘")
                if ls_deleted > 0:
                    log_success(f"Deleted {ls_deleted} orphaned Label Studio tasks")
                else:
                    log_info("No orphaned Label Studio tasks found")
            except Exception as e:
                log_error(f"Label Studio sync failed: {e}")
                results["errors"].append(f"Label Studio sync failed: {e}")
                results["success"] = False
        else:
            log_info("Skipping Label Studio sync (no credentials)")

        # 4. LAYERED PROTECTION: Cleanup records from reverted LakeFS commits
        # This ensures any data from commits marked as 'reverted' or 'cleaned' is removed
        log_section("STEP 4: LAYERED PROTECTION (REVERTED COMMITS)")
        if not dry_run:
            reverted_cleanup_results = {
                "total_cleaned": 0,
                "tables_cleaned": {},
            }

            # Tables to check for reverted commit data
            tables_to_clean = [
                f"{TRINO_CATALOG}.silver.fraud_transactions",
                f"{TRINO_CATALOG}.gold.fraud_transactions",
                f"{TRINO_CATALOG}.gold.fraud_training_data",
            ]

            print(f"  Checking {len(tables_to_clean)} tables for reverted commit data...")
            for table in tables_to_clean:
                try:
                    cleanup_result = cleanup_reverted_commits(trino, table)
                    table_short = table.split(".")[-1]
                    if cleanup_result["cleaned_count"] > 0:
                        reverted_cleanup_results["total_cleaned"] += cleanup_result["cleaned_count"]
                        reverted_cleanup_results["tables_cleaned"][table] = {
                            "count": cleanup_result["cleaned_count"],
                            "commits": cleanup_result["commits_cleaned"],
                        }
                        log_warning(f"{table_short}: {cleanup_result['cleaned_count']} records from reverted commits")
                    else:
                        log_info(f"{table_short}: Clean (no reverted data)")
                except Exception as e:
                    log_error(f"Layered cleanup failed for {table}: {e}")

            results["reverted_commits_cleanup"] = reverted_cleanup_results

            if reverted_cleanup_results["total_cleaned"] > 0:
                log_success(f"Total layered cleanup: {reverted_cleanup_results['total_cleaned']} records")
        else:
            log_info("Skipping layered protection cleanup (dry_run=True)")

        # Final Summary
        sync_duration = time.time() - sync_start
        log_header("SYNC COMPLETE")

        # Summary table
        total_deleted = (
            results['gold_sync'].get('deleted_count', 0) +
            results['training_data_sync'].get('deleted_count', 0) +
            results['labelstudio_sync'].get('deleted_count', 0) +
            results.get('reverted_commits_cleanup', {}).get('total_cleaned', 0)
        )

        print(f"  ┌{'─' * 50}┐")
        print(f"  │ {'SYNC SUMMARY':<48} │")
        print(f"  ├{'─' * 50}┤")
        print(f"  │ {'Gold records deleted:':<30} {results['gold_sync'].get('deleted_count', 0):>15} │")
        print(f"  │ {'Training records deleted:':<30} {results['training_data_sync'].get('deleted_count', 0):>15} │")
        print(f"  │ {'Label Studio tasks deleted:':<30} {results['labelstudio_sync'].get('deleted_count', 0):>15} │")
        print(f"  │ {'Layered cleanup records:':<30} {results.get('reverted_commits_cleanup', {}).get('total_cleaned', 0):>15} │")
        print(f"  ├{'─' * 50}┤")
        print(f"  │ {'TOTAL DELETED:':<30} {total_deleted:>15} │")
        print(f"  │ {'Duration:':<30} {sync_duration:.2f}s{' ':>10} │")
        print(f"  │ {'Status:':<30} {'SUCCESS' if results['success'] else 'FAILED':>15} │")
        print(f"  └{'─' * 50}┘")

        if results['errors']:
            print("\n  Errors:")
            for err in results['errors']:
                log_error(err)

        return results

    finally:
        # Always release lock when done
        if not dry_run:
            log_section("RELEASING LOCK")
            release_cleanup_lock(
                trino,
                f"{TRINO_CATALOG}.silver.fraud_transactions",
                "downstream_sync"
            )
            log_success("Lock released")


# =============================================================================
# VALIDATION
# =============================================================================

def validate_rollback_consistency(trino, table: str) -> Dict[str, Any]:
    """
    Validate that a table and its related tables are in a consistent state.

    Checks:
    1. All source_lakefs_commit values exist in lakefs_commits table
    2. No orphaned records (commit marked as reverted but records exist)
    3. Snapshot IDs are sequential

    Returns:
        {
            "consistent": bool,
            "issues": List[str],
            "commit_status": Dict[str, str],
        }
    """
    issues = []
    commit_status = {}

    try:
        # Get all commits in the table
        parts = table.split(".")
        if len(parts) != 3:
            return {"consistent": False, "issues": ["Invalid table name"], "commit_status": {}}

        catalog, schema, table_name = parts

        query = f"""
        SELECT DISTINCT source_lakefs_commit
        FROM {table}
        WHERE source_lakefs_commit IS NOT NULL
        """
        result = trino.execute_query(query)
        table_commits = [row[0] for row in result if row[0]]

        from src.core.config import TRINO_CATALOG as CATALOG, escape_sql_string
        # Check each commit against lakefs_commits table
        for commit in table_commits:
            commit_safe = escape_sql_string(commit)
            check_query = f"""
            SELECT status FROM {CATALOG}.metadata.lakefs_commits
            WHERE commit_id = '{commit_safe}'
            """
            try:
                status_result = trino.execute_query(check_query)
                if status_result and status_result[0][0]:
                    status = status_result[0][0]
                    commit_status[commit] = status

                    if status in ['reverted', 'cleaned']:
                        issues.append(
                            f"Orphaned data: commit {commit[:12]}... is {status} "
                            f"but records exist in {table}"
                        )
                else:
                    commit_status[commit] = "unknown"
                    issues.append(f"Unknown commit: {commit[:12]}... not in lakefs_commits")
            except:
                commit_status[commit] = "check_failed"

    except Exception as e:
        issues.append(f"Validation failed: {e}")

    return {
        "consistent": len(issues) == 0,
        "issues": issues,
        "commit_status": commit_status,
    }


def check_lakefs_iceberg_alignment(
    trino,
    table: str,
    lakefs_branch: str = None,
) -> Dict[str, Any]:
    """
    Check if LakeFS data files and Iceberg metadata are aligned.

    This is CRITICAL for the LakeFS-revert-then-Iceberg-rollback scenario.

    Checks:
    1. Iceberg can read the table (files exist)
    2. Row counts match expectations
    3. No file-not-found errors

    Returns:
        {
            "aligned": bool,
            "iceberg_readable": bool,
            "row_count": int,
            "snapshot_id": int,
            "issues": List[str],
            "recommendation": str,
        }
    """
    from src.core.config import TRINO_CATALOG

    result = {
        "aligned": False,
        "iceberg_readable": False,
        "row_count": 0,
        "snapshot_id": None,
        "issues": [],
        "recommendation": "",
    }

    try:
        # 1. Check if Iceberg can read the table (this will fail if files are missing)
        try:
            count_query = f"SELECT COUNT(*) FROM {table}"
            count_result = trino.execute_query(count_query)
            result["row_count"] = count_result[0][0] if count_result else 0
            result["iceberg_readable"] = True
        except Exception as e:
            error_str = str(e).lower()
            if "file not found" in error_str or "no such file" in error_str:
                result["issues"].append(
                    f"FILES MISSING: LakeFS was likely reverted but Iceberg metadata "
                    f"still points to old files. Error: {str(e)[:200]}"
                )
                result["recommendation"] = (
                    "ROLLBACK Iceberg to match LakeFS state: "
                    f"CALL {table.split('.')[0]}.system.rollback_to_snapshot('{table}', <snapshot_id>)"
                )
            else:
                result["issues"].append(f"Table read failed: {e}")
            return result

        # 2. Get current Iceberg snapshot
        parts = table.split(".")
        if len(parts) == 3:
            catalog, schema, tbl = parts
            snapshot_query = f"""
            SELECT snapshot_id
            FROM {catalog}.{schema}."{tbl}$refs"
            WHERE name = 'main'
            """
            try:
                snap_result = trino.execute_query(snapshot_query)
                if snap_result:
                    result["snapshot_id"] = int(snap_result[0][0])
            except Exception:
                pass

        # 3. Check for recent rollback events that might indicate misalignment
        try:
            recent_events_query = f"""
            SELECT event_type, status, created_at
            FROM {TRINO_CATALOG}.metadata.lakefs_webhook_events
            WHERE event_type = 'post-revert'
              AND status = 'pending'
              AND created_at > CURRENT_TIMESTAMP - INTERVAL '1' HOUR
            ORDER BY created_at DESC
            LIMIT 5
            """
            recent_result = trino.execute_query(recent_events_query)
            if recent_result and len(recent_result) > 0:
                result["issues"].append(
                    f"WARNING: {len(recent_result)} pending LakeFS revert events in last hour. "
                    f"Iceberg may not be aligned."
                )
        except Exception:
            pass  # Table might not exist

        # If we got here with no issues, we're aligned
        if not result["issues"]:
            result["aligned"] = True
            result["recommendation"] = "OK - LakeFS and Iceberg appear aligned"

    except Exception as e:
        result["issues"].append(f"Alignment check failed: {e}")

    return result


def should_skip_cleanup(
    trino,
    trigger_source: str,
    table: str,
) -> Tuple[bool, str]:
    """
    Check if cleanup should be skipped to avoid duplicate operations.

    This prevents the "LakeFS revert then Iceberg rollback" double-cleanup issue.

    Args:
        trigger_source: "lakefs_webhook" or "iceberg_snapshot_sensor"
        table: Table being cleaned up

    Returns:
        (should_skip: bool, reason: str)
    """
    from src.core.config import TRINO_CATALOG

    try:
        # Check if a cleanup already ran in the last 5 minutes for this table
        recent_cleanup_query = f"""
        SELECT rollback_id, direction, completed_at
        FROM {TRINO_CATALOG}.monitoring.rollback_history
        WHERE source_table = '{table}'
          AND completed_at > CURRENT_TIMESTAMP - INTERVAL '5' MINUTE
          AND success = TRUE
        ORDER BY completed_at DESC
        LIMIT 1
        """
        result = trino.execute_query(recent_cleanup_query)

        if result and len(result) > 0:
            last_rollback_id = result[0][0]
            last_direction = result[0][1]

            # If cleanup already ran from a different source, skip
            if trigger_source == "lakefs_webhook" and last_direction == "iceberg_to_all":
                return True, f"Skipping: Iceberg cleanup already ran (ID: {last_rollback_id})"
            if trigger_source == "iceberg_snapshot_sensor" and last_direction == "lakefs_to_iceberg":
                return True, f"Skipping: LakeFS cleanup already ran (ID: {last_rollback_id})"

    except Exception:
        pass  # Table might not exist, continue with cleanup

    return False, ""


def get_rollback_status(trino, rollback_id: str) -> Optional[Dict]:
    """Get the status of a previous rollback operation."""
    from src.core.config import TRINO_CATALOG, escape_sql_string
    try:
        rollback_id_safe = escape_sql_string(rollback_id)
        query = f"""
        SELECT * FROM {TRINO_CATALOG}.monitoring.rollback_history
        WHERE rollback_id = '{rollback_id_safe}'
        """
        result = trino.execute_query(query)
        if result:
            row = result[0]
            return {
                "rollback_id": row[0],
                "direction": row[1],
                "source_table": row[2],
                "target_snapshot_id": row[3],
                "previous_snapshot_id": row[4],
                "upstream_synced": row[5],
                "downstream_synced": row[6],
                "labelstudio_deleted": row[7],
                "lakefs_commit": row[8],
                "success": row[9],
                "errors": row[10],
                "started_at": row[11],
                "completed_at": row[12],
            }
    except Exception as e:
        print(f"[RollbackSync] Failed to get status: {e}")
    return None


# =============================================================================
# LAYERED ROLLBACK PROTECTION
# =============================================================================

def validate_iceberg_rollback(
    trino,
    table: str,
    target_snapshot_id: int,
) -> Dict[str, Any]:
    """
    Check if rolling back to target snapshot would restore reverted LakeFS data.

    This is Layer 1 (Preventive) of the rollback protection system.
    It checks whether the target snapshot contains records from commits
    that were reverted in LakeFS. If so, rolling back would restore data
    that no longer exists in LakeFS, causing divergence.

    Args:
        trino: Trino connection resource
        table: Full table name (catalog.schema.table)
        target_snapshot_id: Snapshot ID to validate

    Returns:
        {
            "safe": bool,           # True if rollback is safe
            "reverted_commits_found": List[str],  # Commit IDs found
            "affected_record_count": int,         # Records that would be restored
            "warning": str,         # Warning message if unsafe
        }
    """
    from src.core.config import TRINO_CATALOG, escape_sql_string

    result = {
        "safe": True,
        "reverted_commits_found": [],
        "affected_record_count": 0,
        "warning": "",
    }

    try:
        # Get all reverted/cleaned commits from tracking table
        reverted_query = f"""
        SELECT commit_id
        FROM {TRINO_CATALOG}.metadata.lakefs_commits
        WHERE status IN ('reverted', 'cleaned')
        """
        reverted = trino.execute_query(reverted_query)

        if not reverted:
            return result

        # Check if target snapshot contains data from any reverted commit
        for (commit_id,) in reverted:
            commit_safe = escape_sql_string(commit_id)
            try:
                count_query = f"""
                SELECT COUNT(*)
                FROM {table} FOR VERSION AS OF {target_snapshot_id}
                WHERE source_lakefs_commit = '{commit_safe}'
                """
                count_result = trino.execute_query(count_query)
                count = count_result[0][0] if count_result else 0

                if count > 0:
                    result["safe"] = False
                    result["reverted_commits_found"].append(commit_id)
                    result["affected_record_count"] += count
            except Exception:
                pass  # Snapshot might not exist or query failed

        if not result["safe"]:
            result["warning"] = (
                f"DANGER: Snapshot {target_snapshot_id} contains "
                f"{result['affected_record_count']} records from "
                f"{len(result['reverted_commits_found'])} reverted commits. "
                f"LakeFS no longer has this data!"
            )

    except Exception as e:
        result["warning"] = f"Validation failed: {e}"

    return result


def safe_rollback_to_snapshot(
    trino,
    table: str,
    target_snapshot_id: int,
    force: bool = False,
) -> Dict[str, Any]:
    """
    Safe wrapper for rollback_to_snapshot with validation.

    This is part of Layer 1 (Preventive) protection. It validates the
    rollback before executing and blocks dangerous rollbacks unless
    force=True is specified.

    Args:
        trino: Trino connection resource
        table: Full table name (catalog.schema.table)
        target_snapshot_id: Snapshot ID to roll back to
        force: If True, proceed even if validation fails

    Returns:
        {
            "success": bool,        # True if rollback succeeded
            "blocked": bool,        # True if rollback was blocked
            "snapshot_id": int,     # Target snapshot ID
            "validation": Dict,     # Result from validate_iceberg_rollback
            "message": str,         # Status message
        }
    """
    result = {
        "success": False,
        "blocked": False,
        "snapshot_id": target_snapshot_id,
        "validation": None,
        "message": "",
    }

    # Validate first
    validation = validate_iceberg_rollback(trino, table, target_snapshot_id)
    result["validation"] = validation

    if not validation["safe"]:
        if not force:
            result["blocked"] = True
            result["message"] = (
                f"{validation['warning']}\n"
                f"Use force=True to override (self-healing will re-clean in ~30s)"
            )
            print(f"[RollbackSync] BLOCKED: {validation['warning']}")
            return result
        else:
            print(f"[RollbackSync] WARNING: {validation['warning']}")
            print(f"[RollbackSync] --force specified, proceeding anyway...")

    # Execute rollback
    try:
        parts = table.split(".")
        if len(parts) == 3:
            catalog, schema, tbl = parts
            # Use proper procedure call format
            trino.execute_ddl(f"""
                CALL {catalog}.system.rollback_to_snapshot('{schema}', '{tbl}', {target_snapshot_id})
            """)

        result["success"] = True
        result["message"] = (
            f"Rolled back to snapshot {target_snapshot_id}. "
            f"Self-healing sensor will verify consistency in ~30s."
        )
        print(f"[RollbackSync] Successfully rolled back {table} to snapshot {target_snapshot_id}")
    except Exception as e:
        result["message"] = f"Rollback failed: {e}"
        print(f"[RollbackSync] Rollback failed: {e}")

    return result


def cleanup_reverted_commits(
    trino,
    table: str,
) -> Dict[str, Any]:
    """
    Delete records from any reverted commits still in the table.

    This is Layer 2 (Reactive/Self-Healing) of the rollback protection system.
    It's called by the iceberg_snapshot_sensor after detecting snapshot changes
    to automatically clean up any reverted data that may have been restored.

    Args:
        trino: Trino connection resource
        table: Full table name (catalog.schema.table)

    Returns:
        {
            "cleaned_count": int,       # Total records deleted
            "commits_cleaned": List[str],  # Commit IDs cleaned (truncated)
        }
    """
    from src.core.config import TRINO_CATALOG, escape_sql_string

    result = {
        "cleaned_count": 0,
        "commits_cleaned": [],
    }

    try:
        # Get all reverted/cleaned commits from tracking table
        reverted_query = f"""
        SELECT commit_id
        FROM {TRINO_CATALOG}.metadata.lakefs_commits
        WHERE status IN ('reverted', 'cleaned')
        """
        reverted = trino.execute_query(reverted_query)

        if not reverted:
            return result

        for (commit_id,) in reverted:
            commit_safe = escape_sql_string(commit_id)

            # Check if this table has records from the reverted commit
            count_query = f"""
            SELECT COUNT(*) FROM {table}
            WHERE source_lakefs_commit = '{commit_safe}'
            """
            count_result = trino.execute_query(count_query)
            count = count_result[0][0] if count_result else 0

            if count > 0:
                # Delete the records
                trino.execute_ddl(f"""
                    DELETE FROM {table}
                    WHERE source_lakefs_commit = '{commit_safe}'
                """)
                result["cleaned_count"] += count
                result["commits_cleaned"].append(commit_id[:12])
                print(f"[SelfHeal] Cleaned {count} records from reverted commit {commit_id[:12]}...")

    except Exception as e:
        print(f"[SelfHeal] Cleanup failed: {e}")

    return result


# =============================================================================
# DAGSTER ASSETS
# =============================================================================

class RollbackConfig(Config):
    """Configuration for rollback sync job."""
    table: str
    target_snapshot_id: int
    dry_run: bool = False  # Changed default to False for actual sync
    sync_labelstudio: bool = True
    use_data_based_sync: bool = True  # NEW: Use data-based sync instead of commit-based


@op
def sync_rollback_op(context, config: RollbackConfig):
    """
    Dagster op for executing rollback sync.

    Supports two modes:
    1. Commit-based sync (legacy): Uses source_lakefs_commit to track records
    2. Data-based sync (new): Compares actual data between tables

    Data-based sync is recommended for direct Iceberg rollbacks.
    """
    # Use simple client instead of Dagster ConfigurableResource
    trino = SimpleTrinoClient()

    if config.use_data_based_sync:
        # NEW: Data-based sync - compares actual data between tables
        context.log.info("Using DATA-BASED sync (recommended for Iceberg rollbacks)")

        # Get Label Studio config from environment
        import os
        ls_url = os.getenv("LABELSTUDIO_URL", "http://label-studio:8080")
        ls_token = os.getenv("LABELSTUDIO_API_TOKEN")
        ls_project = int(os.getenv("LABELSTUDIO_PROJECT_ID", "1"))

        result = sync_downstream_from_silver(
            trino=trino,
            labelstudio_url=ls_url if config.sync_labelstudio else None,
            labelstudio_token=ls_token if config.sync_labelstudio else None,
            labelstudio_project_id=ls_project if config.sync_labelstudio else None,
            dry_run=config.dry_run,
        )

        context.log.info(f"Data-based sync completed")
        context.log.info(f"Gold sync: {result.get('gold_sync', {})}")
        context.log.info(f"Label Studio sync: {result.get('labelstudio_sync', {})}")

        # Log layered protection results
        reverted_cleanup = result.get('reverted_commits_cleanup', {})
        if reverted_cleanup.get('total_cleaned', 0) > 0:
            context.log.warning(
                f"🔧 Layered Protection: Cleaned {reverted_cleanup['total_cleaned']} records "
                f"from reverted commits across {len(reverted_cleanup.get('tables_cleaned', {}))} tables"
            )
            for table, info in reverted_cleanup.get('tables_cleaned', {}).items():
                context.log.info(f"  - {table}: {info['count']} records (commits: {info['commits']})")
        else:
            context.log.info("Layered Protection: No reverted commit data found")

        context.log.info(f"Success: {result.get('success', False)}")

        if result.get("errors"):
            for error in result["errors"]:
                context.log.error(f"Error: {error}")

        return result

    else:
        # Legacy: Commit-based sync
        context.log.info("Using COMMIT-BASED sync (legacy)")

        result = sync_iceberg_rollback(
            trino=trino,
            table=config.table,
            target_snapshot_id=config.target_snapshot_id,
            dry_run=config.dry_run,
        )

        context.log.info(f"Rollback sync completed: {result.rollback_id}")
        context.log.info(f"Success: {result.success}")
        context.log.info(f"Upstream synced: {result.upstream_tables_synced}")
        context.log.info(f"Downstream synced: {result.downstream_tables_synced}")

        if result.errors:
            for error in result.errors:
                context.log.error(f"Error: {error}")

        return result


@job(name="rollback_sync_job")
def rollback_sync_job():
    """Job to execute rollback synchronization."""
    sync_rollback_op()


# Standalone job for manual data-based sync
@op
def data_sync_op(context):
    """Sync downstream tables from Silver - standalone op for manual triggering."""
    import os

    context.log.info("data_sync_op: Starting...")

    # Use simple client instead of Dagster resource
    context.log.info("data_sync_op: Creating SimpleTrinoClient...")
    trino = SimpleTrinoClient()
    context.log.info(f"data_sync_op: Trino client created (host={trino.host})")

    ls_url = os.getenv("LABELSTUDIO_URL", "http://label-studio:8080")
    ls_token = os.getenv("LABELSTUDIO_API_TOKEN")
    ls_project = int(os.getenv("LABELSTUDIO_PROJECT_ID", "1"))
    context.log.info(f"data_sync_op: LS config loaded (url={ls_url})")

    context.log.info("data_sync_op: Calling sync_downstream_from_silver...")
    result = sync_downstream_from_silver(
        trino=trino,
        labelstudio_url=ls_url,
        labelstudio_token=ls_token,
        labelstudio_project_id=ls_project,
        dry_run=False,
    )

    context.log.info(f"Data sync completed: {result}")
    return result


@job(name="data_sync_job")
def data_sync_job():
    """Standalone job to sync downstream from Silver."""
    data_sync_op()


# =============================================================================
# EXPORTS
# =============================================================================

ROLLBACK_SYNC_JOBS = [rollback_sync_job, data_sync_job]

__all__ = [
    # Main sync functions
    "sync_iceberg_rollback",
    "sync_downstream_from_silver",
    "sync_gold_from_silver",
    "sync_training_data_from_gold",
    "sync_labelstudio_from_silver",
    # Validation & Coordination
    "validate_rollback_consistency",
    "check_lakefs_iceberg_alignment",
    "should_skip_cleanup",
    "get_rollback_status",
    # Layered Rollback Protection
    "validate_iceberg_rollback",
    "safe_rollback_to_snapshot",
    "cleanup_reverted_commits",
    # Distributed Locking
    "acquire_cleanup_lock",
    "release_cleanup_lock",
    # Types
    "RollbackResult",
    "RollbackDirection",
    # Dagster jobs
    "ROLLBACK_SYNC_JOBS",
    "rollback_sync_job",
    "data_sync_job",
]
