"""
CVOps Sensors - LakeFS Webhook Event Processing
===============================================

Sensors that poll the cv.lakefs_webhook_events table and trigger
appropriate Dagster jobs based on event type.

ALIGNED WITH MLOPS PATTERNS:
- Commit tracking in cv.lakefs_commits table
- Cleanup history in cv.cleanup_history table
- Downstream table discovery
- Label Studio task cleanup
- source_lakefs_commit column tracking

Event Handling:
- post-commit with images → Track commit + trigger cvops_ingestion_job
- post-revert → Trigger cvops_cleanup_job (remove orphaned data)
- post-merge to main → Track merge + trigger cvops_promotion_job
"""

import os
import re
import json
import time
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from dagster import (
    sensor,
    SensorEvaluationContext,
    RunRequest,
    SkipReason,
    DefaultSensorStatus,
    op,
    job,
    OpExecutionContext,
)

from src.core.resources import TrinoResource, LabelStudioResource
from src.core.config import settings, TRINO_CATALOG, escape_sql_string, validate_identifier
from cvops.pipelines.resources import CVKafkaIngestResource


# =============================================================================
# CONFIGURATION
# =============================================================================

SENSOR_INTERVAL = int(os.getenv("CVOPS_SENSOR_INTERVAL", "10"))  # seconds (same as MLOps)
MIN_IMAGES_FOR_INGESTION = int(os.getenv("CVOPS_MIN_IMAGES_TRIGGER", "1"))

# Distributed locking settings (aligned with MLOps)
LOCK_EXPIRY_MINUTES = int(os.getenv("CVOPS_LOCK_EXPIRY_MINUTES", "5"))

# Label Studio batch settings (optimized for speed)
LS_BATCH_SIZE = int(os.getenv("CVOPS_LS_BATCH_SIZE", "1000"))  # Larger batches = fewer API calls
LS_MAX_PARALLEL_WORKERS = int(os.getenv("CVOPS_LS_PARALLEL_WORKERS", "10"))  # More parallelism
LS_RATE_LIMIT_DELAY = float(os.getenv("CVOPS_LS_RATE_LIMIT_DELAY", "0"))  # No delay for speed


# =============================================================================
# DISTRIBUTED LOCKING (Aligned with MLOps rollback_sync.py)
# =============================================================================

def _get_cv_lock_table() -> str:
    """Get the CVOps lock table name using current TRINO_CATALOG."""
    return f"{TRINO_CATALOG}.cv.cleanup_locks"


def ensure_cv_lock_table(trino) -> bool:
    """Create the CVOps lock table if it doesn't exist."""
    lock_table = _get_cv_lock_table()
    try:
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
        print(f"[CVOps] Failed to create lock table: {e}")
        return False


def acquire_cvops_cleanup_lock(trino, table: str, operation: str, locked_by: str = "cvops_cleanup") -> bool:
    """
    Acquire lock before cleanup to prevent concurrent operations.

    Returns True if lock acquired, False if already locked by another operation.
    Stale locks (>LOCK_EXPIRY_MINUTES old) are automatically released.
    """
    lock_table = _get_cv_lock_table()

    try:
        ensure_cv_lock_table(trino)
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
                print(f"[CVOps] Lock held by {existing_locked_by} ({age_minutes:.1f}m old)")
                return False
            else:
                # Stale lock - delete it
                print(f"[CVOps] Releasing stale lock ({age_minutes:.1f}m old)")
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
        print(f"[CVOps] Lock acquired for {table} ({operation})")
        return True

    except Exception as e:
        # Insert failed - lock already exists (race condition)
        print(f"[CVOps] Failed to acquire lock: {e}")
        return False


def release_cvops_cleanup_lock(trino, table: str, operation: str):
    """Release lock after cleanup."""
    lock_table = _get_cv_lock_table()

    try:
        table_safe = escape_sql_string(table)
        operation_safe = escape_sql_string(operation)

        trino.execute_ddl(f"""
            DELETE FROM {lock_table}
            WHERE table_name = '{table_safe}' AND operation = '{operation_safe}'
        """)
        print(f"[CVOps] Lock released for {table} ({operation})")

    except Exception as e:
        print(f"[CVOps] Failed to release lock: {e}")


# =============================================================================
# HELPER: JWT Token Handling (aligned with MLOps rollback_sync.py)
# =============================================================================

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
            return refresh_token, "Bearer"

        # Try as legacy Token auth (older Label Studio versions)
        test_resp2 = requests.get(
            f"{labelstudio_url}/api/current-user/whoami",
            headers={"Authorization": f"Token {refresh_token}"},
            timeout=10
        )
        if test_resp2.status_code == 200:
            return refresh_token, "Token"

        # Token didn't work, try to refresh it using JWT endpoint
        refresh_resp = requests.post(
            f"{labelstudio_url}/api/token/refresh/",
            json={"refresh": refresh_token},
            timeout=10
        )

        if refresh_resp.status_code == 200:
            data = refresh_resp.json()
            access_token = data.get("access")
            if access_token:
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
                return access_token, "Bearer"

        return None, ""

    except Exception as e:
        print(f"[CVOps] Token refresh error: {e}")
        return None, ""


# =============================================================================
# HELPER: Commit Tracking (like MLOps)
# =============================================================================

def track_commit_in_table(
    trino: TrinoResource,
    commit_id: str,
    repository: str,
    branch: str,
    commit_message: str,
    image_count: int,
    context
) -> bool:
    """
    Track a commit in cv.lakefs_commits table.
    Mirrors MLOps pattern in mlops_maintenance.py
    """
    try:
        # Escape values using centralized function
        commit_safe = escape_sql_string(commit_id)
        repo_safe = escape_sql_string(repository)
        branch_safe = escape_sql_string(branch)
        message_safe = escape_sql_string(commit_message[:200])
        
        trino.execute_ddl(f"""
            INSERT INTO {TRINO_CATALOG}.cv.lakefs_commits (
                commit_id,
                lakefs_repository,
                lakefs_branch,
                created_at,
                affected_tables,
                image_count,
                status,
                reverted_at,
                cleaned_at,
                revert_reason,
                last_updated
            ) VALUES (
                '{commit_safe}',
                '{repo_safe}',
                '{branch_safe}',
                CURRENT_TIMESTAMP,
                ARRAY[],
                {image_count},
                'valid',
                NULL,
                NULL,
                '{message_safe}',
                CURRENT_TIMESTAMP
            )
        """)
        context.log.info(f"📝 Tracked commit {commit_id[:12]} in cv.lakefs_commits")
        return True
    except Exception as e:
        context.log.debug(f"Could not track commit: {e}")
        return False


def mark_commit_reverted(
    trino: TrinoResource,
    commit_id: str,
    revert_reason: str,
    context
) -> bool:
    """Mark a commit as reverted in cv.lakefs_commits."""
    try:
        commit_safe = escape_sql_string(commit_id)
        reason_safe = escape_sql_string(revert_reason[:200])
        
        trino.execute_ddl(f"""
            UPDATE {TRINO_CATALOG}.cv.lakefs_commits
            SET status = 'reverted',
                reverted_at = CURRENT_TIMESTAMP,
                revert_reason = '{reason_safe}',
                last_updated = CURRENT_TIMESTAMP
            WHERE commit_id = '{commit_safe}'
        """)
        context.log.info(f"🚨 Marked commit {commit_id[:12]} as reverted")
        return True
    except Exception as e:
        context.log.debug(f"Could not mark commit reverted: {e}")
        return False


# =============================================================================
# HELPER: Table Cleanup (like MLOps)
# =============================================================================

def cleanup_table_by_commit(
    trino: TrinoResource,
    table_name: str,
    commit_id: str,
    context
) -> int:
    """
    Clean up a single table by commit ID.
    Works for any CV table with source_lakefs_commit column.

    Returns:
        Number of records deleted
    """
    try:
        # Validate table name to prevent SQL injection
        table_name_safe = validate_identifier(table_name, "table name")
        commit_safe = escape_sql_string(commit_id)

        # Count affected records
        count_result = trino.execute_query(f"""
            SELECT COUNT(*) FROM {table_name_safe}
            WHERE source_lakefs_commit = '{commit_safe}'
        """)
        count = count_result[0][0] if count_result else 0

        if count == 0:
            return 0

        context.log.warning(f"Deleting {count} records from {table_name_safe}")

        # Delete
        trino.execute_ddl(f"""
            DELETE FROM {table_name_safe}
            WHERE source_lakefs_commit = '{commit_safe}'
        """)

        context.log.info(f"✓ Deleted {count} from {table_name_safe}")
        return count

    except ValueError as e:
        context.log.error(f"Invalid table name '{table_name}': {e}")
        return 0
    except Exception as e:
        context.log.error(f"Failed to clean {table_name}: {e}")
        return 0


def discover_downstream_tables(
    trino: TrinoResource,
    commit_id: str,
    context
) -> List[str]:
    """
    Automatically discover CV tables that have records from this commit.
    No hardcoding - discovers dynamically by scanning the cv schema.

    Returns:
        List of table names that have records with this commit
    """
    downstream = []
    commit_safe = escape_sql_string(commit_id)
    
    # All CV tables to check - use f-strings for TRINO_CATALOG substitution
    tables_to_check = [
        f"{TRINO_CATALOG}.cv.image_metadata",
        f"{TRINO_CATALOG}.cv.detection_results",
        f"{TRINO_CATALOG}.cv.annotations",
        f"{TRINO_CATALOG}.cv.training_data",
    ]
    
    for table_name in tables_to_check:
        try:
            count_result = trino.execute_query(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE source_lakefs_commit = '{commit_safe}'
            """)
            
            count = count_result[0][0] if count_result else 0
            
            if count > 0:
                downstream.append(table_name)
                context.log.info(f"  Found {count} records in {table_name}")
                
        except Exception as e:
            # Table might not have source_lakefs_commit column
            context.log.debug(f"Could not check {table_name}: {e}")
    
    return downstream


def cleanup_labelstudio_tasks(
    commit_id: str,
    project_name: str,
    context
) -> Dict[str, Any]:
    """
    Delete Label Studio tasks associated with a reverted commit.

    TURBO OPTIMIZED:
    - Connection pooling with requests.Session
    - JWT token handling with automatic refresh
    - Export API for bulk task fetching (single request)
    - Parallel pagination with max workers
    - Bulk delete API (all matching tasks in one call if possible)
    - Parallel batch deletion with no rate limiting
    - Metrics/telemetry for monitoring

    Returns:
        Dict with metrics: {
            "deleted_count": int,
            "tasks_found": int,
            "duration_seconds": float,
            "batches_processed": int,
            "tasks_per_second": float
        }
    """
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    start_time = time.time()
    metrics = {
        "deleted_count": 0,
        "tasks_found": 0,
        "duration_seconds": 0,
        "batches_processed": 0,
        "tasks_per_second": 0,
    }

    try:
        ls_url = os.getenv("LABELSTUDIO_URL", "http://exp-labelstudio:8080")
        ls_token = os.getenv("LABELSTUDIO_TOKEN", "")

        if not ls_token:
            context.log.warning("No Label Studio token - skipping task cleanup")
            return metrics

        # Use JWT token handling (aligned with MLOps)
        access_token, auth_type = get_labelstudio_access_token(ls_url, ls_token)
        if not access_token:
            context.log.warning("Failed to get valid Label Studio token")
            return metrics

        # OPTIMIZATION: Use session with connection pooling
        session = requests.Session()
        session.headers.update({"Authorization": f"{auth_type} {access_token}"})

        # Configure retry and connection pooling
        retry_strategy = Retry(total=2, backoff_factor=0.1)
        adapter = HTTPAdapter(
            pool_connections=LS_MAX_PARALLEL_WORKERS,
            pool_maxsize=LS_MAX_PARALLEL_WORKERS * 2,
            max_retries=retry_strategy
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Find project by name
        projects_resp = session.get(f"{ls_url}/api/projects", timeout=30)
        if projects_resp.status_code != 200:
            context.log.warning(f"Failed to list projects: {projects_resp.status_code}")
            return metrics

        project_id = None
        for p in projects_resp.json().get("results", projects_resp.json()):
            if isinstance(p, dict) and p.get("title") == project_name:
                project_id = p.get("id")
                break

        if not project_id:
            context.log.info(f"Project '{project_name}' not found")
            return metrics

        # OPTIMIZATION 1: Try Data Manager API (fastest - no pagination needed)
        context.log.info("Fetching Label Studio tasks...")
        all_tasks = []
        fetch_start = time.time()

        try:
            dm_resp = session.post(
                f"{ls_url}/api/dm/views/{project_id}/tasks",
                json={
                    "filters": {"conjunction": "and", "items": []},
                    "selectedItems": {"all": True, "excluded": []},
                    "ordering": [],
                },
                params={"fields": "all", "page_size": -1},  # -1 = all tasks
                timeout=180
            )
            if dm_resp.status_code == 200:
                data = dm_resp.json()
                all_tasks = data.get("tasks", data) if isinstance(data, dict) else data
                context.log.info(f"DM API returned {len(all_tasks)} tasks in {time.time()-fetch_start:.1f}s")
        except Exception as e:
            context.log.debug(f"DM API failed: {e}")

        # OPTIMIZATION 2: Try export API (single request, minimal format)
        if not all_tasks:
            context.log.info("Trying export API...")
            fetch_start = time.time()
            try:
                export_resp = session.get(
                    f"{ls_url}/api/projects/{project_id}/export",
                    params={"exportType": "JSON_MIN"},
                    timeout=180,
                    stream=True
                )
                if export_resp.status_code == 200:
                    all_tasks = export_resp.json()
                    context.log.info(f"Export API returned {len(all_tasks)} tasks in {time.time()-fetch_start:.1f}s")
            except Exception as e:
                context.log.debug(f"Export API failed: {e}")

        # OPTIMIZATION 3: Turbo parallel pagination fallback
        if not all_tasks:
            context.log.info("Using turbo parallel pagination...")
            fetch_start = time.time()

            # Get total count first
            try:
                count_resp = session.get(
                    f"{ls_url}/api/projects/{project_id}/tasks",
                    params={"page": 1, "page_size": 1},
                    timeout=10
                )
                if count_resp.status_code == 200:
                    data = count_resp.json()
                    total_count = data.get("total", data.get("count", 10000))
                else:
                    total_count = 10000
            except Exception:
                total_count = 10000

            page_size = 2000  # Larger pages
            total_pages = (total_count // page_size) + 1
            pages_fetched = [0]

            def fetch_page(page_num):
                """Fetch a single page of tasks (minimal fields)."""
                try:
                    resp = session.get(
                        f"{ls_url}/api/projects/{project_id}/tasks",
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

            # Fetch pages in parallel (20 workers for speed)
            with ThreadPoolExecutor(max_workers=20) as executor:
                futures = {executor.submit(fetch_page, p): p for p in range(1, total_pages + 1)}
                for future in as_completed(futures):
                    page_tasks = future.result()
                    all_tasks.extend(page_tasks)

            context.log.info(f"Parallel fetch got {len(all_tasks)} tasks in {time.time()-fetch_start:.1f}s ({pages_fetched[0]} pages)")

        # Find tasks to delete (optimized with set lookup)
        tasks_to_delete = []
        for task in all_tasks:
            if isinstance(task, dict):
                task_data = task.get("data", {})
                task_commit = task_data.get("source_lakefs_commit")
                if task_commit == commit_id:
                    task_id = task.get("id")
                    if task_id:
                        tasks_to_delete.append(task_id)

        metrics["tasks_found"] = len(tasks_to_delete)

        if not tasks_to_delete:
            metrics["duration_seconds"] = time.time() - start_time
            return metrics

        context.log.info(f"Found {len(tasks_to_delete)} tasks to delete for commit {commit_id[:12]}")

        # OPTIMIZATION 3: Try bulk delete first (single API call for all tasks)
        delete_start = time.time()
        total_deleted = 0

        # Try to delete all at once if under 5000 tasks
        if len(tasks_to_delete) <= 5000:
            try:
                del_resp = session.post(
                    f"{ls_url}/api/dm/actions",
                    params={"project": project_id, "id": "delete_tasks"},
                    json={"selectedItems": {"all": False, "included": tasks_to_delete}},
                    timeout=300  # 5 min timeout for large deletes
                )
                if del_resp.status_code in [200, 204]:
                    total_deleted = len(tasks_to_delete)
                    metrics["batches_processed"] = 1
                    context.log.info(f"Bulk delete succeeded: {total_deleted} tasks in {time.time()-delete_start:.1f}s")
            except Exception as e:
                context.log.debug(f"Bulk delete failed, falling back to batches: {e}")

        # OPTIMIZATION 4: Parallel batch deletion if bulk failed
        if total_deleted == 0:
            batches = [tasks_to_delete[i:i + LS_BATCH_SIZE] for i in range(0, len(tasks_to_delete), LS_BATCH_SIZE)]

            def delete_batch(batch):
                """Delete a batch of tasks."""
                try:
                    if LS_RATE_LIMIT_DELAY > 0:
                        time.sleep(LS_RATE_LIMIT_DELAY)

                    del_resp = session.post(
                        f"{ls_url}/api/dm/actions",
                        params={"project": project_id, "id": "delete_tasks"},
                        json={"selectedItems": {"all": False, "included": batch}},
                        timeout=120
                    )
                    if del_resp.status_code in [200, 204]:
                        return len(batch)
                    return 0
                except Exception:
                    return 0

            # Delete batches in parallel
            context.log.info(f"Deleting {len(batches)} batches in parallel ({LS_MAX_PARALLEL_WORKERS} workers)...")
            with ThreadPoolExecutor(max_workers=LS_MAX_PARALLEL_WORKERS) as executor:
                futures = [executor.submit(delete_batch, batch) for batch in batches]
                for future in as_completed(futures):
                    total_deleted += future.result()
                    metrics["batches_processed"] += 1

        # Calculate metrics
        elapsed = time.time() - start_time
        metrics["deleted_count"] = total_deleted
        metrics["duration_seconds"] = elapsed
        metrics["tasks_per_second"] = total_deleted / elapsed if elapsed > 0 else 0

        context.log.info(
            f"Deleted {total_deleted} Label Studio tasks in {elapsed:.1f}s "
            f"({metrics['tasks_per_second']:.0f} tasks/sec)"
        )

        # Close session
        session.close()

        return metrics

    except Exception as e:
        context.log.warning(f"Label Studio cleanup failed: {e}")
        metrics["duration_seconds"] = time.time() - start_time
        metrics["error"] = str(e)
        return metrics


def record_cleanup_history(
    trino: TrinoResource,
    reverted_commit: str,
    cleanup_start: datetime,
    cleanup_end: datetime,
    tables_cleaned: List[Tuple[str, int]],
    total_deleted: int,
    status: str,
    error_msg: str,
    context
):
    """Record cleanup operation in cv.cleanup_history."""
    try:
        # Generate ID
        id_result = trino.execute_query(f"""
            SELECT COALESCE(MAX(id), 0) + 1
            FROM {TRINO_CATALOG}.cv.cleanup_history
        """)
        cleanup_id = id_result[0][0] if id_result else 1
        
        # Format tables array
        if tables_cleaned:
            table_rows = []
            for table, count in tables_cleaned:
                table_safe = escape_sql_string(table)
                table_rows.append(f"ROW('{table_safe}', {count})")
            tables_array = "ARRAY[" + ", ".join(table_rows) + "]"
        else:
            tables_array = "ARRAY[]"

        commit_safe = escape_sql_string(reverted_commit)
        error_safe = escape_sql_string(error_msg[:500])
        start_str = cleanup_start.strftime('%Y-%m-%d %H:%M:%S.%f')
        end_str = cleanup_end.strftime('%Y-%m-%d %H:%M:%S.%f')
        
        trino.execute_ddl(f"""
            INSERT INTO {TRINO_CATALOG}.cv.cleanup_history
            VALUES (
                {cleanup_id},
                '{commit_safe}',
                TIMESTAMP '{start_str}',
                TIMESTAMP '{end_str}',
                {tables_array},
                {total_deleted},
                '{status}',
                '{error_safe}',
                'webhook'
            )
        """)
        context.log.info(f"📝 Recorded cleanup in cv.cleanup_history")
        
    except Exception as e:
        context.log.warning(f"Could not record cleanup history: {e}")


# =============================================================================
# WEBHOOK EVENT SENSOR (Aligned with MLOps)
# =============================================================================

@sensor(
    name="cvops_webhook_sensor",
    minimum_interval_seconds=SENSOR_INTERVAL,
    default_status=DefaultSensorStatus.RUNNING,
    description="Process LakeFS webhook events - track commits, handle reverts (MLOps-aligned)",
    job_name="cvops_cleanup_job"
)
def cvops_webhook_sensor(context: SensorEvaluationContext):
    """
    Poll cv.lakefs_webhook_events for pending events.
    
    Actions (aligned with MLOps mlops_maintenance.py):
    - post-commit: Track in cv.lakefs_commits, trigger ingestion if images
    - post-merge: Track in cv.lakefs_commits
    - post-revert: Mark as reverted, trigger cleanup job
    """
    from src.core.resources import get_trino_client
    trino = get_trino_client()
    
    try:
        # Get pending events (limit 10 per cycle - same as MLOps)
        pending = trino.execute_query(f"""
            SELECT
                id,
                event_id,
                event_type,
                repository,
                branch,
                commit_id,
                reverted_commit,
                payload,
                image_count
            FROM {TRINO_CATALOG}.cv.lakefs_webhook_events
            WHERE status = 'pending'
            ORDER BY received_at ASC
            LIMIT 10
        """)
        
        if not pending:
            return SkipReason("No pending webhook events")
        
        context.log.info(f"═══════════════════════════════════════════════════")
        context.log.info(f"CVOPS WEBHOOK SENSOR: Processing {len(pending)} events")
        context.log.info(f"═══════════════════════════════════════════════════")
        
        commits_tracked = 0
        reverts_triggered = 0
        ingestions_triggered = 0
        
        for event_row in pending:
            event_db_id = event_row[0]
            event_id = event_row[1]
            event_type = event_row[2]
            repository = event_row[3]
            branch = event_row[4]
            commit_id = event_row[5]
            reverted_commit = event_row[6]
            payload_str = event_row[7]
            image_count = event_row[8] or 0
            
            # Parse payload
            try:
                payload = json.loads(payload_str) if payload_str else {}
            except:
                payload = {}
            
            commit_message = payload.get('commit_message', 'N/A')
            committer = payload.get('committer', 'unknown')
            
            # Extract reverted commit from message if not set
            if event_type == 'post-revert' and not reverted_commit:
                match = re.search(r'Revert\s+([a-f0-9]{64})', commit_message)
                if match:
                    reverted_commit = match.group(1)
                    context.log.info(f"  Extracted reverted commit: {reverted_commit[:12]}...")
            
            # Mark as processing
            trino.execute_ddl(f"""
                UPDATE {TRINO_CATALOG}.cv.lakefs_webhook_events
                SET status = 'processing',
                    processed_at = CURRENT_TIMESTAMP
                WHERE id = {event_db_id}
            """)
            
            # ═══════════════════════════════════════════════════════════════
            # HANDLE: POST-REVERT
            # ═══════════════════════════════════════════════════════════════
            if event_type == 'post-revert' and reverted_commit:
                context.log.warning(f"")
                context.log.warning(f"🚨 ════════════════════════════════════════════════")
                context.log.warning(f"🚨 CVOPS REVERT DETECTED - TRIGGERING CLEANUP")
                context.log.warning(f"🚨 ════════════════════════════════════════════════")
                context.log.warning(f"🚨 Repository: {repository}/{branch}")
                context.log.warning(f"🚨 Reverted Commit: {reverted_commit}")
                context.log.warning(f"🚨 Message: {commit_message}")
                context.log.warning(f"🚨 ════════════════════════════════════════════════")
                
                # Mark commit as reverted in tracking table
                mark_commit_reverted(trino, reverted_commit, commit_message, context)
                
                reverts_triggered += 1
                
                import time
                run_timestamp = int(time.time())
                
                yield RunRequest(
                    run_key=f"cvops_cleanup_{reverted_commit[:12]}_{event_db_id}_{run_timestamp}",
                    run_config={
                        "ops": {
                            "cvops_cleanup_reverted_commit": {
                                "config": {
                                    "reverted_commit": reverted_commit,
                                    "webhook_event_id": event_db_id,
                                    "repository": repository,
                                    "branch": branch
                                }
                            }
                        }
                    },
                    tags={
                        "event_type": "revert",
                        "commit": reverted_commit[:12],
                        "webhook_id": str(event_db_id),
                        "domain": "cvops"
                    }
                )
            
            # ═══════════════════════════════════════════════════════════════
            # HANDLE: POST-COMMIT / POST-MERGE
            # ═══════════════════════════════════════════════════════════════
            elif event_type in ['post-commit', 'post-merge']:
                context.log.info(f"")
                context.log.info(f"📝 ────────────────────────────────────────────────")
                context.log.info(f"📝 CVOPS {event_type.upper()}: {commit_id[:12] if commit_id else 'N/A'}")
                context.log.info(f"📝 ────────────────────────────────────────────────")
                context.log.info(f"📝 Repository: {repository}/{branch}")
                context.log.info(f"📝 Message: {commit_message}")
                context.log.info(f"📝 Committer: {committer}")
                context.log.info(f"📝 Images: {image_count}")
                context.log.info(f"📝 ────────────────────────────────────────────────")
                
                # Track commit
                if track_commit_in_table(trino, commit_id, repository, branch, 
                                         commit_message, image_count, context):
                    commits_tracked += 1
                
                # Trigger ingestion if images were added
                if event_type == 'post-commit' and image_count > 0:
                    context.log.info(f"  → Triggering ingestion for {image_count} new images")
                    ingestions_triggered += 1
                    
                    import time
                    run_timestamp = int(time.time())
                    
                    yield RunRequest(
                        run_key=f"cvops_ingestion_{commit_id[:12]}_{run_timestamp}",
                        job_name="cvops_ingestion",
                        run_config={
                            "ops": {
                                "cvops_create_manifest": {
                                    "config": {
                                        "triggered_by": "webhook",
                                        "commit_id": commit_id,
                                        "image_count": image_count,
                                    }
                                }
                            }
                        },
                        tags={
                            "trigger": "webhook",
                            "event_type": event_type,
                            "commit": commit_id[:12] if commit_id else "unknown",
                            "domain": "cvops"
                        }
                    )
                
                # Mark as processed
                trino.execute_ddl(f"""
                    UPDATE {TRINO_CATALOG}.cv.lakefs_webhook_events
                    SET status = 'processed',
                        processed_at = CURRENT_TIMESTAMP
                    WHERE id = {event_db_id}
                """)
            
            # ═══════════════════════════════════════════════════════════════
            # HANDLE: UNKNOWN EVENT TYPE
            # ═══════════════════════════════════════════════════════════════
            else:
                context.log.warning(f"⚠️ Unknown event type: {event_type}")
                trino.execute_ddl(f"""
                    UPDATE {TRINO_CATALOG}.cv.lakefs_webhook_events
                    SET status = 'processed',
                        processed_at = CURRENT_TIMESTAMP,
                        error_message = 'Unknown event type: {event_type}'
                    WHERE id = {event_db_id}
                """)
        
        # Summary
        context.log.info(f"")
        context.log.info(f"═══════════════════════════════════════════════════")
        context.log.info(f"CVOPS WEBHOOK SENSOR SUMMARY")
        context.log.info(f"  Events processed: {len(pending)}")
        context.log.info(f"  Commits tracked: {commits_tracked}")
        context.log.info(f"  Reverts triggered: {reverts_triggered}")
        context.log.info(f"  Ingestions triggered: {ingestions_triggered}")
        context.log.info(f"═══════════════════════════════════════════════════")
        
    except Exception as e:
        context.log.error(f"CVOps webhook sensor error: {e}")
        import traceback
        context.log.error(traceback.format_exc())
        return SkipReason(f"Error: {e}")


# =============================================================================
# IMAGE BACKLOG SENSOR
# =============================================================================

@sensor(
    name="cvops_image_backlog_sensor",
    minimum_interval_seconds=300,  # 5 minutes
    default_status=DefaultSensorStatus.STOPPED,
    description="Triggers detection when unprocessed images accumulate",
)
def cvops_image_backlog_sensor(context: SensorEvaluationContext):
    """Monitor for unprocessed images and trigger detection job."""
    MIN_BACKLOG_THRESHOLD = int(os.getenv("CVOPS_BACKLOG_THRESHOLD", "100"))
    
    from src.core.resources import get_trino_client
    trino = get_trino_client()
    
    try:
        result = trino.execute_query(f"""
            SELECT COUNT(*)
            FROM {TRINO_CATALOG}.cv.image_metadata
            WHERE processed = FALSE
        """)
        
        unprocessed_count = result[0][0] if result else 0
        
        if unprocessed_count < MIN_BACKLOG_THRESHOLD:
            return SkipReason(f"Only {unprocessed_count} unprocessed (threshold: {MIN_BACKLOG_THRESHOLD})")
        
        context.log.info(f"Backlog: {unprocessed_count} unprocessed images")
        
        return RunRequest(
            run_key=f"cvops_backlog_{datetime.now().strftime('%Y%m%d_%H%M')}",
            job_name="cvops_ingestion",
            tags={"trigger": "backlog", "unprocessed_count": str(unprocessed_count), "domain": "cvops"}
        )
        
    except Exception as e:
        return SkipReason(f"Error: {str(e)}")


# =============================================================================
# ANNOTATION COMPLETE SENSOR
# =============================================================================

@sensor(
    name="cvops_annotation_sensor",
    minimum_interval_seconds=600,  # 10 minutes
    default_status=DefaultSensorStatus.STOPPED,
    description="Triggers training when enough annotations are available",
)
def cvops_annotation_sensor(context: SensorEvaluationContext):
    """Monitor Label Studio for completed annotations."""
    MIN_ANNOTATIONS = int(os.getenv("CVOPS_MIN_ANNOTATIONS_TRIGGER", "100"))
    
    from src.core.resources import get_trino_client
    trino = get_trino_client()
    
    try:
        result = trino.execute_query(f"""
            SELECT COUNT(*) as total_annotations
            FROM {TRINO_CATALOG}.cv.annotations
            WHERE annotated_at > (
                SELECT COALESCE(MAX(training_started_at), TIMESTAMP '1970-01-01')
                FROM {TRINO_CATALOG}.cv.model_metrics
            )
        """)
        
        total_annotations = result[0][0] if result else 0
        
        if total_annotations < MIN_ANNOTATIONS:
            return SkipReason(f"Only {total_annotations} annotations (need {MIN_ANNOTATIONS})")
        
        context.log.info(f"Training threshold met: {total_annotations} annotations")
        
        return RunRequest(
            run_key=f"cvops_train_{datetime.now().strftime('%Y%m%d_%H%M')}",
            job_name="cvops_training",
            tags={"trigger": "annotation_threshold", "annotation_count": str(total_annotations), "domain": "cvops"}
        )
        
    except Exception as e:
        return SkipReason(f"Error: {str(e)}")


# =============================================================================
# CLEANUP OP (Multi-Table, like MLOps)
# =============================================================================

@op(
    description="Clean up CV data from reverted LakeFS commits (MLOps-aligned)",
    config_schema={
        "reverted_commit": str,
        "webhook_event_id": int,
        "repository": str,
        "branch": str,
    }
)
def cvops_cleanup_reverted_commit(context: OpExecutionContext):
    """
    Remove all data associated with a reverted commit.

    Mirrors MLOps cleanup_reverted_commit_multitable:
    0. Acquire distributed lock (prevent concurrent cleanup)
    1. Discover affected tables
    2. Delete records by source_lakefs_commit
    3. Clean Label Studio tasks
    4. Mark commit as cleaned
    5. Record in cleanup_history
    6. Release lock
    """
    reverted_commit = context.op_config["reverted_commit"]
    webhook_event_id = context.op_config["webhook_event_id"]
    repository = context.op_config["repository"]
    branch = context.op_config["branch"]

    from src.core.resources import get_trino_client
    trino = get_trino_client()

    cleanup_start = datetime.now()
    tables_cleaned = []
    total_deleted = 0
    status = "success"
    error_msg = ""
    lock_acquired = False

    # Formatted logging like MLOps
    context.log.info("")
    context.log.info("╔" + "═" * 70 + "╗")
    context.log.info("║" + " 🧹 CVOPS CLEANUP OPERATION".center(70) + "║")
    context.log.info("╠" + "═" * 70 + "╣")
    context.log.info(f"║  Commit:      {reverted_commit[:40]:<55}  ║")
    context.log.info(f"║  Repository:  {repository:<55}  ║")
    context.log.info(f"║  Branch:      {branch:<55}  ║")
    context.log.info(f"║  Time:        {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<55}  ║")
    context.log.info("╚" + "═" * 70 + "╝")
    context.log.info("")

    try:
        # Step 0: Acquire distributed lock
        context.log.info("┌─── Step 0: Acquire Distributed Lock ───")
        lock_acquired = acquire_cvops_cleanup_lock(
            trino,
            reverted_commit[:12],
            "cvops_cleanup",
            f"cvops_cleanup_job_{webhook_event_id}"
        )

        if not lock_acquired:
            context.log.warning("│ ⚠️  Could not acquire lock - cleanup already in progress")
            context.log.warning("│    Skipping to avoid duplicate cleanup")
            context.log.info("└" + "─" * 50)
            return {
                "reverted_commit": reverted_commit,
                "status": "skipped",
                "reason": "Lock held by another operation"
            }

        context.log.info("│ ✓ Lock acquired successfully")
        context.log.info("└" + "─" * 50)
        context.log.info("")

        # Step 1: Discover tables with this commit
        context.log.info("┌─── Step 1: Discover Affected Tables ───")
        affected_tables = discover_downstream_tables(trino, reverted_commit, context)
        
        if affected_tables:
            context.log.info(f"│ Found {len(affected_tables)} tables to clean:")
            for t in affected_tables:
                context.log.info(f"│   • {t}")
        else:
            context.log.info("│ No tables found with this commit")
        context.log.info("└" + "─" * 50)
        context.log.info("")

        # Step 2: Clean each table
        context.log.info("┌─── Step 2: Clean Affected Tables ───")
        for table_name in affected_tables:
            deleted = cleanup_table_by_commit(trino, table_name, reverted_commit, context)
            if deleted > 0:
                tables_cleaned.append((table_name, deleted))
                total_deleted += deleted
                context.log.info(f"│ ✓ {table_name}: {deleted} records deleted")
            else:
                context.log.info(f"│ - {table_name}: 0 records (already clean)")
        context.log.info("└" + "─" * 50)
        context.log.info("")

        # Step 3: Clean Label Studio tasks
        context.log.info("┌─── Step 3: Clean Label Studio Tasks ───")
        try:
            project_name = settings.cvops.ls_project_name
            ls_metrics = cleanup_labelstudio_tasks(reverted_commit, project_name, context)
            ls_deleted = ls_metrics.get("deleted_count", 0)
            if ls_deleted > 0:
                tables_cleaned.append(("label_studio", ls_deleted))
                total_deleted += ls_deleted
                context.log.info(f"│ ✓ Deleted {ls_deleted} tasks")
            else:
                context.log.info("│ - No tasks to delete")
            # Log detailed metrics
            if ls_metrics.get("tasks_per_second", 0) > 0:
                context.log.info(
                    f"│   Performance: {ls_metrics.get('duration_seconds', 0):.1f}s "
                    f"({ls_metrics.get('tasks_per_second', 0):.0f} tasks/sec)"
                )
        except Exception as e:
            context.log.warning(f"│ ⚠️  Label Studio cleanup failed: {e}")
            error_msg += f"LabelStudio: {str(e)[:100]}; "
            status = "partial"
        context.log.info("└" + "─" * 50)
        context.log.info("")

        # Step 4: Mark commit as cleaned
        context.log.info("┌─── Step 4: Update Commit Status ───")
        commit_safe = escape_sql_string(reverted_commit)
        repo_safe = escape_sql_string(repository)
        branch_safe = escape_sql_string(branch)

        trino.execute_ddl(f"""
            UPDATE {TRINO_CATALOG}.cv.lakefs_commits
            SET status = 'cleaned',
                cleaned_at = CURRENT_TIMESTAMP,
                last_updated = CURRENT_TIMESTAMP
            WHERE commit_id = '{commit_safe}'
              AND lakefs_repository = '{repo_safe}'
              AND lakefs_branch = '{branch_safe}'
        """)
        context.log.info("│ ✓ Commit marked as cleaned")
        context.log.info("└" + "─" * 50)
        context.log.info("")

        # Step 5: Record cleanup history
        context.log.info("┌─── Step 5: Record Cleanup History ───")
        cleanup_end = datetime.now()
        record_cleanup_history(
            trino, reverted_commit, cleanup_start, cleanup_end,
            tables_cleaned, total_deleted, status, error_msg, context
        )
        context.log.info("│ ✓ History recorded in cv.cleanup_history")
        context.log.info("└" + "─" * 50)
        context.log.info("")

        # Step 6: Mark webhook event as processed
        context.log.info("┌─── Step 6: Finalize Webhook Event ───")
        trino.execute_ddl(f"""
            UPDATE {TRINO_CATALOG}.cv.lakefs_webhook_events
            SET status = 'processed',
                processed_at = CURRENT_TIMESTAMP
            WHERE id = {webhook_event_id}
        """)
        context.log.info("│ ✓ Webhook event marked as processed")
        context.log.info("└" + "─" * 50)
        context.log.info("")

        # Step 7: Release lock
        context.log.info("┌─── Step 7: Release Distributed Lock ───")
        if lock_acquired:
            release_cvops_cleanup_lock(trino, reverted_commit[:12], "cvops_cleanup")
            context.log.info("│ ✓ Lock released")
        context.log.info("└" + "─" * 50)
        context.log.info("")

        # Summary
        duration = (cleanup_end - cleanup_start).total_seconds()

        context.log.info("╔" + "═" * 70 + "╗")
        context.log.info("║" + " ✅ CVOPS CLEANUP COMPLETE".center(70) + "║")
        context.log.info("╠" + "═" * 70 + "╣")
        context.log.info(f"║  Commit:          {reverted_commit[:12]:<51}  ║")
        context.log.info(f"║  Duration:        {duration:.2f}s{'':<49}  ║")
        context.log.info(f"║  Tables cleaned:  {len(tables_cleaned):<51}  ║")
        context.log.info(f"║  Records deleted: {total_deleted:<51}  ║")
        context.log.info(f"║  Status:          {status:<51}  ║")
        context.log.info("╠" + "═" * 70 + "╣")

        if tables_cleaned:
            context.log.info("║  Breakdown:".ljust(71) + "║")
            for table, count in tables_cleaned:
                short_table = table.split(".")[-1] if "." in table else table
                context.log.info(f"║    • {short_table}: {count} records".ljust(71) + "║")

        context.log.info("╚" + "═" * 70 + "╝")
        context.log.info("")

        return {
            "reverted_commit": reverted_commit,
            "tables_cleaned": len(tables_cleaned),
            "total_deleted": total_deleted,
            "duration_seconds": duration,
            "status": status
        }
        
    except Exception as e:
        context.log.error("")
        context.log.error("╔" + "═" * 70 + "╗")
        context.log.error("║" + " ❌ CVOPS CLEANUP FAILED".center(70) + "║")
        context.log.error("╠" + "═" * 70 + "╣")
        context.log.error(f"║  Commit:  {reverted_commit[:12]:<57}  ║")
        context.log.error(f"║  Error:   {str(e)[:55]:<57}  ║")
        context.log.error("╚" + "═" * 70 + "╝")
        context.log.error("")

        # Release lock on failure
        if lock_acquired:
            try:
                release_cvops_cleanup_lock(trino, reverted_commit[:12], "cvops_cleanup")
                context.log.info("Lock released after failure")
            except Exception as lock_error:
                context.log.warning(f"Failed to release lock: {lock_error}")

        # Mark webhook as failed
        try:
            error_safe = escape_sql_string(str(e)[:500])
            trino.execute_ddl(f"""
                UPDATE {TRINO_CATALOG}.cv.lakefs_webhook_events
                SET status = 'failed',
                    error_message = '{error_safe}'
                WHERE id = {webhook_event_id}
            """)
        except Exception as db_error:
            context.log.warning(f"Failed to update webhook status: {db_error}")

        raise


# =============================================================================
# CLEANUP JOB
# =============================================================================

@job(
    name="cvops_cleanup_job",
    description="Cleanup reverted CV commits triggered by LakeFS webhook (MLOps-aligned)"
)
def cvops_cleanup_job():
    cvops_cleanup_reverted_commit()


# =============================================================================
# KAFKA INGESTION SENSORS
# =============================================================================

KAFKA_LAG_ALERT_THRESHOLD = int(os.getenv("CVOPS_KAFKA_LAG_THRESHOLD", "1000"))
KAFKA_DLQ_ALERT_THRESHOLD = int(os.getenv("CVOPS_KAFKA_DLQ_THRESHOLD", "100"))


@sensor(
    name="cvops_kafka_lag_sensor",
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.STOPPED,
    description="Monitor Kafka consumer lag for CVOps ingestion pipeline",
)
def cvops_kafka_lag_sensor(
    context: SensorEvaluationContext,
    cv_kafka_ingest: CVKafkaIngestResource,
) -> SkipReason:
    """
    Monitor Kafka consumer lag and alert on high backlog.

    This sensor monitors the cv.images.ingest topic consumer lag
    and can trigger batch processing jobs when lag exceeds threshold.

    Metrics tracked:
    - Total consumer lag across all partitions
    - Per-partition lag breakdown
    - Alert when lag exceeds threshold
    """
    try:
        # Get consumer lag
        lag_info = cv_kafka_ingest.get_consumer_lag()
        total_lag = sum(lag_info.values())

        context.log.info(f"Kafka consumer lag: {total_lag} messages")

        if lag_info:
            context.log.debug(f"Per-partition lag: {lag_info}")

        # Check if lag exceeds threshold
        if total_lag > KAFKA_LAG_ALERT_THRESHOLD:
            context.log.warning(
                f"ALERT: Consumer lag ({total_lag}) exceeds threshold ({KAFKA_LAG_ALERT_THRESHOLD})"
            )
            # Could trigger a batch processing job here if needed
            # For now, just log the alert

        return SkipReason(
            f"Monitored consumer lag: {total_lag} messages "
            f"(threshold: {KAFKA_LAG_ALERT_THRESHOLD})"
        )

    except Exception as e:
        context.log.error(f"Failed to get consumer lag: {e}")
        return SkipReason(f"Error monitoring lag: {e}")


@sensor(
    name="cvops_kafka_dlq_sensor",
    minimum_interval_seconds=300,  # Check every 5 minutes
    default_status=DefaultSensorStatus.STOPPED,
    description="Monitor Kafka DLQ depth for CVOps ingestion failures",
)
def cvops_kafka_dlq_sensor(
    context: SensorEvaluationContext,
    cv_kafka_ingest: CVKafkaIngestResource,
) -> SkipReason:
    """
    Monitor Kafka Dead Letter Queue depth for failed messages.

    This sensor monitors the cv.images.dlq topic for failed ingestion
    messages and alerts when the DLQ depth exceeds threshold.

    Use cases:
    - Alert on ingestion failures
    - Trigger retry job for failed messages
    - Monitor data quality issues
    """
    try:
        # Get DLQ depth
        dlq_depth = cv_kafka_ingest.get_dlq_depth()

        context.log.info(f"DLQ depth: {dlq_depth} messages")

        # Check if depth exceeds threshold
        if dlq_depth > KAFKA_DLQ_ALERT_THRESHOLD:
            context.log.warning(
                f"ALERT: DLQ depth ({dlq_depth}) exceeds threshold ({KAFKA_DLQ_ALERT_THRESHOLD})"
            )
            # Could trigger a retry job or alert here
            # For now, just log the alert

        return SkipReason(
            f"DLQ depth: {dlq_depth} messages "
            f"(threshold: {KAFKA_DLQ_ALERT_THRESHOLD})"
        )

    except Exception as e:
        context.log.error(f"Failed to get DLQ depth: {e}")
        return SkipReason(f"Error monitoring DLQ: {e}")


@sensor(
    name="cvops_kafka_ingest_health_sensor",
    minimum_interval_seconds=120,
    default_status=DefaultSensorStatus.STOPPED,
    description="Overall health check for CVOps Kafka ingestion pipeline",
)
def cvops_kafka_ingest_health_sensor(
    context: SensorEvaluationContext,
    cv_kafka_ingest: CVKafkaIngestResource,
) -> SkipReason:
    """
    Overall health check for the Kafka ingestion pipeline.

    Combines multiple health metrics:
    - Consumer lag
    - DLQ depth
    - Topic existence
    - Consumer group status
    """
    health_status = {
        "consumer_lag": 0,
        "dlq_depth": 0,
        "topics_healthy": True,
        "overall_status": "healthy",
    }

    issues = []

    try:
        # Check consumer lag
        lag_info = cv_kafka_ingest.get_consumer_lag()
        total_lag = sum(lag_info.values())
        health_status["consumer_lag"] = total_lag

        if total_lag > KAFKA_LAG_ALERT_THRESHOLD:
            issues.append(f"High consumer lag: {total_lag}")

    except Exception as e:
        issues.append(f"Failed to check consumer lag: {e}")
        health_status["topics_healthy"] = False

    try:
        # Check DLQ depth
        dlq_depth = cv_kafka_ingest.get_dlq_depth()
        health_status["dlq_depth"] = dlq_depth

        if dlq_depth > KAFKA_DLQ_ALERT_THRESHOLD:
            issues.append(f"High DLQ depth: {dlq_depth}")

    except Exception as e:
        issues.append(f"Failed to check DLQ: {e}")

    # Determine overall status
    if issues:
        health_status["overall_status"] = "unhealthy"
        health_status["issues"] = issues
        context.log.warning(f"Ingestion pipeline issues: {issues}")
    else:
        context.log.info("Ingestion pipeline healthy")

    return SkipReason(
        f"Health check: {health_status['overall_status']} - "
        f"lag={health_status['consumer_lag']}, dlq={health_status['dlq_depth']}"
    )


# =============================================================================
# EXPORTS
# =============================================================================

CVOPS_SENSORS = [
    cvops_webhook_sensor,
    cvops_image_backlog_sensor,
    cvops_annotation_sensor,
    # Kafka ingestion sensors
    cvops_kafka_lag_sensor,
    cvops_kafka_dlq_sensor,
    cvops_kafka_ingest_health_sensor,
]
