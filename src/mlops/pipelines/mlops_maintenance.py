"""
MLOps Maintenance Pipeline - Multi-Table LakeFS Commit Sync
Handles webhook events and cleanup for multiple tables automatically
NO FALLBACK - Webhook-only approach for simplicity
"""

import os
import requests
from datetime import datetime
from dagster import (
    op,
    job,
    sensor,
    OpExecutionContext,
    SensorEvaluationContext,
    RunRequest,
    SkipReason,
)
from src.core.resources import (
    TrinoResource,
)
from src.core.config import TRINO_CATALOG, escape_sql_string, validate_identifier


# ═════════════════════════════════════════════════════════════════════════════
# SENSOR: Process Webhook Events
# ═════════════════════════════════════════════════════════════════════════════

@sensor(
    name="lakefs_webhook_sensor",
    minimum_interval_seconds=10,
    description="Process LakeFS webhook events - track commits, handle reverts",
    job_name="lakefs_webhook_cleanup_job"  # Target job for RunRequests
)
def lakefs_webhook_sensor(context: SensorEvaluationContext):
    """
    Poll webhook events table every 10 seconds.
    
    Actions:
    - post-commit: Log commit details, track in lakefs_commits table
    - post-merge: Log merge details, track in lakefs_commits table  
    - post-revert: Trigger cleanup job to propagate revert across tables
    """
    import json
    from src.core.resources import get_trino_client
    trino = get_trino_client()
    
    try:
        # Get pending events (limit 10 per cycle)
        pending = trino.execute_query(f"""
            SELECT
                id,
                event_id,
                event_type,
                repository,
                branch,
                commit_id,
                reverted_commit_id,
                payload
            FROM {TRINO_CATALOG}.metadata.lakefs_webhook_events
            WHERE status = 'pending'
            ORDER BY received_at ASC
            LIMIT 10
        """)
        
        if not pending:
            return SkipReason("No pending webhook events")
        
        context.log.info(f"═══════════════════════════════════════════════════")
        context.log.info(f"WEBHOOK SENSOR: Processing {len(pending)} events")
        context.log.info(f"═══════════════════════════════════════════════════")
        
        commits_tracked = 0
        reverts_triggered = 0
        
        for event_row in pending:
            event_db_id = event_row[0]
            event_id = event_row[1]
            event_type = event_row[2]
            repository = event_row[3]
            branch = event_row[4]
            commit_id = event_row[5]
            reverted_commit = event_row[6]
            payload_str = event_row[7]
            
            # Parse payload for additional details
            try:
                payload = json.loads(payload_str) if payload_str else {}
            except:
                payload = {}
            
            commit_message = payload.get('commit_message', 'N/A')
            committer = payload.get('committer', 'unknown')
            commit_metadata = payload.get('commit_metadata', {})
            source = commit_metadata.get('source', 'unknown')
            
            # ═══════════════════════════════════════════════════════════════
            # FALLBACK: Extract reverted commit from commit_message
            # LakeFS format: "Revert <64-char-commit-id>"
            # ═══════════════════════════════════════════════════════════════
            if event_type == 'post-revert' and not reverted_commit:
                import re
                match = re.search(r'Revert\s+([a-f0-9]{64})', commit_message)
                if match:
                    reverted_commit = match.group(1)
                    context.log.info(f"  Extracted reverted commit from message: {reverted_commit[:12]}...")
            
            # Mark as processing
            trino.execute_ddl(f"""
                UPDATE {TRINO_CATALOG}.metadata.lakefs_webhook_events
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
                context.log.warning(f"🚨 REVERT DETECTED - TRIGGERING CLEANUP")
                context.log.warning(f"🚨 ════════════════════════════════════════════════")
                context.log.warning(f"🚨 Repository: {repository}/{branch}")
                context.log.warning(f"🚨 Reverted Commit: {reverted_commit}")
                context.log.warning(f"🚨 Message: {commit_message}")
                context.log.warning(f"🚨 Committer: {committer}")
                context.log.warning(f"🚨 ════════════════════════════════════════════════")
                
                # Track revert in lakefs_commits table
                try:
                    # Update existing commit record to 'reverted' status
                    reverted_commit_safe = escape_sql_string(reverted_commit)
                    commit_msg_safe = escape_sql_string(commit_message[:200])
                    trino.execute_ddl(f"""
                        UPDATE {TRINO_CATALOG}.metadata.lakefs_commits
                        SET status = 'reverted',
                            reverted_at = CURRENT_TIMESTAMP,
                            revert_reason = '{commit_msg_safe}',
                            last_updated = CURRENT_TIMESTAMP
                        WHERE commit_id = '{reverted_commit_safe}'
                    """)
                    context.log.info(f"🚨 Marked commit {reverted_commit[:12]} as reverted in lakefs_commits")
                except Exception as e:
                    context.log.debug(f"Could not update lakefs_commits: {e}")
                
                reverts_triggered += 1
                
                # Include timestamp in run_key to allow retries after failures
                import time
                run_timestamp = int(time.time())
                
                yield RunRequest(
                    run_key=f"cleanup_revert_{reverted_commit[:12]}_{event_db_id}_{run_timestamp}",
                    run_config={
                        "ops": {
                            "cleanup_reverted_commit_multitable": {
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
                        "repository": repository,
                        "branch": branch
                    }
                )
                
            # ═══════════════════════════════════════════════════════════════
            # HANDLE: POST-COMMIT / POST-MERGE
            # ═══════════════════════════════════════════════════════════════
            elif event_type in ['post-commit', 'post-merge']:
                context.log.info(f"")
                context.log.info(f"📝 ────────────────────────────────────────────────")
                context.log.info(f"📝 {event_type.upper()}: {commit_id[:12]}")
                context.log.info(f"📝 ────────────────────────────────────────────────")
                context.log.info(f"📝 Repository: {repository}/{branch}")
                context.log.info(f"📝 Message: {commit_message}")
                context.log.info(f"📝 Committer: {committer}")
                context.log.info(f"📝 Source: {source}")
                if commit_metadata:
                    for key, value in commit_metadata.items():
                        context.log.info(f"📝 Metadata.{key}: {value}")
                context.log.info(f"📝 ────────────────────────────────────────────────")
                
                # Track commit in lakefs_commits table
                try:
                    commit_id_safe = escape_sql_string(commit_id)
                    repository_safe = escape_sql_string(repository)
                    branch_safe = escape_sql_string(branch)
                    commit_msg_safe = escape_sql_string(commit_message[:200])
                    trino.execute_ddl(f"""
                        INSERT INTO {TRINO_CATALOG}.metadata.lakefs_commits (
                            commit_id,
                            lakefs_repository,
                            lakefs_branch,
                            created_at,
                            affected_bronze_tables,
                            status,
                            reverted_at,
                            cleaned_at,
                            revert_reason,
                            last_updated
                        ) VALUES (
                            '{commit_id_safe}',
                            '{repository_safe}',
                            '{branch_safe}',
                            CURRENT_TIMESTAMP,
                            ARRAY[],
                            'valid',
                            NULL,
                            NULL,
                            '{commit_msg_safe}',
                            CURRENT_TIMESTAMP
                        )
                    """)
                    context.log.info(f"📝 Tracked in lakefs_commits table")
                    commits_tracked += 1
                except Exception as e:
                    # Might already exist or table issue
                    context.log.debug(f"Could not track commit: {e}")
                
                # Mark as processed (no job trigger needed)
                trino.execute_ddl(f"""
                    UPDATE {TRINO_CATALOG}.metadata.lakefs_webhook_events
                    SET status = 'processed',
                        processed_at = CURRENT_TIMESTAMP
                    WHERE id = {event_db_id}
                """)
                
            # ═══════════════════════════════════════════════════════════════
            # HANDLE: UNKNOWN EVENT TYPE
            # ═══════════════════════════════════════════════════════════════
            else:
                context.log.warning(f"⚠️ Unknown event type: {event_type}")
                
                # Mark as processed
                trino.execute_ddl(f"""
                    UPDATE {TRINO_CATALOG}.metadata.lakefs_webhook_events
                    SET status = 'processed',
                        processed_at = CURRENT_TIMESTAMP,
                        error_message = 'Unknown event type: {event_type}'
                    WHERE id = {event_db_id}
                """)
        
        # Summary
        context.log.info(f"")
        context.log.info(f"═══════════════════════════════════════════════════")
        context.log.info(f"WEBHOOK SENSOR SUMMARY")
        context.log.info(f"  Events processed: {len(pending)}")
        context.log.info(f"  Commits tracked: {commits_tracked}")
        context.log.info(f"  Reverts triggered: {reverts_triggered}")
        context.log.info(f"═══════════════════════════════════════════════════")
    
    except Exception as e:
        context.log.error(f"Webhook sensor error: {e}")
        import traceback
        context.log.error(traceback.format_exc())
        return SkipReason(f"Error: {e}")


# ═════════════════════════════════════════════════════════════════════════════
# HELPER: Table Cleanup
# ═════════════════════════════════════════════════════════════════════════════

def cleanup_table_by_commit(
    trino: TrinoResource,
    table_name: str,
    commit_id: str,
    context: OpExecutionContext
) -> int:
    """
    Clean up a single table by commit ID.
    Works for Bronze, Silver, or Gold tables.

    Returns:
        Number of records deleted
    """

    try:
        # Validate table name (identifier) and escape commit_id (string value)
        table_name_validated = validate_identifier(table_name, "table name")
        commit_id_safe = escape_sql_string(commit_id)

        # Count affected records
        count_result = trino.execute_query(f"""
            SELECT COUNT(*) FROM {table_name_validated}
            WHERE source_lakefs_commit = '{commit_id_safe}'
        """)
        count = count_result[0][0] if count_result else 0

        if count == 0:
            return 0

        context.log.warning(f"Deleting {count} records from {table_name_validated}")

        # Delete
        trino.execute_ddl(f"""
            DELETE FROM {table_name_validated}
            WHERE source_lakefs_commit = '{commit_id_safe}'
        """)

        context.log.info(f"✓ Deleted {count} from {table_name_validated}")
        return count

    except ValueError as e:
        context.log.error(f"Invalid table name {table_name}: {e}")
        return 0
    except Exception as e:
        context.log.error(f"Failed to clean {table_name}: {e}")
        return 0


def discover_downstream_tables(
    trino: TrinoResource,
    commit_id: str,
    context: OpExecutionContext
) -> list:
    """
    Automatically discover Silver/Gold tables that have records from this commit.
    No hardcoding - discovers dynamically by scanning schemas.

    Returns:
        List of table names that have records with this commit
    """

    downstream = []
    commit_safe = escape_sql_string(commit_id)
    
    # Get all tables in Silver and Gold schemas
    schemas_to_check = ['silver', 'gold']
    
    for schema in schemas_to_check:
        try:
            # Get all tables in this schema
            tables_result = trino.execute_query(f"""
                SHOW TABLES IN {TRINO_CATALOG}.{schema}
            """)

            for (table_name,) in tables_result:
                full_table = f"{TRINO_CATALOG}.{schema}.{table_name}"
                
                # Check if table has source_lakefs_commit column and records with this commit
                try:
                    count_result = trino.execute_query(f"""
                        SELECT COUNT(*) FROM {full_table}
                        WHERE source_lakefs_commit = '{commit_safe}'
                    """)
                    
                    count = count_result[0][0] if count_result else 0
                    
                    if count > 0:
                        downstream.append(full_table)
                        context.log.info(
                            f"Found {count} records in {full_table}"
                        )
                        
                except Exception:
                    # Table doesn't have source_lakefs_commit column, skip
                    pass
                    
        except Exception as e:
            context.log.warning(f"Could not scan {schema}: {e}")
    
    return downstream


def cleanup_labelstudio_tasks(
    commit_id: str,
    context: OpExecutionContext
) -> int:
    """
    Delete Label Studio tasks for specific commit.
    
    Returns:
        Number of tasks deleted
    """
    
    label_studio_url = os.getenv("LABELSTUDIO_URL", "http://exp-label-studio:8080")
    api_token = os.getenv("LABELSTUDIO_API_TOKEN")
    
    if not api_token:
        context.log.warning("Label Studio API token not configured, skipping")
        return 0
    
    # Try Token auth first, then JWT fallback (same as mlops_labelstudio.py)
    headers = None
    auth_type = None
    
    # Try Token authentication
    token_headers = {
        "Authorization": f"Token {api_token}",
        "Content-Type": "application/json",
    }
    try:
        response = requests.get(
            f"{label_studio_url}/api/current-user/whoami",
            headers=token_headers,
            timeout=10,
        )
        if response.status_code == 200:
            headers = token_headers
            auth_type = "token"
    except:
        pass
    
    # Try JWT authentication as fallback
    if not headers:
        try:
            response = requests.post(
                f"{label_studio_url}/api/token/refresh",
                headers={"Content-Type": "application/json"},
                json={"refresh": api_token},
                timeout=10,
            )
            if response.status_code == 200:
                access_token = response.json().get("access")
                if access_token:
                    headers = {
                        "Authorization": f"Bearer {access_token}",
                        "Content-Type": "application/json",
                    }
                    auth_type = "jwt"
        except:
            pass
    
    if not headers:
        context.log.warning("Label Studio authentication failed (tried Token and JWT)")
        return 0
    
    context.log.info(f"Label Studio authenticated ({auth_type})")
    project_id = os.getenv("LABELSTUDIO_PROJECT_ID", "1")
    
    try:
        # First, verify project exists or find the right one
        projects_response = requests.get(
            f"{label_studio_url}/api/projects/",
            headers=headers,
            timeout=30
        )
        
        if projects_response.status_code != 200:
            context.log.warning(f"Failed to list Label Studio projects: {projects_response.status_code}")
            return 0
        
        projects_data = projects_response.json()
        # Handle paginated or list response
        if isinstance(projects_data, list):
            projects = projects_data
        elif isinstance(projects_data, dict):
            projects = projects_data.get("results", [])
        else:
            projects = []
        
        if not projects:
            context.log.info("No Label Studio projects found")
            return 0
        
        # Find project by ID or use first one with matching name
        target_project = None
        for p in projects:
            if str(p.get("id")) == str(project_id):
                target_project = p
                break
        
        # Fallback: use project with name containing "fraud"
        if not target_project:
            for p in projects:
                if "fraud" in p.get("title", "").lower():
                    target_project = p
                    context.log.info(f"Using project '{p.get('title')}' (id={p.get('id')}) instead of {project_id}")
                    break
        
        # Final fallback: use first project
        if not target_project and projects:
            target_project = projects[0]
            context.log.info(f"Using first project '{target_project.get('title')}' (id={target_project.get('id')})")
        
        if not target_project:
            context.log.info("No suitable Label Studio project found")
            return 0
        
        actual_project_id = target_project.get("id")
        
        # Get ALL tasks for this project (paginated)
        all_tasks = []
        page = 1
        page_size = 100
        
        while True:
            response = requests.get(
                f"{label_studio_url}/api/projects/{actual_project_id}/tasks",
                headers=headers,
                params={"page": page, "page_size": page_size},
                timeout=30
            )
            
            if response.status_code != 200:
                context.log.warning(f"Failed to fetch Label Studio tasks (page {page}): {response.status_code}")
                break
            
            tasks_data = response.json()
            
            # Handle paginated or list response
            if isinstance(tasks_data, list):
                tasks = tasks_data
            elif isinstance(tasks_data, dict):
                tasks = tasks_data.get("tasks", tasks_data.get("results", []))
            else:
                tasks = []
            
            if not tasks:
                break
            
            all_tasks.extend(tasks)
            
            # Check if more pages
            if len(tasks) < page_size:
                break
            
            page += 1
            
            # Safety limit
            if page > 500:
                context.log.warning("Hit pagination limit (500 pages)")
                break
        
        context.log.info(f"Found {len(all_tasks)} total tasks in project {actual_project_id}")
        
        # Find tasks with this commit
        tasks_to_delete = [
            t["id"] for t in all_tasks
            if isinstance(t, dict) and t.get("data", {}).get("source_lakefs_commit") == commit_id
        ]
        
        if not tasks_to_delete:
            context.log.info("No Label Studio tasks to delete")
            return 0
        
        context.log.info(f"Deleting {len(tasks_to_delete)} Label Studio tasks")
        
        # Delete tasks
        deleted = 0
        for task_id in tasks_to_delete:
            try:
                resp = requests.delete(
                    f"{label_studio_url}/api/tasks/{task_id}",
                    headers=headers,
                    timeout=10
                )
                if resp.status_code in [200, 204]:
                    deleted += 1
            except Exception as e:
                context.log.warning(f"Failed to delete task {task_id}: {e}")
        
        return deleted
        
    except Exception as e:
        context.log.error(f"Label Studio cleanup error: {e}")
        return 0


# ═════════════════════════════════════════════════════════════════════════════
# OP: Cleanup Reverted Commit (Multi-Table with Auto-Discovery)
# ═════════════════════════════════════════════════════════════════════════════

@op(
    config_schema={
        "reverted_commit": str,
        "webhook_event_id": int,
        "repository": str,
        "branch": str,
    }
)
def cleanup_reverted_commit_multitable(
    context: OpExecutionContext,
    trino: TrinoResource,
):
    """
    Cleanup reverted commit across ALL affected tables.
    
    Handles:
    - Multiple Bronze tables (from tracking metadata)
    - Downstream Silver/Gold tables (auto-discovered)
    - Label Studio tasks
    
    Works for 1 table or 100 tables automatically.
    """
    
    reverted_commit = context.op_config["reverted_commit"]
    webhook_event_id = context.op_config["webhook_event_id"]
    repository = context.op_config.get("repository", "bronze")
    branch = context.op_config.get("branch", "dev")

    context.log.info("")
    context.log.info("╔" + "═" * 68 + "╗")
    context.log.info("║" + " 🚨 CLEANUP STARTING".center(68) + "║")
    context.log.info("╠" + "═" * 68 + "╣")
    context.log.info(f"║  Commit:      {reverted_commit:<50}  ║")
    context.log.info(f"║  Repository:  {repository:<50}  ║")
    context.log.info(f"║  Branch:      {branch:<50}  ║")
    context.log.info(f"║  Webhook ID:  {webhook_event_id:<50}  ║")
    context.log.info(f"║  Started:     {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<50}  ║")
    context.log.info("╚" + "═" * 68 + "╝")
    context.log.info("")
    
    cleanup_start = datetime.now()
    tables_cleaned = []
    total_deleted = 0
    status = "success"
    error_msg = ""

    # Escape strings for SQL using proper escape function
    reverted_commit_safe = escape_sql_string(reverted_commit)
    repository_safe = escape_sql_string(repository)
    branch_safe = escape_sql_string(branch)
    
    try:
        # ═════════════════════════════════════════════════════════════════════
        # Step 1: Mark commit as reverted
        # ═════════════════════════════════════════════════════════════════════
        context.log.info("┌─── STEP 1: Mark Commit as Reverted ───")
        trino.execute_ddl(f"""
            UPDATE {TRINO_CATALOG}.metadata.lakefs_commits
            SET status = 'reverted',
                reverted_at = CURRENT_TIMESTAMP
            WHERE commit_id = '{reverted_commit_safe}'
              AND lakefs_repository = '{repository_safe}'
              AND lakefs_branch = '{branch_safe}'
        """)

        context.log.info(f"│ ✓ Marked commit {reverted_commit[:12]}... as reverted")
        
        # ═════════════════════════════════════════════════════════════════════
        # Step 2: Get all Bronze tables affected by this commit
        # ═════════════════════════════════════════════════════════════════════
        context.log.info("")
        context.log.info("┌─── STEP 2: Find Affected Bronze Tables ───")
        affected_query = f"""
        SELECT
            table_info.table_name,
            table_info.record_count
        FROM {TRINO_CATALOG}.metadata.lakefs_commits,
             UNNEST(affected_bronze_tables) AS table_info
        WHERE commit_id = '{reverted_commit_safe}'
          AND lakefs_repository = '{repository_safe}'
          AND lakefs_branch = '{branch_safe}'
        """

        bronze_tables = trino.execute_query(affected_query)

        if bronze_tables:
            context.log.info(f"│ Found {len(bronze_tables)} Bronze table(s) to clean:")
            for tbl, cnt in bronze_tables:
                tbl_short = tbl.split(".")[-1] if "." in tbl else tbl
                context.log.info(f"│   • {tbl_short}: ~{cnt} records expected")
        else:
            context.log.warning("│ ⚠️  No Bronze tables found in tracking metadata")
        
        # ═════════════════════════════════════════════════════════════════════
        # Step 3: Clean each Bronze table
        # ═════════════════════════════════════════════════════════════════════
        context.log.info("")
        context.log.info("┌─── STEP 3: Clean Bronze Tables ───")
        for table_name, expected_count in bronze_tables:
            deleted = cleanup_table_by_commit(
                trino, table_name, reverted_commit, context
            )
            table_short = table_name.split(".")[-1] if "." in table_name else table_name

            if deleted > 0:
                tables_cleaned.append((table_name, deleted))
                total_deleted += deleted
                context.log.info(f"│ ✓ {table_short}: deleted {deleted} records")
            else:
                context.log.info(f"│   {table_short}: no records to delete")

            # Verify count matches
            if deleted != expected_count:
                context.log.warning(
                    f"│ ⚠️  Count mismatch in {table_short}: "
                    f"expected {expected_count}, deleted {deleted}"
                )
        
        # ═════════════════════════════════════════════════════════════════════
        # Step 4: Find and clean downstream tables (Silver/Gold)
        # Auto-discovery - no hardcoding!
        # ═════════════════════════════════════════════════════════════════════
        context.log.info("")
        context.log.info("┌─── STEP 4: Clean Downstream Tables (Silver/Gold) ───")
        downstream_tables = discover_downstream_tables(
            trino, reverted_commit, context
        )

        if downstream_tables:
            context.log.info(f"│ Found {len(downstream_tables)} downstream table(s)")
            for table_name in downstream_tables:
                deleted = cleanup_table_by_commit(
                    trino, table_name, reverted_commit, context
                )
                table_short = table_name.split(".")[-1] if "." in table_name else table_name

                if deleted > 0:
                    tables_cleaned.append((table_name, deleted))
                    total_deleted += deleted
                    context.log.info(f"│ ✓ {table_short}: deleted {deleted} records")
                else:
                    context.log.info(f"│   {table_short}: no records to delete")
        else:
            context.log.info("│ No downstream tables found with data from this commit")
        
        # ═════════════════════════════════════════════════════════════════════
        # Step 5: Clean Label Studio tasks
        # ═════════════════════════════════════════════════════════════════════
        context.log.info("")
        context.log.info("┌─── STEP 5: Clean Label Studio Tasks ───")
        try:
            labelstudio_deleted = cleanup_labelstudio_tasks(
                reverted_commit, context
            )
            if labelstudio_deleted > 0:
                tables_cleaned.append(('label_studio', labelstudio_deleted))
                total_deleted += labelstudio_deleted
                context.log.info(f"│ ✓ Label Studio: deleted {labelstudio_deleted} tasks")
            else:
                context.log.info("│   Label Studio: no tasks to delete")
        except Exception as e:
            context.log.warning(f"│ ⚠️  Label Studio cleanup failed: {e}")
            error_msg += f"Label Studio: {str(e)[:200]}; "
            status = "partial"
        
        # ═════════════════════════════════════════════════════════════════════
        # Step 6: Mark commit as cleaned
        # ═════════════════════════════════════════════════════════════════════
        trino.execute_ddl(f"""
            UPDATE {TRINO_CATALOG}.metadata.lakefs_commits
            SET status = 'cleaned',
                cleaned_at = CURRENT_TIMESTAMP
            WHERE commit_id = '{reverted_commit_safe}'
              AND lakefs_repository = '{repository_safe}'
              AND lakefs_branch = '{branch_safe}'
        """)
        
        # ═════════════════════════════════════════════════════════════════════
        # Step 7: Record cleanup history
        # ═════════════════════════════════════════════════════════════════════
        cleanup_end = datetime.now()
        
        # Build tables_cleaned array
        if tables_cleaned:
            table_rows = []
            for table, count in tables_cleaned:
                table_safe = escape_sql_string(table)
                table_rows.append(f"ROW('{table_safe}', {count})")
            tables_array = "ARRAY[" + ", ".join(table_rows) + "]"
        else:
            tables_array = "ARRAY[]"

        error_msg_safe = escape_sql_string(error_msg[:500])
        
        # Generate ID for cleanup_history (Trino doesn't support auto-increment)
        id_result = trino.execute_query(f"""
            SELECT COALESCE(MAX(id), 0) + 1
            FROM {TRINO_CATALOG}.metadata.cleanup_history
        """)
        cleanup_id = id_result[0][0] if id_result else 1
        
        # Format timestamps for Trino (space separator, not 'T')
        cleanup_start_str = cleanup_start.strftime('%Y-%m-%d %H:%M:%S.%f')
        cleanup_end_str = cleanup_end.strftime('%Y-%m-%d %H:%M:%S.%f')
        
        # Insert cleanup history (Trino-compatible: all columns explicit)
        trino.execute_ddl(f"""
            INSERT INTO {TRINO_CATALOG}.metadata.cleanup_history
            VALUES (
                {cleanup_id},
                '{reverted_commit_safe}',
                TIMESTAMP '{cleanup_start_str}',
                TIMESTAMP '{cleanup_end_str}',
                {tables_array},
                {total_deleted},
                '{status}',
                '{error_msg_safe}',
                'webhook'
            )
        """)
        
        # ═════════════════════════════════════════════════════════════════════
        # Step 8: Mark webhook as processed
        # ═════════════════════════════════════════════════════════════════════
        trino.execute_ddl(f"""
            UPDATE {TRINO_CATALOG}.metadata.lakefs_webhook_events
            SET status = 'processed',
                processed_at = CURRENT_TIMESTAMP
            WHERE id = {webhook_event_id}
        """)
        
        # ═════════════════════════════════════════════════════════════════════
        # Summary
        # ═════════════════════════════════════════════════════════════════════
        duration = (cleanup_end - cleanup_start).total_seconds()

        context.log.info("")
        context.log.info("╔" + "═" * 68 + "╗")
        context.log.info("║" + " CLEANUP COMPLETE".center(68) + "║")
        context.log.info("╠" + "═" * 68 + "╣")
        context.log.info(f"║  Commit:          {reverted_commit[:20]:<46}  ║")
        context.log.info(f"║  Duration:        {duration:.2f}s{' ' * 43}║")
        context.log.info(f"║  Tables Cleaned:  {len(tables_cleaned):<46}  ║")
        context.log.info(f"║  Total Deleted:   {total_deleted:<46}  ║")
        context.log.info(f"║  Status:          {status.upper():<46}  ║")
        context.log.info("╠" + "═" * 68 + "╣")

        if tables_cleaned:
            context.log.info("║  BREAKDOWN BY TABLE:".ljust(69) + "║")
            context.log.info("║  " + "─" * 64 + "  ║")
            for table, count in tables_cleaned:
                table_short = table.split(".")[-1] if "." in table else table
                context.log.info(f"║    • {table_short:<40} {count:>15} records  ║")
            context.log.info("║  " + "─" * 64 + "  ║")

        context.log.info("╚" + "═" * 68 + "╝")
        context.log.info("")
        
        return {
            "reverted_commit": reverted_commit,
            "tables_cleaned": len(tables_cleaned),
            "total_deleted": total_deleted,
            "duration_seconds": duration,
            "status": status
        }
        
    except Exception as e:
        context.log.error(f"❌ CLEANUP FAILED: {e}")

        # Mark webhook as failed
        error_safe = escape_sql_string(str(e)[:500])
        trino.execute_ddl(f"""
            UPDATE {TRINO_CATALOG}.metadata.lakefs_webhook_events
            SET status = 'failed',
                error_message = '{error_safe}'
            WHERE id = {webhook_event_id}
        """)

        raise


# ═════════════════════════════════════════════════════════════════════════════
# JOB: Webhook-Triggered Cleanup
# ═════════════════════════════════════════════════════════════════════════════

@job(
    name="lakefs_webhook_cleanup_job",
    description="Cleanup reverted commits triggered by LakeFS webhook (multi-table support)"
)
def lakefs_webhook_cleanup_job():
    cleanup_reverted_commit_multitable()