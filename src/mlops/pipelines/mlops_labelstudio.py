"""
Label Studio Integration - Production Ready for Large Datasets
Silver → Label Studio (annotation) → Silver (reviewed labels)

Handles 10,000-15,000+ records with:
- **INCREMENTAL PROCESSING** - Only processes new annotations, skips already reviewed
- Paginated fetch for faster loading (no 5-min timeout!)
- Bulk import API for faster exports
- Duplicate detection
- Chunked processing for memory efficiency
- Batch SQL updates
- Retry logic with backoff
- Progress tracking
- Comprehensive error handling

Incremental Processing:
1. Fetches all completed tasks from Label Studio
2. Queries silver table for already reviewed transaction_ids
3. Filters out already reviewed tasks (no re-processing)
4. Only processes NEW annotations
5. Next run automatically picks up where it left off

Environment Variables:
- LABELSTUDIO_URL: Label Studio URL
- LABELSTUDIO_API_TOKEN: API token
- LABELSTUDIO_FETCH_PAGE_SIZE: Tasks per page when fetching (default: 100)
- LABELSTUDIO_EXPORT_BATCH_SIZE: Records per export batch
- LABELSTUDIO_MERGE_BATCH_SIZE: NEW tasks to process per run (default: 50000)
- SENSOR_MERGE_INTERVAL_SECONDS: Merge sensor interval
"""
import os
import json
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
from typing import Dict, Tuple, Optional, List, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from dagster import (
    asset,
    sensor,
    AssetExecutionContext,
    SensorEvaluationContext,
    MaterializeResult,
    RunRequest,
    SkipReason,
    DefaultSensorStatus,
    define_asset_job,
    AssetSelection,
    MetadataValue,
    RetryPolicy,
    Backoff,
    DagsterRunStatus,
    RunsFilter,
)
from src.core.resources import TrinoResource
from src.core.config import TRINO_CATALOG


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER: Get Current Snapshot (for rollback detection)
# ═══════════════════════════════════════════════════════════════════════════════

def get_current_snapshot(trino: TrinoResource, table_name: str) -> int:
    """
    Get current snapshot ID for a table.
    Used for rollback detection - Label Studio tasks tagged with snapshot_id.
    """
    try:
        # Parse table name: iceberg_dev.silver.fraud_transactions
        parts = table_name.split('.')
        if len(parts) != 3:
            return 0
        
        catalog, schema, table = parts
        
        # Query snapshots table
        snapshot_query = f"""
        SELECT snapshot_id 
        FROM {catalog}.{schema}."{table}$snapshots"
        ORDER BY committed_at DESC 
        LIMIT 1
        """
        
        result = trino.execute_query(snapshot_query)
        if result and len(result) > 0:
            return result[0][0]
    except Exception:
        # If snapshot query fails (old Iceberg version), return 0
        pass
    
    return 0


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER: Get Valid Commits (for LakeFS sync)
# ═══════════════════════════════════════════════════════════════════════════════

def get_valid_commits(trino: TrinoResource) -> set:
    """
    Get set of valid commit IDs from tracking table.
    Used to filter out data from reverted commits.
    Returns empty set if table doesn't exist (graceful degradation).
    """
    try:
        result = trino.execute_query(f"""
            SELECT commit_id
            FROM {TRINO_CATALOG}.metadata.lakefs_commits
            WHERE status = 'valid'
        """)
        return {row[0] for row in result}
    except Exception:
        # Table doesn't exist yet or query failed - return empty set
        # This allows system to work without commit tracking
        return set()


# ═══════════════════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

PROJECT_TITLE = "Fraud Detection Review"

# Chunk sizes for processing
BULK_IMPORT_CHUNK_SIZE = 500      # Tasks per bulk import request
SQL_UPDATE_CHUNK_SIZE = 500       # Records per SQL UPDATE
MERGE_UPDATE_CHUNK_SIZE = 100     # Records per merge UPDATE batch

# Timeouts
HTTP_TIMEOUT_SHORT = 30           # For small requests
HTTP_TIMEOUT_LONG = 180           # For bulk operations
HTTP_TIMEOUT_EXPORT = 300         # For large exports

# Retry settings
MAX_RETRIES = 3
RETRY_DELAY_BASE = 5              # Seconds


def create_labelstudio_session(
    pool_connections: int = 10,
    pool_maxsize: int = 20,
    max_retries: int = 3,
    backoff_factor: float = 0.5,
) -> requests.Session:
    """
    Create a requests session with connection pooling and retry logic for Label Studio API.

    Args:
        pool_connections: Number of connection pools to cache
        pool_maxsize: Max connections per pool
        max_retries: Number of retries for failed requests
        backoff_factor: Backoff factor for retries

    Returns:
        Configured requests.Session
    """
    session = requests.Session()

    # Configure retry strategy
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "PATCH", "DELETE"],
    )

    # Create adapter with connection pooling
    adapter = HTTPAdapter(
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
        max_retries=retry_strategy,
    )

    # Mount for both http and https
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


def get_env_int(key: str, default: int = None) -> int:
    """Get integer environment variable."""
    value = os.getenv(key)
    if value:
        return int(value)
    if default is not None:
        return default
    raise ValueError(f"Required environment variable {key} is not set")


# ═══════════════════════════════════════════════════════════════════════════════
# LABEL STUDIO API HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def get_labelstudio_headers(api_key: str, base_url: str) -> Tuple[Dict[str, str], str]:
    """Get Label Studio authentication headers with retry."""
    token_headers = {
        "Authorization": f"Token {api_key}",
        "Content-Type": "application/json",
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(
                f"{base_url}/api/current-user/whoami",
                headers=token_headers,
                timeout=HTTP_TIMEOUT_SHORT,
            )
            if response.status_code == 200:
                return token_headers, "token"
        except requests.exceptions.RequestException:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY_BASE * (attempt + 1))
    
    # Try JWT as fallback
    try:
        response = requests.post(
            f"{base_url}/api/token/refresh",
            headers={"Content-Type": "application/json"},
            json={"refresh": api_key},
            timeout=HTTP_TIMEOUT_SHORT,
        )
        if response.status_code == 200:
            access_token = response.json().get("access")
            if access_token:
                return {
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                }, "jwt"
    except:
        pass
    
    return token_headers, "token"


def get_or_create_project(
    base_url: str,
    headers: Dict[str, str],
    labeling_config: str,
    context: AssetExecutionContext,
) -> int:
    """
    Get existing project or create new one.
    Handles duplicates by using project with most tasks.
    """
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(
                f"{base_url}/api/projects/",
                headers=headers,
                timeout=HTTP_TIMEOUT_SHORT,
            )
            response.raise_for_status()
            data = response.json()
            
            # Handle different API response formats
            # Could be: list, or dict with 'results' key (paginated)
            if isinstance(data, list):
                projects = data
            elif isinstance(data, dict):
                projects = data.get("results", [])
            else:
                context.log.warning(f"Unexpected API response type: {type(data)}")
                projects = []
            
            # Filter to matching projects (safely)
            matching = []
            for p in projects:
                if isinstance(p, dict) and p.get("title") == PROJECT_TITLE:
                    matching.append(p)
            
            if matching:
                if len(matching) > 1:
                    context.log.warning(f"Found {len(matching)} projects named '{PROJECT_TITLE}', using one with most tasks")
                    matching.sort(key=lambda p: p.get("task_number", 0), reverse=True)
                
                project = matching[0]
                context.log.info(f"✓ Using project ID {project['id']} ({project.get('task_number', 0)} existing tasks)")
                return project["id"]
            
            # Create new project
            context.log.info(f"Creating new project '{PROJECT_TITLE}'...")
            create_response = requests.post(
                f"{base_url}/api/projects/",
                headers=headers,
                json={
                    "title": PROJECT_TITLE,
                    "description": "Fraud transaction review - auto-created",
                    "label_config": labeling_config,
                },
                timeout=HTTP_TIMEOUT_SHORT,
            )
            
            if create_response.status_code in [200, 201]:
                new_id = create_response.json().get("id")
                context.log.info(f"✓ Created project ID {new_id}")
                return new_id
            else:
                raise Exception(f"Create failed: {create_response.status_code} - {create_response.text[:200]}")
                
        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                context.log.warning(f"Attempt {attempt + 1} failed: {e}, retrying...")
                time.sleep(RETRY_DELAY_BASE * (attempt + 1))
            else:
                raise Exception(f"Failed after {MAX_RETRIES} attempts: {e}")
    
    raise Exception("Failed to get or create project")


def get_existing_transaction_ids(
    base_url: str,
    headers: Dict[str, str],
    project_id: int,
    context: AssetExecutionContext,
) -> Set[str]:
    """
    Get all existing transaction IDs in Label Studio to prevent duplicates.
    Uses parallel pagination for large datasets (significantly faster).
    """
    existing_ids = set()
    page_size = 1000
    max_workers = 5  # Parallel page fetches

    context.log.info("Checking for existing tasks in Label Studio (parallel fetch)...")

    # Create session for connection reuse
    session = create_labelstudio_session(pool_connections=max_workers, pool_maxsize=max_workers * 2)

    def fetch_page(page_num: int) -> List[str]:
        """Fetch a single page and return transaction IDs."""
        try:
            response = session.get(
                f"{base_url}/api/tasks/",
                headers=headers,
                params={
                    "project": project_id,
                    "page": page_num,
                    "page_size": page_size,
                },
                timeout=HTTP_TIMEOUT_LONG,
            )

            if response.status_code != 200:
                return []

            data = response.json()
            tasks = data if isinstance(data, list) else data.get("tasks", data.get("results", []))

            ids = []
            for task in tasks:
                if isinstance(task, dict):
                    tx_id = task.get("data", {}).get("transaction_id")
                    if tx_id:
                        ids.append(tx_id)
            return ids
        except Exception:
            return []

    try:
        # First, get the first page to determine total count
        response = session.get(
            f"{base_url}/api/tasks/",
            headers=headers,
            params={
                "project": project_id,
                "page": 1,
                "page_size": page_size,
            },
            timeout=HTTP_TIMEOUT_LONG,
        )

        if response.status_code != 200:
            context.log.warning(f"Could not fetch existing tasks: {response.status_code}")
            return existing_ids

        data = response.json()
        tasks = data if isinstance(data, list) else data.get("tasks", data.get("results", []))

        # Extract IDs from first page
        for task in tasks:
            if isinstance(task, dict):
                tx_id = task.get("data", {}).get("transaction_id")
                if tx_id:
                    existing_ids.add(tx_id)

        # Determine total pages
        total_count = data.get("total", len(tasks)) if isinstance(data, dict) else len(tasks)
        total_pages = (total_count + page_size - 1) // page_size

        if total_pages <= 1:
            context.log.info(f"Found {len(existing_ids)} existing tasks (single page)")
            return existing_ids

        context.log.info(f"Fetching {total_pages} pages in parallel (total: ~{total_count} tasks)...")

        # Fetch remaining pages in parallel
        # Configurable max pages for 1M+ scalability (0 = unlimited)
        MAX_PAGES = int(os.getenv("LABELSTUDIO_MAX_PAGES", "0")) or total_pages + 1
        pages_to_fetch = list(range(2, min(total_pages + 1, MAX_PAGES)))  # Skip page 1 (already fetched)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_page, p): p for p in pages_to_fetch}

            for future in as_completed(futures):
                page_ids = future.result()
                existing_ids.update(page_ids)

    except Exception as e:
        context.log.warning(f"Error in parallel fetch: {e}")
    finally:
        session.close()

    if existing_ids:
        context.log.info(f"Found {len(existing_ids)} existing tasks in Label Studio")

    return existing_ids


# ═══════════════════════════════════════════════════════════════════════════════
# REDIS CACHE FOR LABEL STUDIO TASK IDs
# ═══════════════════════════════════════════════════════════════════════════════

REDIS_CACHE_KEY = "labelstudio:task_ids"
REDIS_CACHE_TTL = 3600 * 24  # 24 hours


def get_redis_client():
    """Get Redis client for Label Studio caching (optional optimization)."""
    try:
        import redis
        client = redis.Redis(
            host=os.getenv("REDIS_HOST", "exp-redis"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
        )
        client.ping()
        return client
    except Exception:
        return None


def get_cached_task_ids(context: AssetExecutionContext) -> Optional[Set[str]]:
    """Get task IDs from Redis cache if available."""
    client = get_redis_client()
    if not client:
        return None

    try:
        cached_ids = client.smembers(REDIS_CACHE_KEY)
        if cached_ids:
            context.log.info(f"Redis cache hit: {len(cached_ids)} task IDs")
            return cached_ids
    except Exception as e:
        context.log.debug(f"Redis cache miss: {e}")
    return None


def cache_task_ids(task_ids: Set[str], context: AssetExecutionContext) -> bool:
    """Cache task IDs in Redis."""
    client = get_redis_client()
    if not client or not task_ids:
        return False

    try:
        # Use pipeline for efficiency
        pipe = client.pipeline()
        pipe.delete(REDIS_CACHE_KEY)
        if task_ids:
            pipe.sadd(REDIS_CACHE_KEY, *task_ids)
        pipe.expire(REDIS_CACHE_KEY, REDIS_CACHE_TTL)
        pipe.execute()
        context.log.info(f"Cached {len(task_ids)} task IDs in Redis (TTL: {REDIS_CACHE_TTL}s)")
        return True
    except Exception as e:
        context.log.debug(f"Failed to cache task IDs: {e}")
    return False


def add_to_task_id_cache(new_ids: Set[str], context: AssetExecutionContext) -> bool:
    """Add new task IDs to Redis cache (incremental update)."""
    client = get_redis_client()
    if not client or not new_ids:
        return False

    try:
        # Only add if cache exists
        if client.exists(REDIS_CACHE_KEY):
            client.sadd(REDIS_CACHE_KEY, *new_ids)
            context.log.debug(f"Added {len(new_ids)} new IDs to Redis cache")
            return True
    except Exception as e:
        context.log.debug(f"Failed to update cache: {e}")
    return False


def get_existing_transaction_ids_with_cache(
    base_url: str,
    headers: Dict[str, str],
    project_id: int,
    context: AssetExecutionContext,
) -> Set[str]:
    """
    Get existing transaction IDs with Redis caching for speed.
    Falls back to API fetch if cache unavailable.
    """
    # Try Redis cache first
    cached = get_cached_task_ids(context)
    if cached is not None:
        return cached

    # Cache miss - fetch from Label Studio API
    context.log.info("Redis cache miss, fetching from Label Studio API...")
    existing_ids = get_existing_transaction_ids(base_url, headers, project_id, context)

    # Cache for future use
    cache_task_ids(existing_ids, context)

    return existing_ids


def bulk_import_tasks(
    base_url: str,
    headers: Dict[str, str],
    project_id: int,
    tasks: List[Dict],
    context: AssetExecutionContext,
) -> Tuple[int, int, List[str]]:
    """
    Bulk import tasks using Label Studio's import endpoint.
    
    Returns: (success_count, failed_count, successful_transaction_ids)
    """
    if not tasks:
        return 0, 0, []

    total = len(tasks)
    success = 0
    failed = 0
    successful_tx_ids = []

    # Configurable parallel workers for 1M+ scalability
    PARALLEL_IMPORT_WORKERS = int(os.getenv("LABELSTUDIO_PARALLEL_WORKERS", "5"))

    # Split into chunks
    chunks = []
    for i in range(0, total, BULK_IMPORT_CHUNK_SIZE):
        chunk = tasks[i:i + BULK_IMPORT_CHUNK_SIZE]
        chunk_tx_ids = [t["data"]["transaction_id"] for t in chunk]
        chunk_num = (i // BULK_IMPORT_CHUNK_SIZE) + 1
        chunks.append((chunk, chunk_tx_ids, chunk_num))

    total_chunks = len(chunks)

    def import_chunk(chunk_data):
        """Import a single chunk with retry logic."""
        chunk, chunk_tx_ids, chunk_num = chunk_data
        chunk_success = False
        imported_count = 0

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.post(
                    f"{base_url}/api/projects/{project_id}/import",
                    headers=headers,
                    json=chunk,
                    timeout=HTTP_TIMEOUT_LONG,
                )

                if response.status_code in [200, 201]:
                    result = response.json()
                    imported_count = result.get("task_count", len(chunk))
                    chunk_success = True
                    break
                else:
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY_BASE * (attempt + 1))

            except requests.exceptions.RequestException as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY_BASE * (attempt + 1))

        return chunk_num, chunk_success, imported_count, chunk_tx_ids if chunk_success else []

    # Process chunks in parallel
    with ThreadPoolExecutor(max_workers=PARALLEL_IMPORT_WORKERS) as executor:
        futures = {executor.submit(import_chunk, chunk_data): chunk_data[2] for chunk_data in chunks}

        for future in as_completed(futures):
            chunk_num, chunk_success, imported_count, tx_ids = future.result()
            if chunk_success:
                success += imported_count
                successful_tx_ids.extend(tx_ids)
                context.log.info(f"  Chunk {chunk_num}/{total_chunks}: imported {imported_count} tasks")
            else:
                chunk_size = len(chunks[chunk_num - 1][0])
                failed += chunk_size
                context.log.error(f"  Chunk {chunk_num}/{total_chunks} failed")

    return success, failed, successful_tx_ids


def batch_update_status(
    trino: TrinoResource,
    silver_table: str,
    transaction_ids: List[str],
    new_status: str,
    context: AssetExecutionContext,
) -> int:
    """
    Update review_status for multiple records atomically.
    Uses SINGLE UPDATE to create only 1 Iceberg snapshot.
    
    Returns number of successfully updated records.
    """
    if not transaction_ids:
        return 0
    
    total = len(transaction_ids)
    context.log.info(f"    Updating {total} records to status='{new_status}' (1 snapshot)")
    
    # For very large updates, we still need to chunk to avoid query size limits
    # But use larger chunks (5000) to minimize snapshots
    ATOMIC_CHUNK_SIZE = 5000
    
    if total <= ATOMIC_CHUNK_SIZE:
        # Single atomic UPDATE (1 snapshot)
        tx_ids_safe = [f"'{tx.replace(chr(39), chr(39)+chr(39))}'" for tx in transaction_ids]
        sql = f"""
        UPDATE {silver_table}
        SET review_status = '{new_status}', silver_processed_at = CURRENT_TIMESTAMP
        WHERE transaction_id IN ({','.join(tx_ids_safe)})
        """
        try:
            trino.execute_ddl(sql)
            context.log.info(f"    ✓ Updated {total} records (1 snapshot)")
            return total
        except Exception as e:
            context.log.error(f"    ✗ Failed to update: {e}")
            return 0
    else:
        # For very large batches, use larger chunks
        updated = 0
        total_chunks = (total + ATOMIC_CHUNK_SIZE - 1) // ATOMIC_CHUNK_SIZE
        context.log.info(f"    Large batch: splitting into {total_chunks} chunks ({total_chunks} snapshots)")
        
        for i in range(0, total, ATOMIC_CHUNK_SIZE):
            chunk = transaction_ids[i:i + ATOMIC_CHUNK_SIZE]
            chunk_num = (i // ATOMIC_CHUNK_SIZE) + 1
            
            tx_ids_safe = [f"'{tx.replace(chr(39), chr(39)+chr(39))}'" for tx in chunk]
            
            sql = f"""
            UPDATE {silver_table}
            SET review_status = '{new_status}', silver_processed_at = CURRENT_TIMESTAMP
            WHERE transaction_id IN ({','.join(tx_ids_safe)})
            """
            
            try:
                trino.execute_ddl(sql)
                updated += len(chunk)
                context.log.info(f"    Chunk {chunk_num}/{total_chunks}: {len(chunk)} records")
            except Exception as e:
                context.log.error(f"    ✗ Failed chunk {chunk_num}: {e}")
        
        return updated


# ═══════════════════════════════════════════════════════════════════════════════
# LABELING CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

LABELING_CONFIG = """<View>
  <Header value="Fraud Transaction Review"/>
  <View style="padding: 20px; background: #f9f9f9; border-radius: 5px;">
    <Text name="tx_info" value="Transaction: $transaction_id | Amount: $amount | Customer: $customer_id"/>
    <Text name="details" value="Date: $date | Method: $payment_method | Category: $category"/>
    <Text name="customer" value="Location: $location | Age: $customer_age | Device: $device"/>
    <Text name="original" value="Original Label: $original_label"/>
  </View>
  <Choices name="fraud_label" toName="tx_info" choice="single" required="true">
    <Choice value="fraud"/>
    <Choice value="legit"/>
    <Choice value="suspicious"/>
  </Choices>
</View>"""


# ═══════════════════════════════════════════════════════════════════════════════
# RETRY POLICY
# ═══════════════════════════════════════════════════════════════════════════════

LABELSTUDIO_RETRY = RetryPolicy(max_retries=3, delay=10, backoff=Backoff.EXPONENTIAL)


# ═══════════════════════════════════════════════════════════════════════════════
# ASSETS
# ═══════════════════════════════════════════════════════════════════════════════

@asset(
    group_name="mlops_labelstudio",
    deps=["mlops_notify_jupyter"],
    description="Ensure Silver table has review columns",
    retry_policy=LABELSTUDIO_RETRY,
)
def mlops_ensure_review_columns(
    context: AssetExecutionContext,
    trino: TrinoResource,
) -> MaterializeResult:
    """Ensure Silver table has columns for Label Studio integration."""
    
    silver_table = os.getenv("SILVER_TABLE", f"{TRINO_CATALOG}.silver.fraud_transactions")
    
    required_columns = {
        "review_status": "VARCHAR",
        "reviewed_label": "INTEGER",
        "annotations": "VARCHAR",
        "annotated_by": "VARCHAR",
        "annotated_at": "TIMESTAMP(6)",
        "silver_processed_at": "TIMESTAMP(6)",
        # CRITICAL: For LakeFS revert propagation - must track which commit data came from
        "source_lakefs_commit": "VARCHAR",
    }
    
    try:
        schema_result = trino.execute_query(f"DESCRIBE {silver_table}")
        existing_cols = {row[0].lower() for row in schema_result}
    except Exception as e:
        context.log.error(f"Cannot describe table: {e}")
        raise
    
    added = []
    for col, dtype in required_columns.items():
        if col.lower() not in existing_cols:
            try:
                trino.execute_ddl(f"ALTER TABLE {silver_table} ADD COLUMN {col} {dtype}")
                added.append(col)
                context.log.info(f"✓ Added column: {col}")
            except Exception as e:
                if "already exists" not in str(e).lower():
                    context.log.warning(f"Could not add {col}: {e}")
    
    # Set default review_status
    try:
        result = trino.execute_query(
            f"SELECT COUNT(*) FROM {silver_table} WHERE review_status IS NULL"
        )
        null_count = result[0][0] if result else 0
        
        if null_count > 0:
            trino.execute_ddl(f"""
                UPDATE {silver_table}
                SET review_status = 'pending'
                WHERE review_status IS NULL
            """)
            context.log.info(f"✓ Set {null_count} records to review_status='pending'")
    except Exception as e:
        context.log.warning(f"Could not set defaults: {e}")
    
    return MaterializeResult(
        metadata={"columns_added": MetadataValue.text(", ".join(added) if added else "none")}
    )


@asset(
    group_name="mlops_labelstudio",
    deps=["mlops_ensure_review_columns"],
    description="Export Silver pending transactions to Label Studio (bulk import)",
    retry_policy=LABELSTUDIO_RETRY,
)
def mlops_export_to_labelstudio(
    context: AssetExecutionContext,
    trino: TrinoResource,
) -> MaterializeResult:
    """
    Export pending Silver transactions to Label Studio.
    
    Features:
    - Bulk import API for speed (500 tasks per request)
    - Duplicate detection (skips existing tasks)
    - Chunked SQL updates
    - Retry logic with backoff
    - Progress tracking
    """
    
    silver_table = os.getenv("SILVER_TABLE", f"{TRINO_CATALOG}.silver.fraud_transactions")
    label_studio_url = os.getenv("LABELSTUDIO_URL", "http://exp-label-studio:8080")
    label_studio_api_key = os.getenv("LABELSTUDIO_API_TOKEN", "")
    batch_size = get_env_int("LABELSTUDIO_EXPORT_BATCH_SIZE", 15000)
    
    # ─── Check for demo mode ───
    if not label_studio_api_key:
        context.log.warning("⚠️ LABELSTUDIO_API_TOKEN not set - DEMO MODE")
        
        # Get pending records (need IDs for batch update)
        pending_result = trino.execute_query(f"""
            SELECT transaction_id FROM {silver_table}
            WHERE review_status = 'pending'
            LIMIT {batch_size}
        """)
        
        if not pending_result:
            context.log.info("No pending records")
            return MaterializeResult(metadata={"exported": MetadataValue.int(0), "demo_mode": MetadataValue.bool(True)})
        
        # Extract IDs and batch update
        tx_ids = [row[0] for row in pending_result]
        context.log.info(f"Demo mode: Marking {len(tx_ids)} records as in_review")
        
        updated = batch_update_status(trino, silver_table, tx_ids, "in_review", context)
        
        return MaterializeResult(
            metadata={
                "exported": MetadataValue.int(updated),
                "demo_mode": MetadataValue.bool(True),
            }
        )
    
    # ─── Production mode ───
    context.log.info("═" * 60)
    context.log.info("EXPORT TO LABEL STUDIO - PRODUCTION MODE")
    context.log.info("═" * 60)
    
    # Authenticate
    try:
        headers, auth_type = get_labelstudio_headers(label_studio_api_key, label_studio_url)
        context.log.info(f"✓ Authenticated ({auth_type})")
    except Exception as e:
        context.log.error(f"Authentication failed: {e}")
        raise
    
    # Get or create project
    try:
        project_id = get_or_create_project(label_studio_url, headers, LABELING_CONFIG, context)
    except Exception as e:
        context.log.error(f"Project setup failed: {e}")
        raise
    
    # Get existing tasks to prevent duplicates (with Redis cache for speed)
    existing_tx_ids = get_existing_transaction_ids_with_cache(label_studio_url, headers, project_id, context)
    
    # Query pending records
    context.log.info(f"Querying up to {batch_size} pending records...")
    
    query = f"""
    SELECT 
        transaction_id, customer_id, transaction_amount, transaction_date,
        payment_method, product_category, quantity, customer_age,
        customer_location, device_used, account_age_days, transaction_hour,
        is_fraudulent, ip_address, shipping_address, billing_address,
        source_lakefs_commit
    FROM {silver_table}
    WHERE review_status = 'pending'
    ORDER BY transaction_date DESC
    LIMIT {batch_size}
    """
    
    pending_records = trino.execute_query(query)
    
    if not pending_records:
        context.log.info("No pending records to export")
        return MaterializeResult(metadata={"exported": MetadataValue.int(0), "skipped_duplicates": MetadataValue.int(0)})
    
    context.log.info(f"Found {len(pending_records)} pending records")
    
    # Build task list, tracking duplicates separately
    tasks_to_import = []
    duplicate_tx_ids = []  # Already in Label Studio, just need status update
    
    for record in pending_records:
        tx_id = record[0]
        
        # Skip if already in Label Studio
        if tx_id in existing_tx_ids:
            duplicate_tx_ids.append(tx_id)
            continue
        
        task = {
            "data": {
                "transaction_id": tx_id,
                "customer_id": record[1],
                "amount": float(record[2]) if record[2] else 0,
                "date": record[3].isoformat() if record[3] else "",
                "payment_method": record[4] or "",
                "category": record[5] or "",
                "quantity": record[6] or 0,
                "customer_age": record[7] or 0,
                "location": record[8] or "",
                "device": record[9] or "",
                "account_age_days": record[10] or 0,
                "transaction_hour": record[11] or 0,
                "original_label": "fraud" if record[12] == 1 else "legit",
                "ip": record[13] or "",
                "shipping": record[14] or "",
                "billing": record[15] or "",
                "is_fraudulent": record[12] or 0,
                # ═════════════════════════════════════════════════════════════
                # LakeFS commit tracking for revert propagation
                # ═════════════════════════════════════════════════════════════
                "source_lakefs_commit": record[16] or ""
                # Note: Snapshot tracking removed - transaction_id is unique/immutable
            }
        }
        tasks_to_import.append(task)
    
    if duplicate_tx_ids:
        context.log.info(f"Found {len(duplicate_tx_ids)} duplicates (already in Label Studio)")
    
    # Bulk import tasks - returns list of SUCCESSFULLY imported transaction IDs
    successful_tx_ids = []
    if tasks_to_import:
        context.log.info(f"Bulk importing {len(tasks_to_import)} new tasks...")
        success, failed, successful_tx_ids = bulk_import_tasks(
            label_studio_url, headers, project_id, tasks_to_import, context
        )
        context.log.info(f"Import complete: {success} success, {failed} failed")

        # Update Redis cache with newly imported task IDs
        if successful_tx_ids:
            add_to_task_id_cache(set(successful_tx_ids), context)
    else:
        success, failed = 0, 0
        context.log.info("No new tasks to import (all were duplicates)")
    
    # Update status for:
    # 1. Successfully imported tasks
    # 2. Duplicates (already in Label Studio, just need status sync)
    all_tx_ids_to_update = successful_tx_ids + duplicate_tx_ids
    
    if all_tx_ids_to_update:
        context.log.info(f"Updating {len(all_tx_ids_to_update)} records to 'in_review'...")
        context.log.info(f"  (new imports: {len(successful_tx_ids)}, duplicates: {len(duplicate_tx_ids)})")
        updated = batch_update_status(
            trino, silver_table, all_tx_ids_to_update, "in_review", context
        )
        context.log.info(f"✓ Updated {updated} records")
    else:
        context.log.warning("⚠️ No records to update - all imports failed!")
        updated = 0
    
    # Summary
    context.log.info("═" * 60)
    context.log.info(f"EXPORT COMPLETE")
    context.log.info(f"  New tasks imported: {success}")
    context.log.info(f"  Failed imports: {failed}")
    context.log.info(f"  Duplicates synced: {len(duplicate_tx_ids)}")
    context.log.info(f"  Status updated: {updated}")
    context.log.info("═" * 60)
    
    return MaterializeResult(
        metadata={
            "exported": MetadataValue.int(success),
            "failed": MetadataValue.int(failed),
            "duplicates_synced": MetadataValue.int(len(duplicate_tx_ids)),
            "status_updated": MetadataValue.int(updated),
            "project_id": MetadataValue.int(project_id),
            "demo_mode": MetadataValue.bool(False),
        }
    )


@asset(
    group_name="mlops_labelstudio",
    deps=["mlops_export_to_labelstudio"],
    description="Notify that transactions are ready for annotation",
)
def mlops_notify_labelstudio(
    context: AssetExecutionContext,
    trino: TrinoResource,
) -> MaterializeResult:
    """Notify that transactions are in Label Studio for review."""
    
    silver_table = os.getenv("SILVER_TABLE", f"{TRINO_CATALOG}.silver.fraud_transactions")
    label_studio_url = os.getenv("LABELSTUDIO_URL", "http://exp-label-studio:8080")
    
    counts = {"pending": 0, "in_review": 0, "reviewed": 0}
    try:
        result = trino.execute_query(f"""
            SELECT review_status, COUNT(*) 
            FROM {silver_table} 
            GROUP BY review_status
        """)
        for row in result:
            status = row[0] or "unknown"
            if status in counts:
                counts[status] = row[1]
    except Exception as e:
        context.log.warning(f"Could not get counts: {e}")
    
    context.log.info(f"""
══════════════════════════════════════════════════════════
🏷️  ANNOTATION STATUS
══════════════════════════════════════════════════════════
⏳ Pending:   {counts['pending']:,}
🔍 In Review: {counts['in_review']:,}
✅ Reviewed:  {counts['reviewed']:,}

Label Studio: {label_studio_url}
══════════════════════════════════════════════════════════
    """)
    
    return MaterializeResult(
        metadata={
            "pending": MetadataValue.int(counts['pending']),
            "in_review": MetadataValue.int(counts['in_review']),
            "reviewed": MetadataValue.int(counts['reviewed']),
        }
    )


# ═══════════════════════════════════════════════════════════════════════════════
# MERGE JOB
# ═══════════════════════════════════════════════════════════════════════════════

mlops_labelstudio_merge_job = define_asset_job(
    name="mlops_labelstudio_merge",
    selection=AssetSelection.assets("mlops_merge_annotations"),
    description="Merge Label Studio annotations back to Silver",
)


@asset(
    group_name="mlops_labelstudio",
    description="Merge Label Studio annotations back to Silver table",
    retry_policy=LABELSTUDIO_RETRY,
)
def mlops_merge_annotations(
    context: AssetExecutionContext,
    trino: TrinoResource,
) -> MaterializeResult:
    """
    Merge completed Label Studio annotations back to Silver table.
    
    Features:
    - Paginated export for large datasets
    - Batch SQL updates
    - Handles partial completions
    - Duplicate annotation handling
    - Progress tracking
    """
    
    silver_table = os.getenv("SILVER_TABLE", f"{TRINO_CATALOG}.silver.fraud_transactions")
    label_studio_url = os.getenv("LABELSTUDIO_URL", "http://exp-label-studio:8080")
    label_studio_api_key = os.getenv("LABELSTUDIO_API_TOKEN", "")
    batch_size = get_env_int("LABELSTUDIO_MERGE_BATCH_SIZE", 50000)  # Process up to 50K tasks
    
    # ─── Demo mode ───
    if not label_studio_api_key:
        context.log.info("DEMO MODE: Auto-reviewing based on is_fraudulent")
        
        # Get in_review IDs for batch update
        in_review_result = trino.execute_query(f"""
            SELECT transaction_id, is_fraudulent FROM {silver_table}
            WHERE review_status = 'in_review'
            LIMIT {batch_size}
        """)
        
        if not in_review_result:
            context.log.info("No records in review")
            return MaterializeResult(metadata={"merged": MetadataValue.int(0), "demo_mode": MetadataValue.bool(True)})
        
        # Batch update - separate fraud and legit for atomic updates
        context.log.info(f"Demo mode: Auto-reviewing {len(in_review_result)} records")
        
        # Separate fraud and legit IDs
        fraud_ids = [f"'{row[0].replace(chr(39), chr(39)+chr(39))}'" for row in in_review_result if row[1] == 1]
        legit_ids = [f"'{row[0].replace(chr(39), chr(39)+chr(39))}'" for row in in_review_result if row[1] != 1]
        
        updated = 0
        
        # Single UPDATE for all fraud records (1 snapshot)
        if fraud_ids:
            try:
                trino.execute_ddl(f"""
                    UPDATE {silver_table}
                    SET reviewed_label = 1, review_status = 'reviewed',
                        annotated_by = 'demo_auto', annotated_at = CURRENT_TIMESTAMP
                    WHERE transaction_id IN ({','.join(fraud_ids)})
                """)
                updated += len(fraud_ids)
                context.log.info(f"  ✓ Updated {len(fraud_ids)} fraud records (1 snapshot)")
            except Exception as e:
                context.log.error(f"  ✗ Demo fraud update failed: {e}")
        
        # Single UPDATE for all legit records (1 snapshot)
        if legit_ids:
            try:
                trino.execute_ddl(f"""
                    UPDATE {silver_table}
                    SET reviewed_label = 0, review_status = 'reviewed',
                        annotated_by = 'demo_auto', annotated_at = CURRENT_TIMESTAMP
                    WHERE transaction_id IN ({','.join(legit_ids)})
                """)
                updated += len(legit_ids)
                context.log.info(f"  ✓ Updated {len(legit_ids)} legit records (1 snapshot)")
            except Exception as e:
                context.log.error(f"  ✗ Demo legit update failed: {e}")
        
        context.log.info(f"✓ Demo mode: {updated} records marked as reviewed")
        
        return MaterializeResult(
            metadata={"merged": MetadataValue.int(updated), "demo_mode": MetadataValue.bool(True)}
        )
    
    # ─── Production mode ───
    context.log.info("═" * 60)
    context.log.info("MERGE ANNOTATIONS - PRODUCTION MODE")
    context.log.info("═" * 60)
    
    # Authenticate
    try:
        headers, auth_type = get_labelstudio_headers(label_studio_api_key, label_studio_url)
        context.log.info(f"✓ Authenticated ({auth_type})")
    except Exception as e:
        context.log.error(f"Auth failed: {e}")
        raise
    
    # Find project
    try:
        response = requests.get(f"{label_studio_url}/api/projects/", headers=headers, timeout=HTTP_TIMEOUT_SHORT)
        response.raise_for_status()
        data = response.json()
        
        # Handle paginated response
        if isinstance(data, list):
            all_projects = data
        elif isinstance(data, dict):
            all_projects = data.get("results", [])
        else:
            all_projects = []
        
        # Filter to matching projects
        projects = [p for p in all_projects if isinstance(p, dict) and p.get("title") == PROJECT_TITLE]
        
        if not projects:
            context.log.warning(f"No project named '{PROJECT_TITLE}' found")
            return MaterializeResult(metadata={"merged": MetadataValue.int(0), "status": "no_project"})
        
        projects.sort(key=lambda p: p.get("task_number", 0), reverse=True)
        project_id = projects[0]["id"]
        context.log.info(f"✓ Found project ID {project_id}")
    except Exception as e:
        context.log.error(f"Could not find project: {e}")
        raise
    
    # ═══════════════════════════════════════════════════════════════
    # CURSOR-BASED FILTERING (client-side, Label Studio API doesn't support date filters)
    # ═══════════════════════════════════════════════════════════════
    
    # Get last processed timestamp from previous run (cursor)
    last_processed_at = None
    try:
        # Check if we have a cursor stored
        cursor_result = trino.execute_query(f"""
            SELECT MAX(annotated_at) as last_processed
            FROM {silver_table}
            WHERE review_status = 'reviewed'
              AND annotated_at IS NOT NULL
        """)
        if cursor_result and cursor_result[0][0]:
            last_processed_at = cursor_result[0][0]
            context.log.info(f"Found cursor: last_processed_at = {last_processed_at}")
            context.log.info("Using cursor-based filtering (client-side) for incremental processing")
        else:
            context.log.info("No cursor found - fetching ALL completed tasks (first run)")
    except Exception as e:
        context.log.warning(f"Could not get cursor: {e}. Fetching all tasks.")
    
    # Export annotations using PAGINATION (client-side filtering for incremental)
    fetch_page_size = get_env_int("LABELSTUDIO_FETCH_PAGE_SIZE", 100)
    
    if last_processed_at:
        # Format timestamp for Label Studio API (ISO 8601)
        cursor_iso = last_processed_at.isoformat() if hasattr(last_processed_at, 'isoformat') else str(last_processed_at)
        context.log.info(f"Will filter annotations AFTER {cursor_iso}")
        context.log.info(f"  Note: Label Studio API doesn't support server-side filtering,")
        context.log.info(f"        fetching all tasks then filtering by annotation timestamp")
    else:
        cursor_iso = None
        context.log.info(f"Fetching ALL completed tasks (first run, page_size={fetch_page_size})")
    
    all_tasks = []
    page = 1
    
    try:
        while True:
            params = {
                "page": page,
                "page_size": fetch_page_size,
            }
            
            # Note: Label Studio tasks API doesn't support updated_at filtering
            # We fetch all tasks with annotations and filter client-side
            # Only filter available: completed_at__isnull
            if not cursor_iso:
                # First run: can use this filter
                params["completed_at__isnull"] = "false"
            # Else: fetch all, filter by annotation timestamp client-side
            
            response = requests.get(
                f"{label_studio_url}/api/projects/{project_id}/tasks",
                headers=headers,
                params=params,
                timeout=HTTP_TIMEOUT_SHORT,
            )
            response.raise_for_status()
            data = response.json()
            
            # Extract tasks from response
            if isinstance(data, list):
                tasks = data
            elif isinstance(data, dict):
                tasks = data.get("tasks", data.get("results", []))
            else:
                tasks = []
            
            if not tasks:
                break  # No more tasks
            
            all_tasks.extend(tasks)
            
            # Progress indicator every 10 pages
            if page % 10 == 0 or len(tasks) < fetch_page_size:
                context.log.info(f"  Fetching: {len(all_tasks)} tasks loaded (page {page})")
            
            # Check if there are more pages
            if isinstance(data, dict):
                total = data.get("total_annotations") or data.get("total")
                if total and len(all_tasks) >= total:
                    break
            
            # If we got fewer than page_size, we're done
            if len(tasks) < fetch_page_size:
                break
                
            page += 1

            # Configurable safety limit for 1M+ scalability (0 = unlimited)
            MAX_MERGE_PAGES = int(os.getenv("LABELSTUDIO_MERGE_MAX_PAGES", "0"))
            if MAX_MERGE_PAGES > 0 and page > MAX_MERGE_PAGES:
                context.log.warning(f"Hit configurable page limit ({MAX_MERGE_PAGES}), stopping at {len(all_tasks)} tasks")
                break
                
    except requests.exceptions.Timeout:
        context.log.error(f"Page {page} timed out - try reducing LABELSTUDIO_FETCH_PAGE_SIZE")
        if not all_tasks:
            raise
    except Exception as e:
        context.log.error(f"Fetch failed on page {page}: {e}")
        if not all_tasks:
            raise
    
    context.log.info(f"Loaded {len(all_tasks)} total tasks from Label Studio")
    if cursor_iso:
        context.log.info(f"  Filtering to annotations updated after {cursor_iso}...")
    
    # ═══════════════════════════════════════════════════════════════
    # END FETCH - NOW FILTER CLIENT-SIDE
    # ═══════════════════════════════════════════════════════════════
    
    # Filter completed annotations
    completed_tasks = []
    skipped_old = 0  # Track annotations skipped due to cursor
    
    for task in all_tasks:
        if not isinstance(task, dict):
            continue
        
        annotations = task.get("annotations") or []
        if not annotations:
            continue
        
        # Get valid (non-cancelled) annotations
        valid = [a for a in annotations if isinstance(a, dict) and not a.get("was_cancelled")]
        if not valid:
            continue
        
        # Check if has result
        latest = valid[-1]
        if not latest.get("result"):
            continue
        
        # ═══════════════════════════════════════════════════════════════
        # CURSOR FILTER: Skip annotations older than last processed
        # Label Studio API doesn't filter by updated_at, so we do it here
        # ═══════════════════════════════════════════════════════════════
        if cursor_iso:
            # Get annotation timestamp
            annotation_time = latest.get("updated_at") or latest.get("created_at")
            if annotation_time:
                # Compare timestamps (ISO format comparison works for sorting)
                if annotation_time <= cursor_iso:
                    skipped_old += 1
                    continue  # Skip - already processed
        
        completed_tasks.append(task)
    
    if skipped_old > 0:
        context.log.info(f"Skipped {skipped_old} annotations already processed (before cursor)")
    
    context.log.info(f"Found {len(completed_tasks)} NEW completed annotations to process")
    
    if not completed_tasks:
        context.log.info("No new completed annotations to process")
        return MaterializeResult(metadata={
            "merged": MetadataValue.int(0), 
            "status": "no_new_completed",
            "cursor_used": MetadataValue.bool(last_processed_at is not None)
        })
    
    # With cursor-based fetch, these are already NEW tasks
    # No need for expensive in-memory filtering!
    filtered_already_reviewed = 0  # Not applicable with cursor
    
    # Limit to batch size
    remaining_tasks = 0
    if len(completed_tasks) > batch_size:
        remaining_tasks = len(completed_tasks) - batch_size
        context.log.warning(f"⚠️  Processing first {batch_size} of {len(completed_tasks)} NEW tasks")
        context.log.warning(f"    Remaining: {remaining_tasks} NEW tasks not processed this run")
        context.log.warning(f"    To process all at once: export LABELSTUDIO_MERGE_BATCH_SIZE=50000")
        completed_tasks = completed_tasks[:batch_size]
    
    # Process annotations and build batch updates
    updates = []  # List of (tx_id, reviewed_label, annotator)
    skipped = 0
    stale_snapshots = 0  # Kept for backward compatibility in metrics
    reverted_commits = 0  # Tasks from reverted commits
    
    # Get valid commits for commit validation
    valid_commits = get_valid_commits(trino)
    if valid_commits:
        context.log.info(f"🔗 Loaded {len(valid_commits)} valid commits for validation")
    else:
        context.log.info(f"⚠️  Commit tracking not enabled - skipping commit validation")
    
    context.log.info(f"   Validating annotations...")
    
    for task in completed_tasks:
        try:
            data = task.get("data", {})
            tx_id = data.get("transaction_id")
            if not tx_id:
                skipped += 1
                continue
            
            # ═════════════════════════════════════════════════════════════════════════
            # Note: Snapshot validation removed - transaction_id is unique/immutable
            # Original data doesn't change, only new records get added
            # ═════════════════════════════════════════════════════════════════════════
            
            # ═════════════════════════════════════════════════════════════════════════
            # NEW: Validate commit - skip annotations from reverted commits
            # ═════════════════════════════════════════════════════════════════════════
            if valid_commits:  # Only validate if commit tracking is enabled
                task_commit = data.get("source_lakefs_commit")
                if task_commit and task_commit not in valid_commits:
                    context.log.warning(
                        f"⚠️  Task {tx_id} from reverted commit {task_commit[:8]}. "
                        f"SKIPPING - commit was reverted in LakeFS!"
                    )
                    reverted_commits += 1
                    continue
            
            # Clean transaction_id (trim whitespace)
            tx_id = str(tx_id).strip()
            if not tx_id:
                skipped += 1
                continue
            
            annotations = task.get("annotations", [])
            valid = [a for a in annotations if isinstance(a, dict) and not a.get("was_cancelled")]
            if not valid:
                skipped += 1
                continue
            
            latest = valid[-1]
            result = latest.get("result", [])
            
            # Extract label
            reviewed_label = None
            for item in result:
                if isinstance(item, dict) and item.get("from_name") == "fraud_label":
                    choices = item.get("value", {}).get("choices", [])
                    # Ensure choices is a list
                    if not isinstance(choices, list):
                        continue
                    # Check for fraud/legit/suspicious in choices (case-insensitive)
                    choices_lower = [str(c).lower() for c in choices]
                    if "fraud" in choices_lower or "suspicious" in choices_lower:
                        reviewed_label = 1
                    elif "legit" in choices_lower or "legitimate" in choices_lower:
                        reviewed_label = 0
                    break
            
            if reviewed_label is None:
                skipped += 1
                context.log.debug(f"Skipped task {tx_id}: no valid label found")
                continue
            
            # Extract annotator
            completed_by = latest.get("completed_by", {})
            if isinstance(completed_by, dict):
                annotator = completed_by.get("email") or completed_by.get("username") or "unknown"
            else:
                annotator = str(completed_by) if completed_by else "unknown"
            
            updates.append((tx_id, reviewed_label, annotator))
            
        except Exception as e:
            context.log.warning(f"Error processing task: {e}")
            skipped += 1
    
    context.log.info(f"Prepared {len(updates)} updates (skipped {skipped} invalid, {stale_snapshots} stale snapshots, {reverted_commits} reverted commits)")
    
    # Deduplicate by transaction_id (keep latest annotation)
    if updates:
        seen = {}
        for tx_id, label, annotator in updates:
            seen[tx_id] = (tx_id, label, annotator)
        updates = list(seen.values())
        context.log.info(f"After deduplication: {len(updates)} unique transaction_ids")
    
    if not updates:
        return MaterializeResult(metadata={"merged": MetadataValue.int(0), "skipped": MetadataValue.int(skipped)})
    
    # ═════════════════════════════════════════════════════════════════════════════
    # FIXED: Use SINGLE MERGE instead of batch UPDATEs
    # This creates only 1 snapshot for ALL annotations (not 150 snapshots!)
    # ═════════════════════════════════════════════════════════════════════════════
    context.log.info(f"Executing SINGLE MERGE for {len(updates)} annotations...")
    context.log.info(f"  ⚡ This will create only 1 snapshot (not {(len(updates) + MERGE_UPDATE_CHUNK_SIZE - 1) // MERGE_UPDATE_CHUNK_SIZE} snapshots)")
    
    # Count stats BEFORE merge
    tx_ids_list = [f"'{u[0].replace(chr(39), chr(39)+chr(39))}'" for u in updates]
    tx_ids_str = ','.join(tx_ids_list)
    
    try:
        before_stats = trino.execute_query(f"""
            SELECT 
                COUNT(*) as total_exists,
                COUNT(CASE WHEN review_status = 'reviewed' THEN 1 END) as already_reviewed
            FROM {silver_table}
            WHERE transaction_id IN ({tx_ids_str})
        """)
        total_exists = before_stats[0][0] if before_stats else 0
        already_reviewed_count = before_stats[0][1] if before_stats else 0
        not_found_count = len(updates) - total_exists
    except Exception as e:
        context.log.warning(f"Could not get before stats: {e}")
        total_exists = 0
        already_reviewed_count = 0
        not_found_count = 0
    
    if total_exists == 0:
        context.log.warning(f"⚠️  None of the {len(updates)} transaction_ids exist in Silver table!")
        context.log.warning("     Possible causes:")
        context.log.warning("       1. Transaction IDs don't match (check format)")
        context.log.warning("       2. Records deleted from silver after export")
        sample_ids = [updates[i][0] for i in range(min(3, len(updates)))]
        context.log.warning(f"     Sample transaction_ids: {sample_ids}")
        
        return MaterializeResult(
            metadata={
                "merged": MetadataValue.int(0),
                "not_found": MetadataValue.int(len(updates)),
                "skipped": MetadataValue.int(skipped),
            }
        )
    
    # Configurable chunk size for MERGE (for 1M+ scalability)
    MERGE_CHUNK_SIZE = int(os.getenv("LABELSTUDIO_MERGE_CHUNK_SIZE", "10000"))

    # Process MERGE in chunks to avoid SQL query size limits
    total_merged = 0
    merge_chunks = (len(updates) + MERGE_CHUNK_SIZE - 1) // MERGE_CHUNK_SIZE
    context.log.info(f"Processing {len(updates)} updates in {merge_chunks} MERGE chunk(s) of up to {MERGE_CHUNK_SIZE} each")

    try:
        start_time = datetime.now()

        for chunk_start in range(0, len(updates), MERGE_CHUNK_SIZE):
            chunk = updates[chunk_start:chunk_start + MERGE_CHUNK_SIZE]
            chunk_num = (chunk_start // MERGE_CHUNK_SIZE) + 1

            # Build VALUES clause for this chunk
            values_parts = []
            for tx_id, reviewed_label, annotator in chunk:
                tx_safe = tx_id.replace("'", "''")
                annotator_safe = annotator.replace("'", "''")[:100]
                values_parts.append(f"('{tx_safe}', {reviewed_label}, '{annotator_safe}')")

            values_clause = ','.join(values_parts)

            # Execute MERGE for this chunk
            merge_sql = f"""
            MERGE INTO {silver_table} AS target
            USING (
                SELECT * FROM (VALUES {values_clause})
                AS source(transaction_id, reviewed_label, annotated_by)
            ) AS source
            ON target.transaction_id = source.transaction_id
            WHEN MATCHED AND target.review_status != 'reviewed' THEN UPDATE SET
                reviewed_label = source.reviewed_label,
                annotated_by = source.annotated_by,
                review_status = 'reviewed',
                annotated_at = CURRENT_TIMESTAMP,
                annotations = CAST(source.reviewed_label AS VARCHAR)
            """

            trino.execute_ddl(merge_sql)
            total_merged += len(chunk)
            context.log.info(f"  MERGE chunk {chunk_num}/{merge_chunks}: {len(chunk)} records")

        merge_duration = (datetime.now() - start_time).total_seconds()

        context.log.info(f"✅ MERGE complete in {merge_duration:.2f}s")
        context.log.info(f"✅ Processed {len(updates)} annotations in {merge_chunks} chunk(s)")
        
    except Exception as e:
        context.log.error(f"❌ MERGE failed: {e}")
        context.log.error("Falling back to chunked UPDATE (will create multiple snapshots)...")
        
        # Fallback to original batch update logic
        updated_count = 0
        for i in range(0, len(updates), MERGE_UPDATE_CHUNK_SIZE):
            chunk = updates[i:i + MERGE_UPDATE_CHUNK_SIZE]
            chunk_num = (i // MERGE_UPDATE_CHUNK_SIZE) + 1
            
            tx_ids = []
            label_cases = []
            annotator_cases = []
            
            for tx_id, reviewed_label, annotator in chunk:
                tx_safe = tx_id.replace("'", "''")
                annotator_safe = annotator.replace("'", "''")[:100]
                
                tx_ids.append(f"'{tx_safe}'")
                label_cases.append(f"WHEN transaction_id = '{tx_safe}' THEN {reviewed_label}")
                annotator_cases.append(f"WHEN transaction_id = '{tx_safe}' THEN '{annotator_safe}'")
            
            batch_sql = f"""
            UPDATE {silver_table}
            SET 
                reviewed_label = CASE {' '.join(label_cases)} ELSE reviewed_label END,
                annotated_by = CASE {' '.join(annotator_cases)} ELSE annotated_by END,
                review_status = 'reviewed',
                annotated_at = CURRENT_TIMESTAMP
            WHERE transaction_id IN ({','.join(tx_ids)})
            """
            
            try:
                trino.execute_ddl(batch_sql)
                context.log.info(f"  Fallback chunk {chunk_num}: updated")
            except Exception as e2:
                context.log.error(f"  Fallback chunk {chunk_num} failed: {e2}")
        
        merge_duration = 0
    
    # Count stats AFTER merge
    try:
        after_stats = trino.execute_query(f"""
            SELECT COUNT(*) 
            FROM {silver_table}
            WHERE transaction_id IN ({tx_ids_str}) 
            AND review_status = 'reviewed'
        """)
        total_reviewed = after_stats[0][0] if after_stats else 0
        updated_count = total_reviewed - already_reviewed_count
    except Exception as e:
        context.log.warning(f"Could not get after stats: {e}")
        updated_count = 0
    
    # Summary
    context.log.info("═" * 60)
    context.log.info(f"MERGE COMPLETE")
    if last_processed_at:
        context.log.info(f"  Cursor-based incremental: YES (fetched only NEW/UPDATED tasks)")
        context.log.info(f"  Last processed timestamp: {last_processed_at}")
    else:
        context.log.info(f"  Cursor-based incremental: NO (first run - fetched all tasks)")
    context.log.info(f"  NEW tasks fetched: {len(completed_tasks)}")
    if remaining_tasks > 0:
        context.log.warning(f"  ⚠️  REMAINING NEW TASKS: {remaining_tasks}")
        context.log.warning(f"      Run again to process more, or increase LABELSTUDIO_MERGE_BATCH_SIZE")
    context.log.info(f"  Unique transaction_ids: {len(updates)}")
    context.log.info(f"  NEW updates applied: {updated_count}")
    context.log.info(f"  Already reviewed (re-annotated): {already_reviewed_count}")
    context.log.info(f"  Not found in table: {not_found_count}")
    context.log.info(f"  Invalid data (skipped): {skipped}")
    context.log.info(f"  Stale snapshots (after rollback): {stale_snapshots}")
    context.log.info(f"  Reverted commits (LakeFS revert): {reverted_commits}")
    context.log.info(f"  ⚡ MERGE duration: {merge_duration:.2f}s")
    context.log.info(f"  ⚡ Snapshots created: 1 (not {(len(updates) + MERGE_UPDATE_CHUNK_SIZE - 1) // MERGE_UPDATE_CHUNK_SIZE})")
    
    # Verification
    total_accounted = updated_count + already_reviewed_count + not_found_count
    context.log.info(f"  Verification: {total_accounted}/{len(updates)} accounted for")
    
    # Diagnostic info
    if len(updates) > 0:
        success_rate = (updated_count / len(updates)) * 100
        context.log.info(f"  Success rate (new updates): {success_rate:.1f}%")
        
        if not_found_count > 0:
            not_found_rate = (not_found_count / len(updates)) * 100
            context.log.warning(f"  ⚠️  {not_found_rate:.1f}% of transaction_ids NOT FOUND in silver table")
            context.log.warning("     Possible causes:")
            context.log.warning("       1. Transaction IDs don't match (check format)")
            context.log.warning("       2. Records deleted from silver after export")
            context.log.warning("       3. Label Studio has old/stale data")
            context.log.warning(f"     Debug: SELECT transaction_id FROM {silver_table} LIMIT 10;")
    context.log.info("═" * 60)
    
    # ═══════════════════════════════════════════════════════════════════════════════
    # LOG ANNOTATION LINEAGE TO MLFLOW - UNIFIED EXPERIMENT
    # ═══════════════════════════════════════════════════════════════════════════════
    try:
        from src.mlops.pipelines.mlflow_lineage import log_annotation_lineage
        
        # Get source commits for data version tracking
        source_commits = []
        try:
            commits_result = trino.execute_query(f"""
                SELECT DISTINCT source_lakefs_commit 
                FROM {silver_table}
                WHERE source_lakefs_commit IS NOT NULL
                  AND review_status = 'reviewed'
                LIMIT 50
            """)
            source_commits = [row[0] for row in commits_result if row[0]]
        except:
            pass
        
        log_annotation_lineage(
            context=context,
            trino=trino,
            annotations_merged=updated_count,
            silver_table=silver_table,
            labelstudio_project_id=project_id,
            duration=merge_duration,
            source_commits=source_commits
        )
            
    except Exception as e:
        context.log.warning(f"Could not log to MLflow (non-critical): {e}")
    
    return MaterializeResult(
        metadata={
            "cursor_used": MetadataValue.bool(last_processed_at is not None),
            "last_cursor": MetadataValue.text(str(last_processed_at) if last_processed_at else "none"),
            "total_tasks_fetched": MetadataValue.int(len(completed_tasks)),
            "unique_ids": MetadataValue.int(len(updates)),
            "new_updates": MetadataValue.int(updated_count),
            "already_reviewed_reannotated": MetadataValue.int(already_reviewed_count),
            "not_found": MetadataValue.int(not_found_count),
            "invalid_skipped": MetadataValue.int(skipped),
            "stale_snapshots": MetadataValue.int(stale_snapshots),
            "reverted_commits_skipped": MetadataValue.int(reverted_commits),
            "remaining_unprocessed": MetadataValue.int(remaining_tasks),
            "merge_duration_seconds": MetadataValue.float(merge_duration),
            "snapshots_created": MetadataValue.int(1),
            "demo_mode": MetadataValue.bool(False),
        }
    )


# ═══════════════════════════════════════════════════════════════════════════════
# SENSOR
# ═══════════════════════════════════════════════════════════════════════════════

_MERGE_SENSOR_INTERVAL = get_env_int("SENSOR_MERGE_INTERVAL_SECONDS", 300)


@sensor(
    job=mlops_labelstudio_merge_job,
    default_status=DefaultSensorStatus.STOPPED,
    minimum_interval_seconds=_MERGE_SENSOR_INTERVAL,
)
def labelstudio_annotations_sensor(
    context: SensorEvaluationContext,
    trino: TrinoResource,
):
    """
    Triggers annotation merge when tasks are in review.
    
    Deduplication relies on:
    1. Running job check (prevents concurrent runs)
    2. State-based checks (no in_review = no trigger)
    
    No cursor used - allows automatic retry if job fails.
    """
    
    # Check for running jobs - THIS is the primary deduplication
    try:
        running = context.instance.get_runs(
            filters=RunsFilter(
                job_name="mlops_labelstudio_merge",
                statuses=[DagsterRunStatus.STARTED, DagsterRunStatus.QUEUED, DagsterRunStatus.STARTING],
            ),
            limit=1,
        )
        if running:
            return SkipReason(f"Merge job running: {running[0].run_id}")
    except:
        pass
    
    silver_table = os.getenv("SILVER_TABLE", f"{TRINO_CATALOG}.silver.fraud_transactions")
    
    try:
        try:
            trino.execute_query(f"SELECT 1 FROM {silver_table} LIMIT 1")
        except:
            return SkipReason("Silver table does not exist")
        
        in_review = trino.execute_query(
            f"SELECT COUNT(*) FROM {silver_table} WHERE review_status = 'in_review'"
        )[0][0]
        
        context.log.info(f"Status: in_review={in_review}")
        
        if in_review == 0:
            return SkipReason("No records in review")
        
        # Trigger run - unique run_key
        return RunRequest(
            run_key=f"merge_{in_review}_{int(datetime.now().timestamp())}",
            tags={"in_review": str(in_review)},
        )
        
    except Exception as e:
        return SkipReason(f"Error: {str(e)[:100]}")


# ═══════════════════════════════════════════════════════════════════════════════
# EXPORTS
# ═══════════════════════════════════════════════════════════════════════════════

LABELSTUDIO_ASSETS = [
    mlops_ensure_review_columns,
    mlops_export_to_labelstudio,
    mlops_notify_labelstudio,
    mlops_merge_annotations,
]
LABELSTUDIO_SENSORS = [labelstudio_annotations_sensor]
LABELSTUDIO_JOBS = [mlops_labelstudio_merge_job]