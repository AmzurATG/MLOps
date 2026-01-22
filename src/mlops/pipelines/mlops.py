"""
MLOps Pipeline - Following Original Flow
Bronze → Jupyter → Silver → Label Studio → Gold → Feast Training
"""
import os
from datetime import datetime, date
import requests
import psycopg2
import pandas as pd
from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    define_asset_job,
    AssetSelection,
    RetryPolicy,
    Backoff,
)
from src.core.resources import (
    LakeFSResource,
    TrinoResource,
    AirbyteResource,
    NessieResource,
)
from src.core.monitoring import track_pipeline, log_data_stats
from src.core.config import TRINO_CATALOG

# =============================================================================
# RETRY POLICIES
# =============================================================================
NETWORK_RETRY = RetryPolicy(max_retries=3, delay=60, backoff=Backoff.EXPONENTIAL)
DB_RETRY = RetryPolicy(max_retries=2, delay=30)


# =============================================================================
# HELPER: INCREMENTAL LOADING
# =============================================================================
def ensure_audit_table(trino: TrinoResource):
    """Create audit table for tracking processed files"""
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.bronze.file_processing_audit (
        file_path VARCHAR,
        processed_at TIMESTAMP,
        lakefs_commit VARCHAR,
        row_count INTEGER,
        status VARCHAR
    )
    """
    trino.execute_ddl(create_sql)


def get_processed_files(trino: TrinoResource) -> set:
    """Get already processed files"""
    try:
        result = trino.execute_query(
            f"SELECT file_path FROM {TRINO_CATALOG}.bronze.file_processing_audit WHERE status = 'success'"
        )
        return {row[0] for row in result}
    except:
        return set()


def mark_file_processed(trino: TrinoResource, file_path: str, lakefs_commit: str, row_count: int, status: str):
    """Mark file as processed"""
    # Escape single quotes in strings to prevent SQL injection
    file_path_safe = file_path.replace("'", "''")
    lakefs_commit_safe = lakefs_commit.replace("'", "''")
    status_safe = status.replace("'", "''")[:200]  # Truncate to 200 chars

    insert_sql = f"""
    INSERT INTO {TRINO_CATALOG}.bronze.file_processing_audit VALUES (
        '{file_path_safe}', CURRENT_TIMESTAMP, '{lakefs_commit_safe}', {row_count}, '{status_safe}'
    )
    """
    trino.execute_ddl(insert_sql)


# =============================================================================
# ASSETS
# =============================================================================
@asset(group_name="mlops_init")
def mlops_init_lakefs(
    context: AssetExecutionContext,
    lakefs: LakeFSResource,
    nessie: NessieResource,
    trino: TrinoResource,
) -> MaterializeResult:
    """
    Initialize LakeFS repos, Nessie branches, Feast registry database, and Trino schemas.
    
    Environment Variables Required:
    - LAKEHOUSE_BRONZE_REPO (default: bronze)
    - LAKEHOUSE_WAREHOUSE_REPO (default: warehouse)
    - LAKEHOUSE_DEV_BRANCH (default: dev)
    - LAKEHOUSE_MAIN_BRANCH (default: main)
    - DAGSTER_POSTGRES_HOST (default: postgres-dagster)
    - DAGSTER_POSTGRES_USER (default: dagster)
    - DAGSTER_POSTGRES_PASSWORD (default: dagster)
    """
    
    results = {}
    
    # Get configuration from environment variables
    bronze_repo = os.getenv("LAKEHOUSE_BRONZE_REPO", "bronze")
    warehouse_repo = os.getenv("LAKEHOUSE_WAREHOUSE_REPO", "warehouse")
    dev_branch = os.getenv("LAKEHOUSE_DEV_BRANCH", "dev")
    main_branch = os.getenv("LAKEHOUSE_MAIN_BRANCH", "main")
    
    context.log.info("=== Configuration ===")
    context.log.info(f"Bronze repo: {bronze_repo}")
    context.log.info(f"Warehouse repo: {warehouse_repo}")
    context.log.info(f"Dev branch: {dev_branch}")
    context.log.info(f"Main branch: {main_branch}")
    
    # =========================================================================
    # 1. Initialize LakeFS Repositories
    # =========================================================================
    context.log.info("=== Initializing LakeFS Repositories ===")
    
    try:
        lakefs.ensure_repo(bronze_repo, f"s3://lakefs/{bronze_repo}")
        context.log.info(f"✅ LakeFS repo '{bronze_repo}' ready")
        results["lakefs_bronze"] = "ready"
    except Exception as e:
        context.log.error(f"❌ Failed to initialize LakeFS repo '{bronze_repo}': {e}")
        results["lakefs_bronze"] = f"error: {e}"
    
    try:
        lakefs.ensure_repo(warehouse_repo, f"s3://lakefs/{warehouse_repo}")
        context.log.info(f"✅ LakeFS repo '{warehouse_repo}' ready")
        results["lakefs_warehouse"] = "ready"
    except Exception as e:
        context.log.error(f"❌ Failed to initialize LakeFS repo '{warehouse_repo}': {e}")
        results["lakefs_warehouse"] = f"error: {e}"
    
    # =========================================================================
    # 2. Create LakeFS Branches
    # =========================================================================
    context.log.info("=== Creating LakeFS Branches ===")
    
    try:
        lakefs.create_branch(bronze_repo, dev_branch)
        context.log.info(f"✅ Created LakeFS branch '{dev_branch}' in '{bronze_repo}'")
        results["lakefs_bronze_branch"] = "created"
    except Exception as e:
        context.log.warning(f"Branch '{dev_branch}' may already exist in '{bronze_repo}': {e}")
        results["lakefs_bronze_branch"] = "exists"
    
    try:
        lakefs.create_branch(warehouse_repo, dev_branch)
        context.log.info(f"✅ Created LakeFS branch '{dev_branch}' in '{warehouse_repo}'")
        results["lakefs_warehouse_branch"] = "created"
    except Exception as e:
        context.log.warning(f"Branch '{dev_branch}' may already exist in '{warehouse_repo}': {e}")
        results["lakefs_warehouse_branch"] = "exists"
    
    # =========================================================================
    # 3. Initialize Nessie Branches
    # =========================================================================
    context.log.info("=== Initializing Nessie Branches ===")
    
    try:
        nessie.create_branch(dev_branch, main_branch)
        context.log.info(f"✅ Created Nessie branch '{dev_branch}' from '{main_branch}'")
        results["nessie_dev"] = "created"
    except Exception as e:
        context.log.warning(f"Nessie branch '{dev_branch}' may already exist: {e}")
        results["nessie_dev"] = "exists"
    
    # =========================================================================
    # 4. Initialize Feast Registry Database
    # =========================================================================
    context.log.info("=== Initializing Feast Registry ===")
    
    pg_host = os.getenv("DAGSTER_POSTGRES_HOST", "postgres-dagster")
    pg_port = int(os.getenv("DAGSTER_POSTGRES_PORT", "5432"))
    pg_user = os.getenv("DAGSTER_POSTGRES_USER", "dagster")
    pg_password = os.getenv("DAGSTER_POSTGRES_PASSWORD", "dagster")
    
    try:
        # Connect to default 'dagster' database
        conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            user=pg_user,
            password=pg_password,
            database="dagster"
        )
        conn.autocommit = True
        
        with conn.cursor() as cur:
            # Check if feast_registry exists
            cur.execute("SELECT 1 FROM pg_database WHERE datname = 'feast_registry'")
            exists = cur.fetchone()
            
            if exists:
                context.log.info("✅ Feast registry database already exists")
                results["feast_registry"] = "exists"
            else:
                # Create feast_registry database
                cur.execute("CREATE DATABASE feast_registry")
                context.log.info("✅ Created feast_registry database")
                results["feast_registry"] = "created"
        
        conn.close()
        
    except Exception as e:
        context.log.error(f"❌ Failed to create feast_registry: {e}")
        results["feast_registry"] = f"error: {e}"
    
    # =========================================================================
    # 5. Initialize Trino Schemas (bronze, silver, gold, feast)
    # =========================================================================
    context.log.info("=== Initializing Trino Schemas ===")
    
    schemas_to_create = ["bronze", "silver", "gold", "feast", "staging"]
    
    for schema in schemas_to_create:
        try:
            trino.execute_ddl(f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.{schema}")
            context.log.info(f"✅ Schema {TRINO_CATALOG}.{schema} ready")
            results[f"schema_{schema}"] = "ready"
        except Exception as e:
            context.log.warning(f"Schema {TRINO_CATALOG}.{schema} may already exist: {e}")
            results[f"schema_{schema}"] = "exists"
    
    # =========================================================================
    # 6. Initialize LakeFS Webhook Tracking Schema (NEW)
    # =========================================================================
    context.log.info("=== Initializing Webhook Tracking Schema ===")
    
    try:
        # Create metadata schema
        trino.execute_ddl(f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.metadata")

        # Create lakefs_commits table
        trino.execute_ddl(f"""
            CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.metadata.lakefs_commits (
                commit_id VARCHAR,
                lakefs_repository VARCHAR,
                lakefs_branch VARCHAR,
                created_at TIMESTAMP,
                affected_bronze_tables ARRAY(ROW(table_name VARCHAR, record_count INTEGER)),
                status VARCHAR,
                reverted_at TIMESTAMP,
                cleaned_at TIMESTAMP,
                revert_reason VARCHAR,
                last_updated TIMESTAMP
            )
        """)
        context.log.info("✅ Created lakefs_commits table")

        # Create lakefs_webhook_events table
        trino.execute_ddl(f"""
            CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.metadata.lakefs_webhook_events (
                id INTEGER,
                event_id VARCHAR,
                event_type VARCHAR,
                repository VARCHAR,
                branch VARCHAR,
                commit_id VARCHAR,
                reverted_commit_id VARCHAR,
                payload VARCHAR,
                received_at TIMESTAMP,
                processed_at TIMESTAMP,
                status VARCHAR,
                error_message VARCHAR,
                retry_count INTEGER
            )
        """)
        context.log.info("✅ Created lakefs_webhook_events table")

        # Create cleanup_history table
        trino.execute_ddl(f"""
            CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.metadata.cleanup_history (
                id INTEGER,
                reverted_commit VARCHAR,
                cleanup_started TIMESTAMP,
                cleanup_completed TIMESTAMP,
                tables_cleaned ARRAY(ROW(table_name VARCHAR, records_deleted INTEGER)),
                total_records_deleted INTEGER,
                status VARCHAR,
                error_message VARCHAR,
                trigger_source VARCHAR
            )
        """)
        context.log.info("✅ Created cleanup_history table")
        
        results["tracking_schema"] = "ready"
        
    except Exception as e:
        context.log.error(f"❌ Failed to initialize tracking schema: {e}")
        results["tracking_schema"] = f"error: {e}"
    
    # =========================================================================
    # 7. Upload LakeFS Webhook Configuration (NEW)
    # =========================================================================
    context.log.info("=== Uploading LakeFS Webhook Configuration ===")
    
    try:
        # NOTE: LakeFS actions must be in _lakefs_actions/ directory for LakeFS to recognize them
        # The webhook URL must point to unified-webhook service (not lakefs-webhook-receiver)
        webhook_config = """name: dagster_webhook_integration
description: Notify Dagster on commits, merges, and reverts for rollback sync

on:
  post-commit:
    branches:
      - dev
      - main
      - experiment-*
      - feature-*
  post-merge:
    branches:
      - dev
      - main
  post-revert:
    branches:
      - dev
      - main
      - experiment-*
      - feature-*

hooks:
  - id: notify_dagster
    type: webhook
    properties:
      url: "http://unified-webhook:5000/lakefs-webhook"
      timeout: 10s
      query_params:
        source: lakefs
      headers:
        Content-Type: application/json
"""
        
        import requests
        
        # First check if webhook config already exists
        # NOTE: Must use _lakefs_actions/ path for LakeFS to recognize actions
        webhook_path = "_lakefs_actions/dagster_webhook.yaml"
        check_url = f"{lakefs.server_url}/api/v1/repositories/{bronze_repo}/refs/{dev_branch}/objects/stat"
        try:
            check_response = requests.get(
                check_url,
                params={"path": webhook_path},
                auth=(lakefs.access_key, lakefs.secret_key),
                timeout=10
            )

            if check_response.status_code == 200:
                context.log.info("✅ Webhook config already exists in LakeFS")
                results["webhook_config"] = "exists"
                # Skip to summary
            else:
                # File doesn't exist, upload it
                upload_url = f"{lakefs.server_url}/api/v1/repositories/{bronze_repo}/branches/{dev_branch}/objects"
                params = {"path": webhook_path}
                headers = {"Content-Type": "application/octet-stream"}
                
                context.log.info(f"Uploading webhook config to: {upload_url}")
                
                upload_response = requests.post(
                    upload_url,
                    params=params,
                    headers=headers,
                    auth=(lakefs.access_key, lakefs.secret_key),
                    data=webhook_config.encode('utf-8'),
                    timeout=60
                )
                
                if upload_response.status_code in [200, 201]:
                    context.log.info("✅ Uploaded webhook configuration to LakeFS")
                    
                    # Commit the webhook config
                    try:
                        lakefs.commit(
                            bronze_repo,
                            dev_branch,
                            "Initialize Dagster webhook integration",
                            {"source": "dagster_init", "component": "webhooks"}
                        )
                        context.log.info("✅ Committed webhook configuration")
                        results["webhook_config"] = "uploaded"
                    except Exception as e:
                        context.log.info(f"Webhook config already committed or no changes: {e}")
                        results["webhook_config"] = "exists"
                else:
                    error_detail = upload_response.text[:500] if upload_response.text else "No response body"
                    context.log.warning(f"Failed to upload webhook config: {upload_response.status_code}")
                    context.log.warning(f"Error detail: {error_detail}")
                    results["webhook_config"] = f"error: {upload_response.status_code}"
                    
        except Exception as e:
            context.log.warning(f"Could not check/upload webhook config: {e}")
            results["webhook_config"] = f"error: {e}"
            
    except Exception as e:
        context.log.error(f"❌ Failed to process webhook configuration: {e}")
        results["webhook_config"] = f"error: {e}"
    
    # =========================================================================
    # Summary
    # =========================================================================
    context.log.info("=== Initialization Summary ===")
    for key, value in results.items():
        context.log.info(f"{key}: {value}")
    
    return MaterializeResult(
        metadata={
            "bronze_repo": bronze_repo,
            "warehouse_repo": warehouse_repo,
            "dev_branch": dev_branch,
            "main_branch": main_branch,
            **results
        }
    )


@asset(
    group_name="mlops_optimization",
    deps=["mlops_gold_table"],
    description="Initialize Iceberg metadata tables and apply query optimizations",
)
def mlops_init_iceberg_optimizations(
    context: AssetExecutionContext,
    trino: TrinoResource,
) -> MaterializeResult:
    """
    Initialize Iceberg metadata tables and apply query optimizations.

    This asset:
    1. Creates metadata tables for snapshot tracking and rollback detection
    2. Applies sorting optimizations for faster queries

    Runs AFTER gold table is created to ensure tables exist.
    Safe to run multiple times (idempotent).
    """
    results = {}

    # =========================================================================
    # 1. Create Metadata Schema and Tables
    # =========================================================================
    context.log.info("=== Creating Iceberg Metadata Tables ===")

    trino.execute_ddl(f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.metadata")
    trino.execute_ddl(f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.monitoring")

    # Snapshot cursor table (for rollback detection)
    try:
        trino.execute_ddl(f"""
            CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.metadata.snapshot_cursor (
                table_name VARCHAR,
                last_snapshot_id BIGINT,
                last_snapshot_timestamp TIMESTAMP,
                checked_at TIMESTAMP
            ) WITH (format = 'PARQUET')
        """)
        context.log.info("✅ Created/verified snapshot_cursor table")
        results["snapshot_cursor"] = "ready"
    except Exception as e:
        context.log.warning(f"snapshot_cursor: {e}")
        results["snapshot_cursor"] = "exists"

    # Rollback events table (audit trail)
    try:
        trino.execute_ddl(f"""
            CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.metadata.iceberg_rollback_events (
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
        context.log.info("✅ Created/verified iceberg_rollback_events table")
        results["rollback_events"] = "ready"
    except Exception as e:
        context.log.warning(f"rollback_events: {e}")
        results["rollback_events"] = "exists"

    # Schema evolution audit table
    try:
        trino.execute_ddl(f"""
            CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.metadata.schema_evolution_audit (
                audit_id VARCHAR,
                table_name VARCHAR,
                change_type VARCHAR,
                column_name VARCHAR,
                old_type VARCHAR,
                new_type VARCHAR,
                changed_at TIMESTAMP,
                changed_by VARCHAR,
                contract_version VARCHAR,
                partition_date DATE
            ) WITH (
                format = 'PARQUET',
                partitioning = ARRAY['partition_date']
            )
        """)
        context.log.info("✅ Created/verified schema_evolution_audit table")
        results["schema_audit"] = "ready"
    except Exception as e:
        context.log.warning(f"schema_audit: {e}")
        results["schema_audit"] = "exists"

    # Rollback history table
    try:
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
        context.log.info("✅ Created/verified rollback_history table")
        results["rollback_history"] = "ready"
    except Exception as e:
        context.log.warning(f"rollback_history: {e}")
        results["rollback_history"] = "exists"

    # =========================================================================
    # 2. Apply Sorting Optimizations
    # =========================================================================
    context.log.info("=== Applying Sorting Optimizations ===")

    tables_to_optimize = [
        (f"{TRINO_CATALOG}.gold.fraud_transactions", "customer_id"),
        (f"{TRINO_CATALOG}.gold.fraud_training_data", "customer_id"),
        (f"{TRINO_CATALOG}.silver.fraud_transactions", "customer_id"),
        (f"{TRINO_CATALOG}.evaluation.requests", "customer_id"),
        (f"{TRINO_CATALOG}.evaluation.batch_results", "model_stage"),
        (f"{TRINO_CATALOG}.evaluation.streaming_results", "customer_id"),
    ]

    for table, sort_col in tables_to_optimize:
        try:
            trino.execute_query(f"DESCRIBE {table}")
            trino.execute_ddl(f"ALTER TABLE {table} SET PROPERTIES sorted_by = ARRAY['{sort_col}']")
            context.log.info(f"✅ Applied sorting to {table} (sorted_by: {sort_col})")
            results[f"sort_{table.split('.')[-1]}"] = "optimized"
        except Exception as e:
            error_msg = str(e).lower()
            if "does not exist" in error_msg or "not found" in error_msg:
                context.log.info(f"⏭️  Skipping {table} - table doesn't exist yet")
                results[f"sort_{table.split('.')[-1]}"] = "skipped"
            else:
                context.log.warning(f"⚠️  Could not optimize {table}: {e}")
                results[f"sort_{table.split('.')[-1]}"] = "error"

    # =========================================================================
    # Summary
    # =========================================================================
    context.log.info("=== Iceberg Optimization Summary ===")
    for key, value in results.items():
        context.log.info(f"  {key}: {value}")

    return MaterializeResult(
        metadata={
            "catalog": TRINO_CATALOG,
            **results
        }
    )


@asset(
    group_name="mlops_bronze",
    deps=["mlops_init_lakefs"],
    retry_policy=NETWORK_RETRY,
)
@track_pipeline(pipeline_name="mlops", asset_name="airbyte_sync")
def mlops_airbyte_sync(
    context: AssetExecutionContext,
    airbyte: AirbyteResource,
    lakefs: LakeFSResource,
) -> MaterializeResult:
    """Airbyte sync"""
    import time
    
    connection_id = os.getenv("AIRBYTE_CONNECTION_ID")
    bronze_repo = os.getenv("LAKEHOUSE_BRONZE_REPO", "bronze")
    dev_branch = os.getenv("LAKEHOUSE_DEV_BRANCH", "dev")
    
    if not connection_id:
        return MaterializeResult(metadata={"status": "skipped"})
    
    job = airbyte.trigger_sync(connection_id)
    job_id = job.get("jobId", "unknown")
    
    if job_id == "skipped":
        return MaterializeResult(metadata={"job_id": "skipped", "status": "skipped"})
    
    # Poll
    max_wait = 600
    elapsed = 0
    while elapsed < max_wait:
        status_resp = airbyte.get_job_status(str(job_id))
        status = status_resp.get("status", "unknown")
        
        if status in ("succeeded", "completed"):
            lakefs.commit(
                bronze_repo,
                dev_branch,
                f"Airbyte sync - job {job_id}",
                {"source": "airbyte", "job_id": str(job_id)}
            )
            return MaterializeResult(metadata={"job_id": str(job_id), "status": "completed"})
        elif status in ("failed", "cancelled"):
            raise Exception(f"Airbyte job {job_id} {status}")
        
        time.sleep(30)
        elapsed += 30
    
    raise Exception(f"Airbyte job {job_id} timed out")


# =============================================================================
# DEPRECATED: mlops_bronze_table
# This asset is replaced by mlops_bronze_ingestion in mlops_bronze.py
# which provides multi-table support and LakeFS commit tracking.
# Keeping for reference only - not registered in __init__.py
# =============================================================================
@asset(
    group_name="mlops_bronze_DEPRECATED",
    deps=["mlops_airbyte_sync"],
    retry_policy=DB_RETRY,
)
@track_pipeline(pipeline_name="mlops", asset_name="bronze_table")
def mlops_bronze_table(
    context: AssetExecutionContext,
    trino: TrinoResource,
    lakefs: LakeFSResource,
) -> MaterializeResult:
    """
    Bronze table with INCREMENTAL LOADING
    Only processes NEW files using audit table
    """
    import tempfile
    import pandas as pd
    
    bronze_table = os.getenv("BRONZE_TABLE", f"{TRINO_CATALOG}.bronze.fraud_transactions")
    bronze_repo = os.getenv("LAKEHOUSE_BRONZE_REPO", "bronze")
    dev_branch = os.getenv("LAKEHOUSE_DEV_BRANCH", "dev")

    trino.execute_ddl(f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.bronze")
    
    # Create Bronze table
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {bronze_table} (
        transaction_id VARCHAR,
        customer_id VARCHAR,
        transaction_amount DECIMAL(12, 2),
        transaction_date TIMESTAMP,
        payment_method VARCHAR,
        product_category VARCHAR,
        quantity INTEGER,
        customer_age INTEGER,
        customer_location VARCHAR,
        device_used VARCHAR,
        ip_address VARCHAR,
        shipping_address VARCHAR,
        billing_address VARCHAR,
        is_fraudulent INTEGER,
        account_age_days INTEGER,
        transaction_hour INTEGER,
        ingestion_date DATE,
        source_file VARCHAR,
        source_lakefs_commit VARCHAR
    )
    WITH (partitioning = ARRAY['ingestion_date'])
    """
    trino.execute_ddl(create_sql)
    
    # Ensure audit table
    ensure_audit_table(trino)
    
    # Get commit
    try:
        lakefs_commit = lakefs.get_commit(bronze_repo, dev_branch).get("id", "unknown")
    except:
        lakefs_commit = "unknown"
    
    # Get processed files
    processed_files = get_processed_files(trino)
    
    # Find parquet files
    paths_to_check = [
        "raw/demo/fraud_transactions/",
        "raw/fraud_transactions/",
        "demo/fraud_transactions/",
        "raw/",
    ]
    
    parquet_files = []
    for prefix in paths_to_check:
        try:
            raw_files = lakefs.list_objects(bronze_repo, dev_branch, prefix)
            prefix_parquets = [f for f in raw_files if f.endswith('.parquet')]
            if prefix_parquets:
                parquet_files.extend(prefix_parquets)
                break
        except:
            pass
    
    # Filter NEW files only
    new_files = [f for f in parquet_files if f not in processed_files]
    context.log.info(f"New files to process: {len(new_files)} out of {len(parquet_files)} total")
    
    if not new_files:
        count = trino.get_count(bronze_table)
        return MaterializeResult(metadata={"row_count": count, "new_files": 0})
    
    # Process NEW files
    all_rows = []
    for pq_file in new_files:
        local_path = tempfile.mktemp(suffix='.parquet')
        try:
            context.log.info(f"Downloading {pq_file}...")
            lakefs.download_file(bronze_repo, dev_branch, pq_file, local_path)
            df = pd.read_parquet(local_path)
            context.log.info(f"  Loaded {len(df)} rows from parquet")
            context.log.info(f"  Columns: {list(df.columns)}")
            
            for _, row in df.iterrows():
                row_values = [
                    str(row.get('transaction_id', '')) if not pd.isna(row.get('transaction_id')) else None,
                    str(row.get('customer_id', '')) if not pd.isna(row.get('customer_id')) else None,
                    float(row.get('transaction_amount', 0)) if not pd.isna(row.get('transaction_amount')) else None,
                    _parse_timestamp(row.get('transaction_date')),
                    str(row.get('payment_method', '')) if not pd.isna(row.get('payment_method')) else None,
                    str(row.get('product_category', '')) if not pd.isna(row.get('product_category')) else None,
                    int(row.get('quantity', 0)) if not pd.isna(row.get('quantity')) else None,
                    int(row.get('customer_age', 0)) if not pd.isna(row.get('customer_age')) else None,
                    str(row.get('customer_location', '')) if not pd.isna(row.get('customer_location')) else None,
                    str(row.get('device_used', '')) if not pd.isna(row.get('device_used')) else None,
                    str(row.get('ip_address', '')) if not pd.isna(row.get('ip_address')) else None,
                    str(row.get('shipping_address', '')) if not pd.isna(row.get('shipping_address')) else None,
                    str(row.get('billing_address', '')) if not pd.isna(row.get('billing_address')) else None,
                    int(row.get('is_fraudulent', 0)) if not pd.isna(row.get('is_fraudulent')) else None,
                    int(row.get('account_age_days', 0)) if not pd.isna(row.get('account_age_days')) else None,
                    int(row.get('transaction_hour', 0)) if not pd.isna(row.get('transaction_hour')) else None,
                    date.today(),
                    pq_file,
                    lakefs_commit,
                ]
                all_rows.append(tuple(row_values))
            
            context.log.info(f"  Inserting {len(df)} rows into {bronze_table}...")
            trino.insert_rows(bronze_table, all_rows[-len(df):], logger=context.log)
            mark_file_processed(trino, pq_file, lakefs_commit, len(df), "success")
            context.log.info(f"  ✓ File processed successfully")
            
        except Exception as e:
            context.log.error(f"  ✗ Failed to process {pq_file}: {e}")
            mark_file_processed(trino, pq_file, lakefs_commit, 0, f"failed: {e}")
        finally:
            if os.path.exists(local_path):
                os.remove(local_path)
    
    count = trino.get_count(bronze_table)
    
    # Log metrics
    log_data_stats(layer="bronze", table_name="fraud_transactions", row_count=count)
    
    warehouse_repo = os.getenv("LAKEHOUSE_WAREHOUSE_REPO", "warehouse")
    dev_branch = os.getenv("LAKEHOUSE_DEV_BRANCH", "dev")
    
    lakefs.commit(
        warehouse_repo,
        dev_branch,
        f"Bronze - {len(new_files)} new files, {len(all_rows)} rows",
        {"layer": "bronze", "new_files": str(len(new_files))},
    )
    
    return MaterializeResult(metadata={"row_count": count, "new_files": len(new_files)})


def _parse_timestamp(val):
    """Parse timestamp"""
    if pd.isna(val):
        return None
    if isinstance(val, (int, float)):
        from datetime import datetime
        return datetime.fromtimestamp(val)
    elif isinstance(val, str):
        from datetime import datetime
        val = val.replace('Z', '+00:00') if val.endswith('Z') else val
        try:
            return datetime.fromisoformat(val)
        except:
            return datetime.strptime(val[:19], '%Y-%m-%d %H:%M:%S')
    else:
        return val


@asset(group_name="mlops_promotion", deps=["materialize_online_features"])
@track_pipeline(pipeline_name="mlops", asset_name="promote_to_production")
def mlops_promote_to_production(
    context: AssetExecutionContext,
    lakefs: LakeFSResource,
    nessie: NessieResource,
) -> MaterializeResult:
    """Promote dev to main after model training and feature materialization"""
    bronze_repo = os.getenv("LAKEHOUSE_BRONZE_REPO", "bronze")
    warehouse_repo = os.getenv("LAKEHOUSE_WAREHOUSE_REPO", "warehouse")
    dev_branch = os.getenv("LAKEHOUSE_DEV_BRANCH", "dev")
    main_branch = os.getenv("LAKEHOUSE_MAIN_BRANCH", "main")
    
    release_id = f"release_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    
    lakefs.merge_branch(bronze_repo, dev_branch, main_branch, f"Release {release_id}")
    lakefs.merge_branch(warehouse_repo, dev_branch, main_branch, f"Release {release_id}")
    
    try:
        nessie.merge_branch(dev_branch, main_branch, f"Release {release_id}")
    except:
        pass
    
    return MaterializeResult(metadata={"release_id": release_id})


# =============================================================================
# JOBS
# =============================================================================

mlops_ingestion_job = define_asset_job(
    name="mlops_ingestion",
    selection=AssetSelection.groups("mlops_init", "mlops_bronze"),
)

mlops_promotion_job = define_asset_job(
    name="mlops_promote",
    selection=AssetSelection.groups("mlops_promotion"),
)


# =============================================================================
# EXPORTS - KEEP ORIGINAL FLOW
# Note: mlops_bronze_table is DEPRECATED - use mlops_bronze_ingestion instead
# This MLOPS_ASSETS list is NOT used by __init__.py (which has its own asset list)
# =============================================================================

from src.mlops.pipelines.mlops_jupyter import JUPYTER_ASSETS, JUPYTER_SENSORS, JUPYTER_JOBS
from src.mlops.pipelines.mlops_labelstudio import LABELSTUDIO_ASSETS, LABELSTUDIO_SENSORS, LABELSTUDIO_JOBS

MLOPS_ASSETS = [
    mlops_init_lakefs,
    mlops_airbyte_sync,
] + JUPYTER_ASSETS + LABELSTUDIO_ASSETS + [
    mlops_promote_to_production,
]

MLOPS_SENSORS = JUPYTER_SENSORS + LABELSTUDIO_SENSORS

MLOPS_JOBS = [
    mlops_ingestion_job,
    mlops_promotion_job,
] + JUPYTER_JOBS + LABELSTUDIO_JOBS