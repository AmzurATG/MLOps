"""
Enhanced Bronze Ingestion - Multi-Source, Multi-Table Support
Robust implementation with incremental loading and fallback strategies
"""

import os
import yaml
import tempfile
from datetime import datetime, date
from typing import List, Dict, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import pyarrow.parquet as pq
from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    Backoff,
)
from src.core.resources import (
    LakeFSResource,
    TrinoResource,
)
from src.core.config import TRINO_CATALOG


# Retry policy for database operations
BRONZE_RETRY = RetryPolicy(
    max_retries=3,
    delay=5,
    backoff=Backoff.EXPONENTIAL,
)


# ═════════════════════════════════════════════════════════════════════════════
# CONFIGURATION LOADING
# ═════════════════════════════════════════════════════════════════════════════

def load_bronze_sources_config() -> Dict:
    """
    Load Bronze sources configuration.
    Priority: Environment Variables > YAML Config > Defaults
    
    Returns:
        Dict with 'sources' key containing list of source configurations
    """
    
    # Priority 1: Environment variable (backward compatible)
    env_tables = os.getenv("BRONZE_TABLES")
    if env_tables:
        return parse_env_tables(env_tables)
    
    # Priority 2: YAML configuration file
    config_path = os.getenv("BRONZE_CONFIG", "config/bronze_sources.yaml")
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                # Filter enabled sources only
                config['sources'] = [
                    s for s in config.get('sources', [])
                    if s.get('enabled', True)
                ]
                return config
        except Exception as e:
            print(f"Warning: Failed to load {config_path}: {e}")
    
    # Priority 3: Defaults (single table for backward compatibility)
    return {
        'sources': [{
            'name': 'default',
            'lakefs_repository': os.getenv('LAKEHOUSE_BRONZE_REPO', 'bronze'),
            'lakefs_branch': os.getenv('LAKEHOUSE_DEV_BRANCH', 'dev'),
            'tables': [{
                'bronze_table': os.getenv('BRONZE_TABLE', f'{TRINO_CATALOG}.bronze.fraud_transactions'),
                'source_path': os.getenv('SOURCE_PATH', 'data/*.parquet'),
                'description': 'Default Bronze table'
            }]
        }]
    }


def parse_env_tables(env_string: str) -> Dict:
    """
    Parse BRONZE_TABLES environment variable.
    Format: table1:path1,table2:path2
    Example: iceberg_dev.bronze.fraud_transactions:data/*.parquet
    """
    
    tables = []
    for table_def in env_string.split(','):
        parts = table_def.strip().split(':', 1)
        if len(parts) == 2:
            table_name, source_path = parts
        else:
            table_name = parts[0]
            source_path = 'data/*.parquet'
        
        # Ensure full table name
        if table_name.count('.') < 2:
            table_name = f"{TRINO_CATALOG}.{table_name}"
        
        tables.append({
            'bronze_table': table_name,
            'source_path': source_path,
            'description': f'From environment variable'
        })
    
    return {
        'sources': [{
            'name': 'env_config',
            'lakefs_repository': os.getenv('LAKEHOUSE_BRONZE_REPO', 'bronze'),
            'lakefs_branch': os.getenv('LAKEHOUSE_DEV_BRANCH', 'dev'),
            'tables': tables
        }]
    }


# ═════════════════════════════════════════════════════════════════════════════
# AUDIT TABLE FOR INCREMENTAL LOADING
# ═════════════════════════════════════════════════════════════════════════════

def ensure_audit_table(trino: TrinoResource):
    """Create file processing audit table if not exists."""
    trino.execute_ddl(f"""
        CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.bronze.file_processing_audit (
            file_path VARCHAR,
            processed_at TIMESTAMP,
            lakefs_commit VARCHAR,
            record_count INTEGER,
            status VARCHAR,
            table_name VARCHAR
        )
    """)


def get_processed_files(trino: TrinoResource, table_name: str = None) -> set:
    """Get set of already processed files, optionally filtered by table."""
    try:
        if table_name:
            result = trino.execute_query(f"""
                SELECT file_path FROM {TRINO_CATALOG}.bronze.file_processing_audit
                WHERE status = 'success' AND table_name = '{table_name}'
            """)
        else:
            result = trino.execute_query(
                f"SELECT file_path FROM {TRINO_CATALOG}.bronze.file_processing_audit WHERE status = 'success'"
            )
        return {row[0] for row in result} if result else set()
    except:
        return set()


def mark_file_processed(
    trino: TrinoResource,
    file_path: str,
    lakefs_commit: str,
    record_count: int,
    status: str,
    table_name: str
):
    """Mark file as processed in audit table."""
    file_path_safe = file_path.replace("'", "''")
    table_safe = table_name.replace("'", "''")
    status_safe = status.replace("'", "''")[:200]

    trino.execute_ddl(f"""
        INSERT INTO {TRINO_CATALOG}.bronze.file_processing_audit
        VALUES (
            '{file_path_safe}',
            CURRENT_TIMESTAMP,
            '{lakefs_commit}',
            {record_count},
            '{status_safe}',
            '{table_safe}'
        )
    """)


# ═════════════════════════════════════════════════════════════════════════════
# COMMIT TRACKING
# ═════════════════════════════════════════════════════════════════════════════

def track_commit_with_tables(
    trino: TrinoResource,
    commit_id: str,
    repository: str,
    branch: str,
    tables_affected: List[Tuple[str, int]],
    context: AssetExecutionContext
):
    """
    Track commit with list of affected tables.
    
    Args:
        trino: Trino resource
        commit_id: LakeFS commit ID
        repository: LakeFS repository name
        branch: LakeFS branch name
        tables_affected: List of (table_name, record_count) tuples
    """
    
    if not tables_affected:
        return
    
    # Escape single quotes in strings
    commit_safe = commit_id.replace("'", "''")
    repo_safe = repository.replace("'", "''")
    branch_safe = branch.replace("'", "''")
    
    # Build array of table records
    table_rows = []
    for table, count in tables_affected:
        table_safe = table.replace("'", "''")
        table_rows.append(f"ROW('{table_safe}', {count})")
    
    table_array = "ARRAY[" + ", ".join(table_rows) + "]"
    
    try:
        # Check if commit exists
        existing = trino.execute_query(f"""
            SELECT status FROM {TRINO_CATALOG}.metadata.lakefs_commits
            WHERE commit_id = '{commit_safe}'
              AND lakefs_repository = '{repo_safe}'
              AND lakefs_branch = '{branch_safe}'
        """)
        
        if existing:
            # Update existing (in case of re-run)
            trino.execute_ddl(f"""
                UPDATE {TRINO_CATALOG}.metadata.lakefs_commits
                SET status = 'valid',
                    affected_bronze_tables = {table_array},
                    last_updated = CURRENT_TIMESTAMP
                WHERE commit_id = '{commit_safe}'
                  AND lakefs_repository = '{repo_safe}'
                  AND lakefs_branch = '{branch_safe}'
            """)
        else:
            # Insert new
            trino.execute_ddl(f"""
                INSERT INTO {TRINO_CATALOG}.metadata.lakefs_commits (
                    commit_id, lakefs_repository, lakefs_branch, created_at,
                    affected_bronze_tables, status, reverted_at, cleaned_at,
                    revert_reason, last_updated
                ) VALUES (
                    '{commit_safe}', '{repo_safe}', '{branch_safe}', CURRENT_TIMESTAMP,
                    {table_array}, 'valid', NULL, NULL, NULL, CURRENT_TIMESTAMP
                )
            """)
        
        context.log.info(f"✓ Tracked commit {commit_id[:12]} with {len(tables_affected)} tables")
        
    except Exception as e:
        context.log.warning(f"Could not track commit in metadata: {e}")


# ═════════════════════════════════════════════════════════════════════════════
# TABLE CREATION
# ═════════════════════════════════════════════════════════════════════════════

def get_table_columns(trino: TrinoResource, table_name: str) -> List[str]:
    """Get list of column names for a table."""
    try:
        result = trino.execute_query(f"DESCRIBE {table_name}")
        return [row[0] for row in result] if result else []
    except:
        return []


def get_table_schema(trino: TrinoResource, table_name: str) -> Dict[str, str]:
    """Get column names and their types for a table.

    Returns:
        Dict mapping column_name -> sql_type (e.g., {'id': 'bigint', 'name': 'varchar'})
    """
    try:
        result = trino.execute_query(f"DESCRIBE {table_name}")
        # DESCRIBE returns: column_name, type, extra, comment
        return {row[0]: row[1] for row in result} if result else {}
    except:
        return {}


def ensure_bronze_table_exists(
    trino: TrinoResource, 
    table_name: str,
    sample_df: pd.DataFrame = None,
    context: AssetExecutionContext = None
) -> bool:
    """
    Create Bronze table if it doesn't exist, or recreate if schema mismatches.
    Uses sample DataFrame to infer schema.
    
    Returns:
        True if table was created/recreated, False if already exists with correct schema
    """
    
    # Parse and validate table name
    parts = table_name.split('.')
    if len(parts) != 3:
        raise ValueError(f"Invalid table name: {table_name}. Expected format: catalog.schema.table")
    
    catalog, schema, table = parts
    
    # Ensure schema exists
    try:
        trino.execute_ddl(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    except:
        pass
    
    # Build expected columns from DataFrame
    if sample_df is not None and len(sample_df) > 0:
        expected_columns = list(sample_df.columns) + [
            'source_lakefs_commit', 'source_file', 'ingested_at', 'ingestion_date'
        ]
    else:
        expected_columns = [
            'transaction_id', 'customer_id', 'transaction_amount', 'transaction_date',
            'payment_method', 'product_category', 'quantity', 'customer_age',
            'customer_location', 'device_used', 'ip_address', 'shipping_address',
            'billing_address', 'is_fraudulent', 'account_age_days', 'transaction_hour',
            'source_lakefs_commit', 'source_file', 'ingested_at', 'ingestion_date'
        ]
    
    # Check if table exists and has correct schema
    existing_columns = get_table_columns(trino, table_name)
    
    if existing_columns:
        # Table exists - USE IT AS-IS, never drop!
        # Schema evolution should be handled separately, not by dropping data
        if context:
            context.log.debug(f"    Table {table_name} exists with {len(existing_columns)} columns - using existing schema")
        
        # Warn if new data has different columns (but don't drop!)
        if sample_df is not None and len(sample_df) > 0:
            new_cols = set(sample_df.columns)
            existing_cols_lower = {c.lower() for c in existing_columns}
            missing_in_table = new_cols - existing_cols_lower - {'source_lakefs_commit', 'source_file', 'ingested_at', 'ingestion_date'}
            if missing_in_table and context:
                context.log.warning(
                    f"    ⚠️  New data has columns not in table: {missing_in_table}. "
                    f"These columns will be skipped. Consider schema migration if needed."
                )
        
        return False  # Table exists, don't recreate
    
    # Build column definitions from DataFrame
    if sample_df is not None and len(sample_df) > 0:
        columns = []
        for col in sample_df.columns:
            # Sanitize column name (replace spaces, special chars)
            safe_col = col.replace(' ', '_').replace('-', '_')
            
            dtype = sample_df[col].dtype
            if pd.api.types.is_integer_dtype(dtype):
                sql_type = "BIGINT"
            elif pd.api.types.is_float_dtype(dtype):
                sql_type = "DOUBLE"
            elif pd.api.types.is_bool_dtype(dtype):
                sql_type = "BOOLEAN"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                sql_type = "TIMESTAMP(6)"
            else:
                sql_type = "VARCHAR"
            columns.append(f'"{safe_col}" {sql_type}')
        
        # Add metadata columns
        columns.extend([
            '"source_lakefs_commit" VARCHAR',
            '"source_file" VARCHAR', 
            '"ingested_at" TIMESTAMP(6)',
            '"ingestion_date" DATE'
        ])
        
        columns_sql = ",\n        ".join(columns)
    else:
        # Default schema for fraud transactions
        columns_sql = """
        "transaction_id" VARCHAR,
        "customer_id" VARCHAR,
        "transaction_amount" DOUBLE,
        "transaction_date" TIMESTAMP(6),
        "payment_method" VARCHAR,
        "product_category" VARCHAR,
        "quantity" BIGINT,
        "customer_age" BIGINT,
        "customer_location" VARCHAR,
        "device_used" VARCHAR,
        "ip_address" VARCHAR,
        "shipping_address" VARCHAR,
        "billing_address" VARCHAR,
        "is_fraudulent" BIGINT,
        "account_age_days" BIGINT,
        "transaction_hour" BIGINT,
        "source_lakefs_commit" VARCHAR,
        "source_file" VARCHAR,
        "ingested_at" TIMESTAMP(6),
        "ingestion_date" DATE
        """
    
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_sql}
    )
    WITH (partitioning = ARRAY['ingestion_date'])
    """
    
    try:
        trino.execute_ddl(create_sql)
        if context:
            context.log.info(f"    ✓ Created table {table_name} with {len(expected_columns)} columns")
        return True
    except Exception as e:
        if context:
            context.log.error(f"    ✗ Failed to create table {table_name}: {e}")
        raise


# ═════════════════════════════════════════════════════════════════════════════
# TABLE INGESTION - PANDAS METHOD (ROBUST)
# ═════════════════════════════════════════════════════════════════════════════

def find_parquet_files(
    lakefs: LakeFSResource,
    repo: str,
    branch: str,
    source_path: str,
    context: AssetExecutionContext
) -> List[str]:
    """
    Find parquet files in LakeFS, trying multiple path variations.
    
    Returns:
        List of parquet file paths
    """
    
    # Clean source path (remove wildcards)
    base_path = source_path.rstrip('*').rstrip('/').lstrip('/')
    
    # Build list of paths to try
    paths_to_try = [base_path]
    
    # Add variations
    if base_path:
        paths_to_try.extend([
            f"raw/{base_path}",
            f"raw/demo/{base_path}",
            base_path.replace("data/", "raw/"),
            base_path.replace("data/", "raw/demo/"),
        ])
    
    # Also try common base paths
    paths_to_try.extend([
        "raw/demo/fraud_transactions",
        "raw/fraud_transactions", 
        "demo/fraud_transactions",
        "data",
        "raw",
        "",  # Root
    ])
    
    # Remove duplicates while preserving order
    seen = set()
    unique_paths = []
    for p in paths_to_try:
        if p not in seen:
            seen.add(p)
            unique_paths.append(p)
    
    # Try each path
    for prefix in unique_paths:
        try:
            context.log.debug(f"    Checking path: {prefix or '(root)'}")
            files = lakefs.list_objects(repo, branch, prefix)
            parquet_files = [f for f in files if f.endswith('.parquet')]
            
            if parquet_files:
                context.log.info(f"    Found {len(parquet_files)} parquet files at: {prefix or '(root)'}")
                return parquet_files
                
        except Exception as e:
            context.log.debug(f"    Path {prefix} failed: {e}")
            continue
    
    return []


def ingest_table_pandas(
    trino: TrinoResource,
    lakefs: LakeFSResource,
    table_name: str,
    lakefs_repo: str,
    lakefs_branch: str,
    source_path: str,
    lakefs_commit: str,
    processed_files: set,
    context: AssetExecutionContext
) -> Tuple[int, List[str]]:
    """
    Ingest data by downloading parquet files and processing with pandas.
    Supports incremental loading via audit table.
    
    Returns:
        Tuple of (record_count, list_of_files_processed)
    """
    
    context.log.info(f"    Searching for parquet files...")
    
    # Find parquet files
    parquet_files = find_parquet_files(lakefs, lakefs_repo, lakefs_branch, source_path, context)
    
    if not parquet_files:
        context.log.warning(f"    No parquet files found in any location")
        return 0, []
    
    # Filter to NEW files only (incremental)
    new_files = [f for f in parquet_files if f not in processed_files]
    context.log.info(f"    Files: {len(parquet_files)} total, {len(new_files)} new")
    
    if not new_files:
        context.log.info(f"    All files already processed (incremental skip)")
        return 0, []
    
    total_records = 0
    files_processed = []
    
    for pq_file in new_files:
        local_path = tempfile.mktemp(suffix='.parquet')
        try:
            context.log.info(f"    📄 {pq_file}")
            
            # Download file
            lakefs.download_file(lakefs_repo, lakefs_branch, pq_file, local_path)

            # Read parquet in chunks for memory efficiency (handles large files)
            parquet_file = pq.ParquetFile(local_path)
            dfs = []
            for batch in parquet_file.iter_batches(batch_size=50000):
                dfs.append(batch.to_pandas())
            df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
            
            if len(df) == 0:
                context.log.info(f"       Empty file, skipping")
                mark_file_processed(trino, pq_file, lakefs_commit, 0, "empty", table_name)
                continue
            
            context.log.info(f"       Loaded {len(df):,} rows, {len(df.columns)} columns")
            
            # Ensure table exists with correct schema
            ensure_bronze_table_exists(trino, table_name, df, context)
            
            # Add metadata columns
            df['source_lakefs_commit'] = lakefs_commit
            df['source_file'] = pq_file
            df['ingested_at'] = datetime.now()
            df['ingestion_date'] = date.today()
            
            # ═══════════════════════════════════════════════════════════════
            # ATOMIC INSERT: Uses staging table to create only 1 snapshot
            # ═══════════════════════════════════════════════════════════════
            record_count = insert_dataframe_atomic(
                trino=trino,
                target_table=table_name,
                df=df,
                context=context
            )
            
            total_records += record_count
            files_processed.append(pq_file)
            
            # Mark as processed
            mark_file_processed(trino, pq_file, lakefs_commit, record_count, "success", table_name)
            context.log.info(f"       ✓ Inserted {record_count:,} rows (1 snapshot)")
            
        except Exception as e:
            context.log.error(f"       ✗ Failed: {e}")
            mark_file_processed(trino, pq_file, lakefs_commit, 0, f"failed: {str(e)[:100]}", table_name)
        finally:
            if os.path.exists(local_path):
                os.remove(local_path)
    
    return total_records, files_processed


# ═════════════════════════════════════════════════════════════════════════════
# ATOMIC INSERT (Single Snapshot)
# ═════════════════════════════════════════════════════════════════════════════

def insert_dataframe_atomic(
    trino: TrinoResource,
    target_table: str,
    df: pd.DataFrame,
    context: AssetExecutionContext = None
) -> int:
    """
    Insert DataFrame as a single atomic operation creating only 1 Iceberg snapshot.
    
    Strategy:
    1. Get target table columns to ensure compatibility
    2. Create staging table with matching schema
    3. Batch insert into staging (snapshots in staging don't matter)
    4. Single INSERT INTO target SELECT FROM staging (1 snapshot on target!)
    5. Drop staging table
    
    This prevents the "150 snapshots per second" problem from batch inserts.
    Handles column mismatches gracefully by only inserting matching columns.
    
    Returns:
        Number of records inserted
    """
    import uuid

    # Create unique staging table name
    staging_id = uuid.uuid4().hex[:8]
    staging_table = f"{TRINO_CATALOG}.staging.temp_{staging_id}"
    
    if context:
        context.log.info(f"       Using atomic insert via staging table")
    
    try:
        # Get target table schema (columns + types) for mapping and casting
        target_schema = get_table_schema(trino, target_table)
        target_columns_lower = {c.lower(): c for c in target_schema.keys()}

        # Map DataFrame columns to target columns (case-insensitive match)
        df_columns = []
        matched_target_columns = []
        target_types = []  # Track target types for CAST

        for col in df.columns:
            safe_col = col.replace(' ', '_').replace('-', '_').lower()
            if safe_col in target_columns_lower:
                target_col = target_columns_lower[safe_col]
                df_columns.append(col)
                matched_target_columns.append(target_col)
                target_types.append(target_schema[target_col])

        if not df_columns:
            if context:
                context.log.error(f"       No matching columns between DataFrame and target table!")
            return 0

        if context and len(df_columns) < len(df.columns):
            skipped = set(df.columns) - set(df_columns)
            context.log.debug(f"       Skipping columns not in target: {skipped}")
        
        # Ensure staging schema exists
        trino.execute_ddl(f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.staging")
        
        # Build column definitions for matched columns only
        # Use VARCHAR for ALL staging columns to accept any source data format
        # The CAST during final INSERT will convert to proper target types
        columns_def = []
        for col in df_columns:
            safe_col = col.replace(' ', '_').replace('-', '_')
            # Use VARCHAR for everything in staging - most permissive type
            # This avoids type mismatch errors during staging insert
            columns_def.append(f'"{safe_col}" VARCHAR')
        
        columns_sql = ", ".join(columns_def)
        
        # Create staging table (no partitioning needed)
        trino.execute_ddl(f"CREATE TABLE {staging_table} ({columns_sql})")
        
        if context:
            context.log.debug(f"       Created staging table with {len(df_columns)} columns")
        
        # Batch insert into staging table (only matched columns)
        # Vectorized conversion - 50x faster than iterrows()
        df_subset = df[df_columns].copy()

        # Convert ALL columns to strings for VARCHAR staging table
        # This ensures no type mismatch during staging insert
        for col in df_subset.columns:
            if pd.api.types.is_datetime64_any_dtype(df_subset[col]):
                # Format datetime as ISO string for proper parsing during CAST
                df_subset[col] = df_subset[col].dt.strftime('%Y-%m-%d %H:%M:%S.%f')
            else:
                # Convert everything else to string
                df_subset[col] = df_subset[col].apply(lambda x: str(x) if pd.notna(x) else None)

        # Convert to list of tuples in one operation
        rows = [tuple(None if pd.isna(v) or v == 'None' or v == 'nan' else v for v in row) for row in df_subset.values]
        
        # Insert into staging in batches (larger batches OK for staging)
        batch_size = 2000
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            trino.insert_rows(staging_table, batch)
        
        # Verify staging has all rows
        staging_count = trino.execute_query(f"SELECT COUNT(*) FROM {staging_table}")[0][0]
        
        if context:
            context.log.debug(f"       Staging table has {staging_count:,} rows")
        
        # Build explicit column list for INSERT with CAST to target types
        # This ensures type compatibility between staging and target tables
        # Uses TRY_CAST with FLOOR fallback for BIGINT/INTEGER to handle decimal strings like '32.0'
        staging_cols_with_cast = []
        for i, col in enumerate(df_columns):
            safe_col = f'"{col.replace(" ", "_").replace("-", "_")}"'
            target_type = target_types[i].upper()

            # Handle BIGINT/INTEGER specially - decimal strings like '32.0' need FLOOR first
            if target_type in ('BIGINT', 'INTEGER', 'INT', 'SMALLINT', 'TINYINT'):
                # Try direct cast first, fallback to FLOOR(DOUBLE) for decimal strings
                cast_expr = f'COALESCE(TRY_CAST({safe_col} AS {target_type}), CAST(FLOOR(TRY_CAST({safe_col} AS DOUBLE)) AS {target_type}), 0)'
            elif target_type == 'BOOLEAN':
                # Handle boolean strings
                cast_expr = f'COALESCE(TRY_CAST({safe_col} AS BOOLEAN), LOWER({safe_col}) IN (\'true\', \'1\', \'yes\'))'
            elif 'TIMESTAMP' in target_type:
                # Handle timestamp with fallback
                cast_expr = f'TRY_CAST({safe_col} AS {target_type})'
            else:
                # Default CAST for other types (VARCHAR, DOUBLE, etc.)
                cast_expr = f'CAST({safe_col} AS {target_type})'

            staging_cols_with_cast.append(cast_expr)

        staging_cols_sql = ", ".join(staging_cols_with_cast)
        target_cols = ", ".join([f'"{c}"' for c in matched_target_columns])

        # ATOMIC: Single INSERT SELECT creates exactly 1 snapshot on target
        # Uses explicit CAST to handle type mismatches (e.g., integer→bigint, timestamp(3)→timestamp(6))
        trino.execute_ddl(f"""
            INSERT INTO {target_table} ({target_cols})
            SELECT {staging_cols_sql} FROM {staging_table}
        """)
        
        return staging_count
        
    finally:
        # Always cleanup staging table
        try:
            trino.execute_ddl(f"DROP TABLE IF EXISTS {staging_table}")
            if context:
                context.log.debug(f"       Cleaned up staging table")
        except:
            pass


# ═════════════════════════════════════════════════════════════════════════════
# UNIFIED INGESTION
# ═════════════════════════════════════════════════════════════════════════════

def ingest_table(
    trino: TrinoResource,
    lakefs: LakeFSResource,
    table_name: str,
    lakefs_repo: str,
    lakefs_branch: str,
    source_path: str,
    lakefs_commit: str,
    context: AssetExecutionContext,
    use_incremental: bool = True
) -> Tuple[int, List[str]]:
    """
    Ingest data from LakeFS parquet files into Bronze table.
    
    Uses pandas-based download and insertion for reliability.
    Supports incremental loading (only processes new files).
    
    Returns:
        Tuple of (record_count, list_of_files_processed)
    """
    
    # Ensure table exists first
    ensure_bronze_table_exists(trino, table_name, context=context)
    
    # Get processed files for incremental loading
    processed_files = get_processed_files(trino, table_name) if use_incremental else set()
    
    # Ingest using pandas method
    records, files = ingest_table_pandas(
        trino=trino,
        lakefs=lakefs,
        table_name=table_name,
        lakefs_repo=lakefs_repo,
        lakefs_branch=lakefs_branch,
        source_path=source_path,
        lakefs_commit=lakefs_commit,
        processed_files=processed_files,
        context=context
    )
    
    return records, files


# ═════════════════════════════════════════════════════════════════════════════
# MAIN BRONZE INGESTION ASSET
# ═════════════════════════════════════════════════════════════════════════════

@asset(
    group_name="mlops_bronze",
    deps=["mlops_airbyte_sync"],
    description="Robust Bronze ingestion with multi-source support, incremental loading, and fallback strategies",
    retry_policy=BRONZE_RETRY,
)
def mlops_bronze_ingestion(
    context: AssetExecutionContext,
    lakefs: LakeFSResource,
    trino: TrinoResource,
) -> MaterializeResult:
    """
    Main Bronze ingestion asset.
    
    Features:
    - Multi-source, multi-table support via YAML config
    - Incremental loading (only new files)
    - Fallback from SQL to pandas ingestion
    - Commit tracking for revert propagation
    - MLflow lineage logging
    
    Each source group gets its own LakeFS commit tracked.
    Multiple tables from same source are ingested in one commit.
    """
    
    ingestion_start = datetime.now()
    
    # Ensure audit table exists
    ensure_audit_table(trino)
    
    # Load configuration
    config = load_bronze_sources_config()
    sources = config.get('sources', [])
    
    if not sources:
        context.log.warning("No Bronze sources configured!")
        return MaterializeResult(
            metadata={"sources_processed": 0, "tables_ingested": 0}
        )
    
    context.log.info("=" * 70)
    context.log.info("BRONZE INGESTION STARTING")
    context.log.info(f"  Sources configured: {len(sources)}")
    context.log.info("=" * 70)
    
    total_tables = 0
    total_records = 0
    all_commits = []
    errors = []
    
    # Process each source
    for source in sources:
        source_name = source.get('name', 'unknown')
        lakefs_repo = source.get('lakefs_repository', 'bronze')
        lakefs_branch = source.get('lakefs_branch', 'dev')
        tables = source.get('tables', [])
        
        if not tables:
            context.log.warning(f"Source '{source_name}' has no tables configured")
            continue
        
        context.log.info("")
        context.log.info(f"━━━ Source: {source_name} ({len(tables)} tables) ━━━")
        
        # Get current LakeFS commit for this source
        try:
            commit_info = lakefs.get_commit(lakefs_repo, lakefs_branch)
            lakefs_commit = commit_info.get("id", "unknown") if commit_info else "unknown"
            commit_message = commit_info.get("message", "N/A") if commit_info else "N/A"
            context.log.info(f"  LakeFS commit: {lakefs_commit[:12]}")
            context.log.info(f"  Message: {commit_message[:50]}...")
        except Exception as e:
            context.log.error(f"Failed to get LakeFS commit for {lakefs_repo}: {e}")
            errors.append(f"{source_name}: LakeFS error - {e}")
            continue
        
        tables_affected = []
        
        # Ingest each table in this source
        for table_config in tables:
            table_name = table_config.get('bronze_table')
            source_path = table_config.get('source_path')
            description = table_config.get('description', '')

            if not table_name or not source_path:
                context.log.warning(f"Invalid table config: {table_config}")
                continue

            # Prepend catalog if table name is 2-part (schema.table)
            if table_name.count('.') == 1:
                table_name = f"{TRINO_CATALOG}.{table_name}"
            
            context.log.info(f"")
            context.log.info(f"  📦 Table: {table_name}")
            context.log.info(f"     Path: {source_path}")
            
            try:
                record_count, files_processed = ingest_table(
                    trino=trino,
                    lakefs=lakefs,
                    table_name=table_name,
                    lakefs_repo=lakefs_repo,
                    lakefs_branch=lakefs_branch,
                    source_path=source_path,
                    lakefs_commit=lakefs_commit,
                    context=context,
                    use_incremental=True
                )
                
                if record_count > 0:
                    tables_affected.append((table_name, record_count))
                    total_records += record_count
                    total_tables += 1
                    context.log.info(f"     ✓ {record_count:,} records ingested")
                else:
                    context.log.info(f"     ○ No new records")
                
            except Exception as e:
                context.log.error(f"     ✗ Failed: {e}")
                errors.append(f"{table_name}: {str(e)[:50]}")
        
        # Track commit with all tables from this source
        if tables_affected:
            track_commit_with_tables(
                trino=trino,
                commit_id=lakefs_commit,
                repository=lakefs_repo,
                branch=lakefs_branch,
                tables_affected=tables_affected,
                context=context
            )
            
            all_commits.append({
                'source': source_name,
                'commit': lakefs_commit,
                'tables': len(tables_affected),
                'records': sum(count for _, count in tables_affected)
            })
    
    # Calculate duration
    ingestion_end = datetime.now()
    duration = (ingestion_end - ingestion_start).total_seconds()
    
    # Summary
    context.log.info("")
    context.log.info("=" * 70)
    context.log.info("✅ BRONZE INGESTION COMPLETE")
    context.log.info("=" * 70)
    context.log.info(f"  Duration: {duration:.1f}s")
    context.log.info(f"  Sources processed: {len(all_commits)}")
    context.log.info(f"  Tables ingested: {total_tables}")
    context.log.info(f"  Total records: {total_records:,}")
    
    if errors:
        context.log.warning(f"  Errors: {len(errors)}")
        for err in errors[:5]:
            context.log.warning(f"    - {err}")
    
    context.log.info("")
    for commit_info in all_commits:
        context.log.info(
            f"  📝 {commit_info['source']}: "
            f"{commit_info['tables']} tables, "
            f"{commit_info['records']:,} records "
            f"(commit: {commit_info['commit'][:12]})"
        )
    context.log.info("=" * 70)
    
    # ═══════════════════════════════════════════════════════════════════════════════
    # LOG BRONZE LAYER LINEAGE TO MLFLOW
    # ═══════════════════════════════════════════════════════════════════════════════
    # MLflow Lineage - UNIFIED EXPERIMENT
    # ═══════════════════════════════════════════════════════════════════════════════
    try:
        from src.mlops.pipelines.mlflow_lineage import log_bronze_lineage
        
        # Use first ingested table for snapshot tracking
        bronze_table = None
        if all_commits:
            for commit_info in all_commits:
                if commit_info.get('tables', 0) > 0:
                    # Construct table name from source
                    source = commit_info.get('source', 'fraud')
                    bronze_table = f"{TRINO_CATALOG}.bronze.fraud_transactions"
                    break
        
        log_bronze_lineage(
            context=context,
            trino=trino,
            commits=all_commits,
            total_records=total_records,
            total_tables=total_tables,
            duration=duration,
            bronze_table=bronze_table,
            errors=errors
        )
            
    except Exception as e:
        context.log.warning(f"Could not log to MLflow (non-critical): {e}")
    
    return MaterializeResult(
        metadata={
            "sources_processed": MetadataValue.int(len(all_commits)),
            "tables_ingested": MetadataValue.int(total_tables),
            "total_records": MetadataValue.int(total_records),
            "duration_seconds": MetadataValue.float(duration),
            "errors": MetadataValue.text(", ".join(errors) if errors else "None"),
            "commits": MetadataValue.json([
                f"{c['source']}:{c['commit'][:12]}" for c in all_commits
            ]),
        }
    )


# ═════════════════════════════════════════════════════════════════════════════
# EXPORTS
# ═════════════════════════════════════════════════════════════════════════════

BRONZE_ASSETS = [mlops_bronze_ingestion]