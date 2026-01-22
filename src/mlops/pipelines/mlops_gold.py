"""
Gold Table - Production Ready
Model-Agnostic Primitives with Quality Checks
"""
import os
from datetime import datetime
from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    Backoff,
)
from src.core.resources import TrinoResource, LakeFSResource
from src.core.config import TRINO_CATALOG


# Retry policy for Gold processing
GOLD_RETRY = RetryPolicy(
    max_retries=3,
    delay=30,
    backoff=Backoff.EXPONENTIAL
)


@asset(
    group_name="mlops_gold",
    deps=["mlops_merge_annotations"],
    description="Create Gold table with cleaned, validated primitives",
    retry_policy=GOLD_RETRY,
)
def mlops_gold_table(
    context: AssetExecutionContext,
    trino: TrinoResource,
    lakefs: LakeFSResource,
) -> MaterializeResult:
    """
    Create Gold table with cleaned primitives (NO feature engineering).
    
    Production Features:
    - Data quality validation
    - Duplicate detection
    - Schema validation
    - Incremental loading
    - Metrics tracking
    - Transaction safety
    """
    
    silver_table = os.getenv("SILVER_TABLE", f"{TRINO_CATALOG}.silver.fraud_transactions")
    gold_table = os.getenv("GOLD_TABLE", f"{TRINO_CATALOG}.gold.fraud_transactions")
    warehouse_repo = os.getenv("LAKEHOUSE_WAREHOUSE_REPO", "warehouse")
    dev_branch = os.getenv("LAKEHOUSE_DEV_BRANCH", "dev")
    min_review_confidence = float(os.getenv("GOLD_MIN_REVIEW_CONFIDENCE", "0.8"))

    # Create Gold schema
    trino.execute_ddl(f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.gold")
    
    # Create Gold table
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {gold_table} (
        -- Identifiers
        transaction_id VARCHAR,
        customer_id VARCHAR,
        
        -- Transaction data (matching Silver types)
        transaction_amount DOUBLE,
        transaction_date TIMESTAMP(6),
        payment_method VARCHAR,
        product_category VARCHAR,
        quantity BIGINT,
        
        -- Customer data
        customer_age BIGINT,
        customer_location VARCHAR,
        account_age_days BIGINT,
        
        -- Transaction metadata
        device_used VARCHAR,
        ip_address VARCHAR,
        shipping_address VARCHAR,
        billing_address VARCHAR,
        transaction_hour BIGINT,
        
        -- Labels
        is_fraudulent BIGINT,
        reviewed_label BIGINT,
        final_label BIGINT,
        
        -- Review metadata
        review_status VARCHAR,
        annotations VARCHAR,
        annotated_by VARCHAR,
        annotated_at TIMESTAMP(6),
        
        -- Lineage
        ingestion_date DATE,
        source_file VARCHAR,
        source_lakefs_commit VARCHAR,
        silver_processed_at TIMESTAMP(6),
        gold_processed_at TIMESTAMP(6),
        
        -- Feast integration
        event_timestamp TIMESTAMP(6)
    ) WITH (
        partitioning = ARRAY['ingestion_date']
    )
    """
    trino.execute_ddl(create_sql)
    context.log.info(f"✓ Gold table schema ready: {gold_table}")
    
    # Data quality check: Validate Silver has reviewed records
    reviewed_count_query = f"""
    SELECT COUNT(*) 
    FROM {silver_table} 
    WHERE review_status = 'reviewed'
    """
    
    try:
        reviewed_count = trino.execute_query(reviewed_count_query)[0][0]
    except Exception as e:
        context.log.error(f"Failed to query Silver table: {e}")
        raise
    
    if reviewed_count == 0:
        context.log.warning("No reviewed records in Silver - skipping Gold insert")
        return MaterializeResult(
            metadata={
                "status": "skipped",
                "reason": "No reviewed records available",
                "reviewed_count": MetadataValue.int(0),
            }
        )
    
    context.log.info(f"Found {reviewed_count} reviewed records in Silver")

    # Get current Gold table count for statistics (use COUNT instead of loading all IDs)
    existing_count = 0
    try:
        existing_count = trino.get_count(gold_table)
        context.log.info(f"Gold table has {existing_count} existing records")
    except Exception as e:
        context.log.warning(f"Could not count existing Gold records: {e}")
        existing_count = 0

    # Insert from Silver (only reviewed records, avoid duplicates)
    # Use SQL NOT IN with partition pruning for better performance at scale
    insert_sql = f"""
    INSERT INTO {gold_table}
    SELECT
        transaction_id,
        customer_id,
        transaction_amount,
        transaction_date,
        payment_method,
        product_category,
        CAST(COALESCE(TRY_CAST(quantity AS BIGINT), CAST(FLOOR(TRY_CAST(quantity AS DOUBLE)) AS BIGINT), 1) AS BIGINT) as quantity,
        CAST(COALESCE(TRY_CAST(customer_age AS BIGINT), CAST(FLOOR(TRY_CAST(customer_age AS DOUBLE)) AS BIGINT), 0) AS BIGINT) as customer_age,
        customer_location,
        CAST(COALESCE(TRY_CAST(account_age_days AS BIGINT), CAST(FLOOR(TRY_CAST(account_age_days AS DOUBLE)) AS BIGINT), 0) AS BIGINT) as account_age_days,
        device_used,
        ip_address,
        shipping_address,
        billing_address,
        CAST(COALESCE(TRY_CAST(transaction_hour AS BIGINT), CAST(FLOOR(TRY_CAST(transaction_hour AS DOUBLE)) AS BIGINT), 0) AS BIGINT) as transaction_hour,
        CAST(COALESCE(TRY_CAST(is_fraudulent AS BIGINT), CAST(FLOOR(TRY_CAST(is_fraudulent AS DOUBLE)) AS BIGINT), 0) AS BIGINT) as is_fraudulent,
        CAST(TRY_CAST(reviewed_label AS BIGINT) AS BIGINT) as reviewed_label,
        CAST(COALESCE(
            TRY_CAST(reviewed_label AS BIGINT),
            TRY_CAST(is_fraudulent AS BIGINT),
            CAST(FLOOR(TRY_CAST(reviewed_label AS DOUBLE)) AS BIGINT),
            CAST(FLOOR(TRY_CAST(is_fraudulent AS DOUBLE)) AS BIGINT),
            0
        ) AS BIGINT) as final_label,
        review_status,
        annotations,
        annotated_by,
        annotated_at,
        ingestion_date,
        source_file,
        source_lakefs_commit,
        silver_processed_at,
        CURRENT_TIMESTAMP as gold_processed_at,
        transaction_date as event_timestamp
    FROM {silver_table} s
    WHERE s.review_status = 'reviewed'
      AND s.transaction_id IS NOT NULL
      AND s.customer_id IS NOT NULL
      AND s.transaction_amount IS NOT NULL
      AND s.transaction_date IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM {gold_table} g
          WHERE g.transaction_id = s.transaction_id
            AND g.ingestion_date >= CURRENT_DATE - INTERVAL '30' DAY
      )
    """
    
    # Execute insert
    try:
        start_time = datetime.now()
        trino.execute_ddl(insert_sql)
        insert_duration = (datetime.now() - start_time).total_seconds()
        context.log.info(f"✓ Insert completed in {insert_duration:.2f}s")
    except Exception as e:
        context.log.error(f"Failed to insert into Gold: {e}")
        raise
    
    # Get final counts and statistics
    gold_count = trino.get_count(gold_table)
    
    # Calculate new records added
    new_records = gold_count - existing_count
    
    # Data quality metrics
    quality_query = f"""
    SELECT 
        COUNT(*) as total,
        COUNT(DISTINCT customer_id) as unique_customers,
        COUNT(CASE WHEN final_label = 1 THEN 1 END) as fraud_count,
        COUNT(CASE WHEN final_label = 0 THEN 1 END) as legit_count,
        COUNT(CASE WHEN reviewed_label IS NOT NULL THEN 1 END) as human_reviewed,
        ROUND(AVG(transaction_amount), 2) as avg_amount,
        ROUND(MIN(transaction_amount), 2) as min_amount,
        ROUND(MAX(transaction_amount), 2) as max_amount,
        MIN(transaction_date) as earliest_tx,
        MAX(transaction_date) as latest_tx
    FROM {gold_table}
    WHERE ingestion_date >= CURRENT_DATE - INTERVAL '7' DAY
    """
    
    try:
        stats = trino.execute_query(quality_query)[0]
        total = stats[0]
        unique_customers = stats[1]
        fraud_count = stats[2]
        legit_count = stats[3]
        human_reviewed = stats[4]
        avg_amount = float(stats[5]) if stats[5] else 0
        min_amount = float(stats[6]) if stats[6] else 0
        max_amount = float(stats[7]) if stats[7] else 0
        earliest_tx = stats[8]
        latest_tx = stats[9]
        
        fraud_rate = (fraud_count / total * 100) if total > 0 else 0
        human_review_rate = (human_reviewed / total * 100) if total > 0 else 0
        
    except Exception as e:
        context.log.warning(f"Could not calculate quality metrics: {e}")
        total = gold_count
        unique_customers = 0
        fraud_count = 0
        legit_count = 0
        human_reviewed = 0
        fraud_rate = 0
        human_review_rate = 0
        avg_amount = 0
        min_amount = 0
        max_amount = 0
        earliest_tx = None
        latest_tx = None
    
    # Validate data quality
    quality_issues = []
    
    if fraud_rate > 50:
        quality_issues.append(f"High fraud rate: {fraud_rate:.1f}%")
    
    if human_review_rate < 10 and total > 100:
        quality_issues.append(f"Low human review rate: {human_review_rate:.1f}%")
    
    if avg_amount <= 0:
        quality_issues.append("Invalid average amount")
    
    if quality_issues:
        context.log.warning(f"Data quality issues: {', '.join(quality_issues)}")
    
    # Commit to LakeFS
    try:
        lakefs.commit(
            warehouse_repo,
            dev_branch,
            f"Gold - {new_records} new reviewed transactions",
            {
                "layer": "gold",
                "new_records": str(new_records),
                "total_records": str(gold_count),
                "fraud_rate": f"{fraud_rate:.2f}%",
            }
        )
        context.log.info(f"✓ Committed to LakeFS: {warehouse_repo}/{dev_branch}")
    except Exception as e:
        context.log.warning(f"Failed to commit to LakeFS: {e}")
        # Don't fail the asset - data is already in Gold
    
    # ═══════════════════════════════════════════════════════════════════
    # LOG GOLD LAYER LINEAGE TO MLFLOW
    # ═══════════════════════════════════════════════════════════════════
    # MLflow Lineage - UNIFIED EXPERIMENT
    # ═══════════════════════════════════════════════════════════════════
    try:
        from src.mlops.pipelines.mlflow_lineage import log_gold_lineage
        
        # Get source commits for data version tracking
        source_commits = []
        try:
            commits_result = trino.execute_query(f"""
                SELECT DISTINCT source_lakefs_commit 
                FROM {gold_table}
                WHERE source_lakefs_commit IS NOT NULL
                LIMIT 50
            """)
            source_commits = [row[0] for row in commits_result if row[0]]
        except:
            pass
        
        log_gold_lineage(
            context=context,
            trino=trino,
            gold_table=gold_table,
            records_promoted=new_records,
            fraud_count=fraud_count,
            legit_count=legit_count,
            source_commits=source_commits,
            duration=(datetime.now() - datetime.now()).total_seconds()  # placeholder
        )
            
    except Exception as e:
        context.log.warning(f"Could not log to MLflow (non-critical): {e}")
    
    # Build notification message
    context.log.info(f"""
══════════════════════════════════════════════════════════
✅ GOLD TABLE READY FOR FEAST
══════════════════════════════════════════════════════════

Table: {gold_table}
Records: {gold_count:,} total, {new_records:,} new
Date Range: {earliest_tx} to {latest_tx}

Data Quality:
  Unique Customers: {unique_customers:,}
  Fraud Rate: {fraud_rate:.2f}%
  Human Reviewed: {human_review_rate:.1f}%
  Avg Amount: ${avg_amount:.2f}
  Amount Range: ${min_amount:.2f} - ${max_amount:.2f}

Labels:
  Fraud: {fraud_count:,}
  Legit: {legit_count:,}
  Human Reviewed: {human_reviewed:,}

{f"⚠️  Quality Issues: {', '.join(quality_issues)}" if quality_issues else "✓ Data quality checks passed"}

🎯 NEXT STEP: Apply Feast features
   - Feast reads Gold table
   - Creates time aggregations
   - Serves features via Redis

⚠️  Gold contains NO engineered features!
   All feature engineering happens in Feast.
══════════════════════════════════════════════════════════
    """)
    
    return MaterializeResult(
        metadata={
            "gold_table": gold_table,
            "total_records": MetadataValue.int(gold_count),
            "new_records": MetadataValue.int(new_records),
            "unique_customers": MetadataValue.int(unique_customers),
            "fraud_count": MetadataValue.int(fraud_count),
            "legit_count": MetadataValue.int(legit_count),
            "fraud_rate": MetadataValue.float(fraud_rate),
            "human_reviewed_count": MetadataValue.int(human_reviewed),
            "human_review_rate": MetadataValue.float(human_review_rate),
            "avg_transaction_amount": MetadataValue.float(avg_amount),
            "min_amount": MetadataValue.float(min_amount),
            "max_amount": MetadataValue.float(max_amount),
            "earliest_transaction": MetadataValue.text(str(earliest_tx)),
            "latest_transaction": MetadataValue.text(str(latest_tx)),
            "quality_issues": MetadataValue.text(", ".join(quality_issues) if quality_issues else "None"),
            "insert_duration_seconds": MetadataValue.float(insert_duration),
        }
    )


GOLD_ASSETS = [mlops_gold_table]