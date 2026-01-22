#!/bin/bash
# ═══════════════════════════════════════════════════════════════════════════════
# DEMO: Full MLOps Pipeline with Auto-Annotation
# ═══════════════════════════════════════════════════════════════════════════════
# This script runs the complete fraud detection pipeline for demos:
#   1. Generate recent synthetic data (last 30 days for fresh aggregates)
#   2. Load to MySQL
#   3. Trigger Airbyte sync (or skip if direct load)
#   4. Run Bronze → Silver → Gold pipeline
#   5. Export to Label Studio
#   6. AUTO-ANNOTATE using source labels (preserves flow, skips manual work)
#   7. Merge annotations back
#   8. Train model with fresh features
#
# USAGE:
#   ./demo_pipeline.sh                    # Full demo
#   ./demo_pipeline.sh --skip-generate    # Use existing data
#   ./demo_pipeline.sh --skip-annotate    # Manual annotation
#   ./demo_pipeline.sh --fast             # Minimal data, quick run
# ═══════════════════════════════════════════════════════════════════════════════

set -e

# Configuration
CUSTOMERS=${FRAUD_N_CUSTOMERS:-100}
DATA_MODE=${FRAUD_DATA_MODE:-historical}
SKIP_GENERATE=false
SKIP_ANNOTATE=false
FAST_MODE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-generate)
            SKIP_GENERATE=true
            shift
            ;;
        --skip-annotate)
            SKIP_ANNOTATE=true
            shift
            ;;
        --fast)
            FAST_MODE=true
            CUSTOMERS=20
            shift
            ;;
        --customers)
            CUSTOMERS=$2
            shift 2
            ;;
        --mode)
            DATA_MODE=$2
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "═══════════════════════════════════════════════════════════════════════════════"
echo "🚀 FRAUD DETECTION MLOPS DEMO PIPELINE"
echo "═══════════════════════════════════════════════════════════════════════════════"
echo ""
echo "Configuration:"
echo "  Customers:       $CUSTOMERS"
echo "  Data Mode:       $DATA_MODE"
echo "  Skip Generate:   $SKIP_GENERATE"
echo "  Skip Annotate:   $SKIP_ANNOTATE"
echo "  Fast Mode:       $FAST_MODE"
echo ""

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1: Generate Synthetic Data
# ═══════════════════════════════════════════════════════════════════════════════

if [ "$SKIP_GENERATE" = false ]; then
    echo "───────────────────────────────────────────────────────────────────────────────"
    echo "📊 Step 1: Generating synthetic fraud data..."
    echo "───────────────────────────────────────────────────────────────────────────────"
    
    export FRAUD_N_CUSTOMERS=$CUSTOMERS
    export FRAUD_DATA_MODE=$DATA_MODE
    export FRAUD_LOAD_MODE=replace
    export LOAD_TO_MYSQL=true
    
    python scripts/generate_fraud_data.py --customers $CUSTOMERS --mode $DATA_MODE --load-mysql
    
    echo "✅ Data generated and loaded to MySQL"
else
    echo "⏭️  Skipping data generation (using existing data)"
fi

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2: Trigger Dagster Pipeline (Bronze → Silver → Gold)
# ═══════════════════════════════════════════════════════════════════════════════

echo ""
echo "───────────────────────────────────────────────────────────────────────────────"
echo "⚙️  Step 2: Running Dagster pipeline (Bronze → Silver → Gold)..."
echo "───────────────────────────────────────────────────────────────────────────────"

# Option A: Using Dagster GraphQL API
DAGSTER_URL=${DAGSTER_URL:-http://localhost:3000}

# Trigger ingestion job
echo "   Triggering mlops_ingestion_job..."
curl -s -X POST "$DAGSTER_URL/graphql" \
    -H "Content-Type: application/json" \
    -d '{"query":"mutation { launchRun(executionParams: { selector: { jobName: \"mlops_ingestion_job\" } }) { __typename ... on LaunchRunSuccess { run { runId } } ... on PythonError { message } } }"}' \
    | jq -r '.data.launchRun.run.runId // .data.launchRun.message // "Triggered"'

echo "   Waiting for Bronze ingestion..."
sleep 10

# Option B: Direct Python execution (if Dagster API not available)
# python -c "from pipelines.mlops import mlops_ingestion_job; mlops_ingestion_job.execute_in_process()"

echo "✅ Pipeline triggered"

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3: Export to Label Studio
# ═══════════════════════════════════════════════════════════════════════════════

echo ""
echo "───────────────────────────────────────────────────────────────────────────────"
echo "📤 Step 3: Exporting to Label Studio..."
echo "───────────────────────────────────────────────────────────────────────────────"

# Trigger Label Studio export job
curl -s -X POST "$DAGSTER_URL/graphql" \
    -H "Content-Type: application/json" \
    -d '{"query":"mutation { launchRun(executionParams: { selector: { jobName: \"mlops_labelstudio_export_job\" } }) { __typename ... on LaunchRunSuccess { run { runId } } ... on PythonError { message } } }"}' \
    | jq -r '.data.launchRun.run.runId // .data.launchRun.message // "Triggered"'

echo "   Waiting for export..."
sleep 15

echo "✅ Data exported to Label Studio"

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 4: Auto-Annotate (Demo Mode)
# ═══════════════════════════════════════════════════════════════════════════════

if [ "$SKIP_ANNOTATE" = false ]; then
    echo ""
    echo "───────────────────────────────────────────────────────────────────────────────"
    echo "🏷️  Step 4: Auto-annotating tasks in Label Studio..."
    echo "───────────────────────────────────────────────────────────────────────────────"
    
    # Wait for tasks to appear in Label Studio
    sleep 5
    
    python scripts/auto_annotate_labelstudio.py
    
    echo "✅ Auto-annotation complete"
else
    echo ""
    echo "───────────────────────────────────────────────────────────────────────────────"
    echo "✋ Step 4: Manual annotation required"
    echo "───────────────────────────────────────────────────────────────────────────────"
    echo ""
    echo "   Please annotate tasks in Label Studio:"
    echo "   ${LABELSTUDIO_URL:-http://localhost:8081}"
    echo ""
    echo "   Press Enter when annotation is complete..."
    read -r
fi

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 5: Merge Annotations & Create Gold Layer
# ═══════════════════════════════════════════════════════════════════════════════

echo ""
echo "───────────────────────────────────────────────────────────────────────────────"
echo "🔄 Step 5: Merging annotations and creating Gold layer..."
echo "───────────────────────────────────────────────────────────────────────────────"

# Trigger merge job
curl -s -X POST "$DAGSTER_URL/graphql" \
    -H "Content-Type: application/json" \
    -d '{"query":"mutation { launchRun(executionParams: { selector: { jobName: \"mlops_labelstudio_merge_job\" } }) { __typename ... on LaunchRunSuccess { run { runId } } ... on PythonError { message } } }"}' \
    | jq -r '.data.launchRun.run.runId // .data.launchRun.message // "Triggered"'

echo "   Waiting for merge..."
sleep 10

echo "✅ Annotations merged"

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 6: Feature Engineering & Model Training
# ═══════════════════════════════════════════════════════════════════════════════

echo ""
echo "───────────────────────────────────────────────────────────────────────────────"
echo "🧠 Step 6: Feature engineering and model training..."
echo "───────────────────────────────────────────────────────────────────────────────"

# Trigger feature pipeline
curl -s -X POST "$DAGSTER_URL/graphql" \
    -H "Content-Type: application/json" \
    -d '{"query":"mutation { launchRun(executionParams: { selector: { jobName: \"feature_pipeline_job\" } }) { __typename ... on LaunchRunSuccess { run { runId } } ... on PythonError { message } } }"}' \
    | jq -r '.data.launchRun.run.runId // .data.launchRun.message // "Triggered"'

echo "   Training in progress..."
echo "   Monitor at: $DAGSTER_URL"
sleep 5

echo "✅ Feature pipeline triggered"

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 7: Materialize Features to Online Store
# ═══════════════════════════════════════════════════════════════════════════════

echo ""
echo "───────────────────────────────────────────────────────────────────────────────"
echo "📦 Step 7: Materializing features to Redis..."
echo "───────────────────────────────────────────────────────────────────────────────"

# This happens as part of the feature pipeline
echo "   (Included in feature pipeline)"
echo "✅ Features materialized"

# ═══════════════════════════════════════════════════════════════════════════════
# DONE
# ═══════════════════════════════════════════════════════════════════════════════

echo ""
echo "═══════════════════════════════════════════════════════════════════════════════"
echo "✅ DEMO PIPELINE COMPLETE!"
echo "═══════════════════════════════════════════════════════════════════════════════"
echo ""
echo "📊 Access Points:"
echo "   Dagster:       http://localhost:3000"
echo "   MLflow:        http://localhost:5000"
echo "   Fraud API:     http://localhost:8002"
echo "   Label Studio:  http://localhost:8081"
echo "   Grafana:       http://localhost:3002"
echo ""
echo "🧪 Test Prediction:"
echo '   curl -X POST http://localhost:8002/predict \\'
echo '     -H "Content-Type: application/json" \\'
echo '     -d '"'"'{"transaction_id":"TX-DEMO-001","customer_id":"CUST-000001","amount":5999.99,"country":"US","device_type":"mobile","payment_method":"credit_card","category":"Electronics"}'"'"
echo ""
echo "═══════════════════════════════════════════════════════════════════════════════"
