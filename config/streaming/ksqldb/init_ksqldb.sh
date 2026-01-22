#!/bin/bash
# =============================================================================
# ksqlDB Initialization Script
# Initializes streaming feature computation for fraud detection
# =============================================================================

set -e

KSQL_SERVER=${KSQL_SERVER:-http://exp-ksqldb-server:8088}
MAX_RETRIES=30
RETRY_INTERVAL=5

echo "=============================================="
echo "ksqlDB Feature Pipeline Initialization"
echo "=============================================="
echo "Server: $KSQL_SERVER"
echo ""

# Wait for ksqlDB server to be healthy
echo "[1/4] Waiting for ksqlDB server to be ready..."
retries=0
until curl -s "$KSQL_SERVER/healthcheck" 2>/dev/null | grep -q '"isHealthy":true'; do
    retries=$((retries + 1))
    if [ $retries -ge $MAX_RETRIES ]; then
        echo "ERROR: ksqlDB server not ready after $MAX_RETRIES attempts"
        exit 1
    fi
    echo "  Waiting... (attempt $retries/$MAX_RETRIES)"
    sleep $RETRY_INTERVAL
done
echo "  ksqlDB server is ready!"
echo ""

# Function to execute ksqlDB statements
execute_ksql() {
    local sql_file=$1
    local description=$2

    echo "Executing: $description"
    echo "  File: $sql_file"

    # Read SQL file and escape for JSON
    local sql_content
    sql_content=$(cat "$sql_file" | tr '\n' ' ' | sed 's/"/\\"/g')

    # Execute via ksqlDB REST API
    local response
    response=$(curl -s -X POST "$KSQL_SERVER/ksql" \
        -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
        -d "{
            \"ksql\": \"$sql_content\",
            \"streamsProperties\": {
                \"ksql.streams.auto.offset.reset\": \"earliest\"
            }
        }" 2>&1)

    # Check for errors
    if echo "$response" | grep -q '"@type":"currentStatus"'; then
        echo "  Status: SUCCESS"
    elif echo "$response" | grep -q '"@type":"sourceDescription"'; then
        echo "  Status: SUCCESS (object already exists)"
    elif echo "$response" | grep -q 'errorMessage'; then
        local error_msg
        error_msg=$(echo "$response" | grep -o '"message":"[^"]*"' | head -1)
        if echo "$error_msg" | grep -q "already exists"; then
            echo "  Status: SKIPPED (already exists)"
        else
            echo "  Status: WARNING"
            echo "  Response: $error_msg"
        fi
    else
        echo "  Status: COMPLETED"
    fi
    echo ""
}

# Execute SQL files in order
echo "[2/4] Creating source streams and enrichments..."
execute_ksql "/ksqldb/01_streams.sql" "Source Streams & Enrichments"

echo "[3/4] Creating windowed aggregation tables..."
execute_ksql "/ksqldb/02_aggregations.sql" "Velocity Aggregation Tables"

echo "[4/4] Creating feature output stream..."
execute_ksql "/ksqldb/03_feature_output.sql" "Feature Output Stream"

# Verify streams and tables
echo "=============================================="
echo "Verification"
echo "=============================================="
echo ""

echo "Checking created streams..."
curl -s "$KSQL_SERVER/ksql" \
    -H "Content-Type: application/vnd.ksql.v1+json" \
    -d '{"ksql": "SHOW STREAMS;"}' | \
    grep -o '"name":"[^"]*"' | \
    sed 's/"name":"//g' | sed 's/"//g' | \
    while read stream; do
        echo "  - Stream: $stream"
    done

echo ""
echo "Checking created tables..."
curl -s "$KSQL_SERVER/ksql" \
    -H "Content-Type: application/vnd.ksql.v1+json" \
    -d '{"ksql": "SHOW TABLES;"}' | \
    grep -o '"name":"[^"]*"' | \
    sed 's/"name":"//g' | sed 's/"//g' | \
    while read table; do
        echo "  - Table: $table"
    done

echo ""
echo "=============================================="
echo "ksqlDB initialization complete!"
echo "=============================================="
echo ""
echo "Features will be published to: fraud.streaming.features"
echo "Use ksqlDB CLI to monitor:"
echo "  docker exec -it exp-ksqldb-cli ksql http://exp-ksqldb-server:8088"
echo "  ksql> SELECT * FROM FRAUD_STREAMING_FEATURES EMIT CHANGES LIMIT 5;"
echo ""
