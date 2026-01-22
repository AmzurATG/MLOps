#!/bin/bash
# ═══════════════════════════════════════════════════════════════════════════════
# Automation Monitoring Script
# Shows logs and status for all rollback/revert automation components
# ═══════════════════════════════════════════════════════════════════════════════

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Default mode
MODE="${1:-status}"

# Helper function for formatted table output
print_header() {
    echo -e "\n${BLUE}╔══════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║${NC} ${BOLD}$1${NC}"
    echo -e "${BLUE}╚══════════════════════════════════════════════════════════════════╝${NC}"
}

print_section() {
    echo -e "\n${YELLOW}┌─── $1 ───${NC}"
}

print_subsection() {
    echo -e "${CYAN}│ $1${NC}"
}

print_stat() {
    printf "${GREEN}│${NC} %-30s ${BOLD}%s${NC}\n" "$1" "$2"
}

print_table_header() {
    echo -e "${MAGENTA}├──────────────────────────────────────────────────────────────────┤${NC}"
    printf "${MAGENTA}│${NC} ${BOLD}%-15s %-12s %-15s %-15s${NC} ${MAGENTA}│${NC}\n" "$1" "$2" "$3" "$4"
    echo -e "${MAGENTA}├──────────────────────────────────────────────────────────────────┤${NC}"
}

case "$MODE" in
    status)
        print_header "AUTOMATION STATUS DASHBOARD"

        # ─────────────────────────────────────────────────────────────────
        # 1. SERVICE STATUS
        # ─────────────────────────────────────────────────────────────────
        print_section "1. CORE SERVICES"

        echo -e "${CYAN}┌────────────────────────────────────────────────────────────────┐${NC}"
        printf "${CYAN}│${NC} ${BOLD}%-30s %-30s${NC} ${CYAN}│${NC}\n" "SERVICE" "STATUS"
        echo -e "${CYAN}├────────────────────────────────────────────────────────────────┤${NC}"

        # Check each service
        for svc in "unified-webhook" "exp-dagster-webserver" "exp-dagster-daemon" "exp-lakefs" "exp-trino"; do
            status=$(docker ps --filter "name=$svc" --format "{{.Status}}" 2>/dev/null || echo "NOT RUNNING")
            if [[ -z "$status" ]]; then
                status="${RED}NOT RUNNING${NC}"
            elif [[ "$status" == *"Up"* ]]; then
                status="${GREEN}$status${NC}"
            else
                status="${RED}$status${NC}"
            fi
            printf "${CYAN}│${NC} %-30s %-30b ${CYAN}│${NC}\n" "$svc" "$status"
        done
        echo -e "${CYAN}└────────────────────────────────────────────────────────────────┘${NC}"

        # ─────────────────────────────────────────────────────────────────
        # 2. TABLE COUNTS
        # ─────────────────────────────────────────────────────────────────
        print_section "2. DATA PIPELINE TABLE COUNTS"

        echo -e "${CYAN}┌────────────────────────────────────────────────────────────────┐${NC}"
        printf "${CYAN}│${NC} ${BOLD}%-20s %-15s %-25s${NC} ${CYAN}│${NC}\n" "TABLE" "ROWS" "SCHEMA"
        echo -e "${CYAN}├────────────────────────────────────────────────────────────────┤${NC}"

        # Get counts with better formatting
        docker exec exp-trino trino --execute "
        SELECT 'Bronze (fraud_transactions)' as tbl,
               CAST(COUNT(*) AS VARCHAR) as cnt,
               'iceberg_dev.bronze' as schema
        FROM iceberg_dev.bronze.fraud_transactions
        UNION ALL
        SELECT 'Silver (fraud_transactions)',
               CAST(COUNT(*) AS VARCHAR),
               'iceberg_dev.silver'
        FROM iceberg_dev.silver.fraud_transactions
        UNION ALL
        SELECT 'Gold (fraud_transactions)',
               CAST(COUNT(*) AS VARCHAR),
               'iceberg_dev.gold'
        FROM iceberg_dev.gold.fraud_transactions
        UNION ALL
        SELECT 'Gold (training_data)',
               CAST(COUNT(*) AS VARCHAR),
               'iceberg_dev.gold'
        FROM iceberg_dev.gold.fraud_training_data
        " 2>/dev/null | grep -v WARNING | grep -v "Dec" | while IFS=',' read -r tbl cnt schema; do
            # Clean up quotes
            tbl=$(echo "$tbl" | tr -d '"' | xargs)
            cnt=$(echo "$cnt" | tr -d '"' | xargs)
            schema=$(echo "$schema" | tr -d '"' | xargs)
            printf "${CYAN}│${NC} %-20s ${GREEN}%-15s${NC} %-25s ${CYAN}│${NC}\n" "$tbl" "$cnt" "$schema"
        done
        echo -e "${CYAN}└────────────────────────────────────────────────────────────────┘${NC}"

        # ─────────────────────────────────────────────────────────────────
        # 3. RECENT WEBHOOK EVENTS
        # ─────────────────────────────────────────────────────────────────
        print_section "3. RECENT WEBHOOK EVENTS (Last 5)"

        echo -e "${CYAN}┌────────────────────────────────────────────────────────────────────────────┐${NC}"
        printf "${CYAN}│${NC} ${BOLD}%-4s %-12s %-12s %-10s %-12s %-15s${NC} ${CYAN}│${NC}\n" "ID" "EVENT" "REPO" "STATUS" "TIME" "RUN_ID"
        echo -e "${CYAN}├────────────────────────────────────────────────────────────────────────────┤${NC}"

        docker exec exp-trino trino --execute "
        SELECT id,
               COALESCE(event_type, 'N/A'),
               COALESCE(repository, 'N/A'),
               COALESCE(status, 'pending'),
               COALESCE(DATE_FORMAT(created_at, '%H:%i:%s'), 'N/A'),
               COALESCE(SUBSTR(dagster_run_id, 1, 12), 'N/A')
        FROM iceberg_dev.metadata.lakefs_webhook_events
        ORDER BY created_at DESC LIMIT 5
        " 2>/dev/null | grep -v WARNING | grep -v "Dec" | while IFS=',' read -r id evt repo stat tm run_id; do
            id=$(echo "$id" | tr -d '"' | xargs)
            evt=$(echo "$evt" | tr -d '"' | xargs)
            repo=$(echo "$repo" | tr -d '"' | xargs)
            stat=$(echo "$stat" | tr -d '"' | xargs)
            tm=$(echo "$tm" | tr -d '"' | xargs)
            run_id=$(echo "$run_id" | tr -d '"' | xargs)

            # Color status
            if [[ "$stat" == "processed" ]]; then
                stat_col="${GREEN}$stat${NC}"
            elif [[ "$stat" == "failed" ]]; then
                stat_col="${RED}$stat${NC}"
            else
                stat_col="${YELLOW}$stat${NC}"
            fi

            printf "${CYAN}│${NC} %-4s %-12s %-12s %-10b %-12s %-15s ${CYAN}│${NC}\n" "$id" "$evt" "$repo" "$stat_col" "$tm" "$run_id"
        done || echo -e "${CYAN}│${NC} No webhook events found                                                    ${CYAN}│${NC}"
        echo -e "${CYAN}└────────────────────────────────────────────────────────────────────────────┘${NC}"

        # ─────────────────────────────────────────────────────────────────
        # 4. LAKEFS COMMITS TRACKING
        # ─────────────────────────────────────────────────────────────────
        print_section "4. LAKEFS COMMITS TRACKING (Last 5)"

        echo -e "${CYAN}┌────────────────────────────────────────────────────────────────────────────┐${NC}"
        printf "${CYAN}│${NC} ${BOLD}%-14s %-10s %-8s %-10s %-30s${NC} ${CYAN}│${NC}\n" "COMMIT_ID" "REPO" "BRANCH" "STATUS" "CREATED"
        echo -e "${CYAN}├────────────────────────────────────────────────────────────────────────────┤${NC}"

        docker exec exp-trino trino --execute "
        SELECT SUBSTR(commit_id, 1, 12),
               COALESCE(lakefs_repository, 'N/A'),
               COALESCE(lakefs_branch, 'N/A'),
               COALESCE(status, 'N/A'),
               COALESCE(CAST(created_at AS VARCHAR), 'N/A')
        FROM iceberg_dev.metadata.lakefs_commits
        ORDER BY created_at DESC LIMIT 5
        " 2>/dev/null | grep -v WARNING | grep -v "Dec" | while IFS=',' read -r cid repo br stat created; do
            cid=$(echo "$cid" | tr -d '"' | xargs)
            repo=$(echo "$repo" | tr -d '"' | xargs)
            br=$(echo "$br" | tr -d '"' | xargs)
            stat=$(echo "$stat" | tr -d '"' | xargs)
            created=$(echo "$created" | tr -d '"' | xargs | cut -c1-19)

            # Color status
            if [[ "$stat" == "valid" ]]; then
                stat_col="${GREEN}$stat${NC}"
            elif [[ "$stat" == "reverted" ]]; then
                stat_col="${RED}$stat${NC}"
            else
                stat_col="${YELLOW}$stat${NC}"
            fi

            printf "${CYAN}│${NC} %-14s %-10s %-8s %-10b %-30s ${CYAN}│${NC}\n" "$cid" "$repo" "$br" "$stat_col" "$created"
        done || echo -e "${CYAN}│${NC} No commits tracked                                                        ${CYAN}│${NC}"
        echo -e "${CYAN}└────────────────────────────────────────────────────────────────────────────┘${NC}"

        # ─────────────────────────────────────────────────────────────────
        # 5. ACTIVE LOCKS
        # ─────────────────────────────────────────────────────────────────
        print_section "5. ACTIVE CLEANUP LOCKS"

        lock_count=$(docker exec exp-trino trino --execute "SELECT COUNT(*) FROM iceberg_dev.metadata.cleanup_locks" 2>/dev/null | grep -v WARNING | grep -v "Dec" | tr -d '"' || echo "0")

        if [[ "$lock_count" == "0" || -z "$lock_count" ]]; then
            echo -e "${GREEN}│ No active locks - system is idle${NC}"
        else
            echo -e "${CYAN}┌────────────────────────────────────────────────────────────────┐${NC}"
            printf "${CYAN}│${NC} ${BOLD}%-35s %-15s %-15s${NC} ${CYAN}│${NC}\n" "TABLE" "OPERATION" "LOCKED_BY"
            echo -e "${CYAN}├────────────────────────────────────────────────────────────────┤${NC}"

            docker exec exp-trino trino --execute "
            SELECT table_name, operation, locked_by
            FROM iceberg_dev.metadata.cleanup_locks
            " 2>/dev/null | grep -v WARNING | grep -v "Dec" | while IFS=',' read -r tbl op by; do
                tbl=$(echo "$tbl" | tr -d '"' | xargs)
                op=$(echo "$op" | tr -d '"' | xargs)
                by=$(echo "$by" | tr -d '"' | xargs)
                printf "${CYAN}│${NC} ${YELLOW}%-35s${NC} %-15s %-15s ${CYAN}│${NC}\n" "$tbl" "$op" "$by"
            done
            echo -e "${CYAN}└────────────────────────────────────────────────────────────────┘${NC}"
        fi

        # ─────────────────────────────────────────────────────────────────
        # 6. CLEANUP HISTORY
        # ─────────────────────────────────────────────────────────────────
        print_section "6. CLEANUP HISTORY (Last 5)"

        echo -e "${CYAN}┌────────────────────────────────────────────────────────────────────────────┐${NC}"
        printf "${CYAN}│${NC} ${BOLD}%-14s %-8s %-10s %-10s %-8s %-20s${NC} ${CYAN}│${NC}\n" "COMMIT" "TABLES" "DELETED" "STATUS" "SECS" "TIMESTAMP"
        echo -e "${CYAN}├────────────────────────────────────────────────────────────────────────────┤${NC}"

        docker exec exp-trino trino --execute "
        SELECT SUBSTR(reverted_commit, 1, 12),
               CAST(tables_cleaned AS VARCHAR),
               CAST(total_records_deleted AS VARCHAR),
               status,
               CAST(duration_seconds AS VARCHAR),
               COALESCE(DATE_FORMAT(cleanup_start, '%Y-%m-%d %H:%i'), 'N/A')
        FROM iceberg_dev.metadata.cleanup_history
        ORDER BY cleanup_start DESC LIMIT 5
        " 2>/dev/null | grep -v WARNING | grep -v "Dec" | while IFS=',' read -r cid tbls del stat dur ts; do
            cid=$(echo "$cid" | tr -d '"' | xargs)
            tbls=$(echo "$tbls" | tr -d '"' | xargs)
            del=$(echo "$del" | tr -d '"' | xargs)
            stat=$(echo "$stat" | tr -d '"' | xargs)
            dur=$(echo "$dur" | tr -d '"' | xargs)
            ts=$(echo "$ts" | tr -d '"' | xargs)

            if [[ "$stat" == "success" ]]; then
                stat_col="${GREEN}$stat${NC}"
            else
                stat_col="${RED}$stat${NC}"
            fi

            printf "${CYAN}│${NC} %-14s %-8s ${GREEN}%-10s${NC} %-10b %-8s %-20s ${CYAN}│${NC}\n" "$cid" "$tbls" "$del" "$stat_col" "$dur" "$ts"
        done || echo -e "${CYAN}│${NC} No cleanup history                                                        ${CYAN}│${NC}"
        echo -e "${CYAN}└────────────────────────────────────────────────────────────────────────────┘${NC}"

        # ─────────────────────────────────────────────────────────────────
        # 7. ICEBERG SNAPSHOT STATE
        # ─────────────────────────────────────────────────────────────────
        print_section "7. ICEBERG SNAPSHOT SENSOR STATE"

        echo -e "${CYAN}┌────────────────────────────────────────────────────────────────────────────┐${NC}"
        printf "${CYAN}│${NC} ${BOLD}%-45s %-25s${NC} ${CYAN}│${NC}\n" "TABLE" "LAST_SNAPSHOT_ID"
        echo -e "${CYAN}├────────────────────────────────────────────────────────────────────────────┤${NC}"

        docker exec exp-trino trino --execute "
        SELECT table_name, CAST(snapshot_id AS VARCHAR)
        FROM iceberg_dev.metadata.iceberg_snapshot_cursor
        ORDER BY last_checked DESC
        " 2>/dev/null | grep -v WARNING | grep -v "Dec" | while IFS=',' read -r tbl snap; do
            tbl=$(echo "$tbl" | tr -d '"' | xargs)
            snap=$(echo "$snap" | tr -d '"' | xargs)
            printf "${CYAN}│${NC} %-45s ${GREEN}%-25s${NC} ${CYAN}│${NC}\n" "$tbl" "$snap"
        done || echo -e "${CYAN}│${NC} No snapshot cursor data                                                    ${CYAN}│${NC}"
        echo -e "${CYAN}└────────────────────────────────────────────────────────────────────────────┘${NC}"

        # ─────────────────────────────────────────────────────────────────
        # 8. QUICK SUMMARY
        # ─────────────────────────────────────────────────────────────────
        print_section "8. QUICK SUMMARY"

        # Get quick stats
        webhook_pending=$(docker exec exp-trino trino --execute "SELECT COUNT(*) FROM iceberg_dev.metadata.lakefs_webhook_events WHERE status = 'pending'" 2>/dev/null | grep -v WARNING | grep -v "Dec" | tr -d '"' || echo "0")
        reverted_commits=$(docker exec exp-trino trino --execute "SELECT COUNT(*) FROM iceberg_dev.metadata.lakefs_commits WHERE status = 'reverted'" 2>/dev/null | grep -v WARNING | grep -v "Dec" | tr -d '"' || echo "0")
        cleanups_today=$(docker exec exp-trino trino --execute "SELECT COUNT(*) FROM iceberg_dev.metadata.cleanup_history WHERE cleanup_start >= CURRENT_DATE" 2>/dev/null | grep -v WARNING | grep -v "Dec" | tr -d '"' || echo "0")

        echo -e "${CYAN}┌────────────────────────────────────────────────────────────────┐${NC}"
        printf "${CYAN}│${NC} Pending webhook events:     ${BOLD}%-5s${NC}                              ${CYAN}│${NC}\n" "$webhook_pending"
        printf "${CYAN}│${NC} Reverted commits tracked:  ${BOLD}%-5s${NC}                              ${CYAN}│${NC}\n" "$reverted_commits"
        printf "${CYAN}│${NC} Cleanups today:            ${BOLD}%-5s${NC}                              ${CYAN}│${NC}\n" "$cleanups_today"
        printf "${CYAN}│${NC} Active locks:              ${BOLD}%-5s${NC}                              ${CYAN}│${NC}\n" "$lock_count"
        echo -e "${CYAN}└────────────────────────────────────────────────────────────────┘${NC}"

        echo -e "\n${GREEN}TIP: Use 'dagster' mode to see run IDs for detailed logs in Dagster UI${NC}"
        echo -e "${GREEN}     Dagster UI: http://localhost:13000${NC}"
        ;;

    webhook)
        print_header "WEBHOOK CONTAINER LOGS (Live)"
        echo -e "${YELLOW}Showing logs from unified-webhook container...${NC}"
        echo -e "${YELLOW}Press Ctrl+C to stop${NC}\n"
        docker logs unified-webhook -f --tail 50 2>&1 | while read -r line; do
            # Colorize based on content
            if [[ "$line" == *"ERROR"* ]] || [[ "$line" == *"error"* ]]; then
                echo -e "${RED}$line${NC}"
            elif [[ "$line" == *"revert"* ]] || [[ "$line" == *"REVERT"* ]]; then
                echo -e "${YELLOW}$line${NC}"
            elif [[ "$line" == *"SUCCESS"* ]] || [[ "$line" == *"success"* ]] || [[ "$line" == *"Triggered"* ]]; then
                echo -e "${GREEN}$line${NC}"
            else
                echo "$line"
            fi
        done
        ;;

    dagster)
        print_header "DAGSTER DAEMON LOGS (Live)"
        echo -e "${YELLOW}Showing logs from exp-dagster-daemon container...${NC}"
        echo -e "${YELLOW}Look for RUN IDs to check in Dagster UI: http://localhost:13000${NC}"
        echo -e "${YELLOW}Press Ctrl+C to stop${NC}\n"
        docker logs exp-dagster-daemon -f --tail 100 2>&1 | while read -r line; do
            # Colorize based on content
            if [[ "$line" == *"ERROR"* ]] || [[ "$line" == *"FAILURE"* ]]; then
                echo -e "${RED}$line${NC}"
            elif [[ "$line" == *"RollbackSync"* ]]; then
                echo -e "${MAGENTA}$line${NC}"
            elif [[ "$line" == *"SUCCESS"* ]] || [[ "$line" == *"completed"* ]]; then
                echo -e "${GREEN}$line${NC}"
            elif [[ "$line" == *"Lock"* ]]; then
                echo -e "${CYAN}$line${NC}"
            elif [[ "$line" == *"sensor"* ]] || [[ "$line" == *"Sensor"* ]]; then
                echo -e "${BLUE}$line${NC}"
            else
                echo "$line"
            fi
        done
        ;;

    sync)
        print_header "ROLLBACK SYNC LOGS"
        echo -e "${YELLOW}Filtering for RollbackSync, Lock, and Sync operations...${NC}\n"

        docker logs exp-dagster-daemon 2>&1 | grep -E "RollbackSync|Lock acquired|Lock released|Gold sync|Training|Label Studio|Layered|downstream_sync|deleted" | tail -100 | while read -r line; do
            if [[ "$line" == *"Lock acquired"* ]]; then
                echo -e "${GREEN}$line${NC}"
            elif [[ "$line" == *"Lock released"* ]]; then
                echo -e "${CYAN}$line${NC}"
            elif [[ "$line" == *"deleted"* ]]; then
                echo -e "${YELLOW}$line${NC}"
            elif [[ "$line" == *"Layered"* ]]; then
                echo -e "${MAGENTA}$line${NC}"
            else
                echo "$line"
            fi
        done
        ;;

    lakefs)
        print_header "LAKEFS ACTION LOGS"
        echo -e "${YELLOW}Filtering for webhook and revert actions...${NC}\n"
        docker logs exp-lakefs 2>&1 | grep -iE "action|webhook|hook|revert|post-" | tail -50
        ;;

    sensor)
        print_header "ICEBERG SNAPSHOT SENSOR LOGS"
        echo -e "${YELLOW}Filtering for iceberg_snapshot_sensor activity...${NC}\n"

        docker logs exp-dagster-daemon 2>&1 | grep -iE "iceberg_snapshot_sensor|ROLLBACK DETECTED|snapshot|cursor" | tail -50 | while read -r line; do
            if [[ "$line" == *"ROLLBACK DETECTED"* ]]; then
                echo -e "${RED}$line${NC}"
            elif [[ "$line" == *"cursor"* ]]; then
                echo -e "${CYAN}$line${NC}"
            else
                echo "$line"
            fi
        done
        ;;

    all)
        print_header "ALL AUTOMATION LOGS (Live)"
        echo -e "${YELLOW}Streaming logs from webhook + dagster-daemon...${NC}"
        echo -e "${YELLOW}Press Ctrl+C to stop${NC}\n"

        # Use process substitution to merge logs
        (docker logs -f unified-webhook 2>&1 | sed 's/^/[WEBHOOK] /' &
         docker logs -f exp-dagster-daemon 2>&1 | sed 's/^/[DAGSTER] /') | grep -E "webhook|RollbackSync|Lock|Triggered|revert|post-|sensor" --line-buffered | while read -r line; do
            if [[ "$line" == *"[WEBHOOK]"* ]]; then
                echo -e "${BLUE}$line${NC}"
            elif [[ "$line" == *"RollbackSync"* ]]; then
                echo -e "${MAGENTA}$line${NC}"
            elif [[ "$line" == *"Lock"* ]]; then
                echo -e "${CYAN}$line${NC}"
            elif [[ "$line" == *"revert"* ]]; then
                echo -e "${YELLOW}$line${NC}"
            else
                echo "$line"
            fi
        done
        ;;

    test)
        print_header "TEST REVERT FLOW"

        echo -e "\n${YELLOW}Step 1: Recording current table counts...${NC}"
        echo -e "${CYAN}┌────────────────────────────────────────────────────────────────┐${NC}"
        docker exec exp-trino trino --execute "
        SELECT 'Bronze' as tbl, COUNT(*) as before_count FROM iceberg_dev.bronze.fraud_transactions
        UNION ALL SELECT 'Silver', COUNT(*) FROM iceberg_dev.silver.fraud_transactions
        UNION ALL SELECT 'Gold', COUNT(*) FROM iceberg_dev.gold.fraud_transactions
        " 2>/dev/null | grep -v WARNING | grep -v "Dec" | while IFS=',' read -r tbl cnt; do
            tbl=$(echo "$tbl" | tr -d '"' | xargs)
            cnt=$(echo "$cnt" | tr -d '"' | xargs)
            printf "${CYAN}│${NC} %-20s ${GREEN}%-10s${NC} rows ${CYAN}│${NC}\n" "$tbl" "$cnt"
        done
        echo -e "${CYAN}└────────────────────────────────────────────────────────────────┘${NC}"

        echo -e "\n${YELLOW}Step 2: Creating test commit and reverting...${NC}"
        python3 << 'EOF'
import requests
import datetime

LAKEFS_URL = "http://localhost:18000"
LAKEFS_USER = os.environ.get("LAKEFS_ACCESS_KEY_ID", "YOUR_LAKEFS_ACCESS_KEY_HERE")
LAKEFS_PASS = os.environ.get("LAKEFS_SECRET_ACCESS_KEY", "YOUR_LAKEFS_SECRET_KEY_HERE")

print("  Creating test file...")
resp = requests.post(
    f"{LAKEFS_URL}/api/v1/repositories/bronze/branches/main/objects",
    params={"path": f"test/monitor_test_{datetime.datetime.now().strftime('%H%M%S')}.txt"},
    files={"content": f"Test at {datetime.datetime.now()}"},
    auth=(LAKEFS_USER, LAKEFS_PASS)
)
print(f"  Upload: {'OK' if resp.status_code in [200,201] else 'FAILED'} ({resp.status_code})")

print("  Committing...")
resp = requests.post(
    f"{LAKEFS_URL}/api/v1/repositories/bronze/branches/main/commits",
    json={"message": "TEST: Monitor test commit"},
    auth=(LAKEFS_USER, LAKEFS_PASS)
)
commit_id = resp.json().get("id", "unknown")
print(f"  Commit: {'OK' if resp.status_code in [200,201] else 'FAILED'} - ID: {commit_id[:12]}...")

print("  Reverting commit...")
resp = requests.post(
    f"{LAKEFS_URL}/api/v1/repositories/bronze/branches/main/revert",
    json={"ref": commit_id, "parent_number": 1},
    auth=(LAKEFS_USER, LAKEFS_PASS)
)
print(f"  Revert: {'OK' if resp.status_code in [200,201,204] else 'FAILED'} ({resp.status_code})")
EOF

        echo -e "\n${YELLOW}Step 3: Waiting for webhook to trigger (3s)...${NC}"
        sleep 3

        echo -e "\n${YELLOW}Step 4: Checking webhook logs...${NC}"
        echo -e "${CYAN}┌────────────────────────────────────────────────────────────────┐${NC}"
        docker logs unified-webhook 2>&1 | tail -8 | while read -r line; do
            if [[ "$line" == *"Triggered"* ]] || [[ "$line" == *"success"* ]]; then
                echo -e "${CYAN}│${NC} ${GREEN}$line${NC}"
            elif [[ "$line" == *"error"* ]] || [[ "$line" == *"ERROR"* ]]; then
                echo -e "${CYAN}│${NC} ${RED}$line${NC}"
            else
                echo -e "${CYAN}│${NC} $line"
            fi
        done
        echo -e "${CYAN}└────────────────────────────────────────────────────────────────┘${NC}"

        echo -e "\n${YELLOW}Step 5: Checking Dagster job status...${NC}"
        echo -e "${CYAN}┌────────────────────────────────────────────────────────────────┐${NC}"
        docker logs exp-dagster-daemon 2>&1 | grep -E "data_sync_job|RollbackSync|Lock" | tail -5 | while read -r line; do
            echo -e "${CYAN}│${NC} $line"
        done
        echo -e "${CYAN}└────────────────────────────────────────────────────────────────┘${NC}"

        echo -e "\n${YELLOW}Step 6: Checking webhook events table...${NC}"
        docker exec exp-trino trino --execute "
        SELECT id, event_type, status, COALESCE(SUBSTR(dagster_run_id, 1, 12), 'N/A') as run_id
        FROM iceberg_dev.metadata.lakefs_webhook_events
        ORDER BY created_at DESC LIMIT 1
        " 2>/dev/null | grep -v WARNING | grep -v "Dec" | while IFS=',' read -r id evt stat run; do
            id=$(echo "$id" | tr -d '"' | xargs)
            evt=$(echo "$evt" | tr -d '"' | xargs)
            stat=$(echo "$stat" | tr -d '"' | xargs)
            run=$(echo "$run" | tr -d '"' | xargs)
            echo -e "${CYAN}│${NC} Latest event: ID=$id, Type=$evt, Status=${GREEN}$stat${NC}, RunID=$run"
        done

        echo -e "\n${GREEN}╔══════════════════════════════════════════════════════════════════╗${NC}"
        echo -e "${GREEN}║  TEST COMPLETE!                                                  ║${NC}"
        echo -e "${GREEN}║  Check Dagster UI for run details: http://localhost:13000       ║${NC}"
        echo -e "${GREEN}╚══════════════════════════════════════════════════════════════════╝${NC}"
        ;;

    runs)
        print_header "RECENT DAGSTER RUNS"
        echo -e "${YELLOW}Showing recent job runs (check Dagster UI for details)...${NC}\n"

        # Extract run IDs from logs
        echo -e "${CYAN}┌────────────────────────────────────────────────────────────────────────────┐${NC}"
        printf "${CYAN}│${NC} ${BOLD}%-40s %-30s${NC} ${CYAN}│${NC}\n" "RUN_ID" "JOB"
        echo -e "${CYAN}├────────────────────────────────────────────────────────────────────────────┤${NC}"

        docker logs exp-dagster-daemon 2>&1 | grep -oE "Run [a-f0-9-]{36}" | tail -10 | sort -u | while read -r line; do
            run_id=$(echo "$line" | sed 's/Run //')
            printf "${CYAN}│${NC} ${GREEN}%-40s${NC} %-30s ${CYAN}│${NC}\n" "$run_id" "(check UI)"
        done
        echo -e "${CYAN}└────────────────────────────────────────────────────────────────────────────┘${NC}"

        echo -e "\n${GREEN}View run details at: http://localhost:13000/runs/<run_id>${NC}"
        ;;

    *)
        print_header "AUTOMATION MONITORING HELP"
        echo -e "
${BOLD}Usage:${NC} $0 {command}

${BOLD}Commands:${NC}
  ${GREEN}status${NC}   - Show comprehensive automation status dashboard (default)
  ${GREEN}webhook${NC}  - Follow webhook container logs (live)
  ${GREEN}dagster${NC}  - Follow Dagster daemon logs (live)
  ${GREEN}sync${NC}     - Show RollbackSync operation logs
  ${GREEN}lakefs${NC}   - Show LakeFS action/webhook logs
  ${GREEN}sensor${NC}   - Show Iceberg snapshot sensor logs
  ${GREEN}runs${NC}     - Show recent Dagster run IDs
  ${GREEN}all${NC}      - Follow all automation logs live
  ${GREEN}test${NC}     - Run a test revert and show results

${BOLD}Automation Flow:${NC}
  1. LakeFS Revert → unified-webhook → data_sync_job → downstream cleanup
  2. Iceberg Rollback → iceberg_snapshot_sensor → rollback_sync_job
  3. Both → Layered protection (cleanup_reverted_commits)

${BOLD}Key URLs:${NC}
  - Dagster UI:    http://localhost:13000
  - LakeFS UI:     http://localhost:18000
  - Grafana:       http://localhost:3002

${BOLD}Examples:${NC}
  $0 status    # See current state
  $0 test      # Run a test revert
  $0 dagster   # Watch live logs
"
        exit 1
        ;;
esac
