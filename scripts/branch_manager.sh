#!/bin/bash
# Feature Branch Manager for LakeFS + Nessie + Iceberg
# Automates creating, merging, and deleting feature branches across both systems
#
# Usage:
#   ./scripts/branch_manager.sh create <branch-name>   # Create feature branch
#   ./scripts/branch_manager.sh list                   # List all branches
#   ./scripts/branch_manager.sh merge <branch-name>    # Merge branch to dev
#   ./scripts/branch_manager.sh delete <branch-name>   # Delete feature branch
#   ./scripts/branch_manager.sh status <branch-name>   # Compare branch with dev

set -e

# Configuration (override via environment variables)
LAKEFS_REPO="${LAKEFS_REPO:-warehouse}"
NESSIE_URL="${NESSIE_URL:-http://localhost:19120}"
TRINO_CATALOG_DIR="${TRINO_CATALOG_DIR:-./trino/etc/catalog}"
SOURCE_BRANCH="${SOURCE_BRANCH:-dev}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_success() { echo -e "${GREEN}$1${NC}"; }
print_warning() { echo -e "${YELLOW}$1${NC}"; }
print_error() { echo -e "${RED}$1${NC}"; }
print_info() { echo -e "${BLUE}$1${NC}"; }

# Sanitize branch name for catalog filename (replace - with _)
sanitize_name() {
    echo "$1" | tr '-' '_'
}

create_branch() {
    local BRANCH=$1

    if [ -z "$BRANCH" ]; then
        print_error "Error: Branch name required"
        echo "Usage: $0 create <branch-name>"
        exit 1
    fi

    echo ""
    print_info "Creating feature branch: $BRANCH"
    echo "  Source: $SOURCE_BRANCH"
    echo "  LakeFS Repo: $LAKEFS_REPO"
    echo ""

    # 1. LakeFS branch
    echo "Step 1/3: Creating LakeFS branch..."
    if lakectl branch create "lakefs://${LAKEFS_REPO}/${BRANCH}" \
        --source "lakefs://${LAKEFS_REPO}/${SOURCE_BRANCH}" 2>/dev/null; then
        print_success "  LakeFS branch created"
    else
        print_warning "  LakeFS branch may already exist or failed"
    fi

    # 2. Nessie branch
    echo "Step 2/3: Creating Nessie branch..."
    NESSIE_RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${NESSIE_URL}/api/v2/trees" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"BRANCH\",\"name\":\"${BRANCH}\",\"reference\":{\"type\":\"BRANCH\",\"name\":\"${SOURCE_BRANCH}\"}}")

    HTTP_CODE=$(echo "$NESSIE_RESPONSE" | tail -n1)
    if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ]; then
        print_success "  Nessie branch created"
    else
        print_warning "  Nessie branch may already exist (HTTP $HTTP_CODE)"
    fi

    # 3. Create Trino catalog
    echo "Step 3/3: Creating Trino catalog..."
    local SANITIZED_NAME=$(sanitize_name "$BRANCH")
    local CATALOG_FILE="${TRINO_CATALOG_DIR}/iceberg_${SANITIZED_NAME}.properties"

    cat > "$CATALOG_FILE" << EOF
connector.name=iceberg
iceberg.catalog.type=nessie
iceberg.nessie-catalog.uri=\${ENV:NESSIE_URL}/api/v1
iceberg.nessie-catalog.ref=${BRANCH}
iceberg.nessie-catalog.default-warehouse-dir=\${ENV:WAREHOUSE_URI}
fs.native-s3.enabled=true
s3.endpoint=\${ENV:MINIO_ENDPOINT}
s3.path-style-access=true
s3.aws-access-key=\${ENV:MINIO_ROOT_USER}
s3.aws-secret-key=\${ENV:MINIO_ROOT_PASSWORD}
s3.region=\${ENV:AWS_REGION}
EOF

    print_success "  Created catalog: $CATALOG_FILE"

    echo ""
    print_success "Feature branch '$BRANCH' created successfully!"
    echo ""
    print_warning "NEXT STEPS:"
    echo "  1. Restart Trino to load new catalog:"
    echo "     docker restart exp-trino"
    echo ""
    echo "  2. Switch Dagster pipelines to use this branch:"
    echo "     export TRINO_CATALOG=iceberg_${SANITIZED_NAME}"
    echo ""
    echo "  3. Query your branch in Trino:"
    echo "     SELECT COUNT(*) FROM iceberg_${SANITIZED_NAME}.silver.fraud_transactions;"
    echo ""
    echo "  4. Or add to .env file for persistence:"
    echo "     echo 'TRINO_CATALOG=iceberg_${SANITIZED_NAME}' >> .env"
    echo ""
}

list_branches() {
    echo ""
    print_info "=== LakeFS Branches (${LAKEFS_REPO}) ==="
    lakectl branch list "lakefs://${LAKEFS_REPO}" 2>/dev/null || echo "  (failed to list - check lakectl config)"

    echo ""
    print_info "=== Nessie Branches ==="
    curl -s "${NESSIE_URL}/api/v2/trees" 2>/dev/null | jq -r '.references[] | "  \(.name) (\(.type))"' || echo "  (failed to list - check Nessie connection)"

    echo ""
    print_info "=== Trino Catalogs ==="
    ls -1 "${TRINO_CATALOG_DIR}"/iceberg_*.properties 2>/dev/null | while read f; do
        basename "$f" .properties | sed 's/iceberg_/  /'
    done || echo "  (no iceberg catalogs found)"
    echo ""
}

merge_branch() {
    local BRANCH=$1

    if [ -z "$BRANCH" ]; then
        print_error "Error: Branch name required"
        echo "Usage: $0 merge <branch-name>"
        exit 1
    fi

    echo ""
    print_info "Merging '$BRANCH' -> '$SOURCE_BRANCH'"
    echo ""

    # 1. Nessie merge
    echo "Step 1/2: Merging in Nessie..."
    NESSIE_RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${NESSIE_URL}/api/v2/trees/${SOURCE_BRANCH}/merge" \
        -H "Content-Type: application/json" \
        -d "{\"from\":\"${BRANCH}\"}")

    HTTP_CODE=$(echo "$NESSIE_RESPONSE" | tail -n1)
    if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "204" ]; then
        print_success "  Nessie merge completed"
    else
        print_warning "  Nessie merge returned HTTP $HTTP_CODE"
        echo "  Response: $(echo "$NESSIE_RESPONSE" | head -n -1)"
    fi

    # 2. LakeFS merge
    echo "Step 2/2: Merging in LakeFS..."
    if lakectl merge "lakefs://${LAKEFS_REPO}/${BRANCH}" \
        "lakefs://${LAKEFS_REPO}/${SOURCE_BRANCH}" \
        -m "Merge ${BRANCH} to ${SOURCE_BRANCH}" 2>/dev/null; then
        print_success "  LakeFS merge completed"
    else
        print_warning "  LakeFS merge may have failed"
    fi

    echo ""
    print_success "Merge completed: $BRANCH -> $SOURCE_BRANCH"
    echo ""
    echo "Verify in Trino:"
    echo "  SELECT COUNT(*) FROM iceberg_dev.silver.fraud_transactions;"
    echo ""
}

delete_branch() {
    local BRANCH=$1

    if [ -z "$BRANCH" ]; then
        print_error "Error: Branch name required"
        echo "Usage: $0 delete <branch-name>"
        exit 1
    fi

    # Safety check
    if [ "$BRANCH" = "dev" ] || [ "$BRANCH" = "main" ]; then
        print_error "Error: Cannot delete protected branch '$BRANCH'"
        exit 1
    fi

    echo ""
    print_warning "Deleting feature branch: $BRANCH"
    echo ""

    # 1. LakeFS
    echo "Step 1/3: Deleting LakeFS branch..."
    if lakectl branch delete "lakefs://${LAKEFS_REPO}/${BRANCH}" -y 2>/dev/null; then
        print_success "  LakeFS branch deleted"
    else
        print_warning "  LakeFS branch not found or already deleted"
    fi

    # 2. Nessie
    echo "Step 2/3: Deleting Nessie branch..."
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE "${NESSIE_URL}/api/v2/trees/${BRANCH}")
    if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "204" ]; then
        print_success "  Nessie branch deleted"
    else
        print_warning "  Nessie branch not found or already deleted (HTTP $HTTP_CODE)"
    fi

    # 3. Remove catalog file
    echo "Step 3/3: Removing Trino catalog..."
    local SANITIZED_NAME=$(sanitize_name "$BRANCH")
    local CATALOG_FILE="${TRINO_CATALOG_DIR}/iceberg_${SANITIZED_NAME}.properties"

    if [ -f "$CATALOG_FILE" ]; then
        rm -f "$CATALOG_FILE"
        print_success "  Removed catalog: $CATALOG_FILE"
    else
        print_warning "  Catalog file not found: $CATALOG_FILE"
    fi

    echo ""
    print_success "Feature branch '$BRANCH' deleted"
    print_warning "Restart Trino to unload catalog: docker restart exp-trino"
    echo ""
}

status_branch() {
    local BRANCH=$1

    if [ -z "$BRANCH" ]; then
        print_error "Error: Branch name required"
        echo "Usage: $0 status <branch-name>"
        exit 1
    fi

    echo ""
    print_info "=== Branch Status: $BRANCH ==="
    echo ""

    # LakeFS status
    echo "LakeFS diff ($SOURCE_BRANCH -> $BRANCH):"
    lakectl diff "lakefs://${LAKEFS_REPO}/${SOURCE_BRANCH}" \
        "lakefs://${LAKEFS_REPO}/${BRANCH}" 2>/dev/null || echo "  (no changes or branch not found)"

    echo ""

    # Check catalog exists
    local SANITIZED_NAME=$(sanitize_name "$BRANCH")
    local CATALOG_FILE="${TRINO_CATALOG_DIR}/iceberg_${SANITIZED_NAME}.properties"

    if [ -f "$CATALOG_FILE" ]; then
        print_success "Trino catalog: iceberg_${SANITIZED_NAME} (exists)"
    else
        print_warning "Trino catalog: iceberg_${SANITIZED_NAME} (not found)"
    fi
    echo ""
}

show_help() {
    echo ""
    echo "Feature Branch Manager for LakeFS + Nessie + Iceberg"
    echo ""
    echo "Usage: $0 <command> [branch-name]"
    echo ""
    echo "Commands:"
    echo "  create <name>   Create a new feature branch (LakeFS + Nessie + Trino catalog)"
    echo "  list            List all branches in LakeFS, Nessie, and Trino catalogs"
    echo "  merge <name>    Merge feature branch back to $SOURCE_BRANCH"
    echo "  delete <name>   Delete feature branch and cleanup catalog"
    echo "  status <name>   Show diff between branch and $SOURCE_BRANCH"
    echo ""
    echo "Environment Variables:"
    echo "  LAKEFS_REPO       LakeFS repository (default: warehouse)"
    echo "  NESSIE_URL        Nessie API URL (default: http://localhost:19120)"
    echo "  SOURCE_BRANCH     Branch to fork from/merge to (default: dev)"
    echo "  TRINO_CATALOG_DIR Trino catalog directory (default: ./trino/etc/catalog)"
    echo ""
    echo "Examples:"
    echo "  $0 create experiment-v1     # Create new feature branch"
    echo "  $0 list                      # Show all branches"
    echo "  $0 status experiment-v1      # Show changes in branch"
    echo "  $0 merge experiment-v1       # Merge back to dev"
    echo "  $0 delete experiment-v1      # Cleanup after merge"
    echo ""
}

# Main
case "$1" in
    create) create_branch "$2" ;;
    list)   list_branches ;;
    merge)  merge_branch "$2" ;;
    delete) delete_branch "$2" ;;
    status) status_branch "$2" ;;
    help|--help|-h) show_help ;;
    *)
        show_help
        exit 1
        ;;
esac
