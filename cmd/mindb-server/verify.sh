#!/bin/bash
# Mindb HTTP Server - Verification Script
# Verifies that all components are present and tests pass

set -e

echo "========================================="
echo "Mindb HTTP Server - Verification"
echo "========================================="
echo ""

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Counters
PASSED=0
FAILED=0

check_file() {
    if [ -f "$1" ]; then
        echo -e "${GREEN}✓${NC} $1"
        ((PASSED++))
    else
        echo -e "${RED}✗${NC} $1 (missing)"
        ((FAILED++))
    fi
}

check_dir() {
    if [ -d "$1" ]; then
        echo -e "${GREEN}✓${NC} $1/"
        ((PASSED++))
    else
        echo -e "${RED}✗${NC} $1/ (missing)"
        ((FAILED++))
    fi
}

echo "1. Checking Source Files..."
echo "----------------------------"
check_file "main.go"
check_file "go.mod"
check_file "go.sum"
check_file "Makefile"
check_file "Dockerfile"
echo ""

echo "2. Checking Internal Packages..."
echo "--------------------------------"
check_dir "internal/api"
check_file "internal/api/types.go"
check_file "internal/api/handlers.go"
check_file "internal/api/stream.go"

check_dir "internal/config"
check_file "internal/config/config.go"

check_dir "internal/db"
check_file "internal/db/adapter.go"

check_dir "internal/lockfile"
check_file "internal/lockfile/lockfile.go"
check_file "internal/lockfile/lockfile_test.go"

check_dir "internal/middleware"
check_file "internal/middleware/auth.go"
check_file "internal/middleware/logging.go"
check_file "internal/middleware/recovery.go"

check_dir "internal/semaphore"
check_file "internal/semaphore/semaphore.go"

check_dir "internal/txmanager"
check_file "internal/txmanager/txmanager.go"
check_file "internal/txmanager/txmanager_test.go"
echo ""

echo "3. Checking Test Files..."
echo "-------------------------"
check_file "server_test.go"
echo ""

echo "4. Checking Documentation..."
echo "----------------------------"
check_file "README.md"
check_file "QUICKSTART.md"
check_file "DEPLOYMENT.md"
check_file "SUMMARY.md"
check_file "COMPLETE.md"
check_file "IMPLEMENTATION_STATUS.md"
check_file "INDEX.md"
check_file "openapi.yaml"
check_file "examples.sh"
echo ""

echo "5. Running Tests..."
echo "-------------------"
if go test ./internal/lockfile -v > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} internal/lockfile tests PASS"
    ((PASSED++))
else
    echo -e "${RED}✗${NC} internal/lockfile tests FAIL"
    ((FAILED++))
fi

if go test ./internal/txmanager -v > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} internal/txmanager tests PASS"
    ((PASSED++))
else
    echo -e "${RED}✗${NC} internal/txmanager tests FAIL"
    ((FAILED++))
fi

if go test . -v > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} server integration tests PASS"
    ((PASSED++))
else
    echo -e "${RED}✗${NC} server integration tests FAIL"
    ((FAILED++))
fi
echo ""

echo "6. Checking Build..."
echo "--------------------"
if go build -o /tmp/mindb-server-test . > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Build successful"
    ((PASSED++))
    rm -f /tmp/mindb-server-test
else
    echo -e "${RED}✗${NC} Build failed"
    ((FAILED++))
fi
echo ""

echo "========================================="
echo "Verification Summary"
echo "========================================="
echo -e "Passed: ${GREEN}$PASSED${NC}"
echo -e "Failed: ${RED}$FAILED${NC}"
echo ""

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}✓ All checks passed!${NC}"
    echo ""
    echo "Status: PRODUCTION READY ✅"
    echo ""
    echo "Next steps:"
    echo "  1. Read QUICKSTART.md to get started"
    echo "  2. Run 'make run' to start the server"
    echo "  3. Run './examples.sh' to test all endpoints"
    echo ""
    exit 0
else
    echo -e "${RED}✗ Some checks failed${NC}"
    echo ""
    echo "Please review the errors above."
    exit 1
fi
