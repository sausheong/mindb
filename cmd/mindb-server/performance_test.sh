#!/bin/bash
# Performance Testing Script for Mindb HTTP Server
#
# This script runs various performance tests against a running mindb-server instance
# and generates a performance report.

set -e

BASE_URL="${BASE_URL:-http://localhost:8080}"
DATABASE="perftest"
RESULTS_DIR="./performance_results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
REPORT_FILE="$RESULTS_DIR/perf_report_$TIMESTAMP.txt"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}Mindb HTTP Server - Performance Testing${NC}"
echo -e "${BLUE}=========================================${NC}"
echo ""

# Create results directory
mkdir -p "$RESULTS_DIR"

# Initialize report
{
    echo "Mindb HTTP Server - Performance Test Report"
    echo "==========================================="
    echo "Date: $(date)"
    echo "Server: $BASE_URL"
    echo ""
} > "$REPORT_FILE"

# Function to measure operation time
measure_time() {
    local operation=$1
    local command=$2
    local iterations=${3:-100}
    
    echo -e "${YELLOW}Testing: $operation${NC}"
    
    local start=$(date +%s%N)
    for ((i=1; i<=iterations; i++)); do
        eval "$command" > /dev/null 2>&1
    done
    local end=$(date +%s%N)
    
    local total_ms=$(( (end - start) / 1000000 ))
    local avg_ms=$(( total_ms / iterations ))
    local ops_per_sec=$(( iterations * 1000 / total_ms ))
    
    echo "  Iterations: $iterations"
    echo "  Total time: ${total_ms}ms"
    echo "  Average: ${avg_ms}ms per operation"
    echo "  Throughput: ${ops_per_sec} ops/sec"
    echo ""
    
    {
        echo "$operation"
        echo "  Iterations: $iterations"
        echo "  Total time: ${total_ms}ms"
        echo "  Average: ${avg_ms}ms per operation"
        echo "  Throughput: ${ops_per_sec} ops/sec"
        echo ""
    } >> "$REPORT_FILE"
}

# Cleanup function
cleanup() {
    echo -e "${YELLOW}Cleaning up...${NC}"
    curl -s -XPOST "$BASE_URL/execute" \
        -H 'Content-Type: application/json' \
        -d "{\"sql\":\"DROP DATABASE IF EXISTS $DATABASE\"}" > /dev/null 2>&1 || true
}

# Setup
echo -e "${GREEN}Setting up test environment...${NC}"
cleanup

# Create database
curl -s -XPOST "$BASE_URL/execute" \
    -H 'Content-Type: application/json' \
    -d "{\"sql\":\"CREATE DATABASE $DATABASE\"}" > /dev/null

# Create test table
curl -s -XPOST "$BASE_URL/execute" \
    -H 'Content-Type: application/json' \
    -H "X-Mindb-Database: $DATABASE" \
    -d '{"sql":"CREATE TABLE users(id INT PRIMARY KEY, name TEXT, age INT, email TEXT)"}' > /dev/null

echo -e "${GREEN}Setup complete!${NC}"
echo ""

# Test 1: Health Check
measure_time "Health Check" \
    "curl -s $BASE_URL/health" \
    1000

# Test 2: Single INSERT
measure_time "Single INSERT" \
    "curl -s -XPOST $BASE_URL/execute -H 'Content-Type: application/json' -H 'X-Mindb-Database: $DATABASE' -d '{\"sql\":\"INSERT INTO users(id,name,age,email) VALUES(\$RANDOM,\\\"User\$RANDOM\\\",25,\\\"user@example.com\\\")\"}'" \
    100

# Test 3: Batch INSERT (prepare data for queries)
echo -e "${YELLOW}Preparing test data (1000 rows)...${NC}"
for i in {1..1000}; do
    curl -s -XPOST "$BASE_URL/execute" \
        -H 'Content-Type: application/json' \
        -H "X-Mindb-Database: $DATABASE" \
        -d "{\"sql\":\"INSERT INTO users(id,name,age,email) VALUES($i,\\\"User$i\\\",$((20 + i % 50)),\\\"user$i@example.com\\\")\"}" > /dev/null
done
echo -e "${GREEN}Data prepared!${NC}"
echo ""

# Test 4: SELECT * (full table scan)
measure_time "SELECT * FROM users (1000 rows)" \
    "curl -s -XPOST $BASE_URL/query -H 'Content-Type: application/json' -H 'X-Mindb-Database: $DATABASE' -d '{\"sql\":\"SELECT * FROM users\"}'" \
    100

# Test 5: SELECT with WHERE
measure_time "SELECT with WHERE clause" \
    "curl -s -XPOST $BASE_URL/query -H 'Content-Type: application/json' -H 'X-Mindb-Database: $DATABASE' -d '{\"sql\":\"SELECT * FROM users WHERE age > 30\"}'" \
    100

# Test 6: SELECT with LIMIT
measure_time "SELECT with LIMIT 10" \
    "curl -s -XPOST $BASE_URL/query -H 'Content-Type: application/json' -H 'X-Mindb-Database: $DATABASE' -d '{\"sql\":\"SELECT * FROM users LIMIT 10\"}'" \
    100

# Test 7: UPDATE
measure_time "UPDATE single row" \
    "curl -s -XPOST $BASE_URL/execute -H 'Content-Type: application/json' -H 'X-Mindb-Database: $DATABASE' -d '{\"sql\":\"UPDATE users SET age=35 WHERE id=500\"}'" \
    100

# Test 8: DELETE
measure_time "DELETE single row" \
    "curl -s -XPOST $BASE_URL/execute -H 'Content-Type: application/json' -H 'X-Mindb-Database: $DATABASE' -d '{\"sql\":\"DELETE FROM users WHERE id=999\"}'" \
    50

# Test 9: Transaction (BEGIN/EXEC/COMMIT)
measure_time "Transaction (BEGIN/EXEC/COMMIT)" \
    "TX=\$(curl -s -XPOST $BASE_URL/tx/begin | jq -r '.tx'); curl -s -XPOST $BASE_URL/tx/\$TX/exec -H 'Content-Type: application/json' -H 'X-Mindb-Database: $DATABASE' -d '{\"sql\":\"UPDATE users SET age=40 WHERE id=100\"}' > /dev/null; curl -s -XPOST $BASE_URL/tx/\$TX/commit > /dev/null" \
    50

# Test 10: Concurrent reads (using GNU parallel if available)
if command -v parallel &> /dev/null; then
    echo -e "${YELLOW}Testing: Concurrent Reads (10 parallel requests)${NC}"
    
    start=$(date +%s%N)
    seq 1 100 | parallel -j 10 "curl -s -XPOST $BASE_URL/query -H 'Content-Type: application/json' -H 'X-Mindb-Database: $DATABASE' -d '{\"sql\":\"SELECT * FROM users WHERE age > 25\"}' > /dev/null"
    end=$(date +%s%N)
    
    total_ms=$(( (end - start) / 1000000 ))
    avg_ms=$(( total_ms / 100 ))
    ops_per_sec=$(( 100 * 1000 / total_ms ))
    
    echo "  Iterations: 100 (10 parallel)"
    echo "  Total time: ${total_ms}ms"
    echo "  Average: ${avg_ms}ms per operation"
    echo "  Throughput: ${ops_per_sec} ops/sec"
    echo ""
    
    {
        echo "Concurrent Reads (10 parallel)"
        echo "  Iterations: 100"
        echo "  Total time: ${total_ms}ms"
        echo "  Average: ${avg_ms}ms per operation"
        echo "  Throughput: ${ops_per_sec} ops/sec"
        echo ""
    } >> "$REPORT_FILE"
else
    echo -e "${YELLOW}GNU parallel not found, skipping concurrent test${NC}"
    echo ""
fi

# Cleanup
cleanup

echo -e "${GREEN}=========================================${NC}"
echo -e "${GREEN}Performance Testing Complete!${NC}"
echo -e "${GREEN}=========================================${NC}"
echo ""
echo -e "Report saved to: ${BLUE}$REPORT_FILE${NC}"
echo ""

# Display summary
echo -e "${BLUE}Summary:${NC}"
cat "$REPORT_FILE" | grep -E "^[A-Z]|Throughput:" | sed 's/^/  /'
echo ""
