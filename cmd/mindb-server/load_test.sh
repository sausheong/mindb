#!/bin/bash
# Load Testing Script for Mindb HTTP Server
#
# This script performs load testing using various tools (ab, wrk, or hey)
# to measure server performance under different load conditions.

set -e

BASE_URL="${BASE_URL:-http://localhost:8080}"
DATABASE="loadtest"
RESULTS_DIR="./load_test_results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}Mindb HTTP Server - Load Testing${NC}"
echo -e "${BLUE}=========================================${NC}"
echo ""

# Create results directory
mkdir -p "$RESULTS_DIR"

# Check for load testing tools
LOAD_TOOL=""
if command -v hey &> /dev/null; then
    LOAD_TOOL="hey"
    echo -e "${GREEN}Using 'hey' for load testing${NC}"
elif command -v wrk &> /dev/null; then
    LOAD_TOOL="wrk"
    echo -e "${GREEN}Using 'wrk' for load testing${NC}"
elif command -v ab &> /dev/null; then
    LOAD_TOOL="ab"
    echo -e "${GREEN}Using 'ab' (Apache Bench) for load testing${NC}"
else
    echo -e "${RED}Error: No load testing tool found!${NC}"
    echo "Please install one of: hey, wrk, or ab (Apache Bench)"
    echo ""
    echo "Installation:"
    echo "  hey: go install github.com/rakyll/hey@latest"
    echo "  wrk: brew install wrk (macOS) or apt-get install wrk (Linux)"
    echo "  ab:  Usually comes with Apache (httpd-tools package)"
    exit 1
fi
echo ""

# Setup
echo -e "${YELLOW}Setting up test environment...${NC}"

# Create database
curl -s -XPOST "$BASE_URL/execute" \
    -H 'Content-Type: application/json' \
    -d "{\"sql\":\"DROP DATABASE IF EXISTS $DATABASE\"}" > /dev/null 2>&1 || true

curl -s -XPOST "$BASE_URL/execute" \
    -H 'Content-Type: application/json' \
    -d "{\"sql\":\"CREATE DATABASE $DATABASE\"}" > /dev/null

# Create test table
curl -s -XPOST "$BASE_URL/execute" \
    -H 'Content-Type: application/json' \
    -H "X-Mindb-Database: $DATABASE" \
    -d '{"sql":"CREATE TABLE users(id INT PRIMARY KEY, name TEXT, age INT)"}' > /dev/null

# Insert test data
echo -e "${YELLOW}Inserting test data (100 rows)...${NC}"
for i in {1..100}; do
    curl -s -XPOST "$BASE_URL/execute" \
        -H 'Content-Type: application/json' \
        -H "X-Mindb-Database: $DATABASE" \
        -d "{\"sql\":\"INSERT INTO users(id,name,age) VALUES($i,\\\"User$i\\\",$((20 + i % 50)))\"}" > /dev/null
done

echo -e "${GREEN}Setup complete!${NC}"
echo ""

# Create request payload files
QUERY_PAYLOAD='{"sql":"SELECT * FROM users WHERE age > 25"}'
echo "$QUERY_PAYLOAD" > /tmp/query_payload.json

# Test 1: Health endpoint load test
echo -e "${BLUE}Test 1: Health Endpoint Load Test${NC}"
echo -e "${YELLOW}Running 10,000 requests with 50 concurrent connections...${NC}"

case $LOAD_TOOL in
    hey)
        hey -n 10000 -c 50 -m GET "$BASE_URL/health" | tee "$RESULTS_DIR/health_load_$TIMESTAMP.txt"
        ;;
    wrk)
        wrk -t 10 -c 50 -d 30s "$BASE_URL/health" | tee "$RESULTS_DIR/health_load_$TIMESTAMP.txt"
        ;;
    ab)
        ab -n 10000 -c 50 "$BASE_URL/health" | tee "$RESULTS_DIR/health_load_$TIMESTAMP.txt"
        ;;
esac
echo ""

# Test 2: Query endpoint load test
echo -e "${BLUE}Test 2: Query Endpoint Load Test${NC}"
echo -e "${YELLOW}Running 5,000 requests with 25 concurrent connections...${NC}"

case $LOAD_TOOL in
    hey)
        hey -n 5000 -c 25 -m POST \
            -H "Content-Type: application/json" \
            -H "X-Mindb-Database: $DATABASE" \
            -D /tmp/query_payload.json \
            "$BASE_URL/query" | tee "$RESULTS_DIR/query_load_$TIMESTAMP.txt"
        ;;
    wrk)
        cat > /tmp/wrk_query.lua << 'EOF'
wrk.method = "POST"
wrk.headers["Content-Type"] = "application/json"
wrk.headers["X-Mindb-Database"] = "loadtest"
wrk.body = '{"sql":"SELECT * FROM users WHERE age > 25"}'
EOF
        wrk -t 5 -c 25 -d 20s -s /tmp/wrk_query.lua "$BASE_URL/query" | tee "$RESULTS_DIR/query_load_$TIMESTAMP.txt"
        ;;
    ab)
        ab -n 5000 -c 25 -p /tmp/query_payload.json -T "application/json" \
            -H "X-Mindb-Database: $DATABASE" \
            "$BASE_URL/query" | tee "$RESULTS_DIR/query_load_$TIMESTAMP.txt"
        ;;
esac
echo ""

# Test 3: Mixed workload (read-heavy)
echo -e "${BLUE}Test 3: Mixed Workload (80% reads, 20% writes)${NC}"
echo -e "${YELLOW}Running for 30 seconds...${NC}"

if [ "$LOAD_TOOL" = "hey" ]; then
    # Run reads in background
    hey -z 30s -c 40 -m POST \
        -H "Content-Type: application/json" \
        -H "X-Mindb-Database: $DATABASE" \
        -D /tmp/query_payload.json \
        "$BASE_URL/query" > "$RESULTS_DIR/mixed_reads_$TIMESTAMP.txt" &
    
    # Run writes
    for i in {1..100}; do
        curl -s -XPOST "$BASE_URL/execute" \
            -H 'Content-Type: application/json' \
            -H "X-Mindb-Database: $DATABASE" \
            -d "{\"sql\":\"INSERT INTO users(id,name,age) VALUES($((1000+i)),\\\"User$((1000+i))\\\",$((20 + i % 50)))\"}" > /dev/null &
        sleep 0.3
    done
    
    wait
    echo -e "${GREEN}Mixed workload test complete${NC}"
    cat "$RESULTS_DIR/mixed_reads_$TIMESTAMP.txt"
else
    echo -e "${YELLOW}Mixed workload test only supported with 'hey' tool${NC}"
fi
echo ""

# Test 4: Stress test (gradually increasing load)
echo -e "${BLUE}Test 4: Stress Test (Gradually Increasing Load)${NC}"
echo -e "${YELLOW}Testing with 10, 25, 50, 100 concurrent connections...${NC}"

for concurrency in 10 25 50 100; do
    echo -e "${YELLOW}Testing with $concurrency concurrent connections...${NC}"
    
    case $LOAD_TOOL in
        hey)
            hey -n 1000 -c $concurrency -m GET "$BASE_URL/health" | grep "Requests/sec:" | tee -a "$RESULTS_DIR/stress_test_$TIMESTAMP.txt"
            ;;
        wrk)
            wrk -t $((concurrency/5)) -c $concurrency -d 10s "$BASE_URL/health" | grep "Requests/sec:" | tee -a "$RESULTS_DIR/stress_test_$TIMESTAMP.txt"
            ;;
        ab)
            ab -n 1000 -c $concurrency "$BASE_URL/health" | grep "Requests per second:" | tee -a "$RESULTS_DIR/stress_test_$TIMESTAMP.txt"
            ;;
    esac
done
echo ""

# Cleanup
echo -e "${YELLOW}Cleaning up...${NC}"
curl -s -XPOST "$BASE_URL/execute" \
    -H 'Content-Type: application/json' \
    -d "{\"sql\":\"DROP DATABASE $DATABASE\"}" > /dev/null 2>&1 || true

rm -f /tmp/query_payload.json /tmp/wrk_query.lua

echo -e "${GREEN}=========================================${NC}"
echo -e "${GREEN}Load Testing Complete!${NC}"
echo -e "${GREEN}=========================================${NC}"
echo ""
echo -e "Results saved to: ${BLUE}$RESULTS_DIR/${NC}"
echo ""

# Display summary
echo -e "${BLUE}Summary:${NC}"
if [ -f "$RESULTS_DIR/health_load_$TIMESTAMP.txt" ]; then
    echo -e "${YELLOW}Health Endpoint:${NC}"
    grep -E "Requests/sec|Requests per second|Latency" "$RESULTS_DIR/health_load_$TIMESTAMP.txt" | head -5 | sed 's/^/  /'
fi
echo ""
if [ -f "$RESULTS_DIR/query_load_$TIMESTAMP.txt" ]; then
    echo -e "${YELLOW}Query Endpoint:${NC}"
    grep -E "Requests/sec|Requests per second|Latency" "$RESULTS_DIR/query_load_$TIMESTAMP.txt" | head -5 | sed 's/^/  /'
fi
echo ""
