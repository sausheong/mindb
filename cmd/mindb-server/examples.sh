#!/bin/bash
# Mindb HTTP Server - Example API Calls
#
# This script demonstrates all API endpoints using the X-Mindb-Database header
# to specify which database to use for each request.
#
# Usage:
#   1. Start the server: go run main.go
#   2. Set AUTH_DISABLED=true or update API_KEY below
#   3. Run this script: ./examples.sh

set -e

BASE_URL="http://localhost:8080"
API_KEY="your-api-key-here"
DATABASE="testdb"

echo "=== Mindb HTTP Server Examples ==="
echo "Using database: $DATABASE"
echo ""

# Health check
echo "1. Health Check"
curl -s "$BASE_URL/health" | jq .
echo ""

# Create database (if using Execute endpoint)
echo "2. Create Database"
curl -s -XPOST "$BASE_URL/execute" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -d '{"sql":"CREATE DATABASE testdb"}' | jq .
echo ""

# Create table
echo "3. Create Table"
curl -s -XPOST "$BASE_URL/execute" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -H "X-Mindb-Database: $DATABASE" \
  -d '{"sql":"CREATE TABLE users(id INT PRIMARY KEY, name TEXT, age INT)"}' | jq .
echo ""

# Insert with RETURNING
echo "4. Insert with RETURNING"
curl -s -XPOST "$BASE_URL/execute" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -H "X-Mindb-Database: $DATABASE" \
  -d '{"sql":"INSERT INTO users(id, name, age) VALUES(1,\"Alice\",30)"}' | jq .
echo ""

# Insert more data
echo "5. Insert More Data"
curl -s -XPOST "$BASE_URL/execute" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -H "X-Mindb-Database: $DATABASE" \
  -d '{"sql":"INSERT INTO users(id, name, age) VALUES(2,\"Bob\",25)"}' | jq .
echo ""

curl -s -XPOST "$BASE_URL/execute" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -H "X-Mindb-Database: $DATABASE" \
  -d '{"sql":"INSERT INTO users(id, name, age) VALUES(3,\"Charlie\",35)"}' | jq .
echo ""

# Query data
echo "6. Query All Users"
curl -s -XPOST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -H "X-Mindb-Database: $DATABASE" \
  -d '{"sql":"SELECT * FROM users"}' | jq .
echo ""

# Query with WHERE
echo "7. Query with WHERE Clause"
curl -s -XPOST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -H "X-Mindb-Database: $DATABASE" \
  -d '{"sql":"SELECT * FROM users WHERE age > 25"}' | jq .
echo ""

# Update
echo "8. Update User"
curl -s -XPOST "$BASE_URL/execute" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -H "X-Mindb-Database: $DATABASE" \
  -d '{"sql":"UPDATE users SET name = \"Alicia\" WHERE id = 1"}' | jq .
echo ""

# Query after update
echo "9. Query After Update"
curl -s -XPOST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -H "X-Mindb-Database: $DATABASE" \
  -d '{"sql":"SELECT * FROM users WHERE id = 1"}' | jq .
echo ""

# Transaction example
echo "10. Transaction Example"
echo "  a. Begin transaction"
TX=$(curl -s -XPOST "$BASE_URL/tx/begin" \
  -H "X-API-Key: $API_KEY" | jq -r '.tx')
echo "     Transaction ID: $TX"
echo ""

echo "  b. Execute in transaction"
curl -s -XPOST "$BASE_URL/tx/$TX/exec" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -H "X-Mindb-Database: $DATABASE" \
  -d '{"sql":"UPDATE users SET age = 31 WHERE id = 1"}' | jq .
echo ""

echo "  c. Commit transaction"
curl -s -XPOST "$BASE_URL/tx/$TX/commit" \
  -H "X-API-Key: $API_KEY" | jq .
echo ""

# Delete
echo "11. Delete User"
curl -s -XPOST "$BASE_URL/execute" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -H "X-Mindb-Database: $DATABASE" \
  -d '{"sql":"DELETE FROM users WHERE id = 3"}' | jq .
echo ""

# Streaming example
echo "12. Streaming Query (NDJSON)"
curl -N -H "Accept: application/x-ndjson" \
  -H "X-API-Key: $API_KEY" \
  -H "X-Mindb-Database: $DATABASE" \
  "$BASE_URL/stream?sql=SELECT%20*%20FROM%20users&limit=100" | head -5
echo ""

# Aggregates
echo "13. Aggregate Query"
curl -s -XPOST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -H "X-Mindb-Database: $DATABASE" \
  -d '{"sql":"SELECT COUNT(*) FROM users"}' | jq .
echo ""

# Drop database
echo "14. Drop Database"
curl -s -XPOST "$BASE_URL/execute" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -d '{"sql":"DROP DATABASE testdb"}' | jq .
echo ""

echo "=== Examples Complete ==="
