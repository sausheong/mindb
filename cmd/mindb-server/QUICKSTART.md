# Mindb HTTP Server - Quick Start Guide

## 5-Minute Setup

### 1. Prerequisites
- Go 1.22+
- Make (optional)

### 2. Run Server

```bash
# Clone and navigate
cd /Users/sausheong/go/src/github.com/sausheong/mindb/cmd/mindb-server

# Set environment
export MINDB_DATA_DIR=./data
export AUTH_DISABLED=true

# Run server
go run main.go
```

Server starts on `http://localhost:8080`

### 3. Test Health

```bash
curl http://localhost:8080/health
```

Expected response:
```json
{
  "status": "healthy",
  "uptime_seconds": 5,
  "open_transactions": 0,
  "available_exec_slots": 32
}
```

### 4. Create Database

```bash
curl -XPOST http://localhost:8080/execute \
  -H 'Content-Type: application/json' \
  -d '{"sql":"CREATE DATABASE testdb"}'
```

### 5. Create Table (with database header)

```bash
curl -XPOST http://localhost:8080/execute \
  -H 'Content-Type: application/json' \
  -H 'X-Mindb-Database: testdb' \
  -d '{"sql":"CREATE TABLE users(id INT PRIMARY KEY, name TEXT, age INT)"}'
```

### 6. Insert Data (with database header)

```bash
curl -XPOST http://localhost:8080/execute \
  -H 'Content-Type: application/json' \
  -H 'X-Mindb-Database: testdb' \
  -d '{"sql":"INSERT INTO users(id,name,age) VALUES(1,\"Alice\",30)"}'
```

### 7. Query Data (with database header)

```bash
curl -XPOST http://localhost:8080/query \
  -H 'Content-Type: application/json' \
  -H 'X-Mindb-Database: testdb' \
  -d '{"sql":"SELECT * FROM users"}'
```

Expected response:
```json
{
  "columns": ["id", "name", "age"],
  "rows": [["1", "Alice", "30"]],
  "row_count": 1,
  "truncated": false,
  "latency_ms": 0
}
```

---

## Common Operations

### Update Data
```bash
curl -XPOST http://localhost:8080/execute \
  -H 'Content-Type: application/json' \
  -H 'X-Mindb-Database: testdb' \
  -d '{"sql":"UPDATE users SET age=31 WHERE id=1"}'
```

### Delete Data
```bash
curl -XPOST http://localhost:8080/execute \
  -H 'Content-Type: application/json' \
  -H 'X-Mindb-Database: testdb' \
  -d '{"sql":"DELETE FROM users WHERE id=1"}'
```

### Query with WHERE
```bash
curl -XPOST http://localhost:8080/query \
  -H 'Content-Type: application/json' \
  -H 'X-Mindb-Database: testdb' \
  -d '{"sql":"SELECT * FROM users WHERE age > 25"}'
```

---

## Transactions

### 1. Begin Transaction
```bash
TX=$(curl -s -XPOST http://localhost:8080/tx/begin | jq -r '.tx')
echo "Transaction ID: $TX"
```

### 2. Execute in Transaction
```bash
curl -XPOST http://localhost:8080/tx/$TX/exec \
  -H 'Content-Type: application/json' \
  -H 'X-Mindb-Database: testdb' \
  -d '{"sql":"UPDATE users SET name=\"Bob\" WHERE id=1"}'
```

### 3. Commit
```bash
curl -XPOST http://localhost:8080/tx/$TX/commit
```

Or rollback:
```bash
curl -XPOST http://localhost:8080/tx/$TX/rollback
```

---

## Streaming Large Results

```bash
curl -N -H 'Accept: application/x-ndjson' \
  -H 'X-Mindb-Database: testdb' \
  "http://localhost:8080/stream?sql=SELECT%20*%20FROM%20users&limit=1000"
```

Output (NDJSON):
```
{"id":1,"name":"Alice","age":30}
{"id":2,"name":"Bob","age":25}
...
```

---

## With Authentication

### Set API Key
```bash
export API_KEY=your-secret-key
export AUTH_DISABLED=false
```

### Use API Key
```bash
curl -XPOST http://localhost:8080/query \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: your-secret-key' \
  -H 'X-Mindb-Database: testdb' \
  -d '{"sql":"SELECT * FROM users"}'
```

Or with Bearer token:
```bash
curl -XPOST http://localhost:8080/query \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer your-secret-key' \
  -H 'X-Mindb-Database: testdb' \
  -d '{"sql":"SELECT * FROM users"}'
```

---

## Build & Deploy

### Build Binary
```bash
make build
./bin/mindb-server
```

### Run Tests
```bash
make test
```

### Docker
```bash
make docker
docker run -p 8080:8080 -e MINDB_DATA_DIR=/data mindb-server
```

---

## Configuration

### Required
```bash
export MINDB_DATA_DIR=./data
```

### Optional
```bash
export HTTP_ADDR=:8080
export EXEC_CONCURRENCY=32
export STMT_TIMEOUT_MS=2000
export AUTH_DISABLED=true
export LOG_LEVEL=info
```

See `README.md` for complete configuration.

---

## Troubleshooting

### Server won't start - "directory is locked"
```bash
# Check if another instance is running
ps aux | grep mindb-server

# If not, remove stale lock
rm ./data/.lock
```

### Connection refused
```bash
# Check if server is running
curl http://localhost:8080/health

# Check logs
tail -f logs/mindb-server.log
```

### Query timeout
```bash
# Increase timeout in request
curl -XPOST http://localhost:8080/query \
  -H 'Content-Type: application/json' \
  -H 'X-Mindb-Database: testdb' \
  -d '{"sql":"SELECT * FROM users","timeout_ms":5000}'
```

---

## Using X-Mindb-Database Header

You can specify which database to use via the `X-Mindb-Database` HTTP header:

```bash
# Query from different databases
curl -H 'X-Mindb-Database: production' \
  -d '{"sql":"SELECT * FROM orders"}'

curl -H 'X-Mindb-Database: staging' \
  -d '{"sql":"SELECT * FROM test_data"}'
```

**Benefits:**
- ✅ No need for separate `USE` command
- ✅ Each request is independent
- ✅ Easy to switch between databases
- ✅ Better for multi-database applications

See `docs/DATABASE_HEADER.md` for complete documentation.

---

## Next Steps

- Read `README.md` for complete API documentation
- See `docs/DATABASE_HEADER.md` for database header usage
- Run `examples.sh` for more examples
- Check `openapi.yaml` for API specification

---

## Quick Reference

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/health` | GET | Health check |
| `/query` | POST | Read-only query |
| `/execute` | POST | DML/DDL statement |
| `/tx/begin` | POST | Begin transaction |
| `/tx/{id}/exec` | POST | Execute in tx |
| `/tx/{id}/commit` | POST | Commit tx |
| `/tx/{id}/rollback` | POST | Rollback tx |
| `/stream` | GET | Stream results |

---

**Ready to go! 🚀**

For more details, see:
- `README.md` - Complete documentation
- `docs/DATABASE_HEADER.md` - Database header usage
- `examples.sh` - Working examples
