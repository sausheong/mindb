# Mindb HTTP Server

Production-grade HTTP service that embeds Mindb as an in-process database with a clean REST API.

## Architecture

- **Single Process Ownership**: Exclusive lockfile prevents multiple processes from accessing the same data directory
- **MVCC Transactions**: Full snapshot isolation with explicit transaction management
- **Graceful Shutdown**: Clean WAL flush and lock release on SIGTERM/SIGINT
- **Streaming Support**: NDJSON streaming for large result sets
- **Observability**: Structured logging and Prometheus metrics

## Quick Start

```bash
# Set required environment variable
export MINDB_DATA_DIR=./data
export AUTH_DISABLED=true

# Run the server
go run main.go

# Test it
curl -XPOST http://localhost:8080/execute \
  -H 'Content-Type: application/json' \
  -d '{"sql":"CREATE DATABASE testdb"}'

curl -XPOST http://localhost:8080/execute \
  -H 'Content-Type: application/json' \
  -H 'X-Mindb-Database: testdb' \
  -d '{"sql":"CREATE TABLE users(id INT PRIMARY KEY, name TEXT)"}'

curl -XPOST http://localhost:8080/query \
  -H 'Content-Type: application/json' \
  -H 'X-Mindb-Database: testdb' \
  -d '{"sql":"SELECT * FROM users"}'
```

See `QUICKSTART.md` for detailed setup guide.

## Configuration

All configuration via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `MINDB_DATA_DIR` | *required* | Data directory path |
| `HTTP_ADDR` | `:8080` | HTTP listen address |
| `EXEC_CONCURRENCY` | `32` | Max concurrent executions |
| `STMT_TIMEOUT_MS` | `2000` | Default statement timeout (ms) |
| `TX_IDLE_TIMEOUT_MS` | `60000` | Transaction idle timeout (ms) |
| `MAX_OPEN_TX` | `100` | Max open transactions |
| `MAX_TX_PER_CLIENT` | `5` | Max transactions per client |
| `AUTH_DISABLED` | `false` | Disable authentication |
| `API_KEY` | - | API key for authentication |
| `READ_TIMEOUT` | `30s` | HTTP read timeout |
| `WRITE_TIMEOUT` | `30s` | HTTP write timeout |
| `SHUTDOWN_GRACE` | `30s` | Graceful shutdown timeout |
| `LOG_LEVEL` | `info` | Log level (debug/info/warn/error) |
| `ENABLE_METRICS` | `true` | Enable Prometheus metrics |

## API Endpoints

All endpoints support the `X-Mindb-Database` header to specify which database to use:

```bash
curl -H 'X-Mindb-Database: testdb' ...
```

### POST /query

Execute read-only SQL query (auto-commit).

**Request:**
```json
{
  "sql": "SELECT id, name FROM users WHERE id = 1",
  "limit": 1000,
  "timeout_ms": 2000
}
```

**Response:**
```json
{
  "columns": ["id", "name"],
  "rows": [["1", "Alice"]],
  "row_count": 1,
  "truncated": false,
  "latency_ms": 0
}
```

**Example:**
```bash
curl -XPOST http://localhost:8080/query \
  -H 'Content-Type: application/json' \
  -H 'X-Mindb-Database: testdb' \
  -d '{"sql":"SELECT * FROM users"}'
```

### POST /execute

Execute DML/DDL statement (auto-commit).

**Request:**
```json
{
  "sql": "INSERT INTO users(id, name) VALUES($1, $2) RETURNING id",
  "args": [1, "Alice"],
  "timeout_ms": 2000
}
```

**Response:**
```json
{
  "affected_rows": 1,
  "returning": {
    "columns": ["id"],
    "rows": [[1]],
    "row_count": 1,
    "truncated": false,
    "latency_ms": 0
  },
  "latency_ms": 5
}
```

**Examples:**
```bash
# Create database
curl -XPOST http://localhost:8080/execute \
  -H 'Content-Type: application/json' \
  -d '{"sql":"CREATE DATABASE testdb"}'

# Create table
curl -XPOST http://localhost:8080/execute \
  -H 'Content-Type: application/json' \
  -H 'X-Mindb-Database: testdb' \
  -d '{"sql":"CREATE TABLE users(id INT PRIMARY KEY, name TEXT)"}'

# Insert
curl -XPOST http://localhost:8080/execute \
  -H 'Content-Type: application/json' \
  -H 'X-Mindb-Database: testdb' \
  -d '{"sql":"INSERT INTO users(id,name) VALUES(1,\"Alice\")"}'

# Update
curl -XPOST http://localhost:8080/execute \
  -H 'Content-Type: application/json' \
  -H 'X-Mindb-Database: testdb' \
  -d '{"sql":"UPDATE users SET name=\"Alicia\" WHERE id=1"}'
```

### POST /tx/begin

Begin a new transaction with snapshot isolation.

**Response:**
```json
{
  "tx": "abc123...",
  "isolation": "snapshot"
}
```

**Example:**
```bash
TX=$(curl -XPOST http://localhost:8080/tx/begin | jq -r '.tx')
echo $TX
```

### POST /tx/{txID}/exec

Execute statement within a transaction.

**Request:** Same as `/execute`

**Example:**
```bash
curl -XPOST http://localhost:8080/tx/$TX/exec \
  -H 'Content-Type: application/json' \
  -d '{"sql":"UPDATE users SET name=$1 WHERE id=$2","args":["Bob",1]}'
```

### POST /tx/{txID}/commit

Commit a transaction.

**Response:**
```json
{
  "status": "committed"
}
```

**Example:**
```bash
curl -XPOST http://localhost:8080/tx/$TX/commit
```

### POST /tx/{txID}/rollback

Rollback a transaction.

**Response:**
```json
{
  "status": "rolled_back"
}
```

**Example:**
```bash
curl -XPOST http://localhost:8080/tx/$TX/rollback
```

### GET /stream

Stream large result sets as NDJSON.

**Query Parameters:**
- `sql`: SQL query (required)
- `args`: JSON-encoded array of arguments (optional)
- `limit`: Row limit (optional)

**Response:** `application/x-ndjson` - one JSON object per line

**Example:**
```bash
curl -N -H 'Accept: application/x-ndjson' \
  "http://localhost:8080/stream?sql=SELECT%20*%20FROM%20users&limit=100000"
```

### GET /health

Health check endpoint.

**Response:**
```json
{
  "status": "healthy",
  "uptime_seconds": 123,
  "open_transactions": 5
}
```

### GET /metrics

Prometheus metrics endpoint (if `ENABLE_METRICS=true`).

## Error Responses

All errors follow this format:

```json
{
  "error": {
    "code": "TIMEOUT",
    "message": "statement execution timeout"
  }
}
```

**HTTP Status Codes:**
- `400` - Bad request (invalid JSON, SQL errors)
- `401` - Unauthorized (invalid API key)
- `404` - Not found (transaction not found)
- `408` - Request timeout
- `409` - Conflict (transaction conflict)
- `500` - Internal server error
- `504` - Gateway timeout

## Single Process Ownership

The server enforces exclusive access to the data directory using a lockfile (`.lock`):

1. On startup, acquires exclusive flock on `MINDB_DATA_DIR/.lock`
2. Writes PID, hostname, and start time to lockfile
3. If lock is held by another process, exits with error
4. On shutdown, releases lock and removes lockfile

**Important:** Only one `mindb-server` process can access a data directory at a time.

## Graceful Shutdown

On SIGTERM or SIGINT:

1. Stop accepting new HTTP connections
2. Wait for in-flight requests (up to `SHUTDOWN_GRACE`)
3. Cancel remaining requests
4. Rollback open transactions
5. Flush WAL to disk
6. Close database
7. Release lockfile

## Authentication

When `AUTH_DISABLED=false` (default), all requests require authentication:

**API Key (Header):**
```bash
curl -H 'X-API-Key: your-api-key' http://localhost:8080/query ...
```

**Bearer Token:**
```bash
curl -H 'Authorization: Bearer your-token' http://localhost:8080/query ...
```

Set `AUTH_DISABLED=true` for development/testing.

## Observability

### Structured Logging

All requests logged with:
- `sql_hash`: Hash of SQL statement
- `rows`: Number of rows returned/affected
- `latency_ms`: Execution time
- `timeout`: Whether request timed out
- `client_id`: Client identifier
- `tx_id`: Transaction ID (if applicable)

### Prometheus Metrics

Available at `/metrics`:

- `mindb_http_requests_total`: Total HTTP requests
- `mindb_http_request_duration_seconds`: Request duration histogram
- `mindb_open_transactions`: Current open transactions
- `mindb_active_connections`: Active HTTP connections
- `mindb_query_rows_total`: Total rows returned
- `mindb_execute_rows_total`: Total rows affected

## Development

```bash
# Install dependencies
go mod download

# Run tests
make test

# Run server
make run

# Build binary
make build

# Clean
make clean
```

## Production Deployment

### Systemd Service

```ini
[Unit]
Description=Mindb HTTP Server
After=network.target

[Service]
Type=simple
User=mindb
Environment="MINDB_DATA_DIR=/var/lib/mindb"
Environment="HTTP_ADDR=:8080"
Environment="API_KEY=your-secret-key"
ExecStart=/usr/local/bin/mindb-server
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

### Docker

```dockerfile
FROM golang:1.22-alpine AS builder
WORKDIR /build
COPY . .
RUN go build -o mindb-server ./cmd/mindb-server

FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=builder /build/mindb-server /usr/local/bin/
EXPOSE 8080
CMD ["mindb-server"]
```

## Database Selection

### Using X-Mindb-Database Header

Specify which database to use via HTTP header (recommended):

```bash
curl -XPOST http://localhost:8080/query \
  -H 'X-Mindb-Database: testdb' \
  -d '{"sql":"SELECT * FROM users"}'
```

### Using USE Command

Alternatively, use the `USE` command:

```bash
curl -XPOST http://localhost:8080/execute \
  -d '{"sql":"USE testdb"}'

# Subsequent queries will use testdb
curl -XPOST http://localhost:8080/query \
  -d '{"sql":"SELECT * FROM users"}'
```


## Limitations

- Single process per data directory (by design)
- No distributed transactions
- No query result caching
- No connection pooling (single embedded DB)
- Transaction timeout enforced by idle timer

