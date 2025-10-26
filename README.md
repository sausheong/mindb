# Mindb

A minimal relational database written in Go with a client-server architecture, featuring MVCC transactions, WAL recovery, and comprehensive SQL query support.

[![Go Version](https://img.shields.io/badge/Go-1.20+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![Test Coverage](https://img.shields.io/badge/coverage-61.6%25-green)](https://github.com/sausheong/mindb)

## Features

### SQL Support
- **DDL**: CREATE DATABASE, CREATE TABLE, ALTER TABLE, DROP TABLE
- **DML**: SELECT, INSERT, UPDATE, DELETE
- **Queries**: WHERE conditions, ORDER BY, LIMIT, OFFSET
- **Aggregates**: COUNT, SUM, AVG, MIN, MAX with GROUP BY
- **Joins**: INNER, LEFT, RIGHT, FULL OUTER, CROSS JOIN
- **Subqueries**: Scalar, IN, EXISTS (basic support)
- **Transactions**: BEGIN, COMMIT, ROLLBACK

### Storage Engine
- **Paged Storage**: Efficient page-based storage with 4KB pages
- **Buffer Pool**: LRU cache with configurable size
- **B-Tree Indexes**: Fast lookups and range queries
- **MVCC**: Multi-Version Concurrency Control for isolation
- **WAL**: Write-Ahead Logging with ARIES recovery
- **Persistence**: Database save/load to disk

### Advanced Features
- **Constraints**: PRIMARY KEY, UNIQUE, NOT NULL, FOREIGN KEY
- **System Catalog**: Metadata management
- **Concurrent Access**: Thread-safe operations
- **Schema Qualification**: Database.table notation
- **Web Console**: Browser-based SQL interface
- **REST API**: HTTP endpoints for all operations

## User Authentication & Access Control

Mindb ships with a complete, thread‑safe user and permission subsystem. The core data structures model users (with a username, a salted SHA‑256 password hash, and a host constraint) and grants that describe what a user may do against a database or table. A central `UserManager` coordinates creation, lookup, authentication and privilege checks under concurrency.

On the SQL surface, the parser and execution engine understand the familiar MySQL‑style commands for creating and removing users, granting and revoking permissions, inspecting grants, changing passwords, listing users, and working with roles. In practice this means you can `CREATE USER`, `DROP USER`, `GRANT` or `REVOKE` privileges, run `SHOW GRANTS`, change credentials with `ALTER USER`, enumerate users with `SHOW USERS`, and manage roles with `CREATE ROLE`, `DROP ROLE`, `GRANT 'role' TO user`, `REVOKE 'role' FROM user`, and `SHOW ROLES`.

### Privilege Model
The permission model covers the day‑to‑day operations you would expect: SELECT for reading, INSERT and UPDATE for writing, DELETE for removal, and CREATE or DROP for managing schema. When you need a broad stroke, the `ALL` keyword conveys every privilege in one statement. Grants can target a whole database or a specific table.

### Default Root User
Out of the box, the system provides a single administrative account: `root@%` with password `root`. This user has `ALL` privileges on every database and table. Treat this as a bootstrap account and change it immediately in production.

### SQL Examples

```sql
-- Create a readonly user
CREATE USER 'readonly'@'%' IDENTIFIED BY 'password123';

-- Create an admin user
CREATE USER 'admin'@'localhost' IDENTIFIED BY 'secure_password';

-- Create a user for specific IP
CREATE USER 'app_user'@'192.168.1.100' IDENTIFIED BY 'app_pass';

-- Grant SELECT only on all tables in a database
GRANT SELECT ON mydb.* TO 'readonly'@'%';

-- Grant all privileges on a specific database
GRANT ALL ON mydb.* TO 'admin'@'localhost';

-- Grant specific privileges on a table
GRANT SELECT, INSERT, UPDATE ON mydb.users TO 'app_user'@'%';

-- Grant all privileges on all databases
GRANT ALL ON *.* TO 'superuser'@'%';

-- Revoke specific privilege
REVOKE INSERT ON mydb.* FROM 'readonly'@'%';

-- Revoke all privileges
REVOKE ALL ON mydb.* FROM 'user'@'%';

-- Show grants for a user
SHOW GRANTS FOR 'readonly'@'%';

-- Remove a user
DROP USER 'old_user'@'%';
```

### Status
Production Ready. All features are implemented, integrated, persisted to disk, and available through the web console and REST API.

### Quick Start (Auth)

```bash
# Start the server
./mindb

# In another terminal, use the CLI client
./mindb-cli

# Or use curl to create a user
curl -u root:root http://localhost:8080/execute \
  -H "Content-Type: application/json" \
  -d '{"sql": "CREATE USER '\''alice'\''@'\''%'\'' IDENTIFIED BY '\''alice123'\'';"}'

curl -u root:root http://localhost:8080/execute \
  -H "Content-Type: application/json" \
  -d '{"sql": "GRANT SELECT ON mydb.* TO '\''alice'\''@'\''%'\'';"}'
```

### Persistence & File Structure
Users, grants, roles, and role assignments are automatically saved and loaded from `{dataDir}/users.json`.

```
{dataDir}/
├── users.json          ← User data (persistent)
├── catalog.json        ← System catalog
├── wal/                ← Write-ahead log
└── databases/          ← Database files
```

Auto‑save triggers: CREATE/DROP USER, GRANT/REVOKE, CREATE/DROP ROLE, ALTER USER.

### Web Console Login UI
The web console includes a dedicated login screen rather than relying on the browser's Basic Auth dialog. After you sign in, the console stores your session (per tab) in `sessionStorage`, shows your `username@host` in the header, and adds the proper Authorization header to subsequent requests. You can log out at any time, which clears the session and returns you to the login page. Open the console at `http://localhost:8080/console`.

### API Reference (Auth)

```bash
POST /execute
Authorization: Basic base64(username:password)
Content-Type: application/json
{ "sql": "SHOW USERS;" }
```

Common commands:

```sql
ALTER USER 'username'@'host' IDENTIFIED BY 'newpassword';
SHOW USERS;
CREATE ROLE 'rolename';
GRANT 'rolename' TO 'username'@'host';
SHOW ROLES;
```

### Testing (What to verify)
Exercise the user lifecycle (create, alter, drop), grant and revoke privileges, and confirm what the user can or cannot do. If you rely on roles, create one, attach privileges to it, grant it to a user, and verify inheritance through queries. Use the web console to sign in, sign out, and refresh the page to see session restoration in action. Finally, restart the server and confirm that `users.json` brings your users and grants back automatically.

### Troubleshooting
If the server starts without a `users.json`, it will create one and seed it with the default `root:root` user. When logins suddenly fail after a restart, check that the JSON is valid and readable only by the service account (0600). If you see "access denied," revisit the user's direct grants and any roles you expect to apply, making sure the database/table scope matches your query. For security investigations or lockouts, consult the audit logs under `{dataDir}/audit/audit-YYYY-MM-DD.log`.

### Performance
Authentication and user lookups are constant time via in‑memory maps, while privilege checks scale with the number of applicable grants and roles for a user—typically just a handful—so authorization remains fast. Persisting changes to disk takes only a few milliseconds, and loading a typical `users.json` on startup is similarly quick even with hundreds of users.

### Files Modified (Implementation)
The heart of the system lives in `user_management.go`, which models users, roles and grants and handles persistence. SQL support was added in `parser.go`, and the execution paths—including permission checks—reside in `engine_adapter.go`. Security events are recorded by `audit_log.go`. The server boots with user data preloaded by `paged_storage.go`. Finally, the web console under `src/server/web/` provides the login flow and propagates auth to API calls.

### Security Best Practices
Change the default root password, use specific host patterns instead of `%`, apply least privilege, enable HTTPS in production, and review users and grants regularly.

## Installation

```bash
# Clone the repository
git clone https://github.com/sausheong/mindb.git
cd mindb

# Build the project
make build

# This creates:
# bin/mindb     - The server binary
# bin/mindb-cli - The CLI client

# Run tests
make test

# Run with coverage
make test && go tool cover -html=coverage_core.out -o coverage.html
```

## Quick Start

### Basic Usage

```bash
# Start the server
./bin/mindb

# In another terminal, use the CLI client
./bin/mindb-cli

# Or access the web console at http://localhost:8080/console
```

### Server Configuration

Create a `.env` file in the same directory as the server:

```env
MINDB_DATA_DIR=./data
HTTP_ADDR=:8080
ENABLE_TLS=false
```

### Using the REST API

```bash
# Execute SQL
curl -X POST http://localhost:8080/execute \
  -H "Content-Type: application/json" \
  -d '{"sql": "CREATE DATABASE testdb;"}'

# Query data
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM users;"}'
```

## Architecture

Mindb follows a layered client-server architecture:

```
┌─────────────────────────────────────┐
│   Client Layer                      │
│   (CLI, Web Console, REST API)      │
├─────────────────────────────────────┤
│   Server Layer                      │
│   (HTTP Server, Auth, Sessions)     │
├─────────────────────────────────────┤
│   Query Processing Layer            │
│   (Parser, Aggregates, JOINs)       │
├─────────────────────────────────────┤
│   Storage Engine Layer              │
│   (MVCC, Transactions, Catalog)     │
├─────────────────────────────────────┤
│   Persistence Layer                 │
│   (Buffer Pool, B-Tree, WAL, Pages) │
└─────────────────────────────────────┘
```

### Key Components

- **Server**: HTTP server with REST API and web console
- **Core Libraries**: Storage engine, query processing, MVCC
- **CLI Client**: Command-line interface connecting to server
- **PagedEngine**: Main storage engine with MVCC support
- **EngineAdapter**: High-level API for SQL operations
- **BufferPool**: Memory management with LRU eviction
- **BTree**: Index structure for fast lookups
- **WAL**: Write-ahead logging for durability
- **TransactionManager**: MVCC transaction coordination
- **SystemCatalog**: Metadata and schema management

### SQL Examples

```sql
-- Create database
CREATE DATABASE testdb;

-- Create table with constraints
CREATE TABLE users (
    id INT PRIMARY KEY,
    email VARCHAR UNIQUE NOT NULL,
    age INT,
    department VARCHAR
);

-- Insert data
INSERT INTO users VALUES (1, 'alice@test.com', 30, 'Engineering');
INSERT INTO users VALUES (2, 'bob@test.com', 25, 'Sales');

-- Select with conditions
SELECT * FROM users WHERE age > 25;

-- Aggregates with GROUP BY
SELECT department, COUNT(*), AVG(age)
FROM users
GROUP BY department;

-- JOIN operations
SELECT users.name, orders.amount
FROM users
INNER JOIN orders ON users.id = orders.user_id;

-- ORDER BY and LIMIT
SELECT * FROM users
ORDER BY age DESC
LIMIT 10 OFFSET 5;

-- Transactions
BEGIN;
INSERT INTO users VALUES (3, 'charlie@test.com', 35, 'Engineering');
UPDATE users SET age = 31 WHERE id = 1;
COMMIT;
```

## Testing

Mindb has comprehensive test coverage with extensive tests:

```bash
# Run all tests
make test

# Run specific test suite
go test -v -run TestQuery ./src/core/
go test -v -run TestBufferPool ./src/core/
go test -v -run TestPersistence ./src/core/

# Run server tests
make test-server

# View coverage
make test && go tool cover -html=coverage_core.out -o coverage.html
```

### Test Coverage by Component

| Component | Coverage | Status |
|-----------|----------|--------|
| Query Processing | ~88% | Excellent |
| Persistence Manager | ~87% | Excellent |
| Buffer Pool | ~87% | Excellent |
| Aggregates | ~89% | Excellent |
| JOINs | ~88% | Excellent |
| Engine Adapter | ~78% | Very Good |
| Subqueries | ~71% | Very Good |
| B-Tree Persistence | ~62% | Good |

## Project Structure

```
mindb/
├── README.md                      # This file
├── Makefile                       # Build automation
├── go.mod                         # Go module
├── .gitignore                     # Git ignore rules
├── src/
│   ├── core/                      # Core database libraries
│   │   ├── engine_adapter.go      # High-level API
│   │   ├── paged_storage.go       # Main storage engine
│   │   ├── buffer_pool.go         # Memory management
│   │   ├── btree.go               # B-Tree index
│   │   ├── parser.go              # SQL parser
│   │   └── *_test.go              # Test files
│   ├── server/                    # Server implementation
│   │   ├── main.go                # Server entry point
│   │   ├── internal/              # Server internals
│   │   │   ├── api/               # REST API handlers
│   │   │   ├── config/            # Configuration
│   │   │   └── ...
│   │   ├── web/                   # Web console files
│   │   ├── Dockerfile             # Container build
│   │   └── .env.example           # Config example
│   └── cli/                       # CLI client
│       └── main.go                # Client entry point
└── bin/                           # Built binaries (ignored)
```

## Design Goals

Mindb is designed as a minimal relational database that:

- Provides essential SQL functionality with minimal complexity
- Implements proven database techniques (MVCC, WAL, B-Tree)
- Maintains clean, readable code architecture
- Offers production-ready code quality with comprehensive testing
- Balances features with simplicity
- Demonstrates practical database implementation patterns
- Runs as a client-server system with REST API and web console

## Configuration

### Server Configuration

The server reads configuration from environment variables or a `.env` file:

```env
# Data directory
MINDB_DATA_DIR=./data

# Server address
HTTP_ADDR=:8080

# TLS settings
ENABLE_TLS=false
TLS_CERT_FILE=./certs/server.crt
TLS_KEY_FILE=./certs/server.key

# Performance settings
READ_TIMEOUT=30s
WRITE_TIMEOUT=30s
MAX_CONNECTIONS=100

# Authentication
AUTH_DISABLED=false
```

### Buffer Pool Size

The buffer pool size can be configured through the storage engine (default: 128 pages = 512KB).

### WAL Configuration

WAL is always enabled for data durability and crash recovery.

## Performance

Mindb balances simplicity with performance through:

- **Buffer Pool**: LRU caching reduces disk I/O
- **B-Tree Indexes**: O(log n) lookups
- **MVCC**: Non-blocking reads for high concurrency
- **WAL**: Sequential writes for durability
- **Efficient Storage**: Page-based storage with 4KB pages
- **HTTP/2 Support**: Efficient client-server communication

### Benchmarks

```bash
# Run benchmarks
go test -bench=. -benchmem ./src/core/
```

## Contributing

Contributions are welcome! Areas for improvement:

- [ ] Query optimizer
- [ ] More SQL features (HAVING, UNION, etc.)
- [ ] Better subquery support
- [ ] Hash joins
- [ ] Statistics collection
- [ ] Cost-based optimization
- [ ] More index types (Hash, GiST)
- [ ] Parallel query execution
- [ ] Additional client libraries (Python, JavaScript, etc.)

## Acknowledgments

Mindb draws inspiration from established database systems:

- PostgreSQL architecture and MVCC implementation
- SQLite design principles and simplicity
- Modern database research and best practices
- Various open-source database projects

## Contact

- **Author**: Chang Sau Sheong
- **GitHub**: [@sausheong](https://github.com/sausheong)

## Resources

- [OpenAPI Specification](src/server/openapi.yaml)
- [Web Console](http://localhost:8080/console) (when server is running)

---

**Note**: Mindb is a minimal relational database suitable for lightweight applications, embedded systems, or scenarios requiring a simple SQL database. For large-scale production systems, consider established databases like PostgreSQL, MySQL, or SQLite.

**Status**: 61.6% test coverage and comprehensive feature set with client-server architecture.
