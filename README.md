# Mindb

A minimal relational database written in Go, featuring MVCC transactions, WAL recovery, and comprehensive SQL query support.

[![Go Version](https://img.shields.io/badge/Go-1.20+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![Test Coverage](https://img.shields.io/badge/coverage-73.2%25-brightgreen)](https://github.com/sausheong/mindb)

##  Features

### SQL Support
-  **DDL**: CREATE DATABASE, CREATE TABLE, ALTER TABLE, DROP TABLE
-  **DML**: SELECT, INSERT, UPDATE, DELETE
-  **Queries**: WHERE conditions, ORDER BY, LIMIT, OFFSET
-  **Aggregates**: COUNT, SUM, AVG, MIN, MAX with GROUP BY
-  **Joins**: INNER, LEFT, RIGHT, FULL OUTER, CROSS JOIN
-  **Subqueries**: Scalar, IN, EXISTS (basic support)
-  **Transactions**: BEGIN, COMMIT, ROLLBACK

### Storage Engine
-  **Paged Storage**: Efficient page-based storage with 4KB pages
-  **Buffer Pool**: LRU cache with configurable size
-  **B-Tree Indexes**: Fast lookups and range queries
-  **MVCC**: Multi-Version Concurrency Control for isolation
-  **WAL**: Write-Ahead Logging with ARIES recovery
-  **Persistence**: Database save/load to disk

### Advanced Features
-  **Constraints**: PRIMARY KEY, UNIQUE, NOT NULL, FOREIGN KEY
-  **System Catalog**: Metadata management
-  **Concurrent Access**: Thread-safe operations
-  **Schema Qualification**: Database.table notation

##  Installation

```bash
# Clone the repository
git clone https://github.com/sausheong/mindb.git
cd mindb

# Build the project
go build

# Run tests
go test ./...

# Run with coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html
```

##  Quick Start

### Basic Usage

```go
package main

import (
    "fmt"
    "log"
)

func main() {
    // Create engine
    engine, err := NewPagedEngine("./data")
    if err != nil {
        log.Fatal(err)
    }
    defer engine.Close()

    // Create database
    engine.CreateDatabase("mydb")
    engine.UseDatabase("mydb")

    // Create table
    columns := []Column{
        {Name: "id", DataType: "INT", PrimaryKey: true},
        {Name: "name", DataType: "VARCHAR", NotNull: true},
        {Name: "age", DataType: "INT"},
    }
    engine.CreateTable("users", columns)

    // Insert data
    engine.InsertRow("users", Row{"id": 1, "name": "Alice", "age": 30})
    engine.InsertRow("users", Row{"id": 2, "name": "Bob", "age": 25})

    // Query data
    rows, _ := engine.SelectRows("users", []Condition{
        {Column: "age", Operator: ">", Value: 20},
    })
    
    for _, row := range rows {
        fmt.Printf("User: %v\n", row)
    }
}
```

### Using the Engine Adapter (Recommended)

```go
package main

import (
    "fmt"
    "log"
)

func main() {
    // Create adapter
    adapter, err := NewEngineAdapter("./data")
    if err != nil {
        log.Fatal(err)
    }
    defer adapter.Close()

    // Execute SQL
    adapter.Execute(&Statement{
        Type:     CreateDatabase,
        Database: "mydb",
    })
    
    adapter.UseDatabase("mydb")

    // Create table
    adapter.Execute(&Statement{
        Type:  CreateTable,
        Table: "users",
        Columns: []Column{
            {Name: "id", DataType: "INT", PrimaryKey: true},
            {Name: "name", DataType: "VARCHAR"},
        },
    })

    // Insert
    adapter.Execute(&Statement{
        Type:   Insert,
        Table:  "users",
        Values: [][]interface{}{{1, "Alice"}, {2, "Bob"}},
    })

    // Select
    result, _ := adapter.Execute(&Statement{
        Type:  Select,
        Table: "users",
    })
    
    fmt.Println(result)
}
```

##  Documentation

### Architecture

Mindb follows a layered architecture:

```
┌─────────────────────────────────────┐
│   Application Layer                 │
│   (CLI, Engine Adapter)             │
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

##  Testing

Mindb has comprehensive test coverage (73.2%) with 125 tests:

```bash
# Run all tests
go test ./...

# Run specific test suite
go test -v -run TestQuery
go test -v -run TestBufferPool
go test -v -run TestPersistence

# Run with coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html

# View coverage summary
go tool cover -func=coverage.out | tail -1
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

##  Project Structure

```
mindb/
├── README.md                      # This file
├── main.go                        # CLI entry point
├── engine_adapter.go              # High-level API
├── paged_storage.go              # Main storage engine
├── buffer_pool.go                # Memory management
├── btree.go                      # B-Tree index
├── btree_persistence.go          # B-Tree save/load
├── page.go                       # Page management
├── heapfile.go                   # Heap file storage
├── wal.go                        # Write-ahead logging
├── recovery.go                   # ARIES recovery
├── transaction.go                # Transaction manager
├── mvcc.go                       # MVCC implementation
├── catalog.go                    # System catalog
├── constraints.go                # Constraint validation
├── parser.go                     # SQL parser
├── aggregate.go                  # Aggregate functions
├── join.go                       # JOIN operations
├── subquery.go                   # Subquery support
├── persistence.go                # Database persistence
├── docs/                         # Documentation
└── *_test.go                     # Test files
```

##  Design Goals

Mindb is designed as a minimal relational database that:

- Provides essential SQL functionality with minimal complexity
- Implements proven database techniques (MVCC, WAL, B-Tree)
- Maintains clean, readable code architecture
- Offers production-ready code quality with comprehensive testing
- Balances features with simplicity
- Demonstrates practical database implementation patterns

##  Configuration

### Buffer Pool Size

```go
// Create engine with custom buffer pool size
engine, _ := NewPagedEngine("./data")
// Default buffer pool size is 128 pages (512KB)

// Or configure manually
pool := NewBufferPool(256) // 256 pages (1MB)
```

### WAL Configuration

```go
// Enable WAL for durability
engine, _ := NewPagedEngineWithWAL("./data", true)
```

##  Performance

Mindb balances simplicity with performance through:

- **Buffer Pool**: LRU caching reduces disk I/O
- **B-Tree Indexes**: O(log n) lookups
- **MVCC**: Non-blocking reads for high concurrency
- **WAL**: Sequential writes for durability
- **Efficient Storage**: Page-based storage with 4KB pages

### Benchmarks

```bash
# Run benchmarks
go test -bench=. -benchmem
```

##  Contributing

Contributions are welcome! Areas for improvement:

- [ ] Query optimizer
- [ ] More SQL features (HAVING, UNION, etc.)
- [ ] Better subquery support
- [ ] Hash joins
- [ ] Statistics collection
- [ ] Cost-based optimization
- [ ] More index types (Hash, GiST)
- [ ] Parallel query execution


##  Acknowledgments

Mindb draws inspiration from established database systems:

- PostgreSQL architecture and MVCC implementation
- SQLite design principles and simplicity
- Modern database research and best practices
- Various open-source database projects

##  Contact

- **Author**: Chang Sau Sheong
- **GitHub**: [@sausheong](https://github.com/sausheong)

##  Resources

- [Architecture Documentation](docs/architecture.md)
- [Test Coverage Report](docs/COVERAGE_REPORT.md)
- [Implementation Notes](docs/COMPLETE_FINAL_SUMMARY.md)

---

**Note**: Mindb is a minimal relational database suitable for lightweight applications, embedded systems, or scenarios requiring a simple SQL database. For large-scale production systems, consider established databases like PostgreSQL, MySQL, or SQLite.

**Status**: 73.2% test coverage and comprehensive feature set.
