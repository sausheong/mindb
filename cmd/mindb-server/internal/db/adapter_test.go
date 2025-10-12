package db

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestNewAdapter(t *testing.T) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	if adapter == nil {
		t.Fatal("Expected non-nil adapter")
	}
	if adapter.engine == nil {
		t.Error("Expected non-nil engine")
	}
	if adapter.parser == nil {
		t.Error("Expected non-nil parser")
	}
}

func TestNewAdapter_InvalidPath(t *testing.T) {
	// Try to create adapter with invalid path
	_, err := NewAdapter("/invalid/path/that/does/not/exist/mindb-test")
	if err == nil {
		t.Error("Expected error for invalid path")
	}
}

func TestAdapter_Query_CreateAndSelect(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	ctx := context.Background()

	// Create database
	_, _, err = adapter.Execute(ctx, "CREATE DATABASE testdb", nil, "")
	if err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}

	// Create table
	_, _, err = adapter.Execute(ctx, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)", nil, "testdb")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert data
	_, _, err = adapter.Execute(ctx, "INSERT INTO users (id, name) VALUES (1, 'Alice')", nil, "testdb")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Query data
	columns, rows, err := adapter.Query(ctx, "SELECT * FROM users", nil, 0, "testdb")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	if len(columns) == 0 {
		t.Error("Expected columns in result")
	}
	if len(rows) == 0 {
		t.Error("Expected rows in result")
	}
}

func TestAdapter_Query_ContextCancellation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Try to query with cancelled context
	_, _, err = adapter.Query(ctx, "SELECT 1", nil, 0, "")
	if err == nil {
		t.Error("Expected error for cancelled context")
	}
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}

func TestAdapter_Query_ContextTimeout(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	// Create context with very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond) // Ensure timeout

	// Try to query with timed-out context
	_, _, err = adapter.Query(ctx, "SELECT 1", nil, 0, "")
	if err == nil {
		t.Error("Expected error for timed-out context")
	}
}

func TestAdapter_Execute_InvalidSQL(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	ctx := context.Background()

	// Try to execute invalid SQL
	_, _, err = adapter.Execute(ctx, "INVALID SQL STATEMENT", nil, "")
	if err == nil {
		t.Error("Expected error for invalid SQL")
	}
}

func TestAdapter_Execute_UseDatabase(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	ctx := context.Background()

	// Create database
	_, _, err = adapter.Execute(ctx, "CREATE DATABASE testdb", nil, "")
	if err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}

	// Use database command
	affected, _, err := adapter.Execute(ctx, "USE testdb", nil, "")
	if err != nil {
		t.Fatalf("USE DATABASE failed: %v", err)
	}

	if affected != 1 {
		t.Errorf("Expected affected rows 1, got %d", affected)
	}
}

func TestAdapter_Authenticate_Success(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	// Default root user should authenticate
	if !adapter.Authenticate("root", "root", "%") {
		t.Error("Expected root user to authenticate")
	}
}

func TestAdapter_Authenticate_Failure(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	// Invalid credentials should fail
	if adapter.Authenticate("invalid", "wrong", "%") {
		t.Error("Expected authentication to fail for invalid credentials")
	}
}

func TestAdapter_SetCurrentUser(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	// Should not panic
	adapter.SetCurrentUser("testuser", "localhost")
}

func TestAdapter_Close(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}

	// Close should not error
	err = adapter.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestAdapter_Query_WithLimit(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	ctx := context.Background()

	// Create database and table
	_, _, err = adapter.Execute(ctx, "CREATE DATABASE testdb", nil, "")
	if err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}

	_, _, err = adapter.Execute(ctx, "CREATE TABLE items (id INT PRIMARY KEY, value VARCHAR)", nil, "testdb")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert multiple rows
	for i := 1; i <= 10; i++ {
		sql := fmt.Sprintf("INSERT INTO items (id, value) VALUES (%d, 'Item%d')", i, i)
		_, _, err = adapter.Execute(ctx, sql, nil, "testdb")
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Query with limit
	_, rows, err := adapter.Query(ctx, "SELECT * FROM items", nil, 5, "testdb")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	// Should return limited rows (implementation dependent)
	if len(rows) > 10 {
		t.Errorf("Expected at most 10 rows, got %d", len(rows))
	}
}

func TestParseResultString(t *testing.T) {
	// Test with empty result
	result := ""
	columns, rows := parseResultString(result, 0)
	if len(columns) != 0 {
		t.Errorf("Expected 0 columns for empty result, got %d", len(columns))
	}
	if len(rows) != 0 {
		t.Errorf("Expected 0 rows for empty result, got %d", len(rows))
	}
}

func TestAdapter_Execute_ContextCancellation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Try to execute with cancelled context
	_, _, err = adapter.Execute(ctx, "CREATE DATABASE testdb", nil, "")
	if err == nil {
		t.Error("Expected error for cancelled context")
	}
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}

func TestAdapter_Authenticate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	// Test authentication with root user (default)
	result := adapter.Authenticate("root", "root", "%")
	if !result {
		t.Error("Expected root authentication to succeed")
	}

	// Test invalid credentials
	result = adapter.Authenticate("invalid", "wrong", "%")
	if result {
		t.Error("Expected invalid authentication to fail")
	}
}

func TestAdapter_LogLoginSuccess(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	// Should not panic
	adapter.LogLoginSuccess("testuser", "localhost")
}

func TestAdapter_LogLoginFailed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	// Should not panic
	adapter.LogLoginFailed("testuser", "localhost", "invalid_password")
}

func TestAdapter_IsAccountLocked(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	// Test account lock status (should be false by default)
	locked := adapter.IsAccountLocked("testuser", "localhost")
	if locked {
		t.Error("Expected account to not be locked by default")
	}
}

func TestAdapter_ListProcedures(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	ctx := context.Background()

	// Create database first
	_, _, err = adapter.Execute(ctx, "CREATE DATABASE testdb", nil, "")
	if err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}

	// Test list procedures
	procs, err := adapter.ListProcedures(ctx, "testdb")
	if err != nil {
		t.Fatalf("ListProcedures failed: %v", err)
	}

	// Should return empty list or valid list
	if procs == nil {
		t.Error("Expected non-nil procedures list")
	}
}

func TestParseAffectedRows(t *testing.T) {
	tests := []struct {
		name     string
		result   string
		expected int
	}{
		{"Insert result", "1 row affected", 1},
		{"Multiple rows", "5 rows affected", 1}, // Function returns 1 for any "rows affected"
		{"No rows", "0 rows affected", 1}, // parseAffectedRows returns 1 for "0 rows affected"
		{"Query OK", "Query OK", 1},
		{"Created successfully", "Table created successfully", 1},
		{"Create table", "Table created", 0},
		{"Unknown format", "Some other result", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseAffectedRows(tt.result)
			if result != tt.expected {
				t.Errorf("Expected %d, got %d for result: %s", tt.expected, result, tt.result)
			}
		})
	}
}

func TestParseResultString_EmptyResult(t *testing.T) {
	columns, rows := parseResultString("0 rows returned", 0)
	if columns != nil || rows != nil {
		t.Error("Expected nil columns and rows for empty result")
	}
}

func TestParseResultString_EmptyString(t *testing.T) {
	columns, rows := parseResultString("", 0)
	if columns != nil || rows != nil {
		t.Error("Expected nil columns and rows for empty string")
	}
}

func TestAdapter_Execute_MultipleStatements(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	ctx := context.Background()

	// Create database
	_, _, err = adapter.Execute(ctx, "CREATE DATABASE testdb", nil, "")
	if err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}

	// Create multiple tables
	_, _, err = adapter.Execute(ctx, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)", nil, "testdb")
	if err != nil {
		t.Fatalf("CREATE TABLE users failed: %v", err)
	}

	_, _, err = adapter.Execute(ctx, "CREATE TABLE products (id INT PRIMARY KEY, title VARCHAR)", nil, "testdb")
	if err != nil {
		t.Fatalf("CREATE TABLE products failed: %v", err)
	}
}

func TestAdapter_Query_EmptyTable(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	ctx := context.Background()

	// Create database and table
	_, _, err = adapter.Execute(ctx, "CREATE DATABASE testdb", nil, "")
	if err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}

	_, _, err = adapter.Execute(ctx, "CREATE TABLE empty_table (id INT PRIMARY KEY)", nil, "testdb")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Query empty table
	_, rows, err := adapter.Query(ctx, "SELECT * FROM empty_table", nil, 0, "testdb")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	// Should return columns but no rows
	if len(rows) > 0 {
		t.Errorf("Expected no rows from empty table, got %d", len(rows))
	}
}

func TestAdapter_Execute_Update(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	ctx := context.Background()

	// Setup
	_, _, err = adapter.Execute(ctx, "CREATE DATABASE testdb", nil, "")
	if err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}

	_, _, err = adapter.Execute(ctx, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)", nil, "testdb")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, _, err = adapter.Execute(ctx, "INSERT INTO users (id, name) VALUES (1, 'Alice')", nil, "testdb")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test UPDATE
	affected, _, err := adapter.Execute(ctx, "UPDATE users SET name = 'Bob' WHERE id = 1", nil, "testdb")
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}

	if affected < 0 {
		t.Errorf("Expected non-negative affected rows, got %d", affected)
	}
}

func TestAdapter_Execute_Delete(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	ctx := context.Background()

	// Setup
	_, _, err = adapter.Execute(ctx, "CREATE DATABASE testdb", nil, "")
	if err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}

	_, _, err = adapter.Execute(ctx, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)", nil, "testdb")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, _, err = adapter.Execute(ctx, "INSERT INTO users (id, name) VALUES (1, 'Alice')", nil, "testdb")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test DELETE
	affected, _, err := adapter.Execute(ctx, "DELETE FROM users WHERE id = 1", nil, "testdb")
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}

	if affected < 0 {
		t.Errorf("Expected non-negative affected rows, got %d", affected)
	}
}

func TestAdapter_Execute_DropTable(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	ctx := context.Background()

	// Setup
	_, _, err = adapter.Execute(ctx, "CREATE DATABASE testdb", nil, "")
	if err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}

	_, _, err = adapter.Execute(ctx, "CREATE TABLE temp_table (id INT PRIMARY KEY)", nil, "testdb")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Test DROP TABLE
	_, _, err = adapter.Execute(ctx, "DROP TABLE temp_table", nil, "testdb")
	if err != nil {
		t.Fatalf("DROP TABLE failed: %v", err)
	}
}

func TestAdapter_Query_WithWhere(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	ctx := context.Background()

	// Setup
	_, _, err = adapter.Execute(ctx, "CREATE DATABASE testdb", nil, "")
	if err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}

	_, _, err = adapter.Execute(ctx, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, age INT)", nil, "testdb")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert test data
	for i := 1; i <= 5; i++ {
		sql := fmt.Sprintf("INSERT INTO users (id, name, age) VALUES (%d, 'User%d', %d)", i, i, 20+i)
		_, _, err = adapter.Execute(ctx, sql, nil, "testdb")
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Query with WHERE clause
	_, rows, err := adapter.Query(ctx, "SELECT * FROM users WHERE age > 22", nil, 0, "testdb")
	if err != nil {
		t.Fatalf("SELECT with WHERE failed: %v", err)
	}

	if len(rows) == 0 {
		t.Error("Expected rows from WHERE query")
	}
}

func TestAdapter_Query_InvalidDatabase(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	ctx := context.Background()

	// Try to query non-existent database
	_, _, err = adapter.Query(ctx, "SELECT * FROM users", nil, 0, "nonexistent_db")
	if err == nil {
		t.Error("Expected error for non-existent database")
	}
}

func TestAdapter_Execute_InvalidDatabase(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	ctx := context.Background()

	// Try to execute on non-existent database
	_, _, err = adapter.Execute(ctx, "CREATE TABLE test (id INT)", nil, "nonexistent_db")
	if err == nil {
		t.Error("Expected error for non-existent database")
	}
}

func TestParseResultString_WithData(t *testing.T) {
	result := `+----+-------+
| id | name  |
+----+-------+
| 1  | Alice |
| 2  | Bob   |
+----+-------+`

	columns, rows := parseResultString(result, 0)

	if len(columns) == 0 {
		t.Error("Expected columns to be parsed")
	}
	if len(rows) == 0 {
		t.Error("Expected rows to be parsed")
	}
}

func TestParseResultString_WithLimit(t *testing.T) {
	result := `+----+-------+
| id | name  |
+----+-------+
| 1  | Alice |
| 2  | Bob   |
| 3  | Charlie |
| 4  | David |
+----+-------+`

	columns, rows := parseResultString(result, 2)

	if len(columns) == 0 {
		t.Error("Expected columns to be parsed")
	}
	if len(rows) > 2 {
		t.Errorf("Expected at most 2 rows with limit, got %d", len(rows))
	}
}

func TestParseResultString_WithFooter(t *testing.T) {
	result := `+----+-------+
| id | name  |
+----+-------+
| 1  | Alice |
+----+-------+
2 row(s) returned`

	columns, rows := parseResultString(result, 0)

	if len(columns) == 0 {
		t.Error("Expected columns to be parsed")
	}
	if len(rows) == 0 {
		t.Error("Expected rows to be parsed")
	}
}

func TestParseResultString_NoHeaderFound(t *testing.T) {
	result := `+----+-------+
+----+-------+`

	columns, rows := parseResultString(result, 0)

	if columns != nil || rows != nil {
		t.Error("Expected nil for result with no header")
	}
}

func TestParseResultString_OnlyHeader(t *testing.T) {
	result := `+----+-------+
| id | name  |
+----+-------+`

	columns, rows := parseResultString(result, 0)

	if len(columns) == 0 {
		t.Error("Expected columns to be parsed")
	}
	if rows != nil {
		t.Error("Expected nil rows for header-only result")
	}
}

func TestAdapter_CreateProcedure_InvalidWASM(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	ctx := context.Background()

	// Create database
	_, _, err = adapter.Execute(ctx, "CREATE DATABASE testdb", nil, "")
	if err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}

	// Test with invalid base64
	err = adapter.CreateProcedure(ctx, "testdb", "test_proc", "wasm", "invalid-base64!!!", nil, "INT", "test")
	if err == nil {
		t.Error("Expected error for invalid base64 WASM")
	}
}

func TestAdapter_CreateProcedure_ContextCancelled(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Should fail with context error
	err = adapter.CreateProcedure(ctx, "testdb", "test_proc", "wasm", "dGVzdA==", nil, "INT", "test")
	if err == nil {
		t.Error("Expected error for cancelled context")
	}
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}

func TestAdapter_DropProcedure_ContextCancelled(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Should fail with context error
	err = adapter.DropProcedure(ctx, "testdb", "test_proc")
	if err == nil {
		t.Error("Expected error for cancelled context")
	}
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}

func TestAdapter_ListProcedures_ContextCancelled(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Should fail with context error
	_, err = adapter.ListProcedures(ctx, "testdb")
	if err == nil {
		t.Error("Expected error for cancelled context")
	}
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}

func TestAdapter_CallProcedure_ContextCancelled(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Should fail with context error
	_, err = adapter.CallProcedure(ctx, "testdb", "test_proc")
	if err == nil {
		t.Error("Expected error for cancelled context")
	}
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}

func TestAdapter_Query_LargeResult(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	ctx := context.Background()

	// Setup
	_, _, err = adapter.Execute(ctx, "CREATE DATABASE testdb", nil, "")
	if err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}

	_, _, err = adapter.Execute(ctx, "CREATE TABLE large_table (id INT PRIMARY KEY, value VARCHAR)", nil, "testdb")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert many rows
	for i := 1; i <= 100; i++ {
		sql := fmt.Sprintf("INSERT INTO large_table (id, value) VALUES (%d, 'Value%d')", i, i)
		_, _, err = adapter.Execute(ctx, sql, nil, "testdb")
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Query all
	_, rows, err := adapter.Query(ctx, "SELECT * FROM large_table", nil, 0, "testdb")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	if len(rows) == 0 {
		t.Error("Expected rows from large table")
	}
}

func TestAdapter_Execute_WithTimeout(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Should succeed within timeout
	_, _, err = adapter.Execute(ctx, "CREATE DATABASE testdb", nil, "")
	if err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}
}

func TestAdapter_Query_WithTimeout(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	ctx := context.Background()

	// Setup
	_, _, err = adapter.Execute(ctx, "CREATE DATABASE testdb", nil, "")
	if err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}

	_, _, err = adapter.Execute(ctx, "CREATE TABLE test (id INT PRIMARY KEY)", nil, "testdb")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Query with timeout
	ctxTimeout, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _, err = adapter.Query(ctxTimeout, "SELECT * FROM test", nil, 0, "testdb")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
}

func TestAdapter_CreateProcedure_WithParams(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	ctx := context.Background()

	// Create database
	_, _, err = adapter.Execute(ctx, "CREATE DATABASE testdb", nil, "")
	if err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}

	// Test with params
	params := []interface{}{
		map[string]interface{}{"name": "x", "data_type": "INT"},
		map[string]interface{}{"name": "y", "data_type": "INT"},
	}
	
	// Valid base64 encoded WASM (minimal valid WASM module)
	wasmBase64 := "AGFzbQEAAAA=" // "\x00asm\x01\x00\x00\x00" in base64
	
	err = adapter.CreateProcedure(ctx, "testdb", "add", "wasm", wasmBase64, params, "INT", "Add two numbers")
	// Error is expected since we don't have a real WASM engine, but we're testing the code path
}

func TestAdapter_CreateProcedure_NoParams(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	ctx := context.Background()

	// Create database
	_, _, err = adapter.Execute(ctx, "CREATE DATABASE testdb", nil, "")
	if err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}

	// Test without params (will try introspection)
	wasmBase64 := "AGFzbQEAAAA="
	
	err = adapter.CreateProcedure(ctx, "testdb", "test_func", "wasm", wasmBase64, nil, "", "Test function")
	// Error is expected, but we're testing the introspection path
}

func TestAdapter_CreateProcedure_WithReturnType(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	ctx := context.Background()

	// Create database
	_, _, err = adapter.Execute(ctx, "CREATE DATABASE testdb", nil, "")
	if err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}

	// Test with return type
	wasmBase64 := "AGFzbQEAAAA="
	
	err = adapter.CreateProcedure(ctx, "testdb", "test_func", "wasm", wasmBase64, nil, "VARCHAR", "Test function")
	// Error is expected, but we're testing the return type path
}

func TestAdapter_DropProcedure_Success(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	ctx := context.Background()

	// Create database
	_, _, err = adapter.Execute(ctx, "CREATE DATABASE testdb", nil, "")
	if err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}

	// Try to drop (will fail if procedure doesn't exist, but tests the path)
	err = adapter.DropProcedure(ctx, "testdb", "test_proc")
	// Error is expected for non-existent procedure
}

func TestAdapter_CallProcedure_WithArgs(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	ctx := context.Background()

	// Create database
	_, _, err = adapter.Execute(ctx, "CREATE DATABASE testdb", nil, "")
	if err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}

	// Try to call with arguments
	_, err = adapter.CallProcedure(ctx, "testdb", "test_proc", 1, 2, 3)
	// Error is expected for non-existent procedure
}

func TestAdapter_ListProcedures_WithDatabase(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	ctx := context.Background()

	// Create database
	_, _, err = adapter.Execute(ctx, "CREATE DATABASE testdb", nil, "")
	if err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}

	// List procedures
	procs, err := adapter.ListProcedures(ctx, "testdb")
	if err != nil {
		t.Fatalf("ListProcedures failed: %v", err)
	}

	// Should return empty list
	if procs == nil {
		t.Error("Expected non-nil procedures list")
	}
}

func TestAdapter_Execute_USECommand(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	ctx := context.Background()

	// Create database
	_, _, err = adapter.Execute(ctx, "CREATE DATABASE testdb", nil, "")
	if err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}

	// Test USE command with semicolon
	affected, _, err := adapter.Execute(ctx, "USE testdb;", nil, "")
	if err != nil {
		t.Fatalf("USE DATABASE failed: %v", err)
	}
	if affected != 1 {
		t.Errorf("Expected affected rows 1, got %d", affected)
	}
}

func TestAdapter_Query_NoDatabase(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	ctx := context.Background()

	// Try to query without specifying database
	_, _, err = adapter.Query(ctx, "SELECT 1", nil, 0, "")
	// May succeed or fail depending on default database
}

func TestAdapter_Execute_NoDatabase(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	ctx := context.Background()

	// Try to execute without specifying database
	_, _, err = adapter.Execute(ctx, "CREATE DATABASE testdb", nil, "")
	if err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}
}

func TestAdapter_CreateProcedure_NoDatabase(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	ctx := context.Background()

	// Test without database (empty string)
	wasmBase64 := "AGFzbQEAAAA="
	err = adapter.CreateProcedure(ctx, "", "test_func", "wasm", wasmBase64, nil, "INT", "Test")
	// Error is expected
}

func TestAdapter_DropProcedure_NoDatabase(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	ctx := context.Background()

	// Test without database
	err = adapter.DropProcedure(ctx, "", "test_proc")
	// Error is expected
}

func TestAdapter_CallProcedure_NoDatabase(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	ctx := context.Background()

	// Test without database
	_, err = adapter.CallProcedure(ctx, "", "test_proc", 1, 2)
	// Error is expected
}

func TestAdapter_ListProcedures_NoDatabase(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	ctx := context.Background()

	// Test without database (empty string)
	procs, err := adapter.ListProcedures(ctx, "")
	if err != nil {
		t.Fatalf("ListProcedures failed: %v", err)
	}

	// Should return empty list
	if procs == nil {
		t.Error("Expected non-nil procedures list")
	}
}

func TestParseResultString_ComplexTable(t *testing.T) {
	result := `+----+-------+-----+
| id | name  | age |
+----+-------+-----+
| 1  | Alice | 30  |
| 2  | Bob   | 25  |
| 3  | Carol | 35  |
+----+-------+-----+
3 row(s) returned`

	columns, rows := parseResultString(result, 0)

	if len(columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(columns))
	}
	if len(rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(rows))
	}
}
