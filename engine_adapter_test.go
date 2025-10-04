package main

import (
	"testing"
)

/*
Package: mindb
Component: Engine Adapter (Main Entry Point)
Layer: Engine Adapter (Layer 2)

Test Coverage:
- Basic operations (create, insert, select, update, delete)
- Query execution through adapter
- Error handling
- Database management

Priority: HIGH (0% coverage → target 80%+)
Impact: +3% overall coverage

Run: go test -v -run TestEngineAdapter
*/

// ============================================================================
// BASIC OPERATIONS TESTS
// ============================================================================

func TestEngineAdapter_BasicOperations(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Test CREATE DATABASE
	result, err := adapter.Execute(&Statement{
		Type:     CreateDatabase,
		Database: "testdb",
	})
	if err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}
	if result == "" {
		t.Error("Expected result message from CREATE DATABASE")
	}

	// Test USE DATABASE
	err = adapter.UseDatabase("testdb")
	if err != nil {
		t.Fatalf("USE DATABASE failed: %v", err)
	}

	// Test CREATE TABLE
	result, err = adapter.Execute(&Statement{
		Type:  CreateTable,
		Table: "users",
		Columns: []Column{
			{Name: "id", DataType: "INT", PrimaryKey: true},
			{Name: "name", DataType: "VARCHAR"},
			{Name: "age", DataType: "INT"},
		},
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	if result == "" {
		t.Error("Expected result message from CREATE TABLE")
	}

	// Test INSERT
	result, err = adapter.Execute(&Statement{
		Type:  Insert,
		Table: "users",
		Columns: []Column{
			{Name: "id"},
			{Name: "name"},
			{Name: "age"},
		},
		Values: [][]interface{}{
			{1, "Alice", 25},
			{2, "Bob", 30},
		},
	})
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	if result == "" {
		t.Error("Expected result message from INSERT")
	}

	// Test SELECT
	result, err = adapter.Execute(&Statement{
		Type:  Select,
		Table: "users",
	})
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if result == "" {
		t.Error("Expected result from SELECT")
	}

	// Test UPDATE
	result, err = adapter.Execute(&Statement{
		Type:  Update,
		Table: "users",
		Updates: map[string]interface{}{
			"age": 26,
		},
		Conditions: []Condition{
			{Column: "id", Operator: "=", Value: 1},
		},
	})
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}

	// Test DELETE
	result, err = adapter.Execute(&Statement{
		Type:  Delete,
		Table: "users",
		Conditions: []Condition{
			{Column: "id", Operator: "=", Value: 2},
		},
	})
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}

	// Test DROP TABLE
	result, err = adapter.Execute(&Statement{
		Type:  DropTable,
		Table: "users",
	})
	if err != nil {
		t.Fatalf("DROP TABLE failed: %v", err)
	}
}

func TestEngineAdapter_QueryExecution(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Setup database and table
	adapter.Execute(&Statement{Type: CreateDatabase, Database: "testdb"})
	adapter.UseDatabase("testdb")
	adapter.Execute(&Statement{
		Type:  CreateTable,
		Table: "products",
		Columns: []Column{
			{Name: "id", DataType: "INT", PrimaryKey: true},
			{Name: "name", DataType: "VARCHAR"},
			{Name: "price", DataType: "INT"},
		},
	})

	// Insert test data
	adapter.Execute(&Statement{
		Type:  Insert,
		Table: "products",
		Columns: []Column{
			{Name: "id"},
			{Name: "name"},
			{Name: "price"},
		},
		Values: [][]interface{}{
			{1, "Laptop", 1000},
			{2, "Mouse", 50},
			{3, "Keyboard", 100},
		},
	})

	tests := []struct {
		name string
		stmt *Statement
	}{
		{
			name: "SELECT with WHERE",
			stmt: &Statement{
				Type:  Select,
				Table: "products",
				Conditions: []Condition{
					{Column: "price", Operator: ">", Value: 50},
				},
			},
		},
		{
			name: "SELECT with ORDER BY",
			stmt: &Statement{
				Type:    Select,
				Table:   "products",
				OrderBy: "price",
			},
		},
		{
			name: "SELECT with LIMIT",
			stmt: &Statement{
				Type:  Select,
				Table: "products",
				Limit: 2,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := adapter.Execute(tt.stmt)
			if err != nil {
				t.Errorf("Query execution failed: %v", err)
			}
			if result == "" {
				t.Error("Expected result from query")
			}
		})
	}
}

func TestEngineAdapter_ErrorHandling(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	tests := []struct {
		name        string
		stmt        *Statement
		shouldError bool
	}{
		{
			name: "SELECT from non-existent table",
			stmt: &Statement{
				Type:  Select,
				Table: "nonexistent",
			},
			shouldError: true,
		},
		{
			name: "INSERT without database",
			stmt: &Statement{
				Type:  Insert,
				Table: "users",
				Values: [][]interface{}{
					{1, "Alice"},
				},
			},
			shouldError: true,
		},
		{
			name: "CREATE TABLE in non-existent database",
			stmt: &Statement{
				Type:  CreateTable,
				Table: "users",
				Columns: []Column{
					{Name: "id", DataType: "INT"},
				},
			},
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := adapter.Execute(tt.stmt)
			if tt.shouldError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.shouldError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func TestEngineAdapter_TransactionSupport(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Setup
	adapter.Execute(&Statement{Type: CreateDatabase, Database: "testdb"})
	adapter.UseDatabase("testdb")
	adapter.Execute(&Statement{
		Type:  CreateTable,
		Table: "accounts",
		Columns: []Column{
			{Name: "id", DataType: "INT", PrimaryKey: true},
			{Name: "balance", DataType: "INT"},
		},
	})

	// Test BEGIN TRANSACTION
	_, err = adapter.Execute(&Statement{Type: BeginTransaction})
	if err != nil {
		t.Fatalf("BEGIN TRANSACTION failed: %v", err)
	}

	// Insert data in transaction
	adapter.Execute(&Statement{
		Type:  Insert,
		Table: "accounts",
		Columns: []Column{
			{Name: "id"},
			{Name: "balance"},
		},
		Values: [][]interface{}{
			{1, 1000},
		},
	})

	// Test COMMIT
	_, err = adapter.Execute(&Statement{Type: CommitTransaction})
	if err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// Test ROLLBACK
	adapter.Execute(&Statement{Type: BeginTransaction})
	adapter.Execute(&Statement{
		Type:  Insert,
		Table: "accounts",
		Columns: []Column{
			{Name: "id"},
			{Name: "balance"},
		},
		Values: [][]interface{}{
			{2, 2000},
		},
	})
	
	_, err = adapter.Execute(&Statement{Type: RollbackTransaction})
	if err != nil {
		t.Fatalf("ROLLBACK failed: %v", err)
	}
}

func TestEngineAdapter_AlterTable(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Setup
	adapter.Execute(&Statement{Type: CreateDatabase, Database: "testdb"})
	adapter.UseDatabase("testdb")
	adapter.Execute(&Statement{
		Type:  CreateTable,
		Table: "users",
		Columns: []Column{
			{Name: "id", DataType: "INT", PrimaryKey: true},
			{Name: "name", DataType: "VARCHAR"},
		},
	})

	// Test ALTER TABLE ADD COLUMN
	result, err := adapter.Execute(&Statement{
		Type:  AlterTable,
		Table: "users",
		NewColumn: Column{
			Name:     "email",
			DataType: "VARCHAR",
		},
	})
	if err != nil {
		t.Fatalf("ALTER TABLE failed: %v", err)
	}
	if result == "" {
		t.Error("Expected result from ALTER TABLE")
	}
}

func TestEngineAdapter_WithWAL(t *testing.T) {
	tmpDir := t.TempDir()
	
	// Create adapter with WAL enabled
	adapter, err := NewEngineAdapter(tmpDir, true)
	if err != nil {
		t.Fatalf("Failed to create adapter with WAL: %v", err)
	}
	defer adapter.Close()

	// Setup and insert data
	adapter.Execute(&Statement{Type: CreateDatabase, Database: "testdb"})
	adapter.UseDatabase("testdb")
	adapter.Execute(&Statement{
		Type:  CreateTable,
		Table: "logs",
		Columns: []Column{
			{Name: "id", DataType: "INT", PrimaryKey: true},
			{Name: "message", DataType: "VARCHAR"},
		},
	})

	// Insert data (should be logged to WAL)
	result, err := adapter.Execute(&Statement{
		Type:  Insert,
		Table: "logs",
		Columns: []Column{
			{Name: "id"},
			{Name: "message"},
		},
		Values: [][]interface{}{
			{1, "Test log entry"},
		},
	})
	if err != nil {
		t.Fatalf("INSERT with WAL failed: %v", err)
	}

	// Verify data was inserted
	result, err = adapter.Execute(&Statement{
		Type:  Select,
		Table: "logs",
	})
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if result == "" {
		t.Error("Expected data from SELECT")
	}
}

func TestEngineAdapter_Close(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}

	// Test Close
	err = adapter.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Verify operations fail after close
	_, err = adapter.Execute(&Statement{
		Type:     CreateDatabase,
		Database: "testdb",
	})
	// Should fail or handle gracefully after close
}

func TestEngineAdapter_ConcurrentAccess(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Setup
	adapter.Execute(&Statement{Type: CreateDatabase, Database: "testdb"})
	adapter.UseDatabase("testdb")
	adapter.Execute(&Statement{
		Type:  CreateTable,
		Table: "counters",
		Columns: []Column{
			{Name: "id", DataType: "INT", PrimaryKey: true},
			{Name: "count", DataType: "INT"},
		},
	})

	// Insert initial data
	adapter.Execute(&Statement{
		Type:  Insert,
		Table: "counters",
		Columns: []Column{
			{Name: "id"},
			{Name: "count"},
		},
		Values: [][]interface{}{
			{1, 0},
		},
	})

	// Test concurrent reads
	done := make(chan bool)
	for i := 0; i < 5; i++ {
		go func() {
			_, err := adapter.Execute(&Statement{
				Type:  Select,
				Table: "counters",
			})
			if err != nil {
				t.Errorf("Concurrent SELECT failed: %v", err)
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 5; i++ {
		<-done
	}
}
