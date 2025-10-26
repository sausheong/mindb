package mindb

import (
	"testing"
)

// ============================================================================
// ENGINE ADAPTER ADDITIONAL TESTS - MISSING COVERAGE
// ============================================================================

func TestEngineAdapter_UseDatabase_Direct(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Create database
	adapter.Execute(&Statement{Type: CreateDatabase, Database: "testdb"})

	// Use database via direct method
	err = adapter.UseDatabase("testdb")
	if err != nil {
		t.Fatalf("UseDatabase failed: %v", err)
	}
}

func TestEngineAdapter_GetWASMEngine_Direct(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Get WASM engine via direct method
	wasmEngine := adapter.GetWASMEngine()
	if wasmEngine == nil {
		t.Error("GetWASMEngine should return WASM engine")
	}
}

func TestEngineAdapter_Close_Direct(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}

	// Close via direct method
	err = adapter.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestEngineAdapter_DropTableIfExists(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Setup
	adapter.Execute(&Statement{Type: CreateDatabase, Database: "testdb"})
	adapter.UseDatabase("testdb")

	// Drop non-existent table with IF EXISTS
	result, err := adapter.Execute(&Statement{
		Type:     DropTable,
		Table:    "nonexistent",
		IfExists: true,
	})
	if err != nil {
		t.Fatalf("DROP TABLE IF EXISTS failed: %v", err)
	}
	if result == "" {
		t.Error("Expected result message")
	}
}

func TestEngineAdapter_SelectWithJoin(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Setup
	adapter.Execute(&Statement{Type: CreateDatabase, Database: "testdb"})
	adapter.UseDatabase("testdb")
	
	// Create tables
	adapter.Execute(&Statement{
		Type:  CreateTable,
		Table: "users",
		Columns: []Column{
			{Name: "id", DataType: "INT"},
			{Name: "name", DataType: "VARCHAR"},
		},
	})
	adapter.Execute(&Statement{
		Type:  CreateTable,
		Table: "orders",
		Columns: []Column{
			{Name: "id", DataType: "INT"},
			{Name: "user_id", DataType: "INT"},
		},
	})

	// Insert data
	adapter.Execute(&Statement{
		Type:  Insert,
		Table: "users",
		Columns: []Column{
			{Name: "id"},
			{Name: "name"},
		},
		Values: [][]interface{}{
			{1, "Alice"},
		},
	})
	adapter.Execute(&Statement{
		Type:  Insert,
		Table: "orders",
		Columns: []Column{
			{Name: "id"},
			{Name: "user_id"},
		},
		Values: [][]interface{}{
			{1, 1},
		},
	})

	// SELECT with JOIN
	result, err := adapter.Execute(&Statement{
		Type:  Select,
		Table: "users",
		Joins: []JoinClause{
			{
				Type:       InnerJoin,
				Table:      "orders",
				OnLeft:     "users.id",
				OnRight:    "orders.user_id",
				OnOperator: "=",
			},
		},
	})
	if err != nil {
		t.Fatalf("SELECT with JOIN failed: %v", err)
	}
	if result == "" {
		t.Error("Expected join result")
	}
}

func TestEngineAdapter_SelectWithAggregates(t *testing.T) {
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
		Table: "sales",
		Columns: []Column{
			{Name: "id", DataType: "INT"},
			{Name: "amount", DataType: "INT"},
		},
	})

	// Insert data
	for i := 0; i < 5; i++ {
		adapter.Execute(&Statement{
			Type:  Insert,
			Table: "sales",
			Columns: []Column{
				{Name: "id"},
				{Name: "amount"},
			},
			Values: [][]interface{}{
				{i, (i + 1) * 100},
			},
		})
	}

	// SELECT with aggregates
	result, err := adapter.Execute(&Statement{
		Type:  Select,
		Table: "sales",
		Aggregates: []AggregateFunc{
			{Type: CountFunc, Column: "*"},
		},
	})
	if err != nil {
		t.Fatalf("SELECT with aggregates failed: %v", err)
	}
	if result == "" {
		t.Error("Expected aggregate result")
	}
}

func TestEngineAdapter_SetCurrentUser(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Create a user
	adapter.Execute(&Statement{
		Type:     CreateUser,
		Username: "testuser",
		Password: "password",
		Host:     "localhost",
	})

	// Set current user - should not panic
	adapter.SetCurrentUser("testuser", "localhost")
	
	// Method executed successfully - we can't check internal state
	// but we verified the method works without error
}

func TestEngineAdapter_Authenticate(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Create a user
	adapter.Execute(&Statement{
		Type:     CreateUser,
		Username: "testuser",
		Password: "password123",
		Host:     "localhost",
	})

	// Authenticate with correct password
	authenticated := adapter.Authenticate("testuser", "password123", "localhost")
	if !authenticated {
		t.Error("Authentication should succeed with correct password")
	}

	// Authenticate with wrong password
	authenticated = adapter.Authenticate("testuser", "wrongpassword", "localhost")
	if authenticated {
		t.Error("Authentication should fail with wrong password")
	}
}

func TestEngineAdapter_LogLoginSuccess(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Log successful login
	adapter.LogLoginSuccess("testuser", "localhost")
	// Should not panic or error
}

func TestEngineAdapter_LogLoginFailed(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Log failed login
	adapter.LogLoginFailed("testuser", "localhost", "invalid password")
	// Should not panic or error
}

func TestEngineAdapter_IsAccountLocked(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Create a user
	adapter.Execute(&Statement{
		Type:     CreateUser,
		Username: "testuser",
		Password: "password",
		Host:     "localhost",
	})

	// Check if account is locked (should be false initially)
	locked := adapter.IsAccountLocked("testuser", "localhost")
	if locked {
		t.Error("Account should not be locked initially")
	}

	// Lock the account by failing authentication multiple times
	for i := 0; i < 5; i++ {
		adapter.Authenticate("testuser", "wrongpassword", "localhost")
	}

	// Check if account is now locked
	locked = adapter.IsAccountLocked("testuser", "localhost")
	if !locked {
		t.Error("Account should be locked after failed attempts")
	}
}

func TestEngineAdapter_CreateProcedureViaAdapter(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Create procedure via adapter method
	proc := &StoredProcedure{
		Name:     "test_proc",
		Code:     []byte("SELECT 1"),
		Language: "sql",
	}
	err = adapter.CreateProcedureViaAdapter(proc)
	if err != nil {
		t.Fatalf("CreateProcedureViaAdapter failed: %v", err)
	}
}

func TestEngineAdapter_DropProcedureViaAdapter(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Create procedure first
	proc := &StoredProcedure{
		Name:     "test_proc",
		Code:     []byte("SELECT 1"),
		Language: "sql",
	}
	adapter.CreateProcedureViaAdapter(proc)

	// Drop procedure via adapter method
	err = adapter.DropProcedureViaAdapter("test_proc")
	if err != nil {
		t.Fatalf("DropProcedureViaAdapter failed: %v", err)
	}
}

func TestEngineAdapter_ListProceduresViaAdapter(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Create a procedure
	proc := &StoredProcedure{
		Name:     "test_proc",
		Code:     []byte("SELECT 1"),
		Language: "sql",
	}
	adapter.CreateProcedureViaAdapter(proc)

	// List procedures
	procs := adapter.ListProceduresViaAdapter()
	if len(procs) == 0 {
		t.Error("Expected at least one procedure")
	}
}

func TestEngineAdapter_CallProcedureViaAdapter(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Create a procedure
	proc := &StoredProcedure{
		Name:     "test_proc",
		Code:     []byte("SELECT 1"),
		Language: "sql",
	}
	adapter.CreateProcedureViaAdapter(proc)

	// Call procedure via adapter method
	result, err := adapter.CallProcedureViaAdapter("test_proc", []interface{}{})
	if err != nil {
		t.Fatalf("CallProcedureViaAdapter failed: %v", err)
	}
	if result == "" {
		t.Log("Procedure returned empty result")
	}
}
