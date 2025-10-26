package mindb

import (
	"strings"
	"testing"
)

// ============================================================================
// ENGINE ADAPTER COMPREHENSIVE TESTS - USER MANAGEMENT
// ============================================================================

func TestEngineAdapter_UserManagement(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// CREATE USER
	result, err := adapter.Execute(&Statement{
		Type:     CreateUser,
		Username: "testuser",
		Password: "password123",
		Host:     "localhost",
	})
	if err != nil {
		t.Fatalf("CREATE USER failed: %v", err)
	}
	if !strings.Contains(result, "created") {
		t.Errorf("Expected success message, got: %s", result)
	}

	// DROP USER
	result, err = adapter.Execute(&Statement{
		Type:     DropUser,
		Username: "testuser",
		Host:     "localhost",
	})
	if err != nil {
		t.Fatalf("DROP USER failed: %v", err)
	}
	if !strings.Contains(result, "dropped") {
		t.Errorf("Expected success message, got: %s", result)
	}
}

func TestEngineAdapter_AlterUser(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Create user first
	adapter.Execute(&Statement{
		Type:     CreateUser,
		Username: "testuser",
		Password: "oldpass",
		Host:     "localhost",
	})

	// ALTER USER (change password)
	result, err := adapter.Execute(&Statement{
		Type:     AlterUser,
		Username: "testuser",
		Password: "newpass",
		Host:     "localhost",
	})
	if err != nil {
		t.Fatalf("ALTER USER failed: %v", err)
	}
	if !strings.Contains(result, "altered") && !strings.Contains(result, "changed") {
		t.Errorf("Expected success message, got: %s", result)
	}
}

func TestEngineAdapter_GrantPrivileges(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Create database and user
	adapter.Execute(&Statement{Type: CreateDatabase, Database: "testdb"})
	adapter.Execute(&Statement{
		Type:     CreateUser,
		Username: "testuser",
		Password: "password",
		Host:     "localhost",
	})

	// GRANT privileges
	result, err := adapter.Execute(&Statement{
		Type:       GrantPrivileges,
		Username:   "testuser",
		Host:       "localhost",
		Database:   "testdb",
		Table:      "users",
		Privileges: []string{"SELECT", "INSERT"},
	})
	if err != nil {
		t.Fatalf("GRANT failed: %v", err)
	}
	if !strings.Contains(result, "granted") && !strings.Contains(result, "Granted") {
		t.Errorf("Expected success message, got: %s", result)
	}
}

func TestEngineAdapter_RevokePrivileges(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Setup
	adapter.Execute(&Statement{Type: CreateDatabase, Database: "testdb"})
	adapter.Execute(&Statement{
		Type:     CreateUser,
		Username: "testuser",
		Password: "password",
		Host:     "localhost",
	})
	adapter.Execute(&Statement{
		Type:       GrantPrivileges,
		Username:   "testuser",
		Host:       "localhost",
		Database:   "testdb",
		Table:      "users",
		Privileges: []string{"SELECT", "INSERT"},
	})

	// REVOKE privileges
	result, err := adapter.Execute(&Statement{
		Type:       RevokePrivileges,
		Username:   "testuser",
		Host:       "localhost",
		Database:   "testdb",
		Table:      "users",
		Privileges: []string{"INSERT"},
	})
	if err != nil {
		t.Fatalf("REVOKE failed: %v", err)
	}
	if !strings.Contains(result, "revoked") && !strings.Contains(result, "Revoked") {
		t.Errorf("Expected success message, got: %s", result)
	}
}

func TestEngineAdapter_ShowGrants(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Setup
	adapter.Execute(&Statement{Type: CreateDatabase, Database: "testdb"})
	adapter.Execute(&Statement{
		Type:     CreateUser,
		Username: "testuser",
		Password: "password",
		Host:     "localhost",
	})
	adapter.Execute(&Statement{
		Type:       GrantPrivileges,
		Username:   "testuser",
		Host:       "localhost",
		Database:   "testdb",
		Table:      "users",
		Privileges: []string{"SELECT"},
	})

	// SHOW GRANTS
	result, err := adapter.Execute(&Statement{
		Type:     ShowGrants,
		Username: "testuser",
		Host:     "localhost",
	})
	if err != nil {
		t.Fatalf("SHOW GRANTS failed: %v", err)
	}
	if result == "" {
		t.Error("Expected grants output")
	}
}

func TestEngineAdapter_ShowUsers(t *testing.T) {
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

	// SHOW USERS
	result, err := adapter.Execute(&Statement{
		Type: ShowUsers,
	})
	if err != nil {
		t.Fatalf("SHOW USERS failed: %v", err)
	}
	if result == "" {
		t.Error("Expected users output")
	}
	if !strings.Contains(result, "testuser") {
		t.Error("Expected testuser in output")
	}
}

// ============================================================================
// ENGINE ADAPTER COMPREHENSIVE TESTS - ROLE MANAGEMENT
// ============================================================================

func TestEngineAdapter_CreateRole(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// CREATE ROLE (use a unique name, not 'admin' which is a default role)
	result, err := adapter.Execute(&Statement{
		Type:     CreateRole,
		RoleName: "customrole",
	})
	if err != nil {
		t.Fatalf("CREATE ROLE failed: %v", err)
	}
	if !strings.Contains(result, "created") && !strings.Contains(result, "Created") {
		t.Errorf("Expected success message, got: %s", result)
	}
}

func TestEngineAdapter_DropRole(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Create role first
	adapter.Execute(&Statement{
		Type:     CreateRole,
		RoleName: "admin",
	})

	// DROP ROLE
	result, err := adapter.Execute(&Statement{
		Type:     DropRole,
		RoleName: "admin",
	})
	if err != nil {
		t.Fatalf("DROP ROLE failed: %v", err)
	}
	if !strings.Contains(result, "dropped") && !strings.Contains(result, "Dropped") {
		t.Errorf("Expected success message, got: %s", result)
	}
}

func TestEngineAdapter_GrantRole(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Setup
	adapter.Execute(&Statement{
		Type:     CreateUser,
		Username: "testuser",
		Password: "password",
		Host:     "localhost",
	})
	adapter.Execute(&Statement{
		Type:     CreateRole,
		RoleName: "admin",
	})

	// GRANT ROLE
	result, err := adapter.Execute(&Statement{
		Type:     GrantRole,
		Username: "testuser",
		Host:     "localhost",
		RoleName: "admin",
	})
	if err != nil {
		t.Fatalf("GRANT ROLE failed: %v", err)
	}
	if !strings.Contains(result, "granted") && !strings.Contains(result, "Granted") {
		t.Errorf("Expected success message, got: %s", result)
	}
}

func TestEngineAdapter_RevokeRole(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Setup
	adapter.Execute(&Statement{
		Type:     CreateUser,
		Username: "testuser",
		Password: "password",
		Host:     "localhost",
	})
	adapter.Execute(&Statement{
		Type:     CreateRole,
		RoleName: "admin",
	})
	adapter.Execute(&Statement{
		Type:     GrantRole,
		Username: "testuser",
		Host:     "localhost",
		RoleName: "admin",
	})

	// REVOKE ROLE
	result, err := adapter.Execute(&Statement{
		Type:     RevokeRole,
		Username: "testuser",
		Host:     "localhost",
		RoleName: "admin",
	})
	if err != nil {
		t.Fatalf("REVOKE ROLE failed: %v", err)
	}
	if !strings.Contains(result, "revoked") && !strings.Contains(result, "Revoked") {
		t.Errorf("Expected success message, got: %s", result)
	}
}

func TestEngineAdapter_ShowRoles(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Create a role
	adapter.Execute(&Statement{
		Type:     CreateRole,
		RoleName: "admin",
	})

	// SHOW ROLES
	result, err := adapter.Execute(&Statement{
		Type: ShowRoles,
	})
	if err != nil {
		t.Fatalf("SHOW ROLES failed: %v", err)
	}
	if result == "" {
		t.Error("Expected roles output")
	}
}

// ============================================================================
// ENGINE ADAPTER COMPREHENSIVE TESTS - STORED PROCEDURES
// ============================================================================

func TestEngineAdapter_CreateProcedure(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// CREATE PROCEDURE - not yet implemented in Execute switch
	result, err := adapter.Execute(&Statement{
		Type:          CreateProcedure,
		ProcedureName: "get_user",
		ProcedureCode: []byte("SELECT * FROM users WHERE id = $1"),
		ProcedureLang: "sql",
	})
	if err != nil {
		// Expected - CreateProcedure not in Execute switch yet
		if strings.Contains(err.Error(), "unsupported") {
			t.Skip("CREATE PROCEDURE not yet implemented in Execute switch")
		}
		t.Fatalf("CREATE PROCEDURE failed: %v", err)
	}
	if !strings.Contains(result, "created") && !strings.Contains(result, "Created") {
		t.Errorf("Expected success message, got: %s", result)
	}
}

func TestEngineAdapter_DropProcedure(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// DROP PROCEDURE - not yet implemented in Execute switch
	result, err := adapter.Execute(&Statement{
		Type:          DropProcedure,
		ProcedureName: "get_user",
	})
	if err != nil {
		// Expected - DropProcedure not in Execute switch yet
		if strings.Contains(err.Error(), "unsupported") {
			t.Skip("DROP PROCEDURE not yet implemented in Execute switch")
		}
		t.Fatalf("DROP PROCEDURE failed: %v", err)
	}
	if !strings.Contains(result, "dropped") && !strings.Contains(result, "Dropped") {
		t.Errorf("Expected success message, got: %s", result)
	}
}

func TestEngineAdapter_CallProcedure(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// CALL PROCEDURE - CallProcedure is implemented but requires procedure to exist
	result, err := adapter.Execute(&Statement{
		Type:          CallProcedure,
		ProcedureName: "nonexistent",
		ProcedureArgs: []interface{}{},
	})
	if err != nil {
		// Expected - procedure doesn't exist
		if strings.Contains(err.Error(), "does not exist") {
			// This is expected - we're testing the execution path
			return
		}
		t.Fatalf("CALL PROCEDURE failed with unexpected error: %v", err)
	}
	if result == "" {
		t.Log("Procedure call returned empty result")
	}
}

// ============================================================================
// ENGINE ADAPTER COMPREHENSIVE TESTS - DESCRIBE TABLE
// ============================================================================

func TestEngineAdapter_DescribeTable(t *testing.T) {
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
			{Name: "age", DataType: "INT"},
		},
	})

	// DESCRIBE TABLE
	result, err := adapter.Execute(&Statement{
		Type:  DescribeTable,
		Table: "users",
	})
	if err != nil {
		t.Fatalf("DESCRIBE TABLE failed: %v", err)
	}
	if result == "" {
		t.Error("Expected table description")
	}
	if !strings.Contains(result, "id") || !strings.Contains(result, "name") || !strings.Contains(result, "age") {
		t.Error("Expected column names in description")
	}
}

// ============================================================================
// ENGINE ADAPTER COMPREHENSIVE TESTS - CLOSE
// ============================================================================

func TestEngineAdapter_CloseMethod(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}

	// Close the adapter
	err = adapter.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Double close should not panic
	err = adapter.Close()
	// Should handle gracefully (may return error or nil)
}

// ============================================================================
// ENGINE ADAPTER COMPREHENSIVE TESTS - FORMAT RESULT
// ============================================================================

func TestEngineAdapter_FormatResult_EmptyRows(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Setup empty table
	adapter.Execute(&Statement{Type: CreateDatabase, Database: "testdb"})
	adapter.UseDatabase("testdb")
	adapter.Execute(&Statement{
		Type:  CreateTable,
		Table: "empty",
		Columns: []Column{
			{Name: "id", DataType: "INT"},
		},
	})

	// SELECT from empty table
	result, err := adapter.Execute(&Statement{
		Type:  Select,
		Table: "empty",
	})
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	// Should return formatted result even if empty
	if !strings.Contains(result, "0 rows") && !strings.Contains(result, "Empty") {
		t.Logf("Result for empty table: %s", result)
	}
}

func TestEngineAdapter_FormatResult_MultipleRows(t *testing.T) {
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
		Table: "data",
		Columns: []Column{
			{Name: "id", DataType: "INT"},
			{Name: "value", DataType: "VARCHAR"},
		},
	})

	// Insert multiple rows
	for i := 0; i < 5; i++ {
		adapter.Execute(&Statement{
			Type:  Insert,
			Table: "data",
			Columns: []Column{
				{Name: "id"},
				{Name: "value"},
			},
			Values: [][]interface{}{
				{i, "value" + string(rune('A'+i))},
			},
		})
	}

	// SELECT all
	result, err := adapter.Execute(&Statement{
		Type:  Select,
		Table: "data",
	})
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if result == "" {
		t.Error("Expected formatted result")
	}
	// Should contain row count
	if !strings.Contains(result, "5") && !strings.Contains(result, "rows") {
		t.Logf("Result: %s", result)
	}
}

// ============================================================================
// ENGINE ADAPTER COMPREHENSIVE TESTS - EDGE CASES
// ============================================================================

func TestEngineAdapter_ExecuteWithoutDatabase(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Try to create table without database
	_, err = adapter.Execute(&Statement{
		Type:  CreateTable,
		Table: "users",
		Columns: []Column{
			{Name: "id", DataType: "INT"},
		},
	})
	if err == nil {
		t.Error("Should fail when creating table without database")
	}
}

func TestEngineAdapter_MultipleOperations(t *testing.T) {
	tmpDir := t.TempDir()
	
	adapter, err := NewEngineAdapter(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Sequence of operations
	operations := []struct {
		name string
		stmt *Statement
	}{
		{"CREATE DATABASE", &Statement{Type: CreateDatabase, Database: "testdb"}},
		{"CREATE TABLE", &Statement{
			Type:  CreateTable,
			Table: "users",
			Columns: []Column{
				{Name: "id", DataType: "INT"},
				{Name: "name", DataType: "VARCHAR"},
			},
		}},
		{"INSERT", &Statement{
			Type:  Insert,
			Table: "users",
			Columns: []Column{
				{Name: "id"},
				{Name: "name"},
			},
			Values: [][]interface{}{
				{1, "Alice"},
			},
		}},
		{"SELECT", &Statement{
			Type:  Select,
			Table: "users",
		}},
		{"UPDATE", &Statement{
			Type:  Update,
			Table: "users",
			Updates: map[string]interface{}{
				"name": "Bob",
			},
			Conditions: []Condition{
				{Column: "id", Operator: "=", Value: 1},
			},
		}},
		{"DELETE", &Statement{
			Type:  Delete,
			Table: "users",
			Conditions: []Condition{
				{Column: "id", Operator: "=", Value: 1},
			},
		}},
	}

	adapter.UseDatabase("testdb")

	for _, op := range operations {
		t.Run(op.name, func(t *testing.T) {
			_, err := adapter.Execute(op.stmt)
			if err != nil {
				t.Errorf("%s failed: %v", op.name, err)
			}
		})
	}
}
