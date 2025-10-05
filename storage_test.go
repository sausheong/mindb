package mindb

import (
	"strings"
	"testing"
)

func TestCreateDatabase(t *testing.T) {
	engine := NewEngine()
	stmt := &Statement{
		Type:     CreateDatabase,
		Database: "testdb",
	}

	result, err := engine.Execute(stmt)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	if !strings.Contains(result, "created successfully") {
		t.Errorf("Expected success message, got: %s", result)
	}

	// Try creating the same database again
	_, err = engine.Execute(stmt)
	if err == nil {
		t.Error("Expected error when creating duplicate database")
	}
}

func TestCreateTable(t *testing.T) {
	engine := NewEngine()

	// First create a database
	engine.Execute(&Statement{
		Type:     CreateDatabase,
		Database: "testdb",
	})

	stmt := &Statement{
		Type:  CreateTable,
		Table: "users",
		Columns: []Column{
			{Name: "id", DataType: "INT"},
			{Name: "name", DataType: "VARCHAR"},
		},
	}

	result, err := engine.Execute(stmt)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	if !strings.Contains(result, "created successfully") {
		t.Errorf("Expected success message, got: %s", result)
	}
}

func TestInsertAndSelect(t *testing.T) {
	engine := NewEngine()

	// Setup
	engine.Execute(&Statement{Type: CreateDatabase, Database: "testdb"})
	engine.Execute(&Statement{
		Type:  CreateTable,
		Table: "users",
		Columns: []Column{
			{Name: "id", DataType: "INT"},
			{Name: "name", DataType: "VARCHAR"},
			{Name: "age", DataType: "INT"},
		},
	})

	// Insert data
	insertStmt := &Statement{
		Type:  Insert,
		Table: "users",
		Columns: []Column{
			{Name: "id"},
			{Name: "name"},
			{Name: "age"},
		},
		Values: [][]interface{}{
			{1, "John", 30},
		},
	}

	result, err := engine.Execute(insertStmt)
	if err != nil {
		t.Fatalf("Insert error: %v", err)
	}

	if !strings.Contains(result, "1 row inserted") {
		t.Errorf("Expected insert success message, got: %s", result)
	}

	// Select data
	selectStmt := &Statement{
		Type:  Select,
		Table: "users",
	}

	result, err = engine.Execute(selectStmt)
	if err != nil {
		t.Fatalf("Select error: %v", err)
	}

	if !strings.Contains(result, "John") {
		t.Errorf("Expected to find 'John' in result, got: %s", result)
	}
}

func TestSelectWithCondition(t *testing.T) {
	engine := NewEngine()

	// Setup
	engine.Execute(&Statement{Type: CreateDatabase, Database: "testdb"})
	engine.Execute(&Statement{
		Type:  CreateTable,
		Table: "users",
		Columns: []Column{
			{Name: "id", DataType: "INT"},
			{Name: "name", DataType: "VARCHAR"},
			{Name: "age", DataType: "INT"},
		},
	})

	// Insert multiple rows
	engine.Execute(&Statement{
		Type:    Insert,
		Table:   "users",
		Columns: []Column{{Name: "id"}, {Name: "name"}, {Name: "age"}},
		Values:  [][]interface{}{{1, "John", 30}},
	})
	engine.Execute(&Statement{
		Type:    Insert,
		Table:   "users",
		Columns: []Column{{Name: "id"}, {Name: "name"}, {Name: "age"}},
		Values:  [][]interface{}{{2, "Jane", 25}},
	})

	// Select with condition
	selectStmt := &Statement{
		Type:  Select,
		Table: "users",
		Conditions: []Condition{
			{Column: "age", Operator: ">", Value: 26},
		},
	}

	result, err := engine.Execute(selectStmt)
	if err != nil {
		t.Fatalf("Select error: %v", err)
	}

	if !strings.Contains(result, "John") {
		t.Errorf("Expected to find 'John' in result")
	}

	if strings.Contains(result, "Jane") {
		t.Errorf("Did not expect to find 'Jane' in result")
	}
}

func TestUpdate(t *testing.T) {
	engine := NewEngine()

	// Setup
	engine.Execute(&Statement{Type: CreateDatabase, Database: "testdb"})
	engine.Execute(&Statement{
		Type:  CreateTable,
		Table: "users",
		Columns: []Column{
			{Name: "id", DataType: "INT"},
			{Name: "name", DataType: "VARCHAR"},
			{Name: "age", DataType: "INT"},
		},
	})
	engine.Execute(&Statement{
		Type:    Insert,
		Table:   "users",
		Columns: []Column{{Name: "id"}, {Name: "name"}, {Name: "age"}},
		Values:  [][]interface{}{{1, "John", 30}},
	})

	// Update
	updateStmt := &Statement{
		Type:  Update,
		Table: "users",
		Updates: map[string]interface{}{
			"age": 31,
		},
		Conditions: []Condition{
			{Column: "id", Operator: "=", Value: 1},
		},
	}

	result, err := engine.Execute(updateStmt)
	if err != nil {
		t.Fatalf("Update error: %v", err)
	}

	if !strings.Contains(result, "1 row(s) updated") {
		t.Errorf("Expected update success message, got: %s", result)
	}

	// Verify update
	selectStmt := &Statement{
		Type:  Select,
		Table: "users",
	}

	result, err = engine.Execute(selectStmt)
	if err != nil {
		t.Fatalf("Select error: %v", err)
	}

	if !strings.Contains(result, "31") {
		t.Errorf("Expected to find updated age '31' in result")
	}
}

func TestDelete(t *testing.T) {
	engine := NewEngine()

	// Setup
	engine.Execute(&Statement{Type: CreateDatabase, Database: "testdb"})
	engine.Execute(&Statement{
		Type:  CreateTable,
		Table: "users",
		Columns: []Column{
			{Name: "id", DataType: "INT"},
			{Name: "name", DataType: "VARCHAR"},
		},
	})
	engine.Execute(&Statement{
		Type:    Insert,
		Table:   "users",
		Columns: []Column{{Name: "id"}, {Name: "name"}},
		Values:  [][]interface{}{{1, "John"}},
	})

	// Delete
	deleteStmt := &Statement{
		Type:  Delete,
		Table: "users",
		Conditions: []Condition{
			{Column: "id", Operator: "=", Value: 1},
		},
	}

	result, err := engine.Execute(deleteStmt)
	if err != nil {
		t.Fatalf("Delete error: %v", err)
	}

	if !strings.Contains(result, "1 row(s) deleted") {
		t.Errorf("Expected delete success message, got: %s", result)
	}

	// Verify deletion
	selectStmt := &Statement{
		Type:  Select,
		Table: "users",
	}

	result, err = engine.Execute(selectStmt)
	if err != nil {
		t.Fatalf("Select error: %v", err)
	}

	if strings.Contains(result, "John") {
		t.Errorf("Did not expect to find 'John' after deletion")
	}
}

func TestAlterTable(t *testing.T) {
	engine := NewEngine()

	// Setup
	engine.Execute(&Statement{Type: CreateDatabase, Database: "testdb"})
	engine.Execute(&Statement{
		Type:  CreateTable,
		Table: "users",
		Columns: []Column{
			{Name: "id", DataType: "INT"},
			{Name: "name", DataType: "VARCHAR"},
		},
	})

	// Alter table
	alterStmt := &Statement{
		Type:  AlterTable,
		Table: "users",
		NewColumn: Column{
			Name:     "email",
			DataType: "VARCHAR",
		},
	}

	result, err := engine.Execute(alterStmt)
	if err != nil {
		t.Fatalf("Alter error: %v", err)
	}

	if !strings.Contains(result, "added") {
		t.Errorf("Expected alter success message, got: %s", result)
	}
}

func TestDropTable(t *testing.T) {
	engine := NewEngine()

	// Setup
	engine.Execute(&Statement{Type: CreateDatabase, Database: "testdb"})
	engine.Execute(&Statement{
		Type:  CreateTable,
		Table: "users",
		Columns: []Column{
			{Name: "id", DataType: "INT"},
		},
	})

	// Drop table
	dropStmt := &Statement{
		Type:  DropTable,
		Table: "users",
	}

	result, err := engine.Execute(dropStmt)
	if err != nil {
		t.Fatalf("Drop error: %v", err)
	}

	if !strings.Contains(result, "dropped successfully") {
		t.Errorf("Expected drop success message, got: %s", result)
	}

	// Try to select from dropped table
	selectStmt := &Statement{
		Type:  Select,
		Table: "users",
	}

	_, err = engine.Execute(selectStmt)
	if err == nil {
		t.Error("Expected error when selecting from dropped table")
	}
}
