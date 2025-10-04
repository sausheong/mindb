package main

import (
	"testing"
)

func TestConstraintValidation(t *testing.T) {
	tmpDir := t.TempDir()
	
	// Create engine
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()
	
	// Create database and table
	engine.CreateDatabase("testdb")
	engine.UseDatabase("testdb")
	
	columns := []Column{
		{Name: "id", DataType: "INT", PrimaryKey: true},
		{Name: "email", DataType: "VARCHAR", Unique: true},
		{Name: "name", DataType: "VARCHAR", NotNull: true},
	}
	
	err = engine.CreateTable("users", columns)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	
	// Test PRIMARY KEY constraint
	t.Run("PRIMARY KEY uniqueness", func(t *testing.T) {
		// First insert should succeed
		err := engine.InsertRow("users", Row{"id": 1, "email": "alice@test.com", "name": "Alice"})
		if err != nil {
			t.Errorf("First insert failed: %v", err)
		}
		
		// Second insert with same ID should fail
		err = engine.InsertRow("users", Row{"id": 1, "email": "bob@test.com", "name": "Bob"})
		if err == nil {
			t.Error("Expected PRIMARY KEY violation, but insert succeeded")
		} else {
			t.Logf("Correctly rejected duplicate PRIMARY KEY: %v", err)
		}
	})
	
	// Test UNIQUE constraint
	t.Run("UNIQUE constraint", func(t *testing.T) {
		// Insert with different ID but same email should fail
		err := engine.InsertRow("users", Row{"id": 2, "email": "alice@test.com", "name": "Charlie"})
		if err == nil {
			t.Error("Expected UNIQUE violation, but insert succeeded")
		} else {
			t.Logf("Correctly rejected duplicate UNIQUE value: %v", err)
		}
	})
	
	// Test NOT NULL constraint
	t.Run("NOT NULL constraint", func(t *testing.T) {
		// Insert with null name should fail
		err := engine.InsertRow("users", Row{"id": 3, "email": "dave@test.com", "name": nil})
		if err == nil {
			t.Error("Expected NOT NULL violation, but insert succeeded")
		} else {
			t.Logf("Correctly rejected NULL value: %v", err)
		}
	})
}
