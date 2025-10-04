package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestPostgresCreateDatabaseIfNotExists(t *testing.T) {
	parser := NewParser()
	stmt, err := parser.Parse("CREATE DATABASE IF NOT EXISTS testdb")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if !stmt.IfNotExists {
		t.Error("Expected IfNotExists to be true")
	}

	// Test execution
	engine := NewEngine()
	result, err := engine.Execute(stmt)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	// Create again with IF NOT EXISTS
	result, err = engine.Execute(stmt)
	if err != nil {
		t.Fatalf("Expected no error with IF NOT EXISTS, got: %v", err)
	}
	if !strings.Contains(result, "skipping") {
		t.Errorf("Expected skipping message, got: %s", result)
	}
}

func TestPostgresCreateTableIfNotExists(t *testing.T) {
	parser := NewParser()
	engine := NewEngine()

	// Create database first
	engine.Execute(&Statement{Type: CreateDatabase, Database: "testdb"})

	stmt, err := parser.Parse("CREATE TABLE IF NOT EXISTS users (id INT, name VARCHAR)")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if !stmt.IfNotExists {
		t.Error("Expected IfNotExists to be true")
	}

	// Execute twice
	engine.Execute(stmt)
	result, err := engine.Execute(stmt)
	if err != nil {
		t.Fatalf("Expected no error with IF NOT EXISTS, got: %v", err)
	}
	if !strings.Contains(result, "skipping") {
		t.Errorf("Expected skipping message, got: %s", result)
	}
}

func TestPostgresDropTableIfExists(t *testing.T) {
	parser := NewParser()
	engine := NewEngine()

	engine.Execute(&Statement{Type: CreateDatabase, Database: "testdb"})

	stmt, err := parser.Parse("DROP TABLE IF EXISTS users")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if !stmt.IfExists {
		t.Error("Expected IfExists to be true")
	}

	// Drop non-existent table with IF EXISTS
	result, err := engine.Execute(stmt)
	if err != nil {
		t.Fatalf("Expected no error with IF EXISTS, got: %v", err)
	}
	if !strings.Contains(result, "skipping") {
		t.Errorf("Expected skipping message, got: %s", result)
	}
}

func TestPostgresSchemaQualifiedNames(t *testing.T) {
	parser := NewParser()

	// Test CREATE TABLE with schema
	stmt, err := parser.Parse("CREATE TABLE public.users (id INT, name VARCHAR)")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if stmt.Schema != "public" {
		t.Errorf("Expected schema 'public', got '%s'", stmt.Schema)
	}

	if stmt.Table != "users" {
		t.Errorf("Expected table 'users', got '%s'", stmt.Table)
	}

	// Test SELECT with schema
	stmt, err = parser.Parse("SELECT * FROM public.users")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if stmt.Schema != "public" {
		t.Errorf("Expected schema 'public', got '%s'", stmt.Schema)
	}
}

func TestPostgresColumnConstraints(t *testing.T) {
	parser := NewParser()

	stmt, err := parser.Parse("CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR NOT NULL, email VARCHAR UNIQUE, age INT DEFAULT 0)")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if len(stmt.Columns) != 4 {
		t.Fatalf("Expected 4 columns, got %d", len(stmt.Columns))
	}

	// Check PRIMARY KEY
	if !stmt.Columns[0].PrimaryKey {
		t.Error("Expected id column to have PRIMARY KEY")
	}

	// Check NOT NULL
	if !stmt.Columns[1].NotNull {
		t.Error("Expected name column to have NOT NULL")
	}

	// Check UNIQUE
	if !stmt.Columns[2].Unique {
		t.Error("Expected email column to have UNIQUE")
	}

	// Check DEFAULT
	if stmt.Columns[3].Default == nil {
		t.Error("Expected age column to have DEFAULT value")
	}
}

func TestPostgresLimitOffset(t *testing.T) {
	parser := NewParser()
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

	// Insert test data
	for i := 1; i <= 5; i++ {
		engine.Execute(&Statement{
			Type:    Insert,
			Table:   "users",
			Columns: []Column{{Name: "id"}, {Name: "name"}},
			Values:  [][]interface{}{{i, fmt.Sprintf("User%d", i)}},
		})
	}

	// Test LIMIT
	stmt, err := parser.Parse("SELECT * FROM users LIMIT 2")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if stmt.Limit != 2 {
		t.Errorf("Expected LIMIT 2, got %d", stmt.Limit)
	}

	result, err := engine.Execute(stmt)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	if !strings.Contains(result, "2 row(s) in set") {
		t.Errorf("Expected 2 rows, got: %s", result)
	}

	// Test OFFSET
	stmt, err = parser.Parse("SELECT * FROM users OFFSET 2")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if stmt.Offset != 2 {
		t.Errorf("Expected OFFSET 2, got %d", stmt.Offset)
	}

	result, err = engine.Execute(stmt)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	if !strings.Contains(result, "3 row(s) in set") {
		t.Errorf("Expected 3 rows, got: %s", result)
	}

	// Test LIMIT with OFFSET
	stmt, err = parser.Parse("SELECT * FROM users LIMIT 2 OFFSET 1")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if stmt.Limit != 2 || stmt.Offset != 1 {
		t.Errorf("Expected LIMIT 2 OFFSET 1, got LIMIT %d OFFSET %d", stmt.Limit, stmt.Offset)
	}

	result, err = engine.Execute(stmt)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	if !strings.Contains(result, "2 row(s) in set") {
		t.Errorf("Expected 2 rows, got: %s", result)
	}
}

func TestPostgresReturningClause(t *testing.T) {
	parser := NewParser()
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

	// Test INSERT with RETURNING
	stmt, err := parser.Parse("INSERT INTO users (id, name) VALUES (1, 'John') RETURNING id, name")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if len(stmt.Returning) != 2 {
		t.Errorf("Expected 2 RETURNING columns, got %d", len(stmt.Returning))
	}

	result, err := engine.Execute(stmt)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	if !strings.Contains(result, "John") {
		t.Errorf("Expected RETURNING output to contain 'John', got: %s", result)
	}

	// Test UPDATE with RETURNING
	stmt, err = parser.Parse("UPDATE users SET name = 'Jane' WHERE id = 1 RETURNING name")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if len(stmt.Returning) != 1 {
		t.Errorf("Expected 1 RETURNING column, got %d", len(stmt.Returning))
	}

	result, err = engine.Execute(stmt)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	if !strings.Contains(result, "Jane") {
		t.Errorf("Expected RETURNING output to contain 'Jane', got: %s", result)
	}

	// Test DELETE with RETURNING
	stmt, err = parser.Parse("DELETE FROM users WHERE id = 1 RETURNING *")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	result, err = engine.Execute(stmt)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	if !strings.Contains(result, "Jane") {
		t.Errorf("Expected RETURNING output to contain 'Jane', got: %s", result)
	}
}

func TestPostgresAlterTableColumn(t *testing.T) {
	parser := NewParser()

	// Test with COLUMN keyword
	stmt, err := parser.Parse("ALTER TABLE users ADD COLUMN email VARCHAR")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if stmt.NewColumn.Name != "email" {
		t.Errorf("Expected column name 'email', got '%s'", stmt.NewColumn.Name)
	}

	// Test with data type with size
	stmt, err = parser.Parse("ALTER TABLE users ADD COLUMN description VARCHAR(255)")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if stmt.NewColumn.DataType != "VARCHAR(255)" {
		t.Errorf("Expected data type 'VARCHAR(255)', got '%s'", stmt.NewColumn.DataType)
	}
}

func TestPostgresDefaultValues(t *testing.T) {
	engine := NewEngine()

	// Setup
	engine.Execute(&Statement{Type: CreateDatabase, Database: "testdb"})
	engine.Execute(&Statement{
		Type:  CreateTable,
		Table: "users",
		Columns: []Column{
			{Name: "id", DataType: "INT"},
			{Name: "name", DataType: "VARCHAR"},
			{Name: "status", DataType: "VARCHAR", Default: "active"},
		},
	})

	// Insert without providing status
	engine.Execute(&Statement{
		Type:    Insert,
		Table:   "users",
		Columns: []Column{{Name: "id"}, {Name: "name"}},
		Values:  [][]interface{}{{1, "John"}},
	})

	// Select and verify default value
	result, err := engine.Execute(&Statement{
		Type:  Select,
		Table: "users",
	})

	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	if !strings.Contains(result, "active") {
		t.Errorf("Expected default value 'active' to be present, got: %s", result)
	}
}
