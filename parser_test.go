package main

import (
	"testing"
)

func TestParseCreateDatabase(t *testing.T) {
	parser := NewParser()
	stmt, err := parser.Parse("CREATE DATABASE testdb")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if stmt.Type != CreateDatabase {
		t.Errorf("Expected CreateDatabase, got %v", stmt.Type)
	}

	if stmt.Database != "testdb" {
		t.Errorf("Expected database name 'testdb', got '%s'", stmt.Database)
	}
}

func TestParseCreateTable(t *testing.T) {
	parser := NewParser()
	stmt, err := parser.Parse("CREATE TABLE users (id INT, name VARCHAR, age INT)")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if stmt.Type != CreateTable {
		t.Errorf("Expected CreateTable, got %v", stmt.Type)
	}

	if stmt.Table != "users" {
		t.Errorf("Expected table name 'users', got '%s'", stmt.Table)
	}

	if len(stmt.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(stmt.Columns))
	}

	expectedColumns := []struct {
		name     string
		dataType string
	}{
		{"id", "INT"},
		{"name", "VARCHAR"},
		{"age", "INT"},
	}

	for i, expected := range expectedColumns {
		if stmt.Columns[i].Name != expected.name {
			t.Errorf("Column %d: expected name '%s', got '%s'", i, expected.name, stmt.Columns[i].Name)
		}
		if stmt.Columns[i].DataType != expected.dataType {
			t.Errorf("Column %d: expected type '%s', got '%s'", i, expected.dataType, stmt.Columns[i].DataType)
		}
	}
}

func TestParseAlterTable(t *testing.T) {
	parser := NewParser()
	stmt, err := parser.Parse("ALTER TABLE users ADD email VARCHAR")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if stmt.Type != AlterTable {
		t.Errorf("Expected AlterTable, got %v", stmt.Type)
	}

	if stmt.Table != "users" {
		t.Errorf("Expected table name 'users', got '%s'", stmt.Table)
	}

	if stmt.NewColumn.Name != "email" {
		t.Errorf("Expected column name 'email', got '%s'", stmt.NewColumn.Name)
	}

	if stmt.NewColumn.DataType != "VARCHAR" {
		t.Errorf("Expected data type 'VARCHAR', got '%s'", stmt.NewColumn.DataType)
	}
}

func TestParseDropTable(t *testing.T) {
	parser := NewParser()
	stmt, err := parser.Parse("DROP TABLE users")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if stmt.Type != DropTable {
		t.Errorf("Expected DropTable, got %v", stmt.Type)
	}

	if stmt.Table != "users" {
		t.Errorf("Expected table name 'users', got '%s'", stmt.Table)
	}
}

func TestParseInsert(t *testing.T) {
	parser := NewParser()
	stmt, err := parser.Parse("INSERT INTO users (id, name, age) VALUES (1, 'John', 30)")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if stmt.Type != Insert {
		t.Errorf("Expected Insert, got %v", stmt.Type)
	}

	if stmt.Table != "users" {
		t.Errorf("Expected table name 'users', got '%s'", stmt.Table)
	}

	if len(stmt.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(stmt.Columns))
	}

	if len(stmt.Values) != 1 || len(stmt.Values[0]) != 3 {
		t.Errorf("Expected 1 row with 3 values")
	}
}

func TestParseSelect(t *testing.T) {
	parser := NewParser()
	stmt, err := parser.Parse("SELECT id, name FROM users WHERE age > 25")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if stmt.Type != Select {
		t.Errorf("Expected Select, got %v", stmt.Type)
	}

	if stmt.Table != "users" {
		t.Errorf("Expected table name 'users', got '%s'", stmt.Table)
	}

	if len(stmt.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(stmt.Columns))
	}

	if len(stmt.Conditions) != 1 {
		t.Errorf("Expected 1 condition, got %d", len(stmt.Conditions))
	}

	if stmt.Conditions[0].Column != "age" {
		t.Errorf("Expected condition column 'age', got '%s'", stmt.Conditions[0].Column)
	}

	if stmt.Conditions[0].Operator != ">" {
		t.Errorf("Expected operator '>', got '%s'", stmt.Conditions[0].Operator)
	}
}

func TestParseSelectWithOrderBy(t *testing.T) {
	parser := NewParser()
	stmt, err := parser.Parse("SELECT * FROM users ORDER BY age DESC")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if stmt.Type != Select {
		t.Errorf("Expected Select, got %v", stmt.Type)
	}

	if stmt.OrderBy != "age" {
		t.Errorf("Expected ORDER BY 'age', got '%s'", stmt.OrderBy)
	}

	if !stmt.OrderDesc {
		t.Errorf("Expected DESC order")
	}
}

func TestParseUpdate(t *testing.T) {
	parser := NewParser()
	stmt, err := parser.Parse("UPDATE users SET age = 31 WHERE id = 1")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if stmt.Type != Update {
		t.Errorf("Expected Update, got %v", stmt.Type)
	}

	if stmt.Table != "users" {
		t.Errorf("Expected table name 'users', got '%s'", stmt.Table)
	}

	if len(stmt.Updates) != 1 {
		t.Errorf("Expected 1 update, got %d", len(stmt.Updates))
	}

	if len(stmt.Conditions) != 1 {
		t.Errorf("Expected 1 condition, got %d", len(stmt.Conditions))
	}
}

func TestParseDelete(t *testing.T) {
	parser := NewParser()
	stmt, err := parser.Parse("DELETE FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if stmt.Type != Delete {
		t.Errorf("Expected Delete, got %v", stmt.Type)
	}

	if stmt.Table != "users" {
		t.Errorf("Expected table name 'users', got '%s'", stmt.Table)
	}

	if len(stmt.Conditions) != 1 {
		t.Errorf("Expected 1 condition, got %d", len(stmt.Conditions))
	}
}
