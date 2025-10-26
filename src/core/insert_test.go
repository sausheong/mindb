package mindb

import (
	"strings"
	"testing"
)

func TestInsertWithSpacesInValues(t *testing.T) {
	parser := NewParser()
	
	// Test the exact statement from sample.sql
	sql := "INSERT INTO users (id, name, email, created_at) VALUES (1, 'Alice Johnson', 'alice@example.com', '2024-01-15 10:30:00')"
	stmt, err := parser.Parse(sql)
	
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	
	if stmt.Type != Insert {
		t.Errorf("Expected Insert type, got %v", stmt.Type)
	}
	
	if stmt.Table != "users" {
		t.Errorf("Expected table 'users', got '%s'", stmt.Table)
	}
	
	if len(stmt.Columns) != 4 {
		t.Errorf("Expected 4 columns, got %d", len(stmt.Columns))
	}
	
	if len(stmt.Values) != 1 || len(stmt.Values[0]) != 4 {
		t.Errorf("Expected 1 row with 4 values, got %d rows", len(stmt.Values))
	}
	
	// Check the parsed values
	if stmt.Values[0][0] != 1 {
		t.Errorf("Expected id=1, got %v", stmt.Values[0][0])
	}
	
	if stmt.Values[0][1] != "Alice Johnson" {
		t.Errorf("Expected name='Alice Johnson', got '%v'", stmt.Values[0][1])
	}
	
	if stmt.Values[0][2] != "alice@example.com" {
		t.Errorf("Expected email='alice@example.com', got '%v'", stmt.Values[0][2])
	}
	
	if stmt.Values[0][3] != "2024-01-15 10:30:00" {
		t.Errorf("Expected created_at='2024-01-15 10:30:00', got '%v'", stmt.Values[0][3])
	}
}

func TestInsertWithSpecialCharacters(t *testing.T) {
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
			{Name: "email", DataType: "VARCHAR"},
		},
	})
	
	// Test INSERT with special characters
	testCases := []struct {
		sql         string
		expectedName string
	}{
		{
			"INSERT INTO users (id, name, email) VALUES (1, 'John Doe', 'john@example.com')",
			"John Doe",
		},
		{
			"INSERT INTO users (id, name, email) VALUES (2, 'Jane O''Brien', 'jane@example.com')",
			"Jane O'Brien",
		},
		{
			"INSERT INTO users (id, name, email) VALUES (3, 'Bob Smith Jr.', 'bob@example.com')",
			"Bob Smith Jr.",
		},
	}
	
	for i, tc := range testCases {
		stmt, err := parser.Parse(tc.sql)
		if err != nil {
			t.Errorf("Test case %d: Parse error: %v", i, err)
			continue
		}
		
		result, err := engine.Execute(stmt)
		if err != nil {
			t.Errorf("Test case %d: Execute error: %v", i, err)
			continue
		}
		
		if !strings.Contains(result, "inserted") {
			t.Errorf("Test case %d: Expected success message, got: %s", i, result)
		}
	}
	
	// Verify all inserts
	selectStmt := &Statement{
		Type:  Select,
		Table: "users",
	}
	
	result, err := engine.Execute(selectStmt)
	if err != nil {
		t.Fatalf("Select error: %v", err)
	}
	
	if !strings.Contains(result, "John Doe") {
		t.Error("Expected to find 'John Doe' in results")
	}
}

func TestInsertWithTimestamps(t *testing.T) {
	parser := NewParser()
	engine := NewEngine()
	
	// Setup
	engine.Execute(&Statement{Type: CreateDatabase, Database: "testdb"})
	engine.Execute(&Statement{
		Type:  CreateTable,
		Table: "events",
		Columns: []Column{
			{Name: "id", DataType: "INT"},
			{Name: "name", DataType: "VARCHAR"},
			{Name: "timestamp", DataType: "TIMESTAMP"},
		},
	})
	
	// Test INSERT with timestamp
	sql := "INSERT INTO events (id, name, timestamp) VALUES (1, 'Login Event', '2024-01-15 10:30:45')"
	stmt, err := parser.Parse(sql)
	
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	
	result, err := engine.Execute(stmt)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	
	if !strings.Contains(result, "inserted") {
		t.Errorf("Expected success message, got: %s", result)
	}
	
	// Verify
	selectStmt := &Statement{
		Type:  Select,
		Table: "events",
	}
	
	result, err = engine.Execute(selectStmt)
	if err != nil {
		t.Fatalf("Select error: %v", err)
	}
	
	if !strings.Contains(result, "2024-01-15 10:30:45") {
		t.Error("Expected to find timestamp in results")
	}
}

func TestInsertMultipleWithComplexValues(t *testing.T) {
	parser := NewParser()
	engine := NewEngine()
	
	// Setup
	engine.Execute(&Statement{Type: CreateDatabase, Database: "testdb"})
	engine.Execute(&Statement{
		Type:  CreateTable,
		Table: "products",
		Columns: []Column{
			{Name: "id", DataType: "INT"},
			{Name: "name", DataType: "VARCHAR"},
			{Name: "description", DataType: "VARCHAR"},
			{Name: "price", DataType: "FLOAT"},
		},
	})
	
	// Test multiple inserts with complex values
	testSQLs := []string{
		"INSERT INTO products (id, name, description, price) VALUES (1, 'Laptop Pro 15', 'High-performance laptop with 16GB RAM', 1299.99)",
		"INSERT INTO products (id, name, description, price) VALUES (2, 'USB-C Cable (2m)', 'Durable charging cable', 19.99)",
		"INSERT INTO products (id, name, description, price) VALUES (3, 'Wireless Mouse', 'Ergonomic design, 2.4GHz', 29.50)",
	}
	
	for i, sql := range testSQLs {
		stmt, err := parser.Parse(sql)
		if err != nil {
			t.Errorf("Test %d: Parse error: %v", i, err)
			continue
		}
		
		result, err := engine.Execute(stmt)
		if err != nil {
			t.Errorf("Test %d: Execute error: %v", i, err)
			continue
		}
		
		if !strings.Contains(result, "inserted") {
			t.Errorf("Test %d: Expected success message, got: %s", i, result)
		}
	}
	
	// Verify all products
	selectStmt := &Statement{
		Type:  Select,
		Table: "products",
	}
	
	result, err := engine.Execute(selectStmt)
	if err != nil {
		t.Fatalf("Select error: %v", err)
	}
	
	if !strings.Contains(result, "3 row(s) in set") {
		t.Errorf("Expected 3 rows, got: %s", result)
	}
}
