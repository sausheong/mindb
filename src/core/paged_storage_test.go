package mindb

import (
	"fmt"
	"testing"
)

func TestPagedEngineBasics(t *testing.T) {
	tmpDir := t.TempDir()
	
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create paged engine: %v", err)
	}
	defer engine.Close()
	
	// Create database
	if err := engine.CreateDatabase("testdb"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	
	// Create table
	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "name", DataType: "VARCHAR"},
		{Name: "age", DataType: "INT"},
	}
	
	if err := engine.CreateTable("users", columns); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	
	// Insert row
	row := Row{
		"id":   1,
		"name": "Alice",
		"age":  30,
	}
	
	if err := engine.InsertRow("users", row); err != nil {
		t.Fatalf("Failed to insert row: %v", err)
	}
	
	// Select rows
	rows, err := engine.SelectRows("users", nil)
	if err != nil {
		t.Fatalf("Failed to select rows: %v", err)
	}
	
	if len(rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(rows))
	}
	
	if rows[0]["name"] != "Alice" {
		t.Errorf("Expected name 'Alice', got %v", rows[0]["name"])
	}
}

func TestPagedEngineMultipleRows(t *testing.T) {
	tmpDir := t.TempDir()
	
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create paged engine: %v", err)
	}
	defer engine.Close()
	
	engine.CreateDatabase("testdb")
	
	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "name", DataType: "VARCHAR"},
	}
	
	engine.CreateTable("users", columns)
	
	// Insert multiple rows
	users := []Row{
		{"id": 1, "name": "Alice"},
		{"id": 2, "name": "Bob"},
		{"id": 3, "name": "Charlie"},
	}
	
	for _, user := range users {
		if err := engine.InsertRow("users", user); err != nil {
			t.Fatalf("Failed to insert row: %v", err)
		}
	}
	
	// Select all
	rows, err := engine.SelectRows("users", nil)
	if err != nil {
		t.Fatalf("Failed to select rows: %v", err)
	}
	
	if len(rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(rows))
	}
}

func TestPagedEngineWithConditions(t *testing.T) {
	tmpDir := t.TempDir()
	
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create paged engine: %v", err)
	}
	defer engine.Close()
	
	engine.CreateDatabase("testdb")
	
	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "age", DataType: "INT"},
	}
	
	engine.CreateTable("users", columns)
	
	// Insert rows
	engine.InsertRow("users", Row{"id": 1, "age": 25})
	engine.InsertRow("users", Row{"id": 2, "age": 30})
	engine.InsertRow("users", Row{"id": 3, "age": 35})
	
	// Select with condition
	conditions := []Condition{
		{Column: "age", Operator: ">", Value: 28},
	}
	
	rows, err := engine.SelectRows("users", conditions)
	if err != nil {
		t.Fatalf("Failed to select rows: %v", err)
	}
	
	if len(rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(rows))
	}
}

func TestPagedEngineUpdate(t *testing.T) {
	tmpDir := t.TempDir()
	
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create paged engine: %v", err)
	}
	defer engine.Close()
	
	engine.CreateDatabase("testdb")
	
	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "name", DataType: "VARCHAR"},
	}
	
	engine.CreateTable("users", columns)
	
	// Insert
	engine.InsertRow("users", Row{"id": 1, "name": "Alice"})
	
	// Update
	updates := map[string]interface{}{
		"name": "Alicia",
	}
	conditions := []Condition{
		{Column: "id", Operator: "=", Value: 1},
	}
	
	count, err := engine.UpdateRows("users", updates, conditions)
	if err != nil {
		t.Fatalf("Failed to update rows: %v", err)
	}
	
	if count != 1 {
		t.Errorf("Expected 1 row updated, got %d", count)
	}
	
	// Verify update
	rows, _ := engine.SelectRows("users", nil)
	if rows[0]["name"] != "Alicia" {
		t.Errorf("Expected name 'Alicia', got %v", rows[0]["name"])
	}
}

func TestPagedEngineDelete(t *testing.T) {
	tmpDir := t.TempDir()
	
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create paged engine: %v", err)
	}
	defer engine.Close()
	
	engine.CreateDatabase("testdb")
	
	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "name", DataType: "VARCHAR"},
	}
	
	engine.CreateTable("users", columns)
	
	// Insert rows
	engine.InsertRow("users", Row{"id": 1, "name": "Alice"})
	engine.InsertRow("users", Row{"id": 2, "name": "Bob"})
	
	// Delete
	conditions := []Condition{
		{Column: "id", Operator: "=", Value: 1},
	}
	
	count, err := engine.DeleteRows("users", conditions)
	if err != nil {
		t.Fatalf("Failed to delete rows: %v", err)
	}
	
	if count != 1 {
		t.Errorf("Expected 1 row deleted, got %d", count)
	}
	
	// Verify deletion
	rows, _ := engine.SelectRows("users", nil)
	if len(rows) != 1 {
		t.Errorf("Expected 1 remaining row, got %d", len(rows))
	}
	
	if rows[0]["name"] != "Bob" {
		t.Errorf("Expected remaining row to be Bob")
	}
}

func TestPagedEngineDropTable(t *testing.T) {
	tmpDir := t.TempDir()
	
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create paged engine: %v", err)
	}
	defer engine.Close()
	
	engine.CreateDatabase("testdb")
	
	columns := []Column{
		{Name: "id", DataType: "INT"},
	}
	
	engine.CreateTable("users", columns)
	engine.InsertRow("users", Row{"id": 1})
	
	// Drop table
	if err := engine.DropTable("users"); err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}
	
	// Verify table is gone
	_, err = engine.SelectRows("users", nil)
	if err == nil {
		t.Error("Expected error when selecting from dropped table")
	}
}

func TestPagedEnginePersistence(t *testing.T) {
	tmpDir := t.TempDir()
	
	// Create engine and insert data
	engine1, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create paged engine: %v", err)
	}
	
	engine1.CreateDatabase("testdb")
	
	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "name", DataType: "VARCHAR"},
	}
	
	engine1.CreateTable("users", columns)
	engine1.InsertRow("users", Row{"id": 1, "name": "Alice"})
	engine1.InsertRow("users", Row{"id": 2, "name": "Bob"})
	
	engine1.Close()
	
	// Reopen and verify data persisted
	engine2, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to reopen paged engine: %v", err)
	}
	defer engine2.Close()
	
	// Database and table should be auto-loaded from catalog
	// Just need to select the database
	if err := engine2.UseDatabase("testdb"); err != nil {
		t.Fatalf("Failed to use database: %v", err)
	}
	
	// The heap file should still contain the data
	rows, err := engine2.SelectRows("users", nil)
	if err != nil {
		t.Fatalf("Failed to select rows after reopen: %v", err)
	}
	
	if len(rows) != 2 {
		t.Errorf("Expected 2 rows after reopen, got %d", len(rows))
	}
}

func TestPagedEngineLargeDataset(t *testing.T) {
	tmpDir := t.TempDir()
	
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create paged engine: %v", err)
	}
	defer engine.Close()
	
	engine.CreateDatabase("testdb")
	
	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "value", DataType: "VARCHAR"},
	}
	
	engine.CreateTable("data", columns)
	
	// Insert many rows to test multiple pages
	rowCount := 1000
	for i := 0; i < rowCount; i++ {
		row := Row{
			"id":    i,
			"value": fmt.Sprintf("value_%d", i),
		}
		if err := engine.InsertRow("data", row); err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}
	
	// Verify all rows
	rows, err := engine.SelectRows("data", nil)
	if err != nil {
		t.Fatalf("Failed to select rows: %v", err)
	}
	
	if len(rows) != rowCount {
		t.Errorf("Expected %d rows, got %d", rowCount, len(rows))
	}
	
	// Test selective query
	conditions := []Condition{
		{Column: "id", Operator: "<", Value: 10},
	}
	
	rows, err = engine.SelectRows("data", conditions)
	if err != nil {
		t.Fatalf("Failed to select with conditions: %v", err)
	}
	
	if len(rows) != 10 {
		t.Errorf("Expected 10 rows with id < 10, got %d", len(rows))
	}
}
