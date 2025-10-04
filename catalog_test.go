package main

import (
	"os"
	"testing"
)

func TestCatalogBasics(t *testing.T) {
	tmpDir := t.TempDir()
	catalog := NewSystemCatalog(tmpDir)

	// Create database
	if err := catalog.CreateDatabase("testdb"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Verify database exists
	db, err := catalog.GetDatabase("testdb")
	if err != nil {
		t.Fatalf("Failed to get database: %v", err)
	}
	if db.Name != "testdb" {
		t.Errorf("Expected database name 'testdb', got '%s'", db.Name)
	}

	// List databases
	dbNames := catalog.ListDatabases()
	if len(dbNames) != 1 || dbNames[0] != "testdb" {
		t.Errorf("Expected 1 database named 'testdb', got %v", dbNames)
	}
}

func TestCatalogTables(t *testing.T) {
	tmpDir := t.TempDir()
	catalog := NewSystemCatalog(tmpDir)

	// Create database
	catalog.CreateDatabase("testdb")

	// Create table
	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "name", DataType: "VARCHAR"},
	}
	heapPath := tmpDir + "/testdb/users.heap"

	if err := catalog.CreateTable("testdb", "users", columns, heapPath); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Get table
	table, err := catalog.GetTable("testdb", "users")
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}
	if table.Name != "users" {
		t.Errorf("Expected table name 'users', got '%s'", table.Name)
	}
	if len(table.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(table.Columns))
	}
	if table.HeapFile != heapPath {
		t.Errorf("Expected heap file '%s', got '%s'", heapPath, table.HeapFile)
	}

	// List tables
	tableNames, err := catalog.ListTables("testdb")
	if err != nil {
		t.Fatalf("Failed to list tables: %v", err)
	}
	if len(tableNames) != 1 || tableNames[0] != "users" {
		t.Errorf("Expected 1 table named 'users', got %v", tableNames)
	}
}

func TestCatalogPersistence(t *testing.T) {
	tmpDir := t.TempDir()

	// Create catalog and add data
	catalog1 := NewSystemCatalog(tmpDir)
	catalog1.CreateDatabase("db1")
	catalog1.CreateDatabase("db2")

	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "value", DataType: "VARCHAR"},
	}
	catalog1.CreateTable("db1", "table1", columns, tmpDir+"/db1/table1.heap")
	catalog1.CreateTable("db1", "table2", columns, tmpDir+"/db1/table2.heap")
	catalog1.CreateTable("db2", "table3", columns, tmpDir+"/db2/table3.heap")

	// Save catalog
	if err := catalog1.SaveCatalog(); err != nil {
		t.Fatalf("Failed to save catalog: %v", err)
	}

	// Create new catalog and load
	catalog2 := NewSystemCatalog(tmpDir)
	if err := catalog2.LoadCatalog(); err != nil {
		t.Fatalf("Failed to load catalog: %v", err)
	}

	// Verify databases
	dbNames := catalog2.ListDatabases()
	if len(dbNames) != 2 {
		t.Errorf("Expected 2 databases, got %d", len(dbNames))
	}

	// Verify tables in db1
	tables1, err := catalog2.ListTables("db1")
	if err != nil {
		t.Fatalf("Failed to list tables in db1: %v", err)
	}
	if len(tables1) != 2 {
		t.Errorf("Expected 2 tables in db1, got %d", len(tables1))
	}

	// Verify tables in db2
	tables2, err := catalog2.ListTables("db2")
	if err != nil {
		t.Fatalf("Failed to list tables in db2: %v", err)
	}
	if len(tables2) != 1 {
		t.Errorf("Expected 1 table in db2, got %d", len(tables2))
	}

	// Verify table metadata
	table, err := catalog2.GetTable("db1", "table1")
	if err != nil {
		t.Fatalf("Failed to get table1: %v", err)
	}
	if len(table.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(table.Columns))
	}
}

func TestCatalogDropOperations(t *testing.T) {
	tmpDir := t.TempDir()
	catalog := NewSystemCatalog(tmpDir)

	// Create database and table
	catalog.CreateDatabase("testdb")
	columns := []Column{{Name: "id", DataType: "INT"}}
	catalog.CreateTable("testdb", "users", columns, tmpDir+"/testdb/users.heap")

	// Drop table
	if err := catalog.DropTable("testdb", "users"); err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}

	// Verify table is gone
	_, err := catalog.GetTable("testdb", "users")
	if err == nil {
		t.Error("Expected error when getting dropped table")
	}

	// Drop database
	if err := catalog.DropDatabase("testdb"); err != nil {
		t.Fatalf("Failed to drop database: %v", err)
	}

	// Verify database is gone
	_, err = catalog.GetDatabase("testdb")
	if err == nil {
		t.Error("Expected error when getting dropped database")
	}
}

func TestCatalogAlterTable(t *testing.T) {
	tmpDir := t.TempDir()
	catalog := NewSystemCatalog(tmpDir)

	// Create database and table
	catalog.CreateDatabase("testdb")
	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "name", DataType: "VARCHAR"},
	}
	catalog.CreateTable("testdb", "users", columns, tmpDir+"/testdb/users.heap")

	// Alter table - add column
	newColumns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "name", DataType: "VARCHAR"},
		{Name: "email", DataType: "VARCHAR"},
	}
	if err := catalog.AlterTable("testdb", "users", newColumns); err != nil {
		t.Fatalf("Failed to alter table: %v", err)
	}

	// Verify new columns
	table, err := catalog.GetTable("testdb", "users")
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}
	if len(table.Columns) != 3 {
		t.Errorf("Expected 3 columns after alter, got %d", len(table.Columns))
	}
	if table.Columns[2].Name != "email" {
		t.Errorf("Expected new column 'email', got '%s'", table.Columns[2].Name)
	}
}

func TestCatalogConcurrency(t *testing.T) {
	tmpDir := t.TempDir()
	catalog := NewSystemCatalog(tmpDir)

	// Create database
	catalog.CreateDatabase("testdb")

	// Concurrent table creation
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(n int) {
			columns := []Column{{Name: "id", DataType: "INT"}}
			tableName := "table" + string(rune('0'+n))
			heapPath := tmpDir + "/testdb/" + tableName + ".heap"
			catalog.CreateTable("testdb", tableName, columns, heapPath)
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify all tables were created
	tables, err := catalog.ListTables("testdb")
	if err != nil {
		t.Fatalf("Failed to list tables: %v", err)
	}
	if len(tables) != 10 {
		t.Errorf("Expected 10 tables, got %d", len(tables))
	}
}

func TestCatalogEmptyLoad(t *testing.T) {
	tmpDir := t.TempDir()
	catalog := NewSystemCatalog(tmpDir)

	// Load from empty directory (no catalog file)
	if err := catalog.LoadCatalog(); err != nil {
		t.Fatalf("Failed to load empty catalog: %v", err)
	}

	// Should have no databases
	dbNames := catalog.ListDatabases()
	if len(dbNames) != 0 {
		t.Errorf("Expected 0 databases in empty catalog, got %d", len(dbNames))
	}
}

func TestCatalogCorruptedFile(t *testing.T) {
	tmpDir := t.TempDir()

	// Write corrupted catalog file
	catalogPath := tmpDir + "/catalog.json"
	if err := os.WriteFile(catalogPath, []byte("corrupted json{{{"), 0644); err != nil {
		t.Fatalf("Failed to write corrupted file: %v", err)
	}

	catalog := NewSystemCatalog(tmpDir)
	err := catalog.LoadCatalog()
	if err == nil {
		t.Error("Expected error when loading corrupted catalog")
	}
}

func TestPagedEngineWithCatalog(t *testing.T) {
	tmpDir := t.TempDir()

	// Create engine and add data
	engine1, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	engine1.CreateDatabase("testdb")
	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "name", DataType: "VARCHAR"},
	}
	engine1.CreateTable("users", columns)
	engine1.InsertRow("users", Row{"id": 1, "name": "Alice"})
	engine1.InsertRow("users", Row{"id": 2, "name": "Bob"})

	// Create another table
	columns2 := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "product", DataType: "VARCHAR"},
	}
	engine1.CreateTable("products", columns2)
	engine1.InsertRow("products", Row{"id": 1, "product": "Widget"})

	engine1.Close()

	// Reopen engine - should auto-load from catalog
	engine2, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to reopen engine: %v", err)
	}
	defer engine2.Close()

	// Verify database exists
	if err := engine2.UseDatabase("testdb"); err != nil {
		t.Fatalf("Failed to use database: %v", err)
	}

	// Verify users table
	rows, err := engine2.SelectRows("users", nil)
	if err != nil {
		t.Fatalf("Failed to select from users: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("Expected 2 rows in users, got %d", len(rows))
	}

	// Verify products table
	rows, err = engine2.SelectRows("products", nil)
	if err != nil {
		t.Fatalf("Failed to select from products: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("Expected 1 row in products, got %d", len(rows))
	}
}

func TestMultipleDatabasesPersistence(t *testing.T) {
	tmpDir := t.TempDir()

	// Create engine with multiple databases
	engine1, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	// Create first database
	engine1.CreateDatabase("db1")
	columns := []Column{{Name: "id", DataType: "INT"}}
	engine1.CreateTable("table1", columns)
	engine1.InsertRow("table1", Row{"id": 1})

	// Create second database
	engine1.CreateDatabase("db2")
	engine1.CreateTable("table2", columns)
	engine1.InsertRow("table2", Row{"id": 2})

	engine1.Close()

	// Reopen and verify both databases
	engine2, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to reopen engine: %v", err)
	}
	defer engine2.Close()

	// Check db1
	if err := engine2.UseDatabase("db1"); err != nil {
		t.Fatalf("Failed to use db1: %v", err)
	}
	rows, err := engine2.SelectRows("table1", nil)
	if err != nil {
		t.Fatalf("Failed to select from table1: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("Expected 1 row in table1, got %d", len(rows))
	}

	// Check db2
	if err := engine2.UseDatabase("db2"); err != nil {
		t.Fatalf("Failed to use db2: %v", err)
	}
	rows, err = engine2.SelectRows("table2", nil)
	if err != nil {
		t.Fatalf("Failed to select from table2: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("Expected 1 row in table2, got %d", len(rows))
	}
}
