package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

/*
Package: mindb
Component: Persistence Manager
Layer: Storage Layer (Layer 4)

Test Coverage:
- Database save/load operations
- File management
- Serialization/deserialization
- Error handling
- Concurrent access

Priority: HIGH (0% coverage → target 80%+)
Impact: +1% overall coverage

Run: go test -v -run TestPersistence
*/

// ============================================================================
// PERSISTENCE MANAGER TESTS
// ============================================================================

func TestPersistenceManager_NewPersistenceManager(t *testing.T) {
	tmpDir := t.TempDir()
	
	pm, err := NewPersistenceManager(tmpDir)
	if err != nil {
		t.Fatalf("NewPersistenceManager failed: %v", err)
	}
	
	if pm == nil {
		t.Fatal("Expected non-nil PersistenceManager")
	}
	
	// Verify directory was created
	if _, err := os.Stat(tmpDir); os.IsNotExist(err) {
		t.Error("Data directory was not created")
	}
}

func TestPersistenceManager_NewPersistenceManager_CreatesDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	subDir := filepath.Join(tmpDir, "nested", "data")
	
	pm, err := NewPersistenceManager(subDir)
	if err != nil {
		t.Fatalf("NewPersistenceManager failed: %v", err)
	}
	
	if pm == nil {
		t.Fatal("Expected non-nil PersistenceManager")
	}
	
	// Verify nested directory was created
	if _, err := os.Stat(subDir); os.IsNotExist(err) {
		t.Error("Nested data directory was not created")
	}
}

func TestPersistenceManager_SaveAndLoadDatabase(t *testing.T) {
	tmpDir := t.TempDir()
	pm, err := NewPersistenceManager(tmpDir)
	if err != nil {
		t.Fatalf("NewPersistenceManager failed: %v", err)
	}
	
	// Create a test database
	db := &Database{
		Name:   "testdb",
		Tables: make(map[string]*Table),
	}
	
	// Add a table with data
	db.Tables["users"] = &Table{
		Name: "users",
		Columns: []Column{
			{Name: "id", DataType: "INTEGER"},
			{Name: "name", DataType: "TEXT"},
		},
		Rows: []Row{
			{"id": 1, "name": "Alice"},
			{"id": 2, "name": "Bob"},
		},
	}
	
	// Save database
	err = pm.SaveDatabase(db)
	if err != nil {
		t.Fatalf("SaveDatabase failed: %v", err)
	}
	
	// Verify file was created
	filename := filepath.Join(tmpDir, "testdb.json")
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		t.Error("Database file was not created")
	}
	
	// Load database
	loadedDB, err := pm.LoadDatabase("testdb")
	if err != nil {
		t.Fatalf("LoadDatabase failed: %v", err)
	}
	
	// Verify loaded data
	if loadedDB.Name != "testdb" {
		t.Errorf("Expected database name 'testdb', got '%s'", loadedDB.Name)
	}
	
	if len(loadedDB.Tables) != 1 {
		t.Errorf("Expected 1 table, got %d", len(loadedDB.Tables))
	}
	
	usersTable, ok := loadedDB.Tables["users"]
	if !ok {
		t.Fatal("Expected 'users' table not found")
	}
	
	if len(usersTable.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(usersTable.Rows))
	}
	
	if len(usersTable.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(usersTable.Columns))
	}
}

func TestPersistenceManager_SaveDatabase_EmptyDatabase(t *testing.T) {
	tmpDir := t.TempDir()
	pm, err := NewPersistenceManager(tmpDir)
	if err != nil {
		t.Fatalf("NewPersistenceManager failed: %v", err)
	}
	
	// Create empty database
	db := &Database{
		Name:   "emptydb",
		Tables: make(map[string]*Table),
	}
	
	// Save database
	err = pm.SaveDatabase(db)
	if err != nil {
		t.Fatalf("SaveDatabase failed: %v", err)
	}
	
	// Load database
	loadedDB, err := pm.LoadDatabase("emptydb")
	if err != nil {
		t.Fatalf("LoadDatabase failed: %v", err)
	}
	
	if len(loadedDB.Tables) != 0 {
		t.Errorf("Expected 0 tables, got %d", len(loadedDB.Tables))
	}
}

func TestPersistenceManager_SaveDatabase_MultipleTables(t *testing.T) {
	tmpDir := t.TempDir()
	pm, err := NewPersistenceManager(tmpDir)
	if err != nil {
		t.Fatalf("NewPersistenceManager failed: %v", err)
	}
	
	// Create database with multiple tables
	db := &Database{
		Name:   "multidb",
		Tables: make(map[string]*Table),
	}
	
	db.Tables["users"] = &Table{
		Name: "users",
		Columns: []Column{
			{Name: "id", DataType: "INTEGER"},
			{Name: "name", DataType: "TEXT"},
		},
		Rows: []Row{
			{"id": 1, "name": "Alice"},
		},
	}
	
	db.Tables["products"] = &Table{
		Name: "products",
		Columns: []Column{
			{Name: "id", DataType: "INTEGER"},
			{Name: "title", DataType: "TEXT"},
			{Name: "price", DataType: "REAL"},
		},
		Rows: []Row{
			{"id": 1, "title": "Widget", "price": 9.99},
			{"id": 2, "title": "Gadget", "price": 19.99},
		},
	}
	
	// Save database
	err = pm.SaveDatabase(db)
	if err != nil {
		t.Fatalf("SaveDatabase failed: %v", err)
	}
	
	// Load database
	loadedDB, err := pm.LoadDatabase("multidb")
	if err != nil {
		t.Fatalf("LoadDatabase failed: %v", err)
	}
	
	if len(loadedDB.Tables) != 2 {
		t.Errorf("Expected 2 tables, got %d", len(loadedDB.Tables))
	}
	
	if _, ok := loadedDB.Tables["users"]; !ok {
		t.Error("Expected 'users' table not found")
	}
	
	if _, ok := loadedDB.Tables["products"]; !ok {
		t.Error("Expected 'products' table not found")
	}
}

func TestPersistenceManager_LoadDatabase_NotFound(t *testing.T) {
	tmpDir := t.TempDir()
	pm, err := NewPersistenceManager(tmpDir)
	if err != nil {
		t.Fatalf("NewPersistenceManager failed: %v", err)
	}
	
	// Try to load non-existent database
	_, err = pm.LoadDatabase("nonexistent")
	if err == nil {
		t.Error("Expected error when loading non-existent database")
	}
}

func TestPersistenceManager_LoadDatabase_CorruptedFile(t *testing.T) {
	tmpDir := t.TempDir()
	pm, err := NewPersistenceManager(tmpDir)
	if err != nil {
		t.Fatalf("NewPersistenceManager failed: %v", err)
	}
	
	// Create corrupted file
	filename := filepath.Join(tmpDir, "corrupted.json")
	err = os.WriteFile(filename, []byte("invalid json {{{"), 0644)
	if err != nil {
		t.Fatalf("Failed to create corrupted file: %v", err)
	}
	
	// Try to load corrupted database
	_, err = pm.LoadDatabase("corrupted")
	if err == nil {
		t.Error("Expected error when loading corrupted database")
	}
}

func TestPersistenceManager_ListDatabases(t *testing.T) {
	tmpDir := t.TempDir()
	pm, err := NewPersistenceManager(tmpDir)
	if err != nil {
		t.Fatalf("NewPersistenceManager failed: %v", err)
	}
	
	// Initially empty
	databases, err := pm.ListDatabases()
	if err != nil {
		t.Fatalf("ListDatabases failed: %v", err)
	}
	
	if len(databases) != 0 {
		t.Errorf("Expected 0 databases, got %d", len(databases))
	}
	
	// Save some databases
	for _, name := range []string{"db1", "db2", "db3"} {
		db := &Database{
			Name:   name,
			Tables: make(map[string]*Table),
		}
		err = pm.SaveDatabase(db)
		if err != nil {
			t.Fatalf("SaveDatabase failed: %v", err)
		}
	}
	
	// List databases
	databases, err = pm.ListDatabases()
	if err != nil {
		t.Fatalf("ListDatabases failed: %v", err)
	}
	
	if len(databases) != 3 {
		t.Errorf("Expected 3 databases, got %d", len(databases))
	}
	
	// Verify names
	expectedNames := map[string]bool{"db1": true, "db2": true, "db3": true}
	for _, name := range databases {
		if !expectedNames[name] {
			t.Errorf("Unexpected database name: %s", name)
		}
	}
}

func TestPersistenceManager_ListDatabases_IgnoresNonJSON(t *testing.T) {
	tmpDir := t.TempDir()
	pm, err := NewPersistenceManager(tmpDir)
	if err != nil {
		t.Fatalf("NewPersistenceManager failed: %v", err)
	}
	
	// Create JSON database
	db := &Database{
		Name:   "realdb",
		Tables: make(map[string]*Table),
	}
	err = pm.SaveDatabase(db)
	if err != nil {
		t.Fatalf("SaveDatabase failed: %v", err)
	}
	
	// Create non-JSON files
	os.WriteFile(filepath.Join(tmpDir, "readme.txt"), []byte("test"), 0644)
	os.WriteFile(filepath.Join(tmpDir, "data.xml"), []byte("<data/>"), 0644)
	
	// List databases
	databases, err := pm.ListDatabases()
	if err != nil {
		t.Fatalf("ListDatabases failed: %v", err)
	}
	
	if len(databases) != 1 {
		t.Errorf("Expected 1 database, got %d", len(databases))
	}
	
	if databases[0] != "realdb" {
		t.Errorf("Expected 'realdb', got '%s'", databases[0])
	}
}

func TestPersistenceManager_DeleteDatabase(t *testing.T) {
	tmpDir := t.TempDir()
	pm, err := NewPersistenceManager(tmpDir)
	if err != nil {
		t.Fatalf("NewPersistenceManager failed: %v", err)
	}
	
	// Save database
	db := &Database{
		Name:   "todelete",
		Tables: make(map[string]*Table),
	}
	err = pm.SaveDatabase(db)
	if err != nil {
		t.Fatalf("SaveDatabase failed: %v", err)
	}
	
	// Verify it exists
	if !pm.DatabaseExists("todelete") {
		t.Error("Database should exist before deletion")
	}
	
	// Delete database
	err = pm.DeleteDatabase("todelete")
	if err != nil {
		t.Fatalf("DeleteDatabase failed: %v", err)
	}
	
	// Verify it's gone
	if pm.DatabaseExists("todelete") {
		t.Error("Database should not exist after deletion")
	}
}

func TestPersistenceManager_DeleteDatabase_NotFound(t *testing.T) {
	tmpDir := t.TempDir()
	pm, err := NewPersistenceManager(tmpDir)
	if err != nil {
		t.Fatalf("NewPersistenceManager failed: %v", err)
	}
	
	// Delete non-existent database (should not error)
	err = pm.DeleteDatabase("nonexistent")
	if err != nil {
		t.Errorf("DeleteDatabase should not error on non-existent database: %v", err)
	}
}

func TestPersistenceManager_DatabaseExists(t *testing.T) {
	tmpDir := t.TempDir()
	pm, err := NewPersistenceManager(tmpDir)
	if err != nil {
		t.Fatalf("NewPersistenceManager failed: %v", err)
	}
	
	// Check non-existent database
	if pm.DatabaseExists("nonexistent") {
		t.Error("DatabaseExists should return false for non-existent database")
	}
	
	// Save database
	db := &Database{
		Name:   "existing",
		Tables: make(map[string]*Table),
	}
	err = pm.SaveDatabase(db)
	if err != nil {
		t.Fatalf("SaveDatabase failed: %v", err)
	}
	
	// Check existing database
	if !pm.DatabaseExists("existing") {
		t.Error("DatabaseExists should return true for existing database")
	}
}

func TestPersistenceManager_SaveDatabase_Overwrite(t *testing.T) {
	tmpDir := t.TempDir()
	pm, err := NewPersistenceManager(tmpDir)
	if err != nil {
		t.Fatalf("NewPersistenceManager failed: %v", err)
	}
	
	// Save initial database
	db := &Database{
		Name:   "overwrite",
		Tables: make(map[string]*Table),
	}
	db.Tables["users"] = &Table{
		Name: "users",
		Columns: []Column{
			{Name: "id", DataType: "INTEGER"},
		},
		Rows: []Row{
			{"id": 1},
		},
	}
	
	err = pm.SaveDatabase(db)
	if err != nil {
		t.Fatalf("SaveDatabase failed: %v", err)
	}
	
	// Overwrite with new data
	db.Tables["users"].Rows = append(db.Tables["users"].Rows, Row{"id": 2})
	
	err = pm.SaveDatabase(db)
	if err != nil {
		t.Fatalf("SaveDatabase (overwrite) failed: %v", err)
	}
	
	// Load and verify
	loadedDB, err := pm.LoadDatabase("overwrite")
	if err != nil {
		t.Fatalf("LoadDatabase failed: %v", err)
	}
	
	if len(loadedDB.Tables["users"].Rows) != 2 {
		t.Errorf("Expected 2 rows after overwrite, got %d", len(loadedDB.Tables["users"].Rows))
	}
}

func TestPersistenceManager_ConcurrentAccess(t *testing.T) {
	tmpDir := t.TempDir()
	pm, err := NewPersistenceManager(tmpDir)
	if err != nil {
		t.Fatalf("NewPersistenceManager failed: %v", err)
	}
	
	// Concurrent saves
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			db := &Database{
				Name:   fmt.Sprintf("db%d", id),
				Tables: make(map[string]*Table),
			}
			err := pm.SaveDatabase(db)
			if err != nil {
				t.Errorf("Concurrent SaveDatabase failed: %v", err)
			}
			done <- true
		}(i)
	}
	
	// Wait for all saves
	for i := 0; i < 10; i++ {
		<-done
	}
	
	// Verify all databases were saved
	databases, err := pm.ListDatabases()
	if err != nil {
		t.Fatalf("ListDatabases failed: %v", err)
	}
	
	if len(databases) != 10 {
		t.Errorf("Expected 10 databases, got %d", len(databases))
	}
}
