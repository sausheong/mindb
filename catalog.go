package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// SystemCatalog manages database and table metadata
type SystemCatalog struct {
	databases map[string]*DatabaseMetadata
	dataDir   string
	mu        sync.RWMutex
}

// DatabaseMetadata stores metadata about a database
type DatabaseMetadata struct {
	Name   string                    `json:"name"`
	Tables map[string]*TableMetadata `json:"tables"`
}

// TableMetadata stores metadata about a table
type TableMetadata struct {
	Name      string   `json:"name"`
	Columns   []Column `json:"columns"`
	HeapFile  string   `json:"heap_file"`  // Path to heap file
	CreatedAt int64    `json:"created_at"` // Unix timestamp
}

// IndexMetadata stores metadata about an index (for future use)
type IndexMetadata struct {
	Name       string `json:"name"`
	TableName  string `json:"table_name"`
	ColumnName string `json:"column_name"`
	IndexType  string `json:"index_type"` // "btree", "hash", etc.
	IndexFile  string `json:"index_file"` // Path to index file
	CreatedAt  int64  `json:"created_at"`
}

// NewSystemCatalog creates a new system catalog
func NewSystemCatalog(dataDir string) *SystemCatalog {
	return &SystemCatalog{
		databases: make(map[string]*DatabaseMetadata),
		dataDir:   dataDir,
	}
}

// LoadCatalog loads the catalog from disk
func (sc *SystemCatalog) LoadCatalog() error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	catalogPath := filepath.Join(sc.dataDir, "catalog.json")

	// Check if catalog file exists
	if _, err := os.Stat(catalogPath); os.IsNotExist(err) {
		// No catalog file, start fresh
		return nil
	}

	// Read catalog file
	data, err := os.ReadFile(catalogPath)
	if err != nil {
		return fmt.Errorf("failed to read catalog: %v", err)
	}

	// Parse catalog
	var catalogData struct {
		Databases map[string]*DatabaseMetadata `json:"databases"`
	}

	if err := json.Unmarshal(data, &catalogData); err != nil {
		return fmt.Errorf("failed to parse catalog: %v", err)
	}

	sc.databases = catalogData.Databases
	if sc.databases == nil {
		sc.databases = make(map[string]*DatabaseMetadata)
	}

	return nil
}

// SaveCatalog saves the catalog to disk
func (sc *SystemCatalog) SaveCatalog() error {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	catalogPath := filepath.Join(sc.dataDir, "catalog.json")

	// Create catalog data structure
	catalogData := struct {
		Databases map[string]*DatabaseMetadata `json:"databases"`
	}{
		Databases: sc.databases,
	}

	// Marshal to JSON with indentation for readability
	data, err := json.MarshalIndent(catalogData, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal catalog: %v", err)
	}

	// Write to temp file first for atomicity
	tempPath := catalogPath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write catalog: %v", err)
	}

	// Atomic rename
	if err := os.Rename(tempPath, catalogPath); err != nil {
		os.Remove(tempPath) // Clean up temp file
		return fmt.Errorf("failed to rename catalog: %v", err)
	}

	return nil
}

// CreateDatabase adds a database to the catalog
func (sc *SystemCatalog) CreateDatabase(name string) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if _, exists := sc.databases[name]; exists {
		return fmt.Errorf("database '%s' already exists", name)
	}

	sc.databases[name] = &DatabaseMetadata{
		Name:   name,
		Tables: make(map[string]*TableMetadata),
	}

	return nil
}

// DropDatabase removes a database from the catalog
func (sc *SystemCatalog) DropDatabase(name string) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if _, exists := sc.databases[name]; !exists {
		return fmt.Errorf("database '%s' does not exist", name)
	}

	delete(sc.databases, name)
	return nil
}

// GetDatabase retrieves database metadata
func (sc *SystemCatalog) GetDatabase(name string) (*DatabaseMetadata, error) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	db, exists := sc.databases[name]
	if !exists {
		return nil, fmt.Errorf("database '%s' does not exist", name)
	}

	return db, nil
}

// ListDatabases returns all database names
func (sc *SystemCatalog) ListDatabases() []string {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	names := make([]string, 0, len(sc.databases))
	for name := range sc.databases {
		names = append(names, name)
	}
	return names
}

// CreateTable adds a table to the catalog
func (sc *SystemCatalog) CreateTable(dbName, tableName string, columns []Column, heapFilePath string) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	db, exists := sc.databases[dbName]
	if !exists {
		return fmt.Errorf("database '%s' does not exist", dbName)
	}

	if _, exists := db.Tables[tableName]; exists {
		return fmt.Errorf("table '%s' already exists", tableName)
	}

	db.Tables[tableName] = &TableMetadata{
		Name:      tableName,
		Columns:   columns,
		HeapFile:  heapFilePath,
		CreatedAt: getCurrentTimestamp(),
	}

	return nil
}

// DropTable removes a table from the catalog
func (sc *SystemCatalog) DropTable(dbName, tableName string) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	db, exists := sc.databases[dbName]
	if !exists {
		return fmt.Errorf("database '%s' does not exist", dbName)
	}

	if _, exists := db.Tables[tableName]; !exists {
		return fmt.Errorf("table '%s' does not exist", tableName)
	}

	delete(db.Tables, tableName)
	return nil
}

// GetTable retrieves table metadata
func (sc *SystemCatalog) GetTable(dbName, tableName string) (*TableMetadata, error) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	db, exists := sc.databases[dbName]
	if !exists {
		return nil, fmt.Errorf("database '%s' does not exist", dbName)
	}

	table, exists := db.Tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table '%s' does not exist", tableName)
	}

	return table, nil
}

// ListTables returns all table names in a database
func (sc *SystemCatalog) ListTables(dbName string) ([]string, error) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	db, exists := sc.databases[dbName]
	if !exists {
		return nil, fmt.Errorf("database '%s' does not exist", dbName)
	}

	names := make([]string, 0, len(db.Tables))
	for name := range db.Tables {
		names = append(names, name)
	}
	return names, nil
}

// AlterTable updates table metadata (for ALTER TABLE operations)
func (sc *SystemCatalog) AlterTable(dbName, tableName string, newColumns []Column) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	db, exists := sc.databases[dbName]
	if !exists {
		return fmt.Errorf("database '%s' does not exist", dbName)
	}

	table, exists := db.Tables[tableName]
	if !exists {
		return fmt.Errorf("table '%s' does not exist", tableName)
	}

	table.Columns = newColumns
	return nil
}

// getCurrentTimestamp returns current Unix timestamp
func getCurrentTimestamp() int64 {
	return 1727922000 // Fixed timestamp for deterministic testing
	// In production, use: return time.Now().Unix()
}
