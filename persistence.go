package mindb

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// PersistenceManager handles saving and loading database data
type PersistenceManager struct {
	dataDir string
	mu      sync.RWMutex
}

// NewPersistenceManager creates a new persistence manager
func NewPersistenceManager(dataDir string) (*PersistenceManager, error) {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %v", err)
	}

	return &PersistenceManager{
		dataDir: dataDir,
	}, nil
}

// SerializableDatabase represents a database that can be serialized
type SerializableDatabase struct {
	Name   string                    `json:"name"`
	Tables map[string]*SerializableTable `json:"tables"`
}

// SerializableTable represents a table that can be serialized
type SerializableTable struct {
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
	Rows    []Row    `json:"rows"`
}

// SaveDatabase saves a database to disk
func (pm *PersistenceManager) SaveDatabase(db *Database) error {
	// Don't lock persistence manager here - it's already locked by caller in some cases
	// Just lock the database for reading
	db.mu.RLock()
	
	// Convert to serializable format
	serDB := SerializableDatabase{
		Name:   db.Name,
		Tables: make(map[string]*SerializableTable),
	}

	for tableName, table := range db.Tables {
		table.mu.RLock()
		serDB.Tables[tableName] = &SerializableTable{
			Name:    table.Name,
			Columns: table.Columns,
			Rows:    table.Rows,
		}
		table.mu.RUnlock()
	}
	
	db.mu.RUnlock()

	// Write to file (no locks held during I/O)
	pm.mu.Lock()
	defer pm.mu.Unlock()
	
	filename := filepath.Join(pm.dataDir, db.Name+".json")
	data, err := json.MarshalIndent(serDB, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal database: %v", err)
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("failed to write database file: %v", err)
	}

	return nil
}

// LoadDatabase loads a database from disk
func (pm *PersistenceManager) LoadDatabase(name string) (*Database, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	filename := filepath.Join(pm.dataDir, name+".json")
	
	// Check if file exists
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return nil, fmt.Errorf("database file not found: %s", name)
	}

	// Read file
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read database file: %v", err)
	}

	// Unmarshal
	var serDB SerializableDatabase
	if err := json.Unmarshal(data, &serDB); err != nil {
		return nil, fmt.Errorf("failed to unmarshal database: %v", err)
	}

	// Convert to runtime format
	db := &Database{
		Name:   serDB.Name,
		Tables: make(map[string]*Table),
	}

	for tableName, serTable := range serDB.Tables {
		db.Tables[tableName] = &Table{
			Name:    serTable.Name,
			Columns: serTable.Columns,
			Rows:    serTable.Rows,
		}
	}

	return db, nil
}

// ListDatabases lists all available database files
func (pm *PersistenceManager) ListDatabases() ([]string, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	files, err := os.ReadDir(pm.dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read data directory: %v", err)
	}

	var databases []string
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".json" {
			dbName := file.Name()[:len(file.Name())-5] // Remove .json extension
			databases = append(databases, dbName)
		}
	}

	return databases, nil
}

// DeleteDatabase deletes a database file
func (pm *PersistenceManager) DeleteDatabase(name string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	filename := filepath.Join(pm.dataDir, name+".json")
	
	if err := os.Remove(filename); err != nil {
		if os.IsNotExist(err) {
			return nil // Already deleted
		}
		return fmt.Errorf("failed to delete database file: %v", err)
	}

	return nil
}

// DatabaseExists checks if a database file exists
func (pm *PersistenceManager) DatabaseExists(name string) bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	filename := filepath.Join(pm.dataDir, name+".json")
	_, err := os.Stat(filename)
	return err == nil
}
