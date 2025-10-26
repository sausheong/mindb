package prepared

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/sausheong/mindb/src/core"
)

// PreparedStatement represents a prepared SQL statement
type PreparedStatement struct {
	ID        string
	SQL       string
	Statement *mindb.Statement
	CreatedAt time.Time
	LastUsed  time.Time
	UseCount  int64
}

// Manager manages prepared statements
type Manager struct {
	statements map[string]*PreparedStatement
	parser     *mindb.Parser
	mu         sync.RWMutex
	maxStmts   int
}

// NewManager creates a new prepared statement manager
func NewManager(maxStmts int) *Manager {
	return &Manager{
		statements: make(map[string]*PreparedStatement),
		parser:     mindb.NewParser(),
		maxStmts:   maxStmts,
	}
}

// Prepare prepares a SQL statement
func (m *Manager) Prepare(sql string) (*PreparedStatement, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if we've hit the limit
	if len(m.statements) >= m.maxStmts {
		// Evict least recently used
		m.evictLRU()
	}

	// Parse the statement
	stmt, err := m.parser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parse error: %v", err)
	}

	// Create prepared statement
	ps := &PreparedStatement{
		ID:        generateID(),
		SQL:       sql,
		Statement: stmt,
		CreatedAt: time.Now(),
		LastUsed:  time.Now(),
		UseCount:  0,
	}

	m.statements[ps.ID] = ps
	return ps, nil
}

// Get retrieves a prepared statement
func (m *Manager) Get(id string) (*PreparedStatement, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ps, exists := m.statements[id]
	if !exists {
		return nil, fmt.Errorf("prepared statement not found: %s", id)
	}

	return ps, nil
}

// Execute executes a prepared statement with parameters
func (m *Manager) Execute(id string, params []interface{}) (*mindb.Statement, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ps, exists := m.statements[id]
	if !exists {
		return nil, fmt.Errorf("prepared statement not found: %s", id)
	}

	// Update usage stats
	ps.LastUsed = time.Now()
	ps.UseCount++

	// Clone the statement and bind parameters
	stmt := m.cloneStatement(ps.Statement)
	if err := m.bindParameters(stmt, params); err != nil {
		return nil, err
	}

	return stmt, nil
}

// Close closes a prepared statement
func (m *Manager) Close(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.statements[id]; !exists {
		return fmt.Errorf("prepared statement not found: %s", id)
	}

	delete(m.statements, id)
	return nil
}

// CloseAll closes all prepared statements
func (m *Manager) CloseAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.statements = make(map[string]*PreparedStatement)
}

// Stats returns statistics about prepared statements
func (m *Manager) Stats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return map[string]interface{}{
		"total_statements": len(m.statements),
		"max_statements":   m.maxStmts,
	}
}

// evictLRU evicts the least recently used prepared statement
func (m *Manager) evictLRU() {
	var oldestID string
	var oldestTime time.Time

	for id, ps := range m.statements {
		if oldestID == "" || ps.LastUsed.Before(oldestTime) {
			oldestID = id
			oldestTime = ps.LastUsed
		}
	}

	if oldestID != "" {
		delete(m.statements, oldestID)
	}
}

// cloneStatement creates a copy of a statement
func (m *Manager) cloneStatement(stmt *mindb.Statement) *mindb.Statement {
	// Create a shallow copy
	clone := *stmt
	
	// Deep copy slices and maps
	if stmt.Columns != nil {
		clone.Columns = make([]mindb.Column, len(stmt.Columns))
		copy(clone.Columns, stmt.Columns)
	}
	
	if stmt.Values != nil {
		clone.Values = make([][]interface{}, len(stmt.Values))
		for i, row := range stmt.Values {
			clone.Values[i] = make([]interface{}, len(row))
			copy(clone.Values[i], row)
		}
	}
	
	if stmt.Conditions != nil {
		clone.Conditions = make([]mindb.Condition, len(stmt.Conditions))
		copy(clone.Conditions, stmt.Conditions)
	}
	
	if stmt.Updates != nil {
		clone.Updates = make(map[string]interface{})
		for k, v := range stmt.Updates {
			clone.Updates[k] = v
		}
	}
	
	return &clone
}

// bindParameters binds parameters to a statement
func (m *Manager) bindParameters(stmt *mindb.Statement, params []interface{}) error {
	// For now, simple parameter binding for VALUES
	// Future: Support ? placeholders in WHERE clauses
	
	if len(params) == 0 {
		return nil
	}

	// Bind to INSERT values
	if stmt.Type == mindb.Insert && len(stmt.Values) > 0 {
		if len(params) != len(stmt.Values[0]) {
			return fmt.Errorf("parameter count mismatch: expected %d, got %d", len(stmt.Values[0]), len(params))
		}
		stmt.Values[0] = params
	}

	// Bind to WHERE conditions
	paramIndex := 0
	for i := range stmt.Conditions {
		if stmt.Conditions[i].Value == "?" {
			if paramIndex >= len(params) {
				return fmt.Errorf("not enough parameters")
			}
			stmt.Conditions[i].Value = params[paramIndex]
			paramIndex++
		}
	}

	return nil
}

// generateID generates a random ID for prepared statements
func generateID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}
