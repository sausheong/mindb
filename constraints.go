package mindb

import (
	"fmt"
)

// ConstraintValidator validates constraints on data operations
type ConstraintValidator struct{}

// NewConstraintValidator creates a new constraint validator
func NewConstraintValidator() *ConstraintValidator {
	return &ConstraintValidator{}
}

// ValidateInsert validates constraints before inserting a row
func (cv *ConstraintValidator) ValidateInsert(table *PagedTable, row Row) error {
	// Validate NOT NULL constraints
	for _, col := range table.Columns {
		if col.NotNull {
			value, exists := row[col.Name]
			if !exists || value == nil {
				return fmt.Errorf("column '%s' cannot be null", col.Name)
			}
		}
	}

	// Validate PRIMARY KEY and UNIQUE constraints
	for _, col := range table.Columns {
		value, exists := row[col.Name]
		if !exists {
			continue
		}

		// Check PRIMARY KEY uniqueness
		if col.PrimaryKey {
			if err := cv.checkUniqueness(table, col.Name, value); err != nil {
				return fmt.Errorf("PRIMARY KEY violation on column '%s': %v", col.Name, err)
			}
		}

		// Check UNIQUE constraint
		if col.Unique {
			if err := cv.checkUniqueness(table, col.Name, value); err != nil {
				return fmt.Errorf("UNIQUE constraint violation on column '%s': %v", col.Name, err)
			}
		}
	}

	return nil
}

// ValidateForeignKeys validates foreign key constraints before inserting a row
func (cv *ConstraintValidator) ValidateForeignKeys(table *PagedTable, row Row, engine *PagedEngine) error {
	for _, col := range table.Columns {
		if col.ForeignKey == nil {
			continue
		}

		value, exists := row[col.Name]
		if !exists || value == nil {
			// NULL values are allowed in foreign keys unless NOT NULL is specified
			continue
		}

		// Check if referenced value exists
		refTable, err := cv.getTable(engine, col.ForeignKey.RefTable)
		if err != nil {
			return fmt.Errorf("foreign key error: %v", err)
		}

		if !cv.valueExistsInColumn(refTable, col.ForeignKey.RefColumn, value) {
			return fmt.Errorf("FOREIGN KEY constraint violation: value %v does not exist in %s(%s)",
				value, col.ForeignKey.RefTable, col.ForeignKey.RefColumn)
		}
	}

	return nil
}

// getTable retrieves a table from the engine
func (cv *ConstraintValidator) getTable(engine *PagedEngine, tableName string) (*PagedTable, error) {
	db, err := engine.getCurrentDatabase()
	if err != nil {
		return nil, err
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	table, exists := db.Tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table '%s' does not exist", tableName)
	}

	return table, nil
}

// valueExistsInColumn checks if a value exists in a column
func (cv *ConstraintValidator) valueExistsInColumn(table *PagedTable, columnName string, value interface{}) bool {
	// Use index if available
	if btree, exists := table.Indexes[columnName]; exists {
		_, found := btree.Search(value)
		return found
	}

	// Fallback: scan all tuples
	table.mu.RLock()
	defer table.mu.RUnlock()

	for _, tid := range table.TupleIDs {
		tupleData, err := table.HeapFile.GetTuple(tid)
		if err != nil {
			continue
		}

		tuple, err := DeserializeTuple(tupleData)
		if err != nil {
			continue
		}

		if colValue, exists := tuple.Data[columnName]; exists && colValue == value {
			return true
		}
	}

	return false
}

// ValidateUpdate validates constraints before updating a row
func (cv *ConstraintValidator) ValidateUpdate(table *PagedTable, updates map[string]interface{}, tid TupleID) error {
	// Validate NOT NULL constraints
	for colName, value := range updates {
		// Find column definition
		var col *Column
		for i := range table.Columns {
			if table.Columns[i].Name == colName {
				col = &table.Columns[i]
				break
			}
		}

		if col != nil && col.NotNull && value == nil {
			return fmt.Errorf("column '%s' cannot be null", colName)
		}
	}

	// Validate PRIMARY KEY and UNIQUE constraints
	for colName, value := range updates {
		// Find column definition
		var col *Column
		for i := range table.Columns {
			if table.Columns[i].Name == colName {
				col = &table.Columns[i]
				break
			}
		}

		if col == nil {
			continue
		}

		// Check PRIMARY KEY uniqueness (excluding current tuple)
		if col.PrimaryKey {
			if err := cv.checkUniquenessExcluding(table, colName, value, tid); err != nil {
				return fmt.Errorf("PRIMARY KEY violation on column '%s': %v", colName, err)
			}
		}

		// Check UNIQUE constraint (excluding current tuple)
		if col.Unique {
			if err := cv.checkUniquenessExcluding(table, colName, value, tid); err != nil {
				return fmt.Errorf("UNIQUE constraint violation on column '%s': %v", colName, err)
			}
		}
	}

	return nil
}

// checkUniqueness checks if a value is unique in a column
// NOTE: Assumes caller already holds table lock
func (cv *ConstraintValidator) checkUniqueness(table *PagedTable, columnName string, value interface{}) error {
	// Use index if available
	if btree, exists := table.Indexes[columnName]; exists {
		_, found := btree.Search(value)
		if found {
			return fmt.Errorf("duplicate value: %v", value)
		}
		return nil
	}

	// Fallback: scan all tuples (no lock - caller holds it)
	// DEBUG: Check how many tuples we're scanning
	// fmt.Printf("DEBUG: Checking uniqueness for %s=%v, scanning %d tuples\n", columnName, value, len(table.TupleIDs))
	
	for _, tid := range table.TupleIDs {
		tupleData, err := table.HeapFile.GetTuple(tid)
		if err != nil {
			continue
		}

		tuple, err := DeserializeTuple(tupleData)
		if err != nil {
			continue
		}

		// Tuple.Data is already a Row (map[string]interface{})
		if rowValue, exists := tuple.Data[columnName]; exists && rowValue == value {
			return fmt.Errorf("duplicate value: %v", value)
		}
	}

	return nil
}

// checkUniquenessExcluding checks uniqueness excluding a specific tuple
// NOTE: Assumes caller already holds table lock
func (cv *ConstraintValidator) checkUniquenessExcluding(table *PagedTable, columnName string, value interface{}, excludeTID TupleID) error {
	// Use index if available
	if btree, exists := table.Indexes[columnName]; exists {
		foundTID, found := btree.Search(value)
		if found && foundTID != excludeTID {
			return fmt.Errorf("duplicate value: %v", value)
		}
		return nil
	}

	// Fallback: scan all tuples (no lock - caller holds it)
	for _, tid := range table.TupleIDs {
		// Skip the tuple we're updating
		if tid == excludeTID {
			continue
		}

		tupleData, err := table.HeapFile.GetTuple(tid)
		if err != nil {
			continue
		}

		tuple, err := DeserializeTuple(tupleData)
		if err != nil {
			continue
		}

		// Tuple.Data is already a Row (map[string]interface{})
		if rowValue, exists := tuple.Data[columnName]; exists && rowValue == value {
			return fmt.Errorf("duplicate value: %v", value)
		}
	}

	return nil
}

// ValidateSchema validates table schema constraints
func (cv *ConstraintValidator) ValidateSchema(columns []Column) error {
	// Check for multiple PRIMARY KEYs
	primaryKeyCount := 0
	for _, col := range columns {
		if col.PrimaryKey {
			primaryKeyCount++
		}
	}

	if primaryKeyCount > 1 {
		return fmt.Errorf("table can have at most one PRIMARY KEY")
	}

	// Validate column names are unique
	colNames := make(map[string]bool)
	for _, col := range columns {
		if colNames[col.Name] {
			return fmt.Errorf("duplicate column name: %s", col.Name)
		}
		colNames[col.Name] = true
	}

	return nil
}

// CreateIndexForConstraint creates an index for a constrained column
func (cv *ConstraintValidator) CreateIndexForConstraint(table *PagedTable, columnName string) error {
	// Check if index already exists
	if _, exists := table.Indexes[columnName]; exists {
		return nil // Index already exists
	}

	// Create B+ tree index
	btree := NewBTree()

	// Build index from existing data
	table.mu.RLock()
	defer table.mu.RUnlock()

	for _, tid := range table.TupleIDs {
		tupleData, err := table.HeapFile.GetTuple(tid)
		if err != nil {
			continue
		}

		tuple, err := DeserializeTuple(tupleData)
		if err != nil {
			continue
		}

		// Tuple.Data is already a Row (map[string]interface{})
		if value, exists := tuple.Data[columnName]; exists && value != nil {
			btree.Insert(value, tid)
		}
	}

	table.Indexes[columnName] = btree
	return nil
}
