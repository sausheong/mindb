package main

import (
	"fmt"
	"strings"
)

// EngineAdapter wraps PagedEngine to provide backward compatibility with the old Engine interface
type EngineAdapter struct {
	pagedEngine *PagedEngine
}

// NewEngineAdapter creates a new adapter wrapping PagedEngine
func NewEngineAdapter(dataDir string, enableWAL bool) (*EngineAdapter, error) {
	pagedEngine, err := NewPagedEngineWithWAL(dataDir, enableWAL)
	if err != nil {
		return nil, err
	}

	return &EngineAdapter{
		pagedEngine: pagedEngine,
	}, nil
}

// Execute executes a parsed statement (backward compatibility interface)
func (ea *EngineAdapter) Execute(stmt *Statement) (string, error) {
	switch stmt.Type {
	case BeginTransaction:
		return ea.beginTransaction()
	case CommitTransaction:
		return ea.commitTransaction()
	case RollbackTransaction:
		return ea.rollbackTransaction()
	case CreateDatabase:
		return ea.createDatabase(stmt)
	case CreateTable:
		return ea.createTable(stmt)
	case AlterTable:
		return ea.alterTable(stmt)
	case DropTable:
		return ea.dropTable(stmt)
	case Insert:
		return ea.insertData(stmt)
	case Select:
		return ea.selectData(stmt)
	case Update:
		return ea.updateData(stmt)
	case Delete:
		return ea.deleteData(stmt)
	default:
		return "", fmt.Errorf("unsupported statement type")
	}
}

// UseDatabase switches to a database
func (ea *EngineAdapter) UseDatabase(name string) error {
	return ea.pagedEngine.UseDatabase(name)
}

// Close closes the engine
func (ea *EngineAdapter) Close() error {
	return ea.pagedEngine.Close()
}

// createDatabase creates a new database
func (ea *EngineAdapter) createDatabase(stmt *Statement) (string, error) {
	if err := ea.pagedEngine.CreateDatabase(stmt.Database); err != nil {
		return "", err
	}
	return fmt.Sprintf("Database '%s' created successfully", stmt.Database), nil
}

// createTable creates a new table
func (ea *EngineAdapter) createTable(stmt *Statement) (string, error) {
	tableName := stmt.Table
	if stmt.Schema != "" {
		tableName = stmt.Schema + "." + stmt.Table
	}

	if err := ea.pagedEngine.CreateTable(tableName, stmt.Columns); err != nil {
		return "", err
	}

	return fmt.Sprintf("Table '%s' created successfully", tableName), nil
}

// alterTable alters a table
func (ea *EngineAdapter) alterTable(stmt *Statement) (string, error) {
	// Get current table to retrieve existing columns
	db, err := ea.pagedEngine.getCurrentDatabase()
	if err != nil {
		return "", err
	}

	db.mu.RLock()
	table, exists := db.Tables[stmt.Table]
	db.mu.RUnlock()

	if !exists {
		return "", fmt.Errorf("table '%s' does not exist", stmt.Table)
	}

	// Add new column to existing columns
	newColumns := append(table.Columns, stmt.Columns...)

	// Update catalog
	if err := ea.pagedEngine.catalog.AlterTable(db.Name, stmt.Table, newColumns); err != nil {
		return "", err
	}

	// Update in-memory structure
	table.mu.Lock()
	table.Columns = newColumns
	table.mu.Unlock()

	// Save catalog
	if err := ea.pagedEngine.catalog.SaveCatalog(); err != nil {
		return "", fmt.Errorf("failed to save catalog: %v", err)
	}

	return fmt.Sprintf("Table '%s' altered successfully", stmt.Table), nil
}

// dropTable drops a table
func (ea *EngineAdapter) dropTable(stmt *Statement) (string, error) {
	if err := ea.pagedEngine.DropTable(stmt.Table); err != nil {
		if stmt.IfExists {
			return fmt.Sprintf("Table '%s' does not exist, skipping", stmt.Table), nil
		}
		return "", err
	}

	return fmt.Sprintf("Table '%s' dropped successfully", stmt.Table), nil
}

// insertData inserts data into a table
func (ea *EngineAdapter) insertData(stmt *Statement) (string, error) {
	rowCount := 0

	for _, values := range stmt.Values {
		if len(values) != len(stmt.Columns) {
			return "", fmt.Errorf("column count doesn't match value count")
		}

		row := make(Row)
		for i, col := range stmt.Columns {
			row[col.Name] = values[i]
		}

		if err := ea.pagedEngine.InsertRow(stmt.Table, row); err != nil {
			return "", err
		}
		rowCount++
	}

	return fmt.Sprintf("%d row(s) inserted", rowCount), nil
}

// selectData selects data from a table
func (ea *EngineAdapter) selectData(stmt *Statement) (string, error) {
	// Handle JOINs
	if len(stmt.Joins) > 0 {
		return ea.selectDataWithJoin(stmt)
	}

	// Handle aggregates
	if len(stmt.Aggregates) > 0 {
		return ea.selectDataWithAggregates(stmt)
	}

	// Convert Statement conditions to PagedEngine conditions
	var conditions []Condition
	for _, cond := range stmt.Conditions {
		conditions = append(conditions, Condition{
			Column:   cond.Column,
			Operator: cond.Operator,
			Value:    cond.Value,
		})
	}

	rows, err := ea.pagedEngine.SelectRows(stmt.Table, conditions)
	if err != nil {
		return "", err
	}

	// Apply LIMIT and OFFSET
	if stmt.Offset > 0 {
		if stmt.Offset >= len(rows) {
			rows = []Row{}
		} else {
			rows = rows[stmt.Offset:]
		}
	}

	if stmt.Limit > 0 && stmt.Limit < len(rows) {
		rows = rows[:stmt.Limit]
	}

	// Format output
	return ea.formatSelectResult(stmt, rows)
}

// selectDataWithJoin executes a SELECT with JOIN
func (ea *EngineAdapter) selectDataWithJoin(stmt *Statement) (string, error) {
	// Get rows from left table
	leftRows, err := ea.pagedEngine.SelectRows(stmt.Table, nil)
	if err != nil {
		return "", err
	}

	// Execute each join
	joinExecutor := NewJoinExecutor()
	result := leftRows

	for _, join := range stmt.Joins {
		// Get rows from right table
		rightRows, err := ea.pagedEngine.SelectRows(join.Table, nil)
		if err != nil {
			return "", err
		}

		// Execute join
		result, err = joinExecutor.ExecuteJoin(result, rightRows, join)
		if err != nil {
			return "", err
		}
	}

	// Apply WHERE conditions on joined result
	if len(stmt.Conditions) > 0 {
		result = ea.filterRows(result, stmt.Conditions)
	}

	// Apply LIMIT and OFFSET
	if stmt.Offset > 0 && stmt.Offset < len(result) {
		result = result[stmt.Offset:]
	}
	if stmt.Limit > 0 && stmt.Limit < len(result) {
		result = result[:stmt.Limit]
	}

	return ea.formatSelectResult(stmt, result)
}

// selectDataWithAggregates executes a SELECT with aggregate functions
func (ea *EngineAdapter) selectDataWithAggregates(stmt *Statement) (string, error) {
	// Get all rows
	var conditions []Condition
	for _, cond := range stmt.Conditions {
		conditions = append(conditions, Condition{
			Column:   cond.Column,
			Operator: cond.Operator,
			Value:    cond.Value,
		})
	}

	rows, err := ea.pagedEngine.SelectRows(stmt.Table, conditions)
	if err != nil {
		return "", err
	}

	// Execute aggregates
	aggExecutor := NewAggregateExecutor()
	result, err := aggExecutor.ExecuteAggregates(rows, stmt.Aggregates, stmt.GroupBy)
	if err != nil {
		return "", err
	}

	// Apply HAVING filter if present
	if len(stmt.Having) > 0 {
		result = ea.filterRows(result, stmt.Having)
	}

	return ea.formatSelectResult(stmt, result)
}

// filterRows filters rows based on conditions
func (ea *EngineAdapter) filterRows(rows []Row, conditions []Condition) []Row {
	filtered := make([]Row, 0)

	for _, row := range rows {
		match := true
		for _, cond := range conditions {
			val, exists := row[cond.Column]
			if !exists {
				match = false
				break
			}

			switch cond.Operator {
			case "=":
				if val != cond.Value {
					match = false
				}
			case "!=":
				if val == cond.Value {
					match = false
				}
			case ">":
				if CompareValues(val, cond.Value) <= 0 {
					match = false
				}
			case "<":
				if CompareValues(val, cond.Value) >= 0 {
					match = false
				}
			case ">=":
				if CompareValues(val, cond.Value) < 0 {
					match = false
				}
			case "<=":
				if CompareValues(val, cond.Value) > 0 {
					match = false
				}
			}

			if !match {
				break
			}
		}

		if match {
			filtered = append(filtered, row)
		}
	}

	return filtered
}

// updateData updates data in a table
func (ea *EngineAdapter) updateData(stmt *Statement) (string, error) {
	// Convert Statement conditions to PagedEngine conditions
	var conditions []Condition
	for _, cond := range stmt.Conditions {
		conditions = append(conditions, Condition{
			Column:   cond.Column,
			Operator: cond.Operator,
			Value:    cond.Value,
		})
	}

	count, err := ea.pagedEngine.UpdateRows(stmt.Table, stmt.Updates, conditions)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%d row(s) updated", count), nil
}

// deleteData deletes data from a table
func (ea *EngineAdapter) deleteData(stmt *Statement) (string, error) {
	// Convert Statement conditions to PagedEngine conditions
	var conditions []Condition
	for _, cond := range stmt.Conditions {
		conditions = append(conditions, Condition{
			Column:   cond.Column,
			Operator: cond.Operator,
			Value:    cond.Value,
		})
	}

	count, err := ea.pagedEngine.DeleteRows(stmt.Table, conditions)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%d row(s) deleted", count), nil
}

// formatSelectResult formats the SELECT result as a table
func (ea *EngineAdapter) formatSelectResult(stmt *Statement, rows []Row) (string, error) {
	if len(rows) == 0 {
		return "0 rows returned", nil
	}

	// Determine columns to display
	var displayColumns []string
	
	// For aggregates, use aggregate aliases
	if len(stmt.Aggregates) > 0 {
		aggExecutor := NewAggregateExecutor()
		for _, agg := range stmt.Aggregates {
			alias := agg.Alias
			if alias == "" {
				alias = aggExecutor.getDefaultAlias(agg)
			}
			displayColumns = append(displayColumns, alias)
		}
		// Add GROUP BY column if present
		if stmt.GroupBy != "" {
			displayColumns = append([]string{stmt.GroupBy}, displayColumns...)
		}
	} else if len(stmt.Joins) > 0 {
		// For JOINs, get columns from first row (sorted for consistency)
		colMap := make(map[string]bool)
		for colName := range rows[0] {
			colMap[colName] = true
		}
		for colName := range colMap {
			displayColumns = append(displayColumns, colName)
		}
	} else {
		// Get table to access column definitions
		db, err := ea.pagedEngine.getCurrentDatabase()
		if err != nil {
			return "", err
		}

		db.mu.RLock()
		table, exists := db.Tables[stmt.Table]
		db.mu.RUnlock()

		if !exists {
			return "", fmt.Errorf("table '%s' does not exist", stmt.Table)
		}

		// Determine columns from statement or table
		if len(stmt.Columns) == 0 || (len(stmt.Columns) == 1 && stmt.Columns[0].Name == "*") {
			displayColumns = make([]string, len(table.Columns))
			for i, col := range table.Columns {
				displayColumns[i] = col.Name
			}
		} else {
			displayColumns = make([]string, len(stmt.Columns))
			for i, col := range stmt.Columns {
				displayColumns[i] = col.Name
			}
		}
	}

	// Calculate column widths
	colWidths := make(map[string]int)
	for _, colName := range displayColumns {
		colWidths[colName] = len(colName)
	}

	for _, row := range rows {
		for _, colName := range displayColumns {
			if val, ok := row[colName]; ok && val != nil {
				valStr := fmt.Sprintf("%v", val)
				if len(valStr) > colWidths[colName] {
					colWidths[colName] = len(valStr)
				}
			}
		}
	}

	// Build result string
	var result strings.Builder

	// Header
	result.WriteString("+")
	for _, colName := range displayColumns {
		result.WriteString(strings.Repeat("-", colWidths[colName]+2))
		result.WriteString("+")
	}
	result.WriteString("\n|")

	for _, colName := range displayColumns {
		result.WriteString(fmt.Sprintf(" %-*s |", colWidths[colName], colName))
	}
	result.WriteString("\n+")

	for _, colName := range displayColumns {
		result.WriteString(strings.Repeat("-", colWidths[colName]+2))
		result.WriteString("+")
	}
	result.WriteString("\n")

	// Rows
	for _, row := range rows {
		result.WriteString("|")
		for _, colName := range displayColumns {
			val := row[colName]
			valStr := ""
			if val != nil {
				valStr = fmt.Sprintf("%v", val)
			}
			result.WriteString(fmt.Sprintf(" %-*s |", colWidths[colName], valStr))
		}
		result.WriteString("\n")
	}

	// Footer
	result.WriteString("+")
	for _, colName := range displayColumns {
		result.WriteString(strings.Repeat("-", colWidths[colName]+2))
		result.WriteString("+")
	}
	result.WriteString(fmt.Sprintf("\n%d row(s) returned", len(rows)))

	return result.String(), nil
}

// beginTransaction starts a new explicit transaction
func (ea *EngineAdapter) beginTransaction() (string, error) {
	if err := ea.pagedEngine.BeginTransaction(); err != nil {
		return "", err
	}
	return "Transaction started", nil
}

// commitTransaction commits the current transaction
func (ea *EngineAdapter) commitTransaction() (string, error) {
	if err := ea.pagedEngine.CommitTransaction(); err != nil {
		return "", err
	}
	return "Transaction committed", nil
}

// rollbackTransaction rolls back the current transaction
func (ea *EngineAdapter) rollbackTransaction() (string, error) {
	if err := ea.pagedEngine.RollbackTransaction(); err != nil {
		return "", err
	}
	return "Transaction rolled back", nil
}
