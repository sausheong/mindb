package mindb

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Database represents a database
type Database struct {
	Name   string
	Tables map[string]*Table
	mu     sync.RWMutex
}

// Table represents a table
type Table struct {
	Name    string
	Columns []Column
	Rows    []Row
	mu      sync.RWMutex
}

// Row represents a row of data
type Row map[string]interface{}

// Engine manages databases
type Engine struct {
	databases      map[string]*Database
	currentDB      string
	persistence    *PersistenceManager
	mu             sync.RWMutex
}

// NewEngine creates a new database engine
func NewEngine() *Engine {
	return &Engine{
		databases: make(map[string]*Database),
	}
}

// NewEngineWithPersistence creates a new database engine with persistence
func NewEngineWithPersistence(dataDir string) (*Engine, error) {
	pm, err := NewPersistenceManager(dataDir)
	if err != nil {
		return nil, err
	}

	engine := &Engine{
		databases:   make(map[string]*Database),
		persistence: pm,
	}

	// Load existing databases
	if err := engine.loadAllDatabases(); err != nil {
		return nil, err
	}

	return engine, nil
}

// loadAllDatabases loads all databases from disk
func (e *Engine) loadAllDatabases() error {
	if e.persistence == nil {
		return nil
	}

	dbNames, err := e.persistence.ListDatabases()
	if err != nil {
		return err
	}

	for _, dbName := range dbNames {
		db, err := e.persistence.LoadDatabase(dbName)
		if err != nil {
			return fmt.Errorf("failed to load database %s: %v", dbName, err)
		}
		e.databases[dbName] = db
	}

	return nil
}

// saveDatabase saves a database to disk if persistence is enabled
func (e *Engine) saveDatabase(dbName string) error {
	if e.persistence == nil {
		return nil // Persistence not enabled
	}

	db, exists := e.databases[dbName]
	if !exists {
		return fmt.Errorf("database %s not found", dbName)
	}

	return e.persistence.SaveDatabase(db)
}

// Execute executes a parsed statement
func (e *Engine) Execute(stmt *Statement) (string, error) {
	switch stmt.Type {
	case CreateDatabase:
		return e.createDatabase(stmt)
	case CreateTable:
		return e.createTable(stmt)
	case AlterTable:
		return e.alterTable(stmt)
	case DropTable:
		return e.dropTable(stmt)
	case Select:
		return e.selectData(stmt)
	case Insert:
		return e.insertData(stmt)
	case Update:
		return e.updateData(stmt)
	case Delete:
		return e.deleteData(stmt)
	default:
		return "", fmt.Errorf("unknown statement type")
	}
}

// createDatabase creates a new database
func (e *Engine) createDatabase(stmt *Statement) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.databases[stmt.Database]; exists {
		if stmt.IfNotExists {
			return fmt.Sprintf("Database '%s' already exists, skipping", stmt.Database), nil
		}
		return "", fmt.Errorf("database '%s' already exists", stmt.Database)
	}

	e.databases[stmt.Database] = &Database{
		Name:   stmt.Database,
		Tables: make(map[string]*Table),
	}
	e.currentDB = stmt.Database

	// Save to disk
	if err := e.saveDatabase(stmt.Database); err != nil {
		return "", fmt.Errorf("failed to persist database: %v", err)
	}

	return fmt.Sprintf("Database '%s' created successfully", stmt.Database), nil
}

// createTable creates a new table
func (e *Engine) createTable(stmt *Statement) (string, error) {
	db, err := e.getCurrentDatabase()
	if err != nil {
		return "", err
	}

	db.mu.Lock()
	tableName := e.getFullTableName(stmt)
	if _, exists := db.Tables[tableName]; exists {
		db.mu.Unlock()
		if stmt.IfNotExists {
			return fmt.Sprintf("Table '%s' already exists, skipping", tableName), nil
		}
		return "", fmt.Errorf("table '%s' already exists", tableName)
	}

	db.Tables[tableName] = &Table{
		Name:    tableName,
		Columns: stmt.Columns,
		Rows:    make([]Row, 0),
	}
	db.mu.Unlock()

	// Save to disk (after releasing lock)
	if err := e.saveDatabase(db.Name); err != nil {
		return "", fmt.Errorf("failed to persist table: %v", err)
	}

	return fmt.Sprintf("Table '%s' created successfully", tableName), nil
}

// alterTable alters an existing table
func (e *Engine) alterTable(stmt *Statement) (string, error) {
	db, err := e.getCurrentDatabase()
	if err != nil {
		return "", err
	}

	db.mu.RLock()
	tableName := e.getFullTableName(stmt)
	table, exists := db.Tables[tableName]
	db.mu.RUnlock()

	if !exists {
		if stmt.IfExists {
			return fmt.Sprintf("Table '%s' does not exist, skipping", tableName), nil
		}
		return "", fmt.Errorf("table '%s' does not exist", tableName)
	}

	table.mu.Lock()

	// Check if column already exists
	for _, col := range table.Columns {
		if col.Name == stmt.NewColumn.Name {
			table.mu.Unlock()
			return "", fmt.Errorf("column '%s' already exists", stmt.NewColumn.Name)
		}
	}

	table.Columns = append(table.Columns, stmt.NewColumn)

	// Add default or null values for the new column in existing rows
	defaultVal := stmt.NewColumn.Default
	for i := range table.Rows {
		table.Rows[i][stmt.NewColumn.Name] = defaultVal
	}
	table.mu.Unlock()

	// Save to disk (after releasing lock)
	if err := e.saveDatabase(db.Name); err != nil {
		return "", fmt.Errorf("failed to persist changes: %v", err)
	}

	return fmt.Sprintf("Column '%s' added to table '%s'", stmt.NewColumn.Name, tableName), nil
}

// dropTable drops a table
func (e *Engine) dropTable(stmt *Statement) (string, error) {
	db, err := e.getCurrentDatabase()
	if err != nil {
		return "", err
	}

	db.mu.Lock()

	tableName := e.getFullTableName(stmt)
	if _, exists := db.Tables[tableName]; !exists {
		db.mu.Unlock()
		if stmt.IfExists {
			return fmt.Sprintf("Table '%s' does not exist, skipping", tableName), nil
		}
		return "", fmt.Errorf("table '%s' does not exist", tableName)
	}

	delete(db.Tables, tableName)
	db.mu.Unlock()

	// Save to disk (after releasing lock)
	if err := e.saveDatabase(db.Name); err != nil {
		return "", fmt.Errorf("failed to persist changes: %v", err)
	}

	return fmt.Sprintf("Table '%s' dropped successfully", tableName), nil
}

// insertData inserts data into a table
func (e *Engine) insertData(stmt *Statement) (string, error) {
	db, err := e.getCurrentDatabase()
	if err != nil {
		return "", err
	}

	db.mu.RLock()
	tableName := e.getFullTableName(stmt)
	table, exists := db.Tables[tableName]
	db.mu.RUnlock()

	if !exists {
		return "", fmt.Errorf("table '%s' does not exist", tableName)
	}

	table.mu.Lock()

	if len(stmt.Values) == 0 || len(stmt.Values[0]) != len(stmt.Columns) {
		table.mu.Unlock()
		return "", fmt.Errorf("column count doesn't match value count")
	}

	row := make(Row)
	// Initialize with defaults from table definition
	for _, col := range table.Columns {
		if col.Default != nil {
			row[col.Name] = col.Default
		}
	}
	// Override with provided values
	for i, col := range stmt.Columns {
		row[col.Name] = stmt.Values[0][i]
	}

	table.Rows = append(table.Rows, row)
	table.mu.Unlock()

	// Save to disk (after releasing lock)
	if err := e.saveDatabase(db.Name); err != nil {
		return "", fmt.Errorf("failed to persist insert: %v", err)
	}

	// Handle RETURNING clause
	if len(stmt.Returning) > 0 {
		return e.formatReturning(stmt.Returning, []Row{row}), nil
	}

	return fmt.Sprintf("1 row inserted into '%s'", tableName), nil
}

// selectData selects data from a table
func (e *Engine) selectData(stmt *Statement) (string, error) {
	db, err := e.getCurrentDatabase()
	if err != nil {
		return "", err
	}

	db.mu.RLock()
	tableName := e.getFullTableName(stmt)
	table, exists := db.Tables[tableName]
	db.mu.RUnlock()

	if !exists {
		return "", fmt.Errorf("table '%s' does not exist", tableName)
	}

	table.mu.RLock()
	defer table.mu.RUnlock()

	// Filter rows based on conditions
	var filteredRows []Row
	for _, row := range table.Rows {
		if e.matchesConditions(row, stmt.Conditions) {
			filteredRows = append(filteredRows, row)
		}
	}

	// Apply ORDER BY
	if stmt.OrderBy != "" {
		e.sortRows(filteredRows, stmt.OrderBy, stmt.OrderDesc)
	}

	// Apply GROUP BY (simple implementation - just unique values)
	if stmt.GroupBy != "" {
		filteredRows = e.groupRows(filteredRows, stmt.GroupBy)
	}

	// Apply OFFSET
	if stmt.Offset > 0 && stmt.Offset < len(filteredRows) {
		filteredRows = filteredRows[stmt.Offset:]
	} else if stmt.Offset >= len(filteredRows) {
		filteredRows = []Row{}
	}

	// Apply LIMIT
	if stmt.Limit > 0 && stmt.Limit < len(filteredRows) {
		filteredRows = filteredRows[:stmt.Limit]
	}

	// Select columns
	var columns []string
	if len(stmt.Columns) == 0 {
		// SELECT *
		for _, col := range table.Columns {
			columns = append(columns, col.Name)
		}
	} else {
		for _, col := range stmt.Columns {
			columns = append(columns, col.Name)
		}
	}

	// Format output
	return e.formatResult(columns, filteredRows), nil
}

// updateData updates data in a table
func (e *Engine) updateData(stmt *Statement) (string, error) {
	db, err := e.getCurrentDatabase()
	if err != nil {
		return "", err
	}

	db.mu.RLock()
	tableName := e.getFullTableName(stmt)
	table, exists := db.Tables[tableName]
	db.mu.RUnlock()

	if !exists {
		return "", fmt.Errorf("table '%s' does not exist", tableName)
	}

	table.mu.Lock()

	count := 0
	var updatedRows []Row
	for i := range table.Rows {
		if e.matchesConditions(table.Rows[i], stmt.Conditions) {
			for col, val := range stmt.Updates {
				table.Rows[i][col] = val
			}
			updatedRows = append(updatedRows, table.Rows[i])
			count++
		}
	}
	table.mu.Unlock()

	// Save to disk (after releasing lock)
	if count > 0 {
		if err := e.saveDatabase(db.Name); err != nil {
			return "", fmt.Errorf("failed to persist update: %v", err)
		}
	}

	// Handle RETURNING clause
	if len(stmt.Returning) > 0 {
		return e.formatReturning(stmt.Returning, updatedRows), nil
	}

	return fmt.Sprintf("%d row(s) updated in '%s'", count, tableName), nil
}

// deleteData deletes data from a table
func (e *Engine) deleteData(stmt *Statement) (string, error) {
	db, err := e.getCurrentDatabase()
	if err != nil {
		return "", err
	}

	db.mu.RLock()
	tableName := e.getFullTableName(stmt)
	table, exists := db.Tables[tableName]
	db.mu.RUnlock()

	if !exists {
		return "", fmt.Errorf("table '%s' does not exist", tableName)
	}

	table.mu.Lock()

	var newRows []Row
	var deletedRows []Row
	count := 0
	for _, row := range table.Rows {
		if !e.matchesConditions(row, stmt.Conditions) {
			newRows = append(newRows, row)
		} else {
			deletedRows = append(deletedRows, row)
			count++
		}
	}

	table.Rows = newRows
	table.mu.Unlock()

	// Save to disk (after releasing lock)
	if count > 0 {
		if err := e.saveDatabase(db.Name); err != nil {
			return "", fmt.Errorf("failed to persist delete: %v", err)
		}
	}

	// Handle RETURNING clause
	if len(stmt.Returning) > 0 {
		return e.formatReturning(stmt.Returning, deletedRows), nil
	}

	return fmt.Sprintf("%d row(s) deleted from '%s'", count, tableName), nil
}

// matchesConditions checks if a row matches the given conditions
func (e *Engine) matchesConditions(row Row, conditions []Condition) bool {
	if len(conditions) == 0 {
		return true
	}

	for _, cond := range conditions {
		rowVal, exists := row[cond.Column]
		if !exists {
			return false
		}

		if !e.compareValues(rowVal, cond.Operator, cond.Value) {
			return false
		}
	}

	return true
}

// compareValues compares two values based on the operator
func (e *Engine) compareValues(rowVal interface{}, operator string, condVal interface{}) bool {
	switch operator {
	case "=":
		return fmt.Sprintf("%v", rowVal) == fmt.Sprintf("%v", condVal)
	case "!=":
		return fmt.Sprintf("%v", rowVal) != fmt.Sprintf("%v", condVal)
	case ">":
		return e.numericCompare(rowVal, condVal) > 0
	case "<":
		return e.numericCompare(rowVal, condVal) < 0
	case ">=":
		return e.numericCompare(rowVal, condVal) >= 0
	case "<=":
		return e.numericCompare(rowVal, condVal) <= 0
	default:
		return false
	}
}

// numericCompare compares two values numerically
func (e *Engine) numericCompare(a, b interface{}) int {
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)

	var aNum, bNum float64
	fmt.Sscanf(aStr, "%f", &aNum)
	fmt.Sscanf(bStr, "%f", &bNum)

	if aNum > bNum {
		return 1
	} else if aNum < bNum {
		return -1
	}
	return 0
}

// sortRows sorts rows by a column
func (e *Engine) sortRows(rows []Row, column string, desc bool) {
	sort.Slice(rows, func(i, j int) bool {
		valI := fmt.Sprintf("%v", rows[i][column])
		valJ := fmt.Sprintf("%v", rows[j][column])

		if desc {
			return valI > valJ
		}
		return valI < valJ
	})
}

// groupRows groups rows by a column (returns unique values)
func (e *Engine) groupRows(rows []Row, column string) []Row {
	seen := make(map[string]bool)
	var result []Row

	for _, row := range rows {
		key := fmt.Sprintf("%v", row[column])
		if !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}

	return result
}

// formatResult formats the result as a table
func (e *Engine) formatResult(columns []string, rows []Row) string {
	if len(rows) == 0 {
		return "Empty set"
	}

	var result strings.Builder

	// Calculate column widths
	widths := make(map[string]int)
	for _, col := range columns {
		widths[col] = len(col)
	}

	for _, row := range rows {
		for _, col := range columns {
			val := fmt.Sprintf("%v", row[col])
			if len(val) > widths[col] {
				widths[col] = len(val)
			}
		}
	}

	// Print header
	result.WriteString("+")
	for _, col := range columns {
		result.WriteString(strings.Repeat("-", widths[col]+2))
		result.WriteString("+")
	}
	result.WriteString("\n|")

	for _, col := range columns {
		result.WriteString(fmt.Sprintf(" %-*s |", widths[col], col))
	}
	result.WriteString("\n+")

	for _, col := range columns {
		result.WriteString(strings.Repeat("-", widths[col]+2))
		result.WriteString("+")
	}
	result.WriteString("\n")

	// Print rows
	for _, row := range rows {
		result.WriteString("|")
		for _, col := range columns {
			val := fmt.Sprintf("%v", row[col])
			result.WriteString(fmt.Sprintf(" %-*s |", widths[col], val))
		}
		result.WriteString("\n")
	}

	// Print footer
	result.WriteString("+")
	for _, col := range columns {
		result.WriteString(strings.Repeat("-", widths[col]+2))
		result.WriteString("+")
	}
	result.WriteString("\n")

	result.WriteString(fmt.Sprintf("%d row(s) in set\n", len(rows)))

	return result.String()
}

// getCurrentDatabase gets the current database
func (e *Engine) getCurrentDatabase() (*Database, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.currentDB == "" {
		return nil, fmt.Errorf("no database selected")
	}

	db, exists := e.databases[e.currentDB]
	if !exists {
		return nil, fmt.Errorf("database '%s' does not exist", e.currentDB)
	}

	return db, nil
}

// UseDatabase switches to a different database
func (e *Engine) UseDatabase(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.databases[name]; !exists {
		return fmt.Errorf("database '%s' does not exist", name)
	}

	e.currentDB = name
	return nil
}

// getFullTableName returns the full table name (with schema if provided)
func (e *Engine) getFullTableName(stmt *Statement) string {
	if stmt.Schema != "" {
		return stmt.Schema + "." + stmt.Table
	}
	return stmt.Table
}

// formatReturning formats the RETURNING clause output
func (e *Engine) formatReturning(columns []string, rows []Row) string {
	if len(rows) == 0 {
		return "Empty set"
	}

	// If RETURNING *, get all columns from the first row
	if len(columns) == 1 && columns[0] == "*" {
		columns = []string{}
		for col := range rows[0] {
			columns = append(columns, col)
		}
		sort.Strings(columns)
	}

	return e.formatResult(columns, rows)
}
