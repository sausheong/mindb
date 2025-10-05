package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/sausheong/mindb"
)

// Adapter wraps mindb.EngineAdapter with context-aware operations
type Adapter struct {
	engine *mindb.EngineAdapter
	parser *mindb.Parser
}

// NewAdapter creates a new database adapter
func NewAdapter(dataDir string) (*Adapter, error) {
	// Initialize mindb engine with WAL enabled
	engine, err := mindb.NewEngineAdapter(dataDir, true)
	if err != nil {
		return nil, fmt.Errorf("failed to create engine: %w", err)
	}

	return &Adapter{
		engine: engine,
		parser: mindb.NewParser(),
	}, nil
}

// Query executes a read-only query
func (a *Adapter) Query(ctx context.Context, sql string, args []interface{}, limit int, database string) (columns []string, rows [][]interface{}, err error) {
	// Check context before execution
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	default:
	}

	// Switch database if specified
	if database != "" {
		if err := a.engine.UseDatabase(database); err != nil {
			return nil, nil, fmt.Errorf("use database error: %w", err)
		}
	}

	// Parse SQL
	stmt, err := a.parser.Parse(sql)
	if err != nil {
		return nil, nil, fmt.Errorf("parse error: %w", err)
	}

	// Execute query
	result, err := a.engine.Execute(stmt)
	if err != nil {
		return nil, nil, fmt.Errorf("query error: %w", err)
	}

	// Parse result string to extract columns and rows
	// The result format is a formatted table string
	columns, rows = parseResultString(result, limit)

	return columns, rows, nil
}

// Execute executes a DML/DDL statement
func (a *Adapter) Execute(ctx context.Context, sql string, args []interface{}, database string) (affectedRows int, returning *QueryResult, err error) {
	// Check context before execution
	select {
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	default:
	}

	// Handle USE DATABASE command (not parsed by standard parser)
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sql)), "USE ") {
		dbName := strings.TrimSpace(sql[4:])
		dbName = strings.TrimSuffix(dbName, ";")
		if err := a.engine.UseDatabase(dbName); err != nil {
			return 0, nil, fmt.Errorf("use database error: %w", err)
		}
		return 1, nil, nil
	}

	// Switch database if specified
	if database != "" {
		if err := a.engine.UseDatabase(database); err != nil {
			return 0, nil, fmt.Errorf("use database error: %w", err)
		}
	}

	// Parse SQL
	stmt, err := a.parser.Parse(sql)
	if err != nil {
		return 0, nil, fmt.Errorf("parse error: %w", err)
	}

	// Execute statement
	result, err := a.engine.Execute(stmt)
	if err != nil {
		return 0, nil, fmt.Errorf("execution error: %w", err)
	}

	// Parse affected rows from result
	affectedRows = parseAffectedRows(result)

	return affectedRows, nil, nil
}

// Close closes the database
func (a *Adapter) Close() error {
	return a.engine.Close()
}

// QueryResult represents query results
type QueryResult struct {
	Columns []string
	Rows    [][]interface{}
}

// parseResultString parses the formatted result string from mindb
func parseResultString(result string, limit int) (columns []string, rows [][]interface{}) {
	lines := strings.Split(strings.TrimSpace(result), "\n")
	if len(lines) == 0 {
		return nil, nil
	}

	// Check if it's an empty result
	if strings.Contains(result, "0 rows returned") {
		return nil, nil
	}

	// Find the header line (second line, after first separator)
	// Format:
	// +----+-------+-----+
	// | id | name  | age |
	// +----+-------+-----+
	// | 1  | Alice | 30  |
	// +----+-------+-----+
	
	var headerLineIdx = -1
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "|") && !strings.HasPrefix(trimmed, "+") {
			headerLineIdx = i
			break
		}
	}

	if headerLineIdx == -1 {
		return nil, nil
	}

	// Parse column headers
	headerLine := lines[headerLineIdx]
	parts := strings.Split(headerLine, "|")
	for _, part := range parts {
		col := strings.TrimSpace(part)
		if col != "" {
			columns = append(columns, col)
		}
	}

	// Parse data rows (start after the second separator line)
	dataStartIdx := headerLineIdx + 2
	if dataStartIdx >= len(lines) {
		return columns, nil
	}

	for i := dataStartIdx; i < len(lines); i++ {
		if limit > 0 && len(rows) >= limit {
			break
		}

		line := strings.TrimSpace(lines[i])
		
		// Skip separator lines and footer
		if strings.HasPrefix(line, "+") || strings.Contains(line, "row(s) returned") {
			continue
		}

		// Parse data row
		if strings.HasPrefix(line, "|") {
			parts := strings.Split(line, "|")
			var row []interface{}
			for j, part := range parts {
				// Skip first and last empty parts from split
				if j == 0 || j == len(parts)-1 {
					continue
				}
				val := strings.TrimSpace(part)
				row = append(row, val)
			}
			if len(row) > 0 {
				rows = append(rows, row)
			}
		}
	}

	return columns, rows
}

// parseAffectedRows extracts affected row count from result string
func parseAffectedRows(result string) int {
	// Look for patterns like "1 row affected" or "Query OK"
	if strings.Contains(result, "row affected") || strings.Contains(result, "rows affected") {
		return 1 // Simplified - could parse actual number
	}
	if strings.Contains(result, "Query OK") || strings.Contains(result, "created successfully") {
		return 1
	}
	return 0
}
