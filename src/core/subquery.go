package mindb

import (
	"fmt"
	"regexp"
	"strings"
)

// SubqueryExecutor executes subqueries
type SubqueryExecutor struct {
	engine *PagedEngine
}

// NewSubqueryExecutor creates a new subquery executor
func NewSubqueryExecutor(engine *PagedEngine) *SubqueryExecutor {
	return &SubqueryExecutor{engine: engine}
}

// ExecuteScalarSubquery executes a subquery that returns a single value
func (se *SubqueryExecutor) ExecuteScalarSubquery(subquery *Statement) (interface{}, error) {
	// Execute the subquery
	rows, err := se.engine.SelectRows(subquery.Table, subquery.Conditions)
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		return nil, nil // Return NULL for empty result
	}

	if len(rows) > 1 {
		return nil, fmt.Errorf("scalar subquery returned more than one row")
	}

	// Get the first column value from the first row
	row := rows[0]
	for _, value := range row {
		return value, nil
	}

	return nil, nil
}

// ExecuteInSubquery executes a subquery for IN operator
func (se *SubqueryExecutor) ExecuteInSubquery(subquery *Statement) ([]interface{}, error) {
	// Execute the subquery
	rows, err := se.engine.SelectRows(subquery.Table, subquery.Conditions)
	if err != nil {
		return nil, err
	}

	// Extract values from first column
	values := make([]interface{}, 0, len(rows))
	for _, row := range rows {
		for _, value := range row {
			values = append(values, value)
			break // Only take first column
		}
	}

	return values, nil
}

// ExecuteExistsSubquery executes a subquery for EXISTS operator
func (se *SubqueryExecutor) ExecuteExistsSubquery(subquery *Statement) (bool, error) {
	// Execute the subquery
	rows, err := se.engine.SelectRows(subquery.Table, subquery.Conditions)
	if err != nil {
		return false, err
	}

	return len(rows) > 0, nil
}

// ParseSubquery extracts and parses a subquery from SQL
func ParseSubquery(sql string) (string, *Statement, error) {
	// Find subquery in parentheses
	subqueryRe := regexp.MustCompile(`\(([^()]+SELECT[^()]+)\)`)
	matches := subqueryRe.FindStringSubmatch(sql)
	
	if len(matches) < 2 {
		return sql, nil, nil // No subquery found
	}

	subquerySQL := matches[1]
	
	// Parse the subquery
	parser := NewParser()
	subqueryStmt, err := parser.Parse(subquerySQL)
	if err != nil {
		return "", nil, fmt.Errorf("failed to parse subquery: %v", err)
	}

	// Replace subquery with placeholder
	placeholder := "__SUBQUERY_RESULT__"
	modifiedSQL := strings.Replace(sql, matches[0], placeholder, 1)

	return modifiedSQL, subqueryStmt, nil
}

// ReplaceSubqueryPlaceholder replaces subquery placeholder with actual value
func ReplaceSubqueryPlaceholder(sql string, value interface{}) string {
	placeholder := "__SUBQUERY_RESULT__"
	
	var valueStr string
	switch v := value.(type) {
	case string:
		valueStr = fmt.Sprintf("'%s'", v)
	case nil:
		valueStr = "NULL"
	default:
		valueStr = fmt.Sprintf("%v", v)
	}

	return strings.Replace(sql, placeholder, valueStr, 1)
}

// HasSubquery checks if SQL contains a subquery
func HasSubquery(sql string) bool {
	sqlUpper := strings.ToUpper(sql)
	return strings.Contains(sqlUpper, "(SELECT")
}

// ExecuteWithSubquery executes a query with subquery support
func (se *SubqueryExecutor) ExecuteWithSubquery(sql string) ([]Row, error) {
	// Check for subquery
	if !HasSubquery(sql) {
		// No subquery, execute normally
		parser := NewParser()
		stmt, err := parser.Parse(sql)
		if err != nil {
			return nil, err
		}
		return se.engine.SelectRows(stmt.Table, stmt.Conditions)
	}

	// Parse and extract subquery
	modifiedSQL, subquery, err := ParseSubquery(sql)
	if err != nil {
		return nil, err
	}

	if subquery == nil {
		// No valid subquery found
		parser := NewParser()
		stmt, err := parser.Parse(sql)
		if err != nil {
			return nil, err
		}
		return se.engine.SelectRows(stmt.Table, stmt.Conditions)
	}

	// Determine subquery type and execute
	sqlUpper := strings.ToUpper(sql)
	
	if strings.Contains(sqlUpper, "IN (SELECT") {
		// IN subquery
		values, err := se.ExecuteInSubquery(subquery)
		if err != nil {
			return nil, err
		}
		
		// Convert to SQL IN clause
		var valueStrs []string
		for _, v := range values {
			switch val := v.(type) {
			case string:
				valueStrs = append(valueStrs, fmt.Sprintf("'%s'", val))
			default:
				valueStrs = append(valueStrs, fmt.Sprintf("%v", val))
			}
		}
		inClause := fmt.Sprintf("(%s)", strings.Join(valueStrs, ", "))
		modifiedSQL = strings.Replace(modifiedSQL, "__SUBQUERY_RESULT__", inClause, 1)
		
	} else if strings.Contains(sqlUpper, "EXISTS (SELECT") {
		// EXISTS subquery
		exists, err := se.ExecuteExistsSubquery(subquery)
		if err != nil {
			return nil, err
		}
		
		if !exists {
			// EXISTS returned false, return empty result
			return []Row{}, nil
		}
		
		// EXISTS returned true, remove the EXISTS clause and execute
		// This is simplified - in production would need more sophisticated handling
		modifiedSQL = strings.Replace(modifiedSQL, "EXISTS __SUBQUERY_RESULT__", "1=1", 1)
		
	} else {
		// Scalar subquery
		value, err := se.ExecuteScalarSubquery(subquery)
		if err != nil {
			return nil, err
		}
		
		modifiedSQL = ReplaceSubqueryPlaceholder(modifiedSQL, value)
	}

	// Parse and execute modified SQL
	parser := NewParser()
	stmt, err := parser.Parse(modifiedSQL)
	if err != nil {
		return nil, err
	}

	return se.engine.SelectRows(stmt.Table, stmt.Conditions)
}
