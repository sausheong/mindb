package mindb

import (
	"fmt"
	"strings"
)

// JoinType represents the type of join
type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
	FullJoin
	CrossJoin
)

// JoinClause represents a JOIN in a query
type JoinClause struct {
	Type       JoinType
	Table      string
	Alias      string
	OnLeft     string // Left side of ON condition (e.g., "users.id")
	OnRight    string // Right side of ON condition (e.g., "orders.user_id")
	OnOperator string // Operator (usually "=")
}

// JoinExecutor executes join operations
type JoinExecutor struct{}

// NewJoinExecutor creates a new join executor
func NewJoinExecutor() *JoinExecutor {
	return &JoinExecutor{}
}

// ExecuteJoin performs a join between two tables
func (je *JoinExecutor) ExecuteJoin(leftRows []Row, rightRows []Row, join JoinClause) ([]Row, error) {
	switch join.Type {
	case InnerJoin:
		return je.innerJoin(leftRows, rightRows, join)
	case LeftJoin:
		return je.leftJoin(leftRows, rightRows, join)
	case RightJoin:
		return je.rightJoin(leftRows, rightRows, join)
	case FullJoin:
		return je.fullJoin(leftRows, rightRows, join)
	case CrossJoin:
		return je.crossJoin(leftRows, rightRows)
	default:
		return nil, fmt.Errorf("unsupported join type")
	}
}

// innerJoin performs an INNER JOIN
func (je *JoinExecutor) innerJoin(leftRows []Row, rightRows []Row, join JoinClause) ([]Row, error) {
	result := make([]Row, 0)

	for _, leftRow := range leftRows {
		for _, rightRow := range rightRows {
			if je.matchesJoinCondition(leftRow, rightRow, join) {
				// Merge rows
				mergedRow := je.mergeRows(leftRow, rightRow)
				result = append(result, mergedRow)
			}
		}
	}

	return result, nil
}

// leftJoin performs a LEFT JOIN
func (je *JoinExecutor) leftJoin(leftRows []Row, rightRows []Row, join JoinClause) ([]Row, error) {
	result := make([]Row, 0)

	for _, leftRow := range leftRows {
		matched := false
		for _, rightRow := range rightRows {
			if je.matchesJoinCondition(leftRow, rightRow, join) {
				mergedRow := je.mergeRows(leftRow, rightRow)
				result = append(result, mergedRow)
				matched = true
			}
		}

		// If no match, include left row with NULL values for right columns
		if !matched {
			mergedRow := je.mergeRowsWithNull(leftRow, rightRows)
			result = append(result, mergedRow)
		}
	}

	return result, nil
}

// rightJoin performs a RIGHT JOIN
func (je *JoinExecutor) rightJoin(leftRows []Row, rightRows []Row, join JoinClause) ([]Row, error) {
	result := make([]Row, 0)

	for _, rightRow := range rightRows {
		matched := false
		for _, leftRow := range leftRows {
			if je.matchesJoinCondition(leftRow, rightRow, join) {
				mergedRow := je.mergeRows(leftRow, rightRow)
				result = append(result, mergedRow)
				matched = true
			}
		}

		// If no match, include right row with NULL values for left columns
		if !matched {
			mergedRow := je.mergeRowsWithNull(rightRow, leftRows)
			result = append(result, mergedRow)
		}
	}

	return result, nil
}

// fullJoin performs a FULL OUTER JOIN
func (je *JoinExecutor) fullJoin(leftRows []Row, rightRows []Row, join JoinClause) ([]Row, error) {
	result := make([]Row, 0)
	rightMatched := make(map[int]bool)

	// First pass: match left rows
	for _, leftRow := range leftRows {
		matched := false
		for i, rightRow := range rightRows {
			if je.matchesJoinCondition(leftRow, rightRow, join) {
				mergedRow := je.mergeRows(leftRow, rightRow)
				result = append(result, mergedRow)
				rightMatched[i] = true
				matched = true
			}
		}

		if !matched {
			mergedRow := je.mergeRowsWithNull(leftRow, rightRows)
			result = append(result, mergedRow)
		}
	}

	// Second pass: unmatched right rows
	for i, rightRow := range rightRows {
		if !rightMatched[i] {
			mergedRow := je.mergeRowsWithNull(rightRow, leftRows)
			result = append(result, mergedRow)
		}
	}

	return result, nil
}

// crossJoin performs a CROSS JOIN (Cartesian product)
func (je *JoinExecutor) crossJoin(leftRows []Row, rightRows []Row) ([]Row, error) {
	result := make([]Row, 0, len(leftRows)*len(rightRows))

	for _, leftRow := range leftRows {
		for _, rightRow := range rightRows {
			mergedRow := je.mergeRows(leftRow, rightRow)
			result = append(result, mergedRow)
		}
	}

	return result, nil
}

// matchesJoinCondition checks if two rows match the join condition
func (je *JoinExecutor) matchesJoinCondition(leftRow Row, rightRow Row, join JoinClause) bool {
	// Extract column names (strip table prefix if present)
	leftCol := je.stripTablePrefix(join.OnLeft)
	rightCol := je.stripTablePrefix(join.OnRight)
	
	leftValue, leftExists := leftRow[leftCol]
	rightValue, rightExists := rightRow[rightCol]

	if !leftExists || !rightExists {
		return false
	}

	// Handle NULL values
	if leftValue == nil || rightValue == nil {
		return false
	}

	// Compare values based on operator
	switch join.OnOperator {
	case "=", "==":
		return leftValue == rightValue
	case "!=", "<>":
		return leftValue != rightValue
	case ">":
		return CompareValues(leftValue, rightValue) > 0
	case "<":
		return CompareValues(leftValue, rightValue) < 0
	case ">=":
		return CompareValues(leftValue, rightValue) >= 0
	case "<=":
		return CompareValues(leftValue, rightValue) <= 0
	default:
		return false
	}
}

// mergeRows merges two rows into one
func (je *JoinExecutor) mergeRows(leftRow Row, rightRow Row) Row {
	merged := make(Row)

	// Copy left row
	for k, v := range leftRow {
		merged[k] = v
	}

	// Copy right row
	for k, v := range rightRow {
		merged[k] = v
	}

	return merged
}

// mergeRowsWithNull merges a row with NULL values for missing columns
func (je *JoinExecutor) mergeRowsWithNull(row Row, otherRows []Row) Row {
	merged := make(Row)

	// Copy the existing row
	for k, v := range row {
		merged[k] = v
	}

	// Add NULL values for columns from other table
	if len(otherRows) > 0 {
		for k := range otherRows[0] {
			if _, exists := merged[k]; !exists {
				merged[k] = nil
			}
		}
	}

	return merged
}

// stripTablePrefix removes table prefix from column name (e.g., "users.id" -> "id")
func (je *JoinExecutor) stripTablePrefix(colName string) string {
	// Check if column has table prefix
	if idx := strings.Index(colName, "."); idx != -1 {
		return colName[idx+1:]
	}
	return colName
}
