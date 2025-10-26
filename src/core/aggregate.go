package mindb

import (
	"fmt"
	"strings"
)

// AggregateFuncType represents the type of aggregate function
type AggregateFuncType int

const (
	CountFunc AggregateFuncType = iota
	SumFunc
	AvgFunc
	MinFunc
	MaxFunc
)

// AggregateFunc represents an aggregate function in a query
type AggregateFunc struct {
	Type     AggregateFuncType
	Column   string
	Alias    string
	Distinct bool
}

// AggregateExecutor executes aggregate functions
type AggregateExecutor struct{}

// NewAggregateExecutor creates a new aggregate executor
func NewAggregateExecutor() *AggregateExecutor {
	return &AggregateExecutor{}
}

// ExecuteAggregates executes aggregate functions on rows
func (ae *AggregateExecutor) ExecuteAggregates(rows []Row, aggregates []AggregateFunc, groupBy string) ([]Row, error) {
	if groupBy == "" {
		// No grouping - single result row
		return ae.executeWithoutGrouping(rows, aggregates)
	}

	// With grouping - multiple result rows
	return ae.executeWithGrouping(rows, aggregates, groupBy)
}

// executeWithoutGrouping executes aggregates without GROUP BY
func (ae *AggregateExecutor) executeWithoutGrouping(rows []Row, aggregates []AggregateFunc) ([]Row, error) {
	if len(rows) == 0 {
		// Return single row with NULL/0 values
		result := make(Row)
		for _, agg := range aggregates {
			alias := agg.Alias
			if alias == "" {
				alias = ae.getDefaultAlias(agg)
			}
			result[alias] = ae.getEmptyValue(agg.Type)
		}
		return []Row{result}, nil
	}

	result := make(Row)

	for _, agg := range aggregates {
		value, err := ae.computeAggregate(rows, agg)
		if err != nil {
			return nil, err
		}

		alias := agg.Alias
		if alias == "" {
			alias = ae.getDefaultAlias(agg)
		}
		result[alias] = value
	}

	return []Row{result}, nil
}

// executeWithGrouping executes aggregates with GROUP BY
func (ae *AggregateExecutor) executeWithGrouping(rows []Row, aggregates []AggregateFunc, groupBy string) ([]Row, error) {
	// Group rows by the groupBy column
	groups := make(map[interface{}][]Row)

	for _, row := range rows {
		groupValue := row[groupBy]
		groups[groupValue] = append(groups[groupValue], row)
	}

	// Compute aggregates for each group
	results := make([]Row, 0, len(groups))

	for groupValue, groupRows := range groups {
		result := make(Row)
		result[groupBy] = groupValue

		for _, agg := range aggregates {
			value, err := ae.computeAggregate(groupRows, agg)
			if err != nil {
				return nil, err
			}

			alias := agg.Alias
			if alias == "" {
				alias = ae.getDefaultAlias(agg)
			}
			result[alias] = value
		}

		results = append(results, result)
	}

	return results, nil
}

// computeAggregate computes a single aggregate function
func (ae *AggregateExecutor) computeAggregate(rows []Row, agg AggregateFunc) (interface{}, error) {
	switch agg.Type {
	case CountFunc:
		return ae.count(rows, agg)
	case SumFunc:
		return ae.sum(rows, agg)
	case AvgFunc:
		return ae.avg(rows, agg)
	case MinFunc:
		return ae.min(rows, agg)
	case MaxFunc:
		return ae.max(rows, agg)
	default:
		return nil, fmt.Errorf("unsupported aggregate function")
	}
}

// count implements COUNT aggregate
func (ae *AggregateExecutor) count(rows []Row, agg AggregateFunc) (interface{}, error) {
	if agg.Column == "*" {
		return len(rows), nil
	}

	count := 0
	seen := make(map[interface{}]bool)

	for _, row := range rows {
		value, exists := row[agg.Column]
		if !exists || value == nil {
			continue
		}

		if agg.Distinct {
			if !seen[value] {
				count++
				seen[value] = true
			}
		} else {
			count++
		}
	}

	return count, nil
}

// sum implements SUM aggregate
func (ae *AggregateExecutor) sum(rows []Row, agg AggregateFunc) (interface{}, error) {
	var sum float64

	for _, row := range rows {
		value, exists := row[agg.Column]
		if !exists || value == nil {
			continue
		}

		numValue, err := toFloat64(value)
		if err != nil {
			return nil, fmt.Errorf("SUM requires numeric values: %v", err)
		}

		sum += numValue
	}

	return sum, nil
}

// avg implements AVG aggregate
func (ae *AggregateExecutor) avg(rows []Row, agg AggregateFunc) (interface{}, error) {
	var sum float64
	count := 0

	for _, row := range rows {
		value, exists := row[agg.Column]
		if !exists || value == nil {
			continue
		}

		numValue, err := toFloat64(value)
		if err != nil {
			return nil, fmt.Errorf("AVG requires numeric values: %v", err)
		}

		sum += numValue
		count++
	}

	if count == 0 {
		return nil, nil
	}

	return sum / float64(count), nil
}

// min implements MIN aggregate
func (ae *AggregateExecutor) min(rows []Row, agg AggregateFunc) (interface{}, error) {
	var minValue interface{}

	for _, row := range rows {
		value, exists := row[agg.Column]
		if !exists || value == nil {
			continue
		}

		if minValue == nil {
			minValue = value
		} else if CompareValues(value, minValue) < 0 {
			minValue = value
		}
	}

	return minValue, nil
}

// max implements MAX aggregate
func (ae *AggregateExecutor) max(rows []Row, agg AggregateFunc) (interface{}, error) {
	var maxValue interface{}

	for _, row := range rows {
		value, exists := row[agg.Column]
		if !exists || value == nil {
			continue
		}

		if maxValue == nil {
			maxValue = value
		} else if CompareValues(value, maxValue) > 0 {
			maxValue = value
		}
	}

	return maxValue, nil
}

// getDefaultAlias returns the default alias for an aggregate function
func (ae *AggregateExecutor) getDefaultAlias(agg AggregateFunc) string {
	funcName := ae.getFuncName(agg.Type)
	return strings.ToLower(funcName) + "(" + agg.Column + ")"
}

// getFuncName returns the function name as a string
func (ae *AggregateExecutor) getFuncName(funcType AggregateFuncType) string {
	switch funcType {
	case CountFunc:
		return "COUNT"
	case SumFunc:
		return "SUM"
	case AvgFunc:
		return "AVG"
	case MinFunc:
		return "MIN"
	case MaxFunc:
		return "MAX"
	default:
		return "UNKNOWN"
	}
}

// getEmptyValue returns the appropriate empty value for an aggregate type
func (ae *AggregateExecutor) getEmptyValue(funcType AggregateFuncType) interface{} {
	switch funcType {
	case CountFunc:
		return 0
	case SumFunc:
		return 0.0
	case AvgFunc:
		return nil
	case MinFunc:
		return nil
	case MaxFunc:
		return nil
	default:
		return nil
	}
}

// toFloat64 converts a value to float64
func toFloat64(value interface{}) (float64, error) {
	switch v := value.(type) {
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", value)
	}
}

// CompareValues compares two values for ordering (exported for use in join.go)
// Returns: -1 if a < b, 0 if a == b, 1 if a > b
func CompareValues(a, b interface{}) int {
	// Handle nil values
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Try numeric comparison first (handles int, int64, float64, float32)
	aNum, aOk := toNumeric(a)
	bNum, bOk := toNumeric(b)
	
	if aOk && bOk {
		if aNum < bNum {
			return -1
		} else if aNum > bNum {
			return 1
		}
		return 0
	}

	// String comparison
	aStr, aOk := a.(string)
	bStr, bOk := b.(string)
	
	if aOk && bOk {
		if aStr < bStr {
			return -1
		} else if aStr > bStr {
			return 1
		}
		return 0
	}

	// Fallback: try direct comparison
	if a == b {
		return 0
	}
	
	return 0 // Can't compare, consider equal
}

// toNumeric converts various numeric types to float64
func toNumeric(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case int32:
		return float64(val), true
	case float64:
		return val, true
	case float32:
		return float64(val), true
	default:
		return 0, false
	}
}
