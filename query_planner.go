package mindb

// QueryPlan represents an execution plan for a query
type QueryPlan struct {
	UseIndex      bool
	IndexColumn   string
	IndexValues   []interface{}
	ScanType      ScanType
	EstimatedCost float64
}

// ScanType represents the type of scan to perform
type ScanType int

const (
	FullTableScan ScanType = iota
	IndexScan
	IndexSeek
)

// QueryPlanner creates optimal execution plans for queries
type QueryPlanner struct{}

// NewQueryPlanner creates a new query planner
func NewQueryPlanner() *QueryPlanner {
	return &QueryPlanner{}
}

// PlanDelete creates an execution plan for DELETE operations
func (qp *QueryPlanner) PlanDelete(table *PagedTable, conditions []Condition) *QueryPlan {
	return qp.planOperation(table, conditions)
}

// PlanUpdate creates an execution plan for UPDATE operations
func (qp *QueryPlanner) PlanUpdate(table *PagedTable, conditions []Condition) *QueryPlan {
	return qp.planOperation(table, conditions)
}

// PlanSelect creates an execution plan for SELECT operations
func (qp *QueryPlanner) PlanSelect(table *PagedTable, conditions []Condition) *QueryPlan {
	return qp.planOperation(table, conditions)
}

// planOperation creates an execution plan for any operation with WHERE clause
func (qp *QueryPlanner) planOperation(table *PagedTable, conditions []Condition) *QueryPlan {
	plan := &QueryPlan{
		UseIndex:    false,
		ScanType:    FullTableScan,
		IndexValues: make([]interface{}, 0),
	}

	// Check if we can use an index
	if len(conditions) == 0 {
		// No WHERE clause - must do full table scan
		plan.EstimatedCost = float64(len(table.TupleIDs))
		return plan
	}

	// Look for equality conditions on indexed columns
	for _, cond := range conditions {
		if cond.Operator == "=" {
			if _, hasIndex := table.Indexes[cond.Column]; hasIndex {
				// Found an indexed column with equality condition
				plan.UseIndex = true
				plan.IndexColumn = cond.Column
				plan.IndexValues = append(plan.IndexValues, cond.Value)
				plan.ScanType = IndexSeek
				plan.EstimatedCost = 1.0 // O(log n) for B-tree lookup
				return plan
			}
		}
	}

	// Check for range conditions on indexed columns
	for _, cond := range conditions {
		if cond.Operator == ">" || cond.Operator == "<" || 
		   cond.Operator == ">=" || cond.Operator == "<=" {
			if _, hasIndex := table.Indexes[cond.Column]; hasIndex {
				// Use index scan for range queries
				plan.UseIndex = true
				plan.IndexColumn = cond.Column
				plan.IndexValues = append(plan.IndexValues, cond.Value)
				plan.ScanType = IndexScan
				plan.EstimatedCost = float64(len(table.TupleIDs)) * 0.3 // Estimate 30% of rows
				return plan
			}
		}
	}

	// No index available - full table scan
	plan.EstimatedCost = float64(len(table.TupleIDs))
	return plan
}

// ExecutePlan executes a query plan and returns matching tuple IDs
func (qp *QueryPlanner) ExecutePlan(plan *QueryPlan, table *PagedTable, conditions []Condition) []TupleID {
	if !plan.UseIndex {
		// Full table scan
		return table.TupleIDs
	}

	// Index seek for equality conditions
	if plan.ScanType == IndexSeek && len(plan.IndexValues) > 0 {
		index := table.Indexes[plan.IndexColumn]
		result := make([]TupleID, 0)
		
		for _, value := range plan.IndexValues {
			if tid, found := index.Search(value); found {
				result = append(result, tid)
			}
		}
		
		return result
	}

	// Index scan for range queries (Phase 3 optimization)
	if plan.ScanType == IndexScan && len(conditions) > 0 {
		index := table.Indexes[plan.IndexColumn]
		cond := conditions[0]
		
		var startKey, endKey interface{}
		
		switch cond.Operator {
		case ">", ">=":
			// Range from value to infinity
			startKey = cond.Value
			endKey = nil
		case "<", "<=":
			// Range from negative infinity to value
			startKey = nil
			endKey = cond.Value
		default:
			// Unsupported operator, fall back to full scan
			return table.TupleIDs
		}
		
		// Use B+ tree RangeSearch for optimized range query
		return index.RangeSearch(startKey, endKey)
	}

	// Fallback to full scan
	return table.TupleIDs
}
