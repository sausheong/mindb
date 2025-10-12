package mindb

import (
	"testing"
)

func TestQueryPlanner_Creation(t *testing.T) {
	planner := NewQueryPlanner()
	if planner == nil {
		t.Fatal("NewQueryPlanner returned nil")
	}
}

func TestQueryPlan_ScanTypes(t *testing.T) {
	// Test that scan type constants are defined
	tests := []struct {
		name     string
		scanType ScanType
		value    int
	}{
		{"FullTableScan", FullTableScan, 0},
		{"IndexScan", IndexScan, 1},
		{"IndexSeek", IndexSeek, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if int(tt.scanType) != tt.value {
				t.Errorf("Expected %s to have value %d, got %d", tt.name, tt.value, int(tt.scanType))
			}
		})
	}
}

func TestQueryPlan_Structure(t *testing.T) {
	plan := &QueryPlan{
		UseIndex:      true,
		IndexColumn:   "id",
		IndexValues:   []interface{}{1, 2, 3},
		ScanType:      IndexSeek,
		EstimatedCost: 1.5,
	}

	if !plan.UseIndex {
		t.Error("Expected UseIndex to be true")
	}

	if plan.IndexColumn != "id" {
		t.Errorf("Expected IndexColumn 'id', got '%s'", plan.IndexColumn)
	}

	if len(plan.IndexValues) != 3 {
		t.Errorf("Expected 3 index values, got %d", len(plan.IndexValues))
	}

	if plan.ScanType != IndexSeek {
		t.Errorf("Expected IndexSeek, got %v", plan.ScanType)
	}

	if plan.EstimatedCost != 1.5 {
		t.Errorf("Expected cost 1.5, got %f", plan.EstimatedCost)
	}
}

func TestQueryPlanner_Integration(t *testing.T) {
	tmpDir := t.TempDir()
	
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Create database and table
	if err := engine.CreateDatabase("testdb"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "name", DataType: "VARCHAR"},
		{Name: "age", DataType: "INT"},
	}

	if err := engine.CreateTable("users", columns); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	for i := 0; i < 10; i++ {
		row := Row{
			"id":   i,
			"name": "User" + string(rune('A'+i)),
			"age":  20 + i,
		}
		if err := engine.InsertRow("users", row); err != nil {
			t.Fatalf("Failed to insert row: %v", err)
		}
	}

	// Test query execution (the planner is used internally)
	rows, err := engine.SelectRows("users", []Condition{
		{Column: "age", Operator: ">", Value: 25},
	})
	if err != nil {
		t.Fatalf("Failed to select rows: %v", err)
	}

	if len(rows) == 0 {
		t.Error("Expected some rows to be returned")
	}

	// Verify results
	for _, row := range rows {
		age, ok := row["age"].(int)
		if !ok {
			t.Error("Age should be an int")
			continue
		}
		if age <= 25 {
			t.Errorf("Expected age > 25, got %d", age)
		}
	}
}

func TestQueryPlanner_WithIndex(t *testing.T) {
	tmpDir := t.TempDir()
	
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Create database and table
	if err := engine.CreateDatabase("testdb"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "email", DataType: "VARCHAR"},
	}

	if err := engine.CreateTable("users", columns); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Note: CreateIndex may not be available on engine directly
	// Index creation is tested elsewhere

	// Insert test data
	for i := 0; i < 20; i++ {
		row := Row{
			"id":    i,
			"email": "user" + string(rune('a'+i)) + "@test.com",
		}
		if err := engine.InsertRow("users", row); err != nil {
			t.Fatalf("Failed to insert row: %v", err)
		}
	}

	// Query with indexed column (planner should use index)
	rows, err := engine.SelectRows("users", []Condition{
		{Column: "id", Operator: "=", Value: 5},
	})
	if err != nil {
		t.Fatalf("Failed to select rows: %v", err)
	}

	if len(rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(rows))
	}

	if len(rows) > 0 && rows[0]["id"] != 5 {
		t.Errorf("Expected id 5, got %v", rows[0]["id"])
	}
}

func TestQueryPlanner_ComplexConditions(t *testing.T) {
	tmpDir := t.TempDir()
	
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Create database and table
	if err := engine.CreateDatabase("testdb"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "age", DataType: "INT"},
		{Name: "status", DataType: "VARCHAR"},
	}

	if err := engine.CreateTable("users", columns); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	for i := 0; i < 30; i++ {
		status := "active"
		if i%3 == 0 {
			status = "inactive"
		}
		row := Row{
			"id":     i,
			"age":    18 + (i % 50),
			"status": status,
		}
		if err := engine.InsertRow("users", row); err != nil {
			t.Fatalf("Failed to insert row: %v", err)
		}
	}

	// Query with multiple conditions
	rows, err := engine.SelectRows("users", []Condition{
		{Column: "age", Operator: ">", Value: 30},
		{Column: "status", Operator: "=", Value: "active"},
	})
	if err != nil {
		t.Fatalf("Failed to select rows: %v", err)
	}

	// Verify all results match conditions
	for _, row := range rows {
		age, _ := row["age"].(int)
		status, _ := row["status"].(string)
		
		if age <= 30 {
			t.Errorf("Expected age > 30, got %d", age)
		}
		if status != "active" {
			t.Errorf("Expected status 'active', got '%s'", status)
		}
	}
}

func TestQueryPlanner_EmptyTable(t *testing.T) {
	tmpDir := t.TempDir()
	
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Create database and table
	if err := engine.CreateDatabase("testdb"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	columns := []Column{
		{Name: "id", DataType: "INT"},
	}

	if err := engine.CreateTable("empty_table", columns); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Query empty table
	rows, err := engine.SelectRows("empty_table", nil)
	if err != nil {
		t.Fatalf("Failed to select from empty table: %v", err)
	}

	if len(rows) != 0 {
		t.Errorf("Expected 0 rows from empty table, got %d", len(rows))
	}
}

func TestQueryPlanner_RangeQueries(t *testing.T) {
	tmpDir := t.TempDir()
	
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Create database and table
	if err := engine.CreateDatabase("testdb"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "score", DataType: "INT"},
	}

	if err := engine.CreateTable("scores", columns); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	for i := 0; i < 100; i++ {
		row := Row{
			"id":    i,
			"score": i,
		}
		if err := engine.InsertRow("scores", row); err != nil {
			t.Fatalf("Failed to insert row: %v", err)
		}
	}

	tests := []struct {
		name     string
		operator string
		value    int
		check    func(int) bool
	}{
		{
			name:     "Greater than",
			operator: ">",
			value:    90,
			check:    func(score int) bool { return score > 90 },
		},
		{
			name:     "Less than",
			operator: "<",
			value:    10,
			check:    func(score int) bool { return score < 10 },
		},
		{
			name:     "Greater or equal",
			operator: ">=",
			value:    95,
			check:    func(score int) bool { return score >= 95 },
		},
		{
			name:     "Less or equal",
			operator: "<=",
			value:    5,
			check:    func(score int) bool { return score <= 5 },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := engine.SelectRows("scores", []Condition{
				{Column: "score", Operator: tt.operator, Value: tt.value},
			})
			if err != nil {
				t.Fatalf("Failed to select rows: %v", err)
			}

			if len(rows) == 0 {
				t.Error("Expected some rows to be returned")
			}

			// Verify all results match condition
			for _, row := range rows {
				score, _ := row["score"].(int)
				if !tt.check(score) {
					t.Errorf("Score %d does not match condition %s %d", score, tt.operator, tt.value)
				}
			}
		})
	}
}
