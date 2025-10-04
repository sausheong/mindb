package main

import (
	"testing"
)

func TestJoinSupport(t *testing.T) {
	tmpDir := t.TempDir()
	
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()
	
	// Create database
	engine.CreateDatabase("testdb")
	engine.UseDatabase("testdb")
	
	// Create users table
	usersColumns := []Column{
		{Name: "id", DataType: "INT", PrimaryKey: true},
		{Name: "name", DataType: "VARCHAR"},
	}
	engine.CreateTable("users", usersColumns)
	
	// Create orders table
	ordersColumns := []Column{
		{Name: "id", DataType: "INT", PrimaryKey: true},
		{Name: "user_id", DataType: "INT"},
		{Name: "amount", DataType: "INT"},
	}
	engine.CreateTable("orders", ordersColumns)
	
	// Insert test data
	engine.InsertRow("users", Row{"id": 1, "name": "Alice"})
	engine.InsertRow("users", Row{"id": 2, "name": "Bob"})
	engine.InsertRow("users", Row{"id": 3, "name": "Charlie"})
	
	engine.InsertRow("orders", Row{"id": 1, "user_id": 1, "amount": 100})
	engine.InsertRow("orders", Row{"id": 2, "user_id": 1, "amount": 200})
	engine.InsertRow("orders", Row{"id": 3, "user_id": 2, "amount": 150})
	
	// Test INNER JOIN
	t.Run("INNER JOIN", func(t *testing.T) {
		parser := NewParser()
		stmt, err := parser.Parse("SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id")
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		
		if len(stmt.Joins) != 1 {
			t.Errorf("Expected 1 join, got %d", len(stmt.Joins))
		}
		
		if stmt.Joins[0].Type != InnerJoin {
			t.Errorf("Expected INNER JOIN, got %v", stmt.Joins[0].Type)
		}
		
		t.Logf("JOIN parsed successfully: %+v", stmt.Joins[0])
	})
	
	// Test LEFT JOIN
	t.Run("LEFT JOIN", func(t *testing.T) {
		parser := NewParser()
		stmt, err := parser.Parse("SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id")
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		
		if stmt.Joins[0].Type != LeftJoin {
			t.Errorf("Expected LEFT JOIN, got %v", stmt.Joins[0].Type)
		}
	})
}

func TestAggregateFunctions(t *testing.T) {
	tmpDir := t.TempDir()
	
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()
	
	// Create database and table
	engine.CreateDatabase("testdb")
	engine.UseDatabase("testdb")
	
	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "department", DataType: "VARCHAR"},
		{Name: "salary", DataType: "INT"},
	}
	engine.CreateTable("employees", columns)
	
	// Insert test data
	engine.InsertRow("employees", Row{"id": 1, "department": "Sales", "salary": 50000})
	engine.InsertRow("employees", Row{"id": 2, "department": "Sales", "salary": 60000})
	engine.InsertRow("employees", Row{"id": 3, "department": "Engineering", "salary": 80000})
	engine.InsertRow("employees", Row{"id": 4, "department": "Engineering", "salary": 90000})
	
	// Test COUNT
	t.Run("COUNT(*)", func(t *testing.T) {
		parser := NewParser()
		stmt, err := parser.Parse("SELECT COUNT(*) FROM employees")
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		
		if len(stmt.Aggregates) != 1 {
			t.Errorf("Expected 1 aggregate, got %d", len(stmt.Aggregates))
		}
		
		if stmt.Aggregates[0].Type != CountFunc {
			t.Errorf("Expected COUNT, got %v", stmt.Aggregates[0].Type)
		}
		
		t.Logf("COUNT parsed successfully: %+v", stmt.Aggregates[0])
	})
	
	// Test SUM
	t.Run("SUM(salary)", func(t *testing.T) {
		parser := NewParser()
		stmt, err := parser.Parse("SELECT SUM(salary) FROM employees")
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		
		if stmt.Aggregates[0].Type != SumFunc {
			t.Errorf("Expected SUM, got %v", stmt.Aggregates[0].Type)
		}
	})
	
	// Test AVG
	t.Run("AVG(salary)", func(t *testing.T) {
		parser := NewParser()
		stmt, err := parser.Parse("SELECT AVG(salary) FROM employees")
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		
		if stmt.Aggregates[0].Type != AvgFunc {
			t.Errorf("Expected AVG, got %v", stmt.Aggregates[0].Type)
		}
	})
	
	// Test GROUP BY
	t.Run("GROUP BY department", func(t *testing.T) {
		parser := NewParser()
		stmt, err := parser.Parse("SELECT department, COUNT(*) FROM employees GROUP BY department")
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		
		if stmt.GroupBy != "department" {
			t.Errorf("Expected GROUP BY department, got %s", stmt.GroupBy)
		}
		
		t.Logf("GROUP BY parsed successfully: %s", stmt.GroupBy)
	})
}

func TestAggregateExecution(t *testing.T) {
	tmpDir := t.TempDir()
	
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()
	
	engine.CreateDatabase("testdb")
	engine.UseDatabase("testdb")
	
	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "value", DataType: "INT"},
	}
	engine.CreateTable("numbers", columns)
	
	// Insert test data
	engine.InsertRow("numbers", Row{"id": 1, "value": 10})
	engine.InsertRow("numbers", Row{"id": 2, "value": 20})
	engine.InsertRow("numbers", Row{"id": 3, "value": 30})
	
	// Test aggregate execution
	rows, err := engine.SelectRows("numbers", nil)
	if err != nil {
		t.Fatalf("Failed to select rows: %v", err)
	}
	
	aggExecutor := NewAggregateExecutor()
	
	// Test COUNT
	t.Run("Execute COUNT", func(t *testing.T) {
		agg := AggregateFunc{Type: CountFunc, Column: "*"}
		result, err := aggExecutor.ExecuteAggregates(rows, []AggregateFunc{agg}, "")
		if err != nil {
			t.Fatalf("Failed to execute COUNT: %v", err)
		}
		
		if len(result) != 1 {
			t.Errorf("Expected 1 result row, got %d", len(result))
		}
		
		count := result[0]["count(*)"]
		if count != 3 {
			t.Errorf("Expected COUNT = 3, got %v", count)
		}
		
		t.Logf("COUNT result: %v", count)
	})
	
	// Test SUM
	t.Run("Execute SUM", func(t *testing.T) {
		agg := AggregateFunc{Type: SumFunc, Column: "value"}
		result, err := aggExecutor.ExecuteAggregates(rows, []AggregateFunc{agg}, "")
		if err != nil {
			t.Fatalf("Failed to execute SUM: %v", err)
		}
		
		sum := result[0]["sum(value)"]
		if sum != 60.0 {
			t.Errorf("Expected SUM = 60, got %v", sum)
		}
		
		t.Logf("SUM result: %v", sum)
	})
	
	// Test AVG
	t.Run("Execute AVG", func(t *testing.T) {
		agg := AggregateFunc{Type: AvgFunc, Column: "value"}
		result, err := aggExecutor.ExecuteAggregates(rows, []AggregateFunc{agg}, "")
		if err != nil {
			t.Fatalf("Failed to execute AVG: %v", err)
		}
		
		avg := result[0]["avg(value)"]
		if avg != 20.0 {
			t.Errorf("Expected AVG = 20, got %v", avg)
		}
		
		t.Logf("AVG result: %v", avg)
	})
}
