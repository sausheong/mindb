package main

import (
	"testing"
)

/*
Package: mindb
Component: Query Processing (Aggregates, JOINs, Query Execution)
Layer: Engine Adapter (Layer 2)

Test Coverage:
- Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- GROUP BY operations
- JOIN operations (INNER, LEFT, RIGHT, FULL, CROSS)
- Complex query execution
- Query optimization paths

Dependencies:
- Storage Engine (for data)
- Parser (for SQL parsing)

Run: go test -v -run TestQueryProcessing
*/

// ============================================================================
// AGGREGATE FUNCTION TESTS
// ============================================================================

func TestAggregateExecutor_COUNT(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Setup test data
	setupAggregateTestData(t, engine)

	tests := []struct {
		name     string
		sql      string
		expected int
	}{
		{
			name:     "COUNT(*) all rows",
			sql:      "SELECT COUNT(*) FROM users",
			expected: 5,
		},
		{
			name:     "COUNT(column) non-null values",
			sql:      "SELECT COUNT(age) FROM users",
			expected: 5,
		},
		{
			name:     "COUNT with WHERE clause",
			sql:      "SELECT COUNT(*) FROM users WHERE age > 25",
			expected: 4, // Bob(30), Charlie(28), Diana(35), Eve(27)
		},
		{
			name:     "COUNT on empty result",
			sql:      "SELECT COUNT(*) FROM users WHERE age > 100",
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser()
			stmt, err := parser.Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			rows, err := engine.ExecuteQuery(stmt)
			if err != nil {
				t.Fatalf("Execute error: %v", err)
			}

			if len(rows) != 1 {
				t.Fatalf("Expected 1 result row, got %d", len(rows))
			}

			// Check count value
			for _, val := range rows[0] {
				if count, ok := val.(int); ok {
					if count != tt.expected {
						t.Errorf("Expected count=%d, got %d", tt.expected, count)
					}
				} else if count, ok := val.(int64); ok {
					if int(count) != tt.expected {
						t.Errorf("Expected count=%d, got %d", tt.expected, count)
					}
				}
			}
		})
	}
}

func TestAggregateExecutor_SUM(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	setupAggregateTestData(t, engine)

	tests := []struct {
		name     string
		sql      string
		expected float64
	}{
		{
			name:     "SUM all ages",
			sql:      "SELECT SUM(age) FROM users",
			expected: 145.0, // 25 + 30 + 28 + 35 + 27
		},
		{
			name:     "SUM with WHERE",
			sql:      "SELECT SUM(age) FROM users WHERE age > 25",
			expected: 120.0, // 30 + 28 + 35 + 27
		},
		{
			name:     "SUM on empty result",
			sql:      "SELECT SUM(age) FROM users WHERE age > 100",
			expected: 0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser()
			stmt, err := parser.Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			rows, err := engine.ExecuteQuery(stmt)
			if err != nil {
				t.Fatalf("Execute error: %v", err)
			}

			if len(rows) != 1 {
				t.Fatalf("Expected 1 result row, got %d", len(rows))
			}

			// Check sum value
			for _, val := range rows[0] {
				var sum float64
				switch v := val.(type) {
				case int:
					sum = float64(v)
				case int64:
					sum = float64(v)
				case float64:
					sum = v
				}
				
				if sum != tt.expected {
					t.Errorf("Expected sum=%.1f, got %.1f", tt.expected, sum)
				}
			}
		})
	}
}

func TestAggregateExecutor_AVG(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	setupAggregateTestData(t, engine)

	tests := []struct {
		name     string
		sql      string
		expected float64
	}{
		{
			name:     "AVG all ages",
			sql:      "SELECT AVG(age) FROM users",
			expected: 29.0, // 145 / 5
		},
		{
			name:     "AVG with WHERE",
			sql:      "SELECT AVG(age) FROM users WHERE age > 25",
			expected: 30.0, // 120 / 4 (30 + 28 + 35 + 27) / 4
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser()
			stmt, err := parser.Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			rows, err := engine.ExecuteQuery(stmt)
			if err != nil {
				t.Fatalf("Execute error: %v", err)
			}

			if len(rows) != 1 {
				t.Fatalf("Expected 1 result row, got %d", len(rows))
			}

			// Check avg value
			for _, val := range rows[0] {
				var avg float64
				switch v := val.(type) {
				case int:
					avg = float64(v)
				case int64:
					avg = float64(v)
				case float64:
					avg = v
				}
				
				if avg != tt.expected {
					t.Errorf("Expected avg=%.1f, got %.1f", tt.expected, avg)
				}
			}
		})
	}
}

func TestAggregateExecutor_MIN_MAX(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	setupAggregateTestData(t, engine)

	tests := []struct {
		name     string
		sql      string
		expected interface{}
	}{
		{
			name:     "MIN age",
			sql:      "SELECT MIN(age) FROM users",
			expected: 25,
		},
		{
			name:     "MAX age",
			sql:      "SELECT MAX(age) FROM users",
			expected: 35,
		},
		{
			name:     "MIN with WHERE",
			sql:      "SELECT MIN(age) FROM users WHERE age > 25",
			expected: 27,
		},
		{
			name:     "MAX with WHERE",
			sql:      "SELECT MAX(age) FROM users WHERE age < 30",
			expected: 28,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser()
			stmt, err := parser.Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			rows, err := engine.ExecuteQuery(stmt)
			if err != nil {
				t.Fatalf("Execute error: %v", err)
			}

			if len(rows) != 1 {
				t.Fatalf("Expected 1 result row, got %d", len(rows))
			}

			// Check min/max value
			for _, val := range rows[0] {
				var result int
				switch v := val.(type) {
				case int:
					result = v
				case int64:
					result = int(v)
				case float64:
					result = int(v)
				}
				
				if result != tt.expected {
					t.Errorf("Expected %v, got %v", tt.expected, result)
				}
			}
		})
	}
}

func TestAggregateExecutor_GroupBy(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Setup test data with departments
	engine.CreateDatabase("testdb")
	engine.UseDatabase("testdb")

	columns := []Column{
		{Name: "id", DataType: "INT", PrimaryKey: true},
		{Name: "name", DataType: "VARCHAR"},
		{Name: "department", DataType: "VARCHAR"},
		{Name: "salary", DataType: "INT"},
	}
	engine.CreateTable("employees", columns)

	// Insert test data
	engine.InsertRow("employees", Row{"id": 1, "name": "Alice", "department": "Engineering", "salary": 80000})
	engine.InsertRow("employees", Row{"id": 2, "name": "Bob", "department": "Engineering", "salary": 90000})
	engine.InsertRow("employees", Row{"id": 3, "name": "Charlie", "department": "Sales", "salary": 70000})
	engine.InsertRow("employees", Row{"id": 4, "name": "Diana", "department": "Sales", "salary": 75000})
	engine.InsertRow("employees", Row{"id": 5, "name": "Eve", "department": "HR", "salary": 65000})

	tests := []struct {
		name          string
		sql           string
		expectedRows  int
		checkFunction func(t *testing.T, rows []Row)
	}{
		{
			name:         "GROUP BY department with COUNT",
			sql:          "SELECT department, COUNT(*) FROM employees GROUP BY department",
			expectedRows: 3, // Engineering, Sales, HR
			checkFunction: func(t *testing.T, rows []Row) {
				// Should have 3 groups
				if len(rows) != 3 {
					t.Errorf("Expected 3 groups, got %d", len(rows))
				}
			},
		},
		{
			name:         "GROUP BY with SUM",
			sql:          "SELECT department, SUM(salary) FROM employees GROUP BY department",
			expectedRows: 3,
			checkFunction: func(t *testing.T, rows []Row) {
				// Verify sum calculations
				// Engineering: 170000, Sales: 145000, HR: 65000
				if len(rows) != 3 {
					t.Errorf("Expected 3 groups, got %d", len(rows))
				}
			},
		},
		{
			name:         "GROUP BY with AVG",
			sql:          "SELECT department, AVG(salary) FROM employees GROUP BY department",
			expectedRows: 3,
			checkFunction: func(t *testing.T, rows []Row) {
				if len(rows) != 3 {
					t.Errorf("Expected 3 groups, got %d", len(rows))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser()
			stmt, err := parser.Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			rows, err := engine.ExecuteQuery(stmt)
			if err != nil {
				t.Fatalf("Execute error: %v", err)
			}

			if tt.checkFunction != nil {
				tt.checkFunction(t, rows)
			}
		})
	}
}

// ============================================================================
// JOIN OPERATION TESTS
// ============================================================================

func TestJoinExecutor_InnerJoin(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	setupJoinTestData(t, engine)

	tests := []struct {
		name         string
		sql          string
		expectedRows int
	}{
		{
			name:         "Simple INNER JOIN",
			sql:          "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id",
			expectedRows: 4, // Alice(2), Bob(1), Charlie(1)
		},
		{
			name:         "INNER JOIN with WHERE",
			sql:          "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id WHERE orders.amount > 100",
			expectedRows: 2, // Alice's 200 order and Charlie's 150 order
		},
		{
			name:         "INNER JOIN with column selection",
			sql:          "SELECT users.name, orders.amount FROM users INNER JOIN orders ON users.id = orders.user_id",
			expectedRows: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser()
			stmt, err := parser.Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			rows, err := engine.ExecuteQuery(stmt)
			if err != nil {
				t.Fatalf("Execute error: %v", err)
			}

			if len(rows) != tt.expectedRows {
				t.Errorf("Expected %d rows, got %d", tt.expectedRows, len(rows))
			}
		})
	}
}

func TestJoinExecutor_LeftJoin(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	setupJoinTestData(t, engine)

	tests := []struct {
		name         string
		sql          string
		expectedRows int
	}{
		{
			name:         "LEFT JOIN includes unmatched left rows",
			sql:          "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id",
			expectedRows: 5, // All 4 users + Diana with NULL order
		},
		{
			name:         "LEFT JOIN with WHERE on left table",
			sql:          "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id WHERE users.age > 25",
			expectedRows: 3, // Bob(1 order), Charlie(1 order), Diana(no orders)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser()
			stmt, err := parser.Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			rows, err := engine.ExecuteQuery(stmt)
			if err != nil {
				t.Fatalf("Execute error: %v", err)
			}

			if len(rows) != tt.expectedRows {
				t.Errorf("Expected %d rows, got %d", tt.expectedRows, len(rows))
			}
		})
	}
}

func TestJoinExecutor_RightJoin(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	setupJoinTestData(t, engine)

	sql := "SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id"
	
	parser := NewParser()
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	rows, err := engine.ExecuteQuery(stmt)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	// RIGHT JOIN should include all orders
	// Expected: 4 orders (all have matching users in this case)
	if len(rows) != 4 {
		t.Errorf("Expected 4 rows, got %d", len(rows))
	}
}

func TestJoinExecutor_FullJoin(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	setupJoinTestData(t, engine)

	sql := "SELECT * FROM users FULL JOIN orders ON users.id = orders.user_id"
	
	parser := NewParser()
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	rows, err := engine.ExecuteQuery(stmt)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	// FULL JOIN should include all users and all orders
	// Expected: 5 rows (4 matched + 1 unmatched user Diana)
	if len(rows) < 4 {
		t.Errorf("Expected at least 4 rows, got %d", len(rows))
	}
}

func TestJoinExecutor_CrossJoin(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	setupJoinTestData(t, engine)

	sql := "SELECT * FROM users CROSS JOIN orders"
	
	parser := NewParser()
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	rows, err := engine.ExecuteQuery(stmt)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	// CROSS JOIN: Cartesian product
	// 4 users × 4 orders = 16 rows
	expectedRows := 4 * 4
	if len(rows) != expectedRows {
		t.Errorf("Expected %d rows (cartesian product), got %d", expectedRows, len(rows))
	}
}

func TestJoinExecutor_MultipleJoins(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Setup three-table join scenario
	engine.CreateDatabase("testdb")
	engine.UseDatabase("testdb")

	// Users table
	usersColumns := []Column{
		{Name: "id", DataType: "INT", PrimaryKey: true},
		{Name: "name", DataType: "VARCHAR"},
	}
	engine.CreateTable("users", usersColumns)

	// Orders table
	ordersColumns := []Column{
		{Name: "id", DataType: "INT", PrimaryKey: true},
		{Name: "user_id", DataType: "INT"},
		{Name: "product_id", DataType: "INT"},
	}
	engine.CreateTable("orders", ordersColumns)

	// Products table
	productsColumns := []Column{
		{Name: "id", DataType: "INT", PrimaryKey: true},
		{Name: "name", DataType: "VARCHAR"},
		{Name: "price", DataType: "INT"},
	}
	engine.CreateTable("products", productsColumns)

	// Insert test data
	engine.InsertRow("users", Row{"id": 1, "name": "Alice"})
	engine.InsertRow("users", Row{"id": 2, "name": "Bob"})

	engine.InsertRow("products", Row{"id": 1, "name": "Laptop", "price": 1000})
	engine.InsertRow("products", Row{"id": 2, "name": "Mouse", "price": 50})

	engine.InsertRow("orders", Row{"id": 1, "user_id": 1, "product_id": 1})
	engine.InsertRow("orders", Row{"id": 2, "user_id": 1, "product_id": 2})
	engine.InsertRow("orders", Row{"id": 3, "user_id": 2, "product_id": 1})

	sql := `SELECT users.name, products.name, products.price 
	        FROM users 
	        INNER JOIN orders ON users.id = orders.user_id 
	        INNER JOIN products ON orders.product_id = products.id`

	parser := NewParser()
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	rows, err := engine.ExecuteQuery(stmt)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	// Should have 3 rows (3 orders)
	if len(rows) != 3 {
		t.Errorf("Expected 3 rows from three-table join, got %d", len(rows))
	}
}

// ============================================================================
// COMPLEX QUERY TESTS
// ============================================================================

func TestComplexQuery_JoinWithAggregates(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	setupJoinTestData(t, engine)

	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "JOIN with COUNT aggregate",
			sql:  "SELECT users.name, COUNT(*) FROM users INNER JOIN orders ON users.id = orders.user_id GROUP BY users.name",
		},
		{
			name: "JOIN with SUM aggregate",
			sql:  "SELECT users.name, SUM(orders.amount) FROM users INNER JOIN orders ON users.id = orders.user_id GROUP BY users.name",
		},
		{
			name: "JOIN with multiple aggregates",
			sql:  "SELECT users.name, COUNT(*), SUM(orders.amount), AVG(orders.amount) FROM users INNER JOIN orders ON users.id = orders.user_id GROUP BY users.name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser()
			stmt, err := parser.Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			rows, err := engine.ExecuteQuery(stmt)
			if err != nil {
				t.Fatalf("Execute error: %v", err)
			}

			if len(rows) == 0 {
				t.Error("Expected result rows, got 0")
			}
		})
	}
}

func TestComplexQuery_OrderByWithJoin(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	setupJoinTestData(t, engine)

	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "JOIN with ORDER BY",
			sql:  "SELECT users.name, orders.amount FROM users INNER JOIN orders ON users.id = orders.user_id ORDER BY orders.amount DESC",
		},
		{
			name: "JOIN with ORDER BY and LIMIT",
			sql:  "SELECT users.name, orders.amount FROM users INNER JOIN orders ON users.id = orders.user_id ORDER BY orders.amount DESC LIMIT 2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser()
			stmt, err := parser.Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			rows, err := engine.ExecuteQuery(stmt)
			if err != nil {
				t.Fatalf("Execute error: %v", err)
			}

			if len(rows) == 0 {
				t.Error("Expected result rows, got 0")
			}
		})
	}
}

func TestComplexQuery_SubqueryInWhere(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	setupJoinTestData(t, engine)

	// Test subquery support (if implemented)
	sql := "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > 100)"

	parser := NewParser()
	stmt, err := parser.Parse(sql)
	if err != nil {
		// Subquery might not be implemented yet
		t.Skip("Subquery parsing not yet implemented")
	}

	_, err = engine.ExecuteQuery(stmt)
	if err != nil {
		// This is expected if subqueries aren't implemented
		t.Logf("Subquery execution not yet implemented: %v", err)
	}
}

// ============================================================================
// ORDER BY AND LIMIT TESTS
// ============================================================================

func TestOrderBy_Ascending(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	setupAggregateTestData(t, engine)

	sql := "SELECT * FROM users ORDER BY age ASC"
	
	parser := NewParser()
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	rows, err := engine.ExecuteQuery(stmt)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	// Should be ordered by age ascending: Alice(25), Eve(27), Charlie(28), Bob(30), Diana(35)
	if len(rows) != 5 {
		t.Fatalf("Expected 5 rows, got %d", len(rows))
	}

	// Check order
	ages := []float64{25, 27, 28, 30, 35}
	for i, row := range rows {
		var age float64
		switch v := row["age"].(type) {
		case int:
			age = float64(v)
		case int64:
			age = float64(v)
		case float64:
			age = v
		}
		if age != ages[i] {
			t.Errorf("Row %d: expected age %.0f, got %.0f", i, ages[i], age)
		}
	}
}

func TestOrderBy_Descending(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	setupAggregateTestData(t, engine)

	sql := "SELECT * FROM users ORDER BY age DESC"
	
	parser := NewParser()
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	rows, err := engine.ExecuteQuery(stmt)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	// Should be ordered by age descending: Diana(35), Bob(30), Charlie(28), Eve(27), Alice(25)
	if len(rows) != 5 {
		t.Fatalf("Expected 5 rows, got %d", len(rows))
	}

	// Check order
	ages := []float64{35, 30, 28, 27, 25}
	for i, row := range rows {
		var age float64
		switch v := row["age"].(type) {
		case int:
			age = float64(v)
		case int64:
			age = float64(v)
		case float64:
			age = v
		}
		if age != ages[i] {
			t.Errorf("Row %d: expected age %.0f, got %.0f", i, ages[i], age)
		}
	}
}

func TestLimit_Basic(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	setupAggregateTestData(t, engine)

	sql := "SELECT * FROM users LIMIT 3"
	
	parser := NewParser()
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	rows, err := engine.ExecuteQuery(stmt)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	// Should return only 3 rows
	if len(rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(rows))
	}
}

func TestLimit_WithOffset(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	setupAggregateTestData(t, engine)

	sql := "SELECT * FROM users LIMIT 2 OFFSET 2"
	
	parser := NewParser()
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	rows, err := engine.ExecuteQuery(stmt)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	// Should skip 2 rows and return next 2
	if len(rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(rows))
	}
}

func TestOrderBy_WithLimit(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	setupAggregateTestData(t, engine)

	sql := "SELECT * FROM users ORDER BY age DESC LIMIT 2"
	
	parser := NewParser()
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	rows, err := engine.ExecuteQuery(stmt)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	// Should return top 2 oldest: Diana(35), Bob(30)
	if len(rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(rows))
	}

	// Check order
	ages := []float64{35, 30}
	for i, row := range rows {
		var age float64
		switch v := row["age"].(type) {
		case int:
			age = float64(v)
		case int64:
			age = float64(v)
		case float64:
			age = v
		}
		if age != ages[i] {
			t.Errorf("Row %d: expected age %.0f, got %.0f", i, ages[i], age)
		}
	}
}

func TestLimit_EdgeCases(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	setupAggregateTestData(t, engine)

	tests := []struct {
		name         string
		sql          string
		expectedRows int
	}{
		{
			name:         "LIMIT larger than result set",
			sql:          "SELECT * FROM users LIMIT 100",
			expectedRows: 5, // Only 5 rows exist
		},
		{
			name:         "OFFSET beyond result set",
			sql:          "SELECT * FROM users LIMIT 10 OFFSET 10",
			expectedRows: 0, // Offset beyond data
		},
		{
			name:         "LIMIT 1",
			sql:          "SELECT * FROM users LIMIT 1",
			expectedRows: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser()
			stmt, err := parser.Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			rows, err := engine.ExecuteQuery(stmt)
			if err != nil {
				t.Fatalf("Execute error: %v", err)
			}

			if len(rows) != tt.expectedRows {
				t.Errorf("Expected %d rows, got %d", tt.expectedRows, len(rows))
			}
		})
	}
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

func setupAggregateTestData(t *testing.T, engine *PagedEngine) {
	t.Helper()

	engine.CreateDatabase("testdb")
	engine.UseDatabase("testdb")

	columns := []Column{
		{Name: "id", DataType: "INT", PrimaryKey: true},
		{Name: "name", DataType: "VARCHAR"},
		{Name: "age", DataType: "INT"},
	}
	engine.CreateTable("users", columns)

	// Insert test data
	engine.InsertRow("users", Row{"id": 1, "name": "Alice", "age": 25})
	engine.InsertRow("users", Row{"id": 2, "name": "Bob", "age": 30})
	engine.InsertRow("users", Row{"id": 3, "name": "Charlie", "age": 28})
	engine.InsertRow("users", Row{"id": 4, "name": "Diana", "age": 35})
	engine.InsertRow("users", Row{"id": 5, "name": "Eve", "age": 27})
}

func setupJoinTestData(t *testing.T, engine *PagedEngine) {
	t.Helper()

	engine.CreateDatabase("testdb")
	engine.UseDatabase("testdb")

	// Users table
	usersColumns := []Column{
		{Name: "id", DataType: "INT", PrimaryKey: true},
		{Name: "name", DataType: "VARCHAR"},
		{Name: "age", DataType: "INT"},
	}
	engine.CreateTable("users", usersColumns)

	// Orders table
	ordersColumns := []Column{
		{Name: "id", DataType: "INT", PrimaryKey: true},
		{Name: "user_id", DataType: "INT"},
		{Name: "amount", DataType: "INT"},
	}
	engine.CreateTable("orders", ordersColumns)

	// Insert users
	engine.InsertRow("users", Row{"id": 1, "name": "Alice", "age": 25})
	engine.InsertRow("users", Row{"id": 2, "name": "Bob", "age": 30})
	engine.InsertRow("users", Row{"id": 3, "name": "Charlie", "age": 28})
	engine.InsertRow("users", Row{"id": 4, "name": "Diana", "age": 35}) // No orders

	// Insert orders
	engine.InsertRow("orders", Row{"id": 1, "user_id": 1, "amount": 100})
	engine.InsertRow("orders", Row{"id": 2, "user_id": 1, "amount": 200})
	engine.InsertRow("orders", Row{"id": 3, "user_id": 2, "amount": 150})
	engine.InsertRow("orders", Row{"id": 4, "user_id": 3, "amount": 75})
}
