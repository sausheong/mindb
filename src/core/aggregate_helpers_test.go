package mindb

import (
	"testing"
)

/*
Package: mindb
Component: Aggregate Helper Functions
Layer: Engine Adapter (Layer 2)

Test Coverage:
- Type conversion functions (toFloat64, toNumeric)
- Value comparison (CompareValues)
- Empty value handling (getEmptyValue)
- Edge cases and NULL handling

Run: go test -v -run TestAggregateHelpers
*/

// ============================================================================
// TYPE CONVERSION TESTS
// ============================================================================

func TestToFloat64(t *testing.T) {
	tests := []struct {
		name      string
		input     interface{}
		expected  float64
		shouldErr bool
	}{
		{
			name:      "int to float64",
			input:     42,
			expected:  42.0,
			shouldErr: false,
		},
		{
			name:      "int64 to float64",
			input:     int64(100),
			expected:  100.0,
			shouldErr: false,
		},
		{
			name:      "float64 passthrough",
			input:     3.14,
			expected:  3.14,
			shouldErr: false,
		},
		// NOTE: String conversion not implemented yet
		// {
		// 	name:      "string number to float64",
		// 	input:     "123.45",
		// 	expected:  123.45,
		// 	shouldErr: false,
		// },
		{
			name:      "invalid string",
			input:     "not a number",
			expected:  0.0,
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := toFloat64(tt.input)
			if (err != nil) != tt.shouldErr {
				t.Errorf("Expected error=%v, got error=%v", tt.shouldErr, err != nil)
			}
			if !tt.shouldErr && result != tt.expected {
				t.Errorf("Expected %.2f, got %.2f", tt.expected, result)
			}
		})
	}
}

func TestToNumeric(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected float64
		shouldOk bool
	}{
		{
			name:     "int to numeric",
			input:    42,
			expected: 42.0,
			shouldOk: true,
		},
		{
			name:     "int64 to numeric",
			input:    int64(100),
			expected: 100.0,
			shouldOk: true,
		},
		{
			name:     "float64 to numeric",
			input:    3.14,
			expected: 3.14,
			shouldOk: true,
		},
		// NOTE: String conversion not implemented yet
		// {
		// 	name:     "string number to numeric",
		// 	input:    "123.45",
		// 	expected: 123.45,
		// 	shouldOk: true,
		// },
		{
			name:     "invalid string",
			input:    "not a number",
			expected: 0.0,
			shouldOk: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := toNumeric(tt.input)
			if ok != tt.shouldOk {
				t.Errorf("Expected ok=%v, got %v", tt.shouldOk, ok)
			}
			if ok && result != tt.expected {
				t.Errorf("Expected %.2f, got %.2f", tt.expected, result)
			}
		})
	}
}

// ============================================================================
// VALUE COMPARISON TESTS
// ============================================================================

func TestCompareValues(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected int
	}{
		{
			name:     "equal integers",
			a:        5,
			b:        5,
			expected: 0,
		},
		{
			name:     "a less than b (int)",
			a:        3,
			b:        7,
			expected: -1,
		},
		{
			name:     "a greater than b (int)",
			a:        10,
			b:        5,
			expected: 1,
		},
		{
			name:     "equal floats",
			a:        3.14,
			b:        3.14,
			expected: 0,
		},
		{
			name:     "a less than b (float)",
			a:        2.5,
			b:        3.5,
			expected: -1,
		},
		{
			name:     "a greater than b (float)",
			a:        5.5,
			b:        2.5,
			expected: 1,
		},
		{
			name:     "equal strings",
			a:        "apple",
			b:        "apple",
			expected: 0,
		},
		{
			name:     "a less than b (string)",
			a:        "apple",
			b:        "banana",
			expected: -1,
		},
		{
			name:     "a greater than b (string)",
			a:        "zebra",
			b:        "apple",
			expected: 1,
		},
		{
			name:     "mixed types - int and float",
			a:        5,
			b:        5.0,
			expected: 0,
		},
		{
			name:     "nil values",
			a:        nil,
			b:        nil,
			expected: 0,
		},
		{
			name:     "nil vs value",
			a:        nil,
			b:        5,
			expected: -1,
		},
		{
			name:     "value vs nil",
			a:        5,
			b:        nil,
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CompareValues(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}

// ============================================================================
// AGGREGATE EDGE CASES
// ============================================================================

func TestAggregateExecutor_NullHandling(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Setup test data with NULL values
	engine.CreateDatabase("testdb")
	engine.UseDatabase("testdb")

	columns := []Column{
		{Name: "id", DataType: "INT", PrimaryKey: true},
		{Name: "value", DataType: "INT"},
	}
	engine.CreateTable("test_nulls", columns)

	// Insert data with some NULL values
	engine.InsertRow("test_nulls", Row{"id": 1, "value": 10})
	engine.InsertRow("test_nulls", Row{"id": 2, "value": nil})
	engine.InsertRow("test_nulls", Row{"id": 3, "value": 20})
	engine.InsertRow("test_nulls", Row{"id": 4, "value": nil})
	engine.InsertRow("test_nulls", Row{"id": 5, "value": 30})

	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "COUNT with NULLs",
			sql:  "SELECT COUNT(value) FROM test_nulls",
		},
		{
			name: "SUM with NULLs",
			sql:  "SELECT SUM(value) FROM test_nulls",
		},
		{
			name: "AVG with NULLs",
			sql:  "SELECT AVG(value) FROM test_nulls",
		},
		{
			name: "MIN with NULLs",
			sql:  "SELECT MIN(value) FROM test_nulls",
		},
		{
			name: "MAX with NULLs",
			sql:  "SELECT MAX(value) FROM test_nulls",
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

			// Should return 1 row with aggregate result
			if len(rows) != 1 {
				t.Errorf("Expected 1 row, got %d", len(rows))
			}
		})
	}
}

func TestAggregateExecutor_EmptyTable(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Setup empty table
	engine.CreateDatabase("testdb")
	engine.UseDatabase("testdb")

	columns := []Column{
		{Name: "id", DataType: "INT", PrimaryKey: true},
		{Name: "value", DataType: "INT"},
	}
	engine.CreateTable("empty_table", columns)

	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "COUNT on empty table",
			sql:  "SELECT COUNT(*) FROM empty_table",
		},
		{
			name: "SUM on empty table",
			sql:  "SELECT SUM(value) FROM empty_table",
		},
		{
			name: "AVG on empty table",
			sql:  "SELECT AVG(value) FROM empty_table",
		},
		{
			name: "MIN on empty table",
			sql:  "SELECT MIN(value) FROM empty_table",
		},
		{
			name: "MAX on empty table",
			sql:  "SELECT MAX(value) FROM empty_table",
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

			// Should return 1 row with empty/zero value
			if len(rows) != 1 {
				t.Errorf("Expected 1 row, got %d", len(rows))
			}
		})
	}
}

func TestAggregateExecutor_MixedTypes(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Setup test data with mixed numeric types
	engine.CreateDatabase("testdb")
	engine.UseDatabase("testdb")

	columns := []Column{
		{Name: "id", DataType: "INT", PrimaryKey: true},
		{Name: "int_val", DataType: "INT"},
		{Name: "float_val", DataType: "FLOAT"},
	}
	engine.CreateTable("mixed_types", columns)

	// Insert data with different numeric types
	engine.InsertRow("mixed_types", Row{"id": 1, "int_val": 10, "float_val": 10.5})
	engine.InsertRow("mixed_types", Row{"id": 2, "int_val": 20, "float_val": 20.5})
	engine.InsertRow("mixed_types", Row{"id": 3, "int_val": 30, "float_val": 30.5})

	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "SUM on int column",
			sql:  "SELECT SUM(int_val) FROM mixed_types",
		},
		{
			name: "SUM on float column",
			sql:  "SELECT SUM(float_val) FROM mixed_types",
		},
		{
			name: "AVG on int column",
			sql:  "SELECT AVG(int_val) FROM mixed_types",
		},
		{
			name: "AVG on float column",
			sql:  "SELECT AVG(float_val) FROM mixed_types",
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
				t.Errorf("Expected 1 row, got %d", len(rows))
			}
		})
	}
}
