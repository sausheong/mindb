package mindb

import (
	"testing"
)

// ============================================================================
// CONSTRAINT VALIDATOR TESTS
// ============================================================================

func TestNewConstraintValidator(t *testing.T) {
	cv := NewConstraintValidator()
	if cv == nil {
		t.Fatal("NewConstraintValidator returned nil")
	}
}

func TestConstraintValidator_ValidateInsert_NotNull(t *testing.T) {
	tmpDir := t.TempDir()
	
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()
	
	engine.CreateDatabase("testdb")
	engine.UseDatabase("testdb")
	
	// Create table with NOT NULL constraint
	columns := []Column{
		{Name: "id", DataType: "INT", NotNull: true},
		{Name: "name", DataType: "VARCHAR", NotNull: true},
		{Name: "email", DataType: "VARCHAR"},
	}
	
	engine.CreateTable("users", columns)
	
	cv := NewConstraintValidator()
	db, _ := engine.getCurrentDatabase()
	table := db.Tables["users"]
	
	// Test valid row
	validRow := Row{
		"id":   1,
		"name": "John",
	}
	
	err = cv.ValidateInsert(table, validRow)
	if err != nil {
		t.Errorf("Valid row should pass validation: %v", err)
	}
	
	// Test NULL in NOT NULL column
	invalidRow := Row{
		"id":   2,
		"name": nil, // NULL in NOT NULL column
	}
	
	err = cv.ValidateInsert(table, invalidRow)
	if err == nil {
		t.Error("Should fail validation for NULL in NOT NULL column")
	}
	
	// Test missing NOT NULL column
	missingRow := Row{
		"id": 3,
		// name is missing
	}
	
	err = cv.ValidateInsert(table, missingRow)
	if err == nil {
		t.Error("Should fail validation for missing NOT NULL column")
	}
}

func TestConstraintValidator_ValidateInsert_PrimaryKey(t *testing.T) {
	tmpDir := t.TempDir()
	
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()
	
	engine.CreateDatabase("testdb")
	engine.UseDatabase("testdb")
	
	// Create table with PRIMARY KEY
	columns := []Column{
		{Name: "id", DataType: "INT", PrimaryKey: true},
		{Name: "name", DataType: "VARCHAR"},
	}
	
	engine.CreateTable("users", columns)
	
	cv := NewConstraintValidator()
	db, _ := engine.getCurrentDatabase()
	table := db.Tables["users"]
	
	// Insert first row
	row1 := Row{"id": 1, "name": "Alice"}
	err = cv.ValidateInsert(table, row1)
	if err != nil {
		t.Errorf("First insert should succeed: %v", err)
	}
	engine.InsertRow("users", row1)
	
	// Try to insert duplicate primary key
	row2 := Row{"id": 1, "name": "Bob"}
	err = cv.ValidateInsert(table, row2)
	if err == nil {
		t.Error("Should fail validation for duplicate PRIMARY KEY")
	}
	
	// Insert with different primary key should succeed
	row3 := Row{"id": 2, "name": "Charlie"}
	err = cv.ValidateInsert(table, row3)
	if err != nil {
		t.Errorf("Insert with unique PRIMARY KEY should succeed: %v", err)
	}
}

func TestConstraintValidator_ValidateInsert_Unique(t *testing.T) {
	tmpDir := t.TempDir()
	
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()
	
	engine.CreateDatabase("testdb")
	engine.UseDatabase("testdb")
	
	// Create table with UNIQUE constraint
	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "email", DataType: "VARCHAR", Unique: true},
	}
	
	engine.CreateTable("users", columns)
	
	cv := NewConstraintValidator()
	db, _ := engine.getCurrentDatabase()
	table := db.Tables["users"]
	
	// Insert first row
	row1 := Row{"id": 1, "email": "alice@test.com"}
	err = cv.ValidateInsert(table, row1)
	if err != nil {
		t.Errorf("First insert should succeed: %v", err)
	}
	engine.InsertRow("users", row1)
	
	// Try to insert duplicate unique value
	row2 := Row{"id": 2, "email": "alice@test.com"}
	err = cv.ValidateInsert(table, row2)
	if err == nil {
		t.Error("Should fail validation for duplicate UNIQUE value")
	}
	
	// Insert with different unique value should succeed
	row3 := Row{"id": 3, "email": "bob@test.com"}
	err = cv.ValidateInsert(table, row3)
	if err != nil {
		t.Errorf("Insert with unique value should succeed: %v", err)
	}
}

func TestConstraintValidator_ValidateForeignKeys(t *testing.T) {
	tmpDir := t.TempDir()
	
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()
	
	engine.CreateDatabase("testdb")
	engine.UseDatabase("testdb")
	
	// Create parent table
	parentColumns := []Column{
		{Name: "id", DataType: "INT", PrimaryKey: true},
		{Name: "name", DataType: "VARCHAR"},
	}
	engine.CreateTable("users", parentColumns)
	
	// Insert some parent rows
	engine.InsertRow("users", Row{"id": 1, "name": "Alice"})
	engine.InsertRow("users", Row{"id": 2, "name": "Bob"})
	
	// Create child table with foreign key
	childColumns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "user_id", DataType: "INT", ForeignKey: &ForeignKeyDef{
			RefTable:  "users",
			RefColumn: "id",
		}},
		{Name: "amount", DataType: "INT"},
	}
	engine.CreateTable("orders", childColumns)
	
	cv := NewConstraintValidator()
	db, _ := engine.getCurrentDatabase()
	table := db.Tables["orders"]
	
	// Valid foreign key reference
	validRow := Row{"id": 1, "user_id": 1, "amount": 100}
	err = cv.ValidateForeignKeys(table, validRow, engine)
	if err != nil {
		t.Errorf("Valid foreign key should pass: %v", err)
	}
	
	// Invalid foreign key reference
	invalidRow := Row{"id": 2, "user_id": 999, "amount": 200}
	err = cv.ValidateForeignKeys(table, invalidRow, engine)
	if err == nil {
		t.Error("Should fail validation for invalid foreign key reference")
	}
	
	// NULL foreign key should be allowed
	nullRow := Row{"id": 3, "user_id": nil, "amount": 300}
	err = cv.ValidateForeignKeys(table, nullRow, engine)
	if err != nil {
		t.Errorf("NULL foreign key should be allowed: %v", err)
	}
}

func TestConstraintValidator_MultipleConstraints(t *testing.T) {
	tmpDir := t.TempDir()
	
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()
	
	engine.CreateDatabase("testdb")
	engine.UseDatabase("testdb")
	
	// Create table with multiple constraints
	columns := []Column{
		{Name: "id", DataType: "INT", PrimaryKey: true, NotNull: true},
		{Name: "email", DataType: "VARCHAR", Unique: true, NotNull: true},
		{Name: "username", DataType: "VARCHAR", NotNull: true},
	}
	
	engine.CreateTable("users", columns)
	
	cv := NewConstraintValidator()
	db, _ := engine.getCurrentDatabase()
	table := db.Tables["users"]
	
	// Valid row with all constraints satisfied
	validRow := Row{
		"id":       1,
		"email":    "alice@test.com",
		"username": "alice",
	}
	
	err = cv.ValidateInsert(table, validRow)
	if err != nil {
		t.Errorf("Valid row should pass all constraints: %v", err)
	}
	engine.InsertRow("users", validRow)
	
	// Violate PRIMARY KEY
	row2 := Row{
		"id":       1, // Duplicate
		"email":    "bob@test.com",
		"username": "bob",
	}
	err = cv.ValidateInsert(table, row2)
	if err == nil {
		t.Error("Should fail PRIMARY KEY constraint")
	}
	
	// Violate UNIQUE
	row3 := Row{
		"id":       2,
		"email":    "alice@test.com", // Duplicate
		"username": "bob",
	}
	err = cv.ValidateInsert(table, row3)
	if err == nil {
		t.Error("Should fail UNIQUE constraint")
	}
	
	// Violate NOT NULL
	row4 := Row{
		"id":       2,
		"email":    "bob@test.com",
		"username": nil, // NULL
	}
	err = cv.ValidateInsert(table, row4)
	if err == nil {
		t.Error("Should fail NOT NULL constraint")
	}
}

func TestConstraintValidator_EmptyRow(t *testing.T) {
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
		{Name: "name", DataType: "VARCHAR"},
	}
	
	engine.CreateTable("users", columns)
	
	cv := NewConstraintValidator()
	db, _ := engine.getCurrentDatabase()
	table := db.Tables["users"]
	
	// Empty row should pass if no NOT NULL constraints
	emptyRow := Row{}
	err = cv.ValidateInsert(table, emptyRow)
	if err != nil {
		t.Errorf("Empty row should pass with no constraints: %v", err)
	}
}

func TestConstraintValidator_DefaultValues(t *testing.T) {
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
		{Name: "status", DataType: "VARCHAR", Default: "active"},
		{Name: "count", DataType: "INT", Default: 0},
	}
	
	engine.CreateTable("items", columns)
	
	cv := NewConstraintValidator()
	db, _ := engine.getCurrentDatabase()
	table := db.Tables["items"]
	
	// Row without default columns should still validate
	row := Row{"id": 1}
	err = cv.ValidateInsert(table, row)
	if err != nil {
		t.Errorf("Row should validate even without default columns: %v", err)
	}
}

func TestConstraintValidator_CascadingForeignKeys(t *testing.T) {
	tmpDir := t.TempDir()
	
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()
	
	engine.CreateDatabase("testdb")
	engine.UseDatabase("testdb")
	
	// Create parent table
	engine.CreateTable("users", []Column{
		{Name: "id", DataType: "INT", PrimaryKey: true},
	})
	engine.InsertRow("users", Row{"id": 1})
	
	// Create child table with CASCADE
	engine.CreateTable("orders", []Column{
		{Name: "id", DataType: "INT"},
		{Name: "user_id", DataType: "INT", ForeignKey: &ForeignKeyDef{
			RefTable:  "users",
			RefColumn: "id",
			OnDelete:  "CASCADE",
		}},
	})
	
	cv := NewConstraintValidator()
	db, _ := engine.getCurrentDatabase()
	table := db.Tables["orders"]
	
	// Valid reference
	row := Row{"id": 1, "user_id": 1}
	err = cv.ValidateForeignKeys(table, row, engine)
	if err != nil {
		t.Errorf("Valid foreign key with CASCADE should pass: %v", err)
	}
}

func TestConstraintValidator_SelfReferencingForeignKey(t *testing.T) {
	tmpDir := t.TempDir()
	
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()
	
	engine.CreateDatabase("testdb")
	engine.UseDatabase("testdb")
	
	// Create table with self-referencing foreign key
	engine.CreateTable("employees", []Column{
		{Name: "id", DataType: "INT", PrimaryKey: true},
		{Name: "manager_id", DataType: "INT", ForeignKey: &ForeignKeyDef{
			RefTable:  "employees",
			RefColumn: "id",
		}},
	})
	
	// Insert root employee (no manager)
	engine.InsertRow("employees", Row{"id": 1, "manager_id": nil})
	
	cv := NewConstraintValidator()
	db, _ := engine.getCurrentDatabase()
	table := db.Tables["employees"]
	
	// Valid self-reference
	row := Row{"id": 2, "manager_id": 1}
	err = cv.ValidateForeignKeys(table, row, engine)
	if err != nil {
		t.Errorf("Valid self-referencing foreign key should pass: %v", err)
	}
	
	// Invalid self-reference
	invalidRow := Row{"id": 3, "manager_id": 999}
	err = cv.ValidateForeignKeys(table, invalidRow, engine)
	if err == nil {
		t.Error("Invalid self-reference should fail")
	}
}

func TestConstraintValidator_CompositePrimaryKey(t *testing.T) {
	tmpDir := t.TempDir()
	
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()
	
	engine.CreateDatabase("testdb")
	engine.UseDatabase("testdb")
	
	// Create table with composite primary key
	columns := []Column{
		{Name: "user_id", DataType: "INT", PrimaryKey: true},
		{Name: "product_id", DataType: "INT", PrimaryKey: true},
		{Name: "quantity", DataType: "INT"},
	}
	
	engine.CreateTable("cart_items", columns)
	
	cv := NewConstraintValidator()
	db, _ := engine.getCurrentDatabase()
	table := db.Tables["cart_items"]
	
	// Insert first row
	row1 := Row{"user_id": 1, "product_id": 1, "quantity": 5}
	err = cv.ValidateInsert(table, row1)
	if err != nil {
		t.Errorf("First insert should succeed: %v", err)
	}
	engine.InsertRow("cart_items", row1)
	
	// Same user, different product - should succeed
	row2 := Row{"user_id": 1, "product_id": 2, "quantity": 3}
	err = cv.ValidateInsert(table, row2)
	if err != nil {
		t.Errorf("Different composite key should succeed: %v", err)
	}
	
	// Duplicate composite key - should fail
	row3 := Row{"user_id": 1, "product_id": 1, "quantity": 10}
	err = cv.ValidateInsert(table, row3)
	if err == nil {
		t.Error("Duplicate composite primary key should fail")
	}
}

func TestConstraintValidator_NullableUniqueColumn(t *testing.T) {
	tmpDir := t.TempDir()
	
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()
	
	engine.CreateDatabase("testdb")
	engine.UseDatabase("testdb")
	
	// Create table with nullable unique column
	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "email", DataType: "VARCHAR", Unique: true}, // Nullable but unique
	}
	
	engine.CreateTable("users", columns)
	
	cv := NewConstraintValidator()
	db, _ := engine.getCurrentDatabase()
	table := db.Tables["users"]
	
	// Multiple NULL values should be allowed in unique column
	row1 := Row{"id": 1, "email": nil}
	err = cv.ValidateInsert(table, row1)
	if err != nil {
		t.Errorf("NULL in unique column should be allowed: %v", err)
	}
	
	row2 := Row{"id": 2, "email": nil}
	err = cv.ValidateInsert(table, row2)
	if err != nil {
		t.Errorf("Multiple NULLs in unique column should be allowed: %v", err)
	}
}

func TestConstraintValidator_LargeDataset(t *testing.T) {
	tmpDir := t.TempDir()
	
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()
	
	engine.CreateDatabase("testdb")
	engine.UseDatabase("testdb")
	
	columns := []Column{
		{Name: "id", DataType: "INT", PrimaryKey: true},
		{Name: "value", DataType: "INT"},
	}
	
	engine.CreateTable("numbers", columns)
	
	cv := NewConstraintValidator()
	db, _ := engine.getCurrentDatabase()
	table := db.Tables["numbers"]
	
	// Insert many rows
	for i := 0; i < 100; i++ {
		row := Row{"id": i, "value": i * 10}
		err = cv.ValidateInsert(table, row)
		if err != nil {
			t.Errorf("Insert %d should succeed: %v", i, err)
		}
		engine.InsertRow("numbers", row)
	}
	
	// Try to insert duplicate
	dupRow := Row{"id": 50, "value": 999}
	err = cv.ValidateInsert(table, dupRow)
	if err == nil {
		t.Error("Duplicate primary key should fail even with large dataset")
	}
}

func TestConstraintValidator_MissingReferencedTable(t *testing.T) {
	tmpDir := t.TempDir()
	
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()
	
	engine.CreateDatabase("testdb")
	engine.UseDatabase("testdb")
	
	// Create table with foreign key to non-existent table
	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "user_id", DataType: "INT", ForeignKey: &ForeignKeyDef{
			RefTable:  "nonexistent",
			RefColumn: "id",
		}},
	}
	
	engine.CreateTable("orders", columns)
	
	cv := NewConstraintValidator()
	db, _ := engine.getCurrentDatabase()
	table := db.Tables["orders"]
	
	// Should fail because referenced table doesn't exist
	row := Row{"id": 1, "user_id": 1}
	err = cv.ValidateForeignKeys(table, row, engine)
	if err == nil {
		t.Error("Should fail when referenced table doesn't exist")
	}
}

func TestConstraintValidator_ConcurrentValidation(t *testing.T) {
	tmpDir := t.TempDir()
	
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()
	
	engine.CreateDatabase("testdb")
	engine.UseDatabase("testdb")
	
	columns := []Column{
		{Name: "id", DataType: "INT", PrimaryKey: true},
		{Name: "value", DataType: "VARCHAR"},
	}
	
	engine.CreateTable("test", columns)
	
	cv := NewConstraintValidator()
	db, _ := engine.getCurrentDatabase()
	table := db.Tables["test"]
	
	// Concurrent validation
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			row := Row{"id": id, "value": "test"}
			cv.ValidateInsert(table, row)
			done <- true
		}(i)
	}
	
	// Wait for all
	for i := 0; i < 10; i++ {
		<-done
	}
}
