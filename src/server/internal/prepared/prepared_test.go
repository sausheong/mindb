package prepared

import (
	"testing"
	"time"

	"github.com/sausheong/mindb/src/core"
)

func TestNewManager(t *testing.T) {
	mgr := NewManager(100)
	if mgr == nil {
		t.Fatal("NewManager returned nil")
	}
	if mgr.statements == nil {
		t.Error("statements map is nil")
	}
	if mgr.parser == nil {
		t.Error("parser is nil")
	}
	if mgr.maxStmts != 100 {
		t.Errorf("Expected maxStmts 100, got %d", mgr.maxStmts)
	}
}

func TestPrepare_Success(t *testing.T) {
	mgr := NewManager(10)
	
	sql := "SELECT * FROM users WHERE id = ?"
	ps, err := mgr.Prepare(sql)
	
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	if ps == nil {
		t.Fatal("PreparedStatement is nil")
	}
	if ps.ID == "" {
		t.Error("PreparedStatement ID is empty")
	}
	if ps.SQL != sql {
		t.Errorf("Expected SQL %s, got %s", sql, ps.SQL)
	}
	if ps.Statement == nil {
		t.Error("Statement is nil")
	}
	if ps.UseCount != 0 {
		t.Errorf("Expected UseCount 0, got %d", ps.UseCount)
	}
}

func TestPrepare_InvalidSQL(t *testing.T) {
	mgr := NewManager(10)
	
	_, err := mgr.Prepare("INVALID SQL STATEMENT")
	if err == nil {
		t.Error("Expected error for invalid SQL")
	}
}

func TestGet_Success(t *testing.T) {
	mgr := NewManager(10)
	
	// Prepare a statement
	ps1, err := mgr.Prepare("SELECT * FROM users")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	
	// Get it back
	ps2, err := mgr.Get(ps1.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	
	if ps2.ID != ps1.ID {
		t.Errorf("Expected ID %s, got %s", ps1.ID, ps2.ID)
	}
	if ps2.SQL != ps1.SQL {
		t.Errorf("Expected SQL %s, got %s", ps1.SQL, ps2.SQL)
	}
}

func TestGet_NotFound(t *testing.T) {
	mgr := NewManager(10)
	
	_, err := mgr.Get("nonexistent-id")
	if err == nil {
		t.Error("Expected error for nonexistent ID")
	}
}

func TestExecute_Success(t *testing.T) {
	mgr := NewManager(10)
	
	// Prepare a SELECT statement (simpler for testing)
	ps, err := mgr.Prepare("SELECT * FROM users WHERE id = ?")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	
	initialUseCount := ps.UseCount
	initialLastUsed := ps.LastUsed
	
	// Wait a bit to ensure time difference
	time.Sleep(10 * time.Millisecond)
	
	// Execute with parameters
	params := []interface{}{1}
	stmt, err := mgr.Execute(ps.ID, params)
	
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if stmt == nil {
		t.Fatal("Returned statement is nil")
	}
	
	// Check usage stats were updated
	ps2, _ := mgr.Get(ps.ID)
	if ps2.UseCount != initialUseCount+1 {
		t.Errorf("Expected UseCount %d, got %d", initialUseCount+1, ps2.UseCount)
	}
	if !ps2.LastUsed.After(initialLastUsed) {
		t.Error("LastUsed was not updated")
	}
}

func TestExecute_NotFound(t *testing.T) {
	mgr := NewManager(10)
	
	_, err := mgr.Execute("nonexistent-id", nil)
	if err == nil {
		t.Error("Expected error for nonexistent ID")
	}
}

func TestClose_Success(t *testing.T) {
	mgr := NewManager(10)
	
	// Prepare a statement
	ps, err := mgr.Prepare("SELECT * FROM users")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	
	// Close it
	err = mgr.Close(ps.ID)
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	
	// Verify it's gone
	_, err = mgr.Get(ps.ID)
	if err == nil {
		t.Error("Expected error after closing statement")
	}
}

func TestClose_NotFound(t *testing.T) {
	mgr := NewManager(10)
	
	err := mgr.Close("nonexistent-id")
	if err == nil {
		t.Error("Expected error for nonexistent ID")
	}
}

func TestCloseAll(t *testing.T) {
	mgr := NewManager(10)
	
	// Prepare multiple statements
	mgr.Prepare("SELECT * FROM users")
	mgr.Prepare("SELECT * FROM orders")
	mgr.Prepare("SELECT * FROM products")
	
	stats := mgr.Stats()
	if stats["total_statements"].(int) != 3 {
		t.Errorf("Expected 3 statements, got %d", stats["total_statements"])
	}
	
	// Close all
	mgr.CloseAll()
	
	stats = mgr.Stats()
	if stats["total_statements"].(int) != 0 {
		t.Errorf("Expected 0 statements after CloseAll, got %d", stats["total_statements"])
	}
}

func TestStats(t *testing.T) {
	mgr := NewManager(100)
	
	// Prepare some statements
	mgr.Prepare("SELECT * FROM users")
	mgr.Prepare("SELECT * FROM orders")
	
	stats := mgr.Stats()
	
	if stats["total_statements"].(int) != 2 {
		t.Errorf("Expected total_statements 2, got %d", stats["total_statements"])
	}
	if stats["max_statements"].(int) != 100 {
		t.Errorf("Expected max_statements 100, got %d", stats["max_statements"])
	}
}

func TestEvictLRU(t *testing.T) {
	mgr := NewManager(3) // Small limit to trigger eviction
	
	// Prepare statements
	ps1, _ := mgr.Prepare("SELECT * FROM users")
	time.Sleep(10 * time.Millisecond)
	ps2, _ := mgr.Prepare("SELECT * FROM orders")
	time.Sleep(10 * time.Millisecond)
	ps3, _ := mgr.Prepare("SELECT * FROM products")
	
	// All three should exist
	stats := mgr.Stats()
	if stats["total_statements"].(int) != 3 {
		t.Errorf("Expected 3 statements, got %d", stats["total_statements"])
	}
	
	// Prepare one more - should evict ps1 (oldest)
	time.Sleep(10 * time.Millisecond)
	_, err := mgr.Prepare("SELECT * FROM categories")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	
	// Should still have 3 statements
	stats = mgr.Stats()
	if stats["total_statements"].(int) != 3 {
		t.Errorf("Expected 3 statements after eviction, got %d", stats["total_statements"])
	}
	
	// ps1 should be gone
	_, err = mgr.Get(ps1.ID)
	if err == nil {
		t.Error("Expected ps1 to be evicted")
	}
	
	// ps2 and ps3 should still exist
	_, err = mgr.Get(ps2.ID)
	if err != nil {
		t.Error("Expected ps2 to still exist")
	}
	_, err = mgr.Get(ps3.ID)
	if err != nil {
		t.Error("Expected ps3 to still exist")
	}
}

func TestCloneStatement(t *testing.T) {
	mgr := NewManager(10)
	
	// Create a statement with various fields
	original := &mindb.Statement{
		Type:  mindb.Select,
		Table: "users",
		Columns: []mindb.Column{
			{Name: "id", DataType: "INT"},
			{Name: "name", DataType: "VARCHAR"},
		},
		Conditions: []mindb.Condition{
			{Column: "id", Operator: "=", Value: 1},
		},
	}
	
	// Clone it
	clone := mgr.cloneStatement(original)
	
	// Verify it's a different object
	if clone == original {
		t.Error("Clone is the same object as original")
	}
	
	// Verify fields are copied
	if clone.Type != original.Type {
		t.Error("Type not copied")
	}
	if clone.Table != original.Table {
		t.Error("Table not copied")
	}
	if len(clone.Columns) != len(original.Columns) {
		t.Error("Columns not copied")
	}
	if len(clone.Conditions) != len(original.Conditions) {
		t.Error("Conditions not copied")
	}
	
	// Modify clone - should not affect original
	clone.Table = "modified"
	if original.Table == "modified" {
		t.Error("Modifying clone affected original")
	}
}

func TestBindParameters_Insert(t *testing.T) {
	mgr := NewManager(10)
	
	stmt := &mindb.Statement{
		Type:   mindb.Insert,
		Table:  "users",
		Values: [][]interface{}{{nil, nil}}, // Placeholders
	}
	
	params := []interface{}{1, "Alice"}
	err := mgr.bindParameters(stmt, params)
	
	if err != nil {
		t.Fatalf("bindParameters failed: %v", err)
	}
	
	if stmt.Values[0][0] != 1 {
		t.Errorf("Expected first param 1, got %v", stmt.Values[0][0])
	}
	if stmt.Values[0][1] != "Alice" {
		t.Errorf("Expected second param 'Alice', got %v", stmt.Values[0][1])
	}
}

func TestBindParameters_WrongCount(t *testing.T) {
	mgr := NewManager(10)
	
	stmt := &mindb.Statement{
		Type:   mindb.Insert,
		Table:  "users",
		Values: [][]interface{}{{nil, nil}},
	}
	
	params := []interface{}{1} // Wrong count
	err := mgr.bindParameters(stmt, params)
	
	if err == nil {
		t.Error("Expected error for parameter count mismatch")
	}
}

func TestBindParameters_WhereClause(t *testing.T) {
	mgr := NewManager(10)
	
	stmt := &mindb.Statement{
		Type:  mindb.Select,
		Table: "users",
		Conditions: []mindb.Condition{
			{Column: "id", Operator: "=", Value: "?"},
			{Column: "status", Operator: "=", Value: "?"},
		},
	}
	
	params := []interface{}{42, "active"}
	err := mgr.bindParameters(stmt, params)
	
	if err != nil {
		t.Fatalf("bindParameters failed: %v", err)
	}
	
	if stmt.Conditions[0].Value != 42 {
		t.Errorf("Expected first condition value 42, got %v", stmt.Conditions[0].Value)
	}
	if stmt.Conditions[1].Value != "active" {
		t.Errorf("Expected second condition value 'active', got %v", stmt.Conditions[1].Value)
	}
}

func TestBindParameters_NoParams(t *testing.T) {
	mgr := NewManager(10)
	
	stmt := &mindb.Statement{
		Type:  mindb.Select,
		Table: "users",
	}
	
	err := mgr.bindParameters(stmt, nil)
	if err != nil {
		t.Errorf("bindParameters with no params should not error: %v", err)
	}
}

func TestGenerateID(t *testing.T) {
	id1 := generateID()
	id2 := generateID()
	
	if id1 == "" {
		t.Error("Generated ID is empty")
	}
	if id2 == "" {
		t.Error("Generated ID is empty")
	}
	if id1 == id2 {
		t.Error("Generated IDs should be unique")
	}
	if len(id1) != 32 { // 16 bytes = 32 hex chars
		t.Errorf("Expected ID length 32, got %d", len(id1))
	}
}

func TestConcurrentAccess(t *testing.T) {
	mgr := NewManager(100)
	
	// Prepare a statement
	ps, err := mgr.Prepare("SELECT * FROM users WHERE id = ?")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	
	// Execute concurrently
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			_, err := mgr.Execute(ps.ID, []interface{}{id})
			if err != nil {
				t.Errorf("Concurrent execute failed: %v", err)
			}
			done <- true
		}(i)
	}
	
	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
	
	// Check use count
	ps2, _ := mgr.Get(ps.ID)
	if ps2.UseCount != 10 {
		t.Errorf("Expected UseCount 10, got %d", ps2.UseCount)
	}
}
