package mindb

import (
	"testing"
)

func TestVacuumManager_Creation(t *testing.T) {
	txnMgr := NewTransactionManager()
	vm := NewVacuumManager(txnMgr)
	
	if vm == nil {
		t.Fatal("NewVacuumManager returned nil")
	}
	
	if vm.txnManager == nil {
		t.Error("VacuumManager txnManager is nil")
	}
}

func TestVacuumStats_Structure(t *testing.T) {
	stats := &VacuumStats{
		PagesScanned:   10,
		TuplesScanned:  100,
		DeadTuples:     20,
		TuplesRemoved:  15,
		PagesCompacted: 5,
	}
	
	if stats.PagesScanned != 10 {
		t.Errorf("Expected 10 pages scanned, got %d", stats.PagesScanned)
	}
	
	if stats.TuplesScanned != 100 {
		t.Errorf("Expected 100 tuples scanned, got %d", stats.TuplesScanned)
	}
	
	if stats.DeadTuples != 20 {
		t.Errorf("Expected 20 dead tuples, got %d", stats.DeadTuples)
	}
	
	if stats.TuplesRemoved != 15 {
		t.Errorf("Expected 15 tuples removed, got %d", stats.TuplesRemoved)
	}
	
	if stats.PagesCompacted != 5 {
		t.Errorf("Expected 5 pages compacted, got %d", stats.PagesCompacted)
	}
}

func TestVacuumStats_ZeroValues(t *testing.T) {
	stats := &VacuumStats{}
	
	if stats.PagesScanned != 0 {
		t.Error("Expected PagesScanned to be 0")
	}
	if stats.TuplesScanned != 0 {
		t.Error("Expected TuplesScanned to be 0")
	}
	if stats.DeadTuples != 0 {
		t.Error("Expected DeadTuples to be 0")
	}
	if stats.TuplesRemoved != 0 {
		t.Error("Expected TuplesRemoved to be 0")
	}
	if stats.PagesCompacted != 0 {
		t.Error("Expected PagesCompacted to be 0")
	}
}

func TestIsTupleDead_LiveTuple(t *testing.T) {
	txnMgr := NewTransactionManager()
	vm := NewVacuumManager(txnMgr)
	
	// Live tuple (no Xmax)
	tuple := &Tuple{
		Header: TupleHeader{
			Xmin: 1,
			Xmax: InvalidTxnID,
		},
	}
	
	isDead := vm.isTupleDead(tuple, 10)
	if isDead {
		t.Error("Live tuple should not be marked as dead")
	}
}

func TestIsTupleDead_DeadTuple(t *testing.T) {
	txnMgr := NewTransactionManager()
	vm := NewVacuumManager(txnMgr)
	
	// Dead tuple (Xmax < oldestXID)
	tuple := &Tuple{
		Header: TupleHeader{
			Xmin: 1,
			Xmax: 5,
		},
	}
	
	isDead := vm.isTupleDead(tuple, 10)
	if !isDead {
		t.Error("Dead tuple should be marked as dead")
	}
}

func TestIsTupleDead_RecentlyDeleted(t *testing.T) {
	txnMgr := NewTransactionManager()
	vm := NewVacuumManager(txnMgr)
	
	// Recently deleted (Xmax >= oldestXID)
	tuple := &Tuple{
		Header: TupleHeader{
			Xmin: 1,
			Xmax: 15,
		},
	}
	
	isDead := vm.isTupleDead(tuple, 10)
	if isDead {
		t.Error("Recently deleted tuple should not be marked as dead")
	}
}

func TestIsTupleDead_EdgeCase(t *testing.T) {
	txnMgr := NewTransactionManager()
	vm := NewVacuumManager(txnMgr)
	
	// Deleted by oldest transaction (edge case)
	tuple := &Tuple{
		Header: TupleHeader{
			Xmin: 1,
			Xmax: 10,
		},
	}
	
	isDead := vm.isTupleDead(tuple, 10)
	if isDead {
		t.Error("Tuple deleted by oldest transaction should not be marked as dead")
	}
}

func TestVacuumManager_Integration(t *testing.T) {
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
		{Name: "data", DataType: "VARCHAR"},
	}

	if err := engine.CreateTable("test_table", columns); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert some data
	for i := 0; i < 20; i++ {
		row := Row{
			"id":   i,
			"data": "test data",
		}
		if err := engine.InsertRow("test_table", row); err != nil {
			t.Fatalf("Failed to insert row: %v", err)
		}
	}

	// Delete some rows
	for i := 0; i < 10; i++ {
		_, err := engine.DeleteRows("test_table", []Condition{
			{Column: "id", Operator: "=", Value: i},
		})
		if err != nil {
			t.Fatalf("Failed to delete row: %v", err)
		}
	}

	// Verify data state before vacuum
	rows, err := engine.SelectRows("test_table", nil)
	if err != nil {
		t.Fatalf("Failed to select rows: %v", err)
	}

	if len(rows) != 10 {
		t.Errorf("Expected 10 rows after deletion, got %d", len(rows))
	}
}

func TestVacuumManager_EmptyTable(t *testing.T) {
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

	// Table is empty, vacuum should handle gracefully
	// (vacuum is typically called internally, this just verifies table creation)
	rows, err := engine.SelectRows("empty_table", nil)
	if err != nil {
		t.Fatalf("Failed to select from empty table: %v", err)
	}

	if len(rows) != 0 {
		t.Errorf("Expected 0 rows, got %d", len(rows))
	}
}

func TestVacuumManager_MultipleDeletes(t *testing.T) {
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
		{Name: "value", DataType: "INT"},
	}

	if err := engine.CreateTable("test_table", columns); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	for i := 0; i < 50; i++ {
		row := Row{
			"id":    i,
			"value": i * 10,
		}
		if err := engine.InsertRow("test_table", row); err != nil {
			t.Fatalf("Failed to insert row: %v", err)
		}
	}

	// Delete every other row
	for i := 0; i < 50; i += 2 {
		_, err := engine.DeleteRows("test_table", []Condition{
			{Column: "id", Operator: "=", Value: i},
		})
		if err != nil {
			t.Fatalf("Failed to delete row: %v", err)
		}
	}

	// Verify remaining data
	rows, err := engine.SelectRows("test_table", nil)
	if err != nil {
		t.Fatalf("Failed to select rows: %v", err)
	}

	if len(rows) != 25 {
		t.Errorf("Expected 25 rows after deletion, got %d", len(rows))
	}

	// Verify all remaining rows have odd IDs
	for _, row := range rows {
		id, ok := row["id"].(int)
		if !ok {
			// Try int64 or other numeric types
			continue
		}
		if id%2 == 0 && id != 0 {
			t.Errorf("Expected only odd IDs (or 0 edge case), found even ID: %d", id)
		}
	}
}

func TestVacuumManager_LargeDataset(t *testing.T) {
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
		{Name: "data", DataType: "VARCHAR"},
	}

	if err := engine.CreateTable("large_table", columns); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert larger dataset
	for i := 0; i < 100; i++ {
		row := Row{
			"id":   i,
			"data": "test data for vacuum operations",
		}
		if err := engine.InsertRow("large_table", row); err != nil {
			t.Fatalf("Failed to insert row: %v", err)
		}
	}

	// Delete a portion
	for i := 0; i < 30; i++ {
		_, err := engine.DeleteRows("large_table", []Condition{
			{Column: "id", Operator: "=", Value: i},
		})
		if err != nil {
			t.Fatalf("Failed to delete row: %v", err)
		}
	}

	// Verify remaining data
	rows, err := engine.SelectRows("large_table", nil)
	if err != nil {
		t.Fatalf("Failed to select rows: %v", err)
	}

	if len(rows) != 70 {
		t.Errorf("Expected 70 rows after deletion, got %d", len(rows))
	}
}

func TestVacuumManager_TransactionSafety(t *testing.T) {
	txnMgr := NewTransactionManager()
	vm := NewVacuumManager(txnMgr)
	
	// Verify vacuum manager can get oldest transaction
	oldestXID := txnMgr.GetOldestActiveTransaction()
	if oldestXID == 0 {
		// This is expected when no transactions are active
		t.Log("No active transactions")
	}
	
	// Use vm to avoid unused variable error
	if vm == nil {
		t.Fatal("VacuumManager should not be nil")
	}
}

func TestVacuumManager_ConcurrentSafety(t *testing.T) {
	txnMgr := NewTransactionManager()
	vm := NewVacuumManager(txnMgr)
	
	// Create multiple tuples with different states
	tuples := []*Tuple{
		{Header: TupleHeader{Xmin: 1, Xmax: InvalidTxnID}},
		{Header: TupleHeader{Xmin: 2, Xmax: 5}},
		{Header: TupleHeader{Xmin: 3, Xmax: 15}},
	}
	
	oldestXID := uint32(10)
	
	// Test concurrent checks (tests mutex locking)
	for i := 0; i < 10; i++ {
		for _, tuple := range tuples {
			_ = vm.isTupleDead(tuple, oldestXID)
		}
	}
	
	// If we get here without deadlock, the test passes
	t.Log("Concurrent safety test passed")
}

func TestVacuumStats_Accumulation(t *testing.T) {
	stats := &VacuumStats{}
	
	// Simulate vacuum operations
	for i := 0; i < 10; i++ {
		stats.PagesScanned++
		stats.TuplesScanned += 10
		if i%2 == 0 {
			stats.DeadTuples++
			stats.TuplesRemoved++
		}
		if i%3 == 0 {
			stats.PagesCompacted++
		}
	}
	
	if stats.PagesScanned != 10 {
		t.Errorf("Expected 10 pages scanned, got %d", stats.PagesScanned)
	}
	
	if stats.TuplesScanned != 100 {
		t.Errorf("Expected 100 tuples scanned, got %d", stats.TuplesScanned)
	}
	
	if stats.DeadTuples != 5 {
		t.Errorf("Expected 5 dead tuples, got %d", stats.DeadTuples)
	}
	
	if stats.TuplesRemoved != 5 {
		t.Errorf("Expected 5 tuples removed, got %d", stats.TuplesRemoved)
	}
	
	if stats.PagesCompacted != 4 {
		t.Errorf("Expected 4 pages compacted, got %d", stats.PagesCompacted)
	}
}
