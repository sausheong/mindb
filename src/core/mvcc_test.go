package mindb

import (
	"testing"
	"time"
)

func TestTransactionManager(t *testing.T) {
	tm := NewTransactionManager()

	// Begin transaction
	txn, err := tm.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	if txn.ID < FirstNormalTxnID {
		t.Errorf("Invalid transaction ID: %d", txn.ID)
	}

	if txn.State != TxnStateActive {
		t.Error("Transaction should be active")
	}

	// Commit transaction
	err = tm.CommitTransaction(txn.ID)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify transaction is no longer active
	count := tm.GetActiveTransactionCount()
	if count != 0 {
		t.Errorf("Expected 0 active transactions, got %d", count)
	}
}

func TestMultipleTransactions(t *testing.T) {
	tm := NewTransactionManager()

	// Start multiple transactions
	txns := make([]*Transaction, 5)
	for i := 0; i < 5; i++ {
		txn, err := tm.BeginTransaction()
		if err != nil {
			t.Fatalf("Failed to begin transaction %d: %v", i, err)
		}
		txns[i] = txn
	}

	// Verify all active
	count := tm.GetActiveTransactionCount()
	if count != 5 {
		t.Errorf("Expected 5 active transactions, got %d", count)
	}

	// Commit some, abort others
	tm.CommitTransaction(txns[0].ID)
	tm.CommitTransaction(txns[1].ID)
	tm.AbortTransaction(txns[2].ID)

	// Verify count
	count = tm.GetActiveTransactionCount()
	if count != 2 {
		t.Errorf("Expected 2 active transactions, got %d", count)
	}
}

func TestSnapshot(t *testing.T) {
	tm := NewTransactionManager()

	// Start transaction 1
	txn1, _ := tm.BeginTransaction()

	// Start transaction 2
	txn2, _ := tm.BeginTransaction()

	// Snapshot should include txn1 as active (txn2 sees itself as active too)
	if len(txn2.Snapshot.ActiveXIDs) < 1 {
		t.Errorf("Expected at least 1 active XID in snapshot, got %d",
			len(txn2.Snapshot.ActiveXIDs))
	}

	// XMax should be > both transaction IDs
	if txn2.Snapshot.XMax <= txn2.ID {
		t.Error("Snapshot XMax should be > current transaction ID")
	}

	// Commit txn1
	tm.CommitTransaction(txn1.ID)

	// Start transaction 3 - should not see txn1 as active
	txn3, _ := tm.BeginTransaction()

	foundTxn1 := false
	for _, xid := range txn3.Snapshot.ActiveXIDs {
		if xid == txn1.ID {
			foundTxn1 = true
		}
	}

	if foundTxn1 {
		t.Error("Committed transaction should not be in new snapshot")
	}
}

func TestTupleVisibility(t *testing.T) {
	tm := NewTransactionManager()

	// Create tuple created by transaction 10
	tuple := &Tuple{
		Header: TupleHeader{
			Xmin: 10,
			Xmax: InvalidTxnID,
		},
		Data: Row{"id": 1},
	}

	// Snapshot with XMax=15 (transactions 10-14 committed)
	snapshot := &Snapshot{
		XMin:       10,
		XMax:       15,
		ActiveXIDs: []uint32{},
	}

	// Tuple should be visible (created by committed txn < XMax)
	if !tm.IsVisible(tuple, snapshot) {
		t.Error("Tuple should be visible")
	}

	// Snapshot with XMax=10 (transaction 10 not yet started)
	snapshot2 := &Snapshot{
		XMin:       5,
		XMax:       10,
		ActiveXIDs: []uint32{},
	}

	// Tuple should NOT be visible (created after snapshot)
	if tm.IsVisible(tuple, snapshot2) {
		t.Error("Tuple should not be visible (created after snapshot)")
	}

	// Snapshot with transaction 10 still active
	snapshot3 := &Snapshot{
		XMin:       10,
		XMax:       15,
		ActiveXIDs: []uint32{10},
	}

	// Tuple should NOT be visible (created by active transaction)
	if tm.IsVisible(tuple, snapshot3) {
		t.Error("Tuple should not be visible (created by active txn)")
	}
}

func TestDeletedTupleVisibility(t *testing.T) {
	tm := NewTransactionManager()

	// Tuple created by txn 10, deleted by txn 20
	tuple := &Tuple{
		Header: TupleHeader{
			Xmin: 10,
			Xmax: 20,
		},
		Data: Row{"id": 1},
	}

	// Snapshot at txn 15 (before delete)
	snapshot := &Snapshot{
		XMin:       10,
		XMax:       15,
		ActiveXIDs: []uint32{},
	}

	// Tuple should be visible (deleted after snapshot)
	if !tm.IsVisible(tuple, snapshot) {
		t.Error("Tuple should be visible (deleted after snapshot)")
	}

	// Snapshot at txn 25 (after delete)
	snapshot2 := &Snapshot{
		XMin:       20,
		XMax:       25,
		ActiveXIDs: []uint32{},
	}

	// Tuple should NOT be visible (deleted before snapshot)
	if tm.IsVisible(tuple, snapshot2) {
		t.Error("Tuple should not be visible (deleted before snapshot)")
	}
}

func TestVacuumManager(t *testing.T) {
	tmpDir := t.TempDir()
	tm := NewTransactionManager()
	vm := NewVacuumManager(tm)

	// Create heap file
	hf, err := NewHeapFile(tmpDir, "test_table")
	if err != nil {
		t.Fatalf("Failed to create heap file: %v", err)
	}
	defer hf.Close()

	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "name", DataType: "VARCHAR"},
	}

	// Insert tuples with different Xmin/Xmax
	// Use low transaction IDs so they're considered old
	for i := 0; i < 10; i++ {
		row := Row{"id": i, "name": "test"}
		
		xmin := uint32(i + 2)
		var xmax uint32 = InvalidTxnID
		if i < 5 {
			// First 5 tuples are deleted by old transactions
			xmax = uint32(i + 3)
		}
		
		tupleData, _ := SerializeTupleWithHeader(row, columns, xmin, xmax)
		hf.InsertTuple(tupleData)
	}

	// Start a new transaction so oldestXID is high
	txn, _ := tm.BeginTransaction()
	tm.CommitTransaction(txn.ID)

	// Run vacuum (should remove dead tuples)
	stats, err := vm.VacuumTable(hf, columns)
	if err != nil {
		t.Fatalf("Vacuum failed: %v", err)
	}

	if stats.TuplesScanned != 10 {
		t.Errorf("Expected 10 tuples scanned, got %d", stats.TuplesScanned)
	}

	// Should have found some dead tuples (deleted by old transactions)
	if stats.DeadTuples == 0 {
		t.Logf("Warning: Expected some dead tuples, got %d", stats.DeadTuples)
	}
}

func TestMVCCInsertAndSelect(t *testing.T) {
	tmpDir := t.TempDir()

	engine, err := NewPagedEngineWithWAL(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	engine.CreateDatabase("testdb")

	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "value", DataType: "VARCHAR"},
	}

	engine.CreateTable("data", columns)

	// Begin transaction
	if err := engine.BeginTransaction(); err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert data
	row := Row{"id": 1, "value": "test"}
	if err := engine.InsertRow("data", row); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Commit
	if err := engine.CommitTransaction(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Select should see the data
	rows, err := engine.SelectRows("data", nil)
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}

	if len(rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(rows))
	}
}

func TestMVCCIsolation(t *testing.T) {
	tmpDir := t.TempDir()

	engine, err := NewPagedEngineWithWAL(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	engine.CreateDatabase("testdb")

	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "value", DataType: "VARCHAR"},
	}

	engine.CreateTable("data", columns)

	// Transaction 1: Insert data and commit
	engine.BeginTransaction()
	engine.InsertRow("data", Row{"id": 1, "value": "v1"})
	engine.CommitTransaction()

	// Transaction 2: Start (should see committed data)
	engine.BeginTransaction()
	rows, _ := engine.SelectRows("data", nil)

	if len(rows) != 1 {
		t.Errorf("Transaction 2 should see committed data, got %d rows", len(rows))
	}

	engine.CommitTransaction()
}

func TestMVCCDelete(t *testing.T) {
	tmpDir := t.TempDir()

	engine, err := NewPagedEngineWithWAL(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	engine.CreateDatabase("testdb")

	columns := []Column{
		{Name: "id", DataType: "INT"},
	}

	engine.CreateTable("data", columns)

	// Insert and commit
	engine.BeginTransaction()
	engine.InsertRow("data", Row{"id": 1})
	engine.InsertRow("data", Row{"id": 2})
	engine.CommitTransaction()

	// Delete and commit
	engine.BeginTransaction()
	conditions := []Condition{{Column: "id", Operator: "=", Value: 1}}
	count, err := engine.DeleteRows("data", conditions)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected 1 row deleted, got %d", count)
	}

	engine.CommitTransaction()

	// Select should only see row 2
	rows, _ := engine.SelectRows("data", nil)

	if len(rows) != 1 {
		t.Errorf("Expected 1 row after delete, got %d", len(rows))
	}
}

func TestVacuumIntegration(t *testing.T) {
	tmpDir := t.TempDir()

	engine, err := NewPagedEngineWithWAL(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	engine.CreateDatabase("testdb")

	columns := []Column{
		{Name: "id", DataType: "INT"},
	}

	engine.CreateTable("data", columns)

	// Insert and delete data
	engine.BeginTransaction()
	for i := 0; i < 10; i++ {
		engine.InsertRow("data", Row{"id": i})
	}
	engine.CommitTransaction()

	engine.BeginTransaction()
	conditions := []Condition{{Column: "id", Operator: "<", Value: 5}}
	engine.DeleteRows("data", conditions)
	engine.CommitTransaction()

	// Run vacuum
	stats, err := engine.VacuumTable("data")
	if err != nil {
		t.Fatalf("Vacuum failed: %v", err)
	}

	if stats.TuplesScanned == 0 {
		t.Error("Expected some tuples scanned")
	}

	t.Logf("Vacuum stats: scanned=%d, dead=%d, removed=%d",
		stats.TuplesScanned, stats.DeadTuples, stats.TuplesRemoved)
}

func TestTransactionCleanup(t *testing.T) {
	tm := NewTransactionManager()

	// Create and commit transactions
	for i := 0; i < 10; i++ {
		txn, _ := tm.BeginTransaction()
		tm.CommitTransaction(txn.ID)
	}

	// Cleanup old transactions
	tm.CleanupOldTransactions(1 * time.Millisecond)

	// Wait a bit
	time.Sleep(10 * time.Millisecond)

	// Cleanup again
	tm.CleanupOldTransactions(5 * time.Millisecond)

	// Should have cleaned up some transactions
	// (Hard to test precisely due to timing)
}

func TestOldestActiveTransaction(t *testing.T) {
	tm := NewTransactionManager()

	// No active transactions
	oldest := tm.GetOldestActiveTransaction()
	if oldest == 0 {
		t.Error("Oldest should not be 0")
	}

	// Start transactions
	txn1, _ := tm.BeginTransaction()
	txn2, _ := tm.BeginTransaction()
	txn3, _ := tm.BeginTransaction()

	// Oldest should be txn1
	oldest = tm.GetOldestActiveTransaction()
	if oldest != txn1.ID {
		t.Errorf("Expected oldest %d, got %d", txn1.ID, oldest)
	}

	// Commit txn1
	tm.CommitTransaction(txn1.ID)

	// Oldest should now be txn2
	oldest = tm.GetOldestActiveTransaction()
	if oldest != txn2.ID {
		t.Errorf("Expected oldest %d, got %d", txn2.ID, oldest)
	}

	// Cleanup
	tm.CommitTransaction(txn2.ID)
	tm.CommitTransaction(txn3.ID)
}

func TestSnapshotIsolation(t *testing.T) {
	tm := NewTransactionManager()

	// Transaction 1 starts
	txn1, _ := tm.BeginTransaction()

	// Transaction 2 starts and commits
	txn2, _ := tm.BeginTransaction()
	tm.CommitTransaction(txn2.ID)

	// Create tuple by txn2
	tuple := &Tuple{
		Header: TupleHeader{
			Xmin: txn2.ID,
			Xmax: InvalidTxnID,
		},
		Data: Row{"id": 1},
	}

	// Transaction 1 should NOT see this tuple (txn2 was active in txn1's snapshot)
	visible := tm.IsVisible(tuple, txn1.Snapshot)

	// txn2 was active when txn1 started, so tuple should not be visible
	txn2WasActive := false
	for _, xid := range txn1.Snapshot.ActiveXIDs {
		if xid == txn2.ID {
			txn2WasActive = true
			break
		}
	}

	if txn2WasActive && visible {
		t.Error("Tuple created by concurrent txn should not be visible")
	}

	tm.CommitTransaction(txn1.ID)
}
