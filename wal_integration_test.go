package main

import (
	"testing"
)

func TestPagedEngineWithWAL(t *testing.T) {
	tmpDir := t.TempDir()

	// Create engine with WAL enabled
	engine, err := NewPagedEngineWithWAL(tmpDir, true)
	if err != nil {
		t.Fatalf("Failed to create paged engine with WAL: %v", err)
	}
	defer engine.Close()

	// Create database and table
	if err := engine.CreateDatabase("testdb"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "name", DataType: "VARCHAR"},
	}

	if err := engine.CreateTable("users", columns); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert rows (should write to WAL)
	rows := []Row{
		{"id": 1, "name": "Alice"},
		{"id": 2, "name": "Bob"},
		{"id": 3, "name": "Charlie"},
	}

	for _, row := range rows {
		if err := engine.InsertRow("users", row); err != nil {
			t.Fatalf("Failed to insert row: %v", err)
		}
	}

	// Verify data
	results, err := engine.SelectRows("users", nil)
	if err != nil {
		t.Fatalf("Failed to select rows: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(results))
	}

	// Verify WAL was written
	if engine.walManager != nil {
		records, err := engine.walManager.ReadRecords(0)
		if err != nil {
			t.Fatalf("Failed to read WAL records: %v", err)
		}

		if len(records) == 0 {
			t.Error("Expected WAL records to be written")
		}

		// Count insert records
		insertCount := 0
		for _, record := range records {
			if record.Header.RecordType == WALRecordInsert {
				insertCount++
			}
		}

		if insertCount != 3 {
			t.Errorf("Expected 3 insert WAL records, got %d", insertCount)
		}
	}
}

func TestWALRecoveryIntegration(t *testing.T) {
	tmpDir := t.TempDir()

	// Create engine with WAL
	engine1, err := NewPagedEngineWithWAL(tmpDir, true)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	engine1.CreateDatabase("testdb")

	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "value", DataType: "VARCHAR"},
	}

	engine1.CreateTable("data", columns)

	// Insert data
	for i := 0; i < 5; i++ {
		row := Row{
			"id":    i,
			"value": "test",
		}
		engine1.InsertRow("data", row)
	}

	engine1.Close()

	// Reopen with recovery
	engine2, err := NewPagedEngineWithWAL(tmpDir, true)
	if err != nil {
		t.Fatalf("Failed to reopen engine: %v", err)
	}
	defer engine2.Close()

	// Recovery should have run
	// Database and table should be auto-loaded from catalog
	if err := engine2.UseDatabase("testdb"); err != nil {
		t.Fatalf("Failed to use database: %v", err)
	}

	rows, err := engine2.SelectRows("data", nil)
	if err != nil {
		t.Fatalf("Failed to select after recovery: %v", err)
	}

	if len(rows) != 5 {
		t.Errorf("Expected 5 rows after recovery, got %d", len(rows))
	}
}

func TestCheckpointCreation(t *testing.T) {
	tmpDir := t.TempDir()

	engine, err := NewPagedEngineWithWAL(tmpDir, true)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Create checkpoint
	if engine.recoveryMgr != nil {
		activeTxns := []uint32{1, 2}
		dirtyPages := []PageID{10, 20}

		lsn, err := engine.recoveryMgr.CreateCheckpoint(activeTxns, dirtyPages)
		if err != nil {
			t.Fatalf("Failed to create checkpoint: %v", err)
		}

		if lsn == 0 {
			t.Error("Expected non-zero checkpoint LSN")
		}

		// Verify checkpoint was written to WAL
		records, _ := engine.walManager.ReadRecords(0)

		found := false
		for _, record := range records {
			if record.Header.RecordType == WALRecordCheckpoint {
				found = true
				break
			}
		}

		if !found {
			t.Error("Checkpoint record not found in WAL")
		}
	}
}

func TestWALDisabled(t *testing.T) {
	tmpDir := t.TempDir()

	// Create engine without WAL
	engine, err := NewPagedEngineWithWAL(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// WAL should be nil
	if engine.walManager != nil {
		t.Error("Expected WAL manager to be nil when disabled")
	}

	// Operations should still work
	engine.CreateDatabase("testdb")

	columns := []Column{
		{Name: "id", DataType: "INT"},
	}

	engine.CreateTable("test", columns)

	if err := engine.InsertRow("test", Row{"id": 1}); err != nil {
		t.Fatalf("Insert failed with WAL disabled: %v", err)
	}

	rows, err := engine.SelectRows("test", nil)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}

	if len(rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(rows))
	}
}

func TestWALDurability(t *testing.T) {
	tmpDir := t.TempDir()

	engine, err := NewPagedEngineWithWAL(tmpDir, true)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	engine.CreateDatabase("testdb")

	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "data", DataType: "VARCHAR"},
	}

	engine.CreateTable("durable", columns)

	// Insert data
	testData := []Row{
		{"id": 1, "data": "first"},
		{"id": 2, "data": "second"},
	}

	for _, row := range testData {
		if err := engine.InsertRow("durable", row); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// WAL should be synced (fsync called)
	// Verify WAL file exists and has content
	if engine.walManager != nil {
		records, err := engine.walManager.ReadRecords(0)
		if err != nil {
			t.Fatalf("Failed to read WAL: %v", err)
		}

		if len(records) < 2 {
			t.Errorf("Expected at least 2 WAL records, got %d", len(records))
		}

		// All records should have valid checksums
		for i, record := range records {
			if !engine.walManager.verifyChecksum(record) {
				t.Errorf("Record %d has invalid checksum", i)
			}
		}
	}

	engine.Close()
}

func TestMultipleWALTransactions(t *testing.T) {
	tmpDir := t.TempDir()

	engine, err := NewPagedEngineWithWAL(tmpDir, true)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	engine.CreateDatabase("testdb")

	columns := []Column{
		{Name: "id", DataType: "INT"},
	}

	engine.CreateTable("test", columns)

	// Insert multiple rows (each gets a new transaction ID)
	for i := 0; i < 10; i++ {
		if err := engine.InsertRow("test", Row{"id": i}); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Verify transaction IDs are increasing
	if engine.walManager != nil {
		records, _ := engine.walManager.ReadRecords(0)

		var prevTxnID uint32 = 0
		for _, record := range records {
			if record.Header.RecordType == WALRecordInsert {
				if record.Header.TxnID <= prevTxnID && prevTxnID != 0 {
					t.Errorf("Transaction IDs not increasing: %d <= %d",
						record.Header.TxnID, prevTxnID)
				}
				prevTxnID = record.Header.TxnID
			}
		}
	}
}
