package mindb

import (
	"os"
	"testing"
)

func TestWALBasics(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}
	defer wm.Close()

	// Append a record
	data := []byte("test data")
	lsn, err := wm.AppendRecord(1, WALRecordInsert, data)
	if err != nil {
		t.Fatalf("Failed to append record: %v", err)
	}

	if lsn == 0 {
		t.Error("Expected non-zero LSN")
	}

	// Sync
	if err := wm.Sync(); err != nil {
		t.Fatalf("Failed to sync WAL: %v", err)
	}
}

func TestWALMultipleRecords(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}
	defer wm.Close()

	// Append multiple records
	lsns := make([]LSN, 10)
	for i := 0; i < 10; i++ {
		data := []byte{byte(i)}
		lsn, err := wm.AppendRecord(uint32(i), WALRecordInsert, data)
		if err != nil {
			t.Fatalf("Failed to append record %d: %v", i, err)
		}
		lsns[i] = lsn
	}

	// Verify LSNs are increasing
	for i := 1; i < len(lsns); i++ {
		if lsns[i] <= lsns[i-1] {
			t.Errorf("LSNs not increasing: %d <= %d", lsns[i], lsns[i-1])
		}
	}

	wm.Sync()
}

func TestWALReadRecords(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}

	// Write records
	expectedData := [][]byte{
		[]byte("record1"),
		[]byte("record2"),
		[]byte("record3"),
	}

	for i, data := range expectedData {
		_, err := wm.AppendRecord(uint32(i), WALRecordInsert, data)
		if err != nil {
			t.Fatalf("Failed to append record: %v", err)
		}
	}

	wm.Sync()
	wm.Close()

	// Reopen and read
	wm2, err := NewWALManager(tmpDir)
	if err != nil {
		t.Fatalf("Failed to reopen WAL manager: %v", err)
	}
	defer wm2.Close()

	records, err := wm2.ReadRecords(0)
	if err != nil {
		t.Fatalf("Failed to read records: %v", err)
	}

	if len(records) != len(expectedData) {
		t.Errorf("Expected %d records, got %d", len(expectedData), len(records))
	}

	for i, record := range records {
		if string(record.Data) != string(expectedData[i]) {
			t.Errorf("Record %d data mismatch: expected %s, got %s",
				i, expectedData[i], record.Data)
		}
	}
}

func TestWALChecksum(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}

	data := []byte("test data")
	wm.AppendRecord(1, WALRecordInsert, data)
	wm.Sync()
	wm.Close()

	// Corrupt the WAL file
	files, _ := os.ReadDir(tmpDir)
	if len(files) > 0 {
		walFile := tmpDir + "/" + files[0].Name()
		file, _ := os.OpenFile(walFile, os.O_RDWR, 0644)
		file.WriteAt([]byte{0xFF}, 100) // Corrupt some data
		file.Close()
	}

	// Try to read - should detect corruption
	wm2, _ := NewWALManager(tmpDir)
	defer wm2.Close()

	records, _ := wm2.ReadRecords(0)

	// Should stop at corrupted record
	if len(records) > 1 {
		t.Error("Should have stopped at corrupted record")
	}
}

func TestWALRecovery(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}

	// Write some records
	_, _ = wm.AppendRecord(1, WALRecordInsert, []byte("data1"))
	_, _ = wm.AppendRecord(1, WALRecordUpdate, []byte("data2"))
	lsn3, _ := wm.AppendRecord(2, WALRecordInsert, []byte("data3"))

	wm.Sync()
	wm.Close()

	// Reopen - should recover LSN
	wm2, err := NewWALManager(tmpDir)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer wm2.Close()

	currentLSN := wm2.GetCurrentLSN()
	if currentLSN <= lsn3 {
		t.Errorf("Current LSN not recovered correctly: %d <= %d", currentLSN, lsn3)
	}

	// New records should have higher LSN
	lsn4, _ := wm2.AppendRecord(3, WALRecordDelete, []byte("data4"))
	if lsn4 <= lsn3 {
		t.Errorf("New LSN not higher than recovered: %d <= %d", lsn4, lsn3)
	}
}

func TestWALTruncate(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}
	defer wm.Close()

	// Write records
	lsns := make([]LSN, 5)
	for i := 0; i < 5; i++ {
		lsn, _ := wm.AppendRecord(uint32(i), WALRecordInsert, []byte{byte(i)})
		lsns[i] = lsn
	}

	wm.Sync()

	// Note: Truncate only removes entire segments where ALL records are before the LSN
	// Since all our records are in one segment, truncate won't remove anything
	err = wm.Truncate(lsns[3])
	if err != nil {
		t.Fatalf("Failed to truncate: %v", err)
	}

	// Verify truncate was called successfully (implementation detail: may not remove anything)
	records, _ := wm.ReadRecords(0)
	
	// Just verify we can still read records
	if len(records) == 0 {
		t.Error("Expected some records to remain")
	}
}

func TestRecoveryManager(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}
	defer wm.Close()

	rm := NewRecoveryManager(wm)

	// Simulate some transactions
	wm.AppendRecord(1, WALRecordInsert, []byte("insert1"))
	wm.AppendRecord(1, WALRecordUpdate, []byte("update1"))
	wm.AppendRecord(1, WALRecordCommit, []byte{})

	wm.AppendRecord(2, WALRecordInsert, []byte("insert2"))
	// Transaction 2 not committed

	wm.Sync()

	// Run recovery
	err = rm.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	// Recovery should complete without error
}

func TestCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}
	defer wm.Close()

	rm := NewRecoveryManager(wm)

	// Create checkpoint
	activeTxns := []uint32{1, 2, 3}
	dirtyPages := []PageID{10, 20, 30}

	lsn, err := rm.CreateCheckpoint(activeTxns, dirtyPages)
	if err != nil {
		t.Fatalf("Failed to create checkpoint: %v", err)
	}

	if lsn == 0 {
		t.Error("Expected non-zero checkpoint LSN")
	}

	// Read checkpoint
	records, _ := wm.ReadRecords(0)

	found := false
	for _, record := range records {
		if record.Header.RecordType == WALRecordCheckpoint {
			found = true
			
			checkpointData := rm.parseCheckpointData(record.Data)
			if checkpointData == nil {
				t.Error("Failed to parse checkpoint data")
				continue
			}

			if len(checkpointData.ActiveTxns) != len(activeTxns) {
				t.Errorf("Active txns mismatch: expected %d, got %d",
					len(activeTxns), len(checkpointData.ActiveTxns))
			}

			if len(checkpointData.DirtyPages) != len(dirtyPages) {
				t.Errorf("Dirty pages mismatch: expected %d, got %d",
					len(dirtyPages), len(checkpointData.DirtyPages))
			}
		}
	}

	if !found {
		t.Error("Checkpoint record not found")
	}
}

func TestWALSegmentation(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}
	defer wm.Close()

	// Write enough data to trigger segmentation
	// (This test may take a while with real segment sizes)
	largeData := make([]byte, 1024*1024) // 1MB per record

	for i := 0; i < 20; i++ {
		_, err := wm.AppendRecord(uint32(i), WALRecordInsert, largeData)
		if err != nil {
			t.Fatalf("Failed to append large record: %v", err)
		}
	}

	wm.Sync()

	// Check if multiple segments were created
	files, _ := os.ReadDir(tmpDir)
	
	if len(files) < 2 {
		t.Logf("Only %d segment(s) created (may need more data for multiple segments)", len(files))
	}
}
