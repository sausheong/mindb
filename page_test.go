package main

import (
	"os"
	"testing"
)

func TestPageBasics(t *testing.T) {
	page := NewPage(1)
	
	if page.Header.PageID != 1 {
		t.Errorf("Expected PageID 1, got %d", page.Header.PageID)
	}
	
	if page.FreeSpace() < PageSize-PageHeaderSize-100 {
		t.Errorf("Expected large free space, got %d", page.FreeSpace())
	}
}

func TestPageInsertTuple(t *testing.T) {
	page := NewPage(1)
	
	// Insert a tuple
	tupleData := []byte("Hello, World!")
	slotNum, err := page.InsertTuple(tupleData)
	if err != nil {
		t.Fatalf("Failed to insert tuple: %v", err)
	}
	
	if slotNum != 0 {
		t.Errorf("Expected slot 0, got %d", slotNum)
	}
	
	// Retrieve the tuple
	retrieved, err := page.GetTuple(slotNum)
	if err != nil {
		t.Fatalf("Failed to get tuple: %v", err)
	}
	
	if string(retrieved) != string(tupleData) {
		t.Errorf("Expected %s, got %s", tupleData, retrieved)
	}
}

func TestPageMultipleTuples(t *testing.T) {
	page := NewPage(1)
	
	// Insert multiple tuples
	tuples := []string{"tuple1", "tuple2", "tuple3"}
	slots := make([]uint16, len(tuples))
	
	for i, data := range tuples {
		slot, err := page.InsertTuple([]byte(data))
		if err != nil {
			t.Fatalf("Failed to insert tuple %d: %v", i, err)
		}
		slots[i] = slot
	}
	
	// Verify all tuples
	for i, slot := range slots {
		retrieved, err := page.GetTuple(slot)
		if err != nil {
			t.Fatalf("Failed to get tuple %d: %v", i, err)
		}
		
		if string(retrieved) != tuples[i] {
			t.Errorf("Expected %s, got %s", tuples[i], retrieved)
		}
	}
}

func TestPageDeleteTuple(t *testing.T) {
	page := NewPage(1)
	
	// Insert and delete
	tupleData := []byte("test data")
	slotNum, _ := page.InsertTuple(tupleData)
	
	err := page.DeleteTuple(slotNum)
	if err != nil {
		t.Fatalf("Failed to delete tuple: %v", err)
	}
	
	// Try to retrieve deleted tuple
	_, err = page.GetTuple(slotNum)
	if err == nil {
		t.Error("Expected error when getting deleted tuple")
	}
}

func TestPageUpdateTuple(t *testing.T) {
	page := NewPage(1)
	
	// Insert tuple
	originalData := []byte("original")
	slotNum, _ := page.InsertTuple(originalData)
	
	// Update with smaller data
	newData := []byte("new")
	err := page.UpdateTuple(slotNum, newData)
	if err != nil {
		t.Fatalf("Failed to update tuple: %v", err)
	}
	
	// Verify update
	retrieved, _ := page.GetTuple(slotNum)
	if string(retrieved) != string(newData) {
		t.Errorf("Expected %s, got %s", newData, retrieved)
	}
}

func TestPageChecksum(t *testing.T) {
	page := NewPage(1)
	
	// Insert some data
	page.InsertTuple([]byte("test data"))
	
	// Update checksum
	page.UpdateChecksum()
	
	// Verify checksum
	if !page.VerifyChecksum() {
		t.Error("Checksum verification failed")
	}
	
	// Corrupt data
	page.Data[100] = 0xFF
	
	// Checksum should fail
	if page.VerifyChecksum() {
		t.Error("Checksum should have failed after corruption")
	}
}

func TestPageSerialization(t *testing.T) {
	page := NewPage(1)
	
	// Insert data
	page.InsertTuple([]byte("test1"))
	page.InsertTuple([]byte("test2"))
	page.UpdateChecksum()
	
	// Load from bytes
	loaded, err := LoadPageFromBytes(page.Data)
	if err != nil {
		t.Fatalf("Failed to load page: %v", err)
	}
	
	// Verify loaded page
	if loaded.Header.PageID != page.Header.PageID {
		t.Errorf("PageID mismatch")
	}
	
	if loaded.Header.SlotCount != page.Header.SlotCount {
		t.Errorf("SlotCount mismatch")
	}
	
	// Verify tuples
	tuple1, _ := loaded.GetTuple(0)
	if string(tuple1) != "test1" {
		t.Errorf("Tuple 0 mismatch")
	}
}

func TestHeapFileBasics(t *testing.T) {
	// Create temp directory
	tmpDir := t.TempDir()
	
	// Create heap file
	hf, err := NewHeapFile(tmpDir, "test_table")
	if err != nil {
		t.Fatalf("Failed to create heap file: %v", err)
	}
	defer hf.Close()
	
	// Insert tuple
	tupleData := []byte("test tuple")
	tid, err := hf.InsertTuple(tupleData)
	if err != nil {
		t.Fatalf("Failed to insert tuple: %v", err)
	}
	
	// Retrieve tuple
	retrieved, err := hf.GetTuple(tid)
	if err != nil {
		t.Fatalf("Failed to get tuple: %v", err)
	}
	
	if string(retrieved) != string(tupleData) {
		t.Errorf("Expected %s, got %s", tupleData, retrieved)
	}
}

func TestHeapFileMultipleTuples(t *testing.T) {
	tmpDir := t.TempDir()
	
	hf, err := NewHeapFile(tmpDir, "test_table")
	if err != nil {
		t.Fatalf("Failed to create heap file: %v", err)
	}
	defer hf.Close()
	
	// Insert multiple tuples
	tuples := []string{"tuple1", "tuple2", "tuple3", "tuple4", "tuple5"}
	tids := make([]TupleID, len(tuples))
	
	for i, data := range tuples {
		tid, err := hf.InsertTuple([]byte(data))
		if err != nil {
			t.Fatalf("Failed to insert tuple %d: %v", i, err)
		}
		tids[i] = tid
	}
	
	// Verify all tuples
	for i, tid := range tids {
		retrieved, err := hf.GetTuple(tid)
		if err != nil {
			t.Fatalf("Failed to get tuple %d: %v", i, err)
		}
		
		if string(retrieved) != tuples[i] {
			t.Errorf("Expected %s, got %s", tuples[i], retrieved)
		}
	}
}

func TestHeapFileScan(t *testing.T) {
	tmpDir := t.TempDir()
	
	hf, err := NewHeapFile(tmpDir, "test_table")
	if err != nil {
		t.Fatalf("Failed to create heap file: %v", err)
	}
	defer hf.Close()
	
	// Insert tuples
	expected := []string{"a", "b", "c"}
	for _, data := range expected {
		hf.InsertTuple([]byte(data))
	}
	
	// Scan and collect
	var results []string
	err = hf.Scan(func(tid TupleID, data []byte) error {
		results = append(results, string(data))
		return nil
	})
	
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	
	if len(results) != len(expected) {
		t.Errorf("Expected %d tuples, got %d", len(expected), len(results))
	}
}

func TestHeapFilePersistence(t *testing.T) {
	tmpDir := t.TempDir()
	tableName := "persist_test"
	
	// Create and populate heap file
	hf1, err := NewHeapFile(tmpDir, tableName)
	if err != nil {
		t.Fatalf("Failed to create heap file: %v", err)
	}
	
	tupleData := []byte("persistent data")
	tid, err := hf1.InsertTuple(tupleData)
	if err != nil {
		t.Fatalf("Failed to insert tuple: %v", err)
	}
	
	hf1.Close()
	
	// Reopen heap file
	hf2, err := NewHeapFile(tmpDir, tableName)
	if err != nil {
		t.Fatalf("Failed to reopen heap file: %v", err)
	}
	defer hf2.Close()
	
	// Verify data persisted
	retrieved, err := hf2.GetTuple(tid)
	if err != nil {
		t.Fatalf("Failed to get tuple after reopen: %v", err)
	}
	
	if string(retrieved) != string(tupleData) {
		t.Errorf("Data not persisted correctly")
	}
}

func TestTupleSerialization(t *testing.T) {
	// Create test row
	row := Row{
		"id":   1,
		"name": "Alice",
		"age":  30,
	}
	
	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "name", DataType: "VARCHAR"},
		{Name: "age", DataType: "INT"},
	}
	
	// Serialize
	data, err := SerializeTuple(row, columns)
	if err != nil {
		t.Fatalf("Failed to serialize tuple: %v", err)
	}
	
	// Deserialize
	tuple, err := DeserializeTuple(data)
	if err != nil {
		t.Fatalf("Failed to deserialize tuple: %v", err)
	}
	
	// Verify
	if tuple.Data["id"] != float64(1) { // JSON unmarshals numbers as float64
		t.Errorf("ID mismatch")
	}
	
	if tuple.Data["name"] != "Alice" {
		t.Errorf("Name mismatch")
	}
}

func TestTupleWithNulls(t *testing.T) {
	row := Row{
		"id":   1,
		"name": nil,
		"age":  30,
	}
	
	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "name", DataType: "VARCHAR"},
		{Name: "age", DataType: "INT"},
	}
	
	data, err := SerializeTuple(row, columns)
	if err != nil {
		t.Fatalf("Failed to serialize: %v", err)
	}
	
	tuple, err := DeserializeTuple(data)
	if err != nil {
		t.Fatalf("Failed to deserialize: %v", err)
	}
	
	// Check null bitmap
	if !tuple.IsNull(1) { // name is at index 1
		t.Error("Expected name to be null")
	}
	
	if tuple.IsNull(0) || tuple.IsNull(2) {
		t.Error("id and age should not be null")
	}
}

func TestMain(m *testing.M) {
	// Run tests
	code := m.Run()
	
	// Cleanup
	os.Exit(code)
}
