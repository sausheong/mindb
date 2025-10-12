package mindb

import (
	"bytes"
	"testing"
)

func TestGetRow(t *testing.T) {
	row := GetRow()
	if row == nil {
		t.Fatal("GetRow returned nil")
	}
	
	// Should be empty
	if len(row) != 0 {
		t.Errorf("Expected empty row, got %d items", len(row))
	}
}

func TestPutRow(t *testing.T) {
	row := GetRow()
	row["id"] = 1
	row["name"] = "test"
	row["value"] = 42
	
	if len(row) != 3 {
		t.Errorf("Expected 3 items, got %d", len(row))
	}
	
	PutRow(row)
	
	// Row should be cleared
	if len(row) != 0 {
		t.Errorf("Expected row to be cleared, got %d items", len(row))
	}
}

func TestRowPoolReuse(t *testing.T) {
	// Get a row, populate it, and return it
	row1 := GetRow()
	row1["test"] = "value"
	PutRow(row1)
	
	// Get another row - should be reused
	row2 := GetRow()
	if len(row2) != 0 {
		t.Error("Reused row should be empty")
	}
	
	// Use it
	row2["new"] = "data"
	if row2["new"] != "data" {
		t.Error("Row should store data correctly")
	}
	
	PutRow(row2)
}

func TestGetBuffer(t *testing.T) {
	buf := GetBuffer()
	if buf == nil {
		t.Fatal("GetBuffer returned nil")
	}
	
	// Should be empty
	if buf.Len() != 0 {
		t.Errorf("Expected empty buffer, got %d bytes", buf.Len())
	}
}

func TestPutBuffer(t *testing.T) {
	buf := GetBuffer()
	buf.WriteString("test data")
	
	if buf.Len() != 9 {
		t.Errorf("Expected 9 bytes, got %d", buf.Len())
	}
	
	PutBuffer(buf)
	
	// Buffer should be reset
	if buf.Len() != 0 {
		t.Errorf("Expected buffer to be reset, got %d bytes", buf.Len())
	}
}

func TestBufferPoolReuse(t *testing.T) {
	// Get a buffer, use it, and return it
	buf1 := GetBuffer()
	buf1.WriteString("first use")
	PutBuffer(buf1)
	
	// Get another buffer - should be reused
	buf2 := GetBuffer()
	if buf2.Len() != 0 {
		t.Error("Reused buffer should be empty")
	}
	
	// Use it
	buf2.WriteString("second use")
	if buf2.String() != "second use" {
		t.Errorf("Expected 'second use', got '%s'", buf2.String())
	}
	
	PutBuffer(buf2)
}

func TestGetTupleIDSlice(t *testing.T) {
	slice := GetTupleIDSlice()
	if slice == nil {
		t.Fatal("GetTupleIDSlice returned nil")
	}
	
	// Should be empty
	if len(slice) != 0 {
		t.Errorf("Expected empty slice, got %d items", len(slice))
	}
	
	// Should have capacity
	if cap(slice) < 100 {
		t.Errorf("Expected capacity >= 100, got %d", cap(slice))
	}
}

func TestPutTupleIDSlice(t *testing.T) {
	slice := GetTupleIDSlice()
	slice = append(slice, TupleID{PageID: 1, SlotNum: 1})
	slice = append(slice, TupleID{PageID: 2, SlotNum: 2})
	slice = append(slice, TupleID{PageID: 3, SlotNum: 3})
	
	if len(slice) != 3 {
		t.Errorf("Expected 3 items, got %d", len(slice))
	}
	
	PutTupleIDSlice(slice)
	
	// Note: PutTupleIDSlice clears the slice internally before returning to pool,
	// but the caller's slice reference is not modified (Go passes slices by value)
	// The next GetTupleIDSlice() will return an empty slice from the pool
	if len(slice) != 3 {
		t.Errorf("Caller's slice reference unchanged, got %d items", len(slice))
	}
}

func TestTupleIDSlicePoolReuse(t *testing.T) {
	// Get a slice, use it, and return it
	slice1 := GetTupleIDSlice()
	slice1 = append(slice1, TupleID{PageID: 1, SlotNum: 1})
	PutTupleIDSlice(slice1)
	
	// Get another slice - should be reused
	slice2 := GetTupleIDSlice()
	if len(slice2) != 0 {
		t.Error("Reused slice should be empty")
	}
	
	// Use it
	slice2 = append(slice2, TupleID{PageID: 5, SlotNum: 5})
	if len(slice2) != 1 {
		t.Errorf("Expected 1 item, got %d", len(slice2))
	}
	
	PutTupleIDSlice(slice2)
}

func TestPoolsConcurrent(t *testing.T) {
	// Test concurrent access to pools
	done := make(chan bool)
	
	for i := 0; i < 10; i++ {
		go func() {
			// Test row pool
			row := GetRow()
			row["test"] = i
			PutRow(row)
			
			// Test buffer pool
			buf := GetBuffer()
			buf.WriteString("test")
			PutBuffer(buf)
			
			// Test tuple ID slice pool
			slice := GetTupleIDSlice()
			slice = append(slice, TupleID{PageID: PageID(i), SlotNum: uint16(i)})
			PutTupleIDSlice(slice)
			
			done <- true
		}()
	}
	
	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestRowPoolMultipleOperations(t *testing.T) {
	// Test multiple get/put cycles
	for i := 0; i < 100; i++ {
		row := GetRow()
		row["iteration"] = i
		row["data"] = "test"
		
		if len(row) != 2 {
			t.Errorf("Iteration %d: expected 2 items, got %d", i, len(row))
		}
		
		PutRow(row)
	}
}

func TestBufferPoolMultipleOperations(t *testing.T) {
	// Test multiple get/put cycles
	for i := 0; i < 100; i++ {
		buf := GetBuffer()
		buf.WriteString("iteration ")
		buf.WriteString(string(rune('0' + i%10)))
		
		if buf.Len() == 0 {
			t.Errorf("Iteration %d: buffer should not be empty", i)
		}
		
		PutBuffer(buf)
	}
}

func TestTupleIDSlicePoolMultipleOperations(t *testing.T) {
	// Test multiple get/put cycles
	for i := 0; i < 100; i++ {
		slice := GetTupleIDSlice()
		
		for j := 0; j < 10; j++ {
			slice = append(slice, TupleID{PageID: PageID(j), SlotNum: uint16(j)})
		}
		
		if len(slice) != 10 {
			t.Errorf("Iteration %d: expected 10 items, got %d", i, len(slice))
		}
		
		PutTupleIDSlice(slice)
	}
}

func BenchmarkRowPool(b *testing.B) {
	for i := 0; i < b.N; i++ {
		row := GetRow()
		row["id"] = i
		row["name"] = "test"
		PutRow(row)
	}
}

func BenchmarkRowWithoutPool(b *testing.B) {
	for i := 0; i < b.N; i++ {
		row := make(Row)
		row["id"] = i
		row["name"] = "test"
	}
}

func BenchmarkBufferPool(b *testing.B) {
	for i := 0; i < b.N; i++ {
		buf := GetBuffer()
		buf.WriteString("test data")
		PutBuffer(buf)
	}
}

func BenchmarkBufferWithoutPool(b *testing.B) {
	for i := 0; i < b.N; i++ {
		buf := new(bytes.Buffer)
		buf.WriteString("test data")
	}
}

func BenchmarkTupleIDSlicePool(b *testing.B) {
	for i := 0; i < b.N; i++ {
		slice := GetTupleIDSlice()
		slice = append(slice, TupleID{PageID: 1, SlotNum: 1})
		PutTupleIDSlice(slice)
	}
}

func BenchmarkTupleIDSliceWithoutPool(b *testing.B) {
	for i := 0; i < b.N; i++ {
		slice := make([]TupleID, 0, 100)
		slice = append(slice, TupleID{PageID: 1, SlotNum: 1})
	}
}
