package mindb

import (
	"testing"
)

func TestSerializeTuple(t *testing.T) {
	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "name", DataType: "VARCHAR"},
		{Name: "age", DataType: "INT"},
	}
	
	row := Row{
		"id":   1,
		"name": "John",
		"age":  30,
	}
	
	data, err := SerializeTuple(row, columns)
	if err != nil {
		t.Fatalf("Failed to serialize tuple: %v", err)
	}
	
	if len(data) < TupleHeaderSize {
		t.Errorf("Serialized data too short: %d bytes", len(data))
	}
}

func TestSerializeTupleWithHeader(t *testing.T) {
	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "value", DataType: "VARCHAR"},
	}
	
	row := Row{
		"id":    42,
		"value": "test",
	}
	
	xmin := uint32(100)
	xmax := uint32(200)
	
	data, err := SerializeTupleWithHeader(row, columns, xmin, xmax)
	if err != nil {
		t.Fatalf("Failed to serialize tuple with header: %v", err)
	}
	
	// Deserialize and verify
	tuple, err := DeserializeTuple(data)
	if err != nil {
		t.Fatalf("Failed to deserialize tuple: %v", err)
	}
	
	if tuple.Header.Xmin != xmin {
		t.Errorf("Expected Xmin %d, got %d", xmin, tuple.Header.Xmin)
	}
	
	if tuple.Header.Xmax != xmax {
		t.Errorf("Expected Xmax %d, got %d", xmax, tuple.Header.Xmax)
	}
}

func TestSerializeDeserializeTuple(t *testing.T) {
	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "name", DataType: "VARCHAR"},
		{Name: "active", DataType: "BOOL"},
	}
	
	row := Row{
		"id":     123,
		"name":   "Alice",
		"active": true,
	}
	
	// Serialize
	data, err := SerializeTuple(row, columns)
	if err != nil {
		t.Fatalf("Failed to serialize: %v", err)
	}
	
	// Deserialize
	tuple, err := DeserializeTuple(data)
	if err != nil {
		t.Fatalf("Failed to deserialize: %v", err)
	}
	
	// Verify data
	if tuple.Data["id"] != float64(123) { // JSON unmarshals numbers as float64
		t.Errorf("Expected id 123, got %v", tuple.Data["id"])
	}
	
	if tuple.Data["name"] != "Alice" {
		t.Errorf("Expected name 'Alice', got %v", tuple.Data["name"])
	}
	
	if tuple.Data["active"] != true {
		t.Errorf("Expected active true, got %v", tuple.Data["active"])
	}
}

func TestSerializeTupleWithNulls(t *testing.T) {
	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "name", DataType: "VARCHAR"},
		{Name: "email", DataType: "VARCHAR"},
	}
	
	row := Row{
		"id":   1,
		"name": "John",
		// email is missing (null)
	}
	
	data, err := SerializeTuple(row, columns)
	if err != nil {
		t.Fatalf("Failed to serialize tuple with nulls: %v", err)
	}
	
	tuple, err := DeserializeTuple(data)
	if err != nil {
		t.Fatalf("Failed to deserialize: %v", err)
	}
	
	// Check null bitmap
	if tuple.Header.NullBitmap == 0 {
		t.Error("Null bitmap should be set for missing column")
	}
	
	// Check IsNull method
	if !tuple.IsNull(2) { // email is column index 2
		t.Error("Column 2 (email) should be null")
	}
	
	if tuple.IsNull(0) { // id is column index 0
		t.Error("Column 0 (id) should not be null")
	}
}

func TestDeserializeTupleInvalidData(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "Too short",
			data: []byte{1, 2, 3},
		},
		{
			name: "Empty",
			data: []byte{},
		},
		{
			name: "Invalid JSON",
			data: append(make([]byte, TupleHeaderSize), []byte("invalid json")...),
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DeserializeTuple(tt.data)
			if err == nil {
				t.Error("Expected error for invalid data")
			}
		})
	}
}

func TestTupleIsNull(t *testing.T) {
	tuple := &Tuple{
		Header: TupleHeader{
			NullBitmap: 0b0000000000000101, // Columns 0 and 2 are null
		},
	}
	
	if !tuple.IsNull(0) {
		t.Error("Column 0 should be null")
	}
	
	if tuple.IsNull(1) {
		t.Error("Column 1 should not be null")
	}
	
	if !tuple.IsNull(2) {
		t.Error("Column 2 should be null")
	}
	
	// Test out of range
	if tuple.IsNull(16) {
		t.Error("Column 16 (out of range) should return false")
	}
	
	if tuple.IsNull(100) {
		t.Error("Column 100 (out of range) should return false")
	}
}

func TestTupleIsVisible(t *testing.T) {
	tests := []struct {
		name    string
		tuple   *Tuple
		txnID   uint32
		visible bool
	}{
		{
			name: "Not committed (Xmin=0)",
			tuple: &Tuple{
				Header: TupleHeader{Xmin: 0, Xmax: 0},
			},
			txnID:   100,
			visible: false,
		},
		{
			name: "Committed and not deleted",
			tuple: &Tuple{
				Header: TupleHeader{Xmin: 50, Xmax: 0},
			},
			txnID:   100,
			visible: true,
		},
		{
			name: "Committed and deleted",
			tuple: &Tuple{
				Header: TupleHeader{Xmin: 50, Xmax: 75},
			},
			txnID:   100,
			visible: false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			visible := tt.tuple.IsVisible(tt.txnID)
			if visible != tt.visible {
				t.Errorf("Expected visible=%v, got %v", tt.visible, visible)
			}
		})
	}
}

func TestTupleSetXmin(t *testing.T) {
	tuple := &Tuple{
		Header: TupleHeader{},
	}
	
	if tuple.Header.Xmin != 0 {
		t.Error("Initial Xmin should be 0")
	}
	
	tuple.SetXmin(42)
	
	if tuple.Header.Xmin != 42 {
		t.Errorf("Expected Xmin 42, got %d", tuple.Header.Xmin)
	}
}

func TestTupleSetXmax(t *testing.T) {
	tuple := &Tuple{
		Header: TupleHeader{},
	}
	
	if tuple.Header.Xmax != 0 {
		t.Error("Initial Xmax should be 0")
	}
	
	tuple.SetXmax(99)
	
	if tuple.Header.Xmax != 99 {
		t.Errorf("Expected Xmax 99, got %d", tuple.Header.Xmax)
	}
}

func TestTupleClone(t *testing.T) {
	original := &Tuple{
		Header: TupleHeader{
			Length:     100,
			NullBitmap: 5,
			Xmin:       10,
			Xmax:       20,
		},
		Data: Row{
			"id":   1,
			"name": "test",
		},
	}
	
	cloned := original.Clone()
	
	// Verify header is copied
	if cloned.Header.Length != original.Header.Length {
		t.Error("Header Length not copied correctly")
	}
	if cloned.Header.NullBitmap != original.Header.NullBitmap {
		t.Error("Header NullBitmap not copied correctly")
	}
	if cloned.Header.Xmin != original.Header.Xmin {
		t.Error("Header Xmin not copied correctly")
	}
	if cloned.Header.Xmax != original.Header.Xmax {
		t.Error("Header Xmax not copied correctly")
	}
	
	// Verify data is copied
	if cloned.Data["id"] != original.Data["id"] {
		t.Error("Data not copied correctly")
	}
	
	// Verify it's a deep copy (modifying clone doesn't affect original)
	cloned.Data["id"] = 999
	if original.Data["id"] == 999 {
		t.Error("Clone should be independent of original")
	}
	
	cloned.Header.Xmin = 999
	if original.Header.Xmin == 999 {
		t.Error("Clone header should be independent")
	}
}

func TestTupleHeaderSize(t *testing.T) {
	if TupleHeaderSize != 12 {
		t.Errorf("Expected TupleHeaderSize 12, got %d", TupleHeaderSize)
	}
}

func TestSerializeTupleLargeData(t *testing.T) {
	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "data", DataType: "TEXT"},
	}
	
	// Create large data
	largeString := string(make([]byte, 10000))
	row := Row{
		"id":   1,
		"data": largeString,
	}
	
	data, err := SerializeTuple(row, columns)
	if err != nil {
		t.Fatalf("Failed to serialize large tuple: %v", err)
	}
	
	if len(data) < 10000 {
		t.Error("Serialized data should include large string")
	}
	
	// Deserialize and verify
	tuple, err := DeserializeTuple(data)
	if err != nil {
		t.Fatalf("Failed to deserialize large tuple: %v", err)
	}
	
	if len(tuple.Data["data"].(string)) != 10000 {
		t.Error("Large data not preserved")
	}
}

func TestSerializeTupleManyColumns(t *testing.T) {
	// Test with more than 16 columns (null bitmap limit)
	columns := make([]Column, 20)
	row := make(Row)
	
	for i := 0; i < 20; i++ {
		colName := string(rune('a' + i))
		columns[i] = Column{Name: colName, DataType: "INT"}
		if i%2 == 0 {
			row[colName] = i
		}
		// Odd columns are null
	}
	
	data, err := SerializeTuple(row, columns)
	if err != nil {
		t.Fatalf("Failed to serialize tuple with many columns: %v", err)
	}
	
	tuple, err := DeserializeTuple(data)
	if err != nil {
		t.Fatalf("Failed to deserialize: %v", err)
	}
	
	// Check first 16 columns null bitmap
	for i := 0; i < 16; i++ {
		isNull := tuple.IsNull(i)
		shouldBeNull := (i % 2 == 1)
		if isNull != shouldBeNull {
			t.Errorf("Column %d: expected null=%v, got %v", i, shouldBeNull, isNull)
		}
	}
}

func TestTupleWithComplexTypes(t *testing.T) {
	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "tags", DataType: "JSON"},
		{Name: "metadata", DataType: "JSON"},
	}
	
	row := Row{
		"id":   1,
		"tags": []string{"tag1", "tag2", "tag3"},
		"metadata": map[string]interface{}{
			"created": "2025-01-01",
			"version": 1,
		},
	}
	
	data, err := SerializeTuple(row, columns)
	if err != nil {
		t.Fatalf("Failed to serialize complex tuple: %v", err)
	}
	
	tuple, err := DeserializeTuple(data)
	if err != nil {
		t.Fatalf("Failed to deserialize complex tuple: %v", err)
	}
	
	// Verify complex types are preserved
	tags, ok := tuple.Data["tags"].([]interface{})
	if !ok {
		t.Error("Tags should be an array")
	}
	if len(tags) != 3 {
		t.Errorf("Expected 3 tags, got %d", len(tags))
	}
	
	metadata, ok := tuple.Data["metadata"].(map[string]interface{})
	if !ok {
		t.Error("Metadata should be a map")
	}
	if metadata["version"] != float64(1) {
		t.Error("Metadata version not preserved")
	}
}

func TestTupleEmptyRow(t *testing.T) {
	columns := []Column{
		{Name: "id", DataType: "INT"},
	}
	
	row := Row{} // Empty row
	
	data, err := SerializeTuple(row, columns)
	if err != nil {
		t.Fatalf("Failed to serialize empty row: %v", err)
	}
	
	tuple, err := DeserializeTuple(data)
	if err != nil {
		t.Fatalf("Failed to deserialize empty row: %v", err)
	}
	
	if len(tuple.Data) != 0 {
		t.Errorf("Expected empty data, got %d items", len(tuple.Data))
	}
}

func TestTupleConcurrentClone(t *testing.T) {
	original := &Tuple{
		Header: TupleHeader{Xmin: 1, Xmax: 0},
		Data: Row{
			"id":   1,
			"name": "test",
		},
	}
	
	// Clone concurrently
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			clone := original.Clone()
			clone.Data["id"] = 999
			done <- true
		}()
	}
	
	// Wait for all
	for i := 0; i < 10; i++ {
		<-done
	}
	
	// Original should be unchanged
	if original.Data["id"] != 1 {
		t.Error("Original data should be unchanged")
	}
}

func BenchmarkSerializeTuple(b *testing.B) {
	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "name", DataType: "VARCHAR"},
		{Name: "age", DataType: "INT"},
	}
	
	row := Row{
		"id":   1,
		"name": "John",
		"age":  30,
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SerializeTuple(row, columns)
	}
}

func BenchmarkDeserializeTuple(b *testing.B) {
	columns := []Column{
		{Name: "id", DataType: "INT"},
		{Name: "name", DataType: "VARCHAR"},
	}
	
	row := Row{
		"id":   1,
		"name": "John",
	}
	
	data, _ := SerializeTuple(row, columns)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DeserializeTuple(data)
	}
}

func BenchmarkTupleClone(b *testing.B) {
	tuple := &Tuple{
		Header: TupleHeader{Xmin: 1, Xmax: 0},
		Data: Row{
			"id":   1,
			"name": "test",
			"age":  30,
		},
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tuple.Clone()
	}
}
