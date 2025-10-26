package mindb

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

// TupleHeader contains metadata for a tuple
type TupleHeader struct {
	Length     uint16 // Total tuple length including header
	NullBitmap uint16 // Bitmap for null values (supports up to 16 columns)
	// MVCC fields (for future use)
	Xmin uint32 // Transaction ID that inserted this tuple
	Xmax uint32 // Transaction ID that deleted this tuple (0 if active)
}

const TupleHeaderSize = 12 // 2 + 2 + 4 + 4 bytes

// Tuple represents a row with header and data
type Tuple struct {
	Header TupleHeader
	Data   Row // Column name -> value map
}

// SerializeTuple converts a tuple to bytes
func SerializeTuple(row Row, columns []Column) ([]byte, error) {
	return SerializeTupleWithHeader(row, columns, 0, 0)
}

// SerializeTupleWithHeader converts a tuple to bytes with specific Xmin/Xmax
func SerializeTupleWithHeader(row Row, columns []Column, xmin, xmax uint32) ([]byte, error) {
	// Serialize data as JSON for now (simple approach)
	// In production, would use a more efficient binary format
	dataBytes, err := json.Marshal(row)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal tuple: %v", err)
	}
	
	// Calculate null bitmap
	nullBitmap := uint16(0)
	for i, col := range columns {
		if i >= 16 {
			break // Bitmap only supports 16 columns
		}
		if val, exists := row[col.Name]; !exists || val == nil {
			nullBitmap |= (1 << uint(i))
		}
	}
	
	// Create header
	header := TupleHeader{
		Length:     uint16(TupleHeaderSize + len(dataBytes)),
		NullBitmap: nullBitmap,
		Xmin:       xmin,
		Xmax:       xmax,
	}
	
	// Serialize header + data
	result := make([]byte, header.Length)
	binary.LittleEndian.PutUint16(result[0:2], header.Length)
	binary.LittleEndian.PutUint16(result[2:4], header.NullBitmap)
	binary.LittleEndian.PutUint32(result[4:8], header.Xmin)
	binary.LittleEndian.PutUint32(result[8:12], header.Xmax)
	copy(result[TupleHeaderSize:], dataBytes)
	
	return result, nil
}

// DeserializeTuple converts bytes back to a tuple
func DeserializeTuple(data []byte) (*Tuple, error) {
	if len(data) < TupleHeaderSize {
		return nil, fmt.Errorf("tuple data too short: %d bytes", len(data))
	}
	
	// Deserialize header
	header := TupleHeader{
		Length:     binary.LittleEndian.Uint16(data[0:2]),
		NullBitmap: binary.LittleEndian.Uint16(data[2:4]),
		Xmin:       binary.LittleEndian.Uint32(data[4:8]),
		Xmax:       binary.LittleEndian.Uint32(data[8:12]),
	}
	
	if int(header.Length) != len(data) {
		return nil, fmt.Errorf("tuple length mismatch: header=%d, actual=%d", header.Length, len(data))
	}
	
	// Deserialize data
	var row Row
	if err := json.Unmarshal(data[TupleHeaderSize:], &row); err != nil {
		return nil, fmt.Errorf("failed to unmarshal tuple: %v", err)
	}
	
	return &Tuple{
		Header: header,
		Data:   row,
	}, nil
}

// IsNull checks if a column is null based on the null bitmap
func (t *Tuple) IsNull(columnIndex int) bool {
	if columnIndex >= 16 {
		return false
	}
	return (t.Header.NullBitmap & (1 << uint(columnIndex))) != 0
}

// IsVisible checks if a tuple is visible to a transaction (MVCC)
// For now, always return true (no MVCC yet)
func (t *Tuple) IsVisible(txnID uint32) bool {
	// Simple visibility check
	// Tuple is visible if:
	// 1. It was created by a committed transaction (Xmin > 0)
	// 2. It hasn't been deleted (Xmax == 0) or deleted by an uncommitted transaction
	
	if t.Header.Xmin == 0 {
		return false // Not yet committed
	}
	
	if t.Header.Xmax == 0 {
		return true // Not deleted
	}
	
	// For now, treat all transactions as committed
	return false
}

// SetXmin sets the transaction ID that created this tuple
func (t *Tuple) SetXmin(txnID uint32) {
	t.Header.Xmin = txnID
}

// SetXmax sets the transaction ID that deleted this tuple
func (t *Tuple) SetXmax(txnID uint32) {
	t.Header.Xmax = txnID
}

// Clone creates a deep copy of the tuple
func (t *Tuple) Clone() *Tuple {
	newRow := make(Row)
	for k, v := range t.Data {
		newRow[k] = v
	}
	
	return &Tuple{
		Header: t.Header,
		Data:   newRow,
	}
}
