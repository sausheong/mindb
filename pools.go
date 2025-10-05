package mindb

import (
	"bytes"
	"sync"
)

// Object pools to reduce memory allocations

// rowPool pools Row objects
var rowPool = sync.Pool{
	New: func() interface{} {
		return make(Row)
	},
}

// GetRow gets a Row from the pool
func GetRow() Row {
	return rowPool.Get().(Row)
}

// PutRow returns a Row to the pool
func PutRow(row Row) {
	// Clear the map
	for k := range row {
		delete(row, k)
	}
	rowPool.Put(row)
}

// bufferPool pools bytes.Buffer objects
var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// GetBuffer gets a Buffer from the pool
func GetBuffer() *bytes.Buffer {
	return bufferPool.Get().(*bytes.Buffer)
}

// PutBuffer returns a Buffer to the pool
func PutBuffer(buf *bytes.Buffer) {
	buf.Reset()
	bufferPool.Put(buf)
}

// tupleIDSlicePool pools TupleID slices
var tupleIDSlicePool = sync.Pool{
	New: func() interface{} {
		return make([]TupleID, 0, 100)
	},
}

// GetTupleIDSlice gets a TupleID slice from the pool
func GetTupleIDSlice() []TupleID {
	return tupleIDSlicePool.Get().([]TupleID)
}

// PutTupleIDSlice returns a TupleID slice to the pool
func PutTupleIDSlice(slice []TupleID) {
	slice = slice[:0]
	tupleIDSlicePool.Put(slice)
}
