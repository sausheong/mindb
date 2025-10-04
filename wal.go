package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// WAL record types
const (
	WALRecordInsert     = 1
	WALRecordUpdate     = 2
	WALRecordDelete     = 3
	WALRecordCheckpoint = 4
	WALRecordCommit     = 5
	WALRecordAbort      = 6
)

// WAL constants
const (
	WALSegmentSize      = 16 * 1024 * 1024 // 16MB segments
	WALRecordHeaderSize = 29                // Record header size
)

// WALRecordHeader contains metadata for a WAL record
type WALRecordHeader struct {
	LSN        LSN    // Log Sequence Number
	PrevLSN    LSN    // Previous LSN for same transaction
	TxnID      uint32 // Transaction ID
	RecordType uint8  // Type of record
	Length     uint32 // Total record length including header
	Checksum   uint32 // CRC32 checksum
}

// WALRecord represents a complete WAL record
type WALRecord struct {
	Header WALRecordHeader
	Data   []byte // Record-specific data
}

// WALManager manages the write-ahead log
type WALManager struct {
	walDir      string
	currentFile *os.File
	currentLSN  LSN
	segmentNum  uint32
	mu          sync.Mutex
}

// NewWALManager creates a new WAL manager
func NewWALManager(walDir string) (*WALManager, error) {
	// Create WAL directory
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %v", err)
	}

	wm := &WALManager{
		walDir:     walDir,
		currentLSN: 1,
		segmentNum: 0,
	}

	// Open or create first segment
	if err := wm.openSegment(0); err != nil {
		return nil, err
	}

	// Recover current LSN from existing WAL
	if err := wm.recoverLSN(); err != nil {
		return nil, err
	}

	return wm, nil
}

// openSegment opens a WAL segment file
func (wm *WALManager) openSegment(segmentNum uint32) error {
	filename := filepath.Join(wm.walDir, fmt.Sprintf("wal_%08d", segmentNum))

	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open WAL segment: %v", err)
	}

	if wm.currentFile != nil {
		wm.currentFile.Close()
	}

	wm.currentFile = file
	wm.segmentNum = segmentNum

	return nil
}

// recoverLSN recovers the current LSN from existing WAL files
func (wm *WALManager) recoverLSN() error {
	files, err := filepath.Glob(filepath.Join(wm.walDir, "wal_*"))
	if err != nil {
		return err
	}

	if len(files) == 0 {
		wm.currentLSN = 1
		return nil
	}

	maxLSN := LSN(0)

	// Read all segments to find max LSN
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			continue
		}

		for {
			record, err := wm.readRecordFromFile(file)
			if err == io.EOF {
				break
			}
			if err != nil {
				break
			}

			if record.Header.LSN > maxLSN {
				maxLSN = record.Header.LSN
			}
		}

		file.Close()
	}

	wm.currentLSN = maxLSN + 1
	return nil
}

// AppendRecord appends a WAL record and returns its LSN
func (wm *WALManager) AppendRecord(txnID uint32, recordType uint8, data []byte) (LSN, error) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	// Assign LSN
	lsn := wm.currentLSN
	wm.currentLSN++

	// Create record
	record := WALRecord{
		Header: WALRecordHeader{
			LSN:        lsn,
			PrevLSN:    0,
			TxnID:      txnID,
			RecordType: recordType,
			Length:     uint32(WALRecordHeaderSize + len(data)),
			Checksum:   0,
		},
		Data: data,
	}

	// Compute checksum
	record.Header.Checksum = wm.computeChecksum(&record)

	// Serialize record
	recordBytes := wm.serializeRecord(&record)

	// Check if we need a new segment
	fileInfo, err := wm.currentFile.Stat()
	if err != nil {
		return 0, err
	}

	if fileInfo.Size()+int64(len(recordBytes)) > WALSegmentSize {
		if err := wm.openSegment(wm.segmentNum + 1); err != nil {
			return 0, err
		}
	}

	// Write to WAL
	if _, err := wm.currentFile.Write(recordBytes); err != nil {
		return 0, fmt.Errorf("failed to write WAL record: %v", err)
	}

	return lsn, nil
}

// Sync forces WAL to disk
func (wm *WALManager) Sync() error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if wm.currentFile != nil {
		return wm.currentFile.Sync()
	}
	return nil
}

// serializeRecord converts a WAL record to bytes
func (wm *WALManager) serializeRecord(record *WALRecord) []byte {
	buf := make([]byte, record.Header.Length)

	// Serialize header
	binary.LittleEndian.PutUint64(buf[0:8], uint64(record.Header.LSN))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(record.Header.PrevLSN))
	binary.LittleEndian.PutUint32(buf[16:20], record.Header.TxnID)
	buf[20] = record.Header.RecordType
	binary.LittleEndian.PutUint32(buf[21:25], record.Header.Length)
	binary.LittleEndian.PutUint32(buf[25:29], record.Header.Checksum)

	// Copy data
	copy(buf[WALRecordHeaderSize:], record.Data)

	return buf
}

// computeChecksum computes CRC32 checksum for a record
func (wm *WALManager) computeChecksum(record *WALRecord) uint32 {
	buf := make([]byte, WALRecordHeaderSize-4+len(record.Data))

	binary.LittleEndian.PutUint64(buf[0:8], uint64(record.Header.LSN))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(record.Header.PrevLSN))
	binary.LittleEndian.PutUint32(buf[16:20], record.Header.TxnID)
	buf[20] = record.Header.RecordType
	binary.LittleEndian.PutUint32(buf[21:25], record.Header.Length)

	copy(buf[25:], record.Data)

	return crc32.ChecksumIEEE(buf)
}

// verifyChecksum verifies a record's checksum
func (wm *WALManager) verifyChecksum(record *WALRecord) bool {
	computed := wm.computeChecksum(record)
	return computed == record.Header.Checksum
}

// readRecordFromFile reads a single WAL record from a file
func (wm *WALManager) readRecordFromFile(file *os.File) (*WALRecord, error) {
	// Read header
	headerBuf := make([]byte, WALRecordHeaderSize)
	n, err := file.Read(headerBuf)
	if err != nil {
		return nil, err
	}
	if n != WALRecordHeaderSize {
		return nil, io.EOF
	}

	// Parse header
	header := WALRecordHeader{
		LSN:        LSN(binary.LittleEndian.Uint64(headerBuf[0:8])),
		PrevLSN:    LSN(binary.LittleEndian.Uint64(headerBuf[8:16])),
		TxnID:      binary.LittleEndian.Uint32(headerBuf[16:20]),
		RecordType: headerBuf[20],
		Length:     binary.LittleEndian.Uint32(headerBuf[21:25]),
		Checksum:   binary.LittleEndian.Uint32(headerBuf[25:29]),
	}

	// Read data
	dataLen := header.Length - WALRecordHeaderSize
	dataBuf := make([]byte, dataLen)
	n, err = file.Read(dataBuf)
	if err != nil {
		return nil, err
	}
	if n != int(dataLen) {
		return nil, io.EOF
	}

	record := &WALRecord{
		Header: header,
		Data:   dataBuf,
	}

	return record, nil
}

// ReadRecords reads all WAL records starting from a given LSN
func (wm *WALManager) ReadRecords(fromLSN LSN) ([]*WALRecord, error) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	var records []*WALRecord

	// Find all WAL segments
	files, err := filepath.Glob(filepath.Join(wm.walDir, "wal_*"))
	if err != nil {
		return nil, err
	}

	// Read each segment
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			continue
		}

		for {
			record, err := wm.readRecordFromFile(file)
			if err == io.EOF {
				break
			}
			if err != nil {
				break
			}

			// Verify checksum
			if !wm.verifyChecksum(record) {
				break
			}

			if record.Header.LSN >= fromLSN {
				records = append(records, record)
			}
		}

		file.Close()
	}

	return records, nil
}

// GetCurrentLSN returns the current LSN
func (wm *WALManager) GetCurrentLSN() LSN {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	return wm.currentLSN
}

// Close closes the WAL manager
func (wm *WALManager) Close() error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if wm.currentFile != nil {
		if err := wm.currentFile.Sync(); err != nil {
			return err
		}
		return wm.currentFile.Close()
	}
	return nil
}

// Truncate removes WAL records before a given LSN
func (wm *WALManager) Truncate(beforeLSN LSN) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	// Find segments that can be deleted
	files, err := filepath.Glob(filepath.Join(wm.walDir, "wal_*"))
	if err != nil {
		return err
	}

	for _, filename := range files {
		// Check if all records in this segment are before beforeLSN
		file, err := os.Open(filename)
		if err != nil {
			continue
		}

		canDelete := true
		for {
			record, err := wm.readRecordFromFile(file)
			if err == io.EOF {
				break
			}
			if err != nil {
				break
			}

			if record.Header.LSN >= beforeLSN {
				canDelete = false
				break
			}
		}

		file.Close()

		if canDelete {
			os.Remove(filename)
		}
	}

	return nil
}
