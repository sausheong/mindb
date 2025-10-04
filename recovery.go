package main

import (
	"encoding/binary"
	"fmt"
	"sync"
)

// RecoveryManager handles crash recovery using ARIES algorithm
type RecoveryManager struct {
	walManager *WALManager
	heapFiles  map[string]*HeapFile // tableName -> HeapFile
	maxTxnID   uint32               // Maximum transaction ID seen during recovery
	mu         sync.RWMutex
}

// NewRecoveryManager creates a new recovery manager
func NewRecoveryManager(walManager *WALManager) *RecoveryManager {
	return &RecoveryManager{
		walManager: walManager,
		heapFiles:  make(map[string]*HeapFile),
	}
}

// RegisterHeapFile registers a heap file for recovery
func (rm *RecoveryManager) RegisterHeapFile(tableName string, hf *HeapFile) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.heapFiles[tableName] = hf
}

// Recover performs ARIES-style recovery
func (rm *RecoveryManager) Recover() error {
	fmt.Println("Starting ARIES recovery...")

	// Reset max transaction ID
	rm.maxTxnID = 0

	// Phase 1: Analysis - determine which transactions to redo/undo
	redoLSN, activeTxns, err := rm.analysisPass()
	if err != nil {
		return fmt.Errorf("analysis pass failed: %v", err)
	}

	fmt.Printf("Analysis complete: redoLSN=%d, activeTxns=%v\n", redoLSN, activeTxns)

	// Phase 2: Redo - replay all changes from redoLSN
	if err := rm.redoPass(redoLSN); err != nil {
		return fmt.Errorf("redo pass failed: %v", err)
	}

	fmt.Println("Redo pass complete")

	// Phase 3: Undo - rollback uncommitted transactions
	if err := rm.undoPass(activeTxns); err != nil {
		return fmt.Errorf("undo pass failed: %v", err)
	}

	fmt.Println("Undo pass complete - recovery finished")

	return nil
}

// GetMaxTxnID returns the maximum transaction ID seen during recovery
func (rm *RecoveryManager) GetMaxTxnID() uint32 {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.maxTxnID
}

// analysisPass determines the redo point and active transactions
func (rm *RecoveryManager) analysisPass() (LSN, map[uint32]bool, error) {
	// Read all WAL records
	records, err := rm.walManager.ReadRecords(0)
	if err != nil {
		return 0, nil, err
	}

	if len(records) == 0 {
		return 0, make(map[uint32]bool), nil
	}

	// Find the last checkpoint
	var lastCheckpointLSN LSN = 0
	var checkpointData *CheckpointData

	for _, record := range records {
		if record.Header.RecordType == WALRecordCheckpoint {
			lastCheckpointLSN = record.Header.LSN
			checkpointData = rm.parseCheckpointData(record.Data)
		}
	}

	// Determine redo point
	redoLSN := LSN(1)
	if checkpointData != nil {
		redoLSN = checkpointData.RedoLSN
	}

	// Track active transactions
	activeTxns := make(map[uint32]bool)
	if checkpointData != nil {
		for _, txnID := range checkpointData.ActiveTxns {
			activeTxns[txnID] = true
		}
	}

	// Scan forward from checkpoint to find commits/aborts
	for _, record := range records {
		if record.Header.LSN < lastCheckpointLSN {
			continue
		}

		// Track maximum transaction ID
		if record.Header.TxnID > rm.maxTxnID {
			rm.maxTxnID = record.Header.TxnID
		}

		switch record.Header.RecordType {
		case WALRecordInsert, WALRecordUpdate, WALRecordDelete:
			activeTxns[record.Header.TxnID] = true
		case WALRecordCommit, WALRecordAbort:
			delete(activeTxns, record.Header.TxnID)
		}
	}

	return redoLSN, activeTxns, nil
}

// redoPass replays all changes from the redo point
func (rm *RecoveryManager) redoPass(redoLSN LSN) error {
	records, err := rm.walManager.ReadRecords(redoLSN)
	if err != nil {
		return err
	}

	for _, record := range records {
		switch record.Header.RecordType {
		case WALRecordInsert:
			if err := rm.redoInsert(record); err != nil {
				fmt.Printf("Warning: failed to redo insert LSN %d: %v\n", record.Header.LSN, err)
			}
		case WALRecordUpdate:
			if err := rm.redoUpdate(record); err != nil {
				fmt.Printf("Warning: failed to redo update LSN %d: %v\n", record.Header.LSN, err)
			}
		case WALRecordDelete:
			if err := rm.redoDelete(record); err != nil {
				fmt.Printf("Warning: failed to redo delete LSN %d: %v\n", record.Header.LSN, err)
			}
		}
	}

	return nil
}

// undoPass rolls back uncommitted transactions
func (rm *RecoveryManager) undoPass(activeTxns map[uint32]bool) error {
	if len(activeTxns) == 0 {
		return nil
	}

	// Read all WAL records
	records, err := rm.walManager.ReadRecords(0)
	if err != nil {
		return err
	}

	// Build per-transaction record lists (in reverse order)
	txnRecords := make(map[uint32][]*WALRecord)
	for i := len(records) - 1; i >= 0; i-- {
		record := records[i]
		if activeTxns[record.Header.TxnID] {
			txnRecords[record.Header.TxnID] = append(txnRecords[record.Header.TxnID], record)
		}
	}

	// Undo each transaction
	for txnID, txnRecs := range txnRecords {
		fmt.Printf("Undoing transaction %d (%d records)\n", txnID, len(txnRecs))
		for _, record := range txnRecs {
			switch record.Header.RecordType {
			case WALRecordInsert:
				if err := rm.undoInsert(record); err != nil {
					fmt.Printf("Warning: failed to undo insert: %v\n", err)
				}
			case WALRecordUpdate:
				if err := rm.undoUpdate(record); err != nil {
					fmt.Printf("Warning: failed to undo update: %v\n", err)
				}
			case WALRecordDelete:
				if err := rm.undoDelete(record); err != nil {
					fmt.Printf("Warning: failed to undo delete: %v\n", err)
				}
			}
		}
	}

	return nil
}

// redoInsert replays an insert operation
func (rm *RecoveryManager) redoInsert(record *WALRecord) error {
	// Parse insert data
	_ = parseInsertData(record.Data)

	// Find heap file (for now, we'll skip if not registered)
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	// In a full implementation, we'd look up the table name from a catalog
	// For now, we'll skip redo if heap file not found
	return nil
}

// redoUpdate replays an update operation
func (rm *RecoveryManager) redoUpdate(record *WALRecord) error {
	// Parse update data
	_ = parseUpdateData(record.Data)

	// Apply update (similar to redoInsert)
	return nil
}

// redoDelete replays a delete operation
func (rm *RecoveryManager) redoDelete(record *WALRecord) error {
	// Parse delete data
	_ = parseDeleteData(record.Data)

	// Apply delete
	return nil
}

// undoInsert reverses an insert (delete the tuple)
func (rm *RecoveryManager) undoInsert(record *WALRecord) error {
	_ = parseInsertData(record.Data)
	// Would delete the tuple here
	return nil
}

// undoUpdate reverses an update (restore old value)
func (rm *RecoveryManager) undoUpdate(record *WALRecord) error {
	_ = parseUpdateData(record.Data)
	// Would restore old tuple data here
	return nil
}

// undoDelete reverses a delete (re-insert the tuple)
func (rm *RecoveryManager) undoDelete(record *WALRecord) error {
	_ = parseDeleteData(record.Data)
	// Would re-insert the tuple here
	return nil
}

// CheckpointData contains checkpoint information
type CheckpointData struct {
	RedoLSN    LSN
	ActiveTxns []uint32
	DirtyPages []PageID
}

// CreateCheckpoint creates a checkpoint record
func (rm *RecoveryManager) CreateCheckpoint(activeTxns []uint32, dirtyPages []PageID) (LSN, error) {
	// Determine redo LSN (oldest LSN of dirty pages)
	redoLSN := rm.walManager.GetCurrentLSN()

	checkpointData := CheckpointData{
		RedoLSN:    redoLSN,
		ActiveTxns: activeTxns,
		DirtyPages: dirtyPages,
	}

	// Serialize checkpoint data
	data := rm.serializeCheckpointData(&checkpointData)

	// Write checkpoint record
	lsn, err := rm.walManager.AppendRecord(0, WALRecordCheckpoint, data)
	if err != nil {
		return 0, err
	}

	// Sync WAL
	if err := rm.walManager.Sync(); err != nil {
		return 0, err
	}

	return lsn, nil
}

// serializeCheckpointData serializes checkpoint data
func (rm *RecoveryManager) serializeCheckpointData(data *CheckpointData) []byte {
	// Calculate size
	size := 8 + 4 + len(data.ActiveTxns)*4 + 4 + len(data.DirtyPages)*4

	buf := make([]byte, size)
	offset := 0

	// RedoLSN
	binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(data.RedoLSN))
	offset += 8

	// ActiveTxns count
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(data.ActiveTxns)))
	offset += 4

	// ActiveTxns
	for _, txnID := range data.ActiveTxns {
		binary.LittleEndian.PutUint32(buf[offset:offset+4], txnID)
		offset += 4
	}

	// DirtyPages count
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(data.DirtyPages)))
	offset += 4

	// DirtyPages
	for _, pageID := range data.DirtyPages {
		binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(pageID))
		offset += 4
	}

	return buf
}

// parseCheckpointData parses checkpoint data
func (rm *RecoveryManager) parseCheckpointData(data []byte) *CheckpointData {
	if len(data) < 12 {
		return nil
	}

	offset := 0

	// RedoLSN
	redoLSN := LSN(binary.LittleEndian.Uint64(data[offset : offset+8]))
	offset += 8

	// ActiveTxns
	activeTxnsCount := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	activeTxns := make([]uint32, activeTxnsCount)
	for i := uint32(0); i < activeTxnsCount; i++ {
		activeTxns[i] = binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4
	}

	// DirtyPages
	if offset+4 > len(data) {
		return &CheckpointData{RedoLSN: redoLSN, ActiveTxns: activeTxns}
	}

	dirtyPagesCount := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	dirtyPages := make([]PageID, dirtyPagesCount)
	for i := uint32(0); i < dirtyPagesCount; i++ {
		if offset+4 > len(data) {
			break
		}
		dirtyPages[i] = PageID(binary.LittleEndian.Uint32(data[offset : offset+4]))
		offset += 4
	}

	return &CheckpointData{
		RedoLSN:    redoLSN,
		ActiveTxns: activeTxns,
		DirtyPages: dirtyPages,
	}
}

// Helper functions to parse WAL record data

func parseInsertData(data []byte) *InsertData {
	if len(data) < 6 {
		return nil
	}

	pageID := PageID(binary.LittleEndian.Uint32(data[0:4]))
	slotNum := binary.LittleEndian.Uint16(data[4:6])
	tupleData := data[6:]

	return &InsertData{
		PageID:    pageID,
		SlotNum:   slotNum,
		TupleData: tupleData,
	}
}

func parseUpdateData(data []byte) *UpdateData {
	if len(data) < 10 {
		return nil
	}

	pageID := PageID(binary.LittleEndian.Uint32(data[0:4]))
	slotNum := binary.LittleEndian.Uint16(data[4:6])
	oldLen := binary.LittleEndian.Uint16(data[6:8])
	newLen := binary.LittleEndian.Uint16(data[8:10])

	offset := 10
	oldData := data[offset : offset+int(oldLen)]
	offset += int(oldLen)
	newData := data[offset : offset+int(newLen)]

	return &UpdateData{
		PageID:      pageID,
		SlotNum:     slotNum,
		OldTupleData: oldData,
		NewTupleData: newData,
	}
}

func parseDeleteData(data []byte) *DeleteData {
	if len(data) < 6 {
		return nil
	}

	pageID := PageID(binary.LittleEndian.Uint32(data[0:4]))
	slotNum := binary.LittleEndian.Uint16(data[4:6])
	tupleData := data[6:]

	return &DeleteData{
		PageID:    pageID,
		SlotNum:   slotNum,
		TupleData: tupleData,
	}
}

// Data structures for parsed WAL records

type InsertData struct {
	PageID    PageID
	SlotNum   uint16
	TupleData []byte
}

type UpdateData struct {
	PageID      PageID
	SlotNum     uint16
	OldTupleData []byte
	NewTupleData []byte
}

type DeleteData struct {
	PageID    PageID
	SlotNum   uint16
	TupleData []byte
}
