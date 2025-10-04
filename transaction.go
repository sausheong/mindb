package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Transaction ID constants
const (
	InvalidTxnID = 0
	BootstrapTxnID = 1
	FirstNormalTxnID = 2
)

// Transaction states
const (
	TxnStateActive = iota
	TxnStateCommitted
	TxnStateAborted
)

// Transaction represents a database transaction
type Transaction struct {
	ID        uint32
	State     int
	StartTime time.Time
	Snapshot  *Snapshot
}

// Snapshot represents a consistent view of the database
type Snapshot struct {
	XMin       uint32   // Oldest active transaction
	XMax       uint32   // Next transaction ID
	ActiveXIDs []uint32 // Active transaction IDs at snapshot time
}

// TransactionManager manages transactions and snapshots
type TransactionManager struct {
	nextTxnID     uint32
	activeTxns    map[uint32]*Transaction
	committedTxns map[uint32]time.Time
	mu            sync.RWMutex
}

// NewTransactionManager creates a new transaction manager
func NewTransactionManager() *TransactionManager {
	return &TransactionManager{
		nextTxnID:     FirstNormalTxnID,
		activeTxns:    make(map[uint32]*Transaction),
		committedTxns: make(map[uint32]time.Time),
	}
}

// SaveState saves the transaction manager state to disk
func (tm *TransactionManager) SaveState(dataDir string) error {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	metaPath := filepath.Join(dataDir, "txn_meta.dat")
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, tm.nextTxnID)

	return os.WriteFile(metaPath, data, 0644)
}

// LoadState loads the transaction manager state from disk
func (tm *TransactionManager) LoadState(dataDir string) error {
	metaPath := filepath.Join(dataDir, "txn_meta.dat")
	
	data, err := os.ReadFile(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			// No metadata file, start fresh
			return nil
		}
		return fmt.Errorf("failed to read transaction metadata: %v", err)
	}

	if len(data) < 4 {
		return fmt.Errorf("invalid transaction metadata file")
	}

	tm.mu.Lock()
	defer tm.mu.Unlock()

	nextTxnID := binary.LittleEndian.Uint32(data)
	// Ensure we start with at least FirstNormalTxnID
	if nextTxnID < FirstNormalTxnID {
		nextTxnID = FirstNormalTxnID
	}
	tm.nextTxnID = nextTxnID

	return nil
}

// BeginTransaction starts a new transaction
func (tm *TransactionManager) BeginTransaction() (*Transaction, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Assign transaction ID
	txnID := tm.nextTxnID
	tm.nextTxnID++

	// Create snapshot
	snapshot := tm.createSnapshot()

	// Create transaction
	txn := &Transaction{
		ID:        txnID,
		State:     TxnStateActive,
		StartTime: time.Now(),
		Snapshot:  snapshot,
	}

	tm.activeTxns[txnID] = txn

	return txn, nil
}

// CommitTransaction commits a transaction
func (tm *TransactionManager) CommitTransaction(txnID uint32) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	txn, exists := tm.activeTxns[txnID]
	if !exists {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	if txn.State != TxnStateActive {
		return fmt.Errorf("transaction %d not active", txnID)
	}

	// Mark as committed
	txn.State = TxnStateCommitted
	tm.committedTxns[txnID] = time.Now()

	// Remove from active transactions
	delete(tm.activeTxns, txnID)

	return nil
}

// AbortTransaction aborts a transaction
func (tm *TransactionManager) AbortTransaction(txnID uint32) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	txn, exists := tm.activeTxns[txnID]
	if !exists {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	if txn.State != TxnStateActive {
		return fmt.Errorf("transaction %d not active", txnID)
	}

	// Mark as aborted
	txn.State = TxnStateAborted

	// Remove from active transactions
	delete(tm.activeTxns, txnID)

	return nil
}

// createSnapshot creates a snapshot of current transaction state
func (tm *TransactionManager) createSnapshot() *Snapshot {
	// Find oldest active transaction
	xMin := tm.nextTxnID
	for txnID := range tm.activeTxns {
		if txnID < xMin {
			xMin = txnID
		}
	}

	// Collect active transaction IDs
	activeXIDs := make([]uint32, 0, len(tm.activeTxns))
	for txnID := range tm.activeTxns {
		activeXIDs = append(activeXIDs, txnID)
	}

	return &Snapshot{
		XMin:       xMin,
		XMax:       tm.nextTxnID,
		ActiveXIDs: activeXIDs,
	}
}

// IsVisible checks if a tuple is visible to a transaction
func (tm *TransactionManager) IsVisible(tuple *Tuple, snapshot *Snapshot) bool {
	xmin := tuple.Header.Xmin
	xmax := tuple.Header.Xmax

	// Tuple not yet created
	if xmin == InvalidTxnID {
		return false
	}

	// Tuple created by a transaction that started after our snapshot
	if xmin >= snapshot.XMax {
		return false
	}

	// Tuple created by an active transaction (not us)
	if tm.isActiveInSnapshot(xmin, snapshot) {
		return false
	}

	// Tuple not deleted
	if xmax == InvalidTxnID {
		return true
	}

	// Tuple deleted by a transaction that started after our snapshot
	if xmax >= snapshot.XMax {
		return true
	}

	// Tuple deleted by an active transaction
	if tm.isActiveInSnapshot(xmax, snapshot) {
		return true
	}

	// Tuple was deleted by a committed transaction before our snapshot
	return false
}

// isActiveInSnapshot checks if a transaction was active in the snapshot
func (tm *TransactionManager) isActiveInSnapshot(txnID uint32, snapshot *Snapshot) bool {
	for _, activeXID := range snapshot.ActiveXIDs {
		if activeXID == txnID {
			return true
		}
	}
	return false
}

// GetActiveTransactionCount returns the number of active transactions
func (tm *TransactionManager) GetActiveTransactionCount() int {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return len(tm.activeTxns)
}

// GetOldestActiveTransaction returns the oldest active transaction ID
func (tm *TransactionManager) GetOldestActiveTransaction() uint32 {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	if len(tm.activeTxns) == 0 {
		return tm.nextTxnID
	}

	oldest := tm.nextTxnID
	for txnID := range tm.activeTxns {
		if txnID < oldest {
			oldest = txnID
		}
	}

	return oldest
}

// CleanupOldTransactions removes old committed transaction records
func (tm *TransactionManager) CleanupOldTransactions(olderThan time.Duration) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	cutoff := time.Now().Add(-olderThan)

	for txnID, commitTime := range tm.committedTxns {
		if commitTime.Before(cutoff) {
			delete(tm.committedTxns, txnID)
		}
	}
}

// GetNextTxnID returns the next transaction ID (for testing)
func (tm *TransactionManager) GetNextTxnID() uint32 {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.nextTxnID
}

// UpdateNextTxnID updates the next transaction ID if the provided value is higher
func (tm *TransactionManager) UpdateNextTxnID(txnID uint32) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	
	if txnID > tm.nextTxnID {
		tm.nextTxnID = txnID
	}
}
