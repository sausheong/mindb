package mindb

import (
	"fmt"
	"sync"
)

// VacuumManager handles garbage collection of dead tuples
type VacuumManager struct {
	txnManager *TransactionManager
	mu         sync.Mutex
}

// NewVacuumManager creates a new vacuum manager
func NewVacuumManager(txnManager *TransactionManager) *VacuumManager {
	return &VacuumManager{
		txnManager: txnManager,
	}
}

// VacuumStats contains statistics from a vacuum operation
type VacuumStats struct {
	PagesScanned  int
	TuplesScanned int
	DeadTuples    int
	TuplesRemoved int
	PagesCompacted int
}

// VacuumTable performs vacuum on a table
func (vm *VacuumManager) VacuumTable(heapFile *HeapFile, columns []Column) (*VacuumStats, error) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	stats := &VacuumStats{}

	// Get oldest active transaction
	oldestXID := vm.txnManager.GetOldestActiveTransaction()

	// Scan all pages
	pageCount := heapFile.GetPageCount()
	for pageID := PageID(0); pageID < PageID(pageCount); pageID++ {
		stats.PagesScanned++

		page, err := heapFile.ReadPage(pageID)
		if err != nil {
			continue
		}

		deadSlots := make([]uint16, 0)

		// Check each tuple
		for slotNum := uint16(0); slotNum < page.Header.SlotCount; slotNum++ {
			stats.TuplesScanned++

			// Skip already deleted slots
			if page.Slots[slotNum].Length == 0 {
				stats.DeadTuples++
				continue
			}

			tupleData, err := page.GetTuple(slotNum)
			if err != nil {
				continue
			}

			tuple, err := DeserializeTuple(tupleData)
			if err != nil {
				continue
			}

			// Check if tuple is dead (deleted and no longer visible to any transaction)
			if vm.isTupleDead(tuple, oldestXID) {
				stats.DeadTuples++
				deadSlots = append(deadSlots, slotNum)
			}
		}

		// Remove dead tuples
		for _, slotNum := range deadSlots {
			if err := page.DeleteTuple(slotNum); err == nil {
				stats.TuplesRemoved++
			}
		}

		// Compact page if we removed tuples
		if len(deadSlots) > 0 {
			page.Compact()
			stats.PagesCompacted++

			// Write page back
			if err := heapFile.WritePage(page); err != nil {
				return stats, fmt.Errorf("failed to write page: %v", err)
			}
		}
	}

	return stats, nil
}

// isTupleDead checks if a tuple is dead and can be removed
func (vm *VacuumManager) isTupleDead(tuple *Tuple, oldestXID uint32) bool {
	// Tuple must be deleted (Xmax set)
	if tuple.Header.Xmax == InvalidTxnID {
		return false
	}

	// Deleting transaction must be committed and older than oldest active transaction
	if tuple.Header.Xmax >= oldestXID {
		return false
	}

	// Tuple is dead - deleted by a committed transaction and no active transaction can see it
	return true
}

// VacuumDatabase performs vacuum on all tables in a database
func (vm *VacuumManager) VacuumDatabase(db *PagedDatabase) (map[string]*VacuumStats, error) {
	results := make(map[string]*VacuumStats)

	db.mu.RLock()
	tables := make([]*PagedTable, 0, len(db.Tables))
	for _, table := range db.Tables {
		tables = append(tables, table)
	}
	db.mu.RUnlock()

	// Vacuum each table
	for _, table := range tables {
		table.mu.Lock()
		stats, err := vm.VacuumTable(table.HeapFile, table.Columns)
		table.mu.Unlock()

		if err != nil {
			return results, fmt.Errorf("vacuum table %s failed: %v", table.Name, err)
		}

		results[table.Name] = stats
	}

	return results, nil
}

// AutoVacuum performs automatic vacuum based on thresholds
func (vm *VacuumManager) AutoVacuum(db *PagedDatabase, deadTupleThreshold int) error {
	// Check if vacuum is needed
	totalDead := 0

	db.mu.RLock()
	for range db.Tables {
		// Quick estimate of dead tuples (would need tracking in production)
		// For now, always vacuum if threshold is met
	}
	db.mu.RUnlock()

	if totalDead < deadTupleThreshold {
		return nil // No vacuum needed
	}

	// Perform vacuum
	_, err := vm.VacuumDatabase(db)
	return err
}
