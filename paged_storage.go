package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// PagedTable represents a table using paged heap file storage
type PagedTable struct {
	Name      string
	Columns   []Column
	HeapFile  *HeapFile
	Indexes   map[string]*BTree // Column name -> B-tree index
	TupleIDs  []TupleID         // Track tuple IDs for scanning
	mu        sync.RWMutex
}

// PagedDatabase represents a database using paged storage
type PagedDatabase struct {
	Name   string
	Tables map[string]*PagedTable
	mu     sync.RWMutex
}

// PagedEngine manages databases with paged storage
type PagedEngine struct {
	databases      map[string]*PagedDatabase
	currentDB      string
	dataDir        string
	walManager     *WALManager
	recoveryMgr    *RecoveryManager
	txnManager     *TransactionManager
	vacuumManager  *VacuumManager
	catalog        *SystemCatalog
	currentTxn     *Transaction // Current transaction (if any)
	mu             sync.RWMutex
}

// NewPagedEngine creates a new paged storage engine
func NewPagedEngine(dataDir string) (*PagedEngine, error) {
	return NewPagedEngineWithWAL(dataDir, false)
}

// NewPagedEngineWithWAL creates a paged storage engine with optional WAL
func NewPagedEngineWithWAL(dataDir string, enableWAL bool) (*PagedEngine, error) {
	// Create transaction manager
	txnManager := NewTransactionManager()
	
	// Create system catalog
	catalog := NewSystemCatalog(dataDir)
	
	engine := &PagedEngine{
		databases:     make(map[string]*PagedDatabase),
		dataDir:       dataDir,
		txnManager:    txnManager,
		vacuumManager: NewVacuumManager(txnManager),
		catalog:       catalog,
	}
	
	// Load transaction state
	if err := txnManager.LoadState(dataDir); err != nil {
		return nil, fmt.Errorf("failed to load transaction state: %v", err)
	}
	
	// Load catalog
	if err := catalog.LoadCatalog(); err != nil {
		return nil, fmt.Errorf("failed to load catalog: %v", err)
	}
	
	// Initialize WAL if enabled
	if enableWAL {
		walDir := filepath.Join(dataDir, "wal")
		wm, err := NewWALManager(walDir)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize WAL: %v", err)
		}
		engine.walManager = wm
		engine.recoveryMgr = NewRecoveryManager(wm)
		
		// Run recovery
		if err := engine.recoveryMgr.Recover(); err != nil {
			return nil, fmt.Errorf("recovery failed: %v", err)
		}
		
		// Update transaction manager with max transaction ID from WAL
		maxTxnID := engine.recoveryMgr.GetMaxTxnID()
		if maxTxnID > 0 {
			// Ensure nextTxnID is at least maxTxnID + 1
			txnManager.UpdateNextTxnID(maxTxnID + 1)
		}
	}
	
	// Load existing databases
	if err := engine.loadDatabases(); err != nil {
		return nil, err
	}
	
	return engine, nil
}

// loadDatabases loads existing databases from disk using the catalog
func (e *PagedEngine) loadDatabases() error {
	// Get all databases from catalog
	dbNames := e.catalog.ListDatabases()
	
	for _, dbName := range dbNames {
		dbMeta, err := e.catalog.GetDatabase(dbName)
		if err != nil {
			continue
		}
		
		// Create database structure
		db := &PagedDatabase{
			Name:   dbMeta.Name,
			Tables: make(map[string]*PagedTable),
		}
		
		// Load all tables
		for tableName, tableMeta := range dbMeta.Tables {
			// Open heap file
			heapFile, err := OpenHeapFile(tableMeta.HeapFile)
			if err != nil {
				// If heap file doesn't exist, it's a catalog inconsistency
				// Skip this table (in production, might want to log or repair)
				continue
			}
			
			// Create table structure
			table := &PagedTable{
				Name:     tableName,
				Columns:  tableMeta.Columns,
				HeapFile: heapFile,
				Indexes:  make(map[string]*BTree),
				TupleIDs: make([]TupleID, 0),
			}
			
			// Load indexes from disk
			indexDir := filepath.Join(e.dataDir, dbName, "indexes")
			for _, col := range tableMeta.Columns {
				indexPath := filepath.Join(indexDir, tableName+"_"+col.Name+".idx")
				if btree, err := LoadBTreeFromFile(indexPath); err == nil && btree != nil {
					table.Indexes[col.Name] = btree
				}
			}
			
			// Scan heap file to rebuild TupleIDs
			heapFile.Scan(func(tid TupleID, data []byte) error {
				table.TupleIDs = append(table.TupleIDs, tid)
				return nil
			})
			
			db.Tables[tableName] = table
		}
		
		e.databases[dbName] = db
	}
	
	return nil
}

// CreateDatabase creates a new database
func (e *PagedEngine) CreateDatabase(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	if _, exists := e.databases[name]; exists {
		return fmt.Errorf("database '%s' already exists", name)
	}
	
	// Add to catalog
	if err := e.catalog.CreateDatabase(name); err != nil {
		return err
	}
	
	e.databases[name] = &PagedDatabase{
		Name:   name,
		Tables: make(map[string]*PagedTable),
	}
	
	e.currentDB = name
	
	// Save catalog
	if err := e.catalog.SaveCatalog(); err != nil {
		return fmt.Errorf("failed to save catalog: %v", err)
	}
	
	return nil
}

// UseDatabase switches to a database
func (e *PagedEngine) UseDatabase(name string) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	
	if _, exists := e.databases[name]; !exists {
		return fmt.Errorf("database '%s' does not exist", name)
	}
	
	e.currentDB = name
	return nil
}

// getCurrentDatabase returns the current database
func (e *PagedEngine) getCurrentDatabase() (*PagedDatabase, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	
	if e.currentDB == "" {
		return nil, fmt.Errorf("no database selected")
	}
	
	db, exists := e.databases[e.currentDB]
	if !exists {
		return nil, fmt.Errorf("database '%s' not found", e.currentDB)
	}
	
	return db, nil
}

// CreateTable creates a new table with paged storage
func (e *PagedEngine) CreateTable(tableName string, columns []Column) error {
	db, err := e.getCurrentDatabase()
	if err != nil {
		return err
	}
	
	db.mu.Lock()
	defer db.mu.Unlock()
	
	if _, exists := db.Tables[tableName]; exists {
		return fmt.Errorf("table '%s' already exists", tableName)
	}
	
	// Create heap file
	heapFilePath := filepath.Join(e.dataDir, db.Name)
	heapFile, err := NewHeapFile(heapFilePath, tableName)
	if err != nil {
		return fmt.Errorf("failed to create heap file: %v", err)
	}
	
	// Add to catalog
	if err := e.catalog.CreateTable(db.Name, tableName, columns, heapFile.filePath); err != nil {
		heapFile.Close()
		return err
	}
	
	table := &PagedTable{
		Name:     tableName,
		Columns:  columns,
		HeapFile: heapFile,
		Indexes:  make(map[string]*BTree),
		TupleIDs: make([]TupleID, 0),
	}
	
	// Create indexes for PRIMARY KEY and UNIQUE columns
	validator := NewConstraintValidator()
	for _, col := range columns {
		if col.PrimaryKey || col.Unique {
			if err := validator.CreateIndexForConstraint(table, col.Name); err != nil {
				// Log warning but continue
				fmt.Printf("Warning: failed to create index for %s: %v\n", col.Name, err)
			}
		}
	}
	
	db.Tables[tableName] = table
	
	// Save catalog
	if err := e.catalog.SaveCatalog(); err != nil {
		return fmt.Errorf("failed to save catalog: %v", err)
	}
	
	return nil
}

// DropTable drops a table
func (e *PagedEngine) DropTable(tableName string) error {
	db, err := e.getCurrentDatabase()
	if err != nil {
		return err
	}
	
	db.mu.Lock()
	defer db.mu.Unlock()
	
	table, exists := db.Tables[tableName]
	if !exists {
		return fmt.Errorf("table '%s' does not exist", tableName)
	}
	
	// Remove from catalog
	if err := e.catalog.DropTable(db.Name, tableName); err != nil {
		return err
	}
	
	// Close and delete heap file
	if err := table.HeapFile.Delete(); err != nil {
		return fmt.Errorf("failed to delete heap file: %v", err)
	}
	
	delete(db.Tables, tableName)
	
	// Save catalog
	if err := e.catalog.SaveCatalog(); err != nil {
		return fmt.Errorf("failed to save catalog: %v", err)
	}
	
	return nil
}

// InsertRow inserts a row into a table
func (e *PagedEngine) InsertRow(tableName string, row Row) error {
	db, err := e.getCurrentDatabase()
	if err != nil {
		return err
	}
	
	db.mu.RLock()
	table, exists := db.Tables[tableName]
	db.mu.RUnlock()
	
	if !exists {
		return fmt.Errorf("table '%s' does not exist", tableName)
	}
	
	table.mu.Lock()
	defer table.mu.Unlock()
	
	// Validate constraints
	validator := NewConstraintValidator()
	if err := validator.ValidateInsert(table, row); err != nil {
		return err
	}
	
	// Validate foreign keys
	if err := validator.ValidateForeignKeys(table, row, e); err != nil {
		return err
	}
	
	// Get transaction ID
	txnID := e.getNextTxnID()
	
	// Serialize tuple with MVCC fields
	tupleData, err := SerializeTupleWithHeader(row, table.Columns, txnID, InvalidTxnID)
	if err != nil {
		return fmt.Errorf("failed to serialize tuple: %v", err)
	}
	
	// Write WAL record if enabled
	if e.walManager != nil {
		walData := e.serializeInsertWAL(0, 0, tupleData) // PageID/SlotNum set after insert
		if _, err := e.walManager.AppendRecord(txnID, WALRecordInsert, walData); err != nil {
			return fmt.Errorf("failed to write WAL: %v", err)
		}
		// Sync WAL for durability
		if err := e.walManager.Sync(); err != nil {
			return fmt.Errorf("failed to sync WAL: %v", err)
		}
	}
	
	// Insert into heap file
	tid, err := table.HeapFile.InsertTuple(tupleData)
	if err != nil {
		return fmt.Errorf("failed to insert tuple: %v", err)
	}
	
	// Track tuple ID
	table.TupleIDs = append(table.TupleIDs, tid)
	
	// Update indexes
	for colName, btree := range table.Indexes {
		if value, exists := row[colName]; exists && value != nil {
			btree.Insert(value, tid)
		}
	}
	
	return nil
}

// getNextTxnID returns the next transaction ID
func (e *PagedEngine) getNextTxnID() uint32 {
	// Use transaction manager to get transaction ID
	if e.currentTxn != nil {
		return e.currentTxn.ID
	}
	// Auto-transaction mode: create implicit transaction and commit immediately
	txn, _ := e.txnManager.BeginTransaction()
	e.txnManager.CommitTransaction(txn.ID) // Auto-commit
	return txn.ID
}

// serializeInsertWAL serializes insert data for WAL
func (e *PagedEngine) serializeInsertWAL(pageID PageID, slotNum uint16, tupleData []byte) []byte {
	buf := make([]byte, 6+len(tupleData))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(pageID))
	binary.LittleEndian.PutUint16(buf[4:6], slotNum)
	copy(buf[6:], tupleData)
	return buf
}

// ExecuteQuery executes a complete SQL query with all features
func (e *PagedEngine) ExecuteQuery(stmt *Statement) ([]Row, error) {
	var rows []Row
	var err error
	
	// Step 1: Get base table rows
	// If there are JOINs, don't apply WHERE conditions yet (they might reference joined tables)
	if len(stmt.Joins) > 0 {
		rows, err = e.SelectRows(stmt.Table, nil)
	} else {
		rows, err = e.SelectRows(stmt.Table, stmt.Conditions)
	}
	if err != nil {
		return nil, err
	}
	
	// Step 2: Execute JOINs (if any)
	if len(stmt.Joins) > 0 {
		joinExecutor := NewJoinExecutor()
		for _, join := range stmt.Joins {
			// Get rows from the joined table
			rightRows, err := e.SelectRows(join.Table, nil)
			if err != nil {
				return nil, err
			}
			
			// Execute the join
			rows, err = joinExecutor.ExecuteJoin(rows, rightRows, join)
			if err != nil {
				return nil, err
			}
		}
		
		// Apply WHERE conditions after JOIN
		if len(stmt.Conditions) > 0 {
			filteredRows := make([]Row, 0)
			for _, row := range rows {
				if matchesConditions(row, stmt.Conditions) {
					filteredRows = append(filteredRows, row)
				}
			}
			rows = filteredRows
		}
	}
	
	// Step 3: Execute aggregates (if any)
	if len(stmt.Aggregates) > 0 {
		aggExecutor := NewAggregateExecutor()
		rows, err = aggExecutor.ExecuteAggregates(rows, stmt.Aggregates, stmt.GroupBy)
		if err != nil {
			return nil, err
		}
	}
	
	// Step 4: Apply ORDER BY (if any)
	if stmt.OrderBy != "" {
		rows = e.applyOrderBy(rows, stmt.OrderBy, stmt.OrderDesc)
	}
	
	// Step 5: Apply LIMIT/OFFSET (if any)
	// Note: Limit of 0 means return no rows, so we check >= 0 but need to handle it
	if stmt.Limit > 0 || (stmt.Limit == 0 && stmt.Offset > 0) {
		rows = e.applyLimit(rows, stmt.Limit, stmt.Offset)
	}
	
	return rows, nil
}

// SelectRows selects rows from a table
func (e *PagedEngine) SelectRows(tableName string, conditions []Condition) ([]Row, error) {
	db, err := e.getCurrentDatabase()
	if err != nil {
		return nil, err
	}
	
	db.mu.RLock()
	table, exists := db.Tables[tableName]
	db.mu.RUnlock()
	
	if !exists {
		return nil, fmt.Errorf("table '%s' does not exist", tableName)
	}
	
	table.mu.RLock()
	defer table.mu.RUnlock()
	
	var results []Row
	
	// Get snapshot for visibility checks
	var snapshot *Snapshot
	if e.currentTxn != nil {
		snapshot = e.currentTxn.Snapshot
	} else {
		// Create ad-hoc snapshot for this query
		e.mu.RLock()
		txn, _ := e.txnManager.BeginTransaction()
		snapshot = txn.Snapshot
		e.mu.RUnlock()
	}
	
	// Scan heap file
	err = table.HeapFile.Scan(func(tid TupleID, data []byte) error {
		// Deserialize tuple
		tuple, err := DeserializeTuple(data)
		if err != nil {
			return err
		}
		
		// Check MVCC visibility
		if !e.txnManager.IsVisible(tuple, snapshot) {
			return nil // Skip invisible tuples
		}
		
		// Check conditions
		if matchesConditions(tuple.Data, conditions) {
			results = append(results, tuple.Data)
		}
		
		return nil
	})
	
	if err != nil {
		return nil, fmt.Errorf("scan failed: %v", err)
	}
	
	return results, nil
}

// applyOrderBy sorts rows based on ORDER BY column
func (e *PagedEngine) applyOrderBy(rows []Row, orderByCol string, descending bool) []Row {
	if len(rows) == 0 || orderByCol == "" {
		return rows
	}
	
	// Create a copy to avoid modifying the original
	result := make([]Row, len(rows))
	copy(result, rows)
	
	// Simple bubble sort for now (can be optimized later)
	for i := 0; i < len(result)-1; i++ {
		for j := 0; j < len(result)-i-1; j++ {
			val1, ok1 := result[j][orderByCol]
			val2, ok2 := result[j+1][orderByCol]
			
			if !ok1 || !ok2 {
				continue
			}
			
			// Compare values
			cmp := CompareValues(val1, val2)
			
			// Swap if needed based on sort direction
			shouldSwap := false
			if descending {
				shouldSwap = cmp < 0
			} else {
				shouldSwap = cmp > 0
			}
			
			if shouldSwap {
				result[j], result[j+1] = result[j+1], result[j]
			}
		}
	}
	
	return result
}

// applyLimit applies LIMIT and OFFSET to rows
func (e *PagedEngine) applyLimit(rows []Row, limit int, offset int) []Row {
	if len(rows) == 0 {
		return rows
	}
	
	if limit <= 0 {
		return []Row{}
	}
	
	start := 0
	if offset > 0 {
		start = offset
		if start >= len(rows) {
			return []Row{}
		}
	}
	
	end := start + limit
	if end > len(rows) {
		end = len(rows)
	}
	
	return rows[start:end]
}

// UpdateRows updates rows in a table
func (e *PagedEngine) UpdateRows(tableName string, updates map[string]interface{}, conditions []Condition) (int, error) {
	db, err := e.getCurrentDatabase()
	if err != nil {
		return 0, err
	}
	
	db.mu.RLock()
	table, exists := db.Tables[tableName]
	db.mu.RUnlock()
	
	if !exists {
		return 0, fmt.Errorf("table '%s' does not exist", tableName)
	}
	
	table.mu.Lock()
	defer table.mu.Unlock()
	
	// Get snapshot for visibility checks
	var snapshot *Snapshot
	if e.currentTxn != nil {
		snapshot = e.currentTxn.Snapshot
	} else {
		txn, _ := e.txnManager.BeginTransaction()
		snapshot = txn.Snapshot
	}
	
	count := 0
	
	// Scan and update matching tuples
	for _, tid := range table.TupleIDs {
		tupleData, err := table.HeapFile.GetTuple(tid)
		if err != nil {
			continue
		}
		
		tuple, err := DeserializeTuple(tupleData)
		if err != nil {
			continue
		}
		
		// Check MVCC visibility
		if !e.txnManager.IsVisible(tuple, snapshot) {
			continue
		}
		
		if matchesConditions(tuple.Data, conditions) {
			// Apply updates
			for col, val := range updates {
				tuple.Data[col] = val
			}
			
			// Serialize updated tuple (preserve Xmin/Xmax)
			newData, err := SerializeTupleWithHeader(tuple.Data, table.Columns, tuple.Header.Xmin, tuple.Header.Xmax)
			if err != nil {
				continue
			}
			
			// Try in-place update
			if err := table.HeapFile.UpdateTuple(tid, newData); err != nil {
				// If in-place update fails, delete and re-insert
				table.HeapFile.DeleteTuple(tid)
				newTid, err := table.HeapFile.InsertTuple(newData)
				if err != nil {
					continue
				}
				
				// Update TupleID tracking
				for i, t := range table.TupleIDs {
					if t == tid {
						table.TupleIDs[i] = newTid
						break
					}
				}
			}
			
			count++
		}
	}
	
	return count, nil
}

// DeleteRows deletes rows from a table
func (e *PagedEngine) DeleteRows(tableName string, conditions []Condition) (int, error) {
	db, err := e.getCurrentDatabase()
	if err != nil {
		return 0, err
	}
	
	db.mu.RLock()
	table, exists := db.Tables[tableName]
	db.mu.RUnlock()
	
	if !exists {
		return 0, fmt.Errorf("table '%s' does not exist", tableName)
	}
	
	table.mu.Lock()
	defer table.mu.Unlock()
	
	// Get transaction ID
	txnID := e.getNextTxnID()
	
	count := 0
	
	// Scan and mark matching tuples as deleted (MVCC delete)
	for _, tid := range table.TupleIDs {
		tupleData, err := table.HeapFile.GetTuple(tid)
		if err != nil {
			continue
		}
		
		tuple, err := DeserializeTuple(tupleData)
		if err != nil {
			continue
		}
		
		// Check visibility
		var snapshot *Snapshot
		if e.currentTxn != nil {
			snapshot = e.currentTxn.Snapshot
		} else {
			txn, _ := e.txnManager.BeginTransaction()
			snapshot = txn.Snapshot
		}
		
		if !e.txnManager.IsVisible(tuple, snapshot) {
			continue
		}
		
		if matchesConditions(tuple.Data, conditions) {
			// MVCC delete: set Xmax instead of physical delete
			newData, err := SerializeTupleWithHeader(tuple.Data, table.Columns, tuple.Header.Xmin, txnID)
			if err != nil {
				continue
			}
			
			// Update tuple in place
			if err := table.HeapFile.UpdateTuple(tid, newData); err != nil {
				continue
			}
			
			count++
		}
	}
	
	return count, nil
}

// matchesConditions checks if a row matches conditions
func matchesConditions(row Row, conditions []Condition) bool {
	if len(conditions) == 0 {
		return true
	}
	
	for _, cond := range conditions {
		val, exists := row[cond.Column]
		if !exists {
			return false
		}
		
		switch cond.Operator {
		case "=":
			if !valuesEqual(val, cond.Value) {
				return false
			}
		case "!=":
			if valuesEqual(val, cond.Value) {
				return false
			}
		case ">":
			if CompareValues(val, cond.Value) <= 0 {
				return false
			}
		case "<":
			if CompareValues(val, cond.Value) >= 0 {
				return false
			}
		case ">=":
			if CompareValues(val, cond.Value) < 0 {
				return false
			}
		case "<=":
			if CompareValues(val, cond.Value) > 0 {
				return false
			}
		}
	}
	
	return true
}

// valuesEqual checks if two values are equal (handles type conversions)
func valuesEqual(a, b interface{}) bool {
	return CompareValues(a, b) == 0
}

// BeginTransaction starts a new transaction
func (e *PagedEngine) BeginTransaction() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.currentTxn != nil {
		return fmt.Errorf("transaction already active")
	}

	txn, err := e.txnManager.BeginTransaction()
	if err != nil {
		return err
	}

	e.currentTxn = txn
	return nil
}

// CommitTransaction commits the current transaction
func (e *PagedEngine) CommitTransaction() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.currentTxn == nil {
		return fmt.Errorf("no active transaction")
	}

	if err := e.txnManager.CommitTransaction(e.currentTxn.ID); err != nil {
		return err
	}

	// Write commit record to WAL
	if e.walManager != nil {
		if _, err := e.walManager.AppendRecord(e.currentTxn.ID, WALRecordCommit, []byte{}); err != nil {
			return fmt.Errorf("failed to write commit record: %v", err)
		}
		e.walManager.Sync()
	}

	e.currentTxn = nil
	return nil
}

// RollbackTransaction aborts the current transaction
func (e *PagedEngine) RollbackTransaction() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.currentTxn == nil {
		return fmt.Errorf("no active transaction")
	}

	if err := e.txnManager.AbortTransaction(e.currentTxn.ID); err != nil {
		return err
	}

	// Write abort record to WAL
	if e.walManager != nil {
		if _, err := e.walManager.AppendRecord(e.currentTxn.ID, WALRecordAbort, []byte{}); err != nil {
			return fmt.Errorf("failed to write abort record: %v", err)
		}
		e.walManager.Sync()
	}

	e.currentTxn = nil
	return nil
}

// VacuumTable performs vacuum on a table
func (e *PagedEngine) VacuumTable(tableName string) (*VacuumStats, error) {
	db, err := e.getCurrentDatabase()
	if err != nil {
		return nil, err
	}

	db.mu.RLock()
	table, exists := db.Tables[tableName]
	db.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("table '%s' does not exist", tableName)
	}

	table.mu.Lock()
	defer table.mu.Unlock()

	return e.vacuumManager.VacuumTable(table.HeapFile, table.Columns)
}

// VacuumDatabase performs vacuum on all tables
func (e *PagedEngine) VacuumDatabase() (map[string]*VacuumStats, error) {
	db, err := e.getCurrentDatabase()
	if err != nil {
		return nil, err
	}

	return e.vacuumManager.VacuumDatabase(db)
}

// Close closes all heap files and WAL
func (e *PagedEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	for dbName, db := range e.databases {
		db.mu.Lock()
		for tableName, table := range db.Tables {
			// Save indexes
			if len(table.Indexes) > 0 {
				indexDir := filepath.Join(e.dataDir, dbName, "indexes")
				os.MkdirAll(indexDir, 0755)
				
				for colName, btree := range table.Indexes {
					indexPath := filepath.Join(indexDir, tableName+"_"+colName+".idx")
					if err := btree.SaveToFile(indexPath); err != nil {
						// Log error but continue
						fmt.Printf("Warning: failed to save index %s: %v\n", indexPath, err)
					}
				}
			}
			
			// Close heap file
			if table.HeapFile != nil {
				table.HeapFile.Close()
			}
		}
		db.mu.Unlock()
	}
	
	// Save catalog
	if err := e.catalog.SaveCatalog(); err != nil {
		return fmt.Errorf("failed to save catalog: %v", err)
	}
	
	// Save transaction state
	if err := e.txnManager.SaveState(e.dataDir); err != nil {
		return fmt.Errorf("failed to save transaction state: %v", err)
	}
	
	// Close WAL
	if e.walManager != nil {
		return e.walManager.Close()
	}
	
	return nil
}
