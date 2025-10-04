package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// TupleID uniquely identifies a tuple within a table
type TupleID struct {
	PageID  PageID
	SlotNum uint16
}

// HeapFile represents a table stored as a collection of pages
type HeapFile struct {
	tableName  string
	filePath   string
	file       *os.File
	pageCount  uint32
	bufferPool *BufferPool
	fsm        *FreeSpaceMap
	mu         sync.RWMutex
}

// NewHeapFile creates or opens a heap file for a table
func NewHeapFile(dataDir, tableName string) (*HeapFile, error) {
	// Ensure data directory exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %v", err)
	}
	
	filePath := filepath.Join(dataDir, tableName+".heap")
	
	// Open or create file
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open heap file: %v", err)
	}
	
	// Get file size to determine page count
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat heap file: %v", err)
	}
	
	pageCount := uint32(fileInfo.Size() / PageSize)
	
	hf := &HeapFile{
		tableName:  tableName,
		filePath:   filePath,
		file:       file,
		pageCount:  pageCount,
		bufferPool: NewBufferPool(128), // 128 pages = 1MB cache
		fsm:        NewFreeSpaceMap(),
	}
	
	// Initialize FSM by scanning existing pages
	if pageCount > 0 {
		hf.initializeFSM()
	}
	
	return hf, nil
}

// OpenHeapFile opens an existing heap file by path
func OpenHeapFile(filePath string) (*HeapFile, error) {
	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("heap file does not exist: %s", filePath)
	}
	
	// Open file
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open heap file: %v", err)
	}
	
	// Get file size to determine page count
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat heap file: %v", err)
	}
	
	pageCount := uint32(fileInfo.Size() / PageSize)
	
	// Extract table name from file path
	tableName := filepath.Base(filePath)
	if len(tableName) > 5 && tableName[len(tableName)-5:] == ".heap" {
		tableName = tableName[:len(tableName)-5]
	}
	
	hf := &HeapFile{
		tableName:  tableName,
		filePath:   filePath,
		file:       file,
		pageCount:  pageCount,
		bufferPool: NewBufferPool(128), // 128 pages = 1MB cache
		fsm:        NewFreeSpaceMap(),
	}
	
	// Initialize FSM by scanning existing pages
	if pageCount > 0 {
		hf.initializeFSM()
	}
	
	return hf, nil
}

// initializeFSM scans existing pages to build the free space map
func (hf *HeapFile) initializeFSM() {
	// Read pages without locks (called during initialization)
	for pageID := PageID(0); pageID < PageID(hf.pageCount); pageID++ {
		// Read page data directly
		data := make([]byte, PageSize)
		offset := int64(pageID) * PageSize
		
		n, err := hf.file.ReadAt(data, offset)
		if err != nil || n != PageSize {
			continue
		}
		
		// Load page
		page, err := LoadPageFromBytes(data)
		if err == nil {
			hf.fsm.UpdateFreeSpace(pageID, page.GetFreeSpace())
		}
	}
}

// Close closes the heap file
func (hf *HeapFile) Close() error {
	// Flush buffer pool first (before acquiring lock)
	if hf.bufferPool != nil {
		if err := hf.bufferPool.FlushAllPages(); err != nil {
			return fmt.Errorf("failed to flush buffer pool: %v", err)
		}
	}
	
	hf.mu.Lock()
	defer hf.mu.Unlock()
	
	if hf.file != nil {
		return hf.file.Close()
	}
	return nil
}

// AllocatePage allocates a new page and returns its ID
func (hf *HeapFile) AllocatePage() (PageID, error) {
	hf.mu.Lock()
	defer hf.mu.Unlock()
	
	pageID := PageID(hf.pageCount)
	hf.pageCount++
	
	// Create new page
	page := NewPage(pageID)
	page.UpdateChecksum()
	
	// Write page to file
	offset := int64(pageID) * PageSize
	if _, err := hf.file.WriteAt(page.Data, offset); err != nil {
		return 0, fmt.Errorf("failed to write page: %v", err)
	}
	
	// Sync to disk
	if err := hf.file.Sync(); err != nil {
		return 0, fmt.Errorf("failed to sync page: %v", err)
	}
	
	return pageID, nil
}

// readPageDirect reads a page directly from disk (bypasses buffer pool)
func (hf *HeapFile) readPageDirect(pageID PageID) (*Page, error) {
	hf.mu.RLock()
	defer hf.mu.RUnlock()
	
	if pageID >= PageID(hf.pageCount) {
		return nil, fmt.Errorf("invalid page ID: %d", pageID)
	}
	
	// Read page data
	data := make([]byte, PageSize)
	offset := int64(pageID) * PageSize
	
	n, err := hf.file.ReadAt(data, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read page: %v", err)
	}
	
	if n != PageSize {
		return nil, fmt.Errorf("incomplete page read: %d bytes", n)
	}
	
	// Load page
	page, err := LoadPageFromBytes(data)
	if err != nil {
		return nil, fmt.Errorf("failed to load page: %v", err)
	}
	
	return page, nil
}

// writePageDirect writes a page directly to disk (bypasses buffer pool)
func (hf *HeapFile) writePageDirect(page *Page) error {
	hf.mu.Lock()
	defer hf.mu.Unlock()
	return hf.writePageUnsafe(page)
}

// writePageUnsafe writes a page without acquiring locks (internal use only)
func (hf *HeapFile) writePageUnsafe(page *Page) error {
	if page.Header.PageID >= PageID(hf.pageCount) {
		return fmt.Errorf("invalid page ID: %d", page.Header.PageID)
	}
	
	// Update checksum before writing
	page.UpdateChecksum()
	
	// Write page data
	offset := int64(page.Header.PageID) * PageSize
	
	if _, err := hf.file.WriteAt(page.Data, offset); err != nil {
		return fmt.Errorf("failed to write page: %v", err)
	}
	
	// Clear dirty flag
	page.ClearDirty()
	
	return hf.file.Sync()
}

// ReadPage reads a page using the buffer pool
func (hf *HeapFile) ReadPage(pageID PageID) (*Page, error) {
	if hf.bufferPool == nil {
		return hf.readPageDirect(pageID)
	}
	return hf.bufferPool.GetPage(hf, pageID)
}

// WritePage writes a page using the buffer pool
func (hf *HeapFile) WritePage(page *Page) error {
	if hf.bufferPool == nil {
		return hf.writePageDirect(page)
	}
	
	// Mark page as dirty in buffer pool
	if err := hf.bufferPool.UnpinPage(page.Header.PageID, true); err != nil {
		// Page not in buffer pool, write directly
		return hf.writePageDirect(page)
	}
	
	// Update FSM
	hf.fsm.UpdateFreeSpace(page.Header.PageID, page.GetFreeSpace())
	
	return nil
}

// SyncPage writes a page and syncs to disk
func (hf *HeapFile) SyncPage(page *Page) error {
	if err := hf.WritePage(page); err != nil {
		return err
	}
	
	hf.mu.Lock()
	defer hf.mu.Unlock()
	
	return hf.file.Sync()
}

// InsertTuple inserts a tuple and returns its TupleID
func (hf *HeapFile) InsertTuple(tupleData []byte) (TupleID, error) {
	tupleSize := uint16(len(tupleData))
	
	// Use FSM to find a page with enough space
	pageID := hf.fsm.FindPageWithSpace(tupleSize + SlotSize)
	
	if pageID != InvalidPageID {
		// Found a page with space
		page, err := hf.ReadPage(pageID)
		if err == nil && page.CanFit(tupleSize) {
			slotNum, err := page.InsertTuple(tupleData)
			if err == nil {
				// Write page back
				if err := hf.WritePage(page); err != nil {
					return TupleID{}, fmt.Errorf("failed to write page: %v", err)
				}
				
				// Unpin page
				if hf.bufferPool != nil {
					hf.bufferPool.UnpinPage(pageID, false)
				}
				
				return TupleID{PageID: pageID, SlotNum: slotNum}, nil
			}
		}
	}
	
	// FSM didn't find space, try scanning (fallback for FSM inaccuracy)
	for pageID := PageID(0); pageID < PageID(hf.pageCount); pageID++ {
		page, err := hf.ReadPage(pageID)
		if err != nil {
			continue
		}
		
		// Try to insert
		if page.CanFit(tupleSize) {
			slotNum, err := page.InsertTuple(tupleData)
			if err != nil {
				continue
			}
			
			// Write page back
			if err := hf.WritePage(page); err != nil {
				return TupleID{}, fmt.Errorf("failed to write page: %v", err)
			}
			
			// Unpin page
			if hf.bufferPool != nil {
				hf.bufferPool.UnpinPage(pageID, false)
			}
			
			return TupleID{PageID: pageID, SlotNum: slotNum}, nil
		}
	}
	
	// No existing page has space, allocate new page
	pageID, err := hf.AllocatePage()
	if err != nil {
		return TupleID{}, fmt.Errorf("failed to allocate page: %v", err)
	}
	
	// Read the new page
	page, err := hf.ReadPage(pageID)
	if err != nil {
		return TupleID{}, fmt.Errorf("failed to read new page: %v", err)
	}
	
	// Insert tuple
	slotNum, err := page.InsertTuple(tupleData)
	if err != nil {
		return TupleID{}, fmt.Errorf("failed to insert tuple: %v", err)
	}
	
	// Write page back
	if err := hf.WritePage(page); err != nil {
		return TupleID{}, fmt.Errorf("failed to write page: %v", err)
	}
	
	return TupleID{PageID: pageID, SlotNum: slotNum}, nil
}

// GetTuple retrieves a tuple by its TupleID
func (hf *HeapFile) GetTuple(tid TupleID) ([]byte, error) {
	page, err := hf.ReadPage(tid.PageID)
	if err != nil {
		return nil, err
	}
	
	return page.GetTuple(tid.SlotNum)
}

// UpdateTuple updates a tuple in place
func (hf *HeapFile) UpdateTuple(tid TupleID, newData []byte) error {
	page, err := hf.ReadPage(tid.PageID)
	if err != nil {
		return err
	}
	
	if err := page.UpdateTuple(tid.SlotNum, newData); err != nil {
		return err
	}
	
	return hf.WritePage(page)
}

// DeleteTuple marks a tuple as deleted
func (hf *HeapFile) DeleteTuple(tid TupleID) error {
	page, err := hf.ReadPage(tid.PageID)
	if err != nil {
		return err
	}
	
	if err := page.DeleteTuple(tid.SlotNum); err != nil {
		return err
	}
	
	return hf.WritePage(page)
}

// Scan iterates over all tuples in the heap file
func (hf *HeapFile) Scan(callback func(TupleID, []byte) error) error {
	for pageID := PageID(0); pageID < PageID(hf.pageCount); pageID++ {
		page, err := hf.ReadPage(pageID)
		if err != nil {
			return fmt.Errorf("failed to read page %d: %v", pageID, err)
		}
		
		// Iterate over all slots
		for slotNum := uint16(0); slotNum < page.Header.SlotCount; slotNum++ {
			// Skip deleted tuples
			if page.Slots[slotNum].Length == 0 {
				continue
			}
			
			tupleData, err := page.GetTuple(slotNum)
			if err != nil {
				continue
			}
			
			tid := TupleID{PageID: pageID, SlotNum: slotNum}
			if err := callback(tid, tupleData); err != nil {
				return err
			}
		}
	}
	
	return nil
}

// GetPageCount returns the number of pages in the heap file
func (hf *HeapFile) GetPageCount() uint32 {
	hf.mu.RLock()
	defer hf.mu.RUnlock()
	return hf.pageCount
}

// Truncate removes all data from the heap file
func (hf *HeapFile) Truncate() error {
	hf.mu.Lock()
	defer hf.mu.Unlock()
	
	if err := hf.file.Truncate(0); err != nil {
		return fmt.Errorf("failed to truncate file: %v", err)
	}
	
	hf.pageCount = 0
	
	return hf.file.Sync()
}

// Delete removes the heap file from disk
func (hf *HeapFile) Delete() error {
	hf.mu.Lock()
	defer hf.mu.Unlock()
	
	if hf.file != nil {
		hf.file.Close()
		hf.file = nil
	}
	
	return os.Remove(hf.filePath)
}
