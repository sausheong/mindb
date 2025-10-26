package mindb

import (
	"container/list"
	"fmt"
	"sync"
)

// BufferFrame represents a cached page in the buffer pool
type BufferFrame struct {
	PageID    PageID
	Page      *Page
	PinCount  int
	IsDirty   bool
	HeapFile  *HeapFile
	LastUsed  int64 // For LRU
}

// BufferPool manages a cache of pages in memory
type BufferPool struct {
	frames     map[PageID]*BufferFrame
	lruList    *list.List
	lruMap     map[PageID]*list.Element
	maxFrames  int
	clockHand  int
	mu         sync.RWMutex
	hits       uint64
	misses     uint64
}

// NewBufferPool creates a new buffer pool
func NewBufferPool(maxFrames int) *BufferPool {
	if maxFrames <= 0 {
		maxFrames = 128 // Default: 128 pages = 1MB cache
	}

	return &BufferPool{
		frames:    make(map[PageID]*BufferFrame),
		lruList:   list.New(),
		lruMap:    make(map[PageID]*list.Element),
		maxFrames: maxFrames,
	}
}

// GetPage retrieves a page from the buffer pool or loads it from disk
func (bp *BufferPool) GetPage(heapFile *HeapFile, pageID PageID) (*Page, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	// Check if page is in buffer pool
	if frame, exists := bp.frames[pageID]; exists {
		// Cache hit
		bp.hits++
		frame.PinCount++
		
		// Move to front of LRU list
		if elem, ok := bp.lruMap[pageID]; ok {
			bp.lruList.MoveToFront(elem)
		}
		
		return frame.Page, nil
	}

	// Cache miss - need to load from disk
	bp.misses++

	// Check if buffer pool is full
	if len(bp.frames) >= bp.maxFrames {
		// Evict a page
		if err := bp.evictPage(); err != nil {
			return nil, fmt.Errorf("failed to evict page: %v", err)
		}
	}

	// Load page from disk
	page, err := heapFile.readPageDirect(pageID)
	if err != nil {
		return nil, fmt.Errorf("failed to load page from disk: %v", err)
	}

	// Add to buffer pool
	frame := &BufferFrame{
		PageID:   pageID,
		Page:     page,
		PinCount: 1,
		IsDirty:  false,
		HeapFile: heapFile,
	}

	bp.frames[pageID] = frame
	
	// Add to LRU list
	elem := bp.lruList.PushFront(pageID)
	bp.lruMap[pageID] = elem

	return page, nil
}

// UnpinPage decrements the pin count for a page
func (bp *BufferPool) UnpinPage(pageID PageID, isDirty bool) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	frame, exists := bp.frames[pageID]
	if !exists {
		return fmt.Errorf("page %d not in buffer pool", pageID)
	}

	if frame.PinCount > 0 {
		frame.PinCount--
	}

	if isDirty {
		frame.IsDirty = true
	}

	return nil
}

// FlushPage writes a dirty page back to disk
func (bp *BufferPool) FlushPage(pageID PageID) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	frame, exists := bp.frames[pageID]
	if !exists {
		return fmt.Errorf("page %d not in buffer pool", pageID)
	}

	if !frame.IsDirty {
		return nil // Page is clean, no need to flush
	}

	// Write page to disk
	if err := frame.HeapFile.writePageUnsafe(frame.Page); err != nil {
		return fmt.Errorf("failed to write page to disk: %v", err)
	}

	frame.IsDirty = false
	return nil
}

// FlushAllPages writes all dirty pages to disk
func (bp *BufferPool) FlushAllPages() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	for pageID, frame := range bp.frames {
		if frame.IsDirty {
			if err := frame.HeapFile.writePageUnsafe(frame.Page); err != nil {
				return fmt.Errorf("failed to flush page %d: %v", pageID, err)
			}
			frame.IsDirty = false
		}
	}

	return nil
}

// evictPage evicts a page using LRU policy
func (bp *BufferPool) evictPage() error {
	// Find unpinned page from back of LRU list
	for elem := bp.lruList.Back(); elem != nil; elem = elem.Prev() {
		pageID := elem.Value.(PageID)
		frame := bp.frames[pageID]

		// Skip pinned pages
		if frame.PinCount > 0 {
			continue
		}

		// Flush if dirty
		if frame.IsDirty {
			if err := frame.HeapFile.writePageUnsafe(frame.Page); err != nil {
				return fmt.Errorf("failed to flush page during eviction: %v", err)
			}
		}

		// Remove from buffer pool
		delete(bp.frames, pageID)
		bp.lruList.Remove(elem)
		delete(bp.lruMap, pageID)

		return nil
	}

	return fmt.Errorf("no unpinned pages available for eviction")
}

// EvictAll removes all pages from the buffer pool (flushes dirty pages first)
func (bp *BufferPool) EvictAll() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	// Flush all dirty pages
	for _, frame := range bp.frames {
		if frame.IsDirty {
			if err := frame.HeapFile.writePageUnsafe(frame.Page); err != nil {
				return fmt.Errorf("failed to flush page: %v", err)
			}
		}
	}

	// Clear buffer pool
	bp.frames = make(map[PageID]*BufferFrame)
	bp.lruList = list.New()
	bp.lruMap = make(map[PageID]*list.Element)

	return nil
}

// GetStats returns buffer pool statistics
func (bp *BufferPool) GetStats() (hits, misses uint64, hitRate float64) {
	bp.mu.RLock()
	defer bp.mu.RUnlock()

	hits = bp.hits
	misses = bp.misses
	total := hits + misses

	if total > 0 {
		hitRate = float64(hits) / float64(total)
	}

	return hits, misses, hitRate
}

// GetSize returns the current number of pages in the buffer pool
func (bp *BufferPool) GetSize() int {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return len(bp.frames)
}

// GetDirtyPageCount returns the number of dirty pages
func (bp *BufferPool) GetDirtyPageCount() int {
	bp.mu.RLock()
	defer bp.mu.RUnlock()

	count := 0
	for _, frame := range bp.frames {
		if frame.IsDirty {
			count++
		}
	}
	return count
}
