package mindb

import (
	"sync"
)

// FreeSpaceMap tracks free space available in each page
type FreeSpaceMap struct {
	pageSpace map[PageID]uint16 // PageID -> free bytes
	mu        sync.RWMutex
}

// NewFreeSpaceMap creates a new free space map
func NewFreeSpaceMap() *FreeSpaceMap {
	return &FreeSpaceMap{
		pageSpace: make(map[PageID]uint16),
	}
}

// UpdateFreeSpace updates the free space for a page
func (fsm *FreeSpaceMap) UpdateFreeSpace(pageID PageID, freeBytes uint16) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	fsm.pageSpace[pageID] = freeBytes
}

// GetFreeSpace returns the free space for a page
func (fsm *FreeSpaceMap) GetFreeSpace(pageID PageID) uint16 {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	return fsm.pageSpace[pageID]
}

// FindPageWithSpace finds a page with at least the specified free space
// Returns InvalidPageID if no suitable page found
func (fsm *FreeSpaceMap) FindPageWithSpace(needed uint16) PageID {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	for pageID, freeBytes := range fsm.pageSpace {
		if freeBytes >= needed {
			return pageID
		}
	}

	return InvalidPageID
}

// RemovePage removes a page from the FSM
func (fsm *FreeSpaceMap) RemovePage(pageID PageID) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	delete(fsm.pageSpace, pageID)
}

// GetPageCount returns the number of pages tracked
func (fsm *FreeSpaceMap) GetPageCount() int {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	return len(fsm.pageSpace)
}

// Clear clears all entries from the FSM
func (fsm *FreeSpaceMap) Clear() {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	fsm.pageSpace = make(map[PageID]uint16)
}
