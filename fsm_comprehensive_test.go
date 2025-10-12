package mindb

import (
	"testing"
)

// ============================================================================
// FREE SPACE MAP TESTS
// ============================================================================

func TestNewFreeSpaceMap(t *testing.T) {
	fsm := NewFreeSpaceMap()
	if fsm == nil {
		t.Fatal("NewFreeSpaceMap returned nil")
	}
	
	if fsm.pageSpace == nil {
		t.Error("pageSpace map should be initialized")
	}
	
	if fsm.GetPageCount() != 0 {
		t.Error("New FSM should have 0 pages")
	}
}

func TestFSM_UpdateFreeSpace(t *testing.T) {
	fsm := NewFreeSpaceMap()
	
	pageID := PageID(1)
	freeBytes := uint16(1024)
	
	fsm.UpdateFreeSpace(pageID, freeBytes)
	
	retrieved := fsm.GetFreeSpace(pageID)
	if retrieved != freeBytes {
		t.Errorf("Expected %d free bytes, got %d", freeBytes, retrieved)
	}
}

func TestFSM_GetFreeSpace_NonExistent(t *testing.T) {
	fsm := NewFreeSpaceMap()
	
	pageID := PageID(999)
	freeBytes := fsm.GetFreeSpace(pageID)
	
	if freeBytes != 0 {
		t.Errorf("Non-existent page should return 0 free bytes, got %d", freeBytes)
	}
}

func TestFSM_UpdateFreeSpace_Multiple(t *testing.T) {
	fsm := NewFreeSpaceMap()
	
	// Update multiple pages
	fsm.UpdateFreeSpace(PageID(1), 1024)
	fsm.UpdateFreeSpace(PageID(2), 2048)
	fsm.UpdateFreeSpace(PageID(3), 512)
	
	if fsm.GetPageCount() != 3 {
		t.Errorf("Expected 3 pages, got %d", fsm.GetPageCount())
	}
	
	// Verify each page
	if fsm.GetFreeSpace(PageID(1)) != 1024 {
		t.Error("Page 1 free space incorrect")
	}
	if fsm.GetFreeSpace(PageID(2)) != 2048 {
		t.Error("Page 2 free space incorrect")
	}
	if fsm.GetFreeSpace(PageID(3)) != 512 {
		t.Error("Page 3 free space incorrect")
	}
}

func TestFSM_UpdateFreeSpace_Overwrite(t *testing.T) {
	fsm := NewFreeSpaceMap()
	
	pageID := PageID(1)
	
	// Initial update
	fsm.UpdateFreeSpace(pageID, 1024)
	if fsm.GetFreeSpace(pageID) != 1024 {
		t.Error("Initial update failed")
	}
	
	// Overwrite
	fsm.UpdateFreeSpace(pageID, 2048)
	if fsm.GetFreeSpace(pageID) != 2048 {
		t.Error("Overwrite failed")
	}
	
	// Page count should still be 1
	if fsm.GetPageCount() != 1 {
		t.Error("Page count should remain 1 after overwrite")
	}
}

func TestFSM_FindPageWithSpace_Found(t *testing.T) {
	fsm := NewFreeSpaceMap()
	
	fsm.UpdateFreeSpace(PageID(1), 512)
	fsm.UpdateFreeSpace(PageID(2), 1024)
	fsm.UpdateFreeSpace(PageID(3), 2048)
	
	// Find page with at least 1000 bytes
	pageID := fsm.FindPageWithSpace(1000)
	
	if pageID == InvalidPageID {
		t.Error("Should find a page with enough space")
	}
	
	// Verify the found page has enough space
	freeBytes := fsm.GetFreeSpace(pageID)
	if freeBytes < 1000 {
		t.Errorf("Found page has insufficient space: %d", freeBytes)
	}
}

func TestFSM_FindPageWithSpace_NotFound(t *testing.T) {
	fsm := NewFreeSpaceMap()
	
	fsm.UpdateFreeSpace(PageID(1), 512)
	fsm.UpdateFreeSpace(PageID(2), 1024)
	
	// Try to find page with more space than available
	pageID := fsm.FindPageWithSpace(2048)
	
	if pageID != InvalidPageID {
		t.Error("Should not find a page when none have enough space")
	}
}

func TestFSM_FindPageWithSpace_Empty(t *testing.T) {
	fsm := NewFreeSpaceMap()
	
	pageID := fsm.FindPageWithSpace(100)
	
	if pageID != InvalidPageID {
		t.Error("Should return InvalidPageID when FSM is empty")
	}
}

func TestFSM_FindPageWithSpace_ExactMatch(t *testing.T) {
	fsm := NewFreeSpaceMap()
	
	fsm.UpdateFreeSpace(PageID(1), 1024)
	
	// Find page with exactly 1024 bytes
	pageID := fsm.FindPageWithSpace(1024)
	
	if pageID == InvalidPageID {
		t.Error("Should find page with exact match")
	}
}

func TestFSM_RemovePage(t *testing.T) {
	fsm := NewFreeSpaceMap()
	
	pageID := PageID(1)
	fsm.UpdateFreeSpace(pageID, 1024)
	
	if fsm.GetPageCount() != 1 {
		t.Error("Page should be added")
	}
	
	fsm.RemovePage(pageID)
	
	if fsm.GetPageCount() != 0 {
		t.Error("Page should be removed")
	}
	
	if fsm.GetFreeSpace(pageID) != 0 {
		t.Error("Removed page should return 0 free space")
	}
}

func TestFSM_RemovePage_NonExistent(t *testing.T) {
	fsm := NewFreeSpaceMap()
	
	// Remove non-existent page (should not panic)
	fsm.RemovePage(PageID(999))
	
	if fsm.GetPageCount() != 0 {
		t.Error("Page count should remain 0")
	}
}

func TestFSM_RemovePage_Multiple(t *testing.T) {
	fsm := NewFreeSpaceMap()
	
	fsm.UpdateFreeSpace(PageID(1), 1024)
	fsm.UpdateFreeSpace(PageID(2), 2048)
	fsm.UpdateFreeSpace(PageID(3), 512)
	
	fsm.RemovePage(PageID(2))
	
	if fsm.GetPageCount() != 2 {
		t.Errorf("Expected 2 pages after removal, got %d", fsm.GetPageCount())
	}
	
	// Verify remaining pages
	if fsm.GetFreeSpace(PageID(1)) != 1024 {
		t.Error("Page 1 should still exist")
	}
	if fsm.GetFreeSpace(PageID(3)) != 512 {
		t.Error("Page 3 should still exist")
	}
	if fsm.GetFreeSpace(PageID(2)) != 0 {
		t.Error("Page 2 should be removed")
	}
}

func TestFSM_GetPageCount(t *testing.T) {
	fsm := NewFreeSpaceMap()
	
	if fsm.GetPageCount() != 0 {
		t.Error("Initial count should be 0")
	}
	
	fsm.UpdateFreeSpace(PageID(1), 1024)
	if fsm.GetPageCount() != 1 {
		t.Error("Count should be 1 after adding page")
	}
	
	fsm.UpdateFreeSpace(PageID(2), 2048)
	if fsm.GetPageCount() != 2 {
		t.Error("Count should be 2 after adding second page")
	}
	
	fsm.UpdateFreeSpace(PageID(1), 512) // Update existing
	if fsm.GetPageCount() != 2 {
		t.Error("Count should remain 2 after update")
	}
	
	fsm.RemovePage(PageID(1))
	if fsm.GetPageCount() != 1 {
		t.Error("Count should be 1 after removal")
	}
}

func TestFSM_Clear(t *testing.T) {
	fsm := NewFreeSpaceMap()
	
	// Add multiple pages
	fsm.UpdateFreeSpace(PageID(1), 1024)
	fsm.UpdateFreeSpace(PageID(2), 2048)
	fsm.UpdateFreeSpace(PageID(3), 512)
	
	if fsm.GetPageCount() != 3 {
		t.Error("Should have 3 pages before clear")
	}
	
	fsm.Clear()
	
	if fsm.GetPageCount() != 0 {
		t.Error("Should have 0 pages after clear")
	}
	
	// Verify all pages are gone
	if fsm.GetFreeSpace(PageID(1)) != 0 {
		t.Error("Page 1 should be cleared")
	}
	if fsm.GetFreeSpace(PageID(2)) != 0 {
		t.Error("Page 2 should be cleared")
	}
	if fsm.GetFreeSpace(PageID(3)) != 0 {
		t.Error("Page 3 should be cleared")
	}
}

func TestFSM_Clear_Empty(t *testing.T) {
	fsm := NewFreeSpaceMap()
	
	// Clear empty FSM (should not panic)
	fsm.Clear()
	
	if fsm.GetPageCount() != 0 {
		t.Error("Count should remain 0")
	}
}

func TestFSM_ZeroFreeSpace(t *testing.T) {
	fsm := NewFreeSpaceMap()
	
	// Page with 0 free space
	fsm.UpdateFreeSpace(PageID(1), 0)
	
	if fsm.GetPageCount() != 1 {
		t.Error("Page with 0 free space should still be tracked")
	}
	
	if fsm.GetFreeSpace(PageID(1)) != 0 {
		t.Error("Free space should be 0")
	}
	
	// Should not find this page when looking for space
	pageID := fsm.FindPageWithSpace(1)
	if pageID == PageID(1) {
		t.Error("Should not find page with 0 free space")
	}
}

func TestFSM_MaxFreeSpace(t *testing.T) {
	fsm := NewFreeSpaceMap()
	
	// Maximum uint16 value
	maxSpace := uint16(65535)
	fsm.UpdateFreeSpace(PageID(1), maxSpace)
	
	if fsm.GetFreeSpace(PageID(1)) != maxSpace {
		t.Error("Should handle maximum free space value")
	}
	
	pageID := fsm.FindPageWithSpace(maxSpace)
	if pageID != PageID(1) {
		t.Error("Should find page with maximum space")
	}
}

func TestFSM_LargeNumberOfPages(t *testing.T) {
	fsm := NewFreeSpaceMap()
	
	// Add many pages
	numPages := 1000
	for i := 0; i < numPages; i++ {
		fsm.UpdateFreeSpace(PageID(i), uint16(i%1000))
	}
	
	if fsm.GetPageCount() != numPages {
		t.Errorf("Expected %d pages, got %d", numPages, fsm.GetPageCount())
	}
	
	// Find page with specific space
	pageID := fsm.FindPageWithSpace(500)
	if pageID == InvalidPageID {
		t.Error("Should find page in large dataset")
	}
}

func TestFSM_ConcurrentUpdates(t *testing.T) {
	fsm := NewFreeSpaceMap()
	
	done := make(chan bool)
	
	// Concurrent updates
	for i := 0; i < 100; i++ {
		go func(id int) {
			fsm.UpdateFreeSpace(PageID(id), uint16(id*10))
			done <- true
		}(i)
	}
	
	// Wait for all
	for i := 0; i < 100; i++ {
		<-done
	}
	
	if fsm.GetPageCount() != 100 {
		t.Errorf("Expected 100 pages after concurrent updates, got %d", fsm.GetPageCount())
	}
}

func TestFSM_ConcurrentReads(t *testing.T) {
	fsm := NewFreeSpaceMap()
	
	// Setup data
	for i := 0; i < 10; i++ {
		fsm.UpdateFreeSpace(PageID(i), uint16(i*100))
	}
	
	done := make(chan bool)
	
	// Concurrent reads
	for i := 0; i < 50; i++ {
		go func(id int) {
			_ = fsm.GetFreeSpace(PageID(id % 10))
			_ = fsm.GetPageCount()
			_ = fsm.FindPageWithSpace(uint16(id * 10))
			done <- true
		}(i)
	}
	
	// Wait for all
	for i := 0; i < 50; i++ {
		<-done
	}
}

func TestFSM_ConcurrentMixed(t *testing.T) {
	fsm := NewFreeSpaceMap()
	
	done := make(chan bool)
	
	// Mixed concurrent operations
	for i := 0; i < 50; i++ {
		go func(id int) {
			if id%3 == 0 {
				fsm.UpdateFreeSpace(PageID(id), uint16(id*10))
			} else if id%3 == 1 {
				_ = fsm.GetFreeSpace(PageID(id))
			} else {
				_ = fsm.FindPageWithSpace(uint16(id * 5))
			}
			done <- true
		}(i)
	}
	
	// Wait for all
	for i := 0; i < 50; i++ {
		<-done
	}
}

func TestFSM_FindBestFit(t *testing.T) {
	fsm := NewFreeSpaceMap()
	
	// Add pages with different free space
	fsm.UpdateFreeSpace(PageID(1), 100)
	fsm.UpdateFreeSpace(PageID(2), 500)
	fsm.UpdateFreeSpace(PageID(3), 1000)
	fsm.UpdateFreeSpace(PageID(4), 200)
	
	// Find page with at least 150 bytes
	// Should find any page with >= 150 bytes
	pageID := fsm.FindPageWithSpace(150)
	
	if pageID == InvalidPageID {
		t.Error("Should find a suitable page")
	}
	
	freeBytes := fsm.GetFreeSpace(pageID)
	if freeBytes < 150 {
		t.Errorf("Found page has insufficient space: %d", freeBytes)
	}
}

func TestFSM_UpdateAfterFind(t *testing.T) {
	fsm := NewFreeSpaceMap()
	
	fsm.UpdateFreeSpace(PageID(1), 1024)
	
	// Find page
	pageID := fsm.FindPageWithSpace(500)
	if pageID != PageID(1) {
		t.Error("Should find page 1")
	}
	
	// Update the found page to have less space
	fsm.UpdateFreeSpace(pageID, 100)
	
	// Now it shouldn't be found for 500 bytes
	pageID2 := fsm.FindPageWithSpace(500)
	if pageID2 != InvalidPageID {
		t.Error("Should not find page after space reduction")
	}
}

func TestFSM_RemoveAndReAdd(t *testing.T) {
	fsm := NewFreeSpaceMap()
	
	pageID := PageID(1)
	
	// Add page
	fsm.UpdateFreeSpace(pageID, 1024)
	if fsm.GetPageCount() != 1 {
		t.Error("Page should be added")
	}
	
	// Remove page
	fsm.RemovePage(pageID)
	if fsm.GetPageCount() != 0 {
		t.Error("Page should be removed")
	}
	
	// Re-add page with different space
	fsm.UpdateFreeSpace(pageID, 2048)
	if fsm.GetPageCount() != 1 {
		t.Error("Page should be re-added")
	}
	
	if fsm.GetFreeSpace(pageID) != 2048 {
		t.Error("Re-added page should have new free space value")
	}
}

func TestFSM_EdgeCases(t *testing.T) {
	fsm := NewFreeSpaceMap()
	
	// Test with PageID 0
	fsm.UpdateFreeSpace(PageID(0), 1024)
	if fsm.GetFreeSpace(PageID(0)) != 1024 {
		t.Error("Should handle PageID 0")
	}
	
	// Test with very large PageID
	largeID := PageID(4294967295) // Max uint32
	fsm.UpdateFreeSpace(largeID, 512)
	if fsm.GetFreeSpace(largeID) != 512 {
		t.Error("Should handle large PageID")
	}
}

func BenchmarkFSM_UpdateFreeSpace(b *testing.B) {
	fsm := NewFreeSpaceMap()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fsm.UpdateFreeSpace(PageID(i%1000), uint16(i%1000))
	}
}

func BenchmarkFSM_GetFreeSpace(b *testing.B) {
	fsm := NewFreeSpaceMap()
	
	// Setup
	for i := 0; i < 1000; i++ {
		fsm.UpdateFreeSpace(PageID(i), uint16(i))
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = fsm.GetFreeSpace(PageID(i % 1000))
	}
}

func BenchmarkFSM_FindPageWithSpace(b *testing.B) {
	fsm := NewFreeSpaceMap()
	
	// Setup
	for i := 0; i < 1000; i++ {
		fsm.UpdateFreeSpace(PageID(i), uint16(i*10))
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = fsm.FindPageWithSpace(uint16(i % 5000))
	}
}
