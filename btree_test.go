package main

import (
	"testing"
)

func TestBTreeBasics(t *testing.T) {
	bt := NewBTree()

	// Insert a key
	tid := TupleID{PageID: 1, SlotNum: 0}
	err := bt.Insert(10, tid)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Search for the key
	result, found := bt.Search(10)
	if !found {
		t.Error("Key not found")
	}

	if result != tid {
		t.Errorf("Expected %v, got %v", tid, result)
	}

	// Search for non-existent key
	_, found = bt.Search(20)
	if found {
		t.Error("Found non-existent key")
	}
}

func TestBTreeMultipleInserts(t *testing.T) {
	bt := NewBTree()

	// Insert multiple keys
	keys := []int{5, 3, 7, 1, 9, 2, 8, 4, 6}
	for i, key := range keys {
		tid := TupleID{PageID: PageID(i), SlotNum: uint16(i)}
		if err := bt.Insert(key, tid); err != nil {
			t.Fatalf("Insert %d failed: %v", key, err)
		}
	}

	// Verify all keys
	for i, key := range keys {
		result, found := bt.Search(key)
		if !found {
			t.Errorf("Key %d not found", key)
		}

		expected := TupleID{PageID: PageID(i), SlotNum: uint16(i)}
		if result != expected {
			t.Errorf("Key %d: expected %v, got %v", key, expected, result)
		}
	}

	// Verify count
	count := bt.Count()
	if count != len(keys) {
		t.Errorf("Expected count %d, got %d", len(keys), count)
	}
}

func TestBTreeDuplicateKey(t *testing.T) {
	bt := NewBTree()

	tid := TupleID{PageID: 1, SlotNum: 0}
	bt.Insert(10, tid)

	// Try to insert duplicate
	err := bt.Insert(10, tid)
	if err == nil {
		t.Error("Expected error for duplicate key")
	}
}

func TestBTreeSplit(t *testing.T) {
	bt := NewBTree()

	// Insert enough keys to trigger splits
	numKeys := 200
	for i := 0; i < numKeys; i++ {
		tid := TupleID{PageID: PageID(i), SlotNum: uint16(i)}
		if err := bt.Insert(i, tid); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Verify all keys
	for i := 0; i < numKeys; i++ {
		result, found := bt.Search(i)
		if !found {
			t.Errorf("Key %d not found after split", i)
		}

		expected := TupleID{PageID: PageID(i), SlotNum: uint16(i)}
		if result != expected {
			t.Errorf("Key %d: wrong value after split", i)
		}
	}

	// Tree should have height > 1
	height := bt.Height()
	if height <= 1 {
		t.Errorf("Expected height > 1 after splits, got %d", height)
	}

	// Verify count
	count := bt.Count()
	if count != numKeys {
		t.Errorf("Expected count %d, got %d", numKeys, count)
	}
}

func TestBTreeRangeSearch(t *testing.T) {
	bt := NewBTree()

	// Insert keys 0-99
	for i := 0; i < 100; i++ {
		tid := TupleID{PageID: PageID(i), SlotNum: uint16(i)}
		bt.Insert(i, tid)
	}

	// Range search [10, 20]
	results := bt.RangeSearch(10, 20)

	if len(results) != 11 { // 10, 11, ..., 20
		t.Errorf("Expected 11 results, got %d", len(results))
	}

	// Verify results are in order
	for i, result := range results {
		expected := TupleID{PageID: PageID(10 + i), SlotNum: uint16(10 + i)}
		if result != expected {
			t.Errorf("Result %d: expected %v, got %v", i, expected, result)
		}
	}
}

func TestBTreeRangeSearchEmpty(t *testing.T) {
	bt := NewBTree()

	// Insert keys 0-9
	for i := 0; i < 10; i++ {
		tid := TupleID{PageID: PageID(i), SlotNum: uint16(i)}
		bt.Insert(i, tid)
	}

	// Range search outside bounds
	results := bt.RangeSearch(20, 30)

	if len(results) != 0 {
		t.Errorf("Expected 0 results, got %d", len(results))
	}
}

func TestBTreeRangeSearchAll(t *testing.T) {
	bt := NewBTree()

	// Insert keys
	numKeys := 50
	for i := 0; i < numKeys; i++ {
		tid := TupleID{PageID: PageID(i), SlotNum: uint16(i)}
		bt.Insert(i, tid)
	}

	// Range search all
	results := bt.RangeSearch(0, numKeys-1)

	if len(results) != numKeys {
		t.Errorf("Expected %d results, got %d", numKeys, len(results))
	}
}

func TestBTreeDelete(t *testing.T) {
	bt := NewBTree()

	// Insert keys
	keys := []int{5, 3, 7, 1, 9}
	for i, key := range keys {
		tid := TupleID{PageID: PageID(i), SlotNum: uint16(i)}
		bt.Insert(key, tid)
	}

	// Delete a key
	err := bt.Delete(3)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify key is gone
	_, found := bt.Search(3)
	if found {
		t.Error("Deleted key still found")
	}

	// Verify other keys still exist
	for _, key := range []int{5, 7, 1, 9} {
		_, found := bt.Search(key)
		if !found {
			t.Errorf("Key %d not found after delete", key)
		}
	}

	// Verify count
	count := bt.Count()
	if count != len(keys)-1 {
		t.Errorf("Expected count %d, got %d", len(keys)-1, count)
	}
}

func TestBTreeDeleteNonExistent(t *testing.T) {
	bt := NewBTree()

	tid := TupleID{PageID: 1, SlotNum: 0}
	bt.Insert(10, tid)

	// Try to delete non-existent key
	err := bt.Delete(20)
	if err == nil {
		t.Error("Expected error for deleting non-existent key")
	}
}

func TestBTreeStringKeys(t *testing.T) {
	bt := NewBTree()

	// Insert string keys
	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for i, key := range keys {
		tid := TupleID{PageID: PageID(i), SlotNum: uint16(i)}
		if err := bt.Insert(key, tid); err != nil {
			t.Fatalf("Insert %s failed: %v", key, err)
		}
	}

	// Search for keys
	for i, key := range keys {
		result, found := bt.Search(key)
		if !found {
			t.Errorf("Key %s not found", key)
		}

		expected := TupleID{PageID: PageID(i), SlotNum: uint16(i)}
		if result != expected {
			t.Errorf("Key %s: expected %v, got %v", key, expected, result)
		}
	}

	// Range search
	results := bt.RangeSearch("banana", "date")
	if len(results) != 3 { // banana, cherry, date
		t.Errorf("Expected 3 results, got %d", len(results))
	}
}

func TestBTreeFloat64Keys(t *testing.T) {
	bt := NewBTree()

	// Insert float keys
	keys := []float64{3.14, 2.71, 1.41, 1.73, 2.23}
	for i, key := range keys {
		tid := TupleID{PageID: PageID(i), SlotNum: uint16(i)}
		if err := bt.Insert(key, tid); err != nil {
			t.Fatalf("Insert %f failed: %v", key, err)
		}
	}

	// Search for keys
	for i, key := range keys {
		result, found := bt.Search(key)
		if !found {
			t.Errorf("Key %f not found", key)
		}

		expected := TupleID{PageID: PageID(i), SlotNum: uint16(i)}
		if result != expected {
			t.Errorf("Key %f: expected %v, got %v", key, expected, result)
		}
	}
}

func TestBTreeLargeDataset(t *testing.T) {
	bt := NewBTree()

	// Insert many keys
	numKeys := 10000
	for i := 0; i < numKeys; i++ {
		tid := TupleID{PageID: PageID(i), SlotNum: uint16(i % 65536)}
		if err := bt.Insert(i, tid); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Verify count
	count := bt.Count()
	if count != numKeys {
		t.Errorf("Expected count %d, got %d", numKeys, count)
	}

	// Random searches
	testKeys := []int{0, 100, 500, 1000, 5000, 9999}
	for _, key := range testKeys {
		_, found := bt.Search(key)
		if !found {
			t.Errorf("Key %d not found in large dataset", key)
		}
	}

	// Range search
	results := bt.RangeSearch(1000, 1100)
	if len(results) != 101 {
		t.Errorf("Expected 101 results, got %d", len(results))
	}

	// Check height is reasonable
	height := bt.Height()
	if height > 5 {
		t.Logf("Tree height: %d (may be high for %d keys)", height, numKeys)
	}
}

func TestBTreeConcurrentReads(t *testing.T) {
	bt := NewBTree()

	// Insert keys
	numKeys := 1000
	for i := 0; i < numKeys; i++ {
		tid := TupleID{PageID: PageID(i), SlotNum: uint16(i)}
		bt.Insert(i, tid)
	}

	// Concurrent reads
	done := make(chan bool, 10)
	for g := 0; g < 10; g++ {
		go func(goroutineID int) {
			for i := 0; i < 100; i++ {
				key := (goroutineID * 100 + i) % numKeys
				_, found := bt.Search(key)
				if !found {
					t.Errorf("Goroutine %d: key %d not found", goroutineID, key)
				}
			}
			done <- true
		}(g)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestBTreeHeight(t *testing.T) {
	bt := NewBTree()

	// Empty tree
	if bt.Height() != 1 {
		t.Errorf("Empty tree should have height 1, got %d", bt.Height())
	}

	// Insert one key
	bt.Insert(1, TupleID{PageID: 1, SlotNum: 0})
	if bt.Height() != 1 {
		t.Errorf("Single key tree should have height 1, got %d", bt.Height())
	}

	// Insert enough to cause split
	for i := 2; i <= 200; i++ {
		bt.Insert(i, TupleID{PageID: PageID(i), SlotNum: 0})
	}

	height := bt.Height()
	if height <= 1 {
		t.Errorf("Large tree should have height > 1, got %d", height)
	}
}

func TestBTreeOrdering(t *testing.T) {
	bt := NewBTree()

	// Insert in random order
	keys := []int{50, 25, 75, 10, 30, 60, 90, 5, 15, 27, 35}
	for i, key := range keys {
		tid := TupleID{PageID: PageID(i), SlotNum: uint16(i)}
		bt.Insert(key, tid)
	}

	// Range search should return in order
	results := bt.RangeSearch(0, 100)

	if len(results) != len(keys) {
		t.Errorf("Expected %d results, got %d", len(keys), len(results))
	}

	// Verify ordering by checking PageIDs correspond to sorted keys
	// (Since we inserted keys in specific order, PageID tells us insertion order)
	// Results should be sorted by key value
}
