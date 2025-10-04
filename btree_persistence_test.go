package main

import (
	"os"
	"testing"
)

/*
Package: mindb
Component: B-Tree Persistence
Layer: Persistence (Layer 4)

Test Coverage:
- SaveToFile/LoadBTreeFromFile
- Node serialization/deserialization
- Key serialization/deserialization
- Value serialization/deserialization
- Disk persistence

Priority: LOW (38% coverage → target 70%+)
Impact: +1% overall coverage

Run: go test -v -run TestBTreePersistence
*/

// ============================================================================
// B-TREE PERSISTENCE TESTS
// ============================================================================

func TestBTreePersistence_SaveAndLoad(t *testing.T) {
	tmpDir := t.TempDir()
	treePath := tmpDir + "/test_tree.db"

	// Create and populate a B-tree
	tree := NewBTree()
	
	// Insert test data
	tree.Insert(10, TupleID{PageID: 1, SlotNum: 0})
	tree.Insert(20, TupleID{PageID: 1, SlotNum: 1})
	tree.Insert(5, TupleID{PageID: 1, SlotNum: 2})
	tree.Insert(15, TupleID{PageID: 1, SlotNum: 3})
	tree.Insert(25, TupleID{PageID: 2, SlotNum: 0})

	// Save to disk
	err := tree.SaveToFile(treePath)
	if err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	// Verify file was created
	if _, err := os.Stat(treePath); os.IsNotExist(err) {
		t.Fatal("Tree file was not created")
	}

	// Load from disk
	loadedTree, err := LoadBTreeFromFile(treePath)
	if err != nil {
		t.Fatalf("LoadBTreeFromFile failed: %v", err)
	}

	// Verify data integrity
	testKeys := []int{5, 10, 15, 20, 25}
	for _, key := range testKeys {
		_, found := loadedTree.Search(key)
		if !found {
			t.Errorf("Key %d not found after loading", key)
		}
	}
}

func TestBTreePersistence_EmptyTree(t *testing.T) {
	tmpDir := t.TempDir()
	treePath := tmpDir + "/empty_tree.db"

	// Create empty tree
	tree := NewBTree()

	// Save empty tree
	err := tree.SaveToFile(treePath)
	if err != nil {
		t.Fatalf("SaveToFile for empty tree failed: %v", err)
	}

	// Load empty tree
	loadedTree, err := LoadBTreeFromFile(treePath)
	if err != nil {
		t.Fatalf("LoadBTreeFromFile for empty tree failed: %v", err)
	}

	// Verify it's empty
	_, found := loadedTree.Search(1)
	if found {
		t.Error("Empty tree should not contain any keys")
	}
}

func TestBTreePersistence_LargeTree(t *testing.T) {
	tmpDir := t.TempDir()
	treePath := tmpDir + "/large_tree.db"

	// Create large tree
	tree := NewBTree()
	numKeys := 100

	for i := 0; i < numKeys; i++ {
		tree.Insert(i, TupleID{PageID: PageID(i / 10), SlotNum: uint16(i % 10)})
	}

	// Save to disk
	err := tree.SaveToFile(treePath)
	if err != nil {
		t.Fatalf("SaveToFile for large tree failed: %v", err)
	}

	// Load from disk
	loadedTree, err := LoadBTreeFromFile(treePath)
	if err != nil {
		t.Fatalf("LoadBTreeFromFile for large tree failed: %v", err)
	}

	// Verify random keys
	testKeys := []int{0, 25, 50, 75, 99}
	for _, key := range testKeys {
		_, found := loadedTree.Search(key)
		if !found {
			t.Errorf("Key %d not found in loaded large tree", key)
		}
	}
}

func TestBTreePersistence_SequentialKeys(t *testing.T) {
	tmpDir := t.TempDir()
	treePath := tmpDir + "/sequential.db"

	tree := NewBTree()
	
	// Insert keys in sequential order
	for i := 1; i <= 20; i++ {
		tree.Insert(i, TupleID{PageID: PageID(i), SlotNum: 0})
	}

	err := tree.SaveToFile(treePath)
	if err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	loadedTree, err := LoadBTreeFromFile(treePath)
	if err != nil {
		t.Fatalf("LoadBTreeFromFile failed: %v", err)
	}

	// Verify all keys
	for i := 1; i <= 20; i++ {
		_, found := loadedTree.Search(i)
		if !found {
			t.Errorf("Key %d not found after sequential insertion", i)
		}
	}
}

func TestBTreePersistence_ReverseKeys(t *testing.T) {
	tmpDir := t.TempDir()
	treePath := tmpDir + "/reverse.db"

	tree := NewBTree()
	
	// Insert keys in reverse order
	for i := 20; i >= 1; i-- {
		tree.Insert(i, TupleID{PageID: PageID(i), SlotNum: 0})
	}

	err := tree.SaveToFile(treePath)
	if err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	loadedTree, err := LoadBTreeFromFile(treePath)
	if err != nil {
		t.Fatalf("LoadBTreeFromFile failed: %v", err)
	}

	// Verify all keys
	for i := 1; i <= 20; i++ {
		_, found := loadedTree.Search(i)
		if !found {
			t.Errorf("Key %d not found after reverse insertion", i)
		}
	}
}

func TestBTreePersistence_RandomKeys(t *testing.T) {
	tmpDir := t.TempDir()
	treePath := tmpDir + "/random.db"

	tree := NewBTree()
	
	// Insert keys in random order
	keys := []int{15, 3, 22, 8, 19, 1, 27, 11, 5, 30}
	for _, key := range keys {
		tree.Insert(key, TupleID{PageID: PageID(key), SlotNum: 0})
	}

	err := tree.SaveToFile(treePath)
	if err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	loadedTree, err := LoadBTreeFromFile(treePath)
	if err != nil {
		t.Fatalf("LoadBTreeFromFile failed: %v", err)
	}

	// Verify all keys
	for _, key := range keys {
		_, found := loadedTree.Search(key)
		if !found {
			t.Errorf("Key %d not found after random insertion", key)
		}
	}
}

func TestBTreePersistence_DuplicateValues(t *testing.T) {
	tmpDir := t.TempDir()
	treePath := tmpDir + "/duplicates.db"

	tree := NewBTree()
	
	// Insert different keys with same TupleID
	sameTuple := TupleID{PageID: 1, SlotNum: 0}
	tree.Insert(10, sameTuple)
	tree.Insert(20, sameTuple)
	tree.Insert(30, sameTuple)

	err := tree.SaveToFile(treePath)
	if err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	loadedTree, err := LoadBTreeFromFile(treePath)
	if err != nil {
		t.Fatalf("LoadBTreeFromFile failed: %v", err)
	}

	// Verify all keys exist
	for _, key := range []int{10, 20, 30} {
		_, found := loadedTree.Search(key)
		if !found {
			t.Errorf("Key %d not found", key)
		}
	}
}

func TestBTreePersistence_CorruptedFile(t *testing.T) {
	tmpDir := t.TempDir()
	treePath := tmpDir + "/corrupted.db"

	// Create a corrupted file
	err := os.WriteFile(treePath, []byte("corrupted data"), 0644)
	if err != nil {
		t.Fatalf("Failed to create corrupted file: %v", err)
	}

	// Try to load corrupted file - implementation may return error or empty tree
	tree, err := LoadBTreeFromFile(treePath)
	if err != nil {
		// Error is acceptable
		t.Logf("Corrupted file returned error: %v", err)
	} else if tree != nil {
		// Empty tree is also acceptable
		t.Logf("Corrupted file returned tree (possibly empty)")
	}
}

func TestBTreePersistence_NonExistentFile(t *testing.T) {
	tmpDir := t.TempDir()
	treePath := tmpDir + "/nonexistent.db"

	// Try to load non-existent file - implementation returns empty tree
	tree, err := LoadBTreeFromFile(treePath)
	if err != nil {
		// Error is acceptable
		t.Logf("Non-existent file returned error: %v", err)
	} else if tree != nil {
		// Empty tree is also acceptable (current implementation)
		t.Logf("Non-existent file returned empty tree")
		// Verify it's empty
		_, found := tree.Search(1)
		if found {
			t.Error("Tree from non-existent file should be empty")
		}
	}
}

func TestBTreePersistence_OverwriteExisting(t *testing.T) {
	tmpDir := t.TempDir()
	treePath := tmpDir + "/overwrite.db"

	// Create and save first tree
	tree1 := NewBTree()
	tree1.Insert(1, TupleID{PageID: 1, SlotNum: 0})
	tree1.Insert(2, TupleID{PageID: 1, SlotNum: 1})

	err := tree1.SaveToFile(treePath)
	if err != nil {
		t.Fatalf("First SaveToFile failed: %v", err)
	}

	// Create and save second tree (overwrite)
	tree2 := NewBTree()
	tree2.Insert(1, TupleID{PageID: 2, SlotNum: 0})
	tree2.Insert(3, TupleID{PageID: 2, SlotNum: 1})

	err = tree2.SaveToFile(treePath)
	if err != nil {
		t.Fatalf("Second SaveToFile failed: %v", err)
	}

	// Load and verify it's the second tree
	loadedTree, err := LoadBTreeFromFile(treePath)
	if err != nil {
		t.Fatalf("LoadBTreeFromFile failed: %v", err)
	}

	// Should have key 3 from second tree
	_, found := loadedTree.Search(3)
	if !found {
		t.Error("Loaded tree should contain data from second save")
	}

	// Should not have key 2 from first tree
	_, found = loadedTree.Search(2)
	if found {
		t.Error("Loaded tree should not contain data from first save (key 2)")
	}
}

func TestBTreePersistence_MultipleTreesSameDirectory(t *testing.T) {
	tmpDir := t.TempDir()

	// Create multiple trees
	tree1 := NewBTree()
	tree1.Insert(1, TupleID{PageID: 1, SlotNum: 0})

	tree2 := NewBTree()
	tree2.Insert(2, TupleID{PageID: 2, SlotNum: 0})

	// Save to different files
	err := tree1.SaveToFile(tmpDir + "/tree1.db")
	if err != nil {
		t.Fatalf("Save tree1 failed: %v", err)
	}

	err = tree2.SaveToFile(tmpDir + "/tree2.db")
	if err != nil {
		t.Fatalf("Save tree2 failed: %v", err)
	}

	// Load both trees
	loaded1, err := LoadBTreeFromFile(tmpDir + "/tree1.db")
	if err != nil {
		t.Fatalf("Load tree1 failed: %v", err)
	}

	loaded2, err := LoadBTreeFromFile(tmpDir + "/tree2.db")
	if err != nil {
		t.Fatalf("Load tree2 failed: %v", err)
	}

	// Verify each tree has its own data
	_, found := loaded1.Search(1)
	if !found {
		t.Error("Tree1 should contain key 1")
	}

	_, found = loaded2.Search(2)
	if !found {
		t.Error("Tree2 should contain key 2")
	}
}

func TestBTreePersistence_AfterModification(t *testing.T) {
	tmpDir := t.TempDir()
	treePath := tmpDir + "/modified.db"

	// Create, save, load, modify, save again
	tree := NewBTree()
	tree.Insert(1, TupleID{PageID: 1, SlotNum: 0})
	tree.SaveToFile(treePath)

	loadedTree, _ := LoadBTreeFromFile(treePath)
	loadedTree.Insert(2, TupleID{PageID: 1, SlotNum: 1})
	loadedTree.SaveToFile(treePath)

	// Load final version
	finalTree, err := LoadBTreeFromFile(treePath)
	if err != nil {
		t.Fatalf("Load final tree failed: %v", err)
	}

	// Should have both keys
	_, found := finalTree.Search(1)
	if !found {
		t.Error("Final tree should contain original key")
	}

	_, found = finalTree.Search(2)
	if !found {
		t.Error("Final tree should contain added key")
	}
}

func TestBTreePersistence_FilePermissions(t *testing.T) {
	tmpDir := t.TempDir()
	treePath := tmpDir + "/permissions.db"

	tree := NewBTree()
	tree.Insert(1, TupleID{PageID: 1, SlotNum: 0})

	// Save tree
	err := tree.SaveToFile(treePath)
	if err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	// Check file permissions
	info, err := os.Stat(treePath)
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}

	// File should be readable
	mode := info.Mode()
	if mode&0400 == 0 {
		t.Error("File should be readable")
	}
}

func TestBTreePersistence_ConcurrentSave(t *testing.T) {
	tmpDir := t.TempDir()

	// Create multiple trees and save concurrently
	done := make(chan bool)

	for i := 0; i < 5; i++ {
		go func(id int) {
			tree := NewBTree()
			tree.Insert(id, TupleID{PageID: PageID(id), SlotNum: 0})
			
			treePath := tmpDir + "/concurrent_" + string(rune(id+'0')) + ".db"
			err := tree.SaveToFile(treePath)
			if err != nil {
				t.Errorf("Concurrent save %d failed: %v", id, err)
			}
			done <- true
		}(i)
	}

	// Wait for all saves
	for i := 0; i < 5; i++ {
		<-done
	}

	// Verify all files were created
	for i := 0; i < 5; i++ {
		treePath := tmpDir + "/concurrent_" + string(rune(i+'0')) + ".db"
		if _, err := os.Stat(treePath); os.IsNotExist(err) {
			t.Errorf("Concurrent save %d did not create file", i)
		}
	}
}

func TestBTreePersistence_LargeValues(t *testing.T) {
	tmpDir := t.TempDir()
	treePath := tmpDir + "/large_values.db"

	tree := NewBTree()
	
	// Insert with large PageID and SlotNum values
	tree.Insert(1, TupleID{PageID: 999999, SlotNum: 65535})
	tree.Insert(2, TupleID{PageID: 1000000, SlotNum: 32768})

	err := tree.SaveToFile(treePath)
	if err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	loadedTree, err := LoadBTreeFromFile(treePath)
	if err != nil {
		t.Fatalf("LoadBTreeFromFile failed: %v", err)
	}

	// Verify keys exist
	_, found := loadedTree.Search(1)
	if !found {
		t.Error("Key 1 with large values not found")
	}

	_, found = loadedTree.Search(2)
	if !found {
		t.Error("Key 2 with large values not found")
	}
}

func TestBTreePersistence_NegativeKeys(t *testing.T) {
	tmpDir := t.TempDir()
	treePath := tmpDir + "/negative_keys.db"

	tree := NewBTree()
	
	// Insert negative keys
	tree.Insert(-10, TupleID{PageID: 1, SlotNum: 0})
	tree.Insert(-5, TupleID{PageID: 1, SlotNum: 1})
	tree.Insert(0, TupleID{PageID: 1, SlotNum: 2})
	tree.Insert(5, TupleID{PageID: 1, SlotNum: 3})

	err := tree.SaveToFile(treePath)
	if err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	loadedTree, err := LoadBTreeFromFile(treePath)
	if err != nil {
		t.Fatalf("LoadBTreeFromFile failed: %v", err)
	}

	// Verify all keys including negative ones
	for _, key := range []int{-10, -5, 0, 5} {
		_, found := loadedTree.Search(key)
		if !found {
			t.Errorf("Key %d not found", key)
		}
	}
}

func TestBTreePersistence_FileSize(t *testing.T) {
	tmpDir := t.TempDir()
	treePath := tmpDir + "/filesize.db"

	tree := NewBTree()
	
	// Insert known number of keys
	for i := 0; i < 10; i++ {
		tree.Insert(i, TupleID{PageID: PageID(i), SlotNum: 0})
	}

	err := tree.SaveToFile(treePath)
	if err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	// Check file size is reasonable
	info, err := os.Stat(treePath)
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}

	// File should have some content
	if info.Size() == 0 {
		t.Error("File size should not be zero")
	}

	// File should not be unreasonably large
	if info.Size() > 1024*1024 { // 1MB
		t.Errorf("File size %d seems too large for 10 keys", info.Size())
	}
}
