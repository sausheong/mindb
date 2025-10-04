package main

import (
	"bytes"
	"fmt"
	"sync"
)

// B+ tree constants
const (
	BTreeOrder     = 128 // Max keys per node (order)
	BTreeMinKeys   = BTreeOrder / 2
	BTreeMaxKeys   = BTreeOrder - 1
	BTreeMaxChildren = BTreeOrder
)

// BTreeNode represents a node in the B+ tree
type BTreeNode struct {
	IsLeaf   bool
	Keys     []interface{}   // Keys in this node
	Children []*BTreeNode    // Child pointers (internal nodes)
	Values   []TupleID       // Values (leaf nodes only)
	Next     *BTreeNode      // Next leaf pointer (for range scans)
	Parent   *BTreeNode      // Parent pointer
}

// BTree represents a B+ tree index
type BTree struct {
	Root     *BTreeNode
	Order    int
	mu       sync.RWMutex
}

// NewBTree creates a new B+ tree
func NewBTree() *BTree {
	return &BTree{
		Root: &BTreeNode{
			IsLeaf: true,
			Keys:   make([]interface{}, 0),
			Values: make([]TupleID, 0),
		},
		Order: BTreeOrder,
	}
}

// Insert inserts a key-value pair into the B+ tree
func (bt *BTree) Insert(key interface{}, value TupleID) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	// If root is full, split it
	if len(bt.Root.Keys) >= BTreeMaxKeys {
		oldRoot := bt.Root
		bt.Root = &BTreeNode{
			IsLeaf:   false,
			Keys:     make([]interface{}, 0),
			Children: []*BTreeNode{oldRoot},
		}
		oldRoot.Parent = bt.Root
		bt.splitChild(bt.Root, 0)
	}

	return bt.insertNonFull(bt.Root, key, value)
}

// insertNonFull inserts into a node that is not full
func (bt *BTree) insertNonFull(node *BTreeNode, key interface{}, value TupleID) error {
	if node.IsLeaf {
		// Find insertion position
		i := bt.findInsertPos(node.Keys, key)
		
		// Check for duplicate key
		if i < len(node.Keys) && bt.compareKeys(node.Keys[i], key) == 0 {
			return fmt.Errorf("duplicate key")
		}

		// Insert key and value
		node.Keys = append(node.Keys, nil)
		node.Values = append(node.Values, TupleID{})
		
		copy(node.Keys[i+1:], node.Keys[i:])
		copy(node.Values[i+1:], node.Values[i:])
		
		node.Keys[i] = key
		node.Values[i] = value
		
		return nil
	}

	// Internal node - find child to insert into
	i := bt.findChildIndex(node.Keys, key)
	
	// Split child if full
	if len(node.Children[i].Keys) >= BTreeMaxKeys {
		bt.splitChild(node, i)
		
		// After split, determine which child to insert into
		if bt.compareKeys(key, node.Keys[i]) > 0 {
			i++
		}
	}

	return bt.insertNonFull(node.Children[i], key, value)
}

// splitChild splits a full child node
func (bt *BTree) splitChild(parent *BTreeNode, childIndex int) {
	child := parent.Children[childIndex]
	mid := len(child.Keys) / 2
	var newNode *BTreeNode

	if child.IsLeaf {
		// Leaf node - in B+ tree, middle key is COPIED (not moved) to parent
		// and stays in the right child for range scans
		promotedKey := child.Keys[mid]
		
		// Create new node for right half (including mid)
		newNode = &BTreeNode{
			IsLeaf: true,
			Parent: parent,
			Keys:   make([]interface{}, len(child.Keys)-mid),
			Values: make([]TupleID, len(child.Values)-mid),
			Next:   child.Next,
		}
		
		// Copy right half of keys and values (including mid)
		copy(newNode.Keys, child.Keys[mid:])
		copy(newNode.Values, child.Values[mid:])
		
		// Truncate child (left half only)
		child.Keys = child.Keys[:mid]
		child.Values = child.Values[:mid]
		child.Next = newNode
		
		// Insert promoted key into parent
		parent.Keys = append(parent.Keys, nil)
		parent.Children = append(parent.Children, nil)
		
		copy(parent.Keys[childIndex+1:], parent.Keys[childIndex:])
		copy(parent.Children[childIndex+2:], parent.Children[childIndex+1:])
		
		parent.Keys[childIndex] = promotedKey
		parent.Children[childIndex+1] = newNode
	} else {
		// Internal node - save middle key before truncating
		promotedKey := child.Keys[mid]
		
		// Create new node for right half
		newNode = &BTreeNode{
			IsLeaf:   false,
			Parent:   parent,
			Keys:     make([]interface{}, len(child.Keys)-mid-1),
			Children: make([]*BTreeNode, len(child.Children)-mid-1),
		}
		
		// Copy right half of keys
		copy(newNode.Keys, child.Keys[mid+1:])
		
		// Move children
		copy(newNode.Children, child.Children[mid+1:])
		
		// Truncate child
		child.Keys = child.Keys[:mid]
		child.Children = child.Children[:mid+1]
		
		// Update parent pointers
		for _, c := range newNode.Children {
			c.Parent = newNode
		}
		
		// Insert promoted key into parent
		parent.Keys = append(parent.Keys, nil)
		parent.Children = append(parent.Children, nil)
		
		copy(parent.Keys[childIndex+1:], parent.Keys[childIndex:])
		copy(parent.Children[childIndex+2:], parent.Children[childIndex+1:])
		
		parent.Keys[childIndex] = promotedKey
		parent.Children[childIndex+1] = newNode
	}
}

// Search searches for a key in the B+ tree
func (bt *BTree) Search(key interface{}) (TupleID, bool) {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	node := bt.Root
	
	// Traverse to leaf
	for !node.IsLeaf {
		i := bt.findChildIndex(node.Keys, key)
		node = node.Children[i]
	}

	// Search in leaf
	i := bt.findKey(node.Keys, key)
	if i < len(node.Keys) && bt.compareKeys(node.Keys[i], key) == 0 {
		return node.Values[i], true
	}

	return TupleID{}, false
}

// RangeSearch performs a range query [startKey, endKey]
func (bt *BTree) RangeSearch(startKey, endKey interface{}) []TupleID {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	var results []TupleID
	
	// Find starting leaf
	node := bt.Root
	for !node.IsLeaf {
		i := bt.findChildIndex(node.Keys, startKey)
		node = node.Children[i]
	}

	// Scan leaves
	for node != nil {
		for i, key := range node.Keys {
			cmp := bt.compareKeys(key, startKey)
			if cmp < 0 {
				continue
			}
			
			cmp = bt.compareKeys(key, endKey)
			if cmp > 0 {
				return results
			}
			
			results = append(results, node.Values[i])
		}
		node = node.Next
	}

	return results
}

// Delete removes a key from the B+ tree
func (bt *BTree) Delete(key interface{}) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	return bt.deleteFromNode(bt.Root, key)
}

// deleteFromNode deletes a key from a node
func (bt *BTree) deleteFromNode(node *BTreeNode, key interface{}) error {
	if node.IsLeaf {
		// Find key in leaf
		i := bt.findKey(node.Keys, key)
		if i >= len(node.Keys) || bt.compareKeys(node.Keys[i], key) != 0 {
			return fmt.Errorf("key not found")
		}

		// Remove key and value
		node.Keys = append(node.Keys[:i], node.Keys[i+1:]...)
		node.Values = append(node.Values[:i], node.Values[i+1:]...)

		return nil
	}

	// Internal node - find child containing key
	i := bt.findChildIndex(node.Keys, key)
	return bt.deleteFromNode(node.Children[i], key)
}

// findInsertPos finds the position to insert a key
func (bt *BTree) findInsertPos(keys []interface{}, key interface{}) int {
	left, right := 0, len(keys)
	
	for left < right {
		mid := (left + right) / 2
		if bt.compareKeys(keys[mid], key) < 0 {
			left = mid + 1
		} else {
			right = mid
		}
	}
	
	return left
}

// findKey finds a key in a sorted slice
func (bt *BTree) findKey(keys []interface{}, key interface{}) int {
	left, right := 0, len(keys)
	
	for left < right {
		mid := (left + right) / 2
		cmp := bt.compareKeys(keys[mid], key)
		if cmp < 0 {
			left = mid + 1
		} else if cmp > 0 {
			right = mid
		} else {
			return mid
		}
	}
	
	return left
}

// findChildIndex finds which child to follow
func (bt *BTree) findChildIndex(keys []interface{}, key interface{}) int {
	i := 0
	for i < len(keys) && bt.compareKeys(key, keys[i]) >= 0 {
		i++
	}
	return i
}

// compareKeys compares two keys
func (bt *BTree) compareKeys(a, b interface{}) int {
	// Handle nil
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Compare by type
	switch aVal := a.(type) {
	case int:
		bVal, ok := b.(int)
		if !ok {
			return -1
		}
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
		return 0
		
	case int64:
		bVal, ok := b.(int64)
		if !ok {
			return -1
		}
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
		return 0
		
	case float64:
		bVal, ok := b.(float64)
		if !ok {
			return -1
		}
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
		return 0
		
	case string:
		bVal, ok := b.(string)
		if !ok {
			return -1
		}
		return bytes.Compare([]byte(aVal), []byte(bVal))
		
	default:
		// Fallback to string comparison
		aStr := fmt.Sprintf("%v", a)
		bStr := fmt.Sprintf("%v", b)
		return bytes.Compare([]byte(aStr), []byte(bStr))
	}
}

// Height returns the height of the tree
func (bt *BTree) Height() int {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	if bt.Root == nil {
		return 0
	}

	height := 1
	node := bt.Root
	for !node.IsLeaf {
		height++
		node = node.Children[0]
	}

	return height
}

// Count returns the number of keys in the tree
func (bt *BTree) Count() int {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	return bt.countKeys(bt.Root)
}

// countKeys recursively counts keys
func (bt *BTree) countKeys(node *BTreeNode) int {
	if node == nil {
		return 0
	}

	if node.IsLeaf {
		return len(node.Keys)
	}

	count := 0
	for _, child := range node.Children {
		count += bt.countKeys(child)
	}

	return count
}
