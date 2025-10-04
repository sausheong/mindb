package main

import (
	"encoding/binary"
	"fmt"
	"os"
)

// BTreeNodeType represents the type of B-tree node
type BTreeNodeType byte

const (
	BTreeNodeLeaf     BTreeNodeType = 1
	BTreeNodeInternal BTreeNodeType = 2
)

// SerializeBTree saves a B-tree to disk
func (bt *BTree) SaveToFile(filePath string) error {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create btree file: %v", err)
	}
	defer file.Close()

	// Write B-tree metadata
	metadata := make([]byte, 16)
	binary.LittleEndian.PutUint32(metadata[0:4], uint32(bt.Order))
	binary.LittleEndian.PutUint32(metadata[4:8], 0) // Reserved
	binary.LittleEndian.PutUint32(metadata[8:12], 0) // Reserved
	binary.LittleEndian.PutUint32(metadata[12:16], 0) // Reserved

	if _, err := file.Write(metadata); err != nil {
		return fmt.Errorf("failed to write metadata: %v", err)
	}

	// Serialize tree starting from root
	if bt.Root != nil {
		if err := bt.serializeNode(file, bt.Root); err != nil {
			return fmt.Errorf("failed to serialize tree: %v", err)
		}
	}

	return nil
}

// serializeNode recursively serializes a B-tree node
func (bt *BTree) serializeNode(file *os.File, node *BTreeNode) error {
	// Write node type
	nodeType := BTreeNodeLeaf
	if !node.IsLeaf {
		nodeType = BTreeNodeInternal
	}
	if err := binary.Write(file, binary.LittleEndian, nodeType); err != nil {
		return err
	}

	// Write number of keys
	numKeys := uint32(len(node.Keys))
	if err := binary.Write(file, binary.LittleEndian, numKeys); err != nil {
		return err
	}

	// Write keys
	for _, key := range node.Keys {
		if err := bt.serializeKey(file, key); err != nil {
			return err
		}
	}

	// Write values (for leaf nodes)
	if node.IsLeaf {
		for _, value := range node.Values {
			if err := bt.serializeValue(file, value); err != nil {
				return err
			}
		}
	} else {
		// Write number of children
		numChildren := uint32(len(node.Children))
		if err := binary.Write(file, binary.LittleEndian, numChildren); err != nil {
			return err
		}

		// Recursively serialize children
		for _, child := range node.Children {
			if err := bt.serializeNode(file, child); err != nil {
				return err
			}
		}
	}

	return nil
}

// serializeKey writes a key to the file
func (bt *BTree) serializeKey(file *os.File, key interface{}) error {
	switch v := key.(type) {
	case int:
		// Type marker for int
		if err := binary.Write(file, binary.LittleEndian, byte(1)); err != nil {
			return err
		}
		return binary.Write(file, binary.LittleEndian, int64(v))
	case string:
		// Type marker for string
		if err := binary.Write(file, binary.LittleEndian, byte(2)); err != nil {
			return err
		}
		strBytes := []byte(v)
		if err := binary.Write(file, binary.LittleEndian, uint32(len(strBytes))); err != nil {
			return err
		}
		_, err := file.Write(strBytes)
		return err
	default:
		return fmt.Errorf("unsupported key type: %T", key)
	}
}

// serializeValue writes a value to the file
func (bt *BTree) serializeValue(file *os.File, value interface{}) error {
	// For now, values are TupleIDs
	if tid, ok := value.(TupleID); ok {
		if err := binary.Write(file, binary.LittleEndian, tid.PageID); err != nil {
			return err
		}
		return binary.Write(file, binary.LittleEndian, tid.SlotNum)
	}
	return fmt.Errorf("unsupported value type: %T", value)
}

// LoadBTreeFromFile loads a B-tree from disk
func LoadBTreeFromFile(filePath string) (*BTree, error) {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // File doesn't exist, return nil tree
		}
		return nil, fmt.Errorf("failed to open btree file: %v", err)
	}
	defer file.Close()

	// Read metadata
	metadata := make([]byte, 16)
	if _, err := file.Read(metadata); err != nil {
		return nil, fmt.Errorf("failed to read metadata: %v", err)
	}

	order := int(binary.LittleEndian.Uint32(metadata[0:4]))

	bt := &BTree{
		Order: order,
	}

	// Check if there's a root node
	var nodeType BTreeNodeType
	if err := binary.Read(file, binary.LittleEndian, &nodeType); err != nil {
		if err.Error() == "EOF" {
			// Empty tree
			return bt, nil
		}
		return nil, fmt.Errorf("failed to read node type: %v", err)
	}

	// Deserialize root node
	root, err := bt.deserializeNode(file, nodeType)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize root: %v", err)
	}

	bt.Root = root
	return bt, nil
}

// deserializeNode recursively deserializes a B-tree node
func (bt *BTree) deserializeNode(file *os.File, nodeType BTreeNodeType) (*BTreeNode, error) {
	node := &BTreeNode{
		IsLeaf: nodeType == BTreeNodeLeaf,
		Keys:   make([]interface{}, 0),
	}

	// Read number of keys
	var numKeys uint32
	if err := binary.Read(file, binary.LittleEndian, &numKeys); err != nil {
		return nil, err
	}

	// Read keys
	for i := uint32(0); i < numKeys; i++ {
		key, err := bt.deserializeKey(file)
		if err != nil {
			return nil, err
		}
		node.Keys = append(node.Keys, key)
	}

	if node.IsLeaf {
		// Read values
		node.Values = make([]TupleID, 0)
		for i := uint32(0); i < numKeys; i++ {
			value, err := bt.deserializeValue(file)
			if err != nil {
				return nil, err
			}
			// Type assert to TupleID
			if tid, ok := value.(TupleID); ok {
				node.Values = append(node.Values, tid)
			} else {
				return nil, fmt.Errorf("expected TupleID, got %T", value)
			}
		}
	} else {
		// Read number of children
		var numChildren uint32
		if err := binary.Read(file, binary.LittleEndian, &numChildren); err != nil {
			return nil, err
		}

		// Recursively deserialize children
		node.Children = make([]*BTreeNode, 0)
		for i := uint32(0); i < numChildren; i++ {
			var childType BTreeNodeType
			if err := binary.Read(file, binary.LittleEndian, &childType); err != nil {
				return nil, err
			}

			child, err := bt.deserializeNode(file, childType)
			if err != nil {
				return nil, err
			}
			node.Children = append(node.Children, child)
		}
	}

	return node, nil
}

// deserializeKey reads a key from the file
func (bt *BTree) deserializeKey(file *os.File) (interface{}, error) {
	var typeMarker byte
	if err := binary.Read(file, binary.LittleEndian, &typeMarker); err != nil {
		return nil, err
	}

	switch typeMarker {
	case 1: // int
		var value int64
		if err := binary.Read(file, binary.LittleEndian, &value); err != nil {
			return nil, err
		}
		return int(value), nil
	case 2: // string
		var length uint32
		if err := binary.Read(file, binary.LittleEndian, &length); err != nil {
			return nil, err
		}
		strBytes := make([]byte, length)
		if _, err := file.Read(strBytes); err != nil {
			return nil, err
		}
		return string(strBytes), nil
	default:
		return nil, fmt.Errorf("unknown key type marker: %d", typeMarker)
	}
}

// deserializeValue reads a value from the file
func (bt *BTree) deserializeValue(file *os.File) (interface{}, error) {
	var pageID PageID
	if err := binary.Read(file, binary.LittleEndian, &pageID); err != nil {
		return nil, err
	}

	var slotNum uint16
	if err := binary.Read(file, binary.LittleEndian, &slotNum); err != nil {
		return nil, err
	}

	return TupleID{PageID: pageID, SlotNum: slotNum}, nil
}
