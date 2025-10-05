package mindb

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// Page constants
const (
	PageSize     = 8192  // 8KB pages
	PageHeaderSize = 32  // Page header size
	SlotSize     = 4     // Each slot is 4 bytes (2 bytes offset + 2 bytes length)
	MaxTupleSize = PageSize - PageHeaderSize - SlotSize
)

// PageID represents a unique page identifier
type PageID uint32

// InvalidPageID represents an invalid page ID
const InvalidPageID = PageID(0xFFFFFFFF)

// LSN (Log Sequence Number) for WAL integration
type LSN uint64

// PageHeader contains metadata for a page
type PageHeader struct {
	PageID      PageID // Unique page identifier
	LSN         LSN    // Log sequence number for recovery
	Checksum    uint32 // CRC32 checksum of page data
	Flags       uint16 // Page flags (e.g., dirty, full)
	SlotCount   uint16 // Number of slots in use
	FreeStart   uint16 // Offset where free space starts
	FreeEnd     uint16 // Offset where free space ends (grows from end)
	Reserved    [12]byte // Reserved for future use
}

// Page flags
const (
	PageFlagDirty = 1 << 0 // Page has been modified
	PageFlagFull  = 1 << 1 // Page has no free space
)

// Slot represents a tuple location within a page
type Slot struct {
	Offset uint16 // Offset from start of page
	Length uint16 // Length of tuple
}

// Page represents a fixed-size page in the heap file
type Page struct {
	Header PageHeader
	Slots  []Slot
	Data   []byte // Raw page data (PageSize bytes)
}

// NewPage creates a new empty page
func NewPage(pageID PageID) *Page {
	page := &Page{
		Header: PageHeader{
			PageID:    pageID,
			LSN:       0,
			Checksum:  0,
			Flags:     0,
			SlotCount: 0,
			FreeStart: PageHeaderSize,
			FreeEnd:   PageSize,
		},
		Slots: make([]Slot, 0),
		Data:  make([]byte, PageSize),
	}
	
	// Initialize header in data
	page.serializeHeader()
	return page
}

// serializeHeader writes the header to the page data
func (p *Page) serializeHeader() {
	binary.LittleEndian.PutUint32(p.Data[0:4], uint32(p.Header.PageID))
	binary.LittleEndian.PutUint64(p.Data[4:12], uint64(p.Header.LSN))
	binary.LittleEndian.PutUint32(p.Data[12:16], p.Header.Checksum)
	binary.LittleEndian.PutUint16(p.Data[16:18], p.Header.Flags)
	binary.LittleEndian.PutUint16(p.Data[18:20], p.Header.SlotCount)
	binary.LittleEndian.PutUint16(p.Data[20:22], p.Header.FreeStart)
	binary.LittleEndian.PutUint16(p.Data[22:24], p.Header.FreeEnd)
}

// deserializeHeader reads the header from page data
func (p *Page) deserializeHeader() {
	p.Header.PageID = PageID(binary.LittleEndian.Uint32(p.Data[0:4]))
	p.Header.LSN = LSN(binary.LittleEndian.Uint64(p.Data[4:12]))
	p.Header.Checksum = binary.LittleEndian.Uint32(p.Data[12:16])
	p.Header.Flags = binary.LittleEndian.Uint16(p.Data[16:18])
	p.Header.SlotCount = binary.LittleEndian.Uint16(p.Data[18:20])
	p.Header.FreeStart = binary.LittleEndian.Uint16(p.Data[20:22])
	p.Header.FreeEnd = binary.LittleEndian.Uint16(p.Data[22:24])
	
	// Load slots
	p.Slots = make([]Slot, p.Header.SlotCount)
	for i := uint16(0); i < p.Header.SlotCount; i++ {
		slotOffset := PageHeaderSize + (i * SlotSize)
		p.Slots[i].Offset = binary.LittleEndian.Uint16(p.Data[slotOffset : slotOffset+2])
		p.Slots[i].Length = binary.LittleEndian.Uint16(p.Data[slotOffset+2 : slotOffset+4])
	}
}

// serializeSlots writes slots to the page data
func (p *Page) serializeSlots() {
	for i, slot := range p.Slots {
		slotOffset := PageHeaderSize + (uint16(i) * SlotSize)
		binary.LittleEndian.PutUint16(p.Data[slotOffset:slotOffset+2], slot.Offset)
		binary.LittleEndian.PutUint16(p.Data[slotOffset+2:slotOffset+4], slot.Length)
	}
}

// FreeSpace returns the amount of free space in the page
func (p *Page) FreeSpace() uint16 {
	if p.Header.FreeEnd <= p.Header.FreeStart {
		return 0
	}
	return p.Header.FreeEnd - p.Header.FreeStart
}

// CanFit checks if a tuple of given size can fit in the page
func (p *Page) CanFit(tupleSize uint16) bool {
	// Need space for the tuple data + one slot entry
	requiredSpace := tupleSize + SlotSize
	return p.FreeSpace() >= requiredSpace
}

// InsertTuple inserts a tuple into the page and returns the slot number
func (p *Page) InsertTuple(tupleData []byte) (uint16, error) {
	tupleSize := uint16(len(tupleData))
	
	if !p.CanFit(tupleSize) {
		return 0, fmt.Errorf("insufficient space in page")
	}
	
	if tupleSize > MaxTupleSize {
		return 0, fmt.Errorf("tuple too large: %d bytes (max %d)", tupleSize, MaxTupleSize)
	}
	
	// Allocate space from the end of the page
	p.Header.FreeEnd -= tupleSize
	tupleOffset := p.Header.FreeEnd
	
	// Copy tuple data
	copy(p.Data[tupleOffset:tupleOffset+tupleSize], tupleData)
	
	// Add slot entry
	slot := Slot{
		Offset: tupleOffset,
		Length: tupleSize,
	}
	p.Slots = append(p.Slots, slot)
	p.Header.SlotCount++
	
	// Update free space start (grows with slot directory)
	p.Header.FreeStart = PageHeaderSize + (p.Header.SlotCount * SlotSize)
	
	// Mark page as dirty
	p.Header.Flags |= PageFlagDirty
	
	// Update page if full
	if p.FreeSpace() < SlotSize+64 { // Keep some minimum free space
		p.Header.Flags |= PageFlagFull
	}
	
	// Serialize header and slots
	p.serializeHeader()
	p.serializeSlots()
	
	return p.Header.SlotCount - 1, nil
}

// GetTuple retrieves a tuple by slot number
func (p *Page) GetTuple(slotNum uint16) ([]byte, error) {
	if slotNum >= p.Header.SlotCount {
		return nil, fmt.Errorf("invalid slot number: %d", slotNum)
	}
	
	slot := p.Slots[slotNum]
	if slot.Length == 0 {
		return nil, fmt.Errorf("slot %d is empty", slotNum)
	}
	
	// Extract tuple data
	tupleData := make([]byte, slot.Length)
	copy(tupleData, p.Data[slot.Offset:slot.Offset+slot.Length])
	
	return tupleData, nil
}

// DeleteTuple marks a tuple as deleted (sets length to 0)
func (p *Page) DeleteTuple(slotNum uint16) error {
	if slotNum >= p.Header.SlotCount {
		return fmt.Errorf("invalid slot number: %d", slotNum)
	}
	
	// Mark slot as empty
	p.Slots[slotNum].Length = 0
	
	// Mark page as dirty
	p.Header.Flags |= PageFlagDirty
	
	// Clear full flag as we now have space
	p.Header.Flags &^= PageFlagFull
	
	// Serialize slots
	p.serializeSlots()
	p.serializeHeader()
	
	return nil
}

// UpdateTuple updates a tuple in place if it fits, otherwise returns error
func (p *Page) UpdateTuple(slotNum uint16, newData []byte) error {
	if slotNum >= p.Header.SlotCount {
		return fmt.Errorf("invalid slot number: %d", slotNum)
	}
	
	slot := &p.Slots[slotNum]
	newSize := uint16(len(newData))
	
	// For now, only support in-place updates (same size or smaller)
	if newSize > slot.Length {
		return fmt.Errorf("tuple too large for in-place update")
	}
	
	// Copy new data
	copy(p.Data[slot.Offset:slot.Offset+newSize], newData)
	
	// Update slot length if smaller
	slot.Length = newSize
	
	// Mark page as dirty
	p.Header.Flags |= PageFlagDirty
	
	// Serialize slots
	p.serializeSlots()
	p.serializeHeader()
	
	return nil
}

// Compact reorganizes the page to reclaim space from deleted tuples
func (p *Page) Compact() {
	// Build list of active tuples
	activeTuples := make([][]byte, 0, p.Header.SlotCount)
	activeSlots := make([]uint16, 0, p.Header.SlotCount)
	
	for i := uint16(0); i < p.Header.SlotCount; i++ {
		if p.Slots[i].Length > 0 {
			tupleData := make([]byte, p.Slots[i].Length)
			copy(tupleData, p.Data[p.Slots[i].Offset:p.Slots[i].Offset+p.Slots[i].Length])
			activeTuples = append(activeTuples, tupleData)
			activeSlots = append(activeSlots, i)
		}
	}
	
	// Reset page
	p.Header.FreeEnd = PageSize
	p.Slots = make([]Slot, p.Header.SlotCount)
	
	// Re-insert active tuples
	for i, tupleData := range activeTuples {
		tupleSize := uint16(len(tupleData))
		p.Header.FreeEnd -= tupleSize
		
		copy(p.Data[p.Header.FreeEnd:p.Header.FreeEnd+tupleSize], tupleData)
		
		p.Slots[activeSlots[i]] = Slot{
			Offset: p.Header.FreeEnd,
			Length: tupleSize,
		}
	}
	
	// Update header
	p.Header.Flags |= PageFlagDirty
	p.Header.Flags &^= PageFlagFull
	
	p.serializeHeader()
	p.serializeSlots()
}

// ComputeChecksum computes CRC32 checksum of page data (excluding checksum field)
func (p *Page) ComputeChecksum() uint32 {
	// Checksum everything except the checksum field itself
	data := make([]byte, PageSize)
	copy(data, p.Data)
	
	// Zero out checksum field
	binary.LittleEndian.PutUint32(data[12:16], 0)
	
	return crc32.ChecksumIEEE(data)
}

// UpdateChecksum updates the page checksum
func (p *Page) UpdateChecksum() {
	p.Header.Checksum = p.ComputeChecksum()
	binary.LittleEndian.PutUint32(p.Data[12:16], p.Header.Checksum)
}

// VerifyChecksum verifies the page checksum
func (p *Page) VerifyChecksum() bool {
	storedChecksum := p.Header.Checksum
	computedChecksum := p.ComputeChecksum()
	return storedChecksum == computedChecksum
}

// IsDirty returns true if the page has been modified
func (p *Page) IsDirty() bool {
	return (p.Header.Flags & PageFlagDirty) != 0
}

// ClearDirty clears the dirty flag
func (p *Page) ClearDirty() {
	p.Header.Flags &^= PageFlagDirty
	p.serializeHeader()
}

// GetFreeSpace returns the amount of free space in the page
func (p *Page) GetFreeSpace() uint16 {
	if p.Header.FreeEnd <= p.Header.FreeStart {
		return 0
	}
	return p.Header.FreeEnd - p.Header.FreeStart
}

// LoadFromBytes loads a page from raw bytes
func LoadPageFromBytes(data []byte) (*Page, error) {
	if len(data) != PageSize {
		return nil, fmt.Errorf("invalid page size: %d (expected %d)", len(data), PageSize)
	}
	
	page := &Page{
		Data: make([]byte, PageSize),
	}
	copy(page.Data, data)
	
	// Deserialize header and slots
	page.deserializeHeader()
	
	// Verify checksum
	if !page.VerifyChecksum() {
		return nil, fmt.Errorf("page checksum mismatch")
	}
	
	return page, nil
}
