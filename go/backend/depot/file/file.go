package file

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree"
	"github.com/Fantom-foundation/Carmen/go/common"
	"io"
	"os"
	"unsafe"
)

const OffsetSize = 8 // uint64
const LengthSize = 4 // uint32

// Depot is a file-based Depot implementation
type Depot[I common.Identifier] struct {
	contentsFile    *os.File
	offsetsFile     *os.File
	hashTree        hashtree.HashTree
	indexSerializer common.Serializer[I]
	hashItems       int // the amount of items in one hashing group
}

// NewDepot constructs a new instance of Depot.
func NewDepot[I common.Identifier](path string,
	indexSerializer common.Serializer[I],
	hashtreeFactory hashtree.Factory,
	hashItems int,
) (*Depot[I], error) {

	contentsFile, err := os.OpenFile(path+"/data", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open/create data file; %s", err)
	}

	offsetsFile, err := os.OpenFile(path+"/offsets", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open/create offsets file; %s", err)
	}

	m := &Depot[I]{
		contentsFile:    contentsFile,
		offsetsFile:     offsetsFile,
		indexSerializer: indexSerializer,
		hashItems:       hashItems,
	}
	m.hashTree = hashtreeFactory.Create(m)
	return m, nil
}

// itemHashGroup provides the hash group into which the item belongs
func (m *Depot[I]) itemHashGroup(id I) (page int) {
	// casting to I for division in proper bit width
	return int(id / I(m.hashItems))
}

// itemPosition provides the position of an item in data pages
func (m *Depot[I]) itemPosition(id I) (hashGroup int, position int64) {
	hashGroup = int(id / I(m.hashItems)) // casting to I for division in proper bit width
	position = int64(id) * (OffsetSize + LengthSize)
	return
}

// hashGroupRange provides range of data indexes of given hashing group
func (m *Depot[I]) hashGroupRange(group int) (start I, end I) {
	return I(m.hashItems * group), I((m.hashItems * group) + m.hashItems)
}

func (m *Depot[I]) GetPage(hashGroup int) (out []byte, err error) {
	startKey, endKey := m.hashGroupRange(hashGroup)
	offsets := make([]uint64, m.hashItems)
	lengths := make([]uint32, m.hashItems)
	totalLen := uint32(0)
	for i := I(0); startKey+i < endKey; i++ {
		offset, length, err := m.getOffsetLen(startKey + i)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		offsets[i] = offset
		lengths[i] = length
		totalLen += length
	}
	out = make([]byte, totalLen)
	itemStart := uint32(0)
	for i := 0; i < m.hashItems; i++ {
		length := lengths[i]
		_, err = m.contentsFile.ReadAt(out[itemStart:itemStart+length], int64(offsets[i]))
		if err != nil {
			return nil, err
		}
		itemStart += length
	}
	return
}

// Set a value of an item
func (m *Depot[I]) Set(id I, value []byte) error {
	hashGroup, itemPosition := m.itemPosition(id)
	// write the value to the end of data file
	offset, err := m.contentsFile.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	_, err = m.contentsFile.Write(value)
	if err != nil {
		return err
	}
	// write the start of the value into the starts file
	var offsetBytes [OffsetSize + LengthSize]byte
	binary.LittleEndian.PutUint64(offsetBytes[0:OffsetSize], uint64(offset))
	binary.LittleEndian.PutUint32(offsetBytes[OffsetSize:OffsetSize+LengthSize], uint32(len(value)))
	_, err = m.offsetsFile.WriteAt(offsetBytes[:], itemPosition)
	if err != nil {
		return err
	}
	m.hashTree.MarkUpdated(hashGroup)
	return nil
}

// Get a value of the item (or nil if not defined)
func (m *Depot[I]) Get(id I) (out []byte, err error) {
	offset, length, err := m.getOffsetLen(id)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, err
	}
	if length == 0 {
		return nil, nil
	}
	out = make([]byte, length)
	_, err = m.contentsFile.ReadAt(out, int64(offset))
	return
}

func (m *Depot[I]) getOffsetLen(id I) (offset uint64, length uint32, err error) {
	_, itemPosition := m.itemPosition(id)
	var offsetBytes [OffsetSize + LengthSize]byte
	_, err = m.offsetsFile.ReadAt(offsetBytes[:], itemPosition)
	if err != nil {
		return 0, 0, err
	}
	offset = binary.LittleEndian.Uint64(offsetBytes[0:OffsetSize])
	length = binary.LittleEndian.Uint32(offsetBytes[OffsetSize : OffsetSize+LengthSize])
	return
}

// GetSize of the item (or 0 if not defined)
func (m *Depot[I]) GetSize(id I) (length int, err error) {
	_, itemPosition := m.itemPosition(id)
	var lengthBytes [LengthSize]byte
	_, err = m.offsetsFile.ReadAt(lengthBytes[:], itemPosition+OffsetSize)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return 0, nil
		}
		return 0, err
	}
	return int(binary.LittleEndian.Uint32(lengthBytes[:])), nil
}

// GetStateHash computes and returns a cryptographical hash of the stored data
func (m *Depot[I]) GetStateHash() (common.Hash, error) {
	return m.hashTree.HashRoot()
}

// Flush the depot
func (m *Depot[I]) Flush() error {
	// commit state hash root
	_, err := m.GetStateHash()
	return err
}

// Close the store
func (m *Depot[I]) Close() error {
	return m.Flush()
}

// GetMemoryFootprint provides the size of the depot in memory in bytes
func (m *Depot[I]) GetMemoryFootprint() common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*m))
	mf.AddChild("hashTree", m.hashTree.GetMemoryFootprint())
	return mf
}
