package file

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree"
	"github.com/Fantom-foundation/Carmen/go/common"
	"io"
	"os"
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

func (m *Depot[I]) getOffsetLength(offsetBytes []byte) (offset uint64, length uint32) {
	offset = binary.LittleEndian.Uint64(offsetBytes[0:OffsetSize])
	length = binary.LittleEndian.Uint32(offsetBytes[OffsetSize : OffsetSize+LengthSize])
	return
}

func (m *Depot[I]) GetPage(hashGroup int) (out []byte, err error) {
	startKey := I(m.hashItems * hashGroup)
	offsets := make([]uint64, m.hashItems)
	lengths := make([]uint32, m.hashItems)
	totalLen := uint32(0)

	_, startPosition := m.itemPosition(startKey)
	offsetBytes := make([]byte, (OffsetSize+LengthSize)*m.hashItems)
	_, err = m.offsetsFile.ReadAt(offsetBytes[:], startPosition)
	if err != nil && !errors.Is(err, io.EOF) {
		return
	}

	isFragmented := false
	offsetPos := 0
	for i := 0; i < m.hashItems; i++ {
		offset, length := m.getOffsetLength(offsetBytes[offsetPos:])
		offsets[i] = offset
		lengths[i] = length
		totalLen += length
		// follows the item directly the previous one in the data file?
		if i > 0 && length != 0 && offsets[i-1]+uint64(lengths[i-1]) != offset {
			isFragmented = true
		}
		offsetPos += OffsetSize + LengthSize
	}

	out = make([]byte, totalLen)
	if !isFragmented {
		_, err = m.contentsFile.ReadAt(out, int64(offsets[0]))
		return out, err
	}

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
	_, itemPosition := m.itemPosition(id)
	var offsetBytes [OffsetSize + LengthSize]byte
	_, err = m.offsetsFile.ReadAt(offsetBytes[:], itemPosition)
	if err != nil {
		return nil, err
	}
	offset, length := m.getOffsetLength(offsetBytes[:])
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
