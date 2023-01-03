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
	hashItems       int    // the amount of items in one hashing group
	offsetData      []byte // recycled slice for offsets data in GetPage call
	emptyHashPage   []byte // pre-generated slice for empty hashing pages
	pagesCalls      int    // amount of GetPage calls in total
	fragmentedCalls int    // amount of GetPage calls being fragmented
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
		offsetData:      make([]byte, (OffsetSize+LengthSize)*hashItems),
		emptyHashPage:   make([]byte, LengthSize*hashItems),
	}
	m.hashTree = hashtreeFactory.Create(m)
	return m, nil
}

// itemPage provides the page (hash group) into which the item belongs
func (m *Depot[I]) itemPage(id I) int {
	// casting to I for division in proper bit width
	return int(id / I(m.hashItems))
}

// itemPosition provides the position of an item in data pages
func (m *Depot[I]) itemPosition(id I) int64 {
	return int64(id) * (OffsetSize + LengthSize)
}

// GetPage provides all data of one hashing group in a byte slice
func (m *Depot[I]) GetPage(page int) ([]byte, error) {
	startKey := I(m.hashItems * page)
	m.pagesCalls++

	startPosition := m.itemPosition(startKey)
	offsetsLen, err := m.offsetsFile.ReadAt(m.offsetData, startPosition)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	offsetData := m.offsetData[0:offsetsLen]

	dataStart, dataLength, isFragmented := getRangeFromOffsets(offsetData)

	if dataLength == 0 {
		return m.emptyHashPage, nil
	}

	// the output consists of values lengths prefix and the values itself
	prefixLength := m.hashItems * LengthSize
	out := make([]byte, int64(prefixLength)+dataLength)
	copyOffsetsToLengths(out[0:prefixLength], offsetData)

	if isFragmented { // slow path for fragmented data
		m.fragmentedCalls++
		err = readFragmentedPageItems(m.contentsFile, offsetData, out[prefixLength:])
		return out, err
	}

	// fast path
	_, err = m.contentsFile.ReadAt(out[prefixLength:], dataStart)
	return out, err
}

// copyOffsetsToLengths copy values lengths from the offsets slice
func copyOffsetsToLengths(out []byte, offsets []byte) {
	outOffset := 0
	for position := OffsetSize; position < len(offsets); position += OffsetSize + LengthSize {
		copy(out[outOffset:outOffset+LengthSize], offsets[position:])
		outOffset += LengthSize
	}
}

// parseOffsetLength parse the offsets slice to obtain the first offsets and length
func parseOffsetLength(offsetBytes []byte) (offset int64, length int32) {
	offset = int64(binary.LittleEndian.Uint64(offsetBytes[0:OffsetSize]))
	length = int32(binary.LittleEndian.Uint32(offsetBytes[OffsetSize : OffsetSize+LengthSize]))
	return offset, length
}

// readFragmentedPageItems reads the page data by the offsets slice
func readFragmentedPageItems(contentsFile *os.File, offsets []byte, out []byte) error {
	outOffset := int32(0)
	for position := 0; position < len(offsets); position += OffsetSize + LengthSize {
		offset, length := parseOffsetLength(offsets[position:])
		_, err := contentsFile.ReadAt(out[outOffset:outOffset+length], offset)
		if err != nil {
			return err
		}
		outOffset += length
	}
	return nil
}

// getRangeFromOffsets parse offset data of one page (hash group) and provides
// the page data start, length and whether the page is fragmented.
// If the page is fragmented, dataStart output is irrelevant.
func getRangeFromOffsets(offsetData []byte) (dataStart, dataLength int64, isFragmented bool) {
	for position := 0; position < len(offsetData); position += OffsetSize + LengthSize {
		offset, length := parseOffsetLength(offsetData[position:])
		if length != 0 { // zero-length values are ignored
			if dataLength == 0 { // is first not-empty
				dataStart = offset
				dataLength = int64(length)
			} else {
				// follows the item directly the previous one in the data file?
				if offset != dataStart+dataLength {
					isFragmented = true
				}
				dataLength += int64(length)
			}
		}
	}
	return dataStart, dataLength, isFragmented
}

// Set a value of an item
func (m *Depot[I]) Set(id I, value []byte) error {
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
	_, err = m.offsetsFile.WriteAt(offsetBytes[:], m.itemPosition(id))
	if err != nil {
		return err
	}
	m.hashTree.MarkUpdated(m.itemPage(id))
	return nil
}

// Get a value of the item (or nil if not defined)
func (m *Depot[I]) Get(id I) (out []byte, err error) {
	var offsetBytes [OffsetSize + LengthSize]byte
	_, err = m.offsetsFile.ReadAt(offsetBytes[:], m.itemPosition(id))
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, err
	}
	offset, length := parseOffsetLength(offsetBytes[:])
	if length == 0 {
		return nil, nil
	}
	out = make([]byte, length)
	_, err = m.contentsFile.ReadAt(out, int64(offset))
	return
}

// GetSize of the item (or 0 if not defined)
func (m *Depot[I]) GetSize(id I) (length int, err error) {
	var lengthBytes [LengthSize]byte
	_, err = m.offsetsFile.ReadAt(lengthBytes[:], m.itemPosition(id)+OffsetSize)
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
func (m *Depot[I]) GetMemoryFootprint() *common.MemoryFootprint {
	mf := common.NewMemoryFootprint(unsafe.Sizeof(*m))
	mf.AddChild("hashTree", m.hashTree.GetMemoryFootprint())
	mf.SetNote(m.getFragmentationReport())
	return mf
}

func (m *Depot[I]) getFragmentationReport() string {
	fragRatio := float32(m.fragmentedCalls) / float32(m.pagesCalls)
	return fmt.Sprintf("(pagesCalls: %d, fragmented: %d, fragRatio: %f)", m.pagesCalls, m.fragmentedCalls, fragRatio)
}
