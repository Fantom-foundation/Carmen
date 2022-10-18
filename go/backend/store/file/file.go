package file

import (
	"errors"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/store/hashtree"
	"github.com/Fantom-foundation/Carmen/go/common"
	"io"
	"os"
)

// Store is a filesystem-based store.Store implementation - it stores mapping of ID to value in binary files.
type Store[I common.Identifier, V any] struct {
	file        *os.File
	hashTree    hashtree.HashTree
	serializer  common.Serializer[V]
	pageSize    int // the amount of bytes of one page
	pageItems   int // the amount of items stored in one page
	itemSize    int // the amount of bytes per one value
	itemDefault V
}

// NewStore constructs a new instance of FileStore.
// It needs a serializer of data items and the default value for a not-set item.
func NewStore[I common.Identifier, V any](path string, serializer common.Serializer[V], itemDefault V, pageSize int, branchingFactor int) (*Store[I, V], error) {
	if pageSize < serializer.Size() {
		return nil, fmt.Errorf("file store pageSize too small (minimum %d)", serializer.Size())
	}

	err := os.MkdirAll(path+"/hashes", 0700)
	if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(path+"/data", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open/create data file; %s", err)
	}

	s := &Store[I, V]{
		file:        file,
		serializer:  serializer,
		pageSize:    pageSize,
		pageItems:   pageSize / serializer.Size(),
		itemSize:    serializer.Size(),
		itemDefault: itemDefault,
	}
	s.hashTree = CreateHashTreeFactory(path+"/hashes", branchingFactor).Create(s)
	return s, nil
}

// itemPosition provides the position of an item in data pages
func (m *Store[I, V]) itemPosition(id I) (page int, position int64) {
	page = int(id / I(m.pageItems)) // casting to I for division in proper bit width
	pageStart := int64(page) * int64(m.pageSize)
	inPageStart := (int64(id) % int64(m.pageItems)) * int64(m.itemSize)
	position = pageStart + inPageStart
	return
}

// GetPage provides a page bytes for needs of the hash obtaining
func (m *Store[I, V]) GetPage(page int) ([]byte, error) {
	buffer := make([]byte, m.pageSize)

	_, err := m.file.Seek(int64(page)*int64(m.pageSize), io.SeekStart)
	if err != nil {
		return nil, err
	}

	_, err = m.file.Read(buffer)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err // the page does not exist in the data file yet
	}
	return buffer, nil
}

// Set a value of an item
func (m *Store[I, V]) Set(id I, value V) error {
	page, itemPosition := m.itemPosition(id)

	_, err := m.file.Seek(itemPosition, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek in data file to %d; %s", itemPosition, err)
	}

	_, err = m.file.Write(m.serializer.ToBytes(value))
	if err != nil {
		return fmt.Errorf("failed to write into data file; %s", err)
	}

	m.hashTree.MarkUpdated(page)
	return nil
}

// Get a value of the item (or the itemDefault, if not defined)
func (m *Store[I, V]) Get(id I) (V, error) {
	_, itemPosition := m.itemPosition(id)

	_, err := m.file.Seek(itemPosition, io.SeekStart)
	if err != nil {
		return m.itemDefault, err
	}

	bytes := make([]byte, m.itemSize)
	n, err := m.file.Read(bytes)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return m.itemDefault, nil // the item does not exist in the page file (the file is shorter)
		}
		return m.itemDefault, err
	}
	if n != m.itemSize {
		return m.itemDefault, fmt.Errorf("unable to read - page file is corrupted")
	}
	return m.serializer.FromBytes(bytes), nil
}

// GetStateHash computes and returns a cryptographical hash of the stored data
func (m *Store[I, V]) GetStateHash() (common.Hash, error) {
	return m.hashTree.HashRoot()
}

// Close the store
func (m *Store[I, V]) Close() error {
	return m.file.Close()
}
