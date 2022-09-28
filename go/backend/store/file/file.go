package file

import (
	"errors"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/store"
	"github.com/Fantom-foundation/Carmen/go/common"
	"io"
	"io/fs"
	"os"
)

// Store is a filesystem-based store.Store implementation - it stores mapping of ID to value in binary files.
type Store[V any] struct {
	path        string
	hashTree    store.HashTree
	serializer  common.Serializer[V]
	pageSize    uint64 // the amount of items stored in one database page
	itemSize    int    // the amount of bytes per one value
	itemDefault V
}

// NewStore constructs a new instance of FileStore.
// It needs a serializer of data items and the default value for a not-set item.
func NewStore[V any](path string, serializer common.Serializer[V], itemDefault V, pageSize uint64, hashTreeFactor int) (*Store[V], error) {
	err := os.MkdirAll(path+"/pages", 0700)
	if err != nil {
		return nil, err
	}
	err = os.MkdirAll(path+"/hashes", 0700)
	if err != nil {
		return nil, err
	}
	hashTree := NewHashTree(path+"/hashes", hashTreeFactor)
	s := Store[V]{
		path:        path,
		hashTree:    &hashTree,
		serializer:  serializer,
		pageSize:    pageSize,
		itemSize:    serializer.Size(),
		itemDefault: itemDefault,
	}
	return &s, nil
}

// itemPosition provides the position of an item in data pages
func (m *Store[V]) itemPosition(id uint64) (page int, position int64) {
	return int(id / m.pageSize), int64(id%m.pageSize) * int64(m.serializer.Size())
}

func (m *Store[V]) pageFile(page int) (path string) {
	return fmt.Sprintf("%s/pages/%X", m.path, page)
}

// GetPage provides a page bytes for needs of the hash obtaining
func (m *Store[V]) GetPage(page int) ([]byte, error) {
	return os.ReadFile(m.pageFile(page))
}

// Set a value of an item
func (m *Store[V]) Set(id uint64, value V) error {
	page, itemPosition := m.itemPosition(id)

	file, err := os.OpenFile(m.pageFile(page), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return fmt.Errorf("failed to open page file %d; %s", page, err)
	}
	defer file.Close()

	_, err = file.Seek(itemPosition, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek in page file %d to %d; %s", page, itemPosition, err)
	}

	_, err = file.Write(m.serializer.ToBytes(value))
	if err != nil {
		return fmt.Errorf("failed to write into page file %d; %s", page, err)
	}

	m.hashTree.MarkUpdated(page)
	return nil
}

// Get a value of the item (or the itemDefault, if not defined)
func (m *Store[V]) Get(id uint64) (V, error) {
	page, itemPosition := m.itemPosition(id)

	file, err := os.OpenFile(m.pageFile(page), os.O_RDONLY, 0600)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return m.itemDefault, nil // page file does not exist
		}
		return m.itemDefault, fmt.Errorf("failed to open page file %d; %s", id, err)
	}
	defer file.Close()

	_, err = file.Seek(itemPosition, io.SeekStart)
	if err != nil {
		return m.itemDefault, err
	}

	bytes := make([]byte, m.itemSize)
	n, err := file.Read(bytes)
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
func (m *Store[V]) GetStateHash() (common.Hash, error) {
	return m.hashTree.HashRoot(m)
}

// Close the store
func (m *Store[V]) Close() error {
	return nil // no-op - we are not keeping any files open
}
