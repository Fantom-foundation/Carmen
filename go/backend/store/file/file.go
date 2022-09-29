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
type Store[I common.Identifier, V any] struct {
	path            string
	hashTreeFactory store.HashTreeFactory
	serializer      common.Serializer[V]
	pageSize        int // the amount of items stored in one database page
	itemSize        int // the amount of bytes per one value
	itemDefault     V
}

// NewStore constructs a new instance of FileStore.
// It needs a serializer of data items and the default value for a not-set item.
func NewStore[I common.Identifier, V any](path string, serializer common.Serializer[V], itemDefault V, pageSize int, branchingFactor int) (*Store[I, V], error) {
	err := os.MkdirAll(path+"/pages", 0700)
	if err != nil {
		return nil, err
	}
	err = os.MkdirAll(path+"/hashes", 0700)
	if err != nil {
		return nil, err
	}
	s := Store[I, V]{
		path:            path,
		hashTreeFactory: CreateHashTreeFactory(path+"/hashes", branchingFactor),
		serializer:      serializer,
		pageSize:        pageSize,
		itemSize:        serializer.Size(),
		itemDefault:     itemDefault,
	}
	return &s, nil
}

// itemPosition provides the position of an item in data pages
func (m *Store[I, V]) itemPosition(id I) (page int, position int64) {
	return int(id) / m.pageSize, int64(int(id)%m.pageSize) * int64(m.itemSize)
}

func (m *Store[I, V]) pageFile(page int) (path string) {
	return fmt.Sprintf("%s/pages/%X", m.path, page)
}

// GetPage provides a page bytes for needs of the hash obtaining
func (m *Store[I, V]) GetPage(page int) ([]byte, error) {
	data, err := os.ReadFile(m.pageFile(page))
	if err != nil {
		return nil, err
	}
	if len(data) < int(m.pageSize)*m.itemSize {
		data = append(data, make([]byte, int(m.pageSize)*m.itemSize-len(data))...)
	}
	return data, err
}

// Set a value of an item
func (m *Store[I, V]) Set(id I, value V) error {
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

	m.hashTreeFactory.Create(m).MarkUpdated(page)
	return nil
}

// Get a value of the item (or the itemDefault, if not defined)
func (m *Store[I, V]) Get(id I) (V, error) {
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
func (m *Store[I, V]) GetStateHash() (common.Hash, error) {
	return m.hashTreeFactory.Create(m).HashRoot()
}

// Close the store
func (m *Store[I, V]) Close() error {
	return nil // no-op - we are not keeping any files open
}
