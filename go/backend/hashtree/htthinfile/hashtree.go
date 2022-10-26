package htthinfile

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/hashtree"
	"github.com/Fantom-foundation/Carmen/go/common"
	"hash"
	"io"
	"os"
)

const HashSize = 32
const FileHeaderSize = 8

// HashTree is a structure allowing to make a hash of the whole database state.
// It obtains hashes of individual data pages and reduce them to a hash of the entire state.
type HashTree struct {
	path            string
	factor          int          // the branching factor - amount of child nodes per one parent node
	tree            [][]byte     // tree of hashes [layer][bytes of hashes]
	dirtyPages      map[int]bool // set of dirty pages
	maxDirtyPage    int
	totalPages      int
	hashesGroupSize int
	pageProvider    hashtree.PageProvider
}

// HashTreeFactory is used for implementation of HashTreeFactory method
type HashTreeFactory struct {
	path            string
	branchingFactor int
}

// CreateHashTreeFactory creates a new instance of the hashTreeFactory
func CreateHashTreeFactory(path string, branchingFactor int) *HashTreeFactory {
	return &HashTreeFactory{path: path, branchingFactor: branchingFactor}
}

// Create creates a new instance of the HashTree
func (f *HashTreeFactory) Create(pageProvider hashtree.PageProvider) hashtree.HashTree {
	return &HashTree{
		path:            f.path,
		factor:          f.branchingFactor,
		tree:            [][]byte{{}},
		dirtyPages:      make(map[int]bool),
		maxDirtyPage:    -1,
		hashesGroupSize: HashSize * f.branchingFactor,
		pageProvider:    pageProvider,
	}
}

func (ht *HashTree) resetDirtyPages() {
	ht.dirtyPages = make(map[int]bool)
	ht.maxDirtyPage = -1
}

// MarkUpdated marks a page as changed - to be included into the hash recalculation on commit
func (ht *HashTree) MarkUpdated(page int) {
	ht.dirtyPages[page] = true
	if page > ht.maxDirtyPage {
		ht.maxDirtyPage = page
	}
}

// calculateHash computes the hash of given data
func calculateHash(hasher hash.Hash, content []byte) (hash []byte, err error) {
	hasher.Reset()
	_, err = hasher.Write(content)
	if err != nil {
		return nil, err
	}
	return hasher.Sum(nil), nil
}

// buildHashTree builds the tree from hashes in the file, updated by the dirty pages set
func (ht *HashTree) buildHashTree(hasher hash.Hash, hashesFile *os.File) (err error) {
	ht.tree = [][]byte{{}}
	wasEof := false
	for rangeStart := 0; rangeStart < ht.totalPages; rangeStart += ht.factor {
		pagesHashes := make([]byte, ht.hashesGroupSize)

		if !wasEof {
			_, err := hashesFile.Read(pagesHashes)
			if err != nil {
				if errors.Is(err, io.EOF) {
					wasEof = true
				} else {
					return err
				}
			}
		}

		hashGroupChanged, err := ht.updateDirtyHashes(hasher, rangeStart, pagesHashes)
		if err != nil {
			return err
		}

		if hashGroupChanged { // write hashes changes into the hashes file
			_, err = hashesFile.WriteAt(pagesHashes, int64(rangeStart)*int64(HashSize)+FileHeaderSize)
			if err != nil {
				return fmt.Errorf("failed to write into the hashes file; %s", err)
			}
		}

		err = ht.addHashesIntoTree(hasher, pagesHashes)
		if err != nil {
			return err
		}
	}
	ht.resetDirtyPages()
	return nil
}

// addHashesIntoTree adds a batch of pages hashes into the hash tree
func (ht *HashTree) addHashesIntoTree(hasher hash.Hash, pagesHashes []byte) (err error) {
	levelHash, err := calculateHash(hasher, pagesHashes)
	if err != nil {
		return err
	}
	return ht.appendHashToLevel(hasher, 0, levelHash)
}

// appendHashToLevel appends the hash to given level of the tree, if the level is full, reduce to the next level
func (ht *HashTree) appendHashToLevel(h hash.Hash, level int, hash []byte) (err error) {
	if len(ht.tree) <= level {
		ht.tree = append(ht.tree, make([]byte, 0, ht.hashesGroupSize))
	}

	ht.tree[level] = append(ht.tree[level], hash...)

	// layer full - reduce the whole layer to one value of the following level
	if len(ht.tree[level]) >= ht.hashesGroupSize {
		layerHash, err := calculateHash(h, ht.tree[level])
		if err != nil {
			return err
		}
		ht.tree[level] = make([]byte, 0, ht.hashesGroupSize) // empty this layer
		err = ht.appendHashToLevel(h, level+1, layerHash)
		return err
	}
	return nil
}

// updateDirtyHashes updates the group of pages hashes from the dirty pages set
func (ht *HashTree) updateDirtyHashes(hasher hash.Hash, firstHashPage int, pagesHashes []byte) (changed bool, err error) {
	for i := 0; i < ht.factor; i++ {
		page := firstHashPage + i
		if _, isDirty := ht.dirtyPages[page]; isDirty {
			pageContent, err := ht.pageProvider.GetPage(page)
			if err != nil {
				return false, err
			}
			pageHash, err := calculateHash(hasher, pageContent)
			if err != nil {
				return false, err
			}
			copy(pagesHashes[HashSize*i:HashSize*(i+1)], pageHash)
			changed = true
		}
	}
	return changed, nil
}

// finishTreeHash pads the values in the tree and reduce the tree to a single value
func (ht *HashTree) finishTreeHash(h hash.Hash) (out common.Hash, err error) {
	for level := 0; level < len(ht.tree); level++ {
		if level == len(ht.tree)-1 && len(ht.tree[level]) == HashSize { // is the last layer?
			copy(out[:], ht.tree[level])
			return out, nil
		}
		if len(ht.tree[level]) > 0 { // not-empty layer - hash padded with zeros into the next layer
			padding := make([]byte, ht.hashesGroupSize-len(ht.tree[level]))
			paddedLevel := append(ht.tree[level], padding...)
			layerHash, err := calculateHash(h, paddedLevel)
			if err != nil {
				return common.Hash{}, err
			}
			err = ht.appendHashToLevel(h, level+1, layerHash)
			if err != nil {
				return common.Hash{}, err
			}
		}
	}
	panic("unexpected finishTreeHash state")
}

func (ht *HashTree) processFileHeader(hashesFile *os.File) (err error) {
	totalPages, err := loadFileHeader(hashesFile)
	if err != nil {
		return err
	}

	if totalPages < ht.maxDirtyPage+1 {
		totalPages = ht.maxDirtyPage + 1
	}
	ht.totalPages = totalPages

	err = writeFileHeader(hashesFile, totalPages)
	return err
}

func loadFileHeader(hashesFile *os.File) (totalPages int, err error) {
	buffer := make([]byte, FileHeaderSize)
	_, err = hashesFile.Read(buffer)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return 0, nil
		}
		return 0, err
	}
	return int(binary.LittleEndian.Uint64(buffer)), nil
}

func writeFileHeader(hashesFile *os.File, totalPages int) (err error) {
	buffer := make([]byte, FileHeaderSize)
	_, err = hashesFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(buffer, uint64(totalPages))
	_, err = hashesFile.Write(buffer)
	return
}

// HashRoot provides the hash in the root of the hashing tree
func (ht *HashTree) HashRoot() (out common.Hash, err error) {
	hashesFile, err := os.OpenFile(ht.path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed to open/create hashes file; %s", err)
	}
	defer hashesFile.Close()
	hasher := sha256.New() // the hasher is created once for the whole block as it hashes the fastest

	err = ht.processFileHeader(hashesFile)
	if err != nil {
		return common.Hash{}, err
	}

	err = ht.buildHashTree(hasher, hashesFile)
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed to build hashtree; %s", err)
	}

	// special cases
	if ht.totalPages == 0 {
		return common.Hash{}, nil
	}
	if ht.totalPages == 1 {
		_, err = hashesFile.ReadAt(out[:], FileHeaderSize)
		return out, err
	}

	return ht.finishTreeHash(hasher)
}
